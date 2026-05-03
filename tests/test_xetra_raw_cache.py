"""Tests for XetraService raw JSON cache: write, resume, cleanup, reprocess."""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from yf_parqed.xetra.xetra_service import XetraService
from yf_parqed.xetra.exceptions import XetraSchemaUnknownError
from yf_parqed.common.partition_path_builder import PartitionPathBuilder
from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service(tmp_path, fetcher=None, parser=None):
    path_builder = PartitionPathBuilder(tmp_path)
    backend = PartitionedStorageBackend(
        empty_frame_factory=lambda: pd.DataFrame(),
        normalizer=lambda df: df,
        column_provider=lambda: [],
        path_builder=path_builder,
    )
    return XetraService(
        fetcher=fetcher or Mock(),
        parser=parser or Mock(),
        backend=backend,
        root_path=tmp_path,
    )


def _cache_path(tmp_path, venue, date_str, filename, market="de", source="xetra"):
    from datetime import datetime

    d = datetime.strptime(date_str, "%Y-%m-%d")
    return (
        tmp_path
        / market
        / source
        / "raw"
        / venue
        / f"year={d.year}"
        / f"month={d.month:02d}"
        / f"day={d.day:02d}"
        / filename
    )


def _daily_parquet(tmp_path, venue, year, month, day, market="de", source="xetra"):
    return (
        tmp_path
        / market
        / source
        / "trades"
        / f"venue={venue}"
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
        / "trades.parquet"
    )


def _monthly_parquet(tmp_path, venue, year, month, market="de", source="xetra"):
    return (
        tmp_path
        / market
        / source
        / "trades_monthly"
        / f"venue={venue}"
        / f"year={year}"
        / f"month={month:02d}"
        / "trades.parquet"
    )


def _write_minimal_parquet(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"isin": pa.array(["DE0001"], type=pa.string())})
    pq.write_table(table, str(path))


# ---------------------------------------------------------------------------
# _save_to_raw_cache: atomic write
# ---------------------------------------------------------------------------


class TestSaveToRawCache:
    def test_atomic_write_no_tmp_left(self, tmp_path):
        svc = _make_service(tmp_path)
        data = b"compressed_bytes"
        svc._save_to_raw_cache(
            data, "DETR", "2026-04-30", "DETR-posttrade-2026-04-30T09_00.json.gz"
        )

        cache = _cache_path(
            tmp_path, "DETR", "2026-04-30", "DETR-posttrade-2026-04-30T09_00.json.gz"
        )
        assert cache.exists(), "Final cache file must exist"
        assert cache.read_bytes() == data, "Bytes must match"

        tmp = cache.with_name(cache.name + ".tmp")
        assert not tmp.exists(), ".tmp file must be removed after successful write"

    def test_tmp_cleaned_up_on_write_error(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        cache = _cache_path(tmp_path, "DETR", "2026-04-30", fname)
        cache.parent.mkdir(parents=True, exist_ok=True)

        with patch("pathlib.Path.write_bytes", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                svc._save_to_raw_cache(b"data", "DETR", "2026-04-30", fname)

        tmp = cache.with_name(cache.name + ".tmp")
        assert not tmp.exists(), ".tmp must be removed on write failure"

    def test_returns_cache_path(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        result = svc._save_to_raw_cache(b"x", "DETR", "2026-04-30", fname)
        expected = _cache_path(tmp_path, "DETR", "2026-04-30", fname)
        assert result == expected


# ---------------------------------------------------------------------------
# fetch_and_parse_trades: save before parse, non-fatal on cache failure
# ---------------------------------------------------------------------------


class TestFetchAndParseSaveOrder:
    def test_cache_saved_before_parse(self, tmp_path):
        call_order = []
        fetcher = Mock()
        fetcher.download_file.return_value = b"compressed"
        fetcher.decompress_gzip.return_value = '{"trades":[]}'
        parser = Mock()
        parser.parse.return_value = pd.DataFrame()

        svc = _make_service(tmp_path, fetcher, parser)

        original_save = svc._save_to_raw_cache

        def tracking_save(*args, **kwargs):
            call_order.append("save")
            return original_save(*args, **kwargs)

        original_parse = parser.parse

        def tracking_parse(*args, **kwargs):
            call_order.append("parse")
            return original_parse(*args, **kwargs)

        svc._save_to_raw_cache = tracking_save
        parser.parse = tracking_parse

        svc.fetch_and_parse_trades(
            "DETR", "2026-04-30", "DETR-posttrade-2026-04-30T09_00.json.gz"
        )

        assert call_order == ["save", "parse"], "Cache write must precede parse"

    def test_cache_failure_is_nonfatal(self, tmp_path):
        fetcher = Mock()
        fetcher.download_file.return_value = b"data"
        fetcher.decompress_gzip.return_value = '{"trades":[]}'
        parser = Mock()
        parser.parse.return_value = pd.DataFrame()

        svc = _make_service(tmp_path, fetcher, parser)
        svc._save_to_raw_cache = Mock(side_effect=OSError("disk full"))

        # Should not raise despite cache failure
        df = svc.fetch_and_parse_trades(
            "DETR", "2026-04-30", "DETR-posttrade-2026-04-30T09_00.json.gz"
        )
        assert isinstance(df, pd.DataFrame)

    def test_unknown_schema_lands_in_raw_cache_not_quarantine(self, tmp_path):
        fetcher = Mock()
        fetcher.download_file.return_value = b"compressed"
        fetcher.decompress_gzip.return_value = '{"unknown_field": 1}'
        parser = Mock()
        parser.parse.side_effect = XetraSchemaUnknownError({"unknown_field"})

        svc = _make_service(tmp_path, fetcher, parser)

        with pytest.raises(XetraSchemaUnknownError):
            svc.fetch_and_parse_trades(
                "DETR", "2026-04-30", "DETR-posttrade-2026-04-30T09_00.json.gz"
            )

        cache = _cache_path(
            tmp_path, "DETR", "2026-04-30", "DETR-posttrade-2026-04-30T09_00.json.gz"
        )
        assert cache.exists(), (
            "Raw cache must contain the file even when schema is unknown"
        )

        quarantine = (
            tmp_path
            / "de"
            / "xetra"
            / "quarantine"
            / "DETR"
            / "DETR-posttrade-2026-04-30T09_00.json.gz"
        )
        assert not quarantine.exists(), "Quarantine path must no longer be written"


# ---------------------------------------------------------------------------
# _is_cached / resume detection
# ---------------------------------------------------------------------------


class TestIsCached:
    def test_returns_false_when_not_cached(self, tmp_path):
        svc = _make_service(tmp_path)
        assert not svc._is_cached(
            "DETR", "2026-04-30", "DETR-posttrade-2026-04-30T09_00.json.gz"
        )

    def test_returns_true_after_save(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        svc._save_to_raw_cache(b"data", "DETR", "2026-04-30", fname)
        assert svc._is_cached("DETR", "2026-04-30", fname)

    def test_non_detr_venue_works(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DFRA-posttrade-2026-04-30T09_00.json.gz"
        svc._save_to_raw_cache(b"data", "DFRA", "2026-04-30", fname)
        assert svc._is_cached("DFRA", "2026-04-30", fname)
        assert not svc._is_cached("DETR", "2026-04-30", fname)


# ---------------------------------------------------------------------------
# cleanup_raw_cache
# ---------------------------------------------------------------------------


class TestCleanupRawCache:
    def _aged_file(self, path: Path, age_seconds: int):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"data")
        old_mtime = time.time() - age_seconds
        os.utime(path, (old_mtime, old_mtime))

    def test_keeps_recent_files(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        cache = _cache_path(tmp_path, "DETR", "2026-04-30", fname)
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(b"fresh")

        result = svc.cleanup_raw_cache("DETR", max_age_days=7)
        assert result["kept_recent"] == 1
        assert result["deleted"] == 0
        assert cache.exists()

    def test_deletes_old_file_with_readable_daily_parquet(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        cache = _cache_path(tmp_path, "DETR", "2026-04-30", fname)
        self._aged_file(cache, 8 * 86400)

        parquet = _daily_parquet(tmp_path, "DETR", 2026, 4, 30)
        _write_minimal_parquet(parquet)

        result = svc.cleanup_raw_cache("DETR", max_age_days=7)
        assert result["deleted"] == 1
        assert not cache.exists()

    def test_deletes_old_file_with_readable_monthly_parquet(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        cache = _cache_path(tmp_path, "DETR", "2026-04-30", fname)
        self._aged_file(cache, 8 * 86400)

        monthly = _monthly_parquet(tmp_path, "DETR", 2026, 4)
        _write_minimal_parquet(monthly)

        result = svc.cleanup_raw_cache("DETR", max_age_days=7)
        assert result["deleted"] == 1
        assert not cache.exists()

    def test_keeps_old_file_without_parquet(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        cache = _cache_path(tmp_path, "DETR", "2026-04-30", fname)
        self._aged_file(cache, 8 * 86400)

        result = svc.cleanup_raw_cache("DETR", max_age_days=7)
        assert result["kept_no_parquet"] == 1
        assert result["deleted"] == 0
        assert cache.exists()

    def test_removes_orphaned_tmp_files_unconditionally(self, tmp_path):
        svc = _make_service(tmp_path)
        tmp_file = _cache_path(
            tmp_path,
            "DETR",
            "2026-04-30",
            "DETR-posttrade-2026-04-30T09_00.json.gz.tmp",
        )
        tmp_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_file.write_bytes(b"orphan")

        result = svc.cleanup_raw_cache("DETR")
        assert result["deleted"] == 1
        assert not tmp_file.exists()

    def test_dry_run_does_not_delete(self, tmp_path):
        svc = _make_service(tmp_path)
        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        cache = _cache_path(tmp_path, "DETR", "2026-04-30", fname)
        self._aged_file(cache, 8 * 86400)
        _write_minimal_parquet(_daily_parquet(tmp_path, "DETR", 2026, 4, 30))

        result = svc.cleanup_raw_cache("DETR", max_age_days=7, dry_run=True)
        assert result["deleted"] == 1
        assert cache.exists(), "dry_run must not actually delete files"

    def test_no_raw_dir_returns_zeros(self, tmp_path):
        svc = _make_service(tmp_path)
        result = svc.cleanup_raw_cache("DETR")
        assert result == {
            "deleted": 0,
            "kept_recent": 0,
            "kept_no_parquet": 0,
            "errors": 0,
        }


# ---------------------------------------------------------------------------
# reprocess_from_raw_cache
# ---------------------------------------------------------------------------


class TestReprocessFromRawCache:
    def _populate_cache(self, tmp_path, venue, date_str, files: dict[str, bytes]):
        svc = _make_service(tmp_path)
        for fname, data in files.items():
            svc._save_to_raw_cache(data, venue, date_str, fname)
        return svc

    def test_raises_when_no_cache_dir(self, tmp_path):
        svc = _make_service(tmp_path)
        with pytest.raises(FileNotFoundError):
            svc.reprocess_from_raw_cache("DETR", "2026-04-30")

    def test_raises_when_cache_dir_empty(self, tmp_path):
        svc = _make_service(tmp_path)
        cache_dir = _cache_path(tmp_path, "DETR", "2026-04-30", "").parent
        cache_dir.mkdir(parents=True, exist_ok=True)
        with pytest.raises(FileNotFoundError):
            svc.reprocess_from_raw_cache("DETR", "2026-04-30")

    def test_skips_when_parquet_readable_and_no_force(self, tmp_path):
        fetcher = Mock()
        fetcher.decompress_gzip.return_value = '{"trades":[]}'
        parser = Mock()
        parser.parse.return_value = pd.DataFrame()
        svc = _make_service(tmp_path, fetcher, parser)

        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        svc._save_to_raw_cache(b"data", "DETR", "2026-04-30", fname)
        _write_minimal_parquet(_daily_parquet(tmp_path, "DETR", 2026, 4, 30))

        result = svc.reprocess_from_raw_cache("DETR", "2026-04-30")
        assert result == {
            "processed": 0,
            "trades": 0,
            "skipped_unknown_schema": 0,
            "errors": 0,
        }
        parser.parse.assert_not_called()

    def test_reprocesses_with_force_even_if_parquet_readable(self, tmp_path):
        fetcher = Mock()
        fetcher.decompress_gzip.return_value = '{"trades":[]}'
        parser = Mock()
        parser.parse.return_value = pd.DataFrame()
        svc = _make_service(tmp_path, fetcher, parser)

        fname = "DETR-posttrade-2026-04-30T09_00.json.gz"
        svc._save_to_raw_cache(b"data", "DETR", "2026-04-30", fname)
        _write_minimal_parquet(_daily_parquet(tmp_path, "DETR", 2026, 4, 30))

        result = svc.reprocess_from_raw_cache("DETR", "2026-04-30", force=True)
        assert result["processed"] == 1
        parser.parse.assert_called_once()

    def test_reprocesses_all_files_and_stores_trades(self, tmp_path):
        fetcher = Mock()
        fetcher.decompress_gzip.side_effect = ['{"trades":[]}', '{"trades":[]}']
        parser = Mock()
        parser.parse.side_effect = [
            pd.DataFrame(
                {
                    "isin": ["DE001"],
                    "trading_date_time": [pd.Timestamp("2026-04-30 09:00:00")],
                    "price": [100.0],
                }
            ),
            pd.DataFrame(columns=["isin", "trading_date_time", "price"]),
        ]
        svc = _make_service(tmp_path, fetcher, parser)

        for fname in [
            "DETR-posttrade-2026-04-30T09_00.json.gz",
            "DETR-posttrade-2026-04-30T09_01.json.gz",
        ]:
            svc._save_to_raw_cache(b"data", "DETR", "2026-04-30", fname)

        result = svc.reprocess_from_raw_cache("DETR", "2026-04-30")
        assert result["processed"] == 2
        assert result["trades"] == 1
        assert result["errors"] == 0

    def test_unknown_schema_does_not_abort(self, tmp_path):
        fetcher = Mock()
        fetcher.decompress_gzip.side_effect = ['{"bad":1}', '{"trades":[]}']
        parser = Mock()
        parser.parse.side_effect = [
            XetraSchemaUnknownError({"bad"}),
            pd.DataFrame(columns=["isin", "trading_date_time", "price"]),
        ]
        svc = _make_service(tmp_path, fetcher, parser)

        for fname in [
            "DETR-posttrade-2026-04-30T09_00.json.gz",
            "DETR-posttrade-2026-04-30T09_01.json.gz",
        ]:
            svc._save_to_raw_cache(b"data", "DETR", "2026-04-30", fname)

        result = svc.reprocess_from_raw_cache("DETR", "2026-04-30")
        assert result["skipped_unknown_schema"] == 1
        assert result["processed"] == 1
        assert result["errors"] == 0
