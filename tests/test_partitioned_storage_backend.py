import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from loguru import logger

from yf_parqed.common.partition_path_builder import PartitionPathBuilder
from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.common.storage_backend import StorageRequest


@pytest.fixture()
def empty_frame():
    def factory():
        return pd.DataFrame(
            {
                "stock": pd.Series(dtype="string"),
                "date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="Int64"),
                "sequence": pd.Series(dtype="Int64"),
            }
        ).set_index(["stock", "date"])

    return factory


@pytest.fixture()
def normalizer():
    def normalize(df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = [
            "stock",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "sequence",
        ]
        normalized = df.copy()
        for column in expected_cols:
            if column not in normalized.columns:
                if column in {"open", "high", "low", "close"}:
                    normalized[column] = pd.Series(dtype="float64")
                elif column in {"volume", "sequence"}:
                    normalized[column] = pd.Series(dtype="Int64")
                elif column == "date":
                    normalized[column] = pd.Series(dtype="datetime64[ns]")
                else:
                    normalized[column] = pd.Series(dtype="string")
        normalized["stock"] = normalized["stock"].astype("string")
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
        for price_col in ["open", "high", "low", "close"]:
            normalized[price_col] = pd.to_numeric(
                normalized[price_col], errors="coerce"
            ).astype("float64")
        for int_col in ["volume", "sequence"]:
            normalized[int_col] = (
                pd.to_numeric(normalized[int_col], errors="coerce")
                .round()
                .astype("Int64")
            )
        return normalized[expected_cols]

    return normalize


@pytest.fixture()
def columns():
    return [
        "stock",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sequence",
    ]


@pytest.fixture()
def backend(tmp_path: Path, empty_frame, normalizer, columns):
    builder = PartitionPathBuilder(root=tmp_path)
    return PartitionedStorageBackend(
        empty_frame_factory=empty_frame,
        normalizer=normalizer,
        column_provider=lambda: columns,
        path_builder=builder,
    )


def make_request(
    root: Path, interval: str = "1d", ticker: str = "AAPL"
) -> StorageRequest:
    return StorageRequest(
        root=root,
        market="us",
        source="yahoo",
        dataset="stocks",
        interval=interval,
        ticker=ticker,
    )


def make_sample_df(dates: list[str], ticker: str = "AAPL") -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(
        [(ticker, pd.Timestamp(date)) for date in dates], names=["stock", "date"]
    )
    return pd.DataFrame(
        {
            "open": [100.0 + i for i in range(len(dates))],
            "high": [101.0 + i for i in range(len(dates))],
            "low": [99.0 + i for i in range(len(dates))],
            "close": [100.5 + i for i in range(len(dates))],
            "volume": [1_000 + i for i in range(len(dates))],
            "sequence": [i for i in range(len(dates))],
        },
        index=index,
    )


def _compression_codec_name(path: Path) -> str:
    parquet_file = pq.ParquetFile(path)
    codec = parquet_file.metadata.row_group(0).column(0).compression
    if hasattr(codec, "name"):
        return codec.name.lower()
    text = str(codec)
    if "." in text:
        text = text.split(".")[-1]
    return text.lower()


def test_save_requires_market_and_source(backend, tmp_path, empty_frame):
    request = StorageRequest(
        root=tmp_path,
        market=None,
        source=None,
        dataset="stocks",
        interval="1d",
        ticker="AAPL",
    )
    df = make_sample_df(["2024-01-05"])
    with pytest.raises(ValueError):
        backend.save(request, df, empty_frame())


def test_save_writes_partition_files(backend, tmp_path, empty_frame):
    df = make_sample_df(["2024-05-01", "2024-06-01"])
    request = make_request(tmp_path)

    backend.save(request, df, empty_frame())

    base = tmp_path / "us/yahoo/stocks_1d/ticker=AAPL"
    first = base / "year=2024/month=05/data.parquet"
    second = base / "year=2024/month=06/data.parquet"
    assert first.exists()
    assert second.exists()

    reloaded = backend.read(request)
    assert not reloaded.empty
    assert len(reloaded) == 2
    assert reloaded.index.get_level_values("date").min() == pd.Timestamp("2024-05-01")


def test_save_honors_compression_setting(
    tmp_path: Path,
    empty_frame,
    normalizer,
    columns,
) -> None:
    builder = PartitionPathBuilder(root=tmp_path)
    default_backend = PartitionedStorageBackend(
        empty_frame_factory=empty_frame,
        normalizer=normalizer,
        column_provider=lambda: columns,
        path_builder=builder,
    )
    df = make_sample_df(["2024-02-10"], ticker="AAPL")
    default_request = make_request(tmp_path, ticker="AAPL")
    default_backend.save(default_request, df, empty_frame())

    default_path = (
        tmp_path / "us/yahoo/stocks_1d/ticker=AAPL/year=2024/month=02/data.parquet"
    )
    assert _compression_codec_name(default_path) == "gzip"

    no_comp_backend = PartitionedStorageBackend(
        empty_frame_factory=empty_frame,
        normalizer=normalizer,
        column_provider=lambda: columns,
        path_builder=builder,
        compression=None,
    )
    request_no = make_request(tmp_path, ticker="MSFT")
    df_no_comp = make_sample_df(["2024-02-10"], ticker="MSFT")
    no_comp_backend.save(request_no, df_no_comp, empty_frame())

    no_comp_path = (
        tmp_path / "us/yahoo/stocks_1d/ticker=MSFT/year=2024/month=02/data.parquet"
    )
    assert _compression_codec_name(no_comp_path) == "uncompressed"


def test_read_returns_empty_when_no_partitions(backend, tmp_path):
    request = make_request(tmp_path)
    result = backend.read(request)
    assert result.empty


def test_read_removes_corrupt_partition_and_fails(backend, tmp_path, empty_frame):
    request = make_request(tmp_path)
    df = make_sample_df(["2024-01-05"])
    backend.save(request, df, empty_frame())

    corrupt_path = (
        tmp_path / "us/yahoo/stocks_1d/ticker=AAPL/year=2024/month=01/data.parquet"
    )
    corrupt_path.write_text("not parquet")

    with pytest.raises(RuntimeError, match="Failed to read.*partition"):
        backend.read(request)

    # Corrupt file should be DELETED (truly unreadable)
    assert not corrupt_path.exists()


def test_read_preserves_schema_mismatch_partition(backend, tmp_path, empty_frame):
    """Test that partitions with schema issues are preserved, not deleted."""
    request = make_request(tmp_path)
    df = make_sample_df(["2024-01-05"])
    backend.save(request, df, empty_frame())

    # Create a partition file with missing columns (schema mismatch)
    bad_schema_path = (
        tmp_path / "us/yahoo/stocks_1d/ticker=AAPL/year=2024/month=01/data.parquet"
    )
    bad_df = pd.DataFrame(
        {
            "stock": ["AAPL"],
            "date": [pd.Timestamp("2024-01-05")],
            "open": [100.0],
            # Missing required columns: high, low, close, volume, sequence
        }
    )
    bad_df.to_parquet(bad_schema_path, index=False)

    with pytest.raises(RuntimeError, match="Failed to read.*partition"):
        backend.read(request)

    # File with schema mismatch should be PRESERVED for inspection
    assert bad_schema_path.exists()


# --- merge: the daemon's write path (ADR 2026-10-03, Steps B and C) ----------


def _month_file(root: Path, year: int, month: int, ticker: str = "AAPL") -> Path:
    return (
        root
        / f"us/yahoo/stocks_1d/ticker={ticker}/year={year}/month={month:02d}/data.parquet"
    )


@pytest.fixture()
def seeded(backend, tmp_path):
    """One row each in January, February and March 2024, files aged by an hour.

    The old modification time makes "was this file rewritten?" independent of
    the filesystem's timestamp granularity.
    """
    request = make_request(tmp_path)
    backend.merge(request, make_sample_df(["2024-01-10", "2024-02-10", "2024-03-04"]))
    files = {m: _month_file(tmp_path, 2024, m) for m in (1, 2, 3)}
    for path in files.values():
        aged = path.stat().st_mtime - 3600
        os.utime(path, (aged, aged))
    snapshot = {m: (p.stat().st_mtime_ns, p.read_bytes()) for m, p in files.items()}
    return request, files, snapshot


def _unchanged(files, snapshot, month: int) -> bool:
    path = files[month]
    return (path.stat().st_mtime_ns, path.read_bytes()) == snapshot[month]


def test_merge_writes_one_file_per_month_of_new_data(backend, tmp_path):
    request = make_request(tmp_path)

    written = backend.merge(request, make_sample_df(["2024-05-01", "2024-06-01"]))

    assert written == [pd.Period("2024-05"), pd.Period("2024-06")]
    assert _month_file(tmp_path, 2024, 5).exists()
    assert _month_file(tmp_path, 2024, 6).exists()
    assert len(backend.read(request)) == 2


def test_merge_leaves_untouched_months_alone(backend, seeded):
    request, files, snapshot = seeded

    written = backend.merge(request, make_sample_df(["2024-03-05"]))

    assert written == [pd.Period("2024-03")]
    assert _unchanged(files, snapshot, 1)
    assert _unchanged(files, snapshot, 2)
    assert not _unchanged(files, snapshot, 3)


def test_merge_keeps_the_stored_rows_of_a_touched_month(backend, seeded):
    """The stored rows are read from disk, so the caller cannot leave any out."""
    request, files, _ = seeded

    backend.merge(request, make_sample_df(["2024-03-05"]))

    march = pd.read_parquet(files[3])
    assert sorted(march["date"]) == [
        pd.Timestamp("2024-03-04"),
        pd.Timestamp("2024-03-05"),
    ]
    assert len(backend.read(request)) == 4


def test_merge_replaces_a_stored_row_with_the_same_date(backend, seeded):
    request, files, _ = seeded
    new = make_sample_df(["2024-03-04", "2024-03-05"])
    new["close"] = [999.0, 998.0]
    new["sequence"] = [5, 6]

    backend.merge(request, new)

    march = pd.read_parquet(files[3]).set_index("date")
    assert len(march) == 2
    assert march.loc[pd.Timestamp("2024-03-04"), "close"] == 999.0


def test_merge_touches_both_months_at_a_month_boundary(backend, seeded):
    request, files, snapshot = seeded

    written = backend.merge(request, make_sample_df(["2024-02-29", "2024-03-01"]))

    assert written == [pd.Period("2024-02"), pd.Period("2024-03")]
    assert _unchanged(files, snapshot, 1)
    assert not _unchanged(files, snapshot, 2)
    assert not _unchanged(files, snapshot, 3)
    assert len(backend.read(request)) == 5


def test_merge_does_not_rewrite_a_month_that_would_not_change(backend, seeded):
    """Refetching bars that are already stored, as every weekend cycle does."""
    request, files, snapshot = seeded

    written = backend.merge(request, make_sample_df(["2024-03-04"]))

    assert written == []
    assert all(_unchanged(files, snapshot, m) for m in (1, 2, 3))


def test_merge_rewrites_only_the_month_that_changed(backend, seeded):
    request, files, snapshot = seeded
    # February comes back exactly as stored, March with a revised close.
    new = backend.read(request).drop(("AAPL", pd.Timestamp("2024-01-10")))
    new.loc[("AAPL", pd.Timestamp("2024-03-04")), "close"] = 555.0

    written = backend.merge(request, new)

    assert written == [pd.Period("2024-03")]
    assert _unchanged(files, snapshot, 2)


def test_merge_ignores_an_incoming_row_with_fewer_values(backend, seeded):
    """A bar that comes back without prices does not replace the captured one."""
    request, files, snapshot = seeded
    new = make_sample_df(["2024-03-04"])
    new[["open", "high", "low", "close"]] = float("nan")
    new["sequence"] = 99

    written = backend.merge(request, new)

    assert written == []
    assert _unchanged(files, snapshot, 3)


def test_merge_stores_the_good_rows_next_to_an_ignored_one(backend, seeded):
    request, files, _ = seeded
    stored_close = pd.read_parquet(files[3])["close"].iloc[0]
    new = make_sample_df(["2024-03-04", "2024-03-05"])
    new.loc[("AAPL", pd.Timestamp("2024-03-04")), "close"] = float("nan")

    written = backend.merge(request, new)

    assert written == [pd.Period("2024-03")]
    march = pd.read_parquet(files[3]).set_index("date")
    assert len(march) == 2
    assert march.loc[pd.Timestamp("2024-03-04"), "close"] == stored_close


def test_merge_lets_a_fuller_row_replace_an_emptier_stored_one(backend, tmp_path):
    request = make_request(tmp_path)
    partial = make_sample_df(["2024-03-04"])
    partial["close"] = float("nan")
    backend.merge(request, partial)

    written = backend.merge(request, make_sample_df(["2024-03-04"]))

    assert written == [pd.Period("2024-03")]
    assert pd.read_parquet(_month_file(tmp_path, 2024, 3))["close"].notna().all()


def test_merge_refuses_to_write_a_month_that_lost_a_stored_row(
    backend, seeded, monkeypatch
):
    """Cannot happen by construction; the guard is there for the day it does."""
    request, files, snapshot = seeded
    dedupe = backend._dedupe
    monkeypatch.setattr(
        backend,
        "_dedupe",
        lambda frame: dedupe(frame[frame["date"] != pd.Timestamp("2024-03-04")]),
    )
    errors = []
    sink = logger.add(lambda message: errors.append(str(message)), level="ERROR")
    try:
        written = backend.merge(request, make_sample_df(["2024-03-05"]))
    finally:
        logger.remove(sink)

    assert written == []
    assert _unchanged(files, snapshot, 3)
    assert any("would drop 1 of 1 stored rows" in line for line in errors)


def test_merge_drops_rows_without_a_date(backend, seeded):
    request, files, snapshot = seeded
    new = make_sample_df(["2024-03-05", "2024-03-06"])
    new.index = pd.MultiIndex.from_tuples(
        [("AAPL", pd.Timestamp("2024-03-05")), ("AAPL", pd.NaT)],
        names=["stock", "date"],
    )

    written = backend.merge(request, new)

    assert written == [pd.Period("2024-03")]
    assert len(pd.read_parquet(files[3])) == 2
    assert backend.merge(request, new.iloc[[1]]) == []


def test_merge_ignores_a_damaged_file_in_an_untouched_month(backend, seeded):
    request, files, _ = seeded
    files[1].write_bytes(b"not a parquet file")

    written = backend.merge(request, make_sample_df(["2024-03-05"]))

    assert written == [pd.Period("2024-03")]
    assert files[1].read_bytes() == b"not a parquet file"


def test_merge_fails_on_a_damaged_file_in_a_touched_month(backend, seeded, tmp_path):
    """A schema problem keeps the file; nothing is written over it."""
    request, files, _ = seeded
    pd.DataFrame({"unexpected": [1]}).to_parquet(files[3])
    damaged = files[3].read_bytes()

    with pytest.raises(RuntimeError, match="2024-03"):
        backend.merge(request, make_sample_df(["2024-03-05"]))

    assert files[3].read_bytes() == damaged


def test_merge_rejects_another_ticker(backend, tmp_path):
    request = make_request(tmp_path, ticker="AAPL")

    with pytest.raises(ValueError):
        backend.merge(request, make_sample_df(["2024-03-05"], ticker="MSFT"))


def test_merge_of_empty_frame_writes_nothing(backend, tmp_path, empty_frame):
    request = make_request(tmp_path)

    assert backend.merge(request, empty_frame()) == []
    assert not (tmp_path / "us").exists()
