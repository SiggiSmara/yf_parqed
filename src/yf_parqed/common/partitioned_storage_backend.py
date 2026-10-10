from __future__ import annotations

from pathlib import Path
from typing import Callable
import os
import uuid
import time

import pandas as pd
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq

from .partition_path_builder import PartitionPathBuilder
from .parquet_recovery import DamageRecorder, ParquetRecoveryError, safe_read_parquet
from .storage import StorageInterface, StorageRequest


class PartitionedStorageBackend(StorageInterface):
    """Partition-aware parquet storage backend."""

    def save_xetra_trades(
        self,
        trades_df: pd.DataFrame,
        venue: str,
        trade_date,
        market: str = "xetra",
        source: str = "delayed",
    ):
        """
        Save raw Xetra trades in venue-first partitioned Parquet files.

        If file already exists, merges new trades with existing data.
        This supports incremental storage where files are added progressively.

        Path: {root}/{market}/{source}/trades/venue=VENUE/year=YYYY/month=MM/day=DD/trades.parquet
        Atomic write: temp file, fsync, replace.
        """
        import shutil
        from datetime import date as dt_date

        if isinstance(trade_date, str):
            trade_date = pd.to_datetime(trade_date).date()
        elif isinstance(trade_date, pd.Timestamp):
            trade_date = trade_date.date()
        elif not isinstance(trade_date, dt_date):
            raise ValueError("trade_date must be str, pd.Timestamp, or date")
        year = trade_date.year
        month = f"{trade_date.month:02d}"
        day = f"{trade_date.day:02d}"
        base_dir = (
            self._path_builder._root
            / market
            / source
            / "trades"
            / f"venue={venue}"
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
        )
        base_dir.mkdir(parents=True, exist_ok=True)
        out_path = base_dir / "trades.parquet"

        # Merge with existing data if file exists
        if out_path.exists():
            try:
                existing_df = pd.read_parquet(out_path)
                trades_df = pd.concat([existing_df, trades_df], ignore_index=True)
                logger.debug(f"Merged {len(trades_df)} trades with existing data")
            except Exception as e:
                logger.warning(
                    f"Failed to read existing file for merge: {e}, overwriting"
                )

        temp_path = out_path.with_suffix(".tmp")
        try:
            table = pa.Table.from_pandas(trades_df)
            pq.write_table(table, str(temp_path))
            with open(temp_path, "rb") as fd:
                os.fsync(fd.fileno())
            shutil.move(str(temp_path), str(out_path))
            logger.info(f"Saved Xetra trades: {out_path} ({len(trades_df)} rows)")
        except Exception as e:
            logger.warning(f"Failed to save trades for {venue} {trade_date}: {e}")
            if temp_path.exists():
                temp_path.unlink()
            raise

    def __init__(
        self,
        *,
        empty_frame_factory: Callable[[], pd.DataFrame],
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
        column_provider: Callable[[], list[str]],
        path_builder: PartitionPathBuilder,
        compression: str | None = "gzip",
        fsync: bool = True,
        row_group_size: int | None = None,
        damage_recorder: DamageRecorder | None = None,
    ) -> None:
        self._damage_recorder = damage_recorder
        self._empty_frame_factory = empty_frame_factory
        self._normalizer = normalizer
        self._column_provider = column_provider
        self._path_builder = path_builder
        self._compression = compression
        self._fsync = bool(fsync)
        # pyarrow expects a string compression name; map None -> 'NONE'
        self._pyarrow_compression = (
            self._compression if self._compression is not None else "NONE"
        )
        self._row_group_size = (
            int(row_group_size) if row_group_size is not None else None
        )

    def save(
        self,
        request: StorageRequest,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """Write every month present in ``new_data`` or ``existing_data``.

        The caller supplies the stored rows, and a month is replaced by
        exactly what the two frames hold for it. Used by the legacy
        migration, which may overwrite on purpose. The daemon uses
        :meth:`merge`, which reads the stored rows itself.
        """
        self._validate_partition_metadata(request)

        if new_data.empty and existing_data.empty:
            return self._empty_frame_factory()

        if new_data.empty:
            logger.debug("New data empty.. nothing to do")
            return existing_data

        combined = self._merge_frames(new_data, existing_data)

        self._assert_single_ticker(combined, request)
        self._write_partitions(request, combined)

        return combined.set_index(["stock", "date"])

    def merge(
        self,
        request: StorageRequest,
        new_data: pd.DataFrame,
        refused: list[pd.Period] | None = None,
    ) -> list[pd.Period]:
        """Merge ``new_data`` into the stored partitions, one month at a time.

        This is the daemon's write path. Each month that has a row in
        ``new_data`` is read from disk, merged with those rows and written
        back only if the result differs from what is stored. Months without
        a row in ``new_data`` are not opened. Returns the months written.

        The stored rows come from disk, never from the caller, so a month
        cannot lose rows to an incomplete read. For a date present on both
        sides the row with the higher ``sequence`` wins; without sequence
        numbers, the new row. Rows without a date are dropped. An unreadable
        file in a touched month raises ``RuntimeError`` before that month is
        written; it has then been moved aside, so the next call starts a new
        file for that month.

        Captured data is not replaced by less: an incoming row that holds
        fewer values than the stored row for the same date is ignored, and a
        month is not written if any stored date would be missing from it. Such
        a month is logged and, when the caller passes ``refused``, added to it:
        the bars fetched for it are then not on disk.
        """
        self._validate_partition_metadata(request)

        if new_data.empty:
            return []

        new = self._normalizer(new_data.reset_index())
        new = new[new["date"].notna()]
        if new.empty:
            return []
        self._assert_single_ticker(new, request)

        new_months = new["date"].dt.to_period("M")
        written: list[pd.Period] = []
        for month in sorted(new_months.unique()):
            rows = new[new_months == month]
            stored = self._read_month(request, month)
            if stored is None:
                merged = self._dedupe(rows)
            else:
                rows = self._without_emptier_rows(request, month, stored, rows)
                merged = self._dedupe(pd.concat([stored, rows], ignore_index=True))
                if merged.reset_index(drop=True).equals(stored.reset_index(drop=True)):
                    continue
                lost = int((~stored["date"].isin(merged["date"])).sum())
                if lost:
                    logger.error(
                        f"Not writing {request.ticker} {month} ({request.interval}): "
                        f"the merge would drop {lost} of {len(stored)} stored rows. "
                        "The stored file is kept as it is."
                    )
                    if refused is not None:
                        refused.append(month)
                    continue
            self._write_partitions(request, merged)
            written.append(month)
        return written

    def _without_emptier_rows(
        self,
        request: StorageRequest,
        month: pd.Period,
        stored: pd.DataFrame,
        rows: pd.DataFrame,
    ) -> pd.DataFrame:
        """Drop incoming rows that hold fewer values than the stored row for their date."""
        overlap = rows["date"].isin(stored["date"])
        if not overlap.any():
            return rows
        value_columns = [
            column
            for column in rows.columns
            if column not in ("stock", "date", "sequence")
        ]
        stored_filled = (
            stored.drop_duplicates(subset="date", keep="last")
            .set_index("date")[value_columns]
            .notna()
            .sum(axis=1)
        )
        incoming_filled = rows[value_columns].notna().sum(axis=1)
        emptier = incoming_filled < rows["date"].map(stored_filled).fillna(0)
        if not emptier.any():
            return rows
        logger.warning(
            f"Ignoring {int(emptier.sum())} incoming rows for {request.ticker} {month} "
            f"({request.interval}) that hold fewer values than the stored rows "
            "for the same dates"
        )
        return rows[~emptier]

    def _read_month(
        self, request: StorageRequest, month: pd.Period
    ) -> pd.DataFrame | None:
        """Normalized rows of one monthly partition, or None if it has no file."""
        path = self._path_builder.build(
            market=request.market,
            source=request.source,
            dataset=request.dataset,
            interval=request.interval,
            ticker=request.ticker,
            timestamp=month.start_time.to_pydatetime(),
        )
        if not path.is_file():
            return None
        try:
            return safe_read_parquet(
                path=path,
                required_columns=set(self._column_provider()),
                normalizer=self._normalizer,
                empty_frame_factory=self._empty_frame_factory,
                on_damaged=self._damage_recorder,
            )
        except ParquetRecoveryError as exc:
            logger.error(f"Failed to read partition {path}: {exc}")
            raise RuntimeError(
                f"Failed to read partition {month} for {request.ticker}: {exc}"
            ) from exc

    def read(self, request: StorageRequest) -> pd.DataFrame:
        self._validate_partition_metadata(request)

        try:
            ticker_root = self._path_builder.ticker_root(
                market=request.market,
                source=request.source,
                dataset=request.dataset,
                interval=request.interval,
                ticker=request.ticker,
            )
        except ValueError as exc:  # Defensive, should not happen after validation
            raise ValueError("Invalid storage request for partitioned backend") from exc

        if not ticker_root.exists():
            return self._empty_frame_factory()

        partition_files = sorted(ticker_root.rglob("data.parquet"))
        if not partition_files:
            return self._empty_frame_factory()

        frames: list[pd.DataFrame] = []
        required = set(self._column_provider())
        failed_files: list[tuple[Path, str]] = []

        for path in partition_files:
            try:
                df = safe_read_parquet(
                    path=path,
                    required_columns=required,
                    normalizer=self._normalizer,
                    empty_frame_factory=self._empty_frame_factory,
                    on_damaged=self._damage_recorder,
                )
                frames.append(df)
            except ParquetRecoveryError as exc:
                # Recovery failed - file either moved aside (if unreadable) or preserved (if schema issue)
                # Log the error and track the failure
                logger.error(f"Failed to read partition {path}: {exc}")
                failed_files.append((path, str(exc)))

        # If we failed to read any partition files, raise an error with details
        if failed_files:
            error_summary = "\n".join(
                f"  - {path.relative_to(ticker_root)}: {reason}"
                for path, reason in failed_files
            )
            raise RuntimeError(
                f"Failed to read {len(failed_files)} partition file(s) for {request.ticker}:\n{error_summary}"
            )

        if not frames:
            return self._empty_frame_factory()

        combined = pd.concat(frames, axis=0, ignore_index=True)
        combined = self._normalize_and_dedupe(combined)

        return combined.set_index(["stock", "date"])

    def _merge_frames(
        self, new_data: pd.DataFrame, existing_data: pd.DataFrame
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        if not existing_data.empty:
            frames.append(existing_data.reset_index())
        frames.append(new_data.reset_index())

        combined = pd.concat(frames, axis=0, ignore_index=True)
        return self._normalize_and_dedupe(combined)

    def _normalize_and_dedupe(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._dedupe(self._normalizer(frame))

    @staticmethod
    def _dedupe(normalized: pd.DataFrame) -> pd.DataFrame:
        normalized = normalized.sort_values(
            ["stock", "date", "sequence"], kind="mergesort"
        )
        normalized = normalized.drop_duplicates(subset=["stock", "date"], keep="last")
        normalized = normalized.sort_values(["stock", "date"], kind="mergesort")
        return normalized

    def _write_partitions(self, request: StorageRequest, frame: pd.DataFrame) -> None:
        """
        Write one parquet file per ticker/month instead of per full date.
        Groups rows by the YYYY-MM period and calls the path builder with
        the period's start timestamp so callers that emit year/month folders
        will get a single file per month.
        """
        # without copy the month_start column gets populated back up into the original
        frame = frame.copy()
        unique_dates = frame["date"].dropna().sort_values().unique()
        if unique_dates.size == 0:
            return
        # compute month-start timestamp for grouping
        frame["month_start"] = frame["date"].dt.to_period("M").dt.to_timestamp()

        for month_ts in frame["month_start"].dropna().unique():
            partition_df = frame[frame["month_start"] == month_ts].copy()
            # remove internal grouping column before persisting
            partition_df = partition_df.drop(columns=["month_start"], errors="ignore")
            path = self._path_builder.build(
                market=request.market,
                source=request.source,
                dataset=request.dataset,
                interval=request.interval,
                ticker=request.ticker,
                timestamp=pd.Timestamp(month_ts).to_pydatetime(),
            )
            path.parent.mkdir(parents=True, exist_ok=True)

            # atomic write: write to same-dir temp file, fsync, then os.replace
            suffix = uuid.uuid4().hex
            temp_name = (
                f"data.parquet.tmp-{os.getpid()}-{int(time.time() * 1000)}-{suffix}"
            )
            temp_path = path.with_name(temp_name)
            try:
                # write temp parquet for this month partition and time the write
                write_start = time.perf_counter()
                # If a row_group_size is provided, use pyarrow.write_table for finer control
                if self._row_group_size is not None:
                    try:
                        table = pa.Table.from_pandas(partition_df, preserve_index=False)
                        pq.write_table(
                            table,
                            str(temp_path),
                            compression=self._pyarrow_compression,
                            row_group_size=self._row_group_size,
                            use_dictionary=False,
                        )
                    except Exception:
                        # Fallback to pandas method if pyarrow write fails for any reason
                        partition_df.to_parquet(
                            temp_path, index=False, compression=self._compression
                        )
                else:
                    partition_df.to_parquet(
                        temp_path, index=False, compression=self._compression
                    )
                _ = time.perf_counter() - write_start
                # per-month temp-file writes are cheap and verbose; omit detailed logs
                # (higher-level per-ticker timings are recorded by the migration service)
                # ensure data is flushed to disk
                # optional fsync: expensive but ensures data is persisted before rename
                if self._fsync:
                    try:
                        with open(temp_path, "rb") as fd:
                            os.fsync(fd.fileno())
                    except OSError:
                        # best-effort: if fsync fails, proceed to replace anyway
                        logger.debug("fsync failed for {path}", path=str(temp_path))

                # a bad write must not replace a good file
                try:
                    self._verify_temp_file(temp_path, len(partition_df))
                except Exception:
                    temp_path.unlink(missing_ok=True)
                    raise

                # atomic replace
                try:
                    # use pathlib.Path.replace for a cleaner, idiomatic atomic rename
                    temp_path.replace(path)
                except Exception:
                    # If replace fails, attempt to remove temp file and re-raise
                    try:
                        temp_path.unlink(missing_ok=True)
                    except Exception:
                        logger.debug(
                            "Failed to remove temp file {path}", path=str(temp_path)
                        )
                    raise
            except Exception:
                logger.exception(
                    "Failed to write partition file for {ticker} month {month}",
                    ticker=request.ticker,
                    month=str(month_ts),
                )
                raise

        # for timestamp in unique_dates:
        #     partition_df = frame[frame["date"] == timestamp].copy()
        #     path = self._path_builder.build(
        #         market=request.market,
        #         source=request.source,
        #         dataset=request.dataset,
        #         interval=request.interval,
        #         ticker=request.ticker,
        #         timestamp=pd.Timestamp(timestamp).to_pydatetime(),
        #     )
        #     path.parent.mkdir(parents=True, exist_ok=True)
        #     partition_df.to_parquet(path, index=False, compression=self._compression)

    @staticmethod
    def _verify_temp_file(temp_path: Path, expected_rows: int) -> None:
        """Read the footer of a freshly written temp file back and compare its row count."""
        try:
            rows = pq.read_metadata(temp_path).num_rows
        except Exception as exc:
            raise RuntimeError(
                f"{temp_path.name} cannot be read back after writing: {exc}"
            ) from exc
        if rows != expected_rows:
            raise RuntimeError(
                f"{temp_path.name} holds {rows} rows after writing, "
                f"{expected_rows} were meant to be written"
            )

    def _validate_partition_metadata(self, request: StorageRequest) -> None:
        if not request.market or not request.source:
            raise ValueError("Partitioned storage requires market and source metadata")
        if not request.dataset:
            raise ValueError("Partitioned storage requires dataset metadata")
        if not request.interval:
            raise ValueError("Partitioned storage requires interval metadata")
        if not request.ticker:
            raise ValueError("Partitioned storage requires ticker metadata")

    def _assert_single_ticker(
        self, frame: pd.DataFrame, request: StorageRequest
    ) -> None:
        tickers = {str(value) for value in frame["stock"].dropna().unique()}
        if not tickers:
            raise ValueError("No ticker data present for partitioned save")
        if tickers != {request.ticker}:
            raise ValueError("Partitioned storage only supports single-ticker writes")
