from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd
from loguru import logger

from .partition_path_builder import PartitionPathBuilder
from .storage_backend import StorageInterface, StorageRequest


class PartitionedStorageBackend(StorageInterface):
    """Partition-aware parquet storage backend."""

    def __init__(
        self,
        *,
        empty_frame_factory: Callable[[], pd.DataFrame],
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
        column_provider: Callable[[], list[str]],
        path_builder: PartitionPathBuilder,
        compression: str | None = "gzip",
    ) -> None:
        self._empty_frame_factory = empty_frame_factory
        self._normalizer = normalizer
        self._column_provider = column_provider
        self._path_builder = path_builder
        self._compression = compression

    def save(
        self,
        request: StorageRequest,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
    ) -> pd.DataFrame:
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
        for path in partition_files:
            try:
                df = pd.read_parquet(path)
            except (ValueError, FileNotFoundError, OSError) as exc:
                self._safe_remove(path)
                raise RuntimeError(f"Failed to read partition file: {path}") from exc

            if df.empty or not required.issubset(df.columns):
                self._safe_remove(path)
                raise RuntimeError(f"Partition file missing required columns: {path}")

            frames.append(df)

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
        normalized = self._normalizer(frame)
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
            print(partition_df.columns)
            path = self._path_builder.build(
                market=request.market,
                source=request.source,
                dataset=request.dataset,
                interval=request.interval,
                ticker=request.ticker,
                timestamp=pd.Timestamp(month_ts).to_pydatetime(),
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            partition_df.to_parquet(path, index=False, compression=self._compression)

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

    def _safe_remove(self, path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except TypeError:
            if path.exists():
                path.unlink()
