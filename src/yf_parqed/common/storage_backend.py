from __future__ import annotations

from typing import Callable

import pandas as pd
from loguru import logger

from .parquet_recovery import ParquetRecoveryError, safe_read_parquet
from .storage import StorageInterface, StorageRequest


class StorageBackend(StorageInterface):
    """Handle parquet I/O with typed operations and error recovery."""

    def __init__(
        self,
        empty_frame_factory: Callable[[], pd.DataFrame],
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
        column_provider: Callable[[], list[str]],
    ) -> None:
        """
        Initialize storage backend with injected dependencies.

        Args:
            empty_frame_factory: Creates an empty DataFrame with correct schema
            normalizer: Normalizes DataFrame columns and types
            column_provider: Returns list of required column names
        """
        self._empty_frame_factory = empty_frame_factory
        self._normalizer = normalizer
        self._column_provider = column_provider

    def read(self, request: StorageRequest) -> pd.DataFrame:
        """
        Read parquet file with error recovery and schema validation.

        Args:
            request: Storage metadata describing the target

        Returns:
            DataFrame with (stock, date) multi-index, or empty DataFrame if file missing/corrupt
        """
        empty_df = self._empty_frame_factory()
        data_path = request.legacy_path()

        if not data_path.is_file():
            return empty_df

        required = set(self._column_provider())

        try:
            df = safe_read_parquet(
                path=data_path,
                required_columns=required,
                normalizer=self._normalizer,
                empty_frame_factory=self._empty_frame_factory,
            )
            df.set_index(["stock", "date"], inplace=True)
            return df
        except ParquetRecoveryError as exc:
            # Recovery failed - log details and return empty
            # File is either deleted (if corrupt) or preserved (if schema mismatch)
            logger.error(f"Failed to read {data_path}: {exc}")
            return empty_df

    def save(
        self,
        request: StorageRequest,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Merge new data with existing, deduplicate, and save to parquet.

        Args:
            new_data: Fresh data to add (with stock/date multi-index)
            existing_data: Previously persisted data (with stock/date multi-index)
            request: Storage metadata describing the target

        Returns:
            Merged DataFrame with stock/date multi-index
        """
        if new_data.empty and existing_data.empty:
            return self._empty_frame_factory()

        if new_data.empty:
            logger.debug("New data empty.. nothing to do")
            return existing_data

        frames = []
        if not existing_data.empty:
            frames.append(existing_data.reset_index())
        frames.append(new_data.reset_index())

        combined = pd.concat(frames, axis=0, ignore_index=True)
        combined = self._normalizer(combined)

        # Sort by stock, date, and sequence to ensure deterministic deduplication
        combined = combined.sort_values(["stock", "date", "sequence"], kind="mergesort")
        # Keep the last occurrence (highest sequence) for each stock/date pair
        combined = combined.drop_duplicates(subset=["stock", "date"], keep="last")
        # Final sort for consistent output
        combined = combined.sort_values(["stock", "date"], kind="mergesort")

        data_path = request.legacy_path()
        data_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_parquet(data_path, index=False, compression="gzip")
        return combined.set_index(["stock", "date"])
