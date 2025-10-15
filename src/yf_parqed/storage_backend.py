from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

import pandas as pd
from loguru import logger


@dataclass(frozen=True)
class StorageRequest:
    root: Path
    interval: str
    ticker: str
    market: str | None = None
    source: str | None = None
    dataset: str = "stocks"

    def legacy_path(self) -> Path:
        return self.root / f"stocks_{self.interval}" / f"{self.ticker}.parquet"


class StorageInterface(Protocol):
    def read(self, request: StorageRequest) -> pd.DataFrame: ...

    def save(
        self,
        request: StorageRequest,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
    ) -> pd.DataFrame: ...


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

        try:
            df = pd.read_parquet(data_path)
        except (ValueError, FileNotFoundError, OSError):
            logger.debug(
                f"Unable to read parquet file for {data_path.stem}, deleting corrupt file"
            )
            self._remove_file(data_path)
            return empty_df

        required = set(self._column_provider())
        if df.empty or not required.issubset(df.columns):
            logger.debug(
                f"Invalid dataframe schema for {data_path.stem}, deleting file before rehydrating"
            )
            self._remove_file(data_path)
            return empty_df

        df = self._normalizer(df)
        df.set_index(["stock", "date"], inplace=True)
        return df

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

    def _remove_file(self, path: Path) -> None:
        """Safely remove a file, handling both old and new pathlib APIs."""
        try:
            path.unlink(missing_ok=True)
        except TypeError:
            # Older Python versions don't have missing_ok parameter
            if path.exists():
                path.unlink()
