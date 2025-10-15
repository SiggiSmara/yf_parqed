from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union

Timestamp = Union[date, datetime]


class PartitionPathBuilder:
    def __init__(self, root: Union[str, Path] = Path("data")) -> None:
        self._root = Path(root)

    def build(
        self,
        *,
        market: Optional[str],
        source: Optional[str],
        dataset: Optional[str],
        interval: str,
        ticker: str,
        timestamp: Timestamp,
    ) -> Path:
        interval = (interval or "").strip()
        ticker = (ticker or "").strip()
        dataset = (dataset or "").strip()
        if not interval:
            raise ValueError("interval is required")
        if not ticker:
            raise ValueError("ticker is required")
        normalized_date = self._normalize_date(timestamp)
        if not market or not source:
            return self._legacy_path(interval, ticker)
        dataset_segment = f"{dataset.lower()}_{interval}"
        return (
            self._root
            / self._normalize_segment(market)
            / self._normalize_segment(source)
            / dataset_segment
            / f"ticker={ticker}"
            / f"year={normalized_date.year:04d}"
            / f"month={normalized_date.month:02d}"
            / f"day={normalized_date.day:02d}"
            / "data.parquet"
        )

    def _legacy_path(self, interval: str, ticker: str) -> Path:
        return self._root / f"{self._legacy_prefix(interval)}/{ticker}.parquet"

    def _legacy_prefix(self, interval: str) -> str:
        return f"stocks_{interval}"

    def _normalize_segment(self, value: str) -> str:
        return value.strip().lower()

    def _normalize_date(self, timestamp: Timestamp) -> date:
        if isinstance(timestamp, datetime):
            return timestamp.date()
        if isinstance(timestamp, date):
            return timestamp
        raise TypeError("timestamp must be a date or datetime instance")

    def ticker_root(
        self,
        *,
        market: Optional[str],
        source: Optional[str],
        dataset: Optional[str],
        interval: str,
        ticker: str,
    ) -> Path:
        interval = (interval or "").strip()
        ticker = (ticker or "").strip()
        dataset = (dataset or "").strip()
        if not interval:
            raise ValueError("interval is required")
        if not ticker:
            raise ValueError("ticker is required")
        if not market or not source:
            raise ValueError("market and source are required for partitioned paths")
        dataset_segment = f"{dataset.lower()}_{interval}"
        return (
            self._root
            / self._normalize_segment(market)
            / self._normalize_segment(source)
            / dataset_segment
            / f"ticker={ticker}"
        )
