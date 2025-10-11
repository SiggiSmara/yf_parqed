from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

from loguru import logger


class ConfigService:
    """Handle filesystem paths, persistence, and limiter settings."""

    def __init__(self, base_path: Path | None = None):
        self._base_path = Path(base_path) if base_path is not None else Path.cwd()
        self._max_requests = 3
        self._duration = 2

    @property
    def base_path(self) -> Path:
        return self._base_path

    @property
    def tickers_path(self) -> Path:
        return self._base_path / "tickers.json"

    @property
    def intervals_path(self) -> Path:
        return self._base_path / "intervals.json"

    def set_working_path(self, new_path: Path | None) -> Path:
        self._base_path = Path(new_path) if new_path is not None else Path.cwd()
        return self._base_path

    def load_intervals(self) -> list[str]:
        if self.intervals_path.is_file():
            try:
                data = json.loads(self.intervals_path.read_text())
                if isinstance(data, list):
                    return data
            except json.JSONDecodeError:
                logger.warning(
                    "Failed to decode intervals.json; defaulting to empty list"
                )
        return []

    def save_intervals(self, intervals: Iterable[str]) -> list[str]:
        intervals_list = list(intervals)
        self.intervals_path.write_text(json.dumps(intervals_list, indent=4))
        return intervals_list

    def load_tickers(self) -> dict:
        if self.tickers_path.is_file():
            try:
                data = json.loads(self.tickers_path.read_text())
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                logger.warning(
                    "Failed to decode tickers.json; defaulting to empty dict"
                )
        return {}

    def save_tickers(self, tickers: dict) -> None:
        self.tickers_path.write_text(json.dumps(tickers, indent=4))

    def configure_limits(
        self, max_requests: int = 3, duration: int = 2
    ) -> tuple[int, int]:
        logger.info(
            f"Ratelimiting set to max {max_requests} requests per {duration} seconds"
        )
        self._max_requests = max_requests
        self._duration = duration
        return self._max_requests, self._duration

    def get_limits(self) -> tuple[int, int]:
        return self._max_requests, self._duration

    def get_now(self) -> datetime:
        return datetime.now()

    def format_date(self, value: datetime | None = None) -> str:
        target = value if value is not None else self.get_now()
        return target.strftime("%Y-%m-%d")
