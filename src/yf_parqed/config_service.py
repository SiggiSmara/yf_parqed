from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable


from loguru import logger

from .migration_plan import MigrationPlan


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

    @property
    def storage_config_path(self) -> Path:
        return self._base_path / "storage_config.json"

    @property
    def migration_plan_path(self) -> Path:
        return self._base_path / "migration_plan.json"

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

    def load_storage_config(self) -> dict:
        default = self._default_storage_config()
        path = self.storage_config_path
        if path.is_file():
            try:
                data = json.loads(path.read_text())
                if isinstance(data, dict):
                    return self._normalize_storage_config(data)
            except json.JSONDecodeError:
                logger.warning(
                    "Failed to decode storage_config.json; defaulting to global legacy mode"
                )
        return default

    def save_storage_config(self, config: dict) -> dict:
        normalized = self._normalize_storage_config(config)
        self.storage_config_path.write_text(json.dumps(normalized, indent=4))
        return normalized

    def set_partition_mode(self, enabled: bool) -> dict:
        config = self.load_storage_config()
        config["partitioned"] = bool(enabled)
        return self.save_storage_config(config)

    def set_market_partition_mode(self, market: str, enabled: bool) -> dict:
        if not market or not isinstance(market, str):
            raise ValueError("market must be a non-empty string")
        config = self.load_storage_config()
        key = self._normalize_market_key(market)
        config.setdefault("markets", {})[key] = bool(enabled)
        return self.save_storage_config(config)

    def set_source_partition_mode(
        self, market: str, source: str, enabled: bool
    ) -> dict:
        if not market or not isinstance(market, str):
            raise ValueError("market must be a non-empty string")
        if not source or not isinstance(source, str):
            raise ValueError("source must be a non-empty string")
        config = self.load_storage_config()
        key = self._normalize_source_key(market, source)
        config.setdefault("sources", {})[key] = bool(enabled)
        return self.save_storage_config(config)

    def clear_market_partition_mode(self, market: str) -> dict:
        config = self.load_storage_config()
        key = self._normalize_market_key(market)
        config.setdefault("markets", {}).pop(key, None)
        return self.save_storage_config(config)

    def clear_source_partition_mode(self, market: str, source: str) -> dict:
        config = self.load_storage_config()
        key = self._normalize_source_key(market, source)
        config.setdefault("sources", {}).pop(key, None)
        return self.save_storage_config(config)

    def is_partitioned_enabled(
        self, market: str | None = None, source: str | None = None
    ) -> bool:
        config = self.load_storage_config()
        sources = config.get("sources", {})
        markets = config.get("markets", {})

        if market and source:
            key = self._normalize_source_key(market, source)
            if key in sources:
                return bool(sources[key])

        if market:
            market_key = self._normalize_market_key(market)
            if market_key in markets:
                return bool(markets[market_key])

        return bool(config.get("partitioned", False))

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

    def _default_storage_config(self) -> dict:
        return {
            "partitioned": False,
            "markets": {},
            "sources": {},
        }

    def _normalize_storage_config(self, config: dict) -> dict:
        base = self._default_storage_config()
        base["partitioned"] = bool(config.get("partitioned", False))

        markets = config.get("markets", {}) or {}
        if isinstance(markets, dict):
            for key, value in markets.items():
                if isinstance(key, str):
                    base["markets"][self._normalize_market_key(key)] = bool(value)

        sources = config.get("sources", {}) or {}
        if isinstance(sources, dict):
            for key, value in sources.items():
                if isinstance(key, str):
                    if "/" in key:
                        base["sources"][
                            self._normalize_source_key(*key.split("/", 1))
                        ] = bool(value)
                    else:
                        # fallback: treat value as market-only override
                        base["markets"][self._normalize_market_key(key)] = bool(value)

        return {
            "partitioned": base["partitioned"],
            "markets": dict(base["markets"]),
            "sources": dict(base["sources"]),
        }

    def load_migration_plan(self) -> MigrationPlan:
        path = self.migration_plan_path
        if not path.is_file():
            raise FileNotFoundError(
                f"migration plan not found at {path}. Run partition-migrate init to create it."
            )
        return MigrationPlan.from_file(path)

    def _normalize_market_key(self, market: str) -> str:
        return market.strip().lower()

    def _normalize_source_key(self, market: str, source: str) -> str:
        return f"{self._normalize_market_key(market)}/{source.strip().lower()}"
