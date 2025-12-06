from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from yf_parqed.common.config_service import ConfigService
from yf_parqed.yahoo.ticker_registry import TickerRegistry


@pytest.fixture()
def registry(tmp_path: Path) -> TickerRegistry:
    config = ConfigService(tmp_path)
    return TickerRegistry(config)


def test_load_returns_empty_when_file_missing(registry: TickerRegistry) -> None:
    assert registry.tickers == {}


def test_save_round_trip(tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    registry = TickerRegistry(config)
    sample = {
        "AAPL": {
            "ticker": "AAPL",
            "added_date": "2024-01-01",
            "status": "active",
            "last_checked": "2024-01-02",
            "intervals": {},
        }
    }

    registry.replace(sample)
    registry.save()

    reloaded = TickerRegistry(config)
    assert reloaded.tickers == sample


def test_update_current_list_adds_and_reactivates(registry: TickerRegistry) -> None:
    existing = {
        "STAY": {
            "ticker": "STAY",
            "added_date": "2024-01-01",
            "status": "not_found",
            "last_checked": "2024-01-05",
            "intervals": {
                "1d": {
                    "status": "not_found",
                    "last_not_found_date": "2024-01-05",
                    "last_checked": "2024-01-05",
                }
            },
        }
    }
    registry.replace(existing)

    incoming = {
        "NEW": {
            "ticker": "NEW",
            "added_date": "2024-02-01",
            "status": "active",
            "last_checked": None,
            "intervals": {},
        },
        "STAY": {
            "ticker": "STAY",
            "added_date": "2024-02-01",
            "status": "active",
            "last_checked": None,
            "intervals": {},
        },
    }

    registry.update_current_list(incoming)

    assert registry.tickers["NEW"]["status"] == "active"
    assert registry.tickers["STAY"]["status"] == "active"
    assert registry.tickers["STAY"]["intervals"]["1d"]["status"] == "not_found"


def test_is_active_for_interval_respects_flags_and_cooldown(
    registry: TickerRegistry,
) -> None:
    registry.replace(
        {
            "ACTIVE": {
                "ticker": "ACTIVE",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": "2024-01-02",
                "intervals": {},
            },
            "GLOBAL_OFF": {
                "ticker": "GLOBAL_OFF",
                "added_date": "2024-01-01",
                "status": "not_found",
                "last_checked": "2024-01-02",
                "intervals": {},
            },
            "COOLDOWN": {
                "ticker": "COOLDOWN",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": "2024-01-02",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": "2024-02-01",
                        "last_checked": "2024-02-01",
                    }
                },
            },
        }
    )

    assert registry.is_active_for_interval("ACTIVE", "1d") is True
    assert registry.is_active_for_interval("MISSING", "1d") is True
    assert registry.is_active_for_interval("GLOBAL_OFF", "1d") is False

    current = datetime(2024, 2, 5)
    with patch.object(registry._config, "get_now", return_value=current):
        assert registry.is_active_for_interval("COOLDOWN", "1d") is False

    later = current + timedelta(days=40)
    with patch.object(registry._config, "get_now", return_value=later):
        assert registry.is_active_for_interval("COOLDOWN", "1d") is True


def test_update_interval_status_found_data(registry: TickerRegistry) -> None:
    target_date = datetime(2024, 2, 10)
    with patch.object(registry._config, "get_now", return_value=target_date):
        registry.update_ticker_interval_status("TEST", "1d", True, target_date)

    ticker = registry.tickers["TEST"]
    interval = ticker["intervals"]["1d"]
    stamp = target_date.strftime("%Y-%m-%d")
    assert ticker["status"] == "active"
    assert ticker["last_checked"] == stamp
    assert interval["status"] == "active"
    assert interval["last_found_date"] == stamp
    assert interval["last_data_date"] == stamp


def test_update_interval_status_handles_failures(registry: TickerRegistry) -> None:
    base = datetime(2024, 3, 1)
    registry.replace(
        {
            "FAIL": {
                "ticker": "FAIL",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": "2024-01-15",
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": "2024-02-01",
                        "last_data_date": "2024-02-01",
                        "last_checked": "2024-02-01",
                    }
                },
            }
        }
    )

    with patch.object(registry._config, "get_now", return_value=base):
        registry.update_ticker_interval_status("FAIL", "1h", False)
        registry.update_ticker_interval_status("FAIL", "1d", False)

    ticker = registry.tickers["FAIL"]
    stamp = base.strftime("%Y-%m-%d")
    assert ticker["status"] == "not_found"
    assert ticker["last_checked"] == stamp
    assert all(meta["status"] == "not_found" for meta in ticker["intervals"].values())


def test_replace_overwrites_internal_state(registry: TickerRegistry) -> None:
    new_data = {
        "META": {
            "ticker": "META",
            "added_date": "2024-01-01",
            "status": "active",
            "last_checked": "2024-01-02",
            "custom_field": 99,
            "intervals": {
                "1d": {
                    "status": "active",
                    "last_found_date": "2024-01-02",
                    "last_data_date": "2024-01-01",
                    "last_checked": "2024-01-02",
                }
            },
        }
    }

    registry.replace(new_data)
    assert registry.tickers == new_data


def test_get_last_data_date_parses_metadata(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "PARSE": {
                "ticker": "PARSE",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": "2024-01-02",
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_data_date": "2024-02-10",
                    }
                },
            }
        }
    )

    result = registry.get_last_data_date("PARSE", "1d")
    assert isinstance(result, datetime)
    assert result.strftime("%Y-%m-%d") == "2024-02-10"


def test_get_last_data_date_handles_missing_values(registry: TickerRegistry) -> None:
    registry.replace({})
    assert registry.get_last_data_date("MISSING", "1d") is None

    registry.replace(
        {
            "BAD": {
                "ticker": "BAD",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": "2024-01-02",
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_data_date": "20-02-2024",
                    }
                },
            }
        }
    )

    assert registry.get_last_data_date("BAD", "1d") is None

    def test_get_interval_storage_returns_metadata(tmp_path):
        service = ConfigService(tmp_path)
        registry = TickerRegistry(config=service)
        registry.replace(
            {
                "PART": {
                    "ticker": "PART",
                    "intervals": {
                        "1m": {
                            "status": "active",
                            "storage": {
                                "mode": "partitioned",
                                "market": "us",
                                "source": "yahoo",
                                "dataset": "stocks",
                                "root": "data",
                                "verified_at": "2025-10-15T00:00:00Z",
                            },
                        }
                    },
                }
            }
        )

        storage = registry.get_interval_storage("PART", "1m")
        assert storage is not None
        assert storage["mode"] == "partitioned"
        assert registry.get_interval_storage("PART", "1h") is None
