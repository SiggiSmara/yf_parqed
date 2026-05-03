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


def test_save_is_atomic(tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    registry = TickerRegistry(config)
    registry.replace({"X": {"ticker": "X", "status": "active", "intervals": {}}})
    registry.save()

    assert config.tickers_path.exists()
    assert not config.tickers_path.with_suffix(".tmp").exists()
    assert registry.tickers == {
        "X": {"ticker": "X", "status": "active", "intervals": {}}
    }


def test_update_current_list_adds_new_does_not_reactivate(
    registry: TickerRegistry,
) -> None:
    """New tickers are added; existing not_found tickers are NOT reactivated."""
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
    assert registry.tickers["NEW"]["source"] == "csv"
    # STAY must NOT be reactivated — death cycle controls its fate now
    assert registry.tickers["STAY"]["status"] == "not_found"
    assert registry.tickers["STAY"]["intervals"]["1d"]["status"] == "not_found"


def test_update_current_list_prunes_dead_tickers_absent_from_csv(
    registry: TickerRegistry,
) -> None:
    """Permanently dead CSV tickers not in the new CSV are removed from registry."""
    registry.replace(
        {
            "DEAD": {
                "ticker": "DEAD",
                "status": "active",
                "source": "csv",
                "intervals": {
                    "1d": {"status": "not_found", "permanently_dead": True},
                    "1h": {"status": "not_found", "permanently_dead": True},
                },
            },
            "ALIVE": {
                "ticker": "ALIVE",
                "status": "active",
                "source": "csv",
                "intervals": {"1d": {"status": "active"}},
            },
            "MANUAL": {
                "ticker": "MANUAL",
                "status": "active",
                "source": "manual",
                "intervals": {
                    "1d": {"status": "not_found", "permanently_dead": True},
                },
            },
        }
    )

    # CSV now only has ALIVE (DEAD and MANUAL are absent)
    registry.update_current_list(
        {"ALIVE": {"ticker": "ALIVE", "status": "active", "intervals": {}}}
    )

    assert "DEAD" not in registry.tickers  # pruned
    assert "ALIVE" in registry.tickers  # kept (still in CSV)
    assert "MANUAL" in registry.tickers  # kept (manual — never pruned)


def test_update_current_list_keeps_partially_dead_tickers(
    registry: TickerRegistry,
) -> None:
    """Ticker with only some intervals dead is NOT pruned even if absent from CSV."""
    registry.replace(
        {
            "PARTIAL": {
                "ticker": "PARTIAL",
                "status": "active",
                "source": "csv",
                "intervals": {
                    "1d": {"status": "not_found", "permanently_dead": True},
                    "1h": {"status": "active"},
                },
            }
        }
    )

    registry.update_current_list(
        {"OTHER": {"ticker": "OTHER", "status": "active", "intervals": {}}}
    )

    assert "PARTIAL" in registry.tickers


# ── is_active_for_interval ────────────────────────────────────────────────────


def test_is_active_unknown_ticker(registry: TickerRegistry) -> None:
    assert registry.is_active_for_interval("MISSING", "1d") is True


def test_is_active_normal_active_ticker(registry: TickerRegistry) -> None:
    registry.replace({"OK": {"ticker": "OK", "status": "active", "intervals": {}}})
    assert registry.is_active_for_interval("OK", "1d") is True


def test_is_active_legacy_global_not_found(registry: TickerRegistry) -> None:
    registry.replace({"OFF": {"ticker": "OFF", "status": "not_found", "intervals": {}}})
    assert registry.is_active_for_interval("OFF", "1d") is False


def test_is_active_manual_ticker_always_true(registry: TickerRegistry) -> None:
    """Manual tickers are exempt from all death-cycle checks."""
    registry.replace(
        {
            "M": {
                "ticker": "M",
                "status": "not_found",
                "source": "manual",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "permanently_dead": True,
                        "cooling_since": "2024-01-01",
                    },
                },
            }
        }
    )
    assert registry.is_active_for_interval("M", "1d") is True


def test_is_active_manually_removed_always_false(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "R": {
                "ticker": "R",
                "status": "active",
                "manually_removed": True,
                "intervals": {},
            }
        }
    )
    assert registry.is_active_for_interval("R", "1d") is False


def test_is_active_permanently_dead_interval(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "D": {
                "ticker": "D",
                "status": "active",
                "intervals": {"1d": {"status": "not_found", "permanently_dead": True}},
            }
        }
    )
    assert registry.is_active_for_interval("D", "1d") is False


def test_is_active_streak_phase_still_active(registry: TickerRegistry) -> None:
    """Ticker in streak phase (not_found but no cooling_since) is still retried."""
    registry.replace(
        {
            "S": {
                "ticker": "S",
                "status": "active",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "not_found_streak_days": 2,
                        "last_not_found_date": "2024-01-10",
                    },
                },
            }
        }
    )
    assert registry.is_active_for_interval("S", "1d") is True


def test_is_active_cooling_window_inactive(registry: TickerRegistry) -> None:
    """Ticker in cooling window (< 7 workdays elapsed) is not retried."""
    registry.replace(
        {
            "C": {
                "ticker": "C",
                "status": "active",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "not_found_streak_days": 3,
                        "cooling_since": "2024-02-05",
                    },
                },
            }
        }
    )
    # 3 days later — still within 7-workday cooldown
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 2, 8)):
        assert registry.is_active_for_interval("C", "1d") is False


def test_is_active_after_cooling_expires(registry: TickerRegistry) -> None:
    """After 7 work days, ticker becomes active again for single retry."""
    registry.replace(
        {
            "C": {
                "ticker": "C",
                "status": "active",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "cooling_since": "2024-02-05",
                    },
                },
            }
        }
    )
    # 2026-02-05 is a Thursday; 7 workdays later = 2026-02-14 (Friday)
    # Use a Monday 10 workdays out to be safely past the window
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 2, 19)):
        assert registry.is_active_for_interval("C", "1d") is True


# ── update_ticker_interval_status — failure path ─────────────────────────────


def test_failure_increments_streak_once_per_day(registry: TickerRegistry) -> None:
    day1 = datetime(2024, 3, 1)
    with patch.object(registry._config, "get_now", return_value=day1):
        registry.update_ticker_interval_status("T", "1d", False)
        registry.update_ticker_interval_status(
            "T", "1d", False
        )  # same day, no increment

    interval = registry.tickers["T"]["intervals"]["1d"]
    assert interval["not_found_streak_days"] == 1
    assert interval["status"] == "not_found"
    assert "cooling_since" not in interval


def test_failure_streak_across_days_triggers_cooling(registry: TickerRegistry) -> None:
    for offset in range(3):
        day = datetime(2024, 3, 1) + timedelta(days=offset)
        with patch.object(registry._config, "get_now", return_value=day):
            registry.update_ticker_interval_status("T", "1d", False)

    interval = registry.tickers["T"]["intervals"]["1d"]
    assert interval["not_found_streak_days"] == 3
    assert "cooling_since" in interval


def test_failure_post_cooling_marks_permanently_dead(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "T": {
                "ticker": "T",
                "status": "active",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "cooling_since": "2024-02-01",
                        "not_found_streak_days": 3,
                    },
                },
            }
        }
    )
    # 10 workdays after cooling_since = past the 7-workday window
    post_cooling = datetime(2024, 2, 15)
    with patch.object(registry._config, "get_now", return_value=post_cooling):
        registry.update_ticker_interval_status("T", "1d", False)

    interval = registry.tickers["T"]["intervals"]["1d"]
    assert interval.get("permanently_dead") is True
    assert "cooling_since" not in interval


def test_failure_skipped_for_permanently_dead_interval(
    registry: TickerRegistry,
) -> None:
    registry.replace(
        {
            "T": {
                "ticker": "T",
                "status": "active",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "permanently_dead": True,
                        "not_found_streak_days": 3,
                    },
                },
            }
        }
    )
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 5, 1)):
        registry.update_ticker_interval_status("T", "1d", False)

    # Streak must not change
    assert registry.tickers["T"]["intervals"]["1d"]["not_found_streak_days"] == 3


def test_global_status_not_set_on_interval_failures(registry: TickerRegistry) -> None:
    """Global ticker status is no longer set to not_found by per-interval failures."""
    base = datetime(2024, 3, 1)
    registry.replace(
        {
            "FAIL": {
                "ticker": "FAIL",
                "status": "active",
                "intervals": {
                    "1d": {"status": "active"},
                    "1h": {"status": "active"},
                },
            }
        }
    )

    with patch.object(registry._config, "get_now", return_value=base):
        registry.update_ticker_interval_status("FAIL", "1h", False)
        registry.update_ticker_interval_status("FAIL", "1d", False)

    # Global status stays active — per-interval permanently_dead is authoritative
    assert registry.tickers["FAIL"]["status"] == "active"


# ── update_ticker_interval_status — success path ─────────────────────────────


def test_success_clears_streak_and_cooling(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "T": {
                "ticker": "T",
                "status": "active",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "not_found_streak_days": 2,
                        "cooling_since": "2024-02-01",
                    },
                },
            }
        }
    )
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 3, 1)):
        registry.update_ticker_interval_status("T", "1d", True)

    interval = registry.tickers["T"]["intervals"]["1d"]
    assert interval["status"] == "active"
    assert "not_found_streak_days" not in interval
    assert "cooling_since" not in interval


def test_success_does_not_clear_permanently_dead(registry: TickerRegistry) -> None:
    """permanently_dead can only be cleared by add_ticker(), not by a success event."""
    registry.replace(
        {
            "T": {
                "ticker": "T",
                "status": "active",
                "intervals": {
                    "1d": {"status": "not_found", "permanently_dead": True},
                },
            }
        }
    )
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 3, 1)):
        registry.update_ticker_interval_status("T", "1d", True)

    # permanently_dead must survive the success call
    assert registry.tickers["T"]["intervals"]["1d"].get("permanently_dead") is True


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


# ── add_ticker / remove_ticker ────────────────────────────────────────────────


def test_add_ticker_new_creates_manual_entry(registry: TickerRegistry) -> None:
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 5, 1)):
        registry.add_ticker("NEWCO")

    data = registry.tickers["NEWCO"]
    assert data["source"] == "manual"
    assert data["status"] == "active"
    assert data["intervals"] == {}


def test_add_ticker_resurrects_permanently_dead(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "DEAD": {
                "ticker": "DEAD",
                "status": "active",
                "source": "csv",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "permanently_dead": True,
                        "not_found_streak_days": 3,
                    },
                    "1h": {"status": "not_found", "permanently_dead": True},
                },
            }
        }
    )
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 5, 1)):
        registry.add_ticker("DEAD")

    data = registry.tickers["DEAD"]
    assert data["source"] == "manual"
    assert data["status"] == "active"
    for iv in data["intervals"].values():
        assert not iv.get("permanently_dead")
        assert "not_found_streak_days" not in iv
        assert iv["status"] == "active"


def test_add_ticker_is_exempt_from_death_cycle(registry: TickerRegistry) -> None:
    with patch.object(registry._config, "get_now", return_value=datetime(2024, 5, 1)):
        registry.add_ticker("MANUAL")

    assert registry.is_active_for_interval("MANUAL", "1d") is True


def test_remove_ticker_marks_manually_removed(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "BYE": {
                "ticker": "BYE",
                "status": "active",
                "source": "csv",
                "intervals": {"1d": {"status": "active"}, "1h": {"status": "active"}},
            }
        }
    )
    registry.remove_ticker("BYE")

    data = registry.tickers["BYE"]
    assert data["manually_removed"] is True
    assert data["source"] == "manual"
    for iv in data["intervals"].values():
        assert iv["permanently_dead"] is True
    assert registry.is_active_for_interval("BYE", "1d") is False


def test_remove_ticker_not_reactivated_by_csv_update(registry: TickerRegistry) -> None:
    registry.replace(
        {
            "BYE": {
                "ticker": "BYE",
                "status": "active",
                "source": "manual",
                "manually_removed": True,
                "intervals": {"1d": {"status": "not_found", "permanently_dead": True}},
            }
        }
    )

    incoming = {"BYE": {"ticker": "BYE", "status": "active", "intervals": {}}}
    registry.update_current_list(incoming)

    # BYE should still be in registry but still manually_removed
    assert registry.tickers["BYE"].get("manually_removed") is True


def test_remove_ticker_missing_logs_warning(registry: TickerRegistry, caplog) -> None:
    registry.remove_ticker("GHOST")  # should not raise


# ── business_days_since ───────────────────────────────────────────────────────


def test_business_days_since_skips_weekends() -> None:
    # 2024-02-05 is Monday; 2024-02-12 is Monday — 5 workdays between them
    now = datetime(2024, 2, 12)
    count = TickerRegistry._business_days_since("2024-02-05", now)
    assert count == 5


def test_business_days_since_handles_invalid_date() -> None:
    assert TickerRegistry._business_days_since("not-a-date", datetime(2024, 3, 1)) == 0


def test_business_days_since_same_day_is_zero() -> None:
    now = datetime(2024, 2, 5)
    assert TickerRegistry._business_days_since("2024-02-05", now) == 0


# ── misc ──────────────────────────────────────────────────────────────────────


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


def test_get_interval_storage_returns_metadata(tmp_path: Path) -> None:
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
