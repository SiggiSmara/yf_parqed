from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


from yf_parqed.common.config_service import ConfigService
from yf_parqed.common.migration_plan import MigrationPlan


def test_default_base_path_uses_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    service = ConfigService()
    assert service.base_path == tmp_path
    assert service.tickers_path == tmp_path / "tickers.json"
    assert service.intervals_path == tmp_path / "intervals.json"


def test_set_working_path_updates_paths(tmp_path):
    service = ConfigService()
    new_path = service.set_working_path(tmp_path)
    assert new_path == tmp_path
    assert service.base_path == tmp_path
    assert service.tickers_path == tmp_path / "tickers.json"
    assert service.intervals_path == tmp_path / "intervals.json"


def test_load_intervals_handles_missing_file(tmp_path):
    service = ConfigService(tmp_path)
    assert service.load_intervals() == []


def test_load_intervals_handles_invalid_json(tmp_path, caplog):
    intervals_path = tmp_path / "intervals.json"
    intervals_path.write_text("{not-json]")
    service = ConfigService(tmp_path)
    with caplog.at_level("WARNING"):
        result = service.load_intervals()
    assert result == []


def test_save_intervals_persists_content(tmp_path):
    service = ConfigService(tmp_path)
    saved = service.save_intervals(["1d", "1h"])
    assert saved == ["1d", "1h"]
    assert json.loads(service.intervals_path.read_text()) == ["1d", "1h"]


def test_load_tickers_handles_missing_file(tmp_path):
    service = ConfigService(tmp_path)
    assert service.load_tickers() == {}


def test_load_tickers_handles_invalid_json(tmp_path, caplog):
    tickers_path = tmp_path / "tickers.json"
    tickers_path.write_text("[{]")
    service = ConfigService(tmp_path)
    with caplog.at_level("WARNING"):
        result = service.load_tickers()
    assert result == {}


def test_save_tickers_persists_content(tmp_path):
    service = ConfigService(tmp_path)
    payload = {"AAPL": {"ticker": "AAPL"}}
    service.save_tickers(payload)
    assert json.loads(service.tickers_path.read_text()) == payload


def test_configure_limits_updates_state():
    service = ConfigService()
    limits = service.configure_limits(5, 10)
    assert limits == (5, 10)
    assert service.get_limits() == (5, 10)


def test_configure_limits_defaults_exposed():
    service = ConfigService()
    assert service.get_limits() == (3, 2)


def test_format_date_defaults_to_get_now(monkeypatch):
    service = ConfigService()
    fake_now = datetime(2024, 1, 15)
    monkeypatch.setattr(service, "get_now", lambda: fake_now)
    assert service.format_date() == "2024-01-15"


def test_format_date_uses_explicit_value():
    service = ConfigService()
    explicit = datetime(2023, 12, 31)
    assert service.format_date(explicit) == "2023-12-31"


def test_load_storage_config_defaults(tmp_path):
    service = ConfigService(tmp_path)
    config = service.load_storage_config()
    assert config == {
        "partitioned": True,
        "markets": {},
        "sources": {},
    }


def test_set_partition_mode_persists(tmp_path):
    service = ConfigService(tmp_path)
    updated = service.set_partition_mode(True)
    assert updated["partitioned"] is True
    stored = json.loads(service.storage_config_path.read_text())
    assert stored["partitioned"] is True


def test_partition_overrides_precedence(tmp_path):
    service = ConfigService(tmp_path)
    service.set_partition_mode(False)
    # Market-level override
    service.set_market_partition_mode("DE", True)
    assert service.is_partitioned_enabled(market="de") is True
    assert service.is_partitioned_enabled(market="us") is False

    # Source-level override takes precedence over market-level
    service.set_source_partition_mode("DE", "XETRA", False)
    assert service.is_partitioned_enabled(market="de", source="xetra") is False
    # Market still true when source override not supplied
    assert service.is_partitioned_enabled(market="de", source="tradegate") is True


def test_load_storage_config_handles_invalid_json(tmp_path, caplog):
    path = tmp_path / "storage_config.json"
    path.write_text("not-json")
    service = ConfigService(tmp_path)
    with caplog.at_level("WARNING"):
        config = service.load_storage_config()
    assert config["partitioned"] is True


def _sample_plan() -> dict:
    return {
        "schema_version": 1,
        "generated_at": "2025-10-15T12:40:00Z",
        "created_by": "test-suite",
        "legacy_root": "data/legacy",
        "venues": [
            {
                "id": "us:yahoo",
                "market": "US",
                "source": "yahoo",
                "status": "pending",
                "last_updated": "2025-10-15T12:40:00Z",
                "intervals": {
                    "1m": {
                        "legacy_path": "data/legacy/stocks_1m",
                        "partition_path": "data/us/yahoo/stocks/interval=1m",
                        "status": "pending",
                        "totals": {"legacy_rows": None, "partition_rows": None},
                        "jobs": {"total": 0, "completed": 0},
                        "resume_token": None,
                        "verification": {"method": "row_counts", "verified_at": None},
                        "backups": [],
                    }
                },
            }
        ],
    }


def test_migration_plan_path(tmp_path):
    service = ConfigService(tmp_path)
    assert service.migration_plan_path == tmp_path / "migration_plan.json"


def test_load_migration_plan(tmp_path):
    service = ConfigService(tmp_path)
    plan_path = service.migration_plan_path
    plan_path.write_text(json.dumps(_sample_plan(), indent=4))

    plan = service.load_migration_plan()

    assert isinstance(plan, MigrationPlan)
    assert plan.legacy_root == Path("data/legacy")
    assert "us:yahoo" in plan.venues
