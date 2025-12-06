from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
import pytest
import yf_parqed.partition_migration_service as migration_module

import pandas as pd

from yf_parqed.common.config_service import ConfigService
from yf_parqed.partition_migration_service import PartitionMigrationService


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
                        "partition_path": "data/us/yahoo/stocks_1m",
                        "status": "pending",
                        "totals": {
                            "legacy_rows": None,
                            "partition_rows": None,
                        },
                        "jobs": {
                            "total": 0,
                            "completed": 0,
                        },
                        "resume_token": None,
                        "verification": {
                            "method": "row_counts",
                            "verified_at": None,
                        },
                        "backups": [],
                    }
                },
            }
        ],
    }


def _write_plan(tmp_path: Path) -> Path:
    path = tmp_path / "migration_plan.json"
    path.write_text(json.dumps(_sample_plan(), indent=4))
    return path


def test_get_interval_paths(tmp_path: Path) -> None:
    _write_plan(tmp_path)
    service = PartitionMigrationService(
        ConfigService(tmp_path),
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:00:00Z",
    )

    legacy_path, partition_path = service.get_interval_paths("us:yahoo", "1m")

    assert legacy_path == tmp_path / "data/legacy/stocks_1m"
    assert partition_path == tmp_path / "data/us/yahoo/stocks_1m"


def test_update_interval_persists_changes(tmp_path: Path) -> None:
    _write_plan(tmp_path)
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:05:00Z",
    )

    service.update_interval(
        "us:yahoo",
        "1m",
        status="migrating",
        jobs_total=12,
        jobs_completed=5,
        legacy_rows=1500,
        partition_rows=750,
        resume_token="batch-1",
    )

    plan = config.load_migration_plan()
    interval = plan.get_venue("us:yahoo").intervals["1m"]

    assert interval.status == "migrating"
    assert interval.jobs.total == 12
    assert interval.jobs.completed == 5
    assert interval.totals.legacy_rows == 1500
    assert interval.totals.partition_rows == 750
    assert interval.resume_token == "batch-1"
    assert plan.generated_at == "2025-10-15T13:05:00Z"
    assert plan.created_by == "tests"


def test_initialize_plan_creates_file(tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:10:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m", "1h"],
    )

    plan = config.load_migration_plan()
    assert "us:yahoo" in plan.venues
    assert set(plan.get_venue("us:yahoo").intervals.keys()) == {"1m", "1h"}


def test_migrate_interval_moves_data(tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:15:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m"],
    )

    # Seed tickers.json so metadata backfill can succeed
    config.tickers_path.write_text(
        json.dumps(
            {
                "AAA": {
                    "ticker": "AAA",
                    "status": "active",
                    "intervals": {
                        "1m": {
                            "status": "pending",
                        }
                    },
                }
            },
            indent=4,
        )
    )

    legacy_dir = tmp_path / "data/legacy/stocks_1m"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "stock": ["AAA", "AAA"],
            "date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "open": [1.0, 1.1],
            "high": [1.2, 1.3],
            "low": [0.9, 1.0],
            "close": [1.05, 1.15],
            "volume": [100, 110],
            "sequence": [0, 1],
        }
    )
    df.to_parquet(legacy_dir / "AAA.parquet", index=False)

    result = service.migrate_interval("us:yahoo", "1m")

    plan = config.load_migration_plan()
    interval = plan.get_venue("us:yahoo").intervals["1m"]

    assert result["jobs_total"] == 1
    assert result["jobs_completed"] == 1
    assert interval.status == "complete"
    assert interval.totals.legacy_rows == 2
    assert interval.totals.partition_rows == 2
    assert interval.verification.method == "row_counts+checksum"

    checksums = cast(dict[str, str], result["checksums"])
    assert "AAA" in checksums
    assert isinstance(checksums["AAA"], str)

    partition_file = (
        tmp_path / "data/us/yahoo/stocks_1m/ticker=AAA/year=2024/month=01/data.parquet"
    )
    assert partition_file.exists()
    partition_file_day2 = (
        tmp_path / "data/us/yahoo/stocks_1m/ticker=AAA/year=2024/month=02/data.parquet"
    )
    assert partition_file_day2.exists()
    # Legacy file should remain when delete_legacy=False
    assert (legacy_dir / "AAA.parquet").exists()

    tickers_data = json.loads(config.tickers_path.read_text())
    storage_meta = tickers_data["AAA"]["intervals"]["1m"].get("storage")
    assert storage_meta is not None
    assert storage_meta["mode"] == "partitioned"
    assert storage_meta["market"] == "us"
    assert storage_meta["source"] == "yahoo"
    assert storage_meta["root"] == "data"

    storage_config = json.loads((tmp_path / "storage_config.json").read_text())
    assert storage_config["sources"]["us/yahoo"] is True


def test_migrate_interval_with_limit_skips_plan_persistence(tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:17:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m"],
    )

    legacy_dir = tmp_path / "data/legacy/stocks_1m"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    df_aaa = pd.DataFrame(
        {
            "stock": ["AAA", "AAA"],
            "date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            "open": [1.0, 1.1],
            "high": [1.2, 1.3],
            "low": [0.9, 1.0],
            "close": [1.05, 1.15],
            "volume": [100, 110],
            "sequence": [0, 1],
        }
    )
    df_bbb = pd.DataFrame(
        {
            "stock": ["BBB", "BBB"],
            "date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            "open": [2.0, 2.1],
            "high": [2.2, 2.3],
            "low": [1.9, 2.0],
            "close": [2.05, 2.15],
            "volume": [200, 210],
            "sequence": [0, 1],
        }
    )
    df_aaa.to_parquet(legacy_dir / "AAA.parquet", index=False)
    df_bbb.to_parquet(legacy_dir / "BBB.parquet", index=False)

    plan_path = tmp_path / "migration_plan.json"
    before_plan = plan_path.read_text()

    result = service.migrate_interval("us:yahoo", "1m", max_tickers=1)

    after_plan = plan_path.read_text()
    assert after_plan == before_plan

    assert result["jobs_total"] == 1
    assert result["jobs_completed"] == 1
    assert result["available_jobs"] == 2
    assert result["persisted"] is False
    assert result["partial_run"] is True
    assert result["storage_activated"] is False
    assert result["tickers"] == ["AAA"]

    partition_root = tmp_path / "data/us/yahoo/stocks_1m"
    assert (partition_root / "ticker=AAA").exists()
    assert not (partition_root / "ticker=BBB").exists()

    # Legacy parquet files remain untouched in partial runs
    assert (legacy_dir / "AAA.parquet").exists()
    assert (legacy_dir / "BBB.parquet").exists()

    # No storage activation when plan persistence is skipped
    assert not config.storage_config_path.exists()


def test_migrate_interval_can_delete_legacy(tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:20:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m"],
    )

    legacy_dir = tmp_path / "data/legacy/stocks_1m"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "stock": ["BBB"],
            "date": [pd.Timestamp("2024-02-01")],
            "open": [2.0],
            "high": [2.1],
            "low": [1.9],
            "close": [2.05],
            "volume": [200],
            "sequence": [0],
        }
    )
    legacy_file = legacy_dir / "BBB.parquet"
    df.to_parquet(legacy_file, index=False)

    service.migrate_interval("us:yahoo", "1m", delete_legacy=True)

    assert not legacy_file.exists()
    # Parent directory cleaned up if empty
    assert not legacy_dir.exists()


def test_migrate_interval_detects_row_count_mismatch(tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:25:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m"],
    )

    legacy_dir = tmp_path / "data/legacy/stocks_1m"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    legacy_df = pd.DataFrame(
        {
            "stock": ["MMM"],
            "date": [pd.Timestamp("2024-04-01")],
            "open": [4.0],
            "high": [4.1],
            "low": [3.9],
            "close": [4.05],
            "volume": [400],
            "sequence": [0],
        }
    )
    legacy_df.to_parquet(legacy_dir / "MMM.parquet", index=False)

    partition_dir = tmp_path / "data/us/yahoo/stocks_1m/ticker=MMM/year=2024/month=04"
    partition_dir.mkdir(parents=True, exist_ok=True)
    existing_df = pd.DataFrame(
        {
            "stock": ["MMM"],
            "date": [pd.Timestamp("2024-04-02")],
            "open": [4.1],
            "high": [4.2],
            "low": [4.0],
            "close": [4.15],
            "volume": [450],
            "sequence": [0],
        }
    )
    existing_df.to_parquet(partition_dir / "data.parquet", index=False)

    with pytest.raises(ValueError, match="Row count mismatch"):
        service.migrate_interval("us:yahoo", "1m")


def test_migrate_interval_detects_checksum_mismatch(
    monkeypatch, tmp_path: Path
) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:27:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m"],
    )

    legacy_dir = tmp_path / "data/legacy/stocks_1m"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "stock": ["ZZZ"],
            "date": [pd.Timestamp("2024-05-01")],
            "open": [5.0],
            "high": [5.1],
            "low": [4.9],
            "close": [5.05],
            "volume": [500],
            "sequence": [0],
        }
    )
    df.to_parquet(legacy_dir / "ZZZ.parquet", index=False)

    checksum_values = iter(["legacy", "partition"])

    def fake_checksum(_frame: pd.DataFrame) -> str:
        try:
            return next(checksum_values)
        except StopIteration:
            return "final"

    monkeypatch.setattr(service, "_frame_checksum", fake_checksum)

    with pytest.raises(ValueError, match="Checksum mismatch"):
        service.migrate_interval("us:yahoo", "1m")


def test_estimate_disk_requirements_reports_sizes(monkeypatch, tmp_path: Path) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:40:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m"],
    )

    legacy_dir = tmp_path / "data/legacy/stocks_1m"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "AAA.parquet").write_bytes(b"x" * 2048)

    def fake_disk_usage(_path: Path) -> SimpleNamespace:
        return SimpleNamespace(total=10**12, used=0, free=10**12)

    monkeypatch.setattr(migration_module.shutil, "disk_usage", fake_disk_usage)

    estimate = service.estimate_disk_requirements(
        "us:yahoo",
        ["1m"],
        delete_legacy=False,
    )

    assert estimate["can_proceed"] is True
    intervals_payload = cast(dict[str, Any], estimate["intervals"])
    interval_payload = cast(dict[str, Any], intervals_payload["1m"])
    assert interval_payload["legacy_bytes"] == 2048
    totals = cast(dict[str, Any], estimate["totals"])
    assert totals["partition_bytes"] == 2048
    assert totals["required_partition_bytes"] >= 2048
    assert totals["delete_legacy"] is False
    available = totals["available_partition_bytes"]
    projected = totals["projected_free_after"]
    assert projected == available - totals["required_partition_bytes"]
    assert not estimate["suggest_delete_legacy"]


def test_estimate_disk_requirements_flags_low_space(
    monkeypatch, tmp_path: Path
) -> None:
    config = ConfigService(tmp_path)
    service = PartitionMigrationService(
        config,
        created_by="tests",
        now_provider=lambda: "2025-10-15T13:45:00Z",
    )

    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    service.initialize_plan(
        venue_id="us:yahoo",
        market="US",
        source="yahoo",
        intervals=["1m"],
    )

    legacy_dir = tmp_path / "data/legacy/stocks_1m"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "AAA.parquet").write_bytes(b"x" * 4096)

    def low_disk_usage(_path: Path) -> SimpleNamespace:
        return SimpleNamespace(total=10**6, used=9 * 10**5, free=512)

    monkeypatch.setattr(migration_module.shutil, "disk_usage", low_disk_usage)

    estimate = service.estimate_disk_requirements(
        "us:yahoo",
        ["1m"],
        delete_legacy=False,
    )

    assert estimate["can_proceed"] is False
    limitations = cast(list[str], estimate["limitations"])
    assert limitations
    assert "Partition root" in limitations[0]
    assert estimate["suggest_delete_legacy"] is True
    assert any("delete-legacy" in item for item in limitations)
