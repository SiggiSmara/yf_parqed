from __future__ import annotations

import json
import copy
from pathlib import Path
from typing import Sequence

import pandas as pd
from typer.testing import CliRunner

from yf_parqed.tools import partition_migrate
from yf_parqed.config_service import ConfigService
from yf_parqed.migration_plan import MigrationPlan


class DummyInterval:
    def __init__(
        self,
        status: str = "pending",
        jobs_total: int = 0,
        jobs_completed: int = 0,
        legacy_rows: int | None = None,
        partition_rows: int | None = None,
        resume_token: str | None = None,
    ) -> None:
        self.status = status
        self.jobs = type(
            "Jobs", (), {"total": jobs_total, "completed": jobs_completed}
        )()
        self.totals = type(
            "Totals",
            (),
            {"legacy_rows": legacy_rows, "partition_rows": partition_rows},
        )()
        self.resume_token = resume_token


class DummyVenue:
    def __init__(self, venue_id: str, intervals: dict[str, DummyInterval]) -> None:
        self.id = venue_id
        self.intervals = intervals


class DummyPlan:
    def __init__(self, venues: dict[str, DummyVenue]) -> None:
        self.venues = venues


class StubMigrationService:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.migrate_return = {
            "jobs_total": 0,
            "jobs_completed": 0,
            "legacy_rows": 0,
            "partition_rows": 0,
            "checksums": {},
            "available_jobs": 0,
            "persisted": True,
            "partial_run": False,
            "tickers": [],
        }
        self.estimate_return = {
            "intervals": {"1m": {"legacy_bytes": 1024}},
            "totals": {
                "legacy_bytes": 1024,
                "partition_bytes": 1024,
                "overhead_bytes": 0,
                "required_partition_bytes": 1024,
                "available_partition_bytes": 10**9,
                "partition_root": ".",
                "delete_legacy": False,
                "projected_free_after": 10**9 - 1024,
            },
            "can_proceed": True,
            "limitations": [],
            "suggest_delete_legacy": False,
        }

    def update_interval(self, venue: str, interval: str, **kwargs):
        self.calls.append(("update", venue, interval, kwargs))
        return DummyInterval(status=kwargs.get("status", "pending"))

    def estimate_disk_requirements(
        self,
        venue: str,
        intervals: Sequence[str],
        *,
        delete_legacy: bool,
    ):
        self.calls.append(("estimate", venue, tuple(intervals), delete_legacy))
        payload = copy.deepcopy(self.estimate_return)
        interval_list = list(intervals)
        per_interval = {name: {"legacy_bytes": 1024} for name in interval_list}
        payload["intervals"] = per_interval
        totals = payload.get("totals")
        if isinstance(totals, dict):
            total_legacy = 1024 * len(interval_list)
            totals["legacy_bytes"] = total_legacy
            totals["partition_bytes"] = total_legacy
            totals["delete_legacy"] = delete_legacy
            totals["projected_free_after"] = (
                totals["available_partition_bytes"]
                - totals["required_partition_bytes"]
                + (total_legacy if delete_legacy else 0)
            )
        return payload

    def migrate_interval(
        self,
        venue: str,
        interval: str,
        *,
        delete_legacy: bool = False,
        max_tickers: int | None = None,
        overwrite_existing: bool = False,
    ):
        self.calls.append(("migrate", venue, interval, delete_legacy, max_tickers))
        return self.migrate_return


runner = CliRunner()


def _single_interval_plan() -> MigrationPlan:
    data = {
        "schema_version": 1,
        "generated_at": "2025-10-15T12:40:00Z",
        "created_by": "tests",
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
    return MigrationPlan.from_dict(data)


def _multi_interval_plan(statuses: dict[str, str]) -> MigrationPlan:
    intervals: dict[str, dict] = {}
    for key, status in statuses.items():
        intervals[key] = {
            "legacy_path": f"data/legacy/stocks_{key}",
            "partition_path": f"data/us/yahoo/stocks_{key}",
            "status": status,
            "totals": {"legacy_rows": None, "partition_rows": None},
            "jobs": {"total": 0, "completed": 0},
            "resume_token": None,
            "verification": {"method": "row_counts", "verified_at": None},
            "backups": [],
        }

    data = {
        "schema_version": 1,
        "generated_at": "2025-10-15T12:40:00Z",
        "created_by": "tests",
        "legacy_root": "data/legacy",
        "venues": [
            {
                "id": "us:yahoo",
                "market": "US",
                "source": "yahoo",
                "status": "pending",
                "last_updated": "2025-10-15T12:40:00Z",
                "intervals": intervals,
            }
        ],
    }
    return MigrationPlan.from_dict(data)


def test_status_command_prints_table(monkeypatch, tmp_path: Path) -> None:
    plan = DummyPlan(
        venues={
            "us:yahoo": DummyVenue(
                "us:yahoo",
                {
                    "1m": DummyInterval(
                        status="in_progress",
                        jobs_total=10,
                        jobs_completed=4,
                        legacy_rows=1200,
                        partition_rows=600,
                        resume_token="batch-1",
                    )
                },
            )
        }
    )

    monkeypatch.setattr(partition_migrate, "_load_plan", lambda base_dir: plan)

    result = runner.invoke(
        partition_migrate.app,
        ["status", "--base-dir", str(tmp_path)],
    )

    assert result.exit_code == 0
    assert "us:yahoo" in result.stdout
    assert "in_progr" in result.stdout
    assert "batch-1" in result.stdout


def test_mark_command_updates_service(monkeypatch, tmp_path: Path) -> None:
    stub_service = StubMigrationService()
    monkeypatch.setattr(
        partition_migrate,
        "_load_service",
        lambda base, created_by, **kwargs: stub_service,
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "mark",
            "us:yahoo",
            "1m",
            "--status",
            "verified",
            "--jobs-total",
            "12",
            "--jobs-completed",
            "12",
            "--legacy-rows",
            "1500",
            "--partition-rows",
            "1500",
            "--resume-token",
            "final",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert stub_service.calls
    _, venue, interval, kwargs = stub_service.calls[-1]
    assert venue == "us:yahoo"
    assert interval == "1m"
    assert kwargs["status"] == "verified"
    assert kwargs["jobs_total"] == 12
    assert kwargs["jobs_completed"] == 12
    assert kwargs["legacy_rows"] == 1500
    assert kwargs["partition_rows"] == 1500
    assert kwargs["resume_token"] == "final"
    assert "verified" in result.stdout


def test_init_command_creates_plan(tmp_path: Path) -> None:
    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    result = runner.invoke(
        partition_migrate.app,
        [
            "init",
            "--base-dir",
            str(tmp_path),
            "--interval",
            "1m",
        ],
    )

    assert result.exit_code == 0
    plan_path = tmp_path / "migration_plan.json"
    assert plan_path.exists()
    plan_data = json.loads(plan_path.read_text())
    assert plan_data["legacy_root"] == "data/legacy"
    assert plan_data["venues"][0]["id"] == "us:yahoo"
    assert plan_data["venues"][0]["market"] == "US"
    assert plan_data["venues"][0]["source"] == "yahoo"


def test_migrate_command_invokes_service(monkeypatch, tmp_path: Path) -> None:
    stub_service = StubMigrationService()
    stub_service.migrate_return = {
        "jobs_total": 1,
        "jobs_completed": 1,
        "legacy_rows": 2,
        "partition_rows": 2,
        "checksums": {"AAA": "digest"},
    }
    monkeypatch.setattr(
        partition_migrate,
        "_load_service",
        lambda base, created_by, **kwargs: stub_service,
    )
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert "Migration complete" in result.stdout
    assert ("estimate", "us:yahoo", ("1m",), False) in stub_service.calls
    assert ("migrate", "us:yahoo", "1m", False, None) in stub_service.calls


def test_migrate_command_with_delete(monkeypatch, tmp_path: Path) -> None:
    stub_service = StubMigrationService()
    monkeypatch.setattr(
        partition_migrate,
        "_load_service",
        lambda base, created_by, **kwargs: stub_service,
    )
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--delete-legacy",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert ("estimate", "us:yahoo", ("1m",), True) in stub_service.calls
    assert ("migrate", "us:yahoo", "1m", True, None) in stub_service.calls


def test_migrate_command_with_max_tickers(monkeypatch, tmp_path: Path) -> None:
    stub_service = StubMigrationService()
    stub_service.migrate_return = {
        "jobs_total": 5,
        "jobs_completed": 5,
        "legacy_rows": 100,
        "partition_rows": 100,
        "checksums": {},
        "available_jobs": 20,
        "persisted": False,
        "partial_run": True,
        "tickers": ["AAA", "BBB"],
    }
    monkeypatch.setattr(
        partition_migrate,
        "_load_service",
        lambda base, created_by, **kwargs: stub_service,
    )
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--max-tickers",
            "5",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert ("estimate", "us:yahoo", ("1m",), False) in stub_service.calls
    assert ("migrate", "us:yahoo", "1m", False, 5) in stub_service.calls
    assert "Partial run" in result.stdout


def test_migrate_command_with_no_compression(monkeypatch, tmp_path: Path) -> None:
    stub_service = StubMigrationService()
    compression_args: list[str | None] = []

    def _fake_load_service(base, created_by, **kwargs):
        compression_args.append(kwargs.get("compression"))
        return stub_service

    monkeypatch.setattr(partition_migrate, "_load_service", _fake_load_service)
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--base-dir",
            str(tmp_path),
            "--compression",
            "none",
        ],
    )

    assert result.exit_code == 0
    assert compression_args == [None]
    assert "Compression disabled" in result.stdout


def test_migrate_command_blocks_when_estimate_fails(
    monkeypatch, tmp_path: Path
) -> None:
    stub_service = StubMigrationService()
    stub_service.estimate_return["can_proceed"] = False
    stub_service.estimate_return["limitations"] = [
        "Partition root lacks 10 bytes of free space"
    ]
    monkeypatch.setattr(
        partition_migrate,
        "_load_service",
        lambda base, created_by, **kwargs: stub_service,
    )
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "Insufficient disk space" in result.stdout
    migrate_calls = [call for call in stub_service.calls if call[0] == "migrate"]
    assert not migrate_calls


def test_cli_migrate_end_to_end(tmp_path: Path) -> None:
    (tmp_path / "data/legacy").mkdir(parents=True, exist_ok=True)
    legacy_root = tmp_path / "data/legacy/stocks_1m"
    legacy_root.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "stock": ["AAA", "AAA"],
            "date": [pd.Timestamp("2024-03-01"), pd.Timestamp("2024-04-01")],
            "open": [10.0, 10.5],
            "high": [10.2, 10.7],
            "low": [9.8, 10.1],
            "close": [10.1, 10.6],
            "volume": [1000, 1200],
            "sequence": [0, 1],
        }
    )
    df.to_parquet(legacy_root / "AAA.parquet", index=False)

    init_result = runner.invoke(
        partition_migrate.app,
        [
            "init",
            "--base-dir",
            str(tmp_path),
            "--interval",
            "1m",
        ],
    )

    assert init_result.exit_code == 0

    migrate_result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert migrate_result.exit_code == 0
    assert "Migration complete" in migrate_result.stdout

    config = ConfigService(tmp_path)
    plan = config.load_migration_plan()
    interval = plan.get_venue("us:yahoo").intervals["1m"]
    assert interval.status == "complete"
    assert interval.jobs.completed == 1
    assert interval.totals.partition_rows == 2
    assert not interval.backups

    partition_file = (
        tmp_path / "data/us/yahoo/stocks_1m/ticker=AAA/year=2024/month=03/data.parquet"
    )
    partition_file_day2 = (
        tmp_path / "data/us/yahoo/stocks_1m/ticker=AAA/year=2024/month=04/data.parquet"
    )
    assert partition_file.exists()
    assert partition_file_day2.exists()
    # Legacy remains since we did not pass --delete-legacy
    assert (legacy_root / "AAA.parquet").exists()
    assert not (tmp_path / "custom_backups").exists()


def test_fast_flag_enables_defaults(monkeypatch, tmp_path: Path) -> None:
    """Ensure --fast sets overwrite_existing, disables fsync and sets row_group_size to 65536."""
    captured: dict = {"load_calls": [], "migrate_calls": []}

    class FakeService:
        def __init__(self):
            pass

        def estimate_disk_requirements(self, venue, intervals, *, delete_legacy):
            captured["load_calls"].append((venue, tuple(intervals), delete_legacy))
            return {
                "intervals": {name: {"legacy_bytes": 1024} for name in intervals},
                "totals": {
                    "legacy_bytes": 1024 * len(list(intervals)),
                    "partition_bytes": 1024 * len(list(intervals)),
                    "overhead_bytes": 0,
                    "required_partition_bytes": 1024 * len(list(intervals)),
                    "available_partition_bytes": 10**9,
                    "partition_root": ".",
                    "delete_legacy": False,
                    "projected_free_after": 10**9,
                },
                "can_proceed": True,
                "limitations": [],
                "suggest_delete_legacy": False,
            }

        def migrate_interval(
            self,
            venue,
            interval,
            *,
            delete_legacy=False,
            max_tickers=None,
            overwrite_existing=False,
        ):
            captured["migrate_calls"].append(
                (venue, interval, delete_legacy, max_tickers, overwrite_existing)
            )
            return {
                "jobs_total": 0,
                "jobs_completed": 0,
                "legacy_rows": 0,
                "partition_rows": 0,
                "checksums": {},
                "available_jobs": 0,
                "persisted": True,
                "partial_run": False,
                "tickers": [],
            }

    def _fake_load_service(base, created_by, **kwargs):
        # record the kwargs passed when creating service
        captured["service_kwargs"] = kwargs
        return FakeService()

    monkeypatch.setattr(partition_migrate, "_load_service", _fake_load_service)
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        ["migrate", "--base-dir", str(tmp_path), "--fast", "--max-tickers", "1"],
    )
    assert result.exit_code == 0
    # service should have been created with fsync=False and row_group_size=65536
    assert captured.get("service_kwargs") is not None
    assert captured["service_kwargs"].get("fsync") is False
    assert captured["service_kwargs"].get("row_group_size") == 65536
    # migrate_interval should have been called with overwrite_existing=True
    assert captured["migrate_calls"]
    _, _, _, _, overwrite_flag = captured["migrate_calls"][0]
    assert overwrite_flag is True


def test_explicit_nofsync_and_row_group_flags(monkeypatch, tmp_path: Path) -> None:
    """Ensure explicit --no-fsync and --row-group-size values are forwarded to the service."""
    captured_kwargs = {}

    def _fake_load_service(base, created_by, **kwargs):
        captured_kwargs.update(kwargs)

        class FakeStubService:
            def estimate_disk_requirements(
                self, venue, intervals, *, delete_legacy=False
            ):
                interval_list = list(intervals)
                return {
                    "intervals": {
                        name: {"legacy_bytes": 1024} for name in interval_list
                    },
                    "totals": {
                        "legacy_bytes": 1024 * len(interval_list),
                        "partition_bytes": 1024 * len(interval_list),
                        "overhead_bytes": 0,
                        "required_partition_bytes": 1024 * len(interval_list),
                        "available_partition_bytes": 10**9,
                        "partition_root": ".",
                        "delete_legacy": delete_legacy,
                        "projected_free_after": 10**9,
                    },
                    "can_proceed": True,
                    "limitations": [],
                    "suggest_delete_legacy": False,
                }

            def migrate_interval(
                self,
                venue,
                interval,
                *,
                delete_legacy=False,
                max_tickers=None,
                overwrite_existing=False,
            ):
                return StubMigrationService().migrate_interval(
                    venue,
                    interval,
                    delete_legacy=delete_legacy,
                    max_tickers=max_tickers,
                )

        return FakeStubService()

    monkeypatch.setattr(partition_migrate, "_load_service", _fake_load_service)
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--base-dir",
            str(tmp_path),
            "--no-fsync",
            "--row-group-size",
            "131072",
        ],
    )

    assert result.exit_code == 0
    assert captured_kwargs.get("fsync") is False
    assert captured_kwargs.get("row_group_size") == 131072


def test_migrate_prompt_accepts_interval_name(monkeypatch, tmp_path: Path) -> None:
    stub_service = StubMigrationService()
    monkeypatch.setattr(
        partition_migrate,
        "_load_service",
        lambda base, created_by, **kwargs: stub_service,
    )
    monkeypatch.setattr(
        partition_migrate,
        "_load_plan",
        lambda base: _multi_interval_plan({"1m": "pending", "1h": "pending"}),
    )
    monkeypatch.setattr(partition_migrate.typer, "prompt", lambda *args, **kwargs: "1h")

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert ("estimate", "us:yahoo", ("1h",), False) in stub_service.calls
    assert ("migrate", "us:yahoo", "1h", False, None) in stub_service.calls


def test_migrate_all_processes_each_pending(monkeypatch, tmp_path: Path) -> None:
    stub_service = StubMigrationService()
    monkeypatch.setattr(
        partition_migrate,
        "_load_service",
        lambda base, created_by, **kwargs: stub_service,
    )
    plan = _multi_interval_plan({"1m": "pending", "1h": "pending", "1d": "complete"})
    monkeypatch.setattr(partition_migrate, "_load_plan", lambda base: plan)

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--all",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    calls = [call for call in stub_service.calls if call[0] == "migrate"]
    assert calls == [
        ("migrate", "us:yahoo", "1m", False, None),
        ("migrate", "us:yahoo", "1h", False, None),
    ]
    assert ("estimate", "us:yahoo", ("1m", "1h"), False) in stub_service.calls
    assert "All requested intervals migrated successfully" in result.stdout


def test_migrate_all_errors_when_none_pending(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        partition_migrate,
        "_load_plan",
        lambda base: _multi_interval_plan({"1m": "complete"}),
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "--all",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "All intervals already migrated" in result.stdout


def test_migrate_interval_and_all_conflict(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        partition_migrate, "_load_plan", lambda base: _single_interval_plan()
    )

    result = runner.invoke(
        partition_migrate.app,
        [
            "migrate",
            "us:yahoo",
            "1m",
            "--all",
            "--base-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "Specify either an interval or --all" in result.stdout
