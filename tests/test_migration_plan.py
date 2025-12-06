from __future__ import annotations

import json
from pathlib import Path

import pytest

from yf_parqed.common.migration_plan import MigrationPlan


def _write_plan(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "migration_plan.json"
    path.write_text(json.dumps(data, indent=4))
    return path


def _valid_plan_dict() -> dict:
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


def test_load_plan_success(tmp_path: Path) -> None:
    plan_path = _write_plan(tmp_path, _valid_plan_dict())

    plan = MigrationPlan.from_file(plan_path)

    assert plan.schema_version == 1
    assert plan.legacy_root == Path("data/legacy")
    venue = plan.get_venue("us:yahoo")
    assert venue.id == "us:yahoo"
    interval = venue.intervals["1m"]
    assert interval.status == "pending"
    assert interval.jobs.total == 0


def test_load_plan_requires_schema_version_one(tmp_path: Path) -> None:
    data = _valid_plan_dict()
    data["schema_version"] = 2
    plan_path = _write_plan(tmp_path, data)

    with pytest.raises(ValueError, match="schema_version 2 is not supported"):
        MigrationPlan.from_file(plan_path)


def test_load_plan_validates_required_interval_fields(tmp_path: Path) -> None:
    data = _valid_plan_dict()
    del data["venues"][0]["intervals"]["1m"]["legacy_path"]
    plan_path = _write_plan(tmp_path, data)

    with pytest.raises(ValueError, match="legacy_path is required"):
        MigrationPlan.from_file(plan_path)


def test_plan_resolves_relative_paths(tmp_path: Path) -> None:
    plan = MigrationPlan.from_dict(_valid_plan_dict())

    assert plan.resolve_legacy_root(tmp_path) == tmp_path / "data/legacy"

    venue = plan.get_venue("us:yahoo")
    interval = venue.intervals["1m"]
    assert interval.resolve_legacy_path(tmp_path) == tmp_path / "data/legacy/stocks_1m"
    assert (
        interval.resolve_partition_path(tmp_path)
        == tmp_path / "data/us/yahoo/stocks_1m"
    )


def test_update_interval_status(tmp_path: Path) -> None:
    plan = MigrationPlan.from_dict(_valid_plan_dict())

    plan.update_interval(
        "us:yahoo",
        "1m",
        status="migrating",
        jobs_total=10,
        jobs_completed=4,
        legacy_rows=1000,
        partition_rows=500,
        resume_token="batch-1",
        verification_method="row_counts",
        verified_at="2025-10-15T13:00:00Z",
        when="2025-10-15T13:00:00Z",
    )

    venue = plan.get_venue("us:yahoo")
    assert venue.last_updated == "2025-10-15T13:00:00Z"
    interval = venue.intervals["1m"]
    assert interval.status == "migrating"
    assert interval.jobs.total == 10
    assert interval.jobs.completed == 4
    assert interval.totals.legacy_rows == 1000
    assert interval.totals.partition_rows == 500
    assert interval.resume_token == "batch-1"
    assert interval.verification.method == "row_counts"
    assert interval.verification.verified_at == "2025-10-15T13:00:00Z"
    assert plan.generated_at == "2025-10-15T13:00:00Z"


def test_plan_write_round_trip(tmp_path: Path) -> None:
    plan = MigrationPlan.from_dict(_valid_plan_dict())
    plan.update_interval(
        "us:yahoo", "1m", status="complete", when="2025-10-15T14:00:00Z"
    )

    target = tmp_path / "plan.json"
    plan.write(target, generated_at="2025-10-15T14:30:00Z", created_by="cli@test")

    loaded = MigrationPlan.from_file(target)
    assert loaded.generated_at == "2025-10-15T14:30:00Z"
    assert loaded.created_by == "cli@test"
    assert loaded.get_venue("us:yahoo").intervals["1m"].status == "complete"
