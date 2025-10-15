from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass
class IntervalTotals:
    legacy_rows: int | None
    partition_rows: int | None

    def to_dict(self) -> dict[str, int | None]:
        return {
            "legacy_rows": self.legacy_rows,
            "partition_rows": self.partition_rows,
        }


@dataclass
class IntervalJobs:
    total: int
    completed: int

    def to_dict(self) -> dict[str, int]:
        return {
            "total": self.total,
            "completed": self.completed,
        }


@dataclass
class IntervalVerification:
    method: str
    verified_at: str | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "method": self.method,
            "verified_at": self.verified_at,
        }


@dataclass
class IntervalBackup:
    path: Path
    created_at: str
    size_bytes: int | None
    verified: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "created_at": self.created_at,
            "size_bytes": self.size_bytes,
            "verified": self.verified,
        }


@dataclass
class MigrationInterval:
    legacy_path: Path
    partition_path: Path
    status: str
    totals: IntervalTotals
    jobs: IntervalJobs
    resume_token: str | None
    verification: IntervalVerification
    backups: list[IntervalBackup]

    def resolve_legacy_path(self, base_path: Path | None = None) -> Path:
        return _resolve_path(self.legacy_path, base_path)

    def resolve_partition_path(self, base_path: Path | None = None) -> Path:
        return _resolve_path(self.partition_path, base_path)

    def to_dict(self) -> dict[str, Any]:
        return {
            "legacy_path": str(self.legacy_path),
            "partition_path": str(self.partition_path),
            "status": self.status,
            "totals": self.totals.to_dict(),
            "jobs": self.jobs.to_dict(),
            "resume_token": self.resume_token,
            "verification": self.verification.to_dict(),
            "backups": [backup.to_dict() for backup in self.backups],
        }


@dataclass
class MigrationVenue:
    id: str
    market: str
    source: str
    status: str
    last_updated: str
    intervals: Dict[str, MigrationInterval]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "market": self.market,
            "source": self.source,
            "status": self.status,
            "last_updated": self.last_updated,
            "intervals": {
                key: interval.to_dict() for key, interval in self.intervals.items()
            },
        }


@dataclass
class MigrationPlan:
    schema_version: int
    generated_at: str
    created_by: str
    legacy_root: Path
    venues: Dict[str, MigrationVenue]
    _plan_path: Path | None = None

    @classmethod
    def from_file(cls, path: Path) -> "MigrationPlan":
        data = json.loads(path.read_text())
        plan = cls.from_dict(data)
        plan._plan_path = Path(path)
        return plan

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationPlan":
        schema_version = data.get("schema_version")
        if schema_version != 1:
            raise ValueError(f"schema_version {schema_version} is not supported")

        legacy_root = data.get("legacy_root")
        if not legacy_root:
            raise ValueError("legacy_root is required")

        venues_data = data.get("venues")
        if not isinstance(venues_data, list):
            raise ValueError("venues must be a list")

        venues: Dict[str, MigrationVenue] = {}
        for venue_data in venues_data:
            venue = _parse_venue(venue_data)
            venues[venue.id] = venue

        return cls(
            schema_version=1,
            generated_at=data.get("generated_at", ""),
            created_by=data.get("created_by", ""),
            legacy_root=Path(legacy_root),
            venues=venues,
            _plan_path=None,
        )

    def get_venue(self, venue_id: str) -> MigrationVenue:
        return self.venues[venue_id]

    def resolve_legacy_root(self, base_path: Path | None = None) -> Path:
        return _resolve_path(self.legacy_root, base_path)

    def update_interval(
        self,
        venue_id: str,
        interval_key: str,
        *,
        status: str | None = None,
        jobs_total: int | None = None,
        jobs_completed: int | None = None,
        legacy_rows: int | None = None,
        partition_rows: int | None = None,
        resume_token: object = None,
        verification_method: str | None = None,
        verified_at: str | None = None,
        when: str | None = None,
    ) -> MigrationInterval:
        venue = self.get_venue(venue_id)
        if interval_key not in venue.intervals:
            raise KeyError(f"interval {interval_key} not found for venue {venue_id}")
        interval = venue.intervals[interval_key]

        if status is not None:
            interval.status = status
        if jobs_total is not None:
            interval.jobs.total = jobs_total
        if jobs_completed is not None:
            interval.jobs.completed = jobs_completed
        if legacy_rows is not None:
            interval.totals.legacy_rows = legacy_rows
        if partition_rows is not None:
            interval.totals.partition_rows = partition_rows
        if resume_token is not None:
            interval.resume_token = resume_token  # type: ignore[assignment]
        if verification_method is not None:
            interval.verification.method = verification_method
        if verified_at is not None:
            interval.verification.verified_at = verified_at
        if when is not None:
            venue.last_updated = when
            self.generated_at = when

        return interval

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "created_by": self.created_by,
            "legacy_root": str(self.legacy_root),
            "venues": [venue.to_dict() for venue in self.venues.values()],
        }

    def write(
        self,
        path: Path | None = None,
        *,
        generated_at: str | None = None,
        created_by: str | None = None,
    ) -> Path:
        target = Path(path) if path is not None else self._plan_path
        if target is None:
            raise ValueError("No target path specified for writing migration plan")

        if generated_at is not None:
            self.generated_at = generated_at
        if created_by is not None:
            self.created_by = created_by

        target.write_text(json.dumps(self.to_dict(), indent=4))
        self._plan_path = target
        return target


def _parse_venue(data: Dict[str, Any]) -> MigrationVenue:
    venue_id = data.get("id")
    if not venue_id:
        raise ValueError("venue id is required")

    intervals_data = data.get("intervals")
    if not isinstance(intervals_data, dict):
        raise ValueError("intervals must be a mapping")

    intervals: Dict[str, MigrationInterval] = {}
    for interval_key, interval_value in intervals_data.items():
        intervals[interval_key] = _parse_interval(interval_key, interval_value)

    return MigrationVenue(
        id=venue_id,
        market=data.get("market", ""),
        source=data.get("source", ""),
        status=data.get("status", ""),
        last_updated=data.get("last_updated", ""),
        intervals=intervals,
    )


def _require_field(data: Dict[str, Any], field: str, interval_key: str) -> Any:
    if field not in data:
        raise ValueError(f"{field} is required for interval {interval_key}")
    return data[field]


def _parse_interval(interval_key: str, data: Dict[str, Any]) -> MigrationInterval:
    if not isinstance(data, dict):
        raise ValueError(f"interval {interval_key} must be a mapping")

    legacy_path = Path(_require_field(data, "legacy_path", interval_key))
    partition_path = Path(_require_field(data, "partition_path", interval_key))
    status = _require_field(data, "status", interval_key)

    totals_data = _require_field(data, "totals", interval_key)
    if not isinstance(totals_data, dict):
        raise ValueError(f"totals must be a mapping for interval {interval_key}")
    totals = IntervalTotals(
        legacy_rows=totals_data.get("legacy_rows"),
        partition_rows=totals_data.get("partition_rows"),
    )

    jobs_data = _require_field(data, "jobs", interval_key)
    if not isinstance(jobs_data, dict):
        raise ValueError(f"jobs must be a mapping for interval {interval_key}")
    jobs = IntervalJobs(
        total=int(jobs_data.get("total", 0)),
        completed=int(jobs_data.get("completed", 0)),
    )

    verification_data = _require_field(data, "verification", interval_key)
    if not isinstance(verification_data, dict):
        raise ValueError(f"verification must be a mapping for interval {interval_key}")
    verification = IntervalVerification(
        method=verification_data.get("method", ""),
        verified_at=verification_data.get("verified_at"),
    )

    backups_list = data.get("backups", [])
    backups: list[IntervalBackup] = []
    if not isinstance(backups_list, list):
        raise ValueError(f"backups must be a list for interval {interval_key}")
    for backup in backups_list:
        if not isinstance(backup, dict):
            raise ValueError(
                f"backups must contain mappings for interval {interval_key}"
            )
        backups.append(
            IntervalBackup(
                path=Path(backup.get("path", "")),
                created_at=backup.get("created_at", ""),
                size_bytes=backup.get("size_bytes"),
                verified=bool(backup.get("verified", False)),
            )
        )

    return MigrationInterval(
        legacy_path=legacy_path,
        partition_path=partition_path,
        status=status,
        totals=totals,
        jobs=jobs,
        resume_token=data.get("resume_token"),
        verification=verification,
        backups=backups,
    )


def _resolve_path(path: Path, base_path: Path | None) -> Path:
    if path.is_absolute():
        return path
    base = base_path if base_path is not None else Path.cwd()
    return base / path
