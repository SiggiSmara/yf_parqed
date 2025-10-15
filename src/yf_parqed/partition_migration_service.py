from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence, Tuple
import hashlib
import shutil

import pandas as pd

from .config_service import ConfigService
from .migration_plan import MigrationInterval, MigrationPlan, MigrationVenue
from .partition_path_builder import PartitionPathBuilder
from .partitioned_storage_backend import PartitionedStorageBackend
from .storage_backend import StorageBackend, StorageRequest

DATASET_NAME = "stocks"


def _default_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


class PartitionMigrationService:
    """Coordinate partition-migration state using the persisted migration plan."""

    def __init__(
        self,
        config_service: ConfigService,
        *,
        created_by: str = "yf_parqed-cli",
        now_provider: Callable[[], str] | None = None,
    ) -> None:
        self._config_service = config_service
        self._created_by = created_by
        self._now_provider = now_provider or _default_now
        self._legacy_backend = StorageBackend(
            empty_frame_factory=self._empty_price_frame,
            normalizer=self._normalize_price_frame,
            column_provider=self._price_frame_columns,
        )
        self._partition_backend = PartitionedStorageBackend(
            empty_frame_factory=self._empty_price_frame,
            normalizer=self._normalize_price_frame,
            column_provider=self._price_frame_columns,
            path_builder=PartitionPathBuilder(
                root=self._config_service.base_path / "data"
            ),
        )

    def _load_plan(self) -> MigrationPlan:
        return self._config_service.load_migration_plan()

    def get_interval_paths(self, venue_id: str, interval: str) -> Tuple[Path, Path]:
        plan = self._load_plan()
        base_path = self._config_service.base_path
        venue = plan.get_venue(venue_id)
        interval_state = venue.intervals[interval]
        legacy_path = interval_state.resolve_legacy_path(base_path)
        partition_path = interval_state.resolve_partition_path(base_path)
        return legacy_path, partition_path

    def update_interval(
        self,
        venue_id: str,
        interval: str,
        *,
        status: str | None = None,
        jobs_total: int | None = None,
        jobs_completed: int | None = None,
        legacy_rows: int | None = None,
        partition_rows: int | None = None,
        resume_token: str | None = None,
        verification_method: str | None = None,
        verified_at: str | None = None,
    ) -> MigrationInterval:
        plan = self._load_plan()
        timestamp = self._now_provider()
        interval_state = plan.update_interval(
            venue_id,
            interval,
            status=status,
            jobs_total=jobs_total,
            jobs_completed=jobs_completed,
            legacy_rows=legacy_rows,
            partition_rows=partition_rows,
            resume_token=resume_token,
            verification_method=verification_method,
            verified_at=verified_at,
            when=timestamp,
        )
        plan.write(generated_at=timestamp, created_by=self._created_by)
        return interval_state

    def initialize_plan(
        self,
        *,
        legacy_root: Path | None = None,
        venue_id: str,
        market: str,
        source: str,
        intervals: Sequence[str],
        overwrite: bool = False,
    ) -> MigrationPlan:
        plan_path = self._config_service.migration_plan_path
        if plan_path.exists() and not overwrite:
            raise FileExistsError(
                f"migration plan already exists at {plan_path}. Use --force to overwrite."
            )

        base_path = self._config_service.base_path
        expected_relative = Path("data/legacy")
        expected_absolute = (base_path / expected_relative).resolve()

        if legacy_root is None:
            legacy_root_relative = expected_relative
        else:
            provided_absolute = (
                legacy_root if legacy_root.is_absolute() else (base_path / legacy_root)
            ).resolve()
            if provided_absolute != expected_absolute:
                raise ValueError(
                    "Legacy root must be located at data/legacy relative to the workspace"
                )
            legacy_root_relative = expected_relative

        if not expected_absolute.exists():
            raise FileNotFoundError(
                f"Legacy path does not exist: {expected_absolute}. "
                "Place legacy parquet files under data/legacy before initializing the migration plan."
            )

        timestamp = self._now_provider()
        interval_entries = {}
        for interval in intervals:
            legacy_segment = Path("stocks_" + interval)
            interval_entries[interval] = {
                "legacy_path": str(legacy_root_relative / legacy_segment),
                "partition_path": str(
                    Path("data")
                    / market.lower()
                    / source.lower()
                    / f"{DATASET_NAME}_{interval}"
                ),
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

        plan_dict = {
            "schema_version": 1,
            "generated_at": timestamp,
            "created_by": self._created_by,
            "legacy_root": str(legacy_root_relative),
            "venues": [
                {
                    "id": venue_id,
                    "market": market,
                    "source": source,
                    "status": "pending",
                    "last_updated": timestamp,
                    "intervals": interval_entries,
                }
            ],
        }

        plan = MigrationPlan.from_dict(plan_dict)
        plan.write(path=plan_path, generated_at=timestamp, created_by=self._created_by)
        return plan

    def migrate_interval(
        self,
        venue_id: str,
        interval: str,
        *,
        delete_legacy: bool = False,
    ) -> dict[str, object]:
        plan = self._load_plan()
        base_path = self._config_service.base_path
        venue = plan.get_venue(venue_id)
        interval_state = venue.intervals[interval]
        legacy_root = plan.resolve_legacy_root(base_path)
        legacy_path = interval_state.resolve_legacy_path(base_path)
        partition_root = interval_state.resolve_partition_path(base_path)

        if not legacy_path.exists():
            raise FileNotFoundError(f"Legacy path does not exist: {legacy_path}")

        legacy_resolved = legacy_root.resolve()
        partition_resolved = partition_root.resolve()
        try:
            partition_resolved.relative_to(legacy_resolved)
        except ValueError:
            pass
        else:
            raise ValueError(
                "Partition path is inside legacy root; adjust migration plan before continuing"
            )

        ticker_files = sorted(legacy_path.glob("*.parquet"))
        total_jobs = len(ticker_files)
        timestamp = self._now_provider()
        plan.update_interval(
            venue_id,
            interval,
            status="migrating",
            jobs_total=total_jobs,
            jobs_completed=0,
            legacy_rows=0,
            partition_rows=0,
            when=timestamp,
        )
        plan.write(generated_at=timestamp, created_by=self._created_by)

        completed = 0
        total_legacy_rows = 0
        total_partition_rows = 0
        per_ticker_checksums: dict[str, str] = {}
        migrated_tickers: set[str] = set()

        for ticker_file in ticker_files:
            ticker = ticker_file.stem
            migrated_tickers.add(ticker)
            legacy_request = StorageRequest(
                root=legacy_root,
                interval=interval,
                ticker=ticker,
            )
            legacy_df = self._legacy_backend.read(legacy_request)
            partition_request = StorageRequest(
                root=base_path / "data",
                interval=interval,
                ticker=ticker,
                market=venue.market,
                source=venue.source,
                dataset=DATASET_NAME,
            )
            existing_partition = self._partition_backend.read(partition_request)
            combined = self._partition_backend.save(
                partition_request,
                new_data=legacy_df,
                existing_data=existing_partition,
            )

            legacy_rows = int(len(legacy_df))
            partition_rows = int(len(combined))
            if partition_rows != legacy_rows:
                raise ValueError(
                    "Row count mismatch for ticker "
                    f"{ticker}: legacy={legacy_rows}, partition={partition_rows}"
                )

            legacy_checksum = self._frame_checksum(legacy_df)
            partition_checksum = self._frame_checksum(combined)
            if legacy_checksum != partition_checksum:
                raise ValueError(
                    "Checksum mismatch for ticker "
                    f"{ticker}: legacy={legacy_checksum}, partition={partition_checksum}"
                )

            completed += 1
            total_legacy_rows += legacy_rows
            total_partition_rows += partition_rows
            per_ticker_checksums[ticker] = partition_checksum

            if delete_legacy and ticker_file.exists():
                try:
                    ticker_file.unlink(missing_ok=True)
                except TypeError:
                    if ticker_file.exists():
                        ticker_file.unlink()
                # attempt to clean empty parent
                if not any(ticker_file.parent.iterdir()):
                    try:
                        ticker_file.parent.rmdir()
                    except OSError:
                        pass

            timestamp = self._now_provider()
            plan.update_interval(
                venue_id,
                interval,
                jobs_completed=completed,
                legacy_rows=total_legacy_rows,
                partition_rows=total_partition_rows,
                when=timestamp,
            )
            plan.write(generated_at=timestamp, created_by=self._created_by)

        final_timestamp = self._now_provider()
        plan.update_interval(
            venue_id,
            interval,
            status="complete",
            jobs_completed=completed,
            legacy_rows=total_legacy_rows,
            partition_rows=total_partition_rows,
            verification_method="row_counts+checksum",
            verified_at=final_timestamp,
            when=final_timestamp,
        )
        plan.write(generated_at=final_timestamp, created_by=self._created_by)

        venue = plan.get_venue(venue_id)
        intervals_verified = self._all_intervals_verified(venue)

        if intervals_verified:
            try:
                self._activate_partitioned_storage(venue)
                self._backfill_ticker_storage_metadata(
                    venue=venue,
                    intervals=[interval],
                    verified_at=final_timestamp,
                )
            except Exception as exc:  # pylint: disable=broad-except
                raise RuntimeError(
                    "Failed to finalize partitioned storage activation"
                ) from exc
        else:
            # Update storage metadata for the interval that just completed so mixed-mode routing works.
            try:
                self._backfill_ticker_storage_metadata(
                    venue=venue,
                    intervals=[interval],
                    verified_at=final_timestamp,
                )
            except Exception as exc:  # pylint: disable=broad-except
                raise RuntimeError(
                    "Failed to backfill ticker metadata for migrated interval"
                ) from exc

        result: dict[str, object] = {
            "jobs_total": total_jobs,
            "jobs_completed": completed,
            "legacy_rows": total_legacy_rows,
            "partition_rows": total_partition_rows,
            "checksums": per_ticker_checksums,
            "tickers": sorted(migrated_tickers),
            "storage_activated": intervals_verified,
        }
        return result

    def estimate_disk_requirements(
        self,
        venue_id: str,
        intervals: Sequence[str],
        *,
        delete_legacy: bool,
    ) -> dict[str, object]:
        plan = self._load_plan()
        base_path = self._config_service.base_path
        venue = plan.get_venue(venue_id)

        per_interval: dict[str, dict[str, int]] = {}
        total_legacy_bytes = 0
        for interval_name in intervals:
            interval_state = venue.intervals[interval_name]
            legacy_path = interval_state.resolve_legacy_path(base_path)
            if not legacy_path.exists():
                raise FileNotFoundError(f"Legacy path does not exist: {legacy_path}")
            legacy_bytes = self._directory_size(legacy_path)
            per_interval[interval_name] = {"legacy_bytes": legacy_bytes}
            total_legacy_bytes += legacy_bytes

        partition_root = (base_path / "data").resolve()
        partition_usage_path = self._existing_path_for_usage(partition_root)
        partition_usage = shutil.disk_usage(partition_usage_path)

        partition_estimate = total_legacy_bytes
        overhead_bytes = int(partition_estimate * 0.05) if partition_estimate else 0
        required_partition_bytes = partition_estimate + overhead_bytes

        limitations: list[str] = []

        can_proceed = partition_usage.free >= required_partition_bytes

        if not can_proceed:
            needed = required_partition_bytes - partition_usage.free
            message = "Partition root lacks " f"{needed} additional bytes of free space"
            limitations.append(message)

        potential_with_delete = partition_usage.free + (
            total_legacy_bytes if not delete_legacy else 0
        )
        suggest_delete = False
        if (
            (not can_proceed)
            and (not delete_legacy)
            and potential_with_delete >= required_partition_bytes
        ):
            limitations.append(
                "Re-run with --delete-legacy to reclaim space from legacy parquet files before continuing."
            )
            suggest_delete = True

        return {
            "intervals": per_interval,
            "totals": {
                "legacy_bytes": total_legacy_bytes,
                "partition_bytes": partition_estimate,
                "overhead_bytes": overhead_bytes,
                "required_partition_bytes": required_partition_bytes,
                "available_partition_bytes": partition_usage.free,
                "partition_root": str(partition_usage_path),
                "delete_legacy": delete_legacy,
                "projected_free_after": partition_usage.free
                - required_partition_bytes
                + (total_legacy_bytes if delete_legacy else 0),
            },
            "can_proceed": can_proceed,
            "limitations": limitations,
            "suggest_delete_legacy": suggest_delete,
        }

    @staticmethod
    def _all_intervals_verified(venue: MigrationVenue) -> bool:
        return all(
            interval.status == "complete" and bool(interval.verification.verified_at)
            for interval in venue.intervals.values()
        )

    def _activate_partitioned_storage(self, venue: MigrationVenue) -> None:
        self._config_service.set_source_partition_mode(
            venue.market,
            venue.source,
            True,
        )

    def _collect_partitioned_tickers_for_interval(
        self,
        venue: MigrationVenue,
        interval: str,
    ) -> set[str]:
        data_root = self._config_service.base_path / "data"
        market_segment = venue.market.strip().lower()
        source_segment = venue.source.strip().lower()
        interval_root = (
            data_root / market_segment / source_segment / f"{DATASET_NAME}_{interval}"
        )
        if not interval_root.exists():
            return set()

        discovered: set[str] = set()
        for ticker_dir in interval_root.glob("ticker=*"):
            if ticker_dir.is_dir():
                _, _, ticker_value = ticker_dir.name.partition("=")
                if ticker_value:
                    discovered.add(ticker_value)
        return discovered

    def _backfill_ticker_storage_metadata(
        self,
        *,
        venue: MigrationVenue,
        intervals: Sequence[str],
        verified_at: str,
    ) -> None:
        tickers_data = self._config_service.load_tickers()
        if not tickers_data:
            return

        normalized_market = venue.market.strip().lower()
        normalized_source = venue.source.strip().lower()
        root_token = "data"
        dataset = DATASET_NAME
        normalized_intervals = [interval.strip() for interval in intervals if interval]

        changed = False
        for interval_name in normalized_intervals:
            if not interval_name:
                continue
            tickers = self._collect_partitioned_tickers_for_interval(
                venue, interval_name
            )
            if not tickers:
                continue

            for ticker in tickers:
                entry = tickers_data.get(ticker)
                if not isinstance(entry, dict):
                    continue

                interval_map = entry.setdefault("intervals", {})
                interval_entry = interval_map.setdefault(interval_name, {})
                storage = interval_entry.setdefault("storage", {})

                entry_changed = False
                if storage.get("mode") != "partitioned":
                    storage["mode"] = "partitioned"
                    entry_changed = True
                if storage.get("venue") != venue.id:
                    storage["venue"] = venue.id
                    entry_changed = True
                if storage.get("market") != normalized_market:
                    storage["market"] = normalized_market
                    entry_changed = True
                if storage.get("source") != normalized_source:
                    storage["source"] = normalized_source
                    entry_changed = True
                if storage.get("dataset") != dataset:
                    storage["dataset"] = dataset
                    entry_changed = True
                if storage.get("root") != root_token:
                    storage["root"] = root_token
                    entry_changed = True
                if storage.get("verified_at") != verified_at:
                    storage["verified_at"] = verified_at
                    entry_changed = True

                if entry_changed:
                    changed = True

        if changed:
            self._config_service.save_tickers(tickers_data)

    @staticmethod
    def _existing_path_for_usage(path: Path) -> Path:
        current = path
        while not current.exists():
            if current.parent == current:
                raise FileNotFoundError(
                    f"Unable to determine disk usage for path {path}"
                )
            current = current.parent
        return current

    @staticmethod
    def _directory_size(path: Path) -> int:
        total = 0
        for child in path.rglob("*"):
            if child.is_file():
                total += child.stat().st_size
        return total

    @staticmethod
    def _frame_checksum(frame: pd.DataFrame) -> str:
        if frame.empty:
            return "empty"
        ordered = (
            frame.reset_index()
            .sort_values(["stock", "date"], kind="mergesort")
            .reset_index(drop=True)
        )
        hashed = pd.util.hash_pandas_object(ordered, index=False)
        hashed_array = hashed.to_numpy(dtype="uint64", copy=False)
        return hashlib.sha256(hashed_array.tobytes()).hexdigest()

    @staticmethod
    def _price_frame_columns() -> list[str]:
        return [
            "stock",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "sequence",
        ]

    @staticmethod
    def _empty_price_frame() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "stock": pd.Series(dtype="string"),
                "date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="Int64"),
                "sequence": pd.Series(dtype="Int64"),
            }
        ).set_index(["stock", "date"])

    @classmethod
    def _normalize_price_frame(cls, df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = cls._price_frame_columns()
        normalized = df.copy()

        for column in expected_cols:
            if column not in normalized.columns:
                if column in {"open", "high", "low", "close"}:
                    normalized[column] = pd.Series(dtype="float64")
                elif column in {"volume", "sequence"}:
                    normalized[column] = pd.Series(dtype="Int64")
                elif column == "date":
                    normalized[column] = pd.Series(dtype="datetime64[ns]")
                else:
                    normalized[column] = pd.Series(dtype="string")

        normalized["stock"] = normalized["stock"].astype("string")
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")

        for price_col in ["open", "high", "low", "close"]:
            normalized[price_col] = pd.to_numeric(
                normalized[price_col], errors="coerce"
            ).astype("float64")

        for int_col in ["volume", "sequence"]:
            numeric_series = pd.to_numeric(normalized[int_col], errors="coerce")
            normalized[int_col] = numeric_series.round().astype("Int64")

        normalized = normalized[expected_cols]
        return normalized
