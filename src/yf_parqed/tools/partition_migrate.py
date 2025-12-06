from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Sequence, Tuple

import typer
from rich.console import Console
from rich.table import Table
from loguru import logger

from ..common.config_service import ConfigService
from ..common.migration_plan import MigrationPlan
from ..partition_migration_service import PartitionMigrationService
from ..common.run_lock import GlobalRunLock

app = typer.Typer(help="Partition storage migration utilities")
console = Console()
_FILE_SINK_ID: int | None = None


def _load_service(
    base_dir: Path,
    created_by: str,
    *,
    compression: str | None = "gzip",
    fsync: bool = True,
    row_group_size: int | None = None,
) -> PartitionMigrationService:
    config = ConfigService(base_dir)
    return PartitionMigrationService(
        config,
        created_by=created_by,
        compression=compression,
        fsync=fsync,
        row_group_size=row_group_size,
    )


def _configure_logging(base_dir: Path, log_file: Optional[Path]) -> None:
    global _FILE_SINK_ID  # pylint: disable=global-statement
    if log_file is None:
        return

    resolved = log_file if log_file.is_absolute() else base_dir / log_file
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if _FILE_SINK_ID is not None:
        logger.remove(_FILE_SINK_ID)
        _FILE_SINK_ID = None

    _FILE_SINK_ID = logger.add(
        resolved,
        level="DEBUG",
        rotation="10 MB",
        retention=5,
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )
    logger.debug("File logging enabled at {path}", path=str(resolved))


def _format_bytes(size: int | None) -> str:
    if size is None:
        return "-"
    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} PB"


def _print_disk_estimate(estimate: dict[str, object]) -> None:
    intervals = estimate.get("intervals", {})
    if isinstance(intervals, dict) and intervals:
        table = Table(title="Legacy Footprint by Interval")
        table.add_column("Interval")
        table.add_column("Legacy Size")
        for name, payload in intervals.items():
            if isinstance(payload, dict):
                size = payload.get("legacy_bytes")
            else:
                size = None
            table.add_row(str(name), _format_bytes(_safe_int(size)))
        console.print(table)

    totals = estimate.get("totals", {})
    if isinstance(totals, dict):
        partition_root = str(totals.get("partition_root", ""))
        console.print(
            f"Partition root: {partition_root}"
            f" (available {_format_bytes(_safe_int(totals.get('available_partition_bytes')))})"
        )
        console.print(
            "Estimated writes: "
            f"{_format_bytes(_safe_int(totals.get('partition_bytes')))}"
            f" + overhead {_format_bytes(_safe_int(totals.get('overhead_bytes')))}"
            f" -> minimum free {_format_bytes(_safe_int(totals.get('required_partition_bytes')))}"
        )
        projected = _safe_int(totals.get("projected_free_after"))
        if projected is not None:
            console.print(
                f"Projected free space after migration: {_format_bytes(projected)}"
            )

        delete_legacy = bool(totals.get("delete_legacy"))
        if delete_legacy:
            console.print(
                "Legacy parquet files will be removed after each ticker migration."
            )
        else:
            console.print("Legacy parquet files remain on disk after migration.")

    limitations = estimate.get("limitations", [])
    if isinstance(limitations, list) and limitations:
        console.print("[red]Disk space limitations detected:[/red]")
        for item in limitations:
            console.print(f"  - {item}")

    if bool(estimate.get("suggest_delete_legacy", False)):
        console.print(
            "[yellow]Tip: rerun with --delete-legacy to reclaim space from legacy parquet files.[/yellow]"
        )


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _derive_market_source(venue: str) -> Tuple[str | None, str | None]:
    parts = venue.split(":", 1)
    if len(parts) != 2:
        return None, None
    market, source = parts
    market = market.strip()
    source = source.strip()
    if not market or not source:
        return None, None
    return market.upper(), source.lower()


def _print_interval_status(plan: MigrationPlan) -> None:
    table = Table(title="Partition Migration Status")

    table.add_column("Venue")
    table.add_column("Interval")
    table.add_column("Status")
    table.add_column("Jobs")
    table.add_column("Legacy Rows")
    table.add_column("Partition Rows")
    table.add_column("Resume Token")

    for venue in plan.venues.values():
        for interval_key, interval in venue.intervals.items():
            table.add_row(
                venue.id,
                interval_key,
                interval.status,
                f"{interval.jobs.completed}/{interval.jobs.total}",
                str(interval.totals.legacy_rows),
                str(interval.totals.partition_rows),
                interval.resume_token or "-",
            )
    console.print(table)


def _load_plan(base_dir: Path) -> MigrationPlan:
    config = ConfigService(base_dir)
    return config.load_migration_plan()


def _default_venue(plan: MigrationPlan) -> str:
    if plan.venues:
        return next(iter(plan.venues.keys()))
    raise ValueError("Migration plan has no venues configured")


def _pending_intervals(venue) -> list[str]:
    return [
        key
        for key, interval in venue.intervals.items()
        if interval.status != "complete"
    ]


def _choose_interval(venue, *, prompt_func: Optional[Callable[..., str]] = None) -> str:
    prompt = prompt_func or typer.prompt
    pending = _pending_intervals(venue)
    if not pending:
        raise ValueError("All intervals for this venue are already complete")
    if len(pending) == 1:
        return pending[0]

    console.print("Multiple intervals available. Please choose:")
    for idx, key in enumerate(pending, start=1):
        interval = venue.intervals[key]
        console.print(f"  {idx}. {key} (status={interval.status})")

    while True:
        choice = str(prompt("Enter selection", default="1")).strip()
        if not choice:
            choice = pending[0]
        if choice in pending:
            return choice
        try:
            index = int(choice)
        except ValueError:
            console.print(
                "[red]Invalid selection. Enter a number or interval name.[/red]"
            )
            continue
        if 1 <= index <= len(pending):
            return pending[index - 1]
        console.print("[red]Selection out of range.[/red]")


def _resolve_intervals(
    *,
    plan: MigrationPlan,
    venue_id: str,
    interval: Optional[str],
    migrate_all: bool,
    prompt_func: Optional[Callable[..., str]] = None,
) -> Tuple[Sequence[str], object]:
    venue_state = plan.get_venue(venue_id)
    pending = _pending_intervals(venue_state)
    prompt = prompt_func or typer.prompt

    if migrate_all:
        if not pending:
            raise ValueError("All intervals already migrated for this venue")
        return pending, venue_state

    if interval is not None:
        if interval not in venue_state.intervals:
            raise ValueError(f"Interval '{interval}' not found for venue {venue_id}")
        return [interval], venue_state

    selected = _choose_interval(venue_state, prompt_func=prompt)
    return [selected], venue_state


@app.command()
def init(
    venue: str = typer.Option(
        "us:yahoo", "--venue", "-V", help="Venue identifier (default: us:yahoo)"
    ),
    market: Optional[str] = typer.Option(
        None,
        "--market",
        "-M",
        help="Market code override (defaults derived from venue)",
    ),
    source: Optional[str] = typer.Option(
        None,
        "--source",
        "-S",
        help="Source identifier override (defaults derived from venue)",
    ),
    interval: list[str] = typer.Option(
        [], "--interval", "-i", help="Intervals to include (repeatable)"
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite existing plan"),
    created_by: str = typer.Option(
        "yf-parqed-migrate", help="Identifier stored in plan"
    ),
    base_dir: Path = typer.Option(Path.cwd(), help="Working directory"),
    log_file: Optional[Path] = typer.Option(
        None,
        "--log-file",
        help="Optional log file path (relative to base dir if not absolute)",
    ),
) -> None:
    """Create or overwrite the migration plan."""
    _configure_logging(base_dir, log_file)
    if not interval:
        console.print("[red]At least one --interval must be provided[/red]")
        raise typer.Exit(code=1)

    inferred_market, inferred_source = _derive_market_source(venue)
    market = market or inferred_market
    source = source or inferred_source

    if not market or not source:
        console.print(
            "[red]Unable to determine market/source. Specify --market and --source explicitly.[/red]"
        )
        raise typer.Exit(code=1)

    market = market.upper()
    source = source.lower()

    service = _load_service(base_dir, created_by)
    try:
        service.initialize_plan(
            venue_id=venue,
            market=market,
            source=source,
            intervals=interval,
            overwrite=force,
        )
    except (FileExistsError, FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    console.print("[green]Migration plan written successfully[/green]")


@app.command()
def status(
    base_dir: Path = typer.Option(Path.cwd(), help="Working directory"),
    log_file: Optional[Path] = typer.Option(
        None,
        "--log-file",
        help="Optional log file path (relative to base dir if not absolute)",
    ),
) -> None:
    """Display current migration plan state."""
    _configure_logging(base_dir, log_file)
    try:
        plan = _load_plan(base_dir)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    _print_interval_status(plan)


@app.command()
def mark(
    venue: str = typer.Argument(..., help="Venue identifier (e.g. us:yahoo)"),
    interval: str = typer.Argument(..., help="Interval to update (e.g. 1m)"),
    status_value: str = typer.Option(None, "--status", help="New status value"),
    jobs_total: Optional[int] = typer.Option(None, help="Total job count"),
    jobs_completed: Optional[int] = typer.Option(None, help="Completed job count"),
    legacy_rows: Optional[int] = typer.Option(None, help="Legacy row count"),
    partition_rows: Optional[int] = typer.Option(None, help="Partition row count"),
    resume_token: Optional[str] = typer.Option(None, help="Resume token"),
    created_by: str = typer.Option(
        "yf-parqed-migrate", help="Identifier stored in plan"
    ),
    base_dir: Path = typer.Option(Path.cwd(), help="Working directory"),
    log_file: Optional[Path] = typer.Option(
        None,
        "--log-file",
        help="Optional log file path (relative to base dir if not absolute)",
    ),
) -> None:
    """Update interval metadata in the migration plan (manual override)."""
    _configure_logging(base_dir, log_file)
    service = _load_service(base_dir, created_by)
    try:
        updated = service.update_interval(
            venue,
            interval,
            status=status_value,
            jobs_total=jobs_total,
            jobs_completed=jobs_completed,
            legacy_rows=legacy_rows,
            partition_rows=partition_rows,
            resume_token=resume_token,
        )
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    console.print(f"Updated {venue}/{interval}: status={updated.status}")


@app.command()
def migrate(
    venue: str = typer.Argument(
        "us:yahoo", help="Venue identifier (default: us:yahoo)"
    ),
    interval: Optional[str] = typer.Argument(
        None, help="Interval to migrate (e.g. 1m)"
    ),
    delete_legacy: bool = typer.Option(
        False,
        "--delete-legacy",
        help="Remove legacy parquet files after successful migration",
    ),
    max_tickers: Optional[int] = typer.Option(
        None,
        "--max-tickers",
        "-n",
        min=1,
        help="Process at most this many tickers (useful for sampling runs)",
    ),
    row_group_size: Optional[int] = typer.Option(
        None,
        "--row-group-size",
        help="Row group size to use when writing partition parquet files (pyarrow).",
    ),
    compression: Optional[str] = typer.Option(
        None,
        "--compression",
        help="Compression codec to use for partition parquet files (e.g. gzip, snappy, none).",
    ),
    all_intervals: bool = typer.Option(
        False,
        "--all",
        help="Migrate all pending intervals for the venue",
    ),
    created_by: str = typer.Option(
        "yf-parqed-migrate", help="Identifier stored in plan"
    ),
    base_dir: Path = typer.Option(Path.cwd(), help="Working directory"),
    non_interactive: bool = typer.Option(False, help="Run in non-interactive mode"),
    overwrite_existing: bool = typer.Option(
        False,
        "--overwrite-existing",
        help="Delete the target interval partition folder before starting (destructive)",
    ),
    no_fsync: bool = typer.Option(
        False,
        "--no-fsync",
        help="Disable fsync on temp partition files for faster writes (less durable).",
    ),
    fast: bool = typer.Option(
        False,
        "--fast",
        help="Enable fast migration defaults: overwrite-existing + no-fsync + row_group_size=65536",
    ),
    log_file: Optional[Path] = typer.Option(
        None,
        "--log-file",
        help="Optional log file path (relative to base dir if not absolute)",
    ),
) -> None:
    """Execute migration for the specified venue/interval."""
    _configure_logging(base_dir, log_file)
    # Acquire a global run lock to avoid overlapping updater/migration runs
    lock = GlobalRunLock(base_dir)
    if not lock.try_acquire():
        owner = lock.owner_info() or {}
        console.print(
            "[red]Another update or migration appears to be running. Owner: {}[/red]".format(
                owner
            )
        )
        if non_interactive:
            # Non-interactive: attempt to recover leftover tmp files before aborting
            processed = lock.cleanup_tmp_files()
            console.print(f"Recovered {processed} tmp files")
            lock.release()
        else:
            if typer.confirm(
                "Attempt to recover leftover tmp files and remove stale lock?"
            ):
                processed = lock.cleanup_tmp_files()
                console.print(f"Recovered {processed} tmp files")
                lock.release()
            else:
                raise typer.Exit(code=1)
    plan = _load_plan(base_dir)
    venue = venue or "us:yahoo"

    if all_intervals and interval is not None:
        console.print("[red]Specify either an interval or --all, not both.[/red]")
        raise typer.Exit(code=1)

    try:
        intervals, venue_state = _resolve_intervals(
            plan=plan,
            venue_id=venue,
            interval=interval,
            migrate_all=all_intervals,
            prompt_func=typer.prompt,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

    # If fast mode is requested, enable the performance defaults
    if fast:
        console.print(
            "[yellow]Fast mode: enabling overwrite-existing, disabling fsync, and using row_group_size=65536[/yellow]"
        )
        overwrite_existing = True
        no_fsync = True
        if row_group_size is None:
            row_group_size = 65536

    # map CLI compression value 'none' to None for the service
    comp_val: str | None
    if compression is None:
        comp_val = "gzip"
    elif compression == "none":
        console.print("Compression disabled")
        comp_val = None
    else:
        comp_val = compression

    service = _load_service(
        base_dir,
        created_by,
        compression=comp_val,
        fsync=not no_fsync,
        row_group_size=row_group_size,
    )

    if no_fsync:
        console.print(
            "[yellow]fsync disabled: writes will be faster but less durable until OS flush.[/yellow]"
        )
    try:
        estimate = service.estimate_disk_requirements(
            venue,
            intervals,
            delete_legacy=delete_legacy,
        )
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

    _print_disk_estimate(estimate)

    if not bool(estimate.get("can_proceed", False)):
        console.print(
            "[red]Insufficient disk space for the requested migration. "
            "Free additional space or rerun with --delete-legacy to proceed.[/red]"
        )
        raise typer.Exit(code=1)

    results: list[Tuple[str, dict[str, object]]] = []
    for interval_name in intervals:
        console.print(f"Processing {venue}/{interval_name}...")
        try:
            result = service.migrate_interval(
                venue,
                interval_name,
                delete_legacy=delete_legacy,
                max_tickers=max_tickers,
                overwrite_existing=overwrite_existing,
            )
        except (FileNotFoundError, FileExistsError, ValueError) as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1)
        console.print(
            "[green]Migration complete[/green] "
            f"(jobs: {result['jobs_completed']}/{result['jobs_total']}, "
            f"rows: {result['legacy_rows']} legacy → {result['partition_rows']} partitioned)"
        )
        if not bool(result.get("persisted", True)):
            available = result.get("available_jobs", result.get("jobs_total"))
            console.print(
                "[yellow]Partial run:[/yellow] processed "
                f"{result['jobs_completed']} of {available} available tickers. "
                "Plan state was left unchanged; rerun without --max-tickers to persist."
            )
        results.append((interval_name, result))

    if len(results) > 1:
        console.print("[green]All requested intervals migrated successfully.[/green]")
    # release global lock
    try:
        lock.release()
    except Exception:
        logger.debug("Failed to release global run lock", exc_info=True)


@app.command()
def verify(
    venue: Optional[str] = typer.Argument(
        None, help="Venue identifier (defaults to plan)"
    ),
    interval: Optional[str] = typer.Argument(None, help="Interval to verify (e.g. 1m)"),
    max_tickers: Optional[int] = typer.Option(
        None,
        "--max-tickers",
        "-n",
        min=1,
        help="Verify at most this many tickers (useful for sampling runs)",
    ),
    base_dir: Path = typer.Option(Path.cwd(), help="Working directory"),
    created_by: str = typer.Option(
        "yf-parqed-verify", help="Identifier stored in plan"
    ),
    log_file: Optional[Path] = typer.Option(
        None,
        "--log-file",
        help="Optional log file path (relative to base dir if not absolute)",
    ),
) -> None:
    """Verify that migrated partition data matches legacy data for a venue/interval."""
    _configure_logging(base_dir, log_file)
    service = _load_service(base_dir, created_by)
    plan = _load_plan(base_dir)
    venue = venue or _default_venue(plan)

    try:
        intervals, _ = _resolve_intervals(
            plan=plan,
            venue_id=venue,
            interval=interval,
            migrate_all=False,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

    overall_results: list[tuple[str, dict[str, object]]] = []
    for interval_name in intervals:
        console.print(f"Verifying {venue}/{interval_name}...")
        try:
            result = service.verify_interval(
                venue, interval_name, max_tickers=max_tickers
            )
        except (FileNotFoundError, ValueError) as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1)

        # Normalize mismatches to a list for safe handling
        raw_mismatches = result.get("mismatches")
        if not raw_mismatches:
            mismatches_list: list[str] = []
        elif isinstance(raw_mismatches, list):
            mismatches_list = raw_mismatches
        else:
            # Fallback: represent the value as a single string entry
            mismatches_list = [str(raw_mismatches)]

        # Safely coerce checked count
        total = 0
        try:
            total_raw = result.get("checked", 0) or 0
            # convert via str() to avoid type-checker errors on opaque objects
            total = int(str(total_raw))
        except Exception:
            total = 0

        if not mismatches_list:
            console.print(
                f"[green]Verification passed for {interval_name}: checked {total} tickers[/green]"
            )
        else:
            console.print(
                f"[red]Verification found {len(mismatches_list)} mismatches out of {total} checked tickers[/red]"
            )
            for m in mismatches_list[:20]:
                console.print(f"  - {m}")

        overall_results.append((interval_name, result))

    if len(overall_results) > 1:
        console.print("[green]Verification completed for requested intervals.[/green]")


if __name__ == "__main__":
    app()
