import typer
from pathlib import Path
from loguru import logger
import sys
import signal
import time
import os
import atexit
from datetime import datetime, timedelta
from typing_extensions import Annotated

from .trading_hours_checker import TradingHoursChecker
from .xetra_service import XetraService

app = typer.Typer()


def _check_and_write_pid_file(pid_file: Path) -> None:
    """
    Check if another instance is running and write PID file.

    Args:
        pid_file: Path to PID file

    Raises:
        typer.Exit: If another instance is already running
    """
    if pid_file.exists():
        try:
            # Check if process is still running
            with open(pid_file, "r") as f:
                old_pid = int(f.read().strip())

            # Check if process exists
            try:
                os.kill(old_pid, 0)  # Signal 0 just checks if process exists
                logger.error(
                    f"Another instance is already running (PID {old_pid}). "
                    f"Remove {pid_file} if this is stale."
                )
                raise typer.Exit(1)
            except OSError:
                # Process doesn't exist, remove stale PID file
                logger.warning(f"Removing stale PID file (PID {old_pid} not running)")
                pid_file.unlink()
        except (ValueError, FileNotFoundError):
            # Invalid PID file, remove it
            logger.warning("Removing invalid PID file")
            pid_file.unlink(missing_ok=True)

    # Write our PID
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    logger.info(f"PID file created: {pid_file} (PID: {os.getpid()})")

    # Register cleanup
    def cleanup_pid():
        if pid_file.exists():
            pid_file.unlink()
            logger.info(f"PID file removed: {pid_file}")

    atexit.register(cleanup_pid)


@app.callback()
def main(
    wrk_dir: Annotated[
        Path, typer.Option(help="Working directory, default is current directory")
    ] = Path.cwd(),
    log_level: Annotated[str, typer.Option(help="Log level")] = "INFO",
    log_file: Annotated[
        Path | None,
        typer.Option(help="Log to file instead of stderr (enables rotation)"),
    ] = None,
):
    """
    Xetra delayed data CLI - Deutsche Börse parquet storage.
    Use --wrk-dir to set working directory, --log-level to set logging verbosity.
    Use --log-file for daemon mode with log rotation.
    """
    logger.remove()

    if log_file:
        # File logging with rotation for daemon mode
        logger.add(
            log_file,
            level=log_level,
            rotation="10 MB",  # Rotate when file reaches 10MB
            retention="30 days",  # Keep logs for 30 days
            compression="gz",  # Compress rotated logs
            enqueue=True,  # Thread-safe logging
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        )
    else:
        # Console logging for interactive use
        logger.add(sys.stderr, level=log_level)


@app.command()
def fetch_trades(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    no_store: Annotated[
        bool, typer.Option("--no-store", help="Display only, don't store")
    ] = False,
    daemon: Annotated[
        bool, typer.Option("--daemon", help="Run continuously as daemon")
    ] = False,
    interval: Annotated[
        int, typer.Option("--interval", help="Hours between runs in daemon mode")
    ] = 1,
    active_hours: Annotated[
        str | None,
        typer.Option(
            help="Trading hours HH:MM-HH:MM in CET/CEST (default: venue-specific, e.g. 08:30-18:00)"
        ),
    ] = None,
    pid_file: Annotated[
        Path | None,
        typer.Option(help="PID file to prevent multiple daemon instances"),
    ] = None,
):
    """
    Intelligently fetch and store Xetra trades for a venue.

    Automatically determines what dates to fetch based on:
    - What's available from Deutsche Börse API (~24 hours of data)
    - What's already stored locally

    Only fetches and stores missing data. This is the recommended way to collect
    daily Xetra trade data.

    Data Type:
      Currently fetches POSTTRADE data (executed trades) for OHLCV aggregation.
      PRETRADE data (order book snapshots) is planned for future implementation.

    Venue codes:
      • DETR - Xetra (main Frankfurt electronic exchange)
      • DFRA - Frankfurt Stock Exchange (floor trading)
      • DGAT - Xetra Gateways
      • DEUR - Eurex (derivatives exchange)

    Daemon Mode:
      Use --daemon to run continuously, fetching new data every --interval hours.
      Recommended: use with --log-file and --pid-file for production.

      Trading Hours:
      By default, daemon only runs during venue trading hours (08:30-18:00 CET/CEST).
      Use --active-hours to override (e.g., "00:00-23:59" for 24/7 operation).

    Examples:
        xetra-parqed fetch-trades DETR              # Fetch missing data once
        xetra-parqed fetch-trades DETR --no-store   # Check what's available (dry run)
        xetra-parqed fetch-trades DEUR              # Fetch Eurex derivatives data

        # Daemon mode (respects trading hours)
        xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon --interval 1 --pid-file /tmp/xetra.pid

        # Daemon mode (24/7 operation)
        xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon --interval 1 --active-hours "00:00-23:59"
    """
    # Market and source are fixed for Xetra
    market = "de"
    source = "xetra"

    # Initialize trading hours checker
    # Default to Xetra trading hours (08:30-18:00 CET/CEST)
    default_hours = active_hours or "08:30-18:00"
    start_time, end_time = TradingHoursChecker.parse_active_hours(default_hours)
    hours_checker = TradingHoursChecker(
        start_time=start_time,
        end_time=end_time,
        market_timezone="Europe/Berlin",
    )

    # PID file management for daemon mode
    if pid_file and daemon:
        _check_and_write_pid_file(pid_file)

    # Signal handler for graceful shutdown
    shutdown_requested = {"flag": False}

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown_requested["flag"] = True

    if daemon:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def run_fetch_once():
        """Execute one fetch cycle."""
        with XetraService() as service:
            if no_store:
                # Dry run mode - just show what would be fetched
                logger.info(f"Checking missing dates for {venue} (dry run mode)")
                missing_dates = service.get_missing_dates(venue, market, source)

                if not missing_dates:
                    typer.echo(f"✓ All available data already stored for {venue}")
                else:
                    typer.echo(f"Would fetch {len(missing_dates)} date(s) for {venue}:")
                    for date in missing_dates:
                        typer.echo(f"  - {date}")
                    typer.echo("\nRemove --no-store to fetch and store this data")
            else:
                # Actually fetch and store (using incremental mode for interrupt safety)
                summary = service.fetch_and_store_missing_trades_incremental(
                    venue, market, source
                )

                if summary["total_trades"] == 0:
                    message = f"✓ All available data already stored for {venue}"
                    logger.info(message)
                    if not daemon:  # Only echo in non-daemon mode
                        typer.echo(message)
                else:
                    message_parts = [f"\n✓ Fetched and stored trades for {venue}:"]
                    if summary["dates_fetched"]:
                        message_parts.append(
                            f"  - Completed dates: {', '.join(summary['dates_fetched'])}"
                        )
                    message = "\n".join(message_parts)
                    logger.info(message)
                    if not daemon:
                        typer.echo(message)

                return summary

    try:
        if daemon:
            logger.info(
                f"Starting daemon mode for {venue}: fetching every {interval} hour(s)"
            )
            logger.info(f"Active hours: {default_hours} {hours_checker.market_tz}")
            logger.info(
                f"PID: {Path('/proc/self').resolve().name if Path('/proc/self').exists() else 'unknown'}"
            )

            # Check if this is initial startup with no data
            # If so, fetch all available data (within trading hours)
            initial_fetch_done = False
            with XetraService() as service:
                if not service.has_any_data(venue, market, source):
                    logger.info(f"No existing data found for {venue} - performing initial fetch of all available data")
                    if hours_checker.is_within_hours():
                        try:
                            logger.info("Within trading hours, fetching all available data...")
                            run_fetch_once()
                            initial_fetch_done = True
                            logger.info("Initial data fetch completed successfully")
                        except Exception as e:
                            logger.error(f"Error during initial data fetch: {e}", exc_info=True)
                            logger.info("Will retry on next cycle")
                    else:
                        logger.info("Outside trading hours - will fetch all available data when trading hours start")

            run_count = 0
            while not shutdown_requested["flag"]:
                # Check if within active hours
                if not hours_checker.is_within_hours():
                    wait_seconds = hours_checker.seconds_until_active()
                    next_active = hours_checker.next_active_time()
                    logger.info(
                        f"Outside active hours. Waiting until {next_active.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    )

                    # Sleep in small intervals to check for shutdown
                    sleep_interval = 60  # Check every minute
                    for _ in range(int(wait_seconds / sleep_interval)):
                        if shutdown_requested["flag"]:
                            break
                        time.sleep(sleep_interval)

                    # Sleep remaining time
                    if not shutdown_requested["flag"]:
                        remaining = wait_seconds % sleep_interval
                        if remaining > 0:
                            time.sleep(remaining)

                    if shutdown_requested["flag"]:
                        break

                    logger.info("Entering active hours, starting fetch cycle")
                    
                    # If initial fetch wasn't done yet (started outside trading hours), do it now
                    if not initial_fetch_done:
                        with XetraService() as service:
                            if not service.has_any_data(venue, market, source):
                                logger.info("Performing deferred initial fetch of all available data")
                                try:
                                    run_fetch_once()
                                    initial_fetch_done = True
                                    logger.info("Initial data fetch completed successfully")
                                except Exception as e:
                                    logger.error(f"Error during initial data fetch: {e}", exc_info=True)

                run_count += 1
                logger.info(
                    f"=== Daemon run #{run_count} started at {datetime.now(hours_checker.market_tz).isoformat()} ==="
                )

                try:
                    run_fetch_once()
                except Exception as e:
                    logger.error(
                        f"Error in daemon run #{run_count}: {e}", exc_info=True
                    )
                    # Continue running despite errors

                if shutdown_requested["flag"]:
                    break

                # Calculate next run time
                next_run_local = datetime.now(hours_checker.system_tz) + timedelta(
                    hours=interval
                )

                # Log completion message
                logger.info(
                    f"=== Daemon run #{run_count} completed. "
                    f"Next scheduled: {next_run_local.strftime('%Y-%m-%d %H:%M:%S %Z')} ==="
                )

                # Sleep in small intervals to check for shutdown signal
                sleep_seconds = interval * 3600
                sleep_interval = 10  # Check every 10 seconds
                for _ in range(int(sleep_seconds / sleep_interval)):
                    if shutdown_requested["flag"]:
                        break
                    time.sleep(sleep_interval)

                # Sleep remaining time
                if not shutdown_requested["flag"]:
                    remaining = sleep_seconds % sleep_interval
                    if remaining > 0:
                        time.sleep(remaining)

            logger.info("Daemon shutting down gracefully")
        else:
            # One-time run
            summary = run_fetch_once()

            # Show summary for one-time runs
            if summary and not no_store:
                if summary["dates_partial"]:
                    typer.echo(
                        f"  - Partial dates: {', '.join(summary['dates_partial'])}"
                    )
                typer.echo(f"  - Total trades: {summary['total_trades']:,}")
                typer.echo(f"  - Total files: {summary['total_files']}")

                if summary["consolidated"]:
                    typer.echo("\n📦 Monthly consolidation completed")
                    typer.echo(
                        f"  Daily files preserved in: data/{market}/{source}/trades/venue={venue}/..."
                    )
                    typer.echo(
                        f"  Monthly file: data/{market}/{source}/trades_monthly/venue={venue}/..."
                    )

                if summary.get("dates_partial"):
                    typer.echo(
                        "\n⚠ Process had partial downloads - progress has been saved"
                    )
                    typer.echo("Re-run the command to resume from where you left off")
    finally:
        # Cleanup PID file
        if pid_file and pid_file.exists():
            pid_file.unlink()
            logger.info(f"PID file removed: {pid_file}")


@app.command()
def check_status(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
):
    """
    Check what data is available vs what's already stored for a venue.

    Shows which dates have data available from Deutsche Börse and which are
    already stored locally.

    Data Type:
      Currently checks POSTTRADE data (executed trades) only.
      PRETRADE data (order book) support planned for future.

    Venue codes:
      • DETR - Xetra (main Frankfurt electronic exchange)
      • DFRA - Frankfurt Stock Exchange (floor trading)
      • DGAT - Xetra Gateways
      • DEUR - Eurex (derivatives exchange)

    Examples:
        xetra-parqed check-status DETR    # Check Xetra status
        xetra-parqed check-status DEUR    # Check Eurex status
    """
    from datetime import datetime, timedelta

    market = "de"
    source = "xetra"

    service = XetraService()

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    typer.echo(f"\nStatus for {venue}:")
    typer.echo("-" * 50)

    for check_date in [today, yesterday]:
        date_str = check_date.strftime("%Y-%m-%d")

        # Check API availability
        try:
            files = service.list_files(venue, date_str)
            api_status = f"✓ {len(files)} files available" if files else "✗ No files"
        except Exception as e:
            api_status = f"✗ Error: {e}"

        # Check local storage
        trade_date = datetime.strptime(date_str, "%Y-%m-%d")
        year = trade_date.year
        month = f"{trade_date.month:02d}"
        day = f"{trade_date.day:02d}"

        base_dir = (
            service.backend._path_builder._root
            / market
            / source
            / "trades"
            / f"venue={venue}"
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
        )
        parquet_path = base_dir / "trades.parquet"
        storage_status = "✓ Stored locally" if parquet_path.exists() else "✗ Not stored"

        typer.echo(f"\n{date_str}:")
        typer.echo(f"  API:     {api_status}")
        typer.echo(f"  Storage: {storage_status}")

    service.close()


@app.command()
def list_files(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    date: Annotated[
        str | None, typer.Option(help="Trade date (YYYY-MM-DD, default: today)")
    ] = None,
):
    """
    List available trade files for a venue/date.

    Data Type:
      Currently lists POSTTRADE files (executed trades) only.
      PRETRADE files (order book) support planned for future.

    Venue codes:
      • DETR - Xetra (main Frankfurt electronic exchange)
      • DFRA - Frankfurt Stock Exchange (floor trading)
      • DGAT - Xetra Gateways
      • DEUR - Eurex (derivatives exchange)

    Examples:
        xetra-parqed list-files DETR                    # Today's Xetra files
        xetra-parqed list-files DETR --date 2025-11-01  # Specific date
        xetra-parqed list-files DEUR                    # Today's Eurex files
    """
    from datetime import datetime

    # Default to today if no date provided
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    service = XetraService()
    files = service.list_files(venue, date)
    if files:
        typer.echo(f"Found {len(files)} files for {venue} on {date}:")
        for filename in files:
            typer.echo(f"  - {filename}")
    else:
        typer.echo(f"No files found for {venue} on {date}")


@app.command()
def check_partial(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    market: Annotated[str, typer.Option(help="Market code")] = "de",
    source: Annotated[str, typer.Option(help="Source code")] = "xetra",
):
    """
    Check for partial/interrupted downloads that can be resumed or consolidated.

    Scans stored data to identify:
    - Dates with complete downloads (have parquet files)
    - Dates with partial/empty downloads (interrupted)
    - Months ready for consolidation

    Use this after an interrupted download to see what needs to be resumed.

    Examples:
        xetra-parqed check-partial DETR         # Check Xetra download status
        xetra-parqed check-partial DFRA         # Check Frankfurt status
    """
    service = XetraService()
    status = service.check_partial_downloads(venue, market, source)

    typer.echo(f"\n📊 Download Status for {venue}:\n")

    if status["complete_dates"]:
        typer.echo(f"✓ Complete dates ({len(status['complete_dates'])}):")
        for date in status["complete_dates"][-10:]:  # Show last 10
            typer.echo(f"  - {date}")
        if len(status["complete_dates"]) > 10:
            typer.echo(f"  ... and {len(status['complete_dates']) - 10} more")
    else:
        typer.echo("✓ No complete dates found")

    typer.echo()

    if status["partial_dates"]:
        typer.echo(f"⚠ Partial/empty dates ({len(status['partial_dates'])}):")
        for item in status["partial_dates"]:
            typer.echo(f"  - {item['date']}: {item['status']}")
        typer.echo("\n  💡 Re-run 'fetch-trades' to resume interrupted downloads")
    else:
        typer.echo("✓ No partial downloads found")

    typer.echo()

    if status["months_ready"]:
        typer.echo(
            f"📅 Months ready for consolidation ({len(status['months_ready'])}):"
        )
        for year, month in status["months_ready"]:
            typer.echo(f"  - {year}-{month:02d}")
        typer.echo("\n  💡 Use 'consolidate-month' to create monthly parquet files")
    else:
        typer.echo("✓ No months ready for consolidation")


@app.command()
def consolidate_month(
    venue: Annotated[
        str,
        typer.Argument(
            help=(
                "Xetra venue code:\n"
                "  DETR = Xetra (main Frankfurt electronic exchange)\n"
                "  DFRA = Frankfurt Stock Exchange (floor trading)\n"
                "  DGAT = Xetra Gateways\n"
                "  DEUR = Eurex (derivatives exchange)"
            )
        ),
    ],
    all_months: Annotated[
        bool, typer.Option("--all", help="Consolidate all available months")
    ] = False,
    market: Annotated[str, typer.Option(help="Market code")] = "de",
    source: Annotated[str, typer.Option(help="Source code")] = "xetra",
):
    """
    Consolidate daily files into optimized monthly parquet files.

    Auto-detects years/months from stored daily files. By default, shows what would
    be consolidated and prompts for confirmation. Use --all to consolidate all
    available months without prompting.

    This reads all daily parquet files for each month and combines them into a
    single monthly file with optimal compression and row group sizing.

    Monthly files are written to:
      data/{market}/{source}/trades_monthly/venue={venue}/year={year}/month={month}/trades.parquet

    Daily files are preserved for safety (can be manually deleted after verification).

    File size estimate: ~86 MB/day × 22 trading days = ~1.9 GB/month

    Examples:
        xetra-parqed consolidate-month DETR           # Interactive consolidation
        xetra-parqed consolidate-month DETR --all     # Consolidate all months
    """
    service = XetraService()

    # Auto-detect months from stored data
    status = service.check_partial_downloads(venue, market, source)

    if not status["months_ready"]:
        typer.echo(f"✓ No months found with daily data for {venue}")
        typer.echo("  Run 'fetch-trades' first to download data")
        return

    months_to_consolidate = status["months_ready"]

    typer.echo(
        f"\n📦 Found {len(months_to_consolidate)} month(s) ready for consolidation:\n"
    )
    for year, month in months_to_consolidate:
        typer.echo(f"  - {year}-{month:02d}")

    if not all_months:
        typer.echo("\nℹ️  Use --all to consolidate all months, or Ctrl+C to cancel")
        confirm = typer.confirm("\nConsolidate these months?", default=True)
        if not confirm:
            typer.echo("Cancelled")
            return

    # Consolidate each month
    success_count = 0
    fail_count = 0

    for year, month in months_to_consolidate:
        try:
            typer.echo(f"\n📊 Consolidating {venue} {year}-{month:02d}...")
            service._consolidate_to_monthly(venue, year, month, market, source)
            typer.echo("   ✓ Success")
            success_count += 1
        except Exception as e:
            typer.echo(f"   ❌ Failed: {e}", err=True)
            fail_count += 1

    typer.echo(f"\n{'=' * 60}")
    typer.echo(
        f"Consolidation complete: {success_count} succeeded, {fail_count} failed"
    )

    if success_count > 0:
        typer.echo(
            f"\n✓ Monthly files: data/{market}/{source}/trades_monthly/venue={venue}/..."
        )
        typer.echo(
            f"  Daily files preserved in: data/{market}/{source}/trades/venue={venue}/..."
        )
