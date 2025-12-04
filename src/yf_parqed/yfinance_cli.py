import typer
from pathlib import Path
import os
from loguru import logger
import sys
from datetime import datetime, timedelta, time as dt_time
from typing_extensions import Annotated
from typing import Tuple
import signal
import atexit
import time

from yf_parqed.primary_class import YFParqed, all_intervals
from .run_lock import GlobalRunLock
from .trading_hours_checker import TradingHoursChecker


# remove the defult stderr log sink in loguru and add a new one with the log
# level set to INFO to limit the amount of output to the console
logger.remove()
logger.add(sys.stderr, level="INFO")

app = typer.Typer()

# Initialize yf_parqed lazily to avoid errors during test collection
try:
    yf_parqed = YFParqed(my_path=Path(os.getcwd()))
except ValueError:
    # During test collection, intervals.json may not exist
    # Tests will replace this with a stub anyway
    yf_parqed = None

# run-lock operator subcommands
run_lock_app = typer.Typer()
app.add_typer(
    run_lock_app, name="run-lock", help="Operator commands for the global run lock"
)


@run_lock_app.command("status")
def run_lock_status(
    base_dir: Annotated[Path, typer.Option(help="Working directory")] = Path.cwd(),
):
    """Show owner info for the global run lock (if present)."""
    lock = GlobalRunLock(base_dir)
    info = lock.owner_info()
    if not info:
        typer.echo("No run lock present.")
        raise typer.Exit(code=0)
    typer.echo(str(info))


@run_lock_app.command("cleanup")
def run_lock_cleanup(
    base_dir: Annotated[Path, typer.Option(help="Working directory")] = Path.cwd(),
    non_interactive: Annotated[
        bool, typer.Option(help="Run in non-interactive mode")
    ] = False,
):
    """Run cleanup of leftover tmp partition files.

    In interactive mode the operator will be prompted to confirm.
    """
    lock = GlobalRunLock(base_dir)
    if not non_interactive:
        confirm = typer.confirm(
            "This will scan and recover or remove leftover tmp files. Continue?"
        )
        if not confirm:
            raise typer.Exit(code=1)
    processed = lock.cleanup_tmp_files()
    typer.echo(f"Processed {processed} tmp files")


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
    limits: Annotated[
        Tuple[int, int],
        typer.Option(
            help="API Rate limiting. First argument is the maximum number of requests allowed in the time duration. Second argument is the time duration in seconds.",
        ),
    ] = (3, 2),
    # add option to set the loguru log level
    log_level: Annotated[str, typer.Option(help="Log level")] = "INFO",
):
    """
    Persistent storage of yfinance ticker data in parquet.

    Use --limits to set the rate limiting for the API requests.

    Use --wrk_dir to set the working directory.

    Use --log_level to set the log level.
    """
    global yf_parqed
    logger.remove()
    logger.add(sys.stderr, level=log_level)
    os.environ["YF_PARQED_LOG_LEVEL"] = log_level

    # Initialize yf_parqed if it's None (happens during first run or test collection)
    if yf_parqed is None:
        try:
            yf_parqed = YFParqed(my_path=wrk_dir)
        except ValueError:
            # intervals.json doesn't exist yet - initialize with minimal intervals
            # The initialize command will populate this properly
            yf_parqed = YFParqed(my_path=wrk_dir, my_intervals=["1d"])
    else:
        yf_parqed.set_working_path(wrk_dir)
    
    if limits is not None and limits != (3, 2):
        yf_parqed.set_limiter(max_requests=limits[0], duration=limits[1])


@app.command()
def initialize():
    """Initialize the yf_parqed project with 1-minute interval only."""
    global yf_parqed

    yf_parqed.get_new_list_of_stocks()
    yf_parqed.save_intervals(["1m"])
    yf_parqed.update_current_list_of_stocks()
    yf_parqed.save_tickers()


@app.command()
def add_interval(interval: Annotated[str, typer.Argument()]):
    """Convenience function: Add a new interval to the list of intervals."""
    global yf_parqed

    yf_parqed.add_interval(interval)


@app.command()
def remove_interval(interval: Annotated[str, typer.Argument()]):
    """Convenience function: Remve an interval from the list of intervals."""
    global yf_parqed

    yf_parqed.remove_interval(interval)


@app.command()
def update_data(
    start_date: Annotated[
        str | None,
        typer.Option(
            help="Start date for the initial snapshot the stock data (YYYY-MM-DD). Skip if updating a current snapshot.",
        ),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option(
            help="End date for the initial snapshot the stock data (YYYY-MM-DD). Skip if updating a current snapshot.",
        ),
    ] = None,
    save_not_founds: Annotated[
        bool,
        typer.Option(
            help="Save any tickers not returning data to the exclude list.",
        ),
    ] = False,
    non_interactive: Annotated[
        bool, typer.Option(help="Run in non-interactive mode.")
    ] = False,
    daemon: Annotated[
        bool, typer.Option("--daemon", help="Run continuously as daemon")
    ] = False,
    interval: Annotated[
        int, typer.Option("--interval", help="Hours between runs in daemon mode")
    ] = 1,
    market_timezone: Annotated[
        str,
        typer.Option(help="Market timezone (default: US/Eastern for NYSE)"),
    ] = "US/Eastern",
    system_timezone: Annotated[
        str | None,
        typer.Option(help="System timezone (default: auto-detect)"),
    ] = None,
    trading_hours: Annotated[
        str | None,
        typer.Option(
            help="Trading hours HH:MM-HH:MM in market timezone (default: 09:30-16:00 for NYSE)"
        ),
    ] = None,
    extended_hours: Annotated[
        bool,
        typer.Option(
            "--extended-hours",
            help="Include pre-market (04:00-09:30) and after-hours (16:00-20:00)",
        ),
    ] = False,
    ticker_maintenance: Annotated[
        str,
        typer.Option(help="Ticker maintenance schedule: daily, weekly, monthly, never"),
    ] = "weekly",
    pid_file: Annotated[
        Path | None,
        typer.Option(help="PID file to prevent multiple daemon instances"),
    ] = None,
):
    """
    Update Yahoo Finance data for all stock tickers.

    Basic Usage:
        yf-parqed update-data                    # Update once
        yf-parqed update-data --daemon           # Run continuously during trading hours

    Daemon Mode:
        Use --daemon to run continuously, fetching data every --interval hours.
        By default, only runs during NYSE regular trading hours (09:30-16:00 US/Eastern).

        Trading Hours:
        • Regular: 09:30-16:00 (default)
        • Extended: 04:00-20:00 (--extended-hours)
        • Custom: --trading-hours "HH:MM-HH:MM"

        Ticker Maintenance:
        Periodically updates ticker list, confirms not-founds, and reparses.
        • weekly (default): Every 7 days
        • daily: Every day at 09:00
        • monthly: Every 30 days
        • never: Manual only

    Examples:
        # Regular trading hours, hourly updates
        yf-parqed update-data --daemon --interval 1

        # Extended hours (pre-market + after-hours)
        yf-parqed update-data --daemon --extended-hours

        # Custom trading hours with daily ticker maintenance
        yf-parqed update-data --daemon --trading-hours "08:00-18:00" --ticker-maintenance daily

        # With PID file for production
        yf-parqed update-data --daemon --pid-file /var/run/yf-parqed/yf-parqed.pid
    """
    global yf_parqed

    # PID file management for daemon mode
    if pid_file and daemon:
        _check_and_write_pid_file(pid_file)

    # Determine trading hours
    if trading_hours:
        start_time, end_time = TradingHoursChecker.parse_active_hours(trading_hours)
    elif extended_hours:
        # Extended hours: pre-market + regular + after-hours (04:00-20:00)
        start_time, end_time = dt_time(4, 0), dt_time(20, 0)
    else:
        # Regular NYSE hours (09:30-16:00)
        start_time, end_time = dt_time(9, 30), dt_time(16, 0)

    # Create trading hours checker
    hours_checker = TradingHoursChecker(
        start_time=start_time,
        end_time=end_time,
        market_timezone=market_timezone,
        system_timezone=system_timezone,
    )

    # Ticker maintenance tracking
    last_maintenance = {"time": None}

    def should_run_maintenance() -> bool:
        """Check if ticker maintenance should run."""
        if ticker_maintenance == "never":
            return False

        if last_maintenance["time"] is None:
            return True

        elapsed = datetime.now() - last_maintenance["time"]

        if ticker_maintenance == "daily":
            return elapsed >= timedelta(days=1)
        elif ticker_maintenance == "weekly":
            return elapsed >= timedelta(days=7)
        elif ticker_maintenance == "monthly":
            return elapsed >= timedelta(days=30)

        return False

    def run_ticker_maintenance():
        """Run ticker maintenance tasks."""
        logger.info("Running ticker maintenance...")
        try:
            yf_parqed.update_current_list_of_stocks()
            logger.info("✓ Ticker list updated")

            yf_parqed.confirm_not_founds()
            logger.info("✓ Not-founds confirmed")

            yf_parqed.reparse_not_founds()
            logger.info("✓ Not-founds reparsed")

            last_maintenance["time"] = datetime.now()
            logger.info(
                f"Ticker maintenance completed. Next run: ~{last_maintenance['time'] + timedelta(days={'daily': 1, 'weekly': 7, 'monthly': 30}.get(ticker_maintenance, 7))}"
            )
        except Exception as e:
            logger.error(f"Error during ticker maintenance: {e}", exc_info=True)

    def run_update_once():
        """Execute one update cycle with locking."""
        logger.debug("Updating stock data.")

        # Acquire a global run lock to avoid overlapping updater runs
        base_for_lock = None
        if hasattr(yf_parqed, "my_path"):
            base_for_lock = getattr(yf_parqed, "my_path")
        elif hasattr(yf_parqed, "work_path"):
            base_for_lock = getattr(yf_parqed, "work_path")
        else:
            cfg = getattr(yf_parqed, "config", None)
            if cfg is not None and getattr(cfg, "base_path", None) is not None:
                base_for_lock = cfg.base_path
            else:
                base_for_lock = Path.cwd()

        lock = GlobalRunLock(base_for_lock)
        if not lock.try_acquire():
            owner = lock.owner_info() or {}
            msg = f"Another update or migration run appears to be in progress. Owner: {owner}"
            logger.error(msg)

            if non_interactive or daemon:
                # Attempt automatic recovery
                processed = lock.cleanup_tmp_files()
                logger.info("Recovered %d tmp files", processed)
                try:
                    lock.release()
                    logger.info("Removed stale lock; continuing.")
                except Exception:
                    logger.warning("Could not remove lock; aborting this run.")
                    return
            else:
                # Prompt operator
                should = typer.confirm(
                    "Lock detected. Do you want to attempt to recover leftover tmp files?"
                )
                if should:
                    processed = lock.cleanup_tmp_files()
                    logger.info("Recovered %d tmp files", processed)
                    try:
                        lock.release()
                        logger.info("Removed stale lock; continuing.")
                    except Exception:
                        logger.warning("Could not remove lock; aborting.")
                        raise typer.Exit(code=1)
                else:
                    raise typer.Exit(code=1)

        try:
            logger.debug(f"Supplied start and end dates:{[start_date, end_date]}")
            logger.debug(
                f"Supplied save-not-founds and non-interactive flags: {[save_not_founds, non_interactive]}"
            )

            if all([start_date is None, end_date is None]):
                yf_parqed.update_stock_data()
            else:
                if any([start_date is None, end_date is None]):
                    logger.error(
                        "Both start and end date must be provided if not updating a current snapshot."
                    )
                    return
                # Convert string dates to datetime objects
                # Support both "YYYY-MM-DD" and "YYYY-MM-DDTHH:MM:SS" formats
                if isinstance(start_date, str):
                    if "T" in start_date:
                        start_dt = datetime.fromisoformat(start_date)
                    else:
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                else:
                    start_dt = start_date

                if isinstance(end_date, str):
                    if "T" in end_date:
                        end_dt = datetime.fromisoformat(end_date)
                    else:
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                else:
                    end_dt = end_date
                yf_parqed.update_stock_data(
                    start_date=start_dt,
                    end_date=end_dt,
                )

            logger.info("All tickers were processed.")

            if yf_parqed.new_not_found:
                logger.info("Some tickers did not return any data.")
                if non_interactive or daemon:
                    if save_not_founds:
                        yf_parqed.save_tickers()
                        logger.info("Tickers file updated with not found entries.")
                    else:
                        logger.info("Tickers file was not updated.")
                else:
                    if save_not_founds:
                        yf_parqed.save_tickers()
                        logger.info("Tickers file updated with not found entries.")
                    else:
                        update_nf = typer.prompt(
                            "Do you want to update the not found list? (y/n)",
                            default="y",
                        )
                        if update_nf.lower() == "y":
                            yf_parqed.save_tickers()
                            logger.info("Tickers file updated with not found entries.")
                        else:
                            logger.info("Tickers file not updated.")
        finally:
            # Always release lock
            try:
                lock.release()
            except Exception:
                logger.debug("Failed to release global run lock", exc_info=True)

    # Signal handler for graceful shutdown
    shutdown_requested = {"flag": False}

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown_requested["flag"] = True

    if daemon:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    try:
        if daemon:
            logger.info(f"Starting daemon mode: updating every {interval} hour(s)")
            logger.info(f"Trading hours: {hours_checker._calculate_local_hours()}")
            logger.info(f"Ticker maintenance: {ticker_maintenance}")
            logger.info(
                f"PID: {Path('/proc/self').resolve().name if Path('/proc/self').exists() else os.getpid()}"
            )

            run_count = 0
            while not shutdown_requested["flag"]:
                # Check if within trading hours
                if not hours_checker.is_within_hours():
                    wait_seconds = hours_checker.seconds_until_active()
                    next_active = hours_checker.next_active_time()
                    logger.info(
                        f"Outside trading hours. Waiting until {next_active.strftime('%Y-%m-%d %H:%M:%S %Z')}"
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

                    logger.info("Entering trading hours, starting update cycle")

                run_count += 1
                logger.info(f"=== Daemon run #{run_count} started ===")

                # Check if ticker maintenance should run
                if should_run_maintenance():
                    run_ticker_maintenance()

                # Run the update
                try:
                    run_update_once()
                except Exception as e:
                    logger.error(
                        f"Error in daemon run #{run_count}: {e}", exc_info=True
                    )

                if shutdown_requested["flag"]:
                    break

                # Calculate next run time
                next_run = datetime.now() + timedelta(hours=interval)
                logger.info(
                    f"=== Daemon run #{run_count} completed. "
                    f"Next run at {next_run.strftime('%Y-%m-%d %H:%M:%S')} ==="
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

            logger.info("Daemon shutdown complete.")
        else:
            # Single run mode
            run_update_once()
    finally:
        # Cleanup PID file if daemon
        if daemon and pid_file and pid_file.exists():
            pid_file.unlink()
            logger.info(f"PID file removed: {pid_file}")


@app.command()
def update_tickers():
    """Update the list of tickers."""
    global yf_parqed

    yf_parqed.update_current_list_of_stocks()
    logger.info("Ticker list updated.")


@app.command()
def confirm_not_founds():
    """Update the not found list."""
    global yf_parqed

    yf_parqed.confirm_not_founds()
    logger.info("Tickers file updated.")


@app.command()
def reparse_not_founds():
    """Reparse the not found list."""
    global yf_parqed

    yf_parqed.reparse_not_founds()
    logger.info("Tickers file updated.")
    yf_parqed.reparse_not_founds()
    logger.info("Not found list updated.")


@app.command("partition-toggle")
def partition_toggle(
    market: Annotated[
        str | None,
        typer.Option("--market", "-m", help="Market code (for example US)"),
    ] = None,
    source: Annotated[
        str | None,
        typer.Option("--source", "-s", help="Source identifier (for example yahoo)"),
    ] = None,
    disable: Annotated[
        bool,
        typer.Option("--disable", help="Disable partitioned storage for the target"),
    ] = False,
    clear: Annotated[
        bool,
        typer.Option(
            "--clear", help="Remove an explicit partition override for the target"
        ),
    ] = False,
):
    """Enable, disable, or clear partitioned storage overrides."""
    global yf_parqed

    if source and not market:
        typer.echo("Provide --market when specifying --source", err=True)
        raise typer.Exit(code=1)

    if clear and disable:
        typer.echo("--clear cannot be combined with --disable", err=True)
        raise typer.Exit(code=1)

    try:
        if clear:
            yf_parqed.clear_partition_override(market=market, source=source)
            typer.echo("Partition override cleared.")
            return

        enabled = not disable
        yf_parqed.set_partition_override(
            enabled=enabled,
            market=market,
            source=source,
        )

        target = "globally"
        if market and source:
            target = f"for {market}/{source}"
        elif market:
            target = f"for market {market}"

        state = "enabled" if enabled else "disabled"
        typer.echo(f"Partition mode {state} {target}.")
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1)
