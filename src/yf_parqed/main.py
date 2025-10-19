import typer
from pathlib import Path
import os
from loguru import logger
import sys
from datetime import datetime
from typing_extensions import Annotated
from typing import Tuple

from yf_parqed.primary_class import YFParqed, all_intervals
from .run_lock import GlobalRunLock


# remove the defult stderr log sink in loguru and add a new one with the log
# level set to INFO to limit the amount of output to the console
logger.remove()
logger.add(sys.stderr, level="INFO")

app = typer.Typer()
yf_parqed = YFParqed(my_path=Path(os.getcwd()))

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


# def download_file(url: str, local_path: Path):
#     res = httpx.get(url, follow_redirects=True)
#     local_path.write_text(res.text)


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

    # wrk_dir = Path(wrk_dir)
    yf_parqed.set_working_path(wrk_dir)
    if limits is not None and limits != (3, 2):
        yf_parqed.set_limiter(max_requests=limits[0], duration=limits[1])


@app.command()
def initialize():
    """Initialize the yf_parqed project."""
    global yf_parqed

    yf_parqed.get_new_list_of_stocks()
    yf_parqed.save_intervals(all_intervals)
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
        datetime | None,
        typer.Option(
            help="Start date for the initial snapshot the stock data. Skip if updating a current snapshot.",
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        typer.Option(
            help="End date for the initial snapshot the stock data. Skip if updating a current snapshot.",
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
):
    """Update the yfinace data for all stock tickers. See update --help for options."""
    global yf_parqed

    logger.debug("Updating stock data.")
    # Acquire a global run lock to avoid overlapping updater runs
    # Be defensive: tests may replace `yf_parqed` with a stub that lacks `my_path`.
    # Prefer real object's my_path, otherwise accept test stub's work_path, then config, then cwd
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
        msg = (
            "Another update or migration run appears to be in progress. "
            f"Owner: {owner}"
        )
        logger.error(msg)
        # If running non-interactively (cron), attempt automatic recovery
        if non_interactive:
            # First attempt to recover leftover tmp files before aborting
            processed = lock.cleanup_tmp_files()
            logger.info("Recovered %d tmp files", processed)
            try:
                lock.release()
                logger.info("Removed stale lock; continuing.")
            except Exception:
                logger.warning("Could not remove lock; aborting.")
                raise typer.Exit(code=1)
        else:
            # Prompt operator to cleanup tmp files if running interactively
            should = typer.confirm(
                "Lock detected. Do you want to attempt to recover leftover tmp files?"
            )
            if should:
                processed = lock.cleanup_tmp_files()
                logger.info("Recovered %d tmp files", processed)
                # Try removing the lock if it was stale
                try:
                    lock.release()
                    logger.info("Removed stale lock; continuing.")
                except Exception:
                    logger.warning("Could not remove lock; aborting.")
                    raise typer.Exit(code=1)
            else:
                raise typer.Exit(code=1)
    logger.debug(f"Supplied start and end dates:{[start_date, end_date]}")
    logger.debug(
        f"Supplied save-not-founds and non-interactive flags: {[save_not_founds, non_interactive]}"
    )
    logger.debug(
        f"Checking if we are in update mode: {all([start_date is None, end_date is None])}"
    )
    if all([start_date is None, end_date is None]):
        yf_parqed.update_stock_data()
    else:
        logger.debug(
            f"Checking if either start or end dates are missing: {any([start_date is None, end_date is None])}"
        )
        if any([start_date is None, end_date is None]):
            logger.error(
                "Both start and end date must be provided if not updating a current snapshot."
            )
            return
        yf_parqed.update_stock_data(
            start_date=start_date,
            end_date=end_date,
        )
    logger.info("All tickers were processed.")
    # release global lock
    try:
        lock.release()
    except Exception:
        logger.debug("Failed to release global run lock", exc_info=True)
    if yf_parqed.new_not_found:
        logger.info("Some tickers did not return any data.")
        if non_interactive:
            if save_not_founds:
                yf_parqed.save_tickers()
                logger.info("Tickers file updated with not found entries.")
            else:
                logger.info("Tickers file was not updated.")
            return
        else:
            if save_not_founds:
                yf_parqed.save_tickers()
                logger.info("Tickers file updated with not found entries.")
                return
            update_nf = typer.prompt(
                "".join(
                    [
                        "Do you want to update the not found list so ",
                        "they are not included in the future? (y/n)",
                    ]
                ),
                default="y",
            )
            if update_nf.lower() == "y":
                yf_parqed.save_tickers()
                logger.info("Tickers file updated with not found entries.")
            else:
                logger.info("Tickers file not updated.")


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
