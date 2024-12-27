import typer
import httpx
from pathlib import Path
import os
from loguru import logger
import sys
from datetime import datetime
from typing import Annotated

from yf_parqed.primary_class import YFParqed


# remove the defult stderr log sink in loguru and add a new one with the log
# level set to INFO to limit the amount of output to the console
logger.remove()
logger.add(sys.stderr, level="INFO")

app = typer.Typer()
yf_parqed = YFParqed(my_path=Path(os.getcwd()))


def download_file(url: str, local_path: Path):
    res = httpx.get(url, follow_redirects=True)
    local_path.write_text(res.text)


def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed.csv"
    local_path = yf_parqed.my_path / "nasdaq-listed.csv"
    download_file(url, local_path)

    url = "https://datahub.io/core/nyse-other-listings/_r/-/data/nyse-listed.csv"
    local_path = yf_parqed.my_path / "nyse-listed.csv"
    download_file(url, local_path)


def define_intervals():
    global yf_parqed
    intervals = [
        "1m",
        "2m",
        "5m",
        "15m",
        "30m",
        "60m",
        "90m",
        "1h",
        "1d",
        "5d",
        "1wk",
        "1mo",
        "3mo",
    ]
    yf_parqed.save_intervals(intervals)


@app.callback()
def main(
    wrk_dir: str = typer.Option(
        os.getcwd(), help="Working directory, default is current directory"
    ),
    limits: tuple[int, int] = typer.Option(
        (2, 5),
        help="API Rate limiting. First argument is the maximum number of requests allowed in the time duration. Second argument is the time duration in seconds.",
    ),
    # add option to set the loguru log level
    log_level: str = typer.Option("INFO", help="Log level"),
):
    """
    Persistent storage of yfinance ticker data in parquet.

    Use --limits to set the rate limiting for the API requests.

    Use --wrk_dir to set the working directory.
    """
    global yf_parqed
    logger.remove()
    logger.add(sys.stderr, level=log_level)

    wrk_dir = Path(wrk_dir)
    yf_parqed.set_working_path(wrk_dir)
    yf_parqed.set_limiter(max_requests=limits[0], duration=limits[1])


@app.command()
def initialize():
    """Initialize the yf_parqed project."""
    global yf_parqed

    get_tickers()
    define_intervals()
    tickers = yf_parqed.get_new_list_of_stocks()
    yf_parqed.save_current_list_of_stocks(tickers)
    yf_parqed.save_not_founds()


@app.command()
def add_interval(interval: str):
    """Convenience function: Add a new interval to the list of intervals."""
    global yf_parqed

    yf_parqed.add_interval(interval)


@app.command()
def remove_interval(interval: str):
    """Convenience function: Remve an interval from the list of intervals."""
    global yf_parqed

    yf_parqed.remove_interval(interval)


@app.command()
def update(
    start_date: datetime = typer.Option(
        None,
        help="Start date for the initial snapshot the stock data. Skip if updating a current snapshot.",
    ),
    end_date: datetime = typer.Option(
        None,
        help="End date for the initial snapshot the stock data. Skip if updating a current snapshot.",
    ),
    save_not_found: Annotated[
        bool,
        typer.Option(
            "--save-not-founds",
            help="Save any tickers not returning data to the exclude list.",
        ),
    ] = False,
    non_interactive: Annotated[
        bool, typer.Option("--non-interactive", help="Run in non-interactive mode.")
    ] = False,
):
    """Update the yfinace data for all stock tickers. See update --help for options."""
    global yf_parqed

    logger.debug("Updating stock data.")
    logger.debug(f"Supplied start and end dates:{[start_date, end_date]}")
    logger.debug(
        f"Supplied save-not-founds and non-interactive flags: {[save_not_found, non_interactive]}"
    )
    logger.debug(
        f"Checking if we are in update mode: {all([start_date is None, end_date is None])}"
    )
    if all([start_date is None, end_date is None]):
        yf_parqed.update_stock_data(update_only=True)
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
            start_date=start_date, end_date=end_date, update_only=False
        )
    logger.info("All tickers were processed.")
    if yf_parqed.new_not_found:
        logger.info("Some tickers did not return any data.")
        if non_interactive:
            if save_not_found:
                yf_parqed.save_not_founds()
                logger.info("Not found list updated.")
            else:
                logger.info("Not found list was not updated.")
            return
        else:
            if save_not_found:
                yf_parqed.save_not_founds()
                logger.info("Not found list updated.")
                return
            update_nf = typer.prompt(
                "".join(
                    "Do you want to update the not found list so ",
                    "they are not included in the future? (y/n)",
                ),
                default="y",
            )
            if update_nf.lower() == "y":
                yf_parqed.save_not_founds()
                logger.info("Not found list updated.")
            else:
                logger.info("Not found list not updated.")
