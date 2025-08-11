import typer
from pathlib import Path
import os
from loguru import logger
import sys
from datetime import datetime
from typing_extensions import Annotated
from typing import Tuple

from yf_parqed.primary_class import YFParqed, all_intervals


# remove the defult stderr log sink in loguru and add a new one with the log
# level set to INFO to limit the amount of output to the console
logger.remove()
logger.add(sys.stderr, level="INFO")

app = typer.Typer()
yf_parqed = YFParqed(my_path=Path(os.getcwd()))


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
    # get_tickers()
    yf_parqed.save_intervals(all_intervals)
    # tickers = yf_parqed.get_new_list_of_stocks()
    yf_parqed.save_current_list_of_stocks()
    yf_parqed.save_not_founds()


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
    logger.debug(f"Supplied start and end dates:{[start_date, end_date]}")
    logger.debug(
        f"Supplied save-not-founds and non-interactive flags: {[save_not_founds, non_interactive]}"
    )
    logger.debug(
        f"Checking if we are in update mode: {all([start_date is None, end_date is None])}"
    )
    if all([start_date is None, end_date is None]):
        yf_parqed.update_stock_data()  # update_only=True
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
            end_date=end_date,  # update_only=False
        )
    logger.info("All tickers were processed.")
    if yf_parqed.new_not_found:
        logger.info("Some tickers did not return any data.")
        if non_interactive:
            if save_not_founds:
                yf_parqed.save_not_founds()
                logger.info("Not found list updated.")
            else:
                logger.info("Not found list was not updated.")
            return
        else:
            if save_not_founds:
                yf_parqed.save_not_founds()
                logger.info("Not found list updated.")
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
                yf_parqed.save_not_founds()
                logger.info("Not found list updated.")
            else:
                logger.info("Not found list not updated.")


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
    logger.info("Not found list updated.")


@app.command()
def reparse_not_founds():
    """Reparse the not found list."""
    global yf_parqed

    yf_parqed.reparse_not_founds()
    logger.info("Not found list updated.")
