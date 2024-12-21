import typer
import httpx
from pathlib import Path
import os

from yf_parqed.primary_class import YFParqed

app = typer.Typer()

global wrk_dir
wrk_dir = Path(os.getcwd())
yf_parqed = YFParqed(my_path=wrk_dir)


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
        "1d",
        "1h",
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
):
    """
    Persistent storage of yfinance ticker data in parquet.

    Use --limits to set the rate limiting for the API requests.

    Use --wrk_dir to set the working directory.
    """
    global yf_parqed
    wrk_dir = Path(wrk_dir)
    yf_parqed.set_working_path(wrk_dir)
    yf_parqed.set_limiter(max_requests=limits[0], duration=limits[1])


@app.command()
def initialize():
    global yf_parqed
    get_tickers()
    define_intervals()
    tickers = yf_parqed.get_new_list_of_stocks()
    yf_parqed.save_current_list_of_stocks(tickers)
    yf_parqed.save_not_founds([])


@app.command()
def add_interval(interval: str):
    global yf_parqed
    yf_parqed.add_interval(interval)


@app.command()
def remove_interval(interval: str):
    global yf_parqed
    yf_parqed.remove_interval(interval)
