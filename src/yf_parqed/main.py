import typer
import httpx
from pathlib import Path
import os

from yf_parqed.get_data_parquet import (
    get_new_list_of_stocks,
    save_current_list_of_stocks,
    save_not_founds,
)


app = typer.Typer()

global wrk_dir
wrk_dir = Path(os.getcwd())


def download_file(url: str, local_path: Path):
    res = httpx.get(url, follow_redirects=True)
    print(res)
    local_path.write_text(res.text)


@app.callback()
def set_wd(
    wrk_dir: str = typer.Option(
        os.getcwd(), help="Working directory, default is current directory"
    ),
):
    wrk_dir = Path(wrk_dir)


def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed.csv"
    local_path = wrk_dir / "nasdaq-listed.csv"
    download_file(url, local_path)

    url = "https://datahub.io/core/nyse-other-listings/_r/-/data/nyse-listed.csv"
    local_path = wrk_dir / "nyse-listed.csv"
    download_file(url, local_path)


@app.command()
def initialize():
    get_tickers()
    tickers = get_new_list_of_stocks(my_path=wrk_dir)
    save_current_list_of_stocks(tickers, my_path=wrk_dir)
    save_not_founds([], my_path=wrk_dir)
