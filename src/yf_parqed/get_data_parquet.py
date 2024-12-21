import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import json
from tqdm import tqdm

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


session = CachedLimiterSession(
    limiter=Limiter(
        RequestRate(2, Duration.SECOND * 5)
    ),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)

not_found_path = Path(__file__).parent / "stock_data" / "not_found.json"
not_found = []
not_found_whole = []
if not_found_path.is_file():
    not_found_whole = json.loads(not_found_path.read_text())
    not_found = [x["ticker"] for x in not_found_whole]


def save_yf(df1, df2, data_path):
    if df2.empty:
        df2 = df1
    elif df1.empty:
        print("d1 empty.. nothing to do")
        return df2
    df1.reset_index(inplace=True)
    df2.reset_index(inplace=True)
    df2 = pd.concat([df2, df1], axis=0).drop_duplicates(
        subset=["date", "stock"], keep="last"
    )
    df2.to_parquet(data_path, index=False, compression="gzip")
    return df2.set_index(["stock", "date"])


def read_yf(data_path: Path):
    empty_df = pd.DataFrame(
        columns=["stock", "date", "open", "high", "low", "close", "volume", "sequence"]
    ).set_index(["stock", "date"])
    if data_path.is_file():
        df = pd.read_parquet(data_path)
        if df.empty:
            print(
                f"Empty dataframe found for {data_path.stem}, deleting it before looking for new data"
            )
            data_path.unlink()
            return empty_df
        df.set_index(["stock", "date"], inplace=True)
        return df
    else:
        return empty_df


def save_stock_data(
    stocks: list,
    start_date: datetime,
    end_date: datetime,
    interval: str = "1d",
    add_data: bool = True,
):
    data_path = Path(__file__).parent / "parquets" / f"stocks_{interval}.parquet"

    curr_df = read_yf(data_path)

    for stock in stocks:
        # print(f"Updating finance data for {stock}@{start_date}-{end_date}")
        df = get_yfinance_data(
            stock=stock, start_date=start_date, end_date=end_date, interval=interval
        )
        curr_df = save_yf(df, curr_df, data_path)


def update_stock_data(
    stocks: list, start_date: datetime, end_date: datetime, update_only: bool = True
):
    start_date_long = start_date - timedelta(5 * 365)
    for stock in tqdm(stocks, desc="Processing stocks"):
        save_single_stock_data(
            stock=stock,
            start_date=start_date,
            end_date=end_date,
            interval="1h",
            update_only=update_only,
        )
        save_single_stock_data(
            stock=stock,
            start_date=start_date_long,
            end_date=end_date,
            interval="1d",
            update_only=update_only,
        )


def save_single_stock_data(
    stock: str,
    start_date: datetime,
    end_date: datetime,
    interval: str = "1d",
    update_only: bool = True,
    not_found: list = not_found,
):
    # print(stock)
    data_path = (
        Path(__file__).parent / "parquets" / f"stocks_{interval}" / f"{stock}.parquet"
    )

    if stock in not_found:
        # print(f"{stock} is on the not found list, skipping...")
        return
    data_path.parent.mkdir(parents=True, exist_ok=True)
    df2 = read_yf(data_path)
    if not df2.empty:
        start_date = df2.index.get_level_values("date").max().to_pydatetime()
        # print(f"Updating {stock} from {start_date} to {end_date}")

    if (end_date - start_date).days > 0:
        df1 = get_yfinance_data(
            stock=stock, start_date=start_date, end_date=end_date, interval=interval
        )
        # print(df1)
        # print(df1.shape[0])
        if df1.shape[0] > 0:
            save_yf(df1, df2, data_path)
        else:
            print(
                f"{stock} returned no results for the date range of {start_date} to {end_date}.  Putting it on the not found list."
            )
            not_found_whole.append(
                {
                    "ticker": stock,
                    "start_date": datetime.now().strftime("%Y-%m-%d"),
                }
            )
            not_found.append(stock)
            not_found_path.write_text(json.dumps(not_found_whole, indent=4))

    return df2


def process_yfinance_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    df = df.rename_axis("date").reset_index()
    df["date"] = df["date"].dt.tz_localize(None)
    df.columns = [col.lower() for col in df.columns]
    df["stock"] = ticker
    return df[["date", "open", "high", "low", "close", "volume", "stock"]].set_index(
        ["stock", "date"]
    )


def get_yfinance_data(
    stock: str, start_date: datetime, end_date: datetime, interval: str = "1d"
) -> pd.DataFrame:
    # get the ticker
    ticker = yf.Ticker(stock, session=session)
    # print(f"interval: {interval}")
    if interval == "1h":
        # make sure the day limit is not reached
        today = datetime.now()
        if (today - start_date).days >= 730:
            start_date = today - timedelta(729)
            # print(f"start_date changed to: {start_date}")
        else:
            pass
            # print(f"start date within {(start_date - today).days} of today")
        if (today - end_date).days >= 730:
            end_date = today
            # print(f"end_date changed to: {end_date}")
        else:
            pass
            # print(f"end_date date within {(today - end_date).days} of today")

    df = ticker.history(start=start_date, end=end_date, interval=interval)
    if df.empty:
        print(
            f"{stock} returned no results for the date range of {start_date} to {end_date}."
        )
        return pd.DataFrame(
            columns=[
                "stock",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "sequence",
            ]
        ).set_index(["stock", "date"])
    return process_yfinance_data(df, stock)

    # df = ticker.history(start = start_date, end = end_date, interval = interval)

    # # convert date index to a date column
    # df = df.rename_axis("date").reset_index()
    # # remove timezone
    # df["date"] = [x.replace(tzinfo=None) for x in df["date"]]

    # # make columns lowercase
    # df.columns = [x.lower() for x in df.columns]
    # df = df[["date", "open", "high", "low", "close", "volume"]]
    # df["stock"] = stock
    # return df


def del_no_founds():
    """Cleanup function to remove all the files that are in the not found list"""
    not_founds = get_not_founds()
    nf_tickers = [x["ticker"] for x in not_founds]
    data_path = Path(__file__).parent.glob("stocks_*")
    for one_path in data_path:
        for one_nf in nf_tickers:
            nf_path = one_path / f"{one_nf}.pckl"
            if nf_path.is_file():
                print(f"Removing {nf_path.parent.name} / {nf_path.name}")
                nf_path.unlink()


def get_working_path(*, my_path: Path = None):
    if my_path is None:
        my_path = Path(__file__).parent
    return my_path


def get_new_list_of_stocks(*, my_path: Path = None):
    my_path = get_working_path(my_path=my_path)

    nasdaq_path = my_path / "nasdaq-listed.csv"
    nyse_path = my_path / "nyse-listed.csv"
    if not nasdaq_path.is_file() or not nyse_path.is_file():
        print("Nasdaq and/or Nyse file not found.  Nothing to do")
        return []

    nasdaq = [
        x
        for x in [
            y.split(",")[0]
            for y in nasdaq_path.read_text().split("\n")
            if len(y.strip()) > 0
        ]
        if x is not None or x != "" or not x.startswith("File Creation Time")
    ]

    nyse = [
        x
        for x in [
            y.split(",")[0]
            for y in nyse_path.read_text().split("\n")
            if len(y.strip()) > 0
        ]
        if x is not None or x != ""
    ]
    stocks = nasdaq[1:] + nyse[1:]
    stocks = [
        {
            "ticker": x,
            "added_date": datetime.now().strftime("%Y-%m-%d"),
        }
        for x in stocks
    ]
    return stocks


def get_current_list_of_stocks(*, my_path: Path = None):
    my_path = get_working_path(my_path=my_path)
    stocks = json.loads((my_path / "current_stocks.json").read_text())
    return stocks


def save_current_list_of_stocks(*, stocks: list, my_path: Path = None):
    my_path = get_working_path(my_path=my_path)
    not_founds = json.loads((my_path / "not_found.json").read_text())
    nf_tickers = [x["ticker"] for x in not_founds]
    stocks = [x for x in stocks if x["ticker"] not in nf_tickers]
    (my_path / "current_stocks.json").write_text(json.dumps(stocks, indent=4))


def update_current_list_of_stocks(*, stocks: list, my_path: Path = None):
    curr_stocks = get_current_list_of_stocks()
    curr_tickers = [x["ticker"] for x in curr_stocks]
    new_tickers = [x["ticker"] for x in stocks]
    for ticker in new_tickers:
        if ticker not in curr_tickers:
            curr_stocks.append(
                {"ticker": ticker, "added_date": datetime.now().strftime("%Y-%m-%d")}
            )
    save_current_list_of_stocks(stocks=curr_stocks, my_path=my_path)


def get_not_founds(*, my_path: Path = None):
    my_path = get_working_path(my_path=my_path)
    not_founds = json.loads((my_path / "not_found.json").read_text())
    return not_founds


def save_not_founds(*, not_founds: list, my_path: Path = None):
    my_path = get_working_path(my_path=my_path)
    (my_path / "not_found.json").write_text(json.dumps(not_founds, indent=4))


if __name__ == "__main__":
    pass
    # dow = ["SPY", 'AXP', 'AMGN', 'AAPL', 'BA',  'CAT', 'CSCO', 'CVX', 'GS', 'HD', 'HON', 'IBM', 'INTC',
    #     'JNJ', 'KO', 'JPM', 'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 'UNH',
    #     'CRM', 'VZ', 'WBA', 'WMT', 'DIS']
    # other = ['AMD', 'MU', 'ABT', 'AAL',    'BAC', 'PNC', 'C', 'EBAY', 'AMZN', 'GOOG',   'UAL', 'DAL', 'V',
    #     'FDX', 'MCD', 'PEP', 'SNAP', 'ATVIX', 'ANTMX',  'META',]  #
    # stocks = list(set(dow + other))

    #  del_no_founds()

    # my_path = Path(__file__).parent
    # with open(my_path / "nasdaq-listed.csv", "r") as fp:
    #     nasdaq = [ x.split(",")[0] for x in [y.split(",")[0] for y in fp.read().split("\n")] if x is not None or x != "" or not x.startswith("File Creation Time")]
    # with open(my_path / "nyse-listed.csv", "r") as fp:
    #     nyse = [ x.split(",")[0] for x in [ y.split(",")[0] for y in fp.read().split("\n")] if x is not None or x != ""]
    # stocks = nasdaq[1:] + nyse[1:]
    # stocks = [
    #     {
    #         "ticker": x,
    #         "added_date": "2024-12-18",
    #     } for x in stocks
    # ]
    # save_current_list_of_stocks(stocks=stocks)

    # nfs = get_not_founds()
    # nfs = [
    #     x["ticker"] for x in nfs
    # ]
    # save_not_founds(not_founds=nfs)

    # start_date = datetime(2007, 1, 1)
    # end_date = datetime(2022, 1, 1)
    # base_stock = ["AAPL", "SPY", "AAPB", "DRLL"]

    # end_date = datetime.now()s
    # start_date = end_date - timedelta(730)
    # update_stock_data(stocks=base_stock, start_date=start_date, end_date=end_date)

    # cnt = 0
    # start_date_long = start_date - timedelta(5*365)
    # for stock in stocks:
    #     save_single_stock_data(stock=stock, start_date=start_date, end_date=end_date, interval="1h")
    #     save_single_stock_data(stock=stock, start_date=start_date_long, end_date=end_date, interval="1d")
    #     cnt +=1
    #     if cnt % 500 == 0:
    #         print(cnt)

    # spy = get_yfinance_data(
    #     stock=base_stock,
    #     start_date=start_date,
    #     end_date=end_date,
    #     interval="1h"
    # )
    # theo_shape = spy.shape
    # print(theo_shape)
    # save_stock_data(stocks=stocks, start_date=start_date, end_date=end_date, interval="1d")

    # "V", "DAL",
