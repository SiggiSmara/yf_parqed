import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import json
from rich.progress import track
from loguru import logger
import os
import httpx

from requests import Session
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter


class LimiterSession(LimiterMixin, Session):
    pass


all_intervals = [
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


class YFParqed:
    def __init__(self, my_path: Path = None, my_intervals: list = []):
        self.my_path = None
        self.set_working_path(my_path)

        if len(my_intervals) == 0:
            self.load_intervals()
        else:
            self.my_intervals = my_intervals
            self.save_intervals(self.my_intervals)

        if len(self.my_intervals) == 0:
            # logger.error("No intervals found.  Please set the intervals.")
            raise ValueError("No intervals found.  Please set the intervals.")

        self.new_not_found = False
        self.set_limiter()

    def set_working_path(self, my_path: Path):
        if my_path is None:
            self.my_path = Path.cwd()
        else:
            self.my_path = my_path
        self.update_meta_after_path_change()
        return self.my_path

    def update_meta_after_path_change(self):
        self.current_stocks_path = self.my_path / "current_tickers.json"
        self.not_founds_path = self.my_path / "not_found_tickers.json"
        self.intervals_path = self.my_path / "intervals.json"
        self.load_current_not_founds()
        self.load_current_list_of_stocks()

    def set_limiter(self, max_requests: int = 2, duration: int = 1):
        logger.info(
            f"Ratelimiting set to {max_requests} max requests per {duration} seconds"
        )
        self.session = LimiterSession(
            limiter=Limiter(
                RequestRate(max_requests, Duration.SECOND * duration)
            ),  # max 1 requests per 1 second
            bucket_class=MemoryQueueBucket,
        )

    def load_intervals(self):
        if self.intervals_path.is_file():
            self.my_intervals = json.loads(self.intervals_path.read_text())
        else:
            self.my_intervals = []
        logger.debug(f"Intervals loaded: {self.my_intervals}")

    def save_intervals(self, intervals: list):
        self.intervals_path.write_text(json.dumps(intervals, indent=4))
        self.my_intervals = intervals

    def add_interval(self, interval: str):
        self.my_intervals.append(interval)
        self.save_intervals(self.my_intervals)

    def remove_interval(self, interval: str):
        self.my_intervals = [x for x in self.my_intervals if x != interval]
        self.save_intervals(self.my_intervals)

    def download_file(self, url: str, local_path: Path):
        res = httpx.get(url, follow_redirects=True)
        local_path.write_text(res.text)

    def get_tickers(self):
        url = "https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed.csv"
        local_path_nasdaq = self.my_path / "nasdaq-listed.csv"
        self.download_file(url, local_path_nasdaq)

        url = "https://datahub.io/core/nyse-other-listings/_r/-/data/nyse-listed.csv"
        local_path_nyse = self.my_path / "nyse-listed.csv"
        self.download_file(url, local_path_nyse)
        return local_path_nasdaq, local_path_nyse

    def get_new_list_of_stocks(self, download_tickers: bool = True) -> dict:
        if download_tickers:
            nasdaq_path, nyse_path = self.get_tickers()
        else:
            nasdaq_path = self.my_path / "nasdaq-listed.csv"
            nyse_path = self.my_path / "nyse-listed.csv"
        if not nasdaq_path.is_file() or not nyse_path.is_file():
            logger.debug("Nasdaq and/or Nyse file not found.  Nothing to do")
            return {}

        nasdaq = [
            x
            for x in [
                y.split(",")[0]
                for y in nasdaq_path.read_text().split("\n")
                if len(y.strip()) > 0
            ]
            if x is not None and x != "" and not x.startswith("File")
        ]

        nyse = [
            x
            for x in [
                y.split(",")[0]
                for y in nyse_path.read_text().split("\n")
                if len(y.strip()) > 0
            ]
            if x is not None and x != ""
        ]
        stocks = list(set(nasdaq[1:] + nyse[1:]))
        stocks = {
            x: {
                "ticker": x,
                "added_date": datetime.now().strftime("%Y-%m-%d"),
            }
            for x in stocks
        }
        return stocks

    def load_current_list_of_stocks(self):
        if self.current_stocks_path.is_file():
            self.stocks = json.loads(self.current_stocks_path.read_text())
        else:
            self.stocks = {}

    def save_current_list_of_stocks(self):
        stocks = {
            key: value
            for key, value in self.stocks.items()
            if key not in self.not_founds
        }
        self.current_stocks_path.write_text(json.dumps(stocks, indent=4))

    def update_current_list_of_stocks(self):
        new_tickers = self.get_new_list_of_stocks()
        for ticker, value in new_tickers.items():
            if ticker not in self.stocks:
                self.stocks[ticker] = value
        self.save_current_list_of_stocks()

    def load_current_not_founds(self):
        if self.not_founds_path.is_file():
            self.not_founds = json.loads(self.not_founds_path.read_text())
        else:
            self.not_founds = {}

    def save_not_founds(self):
        self.not_founds_path.write_text(json.dumps(self.not_founds, indent=4))

    def confirm_not_founds(self):
        logger.debug("Confirming not found tickers")
        new_not_founds_whole = {}
        logger.info(f"Number of not found tickers: {len(self.not_founds)}")
        for stock, meta_data in track(
            self.not_founds.items(), "Re-checking not-founds..."
        ):
            meta_data["last_checked"] = datetime.now().strftime("%Y-%m-%d")
            ticker = yf.Ticker(stock, session=self.session)
            hist = ticker.history(period="1d")
            if hist.empty:
                logger.debug(f"{stock} is not found.")
            else:
                logger.debug(f"{stock} is found.")
                last_date = hist.index[-1].to_pydatetime()
                meta_data["last_date_found"] = last_date.strftime("%Y-%m-%d")

            new_not_founds_whole[stock] = meta_data

        self.not_founds = new_not_founds_whole
        self.save_not_founds()
        self.reparse_not_founds()
        self.load_current_not_founds()

    def reparse_not_founds(self):
        # logger.debug("Re-parsing not found tickers")
        new_not_founds_whole = {}
        logger.info(f"Number of not found tickers: {len(self.not_founds)}")
        for ticker, meta_data in track(
            self.not_founds.items(), "Re-parsing not-founds..."
        ):
            if meta_data.get("last_date_found") is not None:
                stock_meta = {
                    "ticker": ticker,
                    "added_date": meta_data["last_checked"],
                }
                if ticker not in self.stocks:
                    logger.info(
                        f"Adding {meta_data['ticker']} to the current stocks list."
                    )
                    self.stocks[ticker] = stock_meta
            else:
                new_not_founds_whole[ticker] = meta_data
        self.not_founds = new_not_founds_whole

        self.save_not_founds()
        self.load_current_not_founds()
        self.save_current_list_of_stocks()

    def save_yf(self, df1, df2, data_path):
        if df2.empty:
            df2 = df1
        elif df1.empty:
            logger.debug("d1 empty.. nothing to do")
            return df2
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df2 = pd.concat([df2, df1], axis=0).drop_duplicates(
            subset=["date", "stock"], keep="last"
        )
        df2.to_parquet(data_path, index=False, compression="gzip")
        return df2.set_index(["stock", "date"])

    def read_yf(self, data_path: Path):
        empty_df = pd.DataFrame(
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
        if data_path.is_file():
            df = pd.read_parquet(data_path)
            if df.empty:
                logger.debug(
                    f"Empty dataframe found for {data_path.stem}, deleting it before looking for new data"
                )
                data_path.unlink()
                return empty_df
            df.set_index(["stock", "date"], inplace=True)
            return df
        else:
            return empty_df

    def update_stock_data(
        self,
        start_date: datetime = None,
        end_date: datetime = None,
        # update_only: bool = True,
    ):
        self.new_not_found = False
        my_stocks = [
            ticker for ticker in self.stocks.keys() if ticker not in self.not_founds
        ]
        logger.info(f"Number of tickers to process: {len(my_stocks)}")
        logger.info(f"Number of tickers in exclude list: {len(self.not_founds)}")
        disable_track = not (os.getenv("YF_PARQED_LOG_LEVEL", "INFO") == "INFO")

        # set the end date to now if we are updating in order to
        # have the same end date for the entire dataset
        if end_date is None:
            end_date = datetime.now()
        for stock in track(
            my_stocks, description="Processing stocks", disable=disable_track
        ):
            for interval in self.my_intervals:
                self.save_single_stock_data(
                    stock=stock,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                    # update_only=update_only,
                )

    def save_single_stock_data(
        self,
        stock: str,
        start_date: datetime = None,
        end_date: datetime = None,
        interval: str = "1d",
        # update_only: bool = True,
    ):
        logger.debug(stock)
        data_path = self.my_path / f"stocks_{interval}" / f"{stock}.parquet"

        if stock in self.not_founds:
            logger.debug(f"{stock} is in the not found list, skipping")
            return

        # if end_date is None:
        #     end_date = datetime.now()

        data_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Data path: {data_path}")

        load_all = False
        df2 = self.read_yf(data_path)
        if df2.empty:
            logger.debug(f"Empty dataframe found for {stock}, will try to get new data")
            # start_date = datetime.now()
            load_all = True
        elif start_date is None:
            start_date = df2.index.get_level_values("date").max().to_pydatetime()

        if all((start_date is None, end_date is None)):
            start_date = datetime.now()
            end_date = datetime.now()
            load_all = True

        if load_all or (end_date - start_date).days > 0:
            logger.debug(f"Reading {stock} from {start_date} to {end_date}")
            df1 = self.get_yfinance_data(
                stock=stock,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                get_all=load_all,
            )
            if not df1.empty:
                self.save_yf(df1, df2, data_path)
            else:
                logger.debug(
                    f"{stock} returned no results for the date range of {start_date} to {end_date} and load_all:{load_all}.  Putting it on the not found list."
                )
                self.not_founds[stock] = {
                    "ticker": stock,
                    "added_date": datetime.now().strftime("%Y-%m-%d"),
                }

                self.new_not_found = True
        else:
            logger.debug(f"{stock} is up to date.")

    def process_yfinance_data(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        df = df.rename_axis("date").reset_index()
        df["date"] = df["date"].dt.tz_localize(None)
        df.columns = [col.lower() for col in df.columns]
        df["stock"] = ticker
        return df[
            ["date", "open", "high", "low", "close", "volume", "stock"]
        ].set_index(["stock", "date"])

    def get_yfinance_data(
        self,
        stock: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        get_all: bool = False,
    ) -> pd.DataFrame:
        ticker = yf.Ticker(stock, session=self.session)
        if not get_all:
            if interval in ("60m", "90m", "1h"):
                # make sure the day limit is not reached
                today = datetime.now()
                if (today - start_date).days >= 730:
                    start_date = today - timedelta(729)

                if (today - end_date).days >= 730:
                    end_date = today

            if interval in ("1m", "2m", "5m", "15m", "30m"):
                # make sure the 6 day limit is not reached
                today = datetime.now()

                if (today - start_date).days >= 6:
                    start_date = today - timedelta(6)

                if (today - end_date).days >= 6:
                    end_date = today
            logger.debug(
                f"Getting {stock} from {start_date} to {end_date} with {interval}"
            )
            # logger.debug(ticker.info)
            df = ticker.history(start=start_date, end=end_date, interval=interval)
        else:
            logger.debug(f"Getting {stock} all data")
            period = "10y"
            if interval in ("60m", "90m", "1h"):
                period = "5y"
            elif interval in ("1m", "2m", "5m", "15m", "30m"):
                period = "8d"
            df = ticker.history(period=period, interval=interval)
        logger.debug(
            f"{stock} returned {df.shape[0]} result(s) for interval {interval} the date range of {start_date} to {end_date}."
        )
        # logger.debug(df.head())
        if df.empty:
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
        return self.process_yfinance_data(df, stock)
