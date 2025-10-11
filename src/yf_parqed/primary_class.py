import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import json
from rich.progress import track
from loguru import logger
import os
import httpx
import time


# from requests.exceptions import HTTPError
from curl_cffi.requests.exceptions import HTTPError
# from requests_ratelimiter import LimiterMixin


# class LimiterSession(LimiterMixin, Session):
#     pass


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
    def __init__(self, my_path: Path = Path.cwd(), my_intervals: list = []):
        self.my_path = Path()
        self.set_working_path(my_path)

        self.call_list = []

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
        self.tickers_path = self.my_path / "tickers.json"
        self.intervals_path = self.my_path / "intervals.json"
        self.load_tickers()

    @staticmethod
    def _price_frame_columns() -> list[str]:
        return [
            "stock",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "sequence",
        ]

    @classmethod
    def _empty_price_frame(cls) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "stock": pd.Series(dtype="string"),
                "date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="Int64"),
                "sequence": pd.Series(dtype="Int64"),
            }
        ).set_index(["stock", "date"])

    @classmethod
    def _normalize_price_frame(cls, df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = cls._price_frame_columns()
        normalized = df.copy()

        for column in expected_cols:
            if column not in normalized.columns:
                if column in {"open", "high", "low", "close"}:
                    normalized[column] = pd.Series(dtype="float64")
                elif column in {"volume", "sequence"}:
                    normalized[column] = pd.Series(dtype="Int64")
                elif column == "date":
                    normalized[column] = pd.Series(dtype="datetime64[ns]")
                else:
                    normalized[column] = pd.Series(dtype="string")

        normalized["stock"] = normalized["stock"].astype("string")
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")

        for price_col in ["open", "high", "low", "close"]:
            normalized[price_col] = pd.to_numeric(
                normalized[price_col], errors="coerce"
            ).astype("float64")

        for int_col in ["volume", "sequence"]:
            numeric_series = pd.to_numeric(normalized[int_col], errors="coerce")
            normalized[int_col] = numeric_series.round().astype("Int64")

        normalized = normalized[expected_cols]
        return normalized

    def set_limiter(self, max_requests: int = 3, duration: int = 2):
        logger.info(
            f"Ratelimiting set to max {max_requests} requests per {duration} seconds"
        )
        self.max_requests = max_requests
        self.duration = duration
        if self.max_requests == 1:
            self.limit_check_idx = 0
        else:
            self.limit_check_idx = 1

        # self.session = LimiterSession(
        #     limiter=Limiter(
        #         RequestRate(max_requests, Duration.SECOND * duration)
        #     ),  # max 1 requests per 1 second
        #     bucket_class=MemoryQueueBucket,
        # )

    def enforce_limits(self):
        logger.debug(f"Enforcing limits: {len(self.call_list)} calls in the list")
        now = datetime.now()
        if self.call_list == []:
            logger.debug("Call list is empty, adding now")
            self.call_list.append(now)
        else:
            logger.debug(f"Now: {now.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.debug(
                f"Max call list: {max(self.call_list).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            delta = (now - max(self.call_list)).total_seconds()
            logger.debug(f"Delta: {delta} seconds")
            sleepytime = self.duration / self.max_requests
            logger.debug(f"Sleepytime: {sleepytime} seconds")
            logger.debug(f"delta < sleepytime: {delta < sleepytime}")
            if delta < sleepytime:
                logger.debug(f"Sleeping for {sleepytime - delta} seconds.")
                time.sleep(sleepytime - delta)
                logger.debug("Calling enforce_limits again after waking up.")
                self.enforce_limits()
            # if delta < self.duration and len(self.call_list) >= self.max_requests:
            #     # sleepytime = self.duration - (now - self.call_list[self.limit_check_idx ]).total_seconds() + 0.05
            #     logger.debug(
            #         f"Sleeping for {sleepytime} seconds.  Len call list => max_requests."
            #     )
            #     time.sleep(sleepytime)
            # elif delta < self.duration:
            #     # sleepytime = delta / self.max_requests
            #     logger.debug(
            #         f"Sleeping for {sleepytime} seconds.  Len call list < max_requests."
            #     )
            #     time.sleep(sleepytime)
            else:
                logger.debug(f"Adding {now} to the call list")
                self.call_list.append(now)
                logger.debug(f"Len call list: {len(self.call_list)}")
                if len(self.call_list) > self.max_requests:
                    self.call_list.pop(0)

    def business_days_between(self, start: datetime, end: datetime) -> int:
        delta = (end - start).days
        logger.debug(f"initial delta: {delta}")
        business_days = sum(
            1 for i in range(delta + 1) if (start + timedelta(days=i)).weekday() < 5
        )
        logger.debug([(start + timedelta(days=i)).weekday() for i in range(delta + 1)])
        if start.weekday() < 5:
            business_days -= 1
        logger.debug(f"final delta: {business_days}")
        return business_days

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
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
            for x in stocks
        }
        return stocks

    def load_tickers(self):
        if self.tickers_path.is_file():
            self.tickers = json.loads(self.tickers_path.read_text())
        else:
            self.tickers = {}

    def save_tickers(self):
        self.tickers_path.write_text(json.dumps(self.tickers, indent=4))

    def update_current_list_of_stocks(self):
        new_tickers = self.get_new_list_of_stocks()
        for ticker, value in new_tickers.items():
            if ticker not in self.tickers:
                self.tickers[ticker] = value
            elif self.tickers[ticker].get("status") == "not_found":
                # Keep existing metadata but mark as active
                self.tickers[ticker]["status"] = "active"
                # Preserve interval data
                if "intervals" not in self.tickers[ticker]:
                    self.tickers[ticker]["intervals"] = {}

        self.save_tickers()

    def is_ticker_active_for_interval(self, ticker: str, interval: str) -> bool:
        """
        Check if a ticker should be processed for a given interval.
        Returns False if ticker is globally not found or if interval-specific
        data suggests it's not trading in this timeframe.
        """
        if ticker not in self.tickers:
            return True  # New ticker, should try

        ticker_data = self.tickers[ticker]

        # Global not found status
        if ticker_data.get("status") == "not_found":
            return False

        # Check interval-specific status
        interval_data = ticker_data.get("intervals", {}).get(interval, {})

        # If we have interval data, check if it's marked as not found for this interval
        if interval_data.get("status") == "not_found":
            # Check if we should retry (e.g., after 30 days)
            last_not_found = interval_data.get("last_not_found_date")
            if last_not_found:
                try:
                    last_date = datetime.strptime(last_not_found, "%Y-%m-%d")
                    days_since = (datetime.now() - last_date).days
                    if days_since < 30:  # Don't retry for 30 days
                        return False
                except ValueError:
                    pass  # If date parsing fails, proceed with processing

        return True

    def update_ticker_interval_status(
        self, ticker: str, interval: str, found_data: bool, last_date: datetime = None
    ):
        """
        Update the status of a ticker for a specific interval.

        Args:
            ticker: The ticker symbol
            interval: The trading interval (1d, 1h, etc.)
            found_data: Whether data was found for this ticker/interval
            last_date: Last date with data (if found_data is True)
        """
        current_date = datetime.now().strftime("%Y-%m-%d")

        if ticker not in self.tickers:
            self.tickers[ticker] = {
                "ticker": ticker,
                "added_date": current_date,
                "status": "active",
                "last_checked": current_date,
                "intervals": {},
            }

        # Ensure intervals structure exists
        if "intervals" not in self.tickers[ticker]:
            self.tickers[ticker]["intervals"] = {}

        if interval not in self.tickers[ticker]["intervals"]:
            self.tickers[ticker]["intervals"][interval] = {}

        interval_data = self.tickers[ticker]["intervals"][interval]

        if found_data:
            # Data was found for this interval
            interval_data["status"] = "active"
            interval_data["last_found_date"] = current_date
            if last_date:
                interval_data["last_data_date"] = last_date.strftime("%Y-%m-%d")
            interval_data["last_checked"] = current_date

            # Update global ticker status
            self.tickers[ticker]["status"] = "active"
            self.tickers[ticker]["last_checked"] = current_date

        else:
            # No data found for this interval
            interval_data["status"] = "not_found"
            interval_data["last_not_found_date"] = current_date
            interval_data["last_checked"] = current_date

            # Update global last_checked but don't change global status unless all intervals fail
            self.tickers[ticker]["last_checked"] = current_date

            # Check if ALL intervals are not found - if so, mark ticker as globally not found
            all_intervals_not_found = True
            for int_name, int_data in self.tickers[ticker]["intervals"].items():
                if int_data.get("status") != "not_found":
                    all_intervals_not_found = False
                    break

            if all_intervals_not_found and len(self.tickers[ticker]["intervals"]) > 0:
                self.tickers[ticker]["status"] = "not_found"

    def confirm_not_founds(self):
        logger.debug("Confirming not found tickers")
        not_found_tickers = {
            ticker: data
            for ticker, data in self.tickers.items()
            if data.get("status") == "not_found"
        }

        logger.info(f"Number of not found tickers: {len(not_found_tickers)}")
        for stock, meta_data in track(
            not_found_tickers.items(), "Re-checking not-founds..."
        ):
            self.enforce_limits()
            current_date = datetime.now().strftime("%Y-%m-%d")
            meta_data["last_checked"] = current_date

            try:
                ticker = yf.Ticker(stock)
                hist = ticker.history(period="1d")
                if not hist.empty:
                    logger.debug(f"{stock} is found.")
                    last_date = hist.index[-1].to_pydatetime()

                    # Update for 1d interval specifically
                    self.update_ticker_interval_status(stock, "1d", True, last_date)
                else:
                    logger.debug(f"{stock} is not found.")

            except HTTPError as e:
                status_code = None
                if hasattr(e, "response"):
                    status_code = e.response.status_code
                logger.error(
                    f"Error getting data for {stock}: HTTP {status_code} - {str(e)}, most likely not available anymore."
                )

        self.save_tickers()
        self.reparse_not_founds()

    def reparse_not_founds(self):
        not_found_tickers = {
            ticker: data
            for ticker, data in self.tickers.items()
            if data.get("status") == "not_found"
        }

        logger.info(f"Number of not found tickers: {len(not_found_tickers)}")
        for ticker, meta_data in track(
            not_found_tickers.items(), "Re-parsing not-founds..."
        ):
            # Check if any interval has recent data
            has_recent_data = False
            intervals_data = meta_data.get("intervals", {})

            for interval_name, interval_data in intervals_data.items():
                if interval_data.get("status") == "active":
                    # Check if the data is recent (within last 90 days)
                    last_found = interval_data.get("last_found_date")
                    if last_found:
                        try:
                            last_date = datetime.strptime(last_found, "%Y-%m-%d")
                            days_since = (datetime.now() - last_date).days
                            if days_since <= 90:
                                has_recent_data = True
                                break
                        except ValueError:
                            continue

            if has_recent_data:
                # Reactivate ticker
                stock_meta = {
                    "ticker": ticker,
                    "added_date": meta_data.get(
                        "added_date", datetime.now().strftime("%Y-%m-%d")
                    ),
                    "status": "active",
                    "last_checked": datetime.now().strftime("%Y-%m-%d"),
                    "intervals": meta_data.get("intervals", {}),
                }
                logger.info(f"Reactivating {ticker} - found recent data in intervals.")
                self.tickers[ticker] = stock_meta

        self.save_tickers()

    def save_yf(self, df1, df2, data_path):
        if df1.empty and df2.empty:
            return self._empty_price_frame()

        if df1.empty:
            logger.debug("d1 empty.. nothing to do")
            return df2

        frames = []
        if not df2.empty:
            frames.append(df2.reset_index())
        frames.append(df1.reset_index())

        combined = pd.concat(frames, axis=0, ignore_index=True)
        combined = self._normalize_price_frame(combined)

        combined = combined.sort_values(["stock", "date", "sequence"], kind="mergesort")
        combined = combined.drop_duplicates(subset=["stock", "date"], keep="last")
        combined = combined.sort_values(["stock", "date"], kind="mergesort")

        combined.to_parquet(data_path, index=False, compression="gzip")
        return combined.set_index(["stock", "date"])

    def read_yf(self, data_path: Path):
        empty_df = self._empty_price_frame()
        if data_path.is_file():
            try:
                df = pd.read_parquet(data_path)
            except (ValueError, FileNotFoundError, OSError):
                logger.debug(
                    f"Unable to read parquet file for {data_path.stem}, deleting corrupt file"
                )
                try:
                    data_path.unlink(missing_ok=True)
                except TypeError:
                    if data_path.exists():
                        data_path.unlink()
                return empty_df

            required = set(self._price_frame_columns())
            if df.empty or not required.issubset(df.columns):
                logger.debug(
                    f"Invalid dataframe schema for {data_path.stem}, deleting file before rehydrating"
                )
                try:
                    data_path.unlink(missing_ok=True)
                except TypeError:
                    if data_path.exists():
                        data_path.unlink()
                return empty_df

            df = self._normalize_price_frame(df)
            df.set_index(["stock", "date"], inplace=True)
            return df
        else:
            return empty_df

    def update_stock_data(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        self.new_not_found = False
        # Load current data
        self.load_tickers()

        my_stocks = [
            ticker
            for ticker, data in self.tickers.items()
            if data.get("status", "active") == "active"
        ]
        not_found_count = len(
            [
                ticker
                for ticker, data in self.tickers.items()
                if data.get("status") == "not_found"
            ]
        )

        logger.info(f"Number of tickers to process: {len(my_stocks)}")
        logger.info(f"Number of tickers in exclude list: {not_found_count}")
        disable_track = not (os.getenv("YF_PARQED_LOG_LEVEL", "INFO") == "INFO")

        # set the end date to now if we are updating in order to
        # have the same end date for the entire dataset
        if end_date is None:
            end_date = self.get_today()

        for interval in self.my_intervals:
            # Filter stocks that should be processed for this interval
            interval_stocks = [
                stock
                for stock in my_stocks
                if self.is_ticker_active_for_interval(stock, interval)
            ]

            logger.info(
                f"Processing {len(interval_stocks)} tickers for interval {interval}"
            )

            for stock in track(
                interval_stocks,
                description=f"Processing stocks for interval:{interval}",
                disable=disable_track,
            ):
                self.enforce_limits()
                self.save_single_stock_data(
                    stock=stock,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                )

    def save_single_stock_data(
        self,
        stock: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        interval: str = "1d",
    ):
        logger.debug(stock)
        data_path = self.my_path / f"stocks_{interval}" / f"{stock}.parquet"

        # Check if stock should be processed for this interval
        if not self.is_ticker_active_for_interval(stock, interval):
            logger.debug(f"{stock} is not active for interval {interval}, skipping")
            return

        data_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Data path: {data_path}")

        load_all = False
        df2 = self.read_yf(data_path)
        if df2.empty:
            logger.debug(f"Empty dataframe found for {stock}, will try to get new data")
            load_all = True
        elif start_date is None:
            start_date = df2.index.get_level_values("date").max().to_pydatetime()

        if end_date is None:
            end_date = self.get_today()

        if start_date is None:
            start_date = self.get_today()
            load_all = True

        if load_all or self.business_days_between(start=start_date, end=end_date) > 0:
            logger.debug(
                f"Reading {stock} from {start_date} to {end_date} and {load_all} load_all and {self.business_days_between(start=start_date, end=end_date)} business days"
            )
            df1 = self.get_yfinance_data(
                stock=stock,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                get_all=load_all,
            )
            if not df1.empty:
                last_data_date = (
                    df1.index.get_level_values("date").max().to_pydatetime()
                )
                self.save_yf(df1, df2, data_path)

                # Update ticker status - data found for this interval
                self.update_ticker_interval_status(
                    stock, interval, True, last_data_date
                )

            else:
                logger.debug(
                    f"{stock} returned no results for the date range of {start_date} to {end_date} and load_all:{load_all} for interval {interval}."
                )

                # Update ticker status - no data found for this interval
                self.update_ticker_interval_status(stock, interval, False)
                self.new_not_found = True
        else:
            logger.debug(f"{stock} is up to date for interval {interval}.")

    def process_yfinance_data(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        df = df.rename_axis("date").reset_index()
        df["date"] = df["date"].dt.tz_localize(None)
        df.columns = [col.lower() for col in df.columns]
        df["stock"] = ticker
        return df[
            ["date", "open", "high", "low", "close", "volume", "stock"]
        ].set_index(["stock", "date"])

    def get_today(self) -> datetime:
        # get the now datetime
        today = datetime.now()
        # if today is saturday or sunday set the date to the last weekday of the same week
        if today.weekday() > 4:
            today = today - timedelta(days=today.weekday() - 4)
        # set the time to 23:59:59
        today = today.replace(hour=17, minute=00, second=00, microsecond=0)
        logger.debug(today)
        return today

    def get_yfinance_data(
        self,
        stock: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        get_all: bool = False,
    ) -> pd.DataFrame:
        ticker = yf.Ticker(stock)  # , session=self.session)
        if not get_all:
            # make sure the day limit is not reached
            today = self.get_today()
            if interval in ("60m", "90m", "1h"):
                if (today - start_date).days >= 729:
                    start_date = today - timedelta(729)
                    start_date = start_date.replace(
                        hour=8, minute=0, second=0, microsecond=0
                    )

                if (today - end_date).days >= 729:
                    end_date = today

                if (end_date - start_date).days >= 729:
                    logger.error(
                        f"The date range is too large for this interval and I can't fix it: {end_date} - {start_date}"
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

            if interval in ("1m", "2m", "5m", "15m", "30m"):
                if (today - start_date).days >= 7:
                    start_date = today - timedelta(7)
                    start_date = start_date.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )

                if (today - end_date).days >= 7:
                    end_date = today
            logger.debug(
                f"Getting {stock} from {start_date} to {end_date} with {interval}"
            )
            # logger.debug(ticker.info)
            try:
                df = ticker.history(start=start_date, end=end_date, interval=interval)
            except Exception as e:
                logger.error(f"Error getting data for {stock}: {e}")
                df = pd.DataFrame(
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
                )
        else:
            period = "10y"
            if interval in ("60m", "90m", "1h"):
                period = "729d"
            elif interval in ("1m", "2m", "5m", "15m", "30m"):
                period = "8d"
            logger.debug(
                f"Getting {stock} all data with period {period} and interval {interval}"
            )
            try:
                df = ticker.history(period=period, interval=interval)
            except HTTPError as e:
                logger.error(f"Error getting data for {stock}: {e}")
                df = pd.DataFrame(
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
                )

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
