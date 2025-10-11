import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from loguru import logger
import httpx
import time


# from requests.exceptions import HTTPError
# from requests_ratelimiter import LimiterMixin


# class LimiterSession(LimiterMixin, Session):
#     pass


from .config_service import ConfigService
from .data_fetcher import DataFetcher
from .ticker_registry import TickerRegistry
from .interval_scheduler import IntervalScheduler
from .storage_backend import StorageBackend


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
        self.config = ConfigService(my_path)
        self.call_list = []

        self._sync_paths()

        # Initialize registry early (before load_tickers is called)
        # Will be re-initialized with callbacks after limiter setup
        self.registry = TickerRegistry(config=self.config)
        self.load_tickers()

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

        # Re-initialize registry with callbacks for not-found maintenance
        self.registry = TickerRegistry(
            config=self.config,
            initial_tickers=self.registry.tickers,  # Preserve loaded tickers
            limiter=lambda: self.enforce_limits(),
            fetch_callback=self._fetch_for_not_found_check,
        )

        self.data_fetcher = DataFetcher(
            limiter=lambda: self.enforce_limits(),
            today_provider=lambda: self.get_today(),
            empty_frame_factory=self._empty_price_frame,
        )
        self.storage = StorageBackend(
            empty_frame_factory=self._empty_price_frame,
            normalizer=self._normalize_price_frame,
            column_provider=self._price_frame_columns,
        )
        self.scheduler = IntervalScheduler(
            registry=self.registry,
            intervals=lambda: list(self.my_intervals),
            loader=lambda: self.load_tickers(),
            limiter=lambda: self.enforce_limits(),
            processor=lambda stock,
            start_date,
            end_date,
            interval: self.save_single_stock_data(
                stock=stock,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
            ),
            today_provider=lambda: self.get_today(),
        )

    def _sync_paths(self):
        self.my_path = self.config.base_path
        self.tickers_path = self.config.tickers_path
        self.intervals_path = self.config.intervals_path

    def set_working_path(self, my_path: Path):
        new_path = self.config.set_working_path(my_path)
        self._sync_paths()
        self.load_tickers()
        return new_path

    def update_meta_after_path_change(self):
        self._sync_paths()
        self.load_tickers()

    @property
    def tickers(self) -> dict:
        return self.registry.tickers

    @tickers.setter
    def tickers(self, value: dict) -> None:
        self.registry.replace(value)

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
        max_requests, duration = self.config.configure_limits(max_requests, duration)
        self.max_requests = max_requests
        self.duration = duration
        if self.max_requests == 1:
            self.limit_check_idx = 0
        else:
            self.limit_check_idx = 1

    def _fetch_for_not_found_check(
        self, ticker: str, interval: str, period: str
    ) -> tuple[bool, datetime | None]:
        """Fetch data for not-found ticker confirmation.

        Returns:
            Tuple of (found_data: bool, last_date: datetime | None)
        """

        ticker_obj = yf.Ticker(ticker)
        hist = ticker_obj.history(period=period)

        if not hist.empty:
            last_date = hist.index[-1].to_pydatetime()
            return True, last_date
        return False, None

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
        self.my_intervals = self.config.load_intervals()
        logger.debug(f"Intervals loaded: {self.my_intervals}")

    def save_intervals(self, intervals: list):
        self.my_intervals = self.config.save_intervals(intervals)

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
        self.registry.load()

    def save_tickers(self):
        self.registry.save()

    def update_current_list_of_stocks(self):
        new_tickers = self.get_new_list_of_stocks()
        self.registry.update_current_list(new_tickers)
        self.save_tickers()

    def is_ticker_active_for_interval(self, ticker: str, interval: str) -> bool:
        """
        Check if a ticker should be processed for a given interval.
        Returns False if ticker is globally not found or if interval-specific
        data suggests it's not trading in this timeframe.
        """
        return self.registry.is_active_for_interval(ticker, interval)

    def update_ticker_interval_status(
        self,
        ticker: str,
        interval: str,
        found_data: bool,
        last_date: datetime | None = None,
    ):
        """
        Update the status of a ticker for a specific interval.

        Args:
            ticker: The ticker symbol
            interval: The trading interval (1d, 1h, etc.)
            found_data: Whether data was found for this ticker/interval
            last_date: Last date with data (if found_data is True)
        """
        self.registry.update_ticker_interval_status(
            ticker=ticker,
            interval=interval,
            found_data=found_data,
            last_date=last_date,
        )

    def confirm_not_founds(self):
        """Delegate to registry for not-found ticker confirmation."""
        self.registry.confirm_not_founds()

    def reparse_not_founds(self):
        """Delegate to registry for not-found ticker reactivation."""
        self.registry.reparse_not_founds()

    def save_yf(self, df1, df2, data_path):
        return self.storage.save(df1, df2, data_path)

    def read_yf(self, data_path: Path):
        return self.storage.read(data_path)

    def update_stock_data(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        self.new_not_found = False
        self.scheduler.run(start_date=start_date, end_date=end_date)

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
            df1 = self.data_fetcher.fetch(
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
