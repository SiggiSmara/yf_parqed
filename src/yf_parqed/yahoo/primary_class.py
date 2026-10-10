import csv
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Sequence

import yfinance as yf
import pandas as pd
from loguru import logger
import httpx
import time


from ..common.config_service import ConfigService
from ..common.damage_log import DAMAGE_LOG_NAME, DamageLog
from ..common.partition_check import (
    CHECK_STATE_NAME,
    PartitionCheck,
    PartitionCheckState,
    check_partitions,
    last_closed_month,
    month_of,
    stock_datasets,
)
from ..common.partitioned_storage_backend import PartitionedStorageBackend
from ..common.storage_backend import StorageBackend
from ..common.storage import StorageInterface, StorageRequest
from ..common.storage_router import StorageRouter
from ..common.rate_limiter import wrap_callable
from ..common.shutdown import StopCheck
from .data_fetcher import SHORT_LIVED_INTERVALS, DataFetcher
from .interval_scheduler import BARS, EMPTY, WENT_QUIET, CycleResult, IntervalScheduler
from .ticker_registry import TickerRegistry


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

DATASET_NAME = "stocks"

# A collection night starts at this hour, UTC. The US close is 20:00 UTC in
# summer and 21:00 UTC in winter, so 22:00 is after it all year, half-day
# sessions included, and no market calendar or time zone is needed.
NIGHT_START_HOUR_UTC = 22


class YFParqed:
    def __init__(
        self,
        my_path: Path = Path.cwd(),
        my_intervals: Sequence[str] | None = None,
        storage_backend: StorageInterface | None = None,
    ):
        self.config = ConfigService(my_path)
        self.call_list = []

        self._sync_paths()

        # Initialize registry early (before load_tickers is called)
        # Will be re-initialized with callbacks after limiter setup
        self.registry = TickerRegistry(config=self.config)
        self.load_tickers()

        intervals_arg = list(my_intervals) if my_intervals is not None else []

        if len(intervals_arg) == 0:
            self.load_intervals()
        else:
            self.my_intervals = list(intervals_arg)
            self.save_intervals(self.my_intervals)

        if len(self.my_intervals) == 0:
            # logger.error("No intervals found.  Please set the intervals.")
            raise ValueError("No intervals found.  Please set the intervals.")

        self.new_not_found = False
        # False: every run fetches. Switched on by use_nightly_schedule (the daemon).
        self.nightly = False
        self.night_start_hour_utc = NIGHT_START_HOUR_UTC
        self.set_limiter()
        # Wrap a lambda so monkeypatching enforce_limits in tests still affects the limiter
        self.rate_limiter = wrap_callable(lambda: self.enforce_limits())

        # Re-initialize registry with callbacks for not-found maintenance
        self.registry = TickerRegistry(
            config=self.config,
            initial_tickers=self.registry.tickers,  # Preserve loaded tickers
            limiter=self.rate_limiter.enforce_limits,
            fetch_callback=self._fetch_for_not_found_check,
            clock=lambda: self.utc_now(),
        )
        # These are the tickers just read from disk, not a change to write.
        self.registry.mark_clean()

        self.data_fetcher = DataFetcher(
            limiter=self.rate_limiter.enforce_limits,
            today_provider=lambda: self.get_today(),
            empty_frame_factory=self._empty_price_frame,
        )
        self._custom_storage_injected = storage_backend is not None
        base_backend = storage_backend or self._create_storage_backend()
        self._legacy_storage = base_backend
        self.storage = base_backend
        self._partition_storage = self._create_partition_backend()
        self.scheduler = IntervalScheduler(
            registry=self.registry,
            intervals=lambda: list(self.my_intervals),
            loader=lambda: self._load_for_cycle(),
            limiter=self.rate_limiter.enforce_limits,
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

    def utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def use_nightly_schedule(self, start_hour_utc: int = NIGHT_START_HOUR_UTC) -> None:
        """
        Collect once per night instead of in every run (the daemon's mode).

        A ticker is fetched when Yahoo has not answered for it since the most
        recent ``start_hour_utc``; a run later in the same night asks only for
        the tickers whose request failed. For the intervals Yahoo keeps only
        for days (``SHORT_LIVED_INTERVALS``) the fetch is always the full
        period, and the registry is saved during the cycle.
        """
        self.nightly = True
        self.night_start_hour_utc = start_hour_utc
        self.scheduler.is_due = self.is_due_tonight
        self.scheduler.checkpoint = self.save_ticker_changes
        self.scheduler.health_check = self.yahoo_answers

    def night_start(self, now: datetime | None = None) -> datetime:
        """The start of the collection night that ``now`` falls into."""
        now = now or self.utc_now()
        start = now.replace(
            hour=self.night_start_hour_utc, minute=0, second=0, microsecond=0
        )
        if start > now:
            start -= timedelta(days=1)
        return start

    def seconds_until_next_night(self, now: datetime | None = None) -> float:
        now = now or self.utc_now()
        return (self.night_start(now) + timedelta(days=1) - now).total_seconds()

    def is_due_tonight(self, ticker: str, interval: str) -> bool:
        return not self.registry.fetched_since(ticker, interval, self.night_start())

    def yahoo_answers(self, interval: str, reference: str | None = None) -> bool:
        """
        Find out whether Yahoo is answering properly: ask for a ticker that is
        known to have bars. ``reference`` is one that had bars a moment ago;
        without it, a ticker that had bars tonight, or in the last few days.
        False when no such ticker is known: then nothing can be said.
        """
        reference = reference or self._reference_ticker(interval)
        if reference is None:
            return False
        try:
            answered = self.data_fetcher.has_recent_bars(reference, interval)
        except Exception as exc:
            logger.warning(
                f"Yahoo did not answer for {reference}: {type(exc).__name__}: {exc}"
            )
            return False
        if not answered:
            logger.warning(f"Yahoo returned no bars for {reference}")
        return answered

    def _reference_ticker(self, interval: str) -> str | None:
        night = self.night_start()
        recent = self.config.format_date(self.get_today() - timedelta(days=5))
        fallback = None
        for ticker, data in self.registry.tickers.items():
            meta = data.get("intervals", {}).get(interval) or {}
            if meta.get("status") != "active":
                continue
            if self.registry.fetched_since(ticker, interval, night):
                return ticker
            if fallback is None and str(meta.get("newest_bar_date", "")) >= recent:
                fallback = ticker
        return fallback

    def save_ticker_changes(self) -> bool:
        """
        Save what changed in the registry since it was read, keeping what other
        writers of tickers.json did in the meantime. A save that fails is
        logged; the changes stay in memory and go out with the next save. In
        the nightly schedule that holds across cycles too: see ``_load_for_cycle``.
        """
        try:
            return self.registry.save_changes()
        except (OSError, ValueError) as exc:
            logger.error(f"Could not save the ticker registry: {exc}")
            return False

    def _load_for_cycle(self) -> None:
        """
        Read tickers.json at the start of a cycle.

        In the nightly schedule, what an earlier save could not write is saved
        first. If that fails again, the registry in memory is kept for this
        cycle: reading the file would forget which tickers were fetched
        tonight, and every cycle would fetch all of them again.
        """
        if self.nightly and self.registry.has_unsaved_changes():
            if not self.save_ticker_changes():
                logger.error(
                    "The ticker registry is still unsaved; this cycle works from "
                    "memory and does not read tickers.json"
                )
                return
        self.load_tickers()

    def _sync_paths(self):
        self.my_path = self.config.base_path
        self.tickers_path = self.config.tickers_path
        self.intervals_path = self.config.intervals_path

    def set_working_path(self, my_path: Path):
        new_path = self.config.set_working_path(my_path)
        self._sync_paths()
        self.load_tickers()
        return new_path

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

    def _create_storage_backend(self) -> StorageInterface:
        return StorageBackend(
            empty_frame_factory=self._empty_price_frame,
            normalizer=self._normalize_price_frame,
            column_provider=self._price_frame_columns,
            damage_recorder=self._record_damaged_file,
        )

    def _create_partition_backend(self) -> PartitionedStorageBackend:
        data_root = self.my_path / "data"
        self._storage_router = StorageRouter(root=data_root)
        return PartitionedStorageBackend(
            empty_frame_factory=self._empty_price_frame,
            normalizer=self._normalize_price_frame,
            column_provider=self._price_frame_columns,
            path_builder=self._storage_router.path_builder,
            damage_recorder=self._record_damaged_file,
        )

    @property
    def damage_log(self) -> DamageLog:
        return DamageLog(self.my_path / DAMAGE_LOG_NAME)

    @property
    def partition_check_state(self) -> PartitionCheckState:
        return PartitionCheckState(self.my_path / CHECK_STATE_NAME)

    def _record_damaged_file(
        self, path: Path, moved_to: Path | None, error: BaseException
    ) -> None:
        """Called by the storage backends for a file they could not read."""
        self.damage_log.record(
            path=path, error=error, moved_to=moved_to, found_by="read"
        )

    def verify_partitions(
        self,
        month: str | None = None,
        should_stop: StopCheck | None = None,
        found_by: str = "verify-partitions",
    ) -> PartitionCheck:
        """
        Read every stored partition file of ``month`` (all months when None).

        Read-only on the data. Each damaged file is logged and written to the
        damage record once; the file itself is left where it is. A run that
        was not cut short notes its result for every closed month it covered.
        The result is returned even if those two files cannot be written.
        """
        result = check_partitions(
            stock_datasets(self.my_path / "data"), month, should_stop
        )

        # The record and the note are written independently: a record that
        # cannot be written must not make the daemon read the month again in
        # every cycle. The log line and the damaged count in the note remain.
        for path, error in result.damaged:
            logger.error(f"Damaged partition file {path}: {error}")
            try:
                self.damage_log.record(
                    path=path, error=error, found_by=found_by, once=True
                )
            except (OSError, ValueError) as exc:
                logger.error(f"Could not record damaged file {path}: {exc}")

        if result.complete:
            # An open month still changes; an entry for it would make the
            # daemon skip that month's check once it closes.
            current = month_of(datetime.now())
            closed = {k: v for k, v in result.months.items() if k < current}
            try:
                self.partition_check_state.record(closed)
            except (OSError, ValueError) as exc:
                logger.error(f"Could not note the partition check: {exc}")
        return result

    def check_last_closed_month(
        self, should_stop: StopCheck | None = None
    ) -> PartitionCheck | None:
        """Check the last closed month once; None when it has been checked before."""
        month = last_closed_month(datetime.now())
        if self.partition_check_state.has(month):
            return None

        logger.info(f"Month-close check: reading every stored file of {month}")
        result = self.verify_partitions(month, should_stop, "month-close check")
        if not result.complete:
            logger.info(
                f"Month-close check of {month} stopped after {result.files} files; "
                "it starts again in a later cycle"
            )
            return result
        check = result.months[month]
        logger.info(
            f"Month-close check of {month}: {check.files} files, {check.rows} rows, "
            f"{len(check.damaged)} damaged, {result.seconds:.0f} seconds"
        )
        return result

    def set_limiter(self, max_requests: int = 3, duration: int = 2):
        max_requests, duration = self.config.configure_limits(max_requests, duration)
        self.max_requests = max_requests
        self.duration = duration

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

    # Conservative dead-instrument name patterns:
    # dash-suffix derivatives (warrants, units, rights) and depositary instruments
    _DEAD_NAME_PATTERN = re.compile(
        r"(?i)"
        r"\s+-\s+warrants?\b"
        r"|\s+-\s+units?\b"
        r"|\s+-\s+rights?\b"
        r"|\bdepositary\s+(?:shares?|receipts?)\b"
        r"|\bamerican\s+depositary\b"
    )

    @classmethod
    def _is_dead_instrument(cls, symbol: str, name: str) -> bool:
        """Return True if the instrument is a derivative that will never trade as a stock."""
        # Known derivative symbol suffixes: .WS .WT .WI .W (warrants), .RT .R (rights), .U (units)
        if re.search(r"\.(?:W[STI]?|R[T]?|U)$", symbol, re.IGNORECASE):
            return True
        return bool(cls._DEAD_NAME_PATTERN.search(name))

    def _parse_csv_tickers(
        self, path: Path, symbol_col: str, name_col: str
    ) -> list[str]:
        """Parse a ticker CSV and return live symbols, filtering dead instruments."""
        symbols: list[str] = []
        filtered = 0
        try:
            with path.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = (row.get(symbol_col) or "").strip()
                    name = (row.get(name_col) or "").strip()
                    if not symbol:
                        continue
                    if self._is_dead_instrument(symbol, name):
                        filtered += 1
                        continue
                    symbols.append(symbol)
        except Exception as e:
            logger.warning(f"Failed to parse {path}: {e}")
        if filtered:
            logger.debug(f"Filtered {filtered} dead instruments from {path.name}")
        return symbols

    def get_new_list_of_stocks(self, download_tickers: bool = True) -> dict:
        if download_tickers:
            nasdaq_path, nyse_path = self.get_tickers()
        else:
            nasdaq_path = self.my_path / "nasdaq-listed.csv"
            nyse_path = self.my_path / "nyse-listed.csv"
        if not nasdaq_path.is_file() or not nyse_path.is_file():
            logger.debug("Nasdaq and/or Nyse file not found.  Nothing to do")
            return {}

        nasdaq = self._parse_csv_tickers(nasdaq_path, "Symbol", "Security Name")
        nyse = self._parse_csv_tickers(nyse_path, "ACT Symbol", "Company Name")

        today = datetime.now().strftime("%Y-%m-%d")
        stocks = {
            x: {
                "ticker": x,
                "added_date": today,
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
            for x in set(nasdaq + nyse)
        }
        return stocks

    def load_tickers(self):
        self.registry.load()

    def save_tickers(self):
        """
        Save the registry, keeping what other writers of tickers.json did since
        it was read here (see ``TickerRegistry.save_changes``). A registry
        without a file yet is written even when it is empty.
        """
        if not self.registry.save_changes() and not self.tickers_path.is_file():
            self.registry.save()

    def refresh_tickers(self) -> None:
        """
        Read tickers.json again unless there is something here still to save.

        Ticker maintenance in the daemon starts from the registry the last
        cycle left in memory, which can be hours old; a command may have
        changed the file since.
        """
        if not self.registry.has_unsaved_changes():
            self.load_tickers()

    def update_current_list_of_stocks(self):
        new_tickers = self.get_new_list_of_stocks()
        self.refresh_tickers()
        self.registry.update_current_list(new_tickers)
        # Only the added and pruned entries are written, so a change another
        # process made to tickers.json since it was read here is kept.
        self.registry.save_changes()

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
        storage_info: dict | None = None,
        record_fetch: bool = True,
    ):
        """
        Update the status of a ticker for a specific interval.

        Args:
            ticker: The ticker symbol
            interval: The trading interval (1d, 1h, etc.)
            found_data: Whether data was found for this ticker/interval
            last_date: Last date with data (if found_data is True)
            storage_info: Storage backend information (for partitioned storage)
            record_fetch: Whether this answer is the night's collection for the
                ticker (see TickerRegistry.update_ticker_interval_status)
        """
        self.registry.update_ticker_interval_status(
            ticker=ticker,
            interval=interval,
            found_data=found_data,
            last_date=last_date,
            storage_info=storage_info,
            record_fetch=record_fetch,
        )

    def confirm_not_founds(self):
        """Delegate to registry for not-found ticker confirmation."""
        self.registry.confirm_not_founds()

    def reparse_not_founds(self):
        """Delegate to registry for not-found ticker reactivation."""
        self.registry.reparse_not_founds()

    def save_yf(
        self,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
        target: StorageRequest | Path | str,
    ) -> pd.DataFrame:
        request = self._ensure_storage_request(target)
        backend = self._select_storage_backend(request)
        return backend.save(request, new_data, existing_data)

    def merge_yf(
        self, new_data: pd.DataFrame, target: StorageRequest | Path | str
    ) -> None:
        """Store freshly fetched bars for one ticker and interval.

        Partitioned storage opens and rewrites only the months the new bars
        fall into. The legacy layout is one file per ticker, read and
        rewritten whole; it gets none of that. Raises when the backend refused
        to write a month: its bars are not on disk, so the ticker must not be
        recorded as stored.
        """
        request = self._ensure_storage_request(target)
        backend = self._select_storage_backend(request)
        if backend is self._partition_storage:
            refused: list = []
            self._partition_storage.merge(request, new_data, refused=refused)
            if refused:
                months = ", ".join(str(month) for month in refused)
                raise RuntimeError(
                    f"{request.ticker} ({request.interval}): {months} not written, "
                    "the stored file was kept; see the error above"
                )
        else:
            backend.save(request, new_data, backend.read(request))

    def read_yf(self, target: StorageRequest | Path | str) -> pd.DataFrame:
        request = self._ensure_storage_request(target)
        backend = self._select_storage_backend(request)
        return backend.read(request)

    def _ensure_storage_request(
        self,
        target: StorageRequest | Path | str,
    ) -> StorageRequest:
        if isinstance(target, StorageRequest):
            return target

        path = Path(target)
        parent = path.parent
        prefix, _, interval = parent.name.partition("_")
        if prefix != "stocks" or not interval:
            raise ValueError(
                "Unable to infer storage request from path; expected legacy stocks_<interval> structure"
            )

        root = parent.parent
        return StorageRequest(root=root, interval=interval, ticker=path.stem)

    def _select_storage_backend(self, request: StorageRequest) -> StorageInterface:
        if request.market and request.source:
            return self._partition_storage
        return self._legacy_storage

    def _build_storage_request(self, ticker: str, interval: str) -> StorageRequest:
        storage_info = self.registry.get_interval_storage(ticker, interval)
        if storage_info and storage_info.get("mode") == "partitioned":
            market = storage_info.get("market")
            source = storage_info.get("source")
            dataset = storage_info.get("dataset") or DATASET_NAME
            root_token = storage_info.get("root") or "data"
            root_path = (
                Path(root_token)
                if Path(root_token).is_absolute()
                else self.my_path / root_token
            )
            return StorageRequest(
                root=root_path,
                interval=interval,
                ticker=ticker,
                market=str(market).lower() if isinstance(market, str) else None,
                source=str(source).lower() if isinstance(source, str) else None,
                dataset=str(dataset),
            )

        # Check global partitioned storage config as fallback
        # (but not if a custom storage backend was injected)
        if not self._custom_storage_injected and self.config.is_partitioned_enabled():
            return StorageRequest(
                root=self.my_path / "data",
                interval=interval,
                ticker=ticker,
                market="us",
                source="yahoo",
                dataset=DATASET_NAME,
            )

        return StorageRequest(
            root=self.my_path,
            interval=interval,
            ticker=ticker,
        )

    def update_stock_data(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        should_stop: StopCheck | None = None,
    ) -> CycleResult:
        self.new_not_found = False
        return self.scheduler.run(
            start_date=start_date, end_date=end_date, should_stop=should_stop
        )

    def save_single_stock_data(
        self,
        stock: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        interval: str = "1d",
    ) -> str | None:
        """
        Fetch and store one ticker for one interval.

        Returns what Yahoo answered (``BARS``, ``EMPTY`` or ``WENT_QUIET``) or
        None when nothing was asked. A failed request or a failed write
        raises, and nothing is recorded for the ticker.
        """
        logger.debug(stock)
        # Only the nightly collection closes a ticker's night. A single run, or
        # a window given on the command line, fetches something else and must
        # not make the daemon skip the ticker.
        closes_the_night = self.nightly and start_date is None
        storage_request = self._build_storage_request(stock, interval)
        backend = self._select_storage_backend(storage_request)

        # Check if stock should be processed for this interval
        if not self.is_ticker_active_for_interval(stock, interval):
            logger.debug(f"{stock} is not active for interval {interval}, skipping")
            return None

        if backend is self._legacy_storage:
            logger.debug(f"Data path: {storage_request.legacy_path()}")
        else:
            logger.debug(
                "Using partitioned storage for {stock} interval {interval}",
                stock=stock,
                interval=interval,
            )

        last_data_date = self.registry.get_last_data_date(stock, interval)
        interval_meta = self.registry.get_interval_metadata(stock, interval)
        had_bars = bool(interval_meta) and interval_meta.get("status") == "active"

        if end_date is None:
            end_date = self.get_today()

        load_all = False
        if start_date is None:
            # In the nightly schedule a short-lived interval is always fetched
            # for its full period: each bar is then asked for on every night
            # Yahoo still serves it.
            full_period = self.nightly and interval in SHORT_LIVED_INTERVALS
            if last_data_date is not None and not full_period:
                start_date = min(last_data_date, end_date)
            else:
                load_all = True
                start_date = end_date

        should_fetch = load_all or (
            start_date is not None
            and end_date is not None
            and self.business_days_between(start=start_date, end=end_date) > 0
            # A window with nothing in it is not sent to Yahoo, so there is
            # no answer to record either.
            and self.data_fetcher.has_window(start_date, end_date, interval)
        )

        if should_fetch:
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
                self.merge_yf(df1, storage_request)

                # Update ticker status - data found for this interval
                # Also record storage backend information
                storage_info = None
                if backend is self._partition_storage:
                    storage_info = {
                        "mode": "partitioned",
                        "market": storage_request.market,
                        "source": storage_request.source,
                        "dataset": storage_request.dataset,
                    }
                self.update_ticker_interval_status(
                    stock,
                    interval,
                    True,
                    last_data_date,
                    storage_info,
                    record_fetch=closes_the_night,
                )
                return BARS

            else:
                logger.debug(
                    f"{stock} returned no results for the date range of {start_date} to {end_date} and load_all:{load_all} for interval {interval}."
                )

                # Update ticker status - no data found for this interval.
                # In the nightly collection, a ticker that had bars at its last
                # answer is not done for the night on an empty one: every later
                # cycle of this night asks again. The night is remembered in the
                # registry, so a second empty answer does not close it either.
                self.new_not_found = True
                if closes_the_night:
                    night = self.night_start().isoformat(timespec="seconds")
                    if had_bars or self.registry.went_quiet_in(stock, interval, night):
                        self.registry.note_went_quiet(stock, interval, night)
                        return WENT_QUIET
                self.update_ticker_interval_status(
                    stock, interval, False, record_fetch=closes_the_night
                )
                return EMPTY
        else:
            logger.debug(f"{stock} is up to date for interval {interval}.")
            if closes_the_night:
                # Otherwise every later cycle of the night would walk through
                # this ticker again, limiter wait included, to fetch nothing.
                self.registry.mark_done_for_the_night(stock, interval)
            return None

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

    def set_partition_override(
        self,
        *,
        enabled: bool,
        market: str | None = None,
        source: str | None = None,
    ) -> dict:
        if source and not market:
            raise ValueError("Market must be provided when setting a source override")

        if market and source:
            return self.config.set_source_partition_mode(market, source, enabled)

        if market:
            return self.config.set_market_partition_mode(market, enabled)

        return self.config.set_partition_mode(enabled)

    def clear_partition_override(
        self,
        *,
        market: str | None = None,
        source: str | None = None,
    ) -> dict:
        if source and not market:
            raise ValueError("Market must be provided when clearing a source override")

        if market and source:
            return self.config.clear_source_partition_mode(market, source)

        if market:
            return self.config.clear_market_partition_mode(market)

        raise ValueError(
            "Provide --market (and optionally --source) when clearing overrides"
        )
