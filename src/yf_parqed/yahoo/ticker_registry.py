from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Iterable
from urllib.error import HTTPError

from loguru import logger
from rich.progress import track

from ..common.config_service import ConfigService


class TickerRegistry:
    """Manage ticker metadata persistence and lifecycle transitions."""

    def __init__(
        self,
        config: ConfigService,
        initial_tickers: dict | None = None,
        limiter: Callable[[], None] | None = None,
        fetch_callback: Callable[[str, str, str], tuple[bool, datetime | None]]
        | None = None,
    ):
        self._config = config
        self._tickers: dict = {}
        self._limiter = limiter
        self._fetch_callback = fetch_callback
        if initial_tickers is not None:
            self.replace(initial_tickers)
        else:
            self.load()

    @property
    def tickers(self) -> dict:
        return self._tickers

    def load(self) -> dict:
        self._tickers = self._config.load_tickers()
        return self._tickers

    def save(self) -> None:
        self._config.save_tickers(self._tickers)

    def replace(self, tickers: dict) -> dict:
        self._tickers = tickers
        return self._tickers

    def update_current_list(self, new_tickers: dict) -> None:
        new_symbols = set(new_tickers.keys())

        for ticker, metadata in new_tickers.items():
            if ticker not in self._tickers:
                self._tickers[ticker] = metadata
                self._tickers[ticker].setdefault("intervals", {})
                self._tickers[ticker]["source"] = "csv"
            # Existing tickers are NOT reactivated — they work through the death cycle

        # Prune permanently dead CSV tickers no longer in CSV
        # Collect keys first (never modify dict while iterating)
        to_remove = [
            t for t, data in self._tickers.items()
            if data.get("source") != "manual"
            and not data.get("manually_removed")
            and t not in new_symbols
            and data.get("intervals")
            and all(
                iv.get("permanently_dead", False)
                for iv in data["intervals"].values()
            )
        ]
        for t in to_remove:
            del self._tickers[t]
        if to_remove:
            logger.info(f"Pruned {len(to_remove)} permanently dead tickers no longer in CSV")

    def is_active_for_interval(self, ticker: str, interval: str) -> bool:
        ticker_data = self._tickers.get(ticker)
        if ticker_data is None:
            return True

        # Manually removed tickers are never active
        if ticker_data.get("manually_removed"):
            return False

        # Manual tickers are always active — exempt from death cycle
        if ticker_data.get("source") == "manual":
            return True

        # Legacy global not_found (old schema compatibility)
        if ticker_data.get("status") == "not_found":
            return False

        interval_data = ticker_data.get("intervals", {}).get(interval)
        if interval_data is None:
            return True

        if interval_data.get("permanently_dead"):
            return False

        if interval_data.get("status") != "not_found":
            return True

        # In cooling window?
        cooling_since = interval_data.get("cooling_since")
        if cooling_since:
            now = self._config.get_now()
            # True when cooling expired → allow single retry
            return self._business_days_since(cooling_since, now) >= 7

        # Still in streak phase — always allow retries
        return True

    def get_interval_metadata(self, ticker: str, interval: str) -> dict | None:
        ticker_data = self._tickers.get(ticker)
        if not ticker_data:
            return None
        intervals = ticker_data.get("intervals", {})
        return intervals.get(interval)

    def get_interval_storage(self, ticker: str, interval: str) -> dict | None:
        interval_meta = self.get_interval_metadata(ticker, interval)
        if not interval_meta:
            return None
        storage = interval_meta.get("storage")
        return storage if isinstance(storage, dict) else None

    def get_last_data_date(self, ticker: str, interval: str) -> datetime | None:
        interval_meta = self.get_interval_metadata(ticker, interval)
        if not interval_meta:
            return None

        last_data = interval_meta.get("last_data_date")
        if not last_data:
            return None

        try:
            return datetime.strptime(last_data, "%Y-%m-%d")
        except ValueError:
            return None

    def update_ticker_interval_status(
        self,
        ticker: str,
        interval: str,
        found_data: bool,
        last_date: datetime | None = None,
        storage_info: dict | None = None,
    ) -> None:
        current_date = self._config.format_date()

        if ticker not in self._tickers:
            self._tickers[ticker] = {
                "ticker": ticker,
                "added_date": current_date,
                "status": "active",
                "last_checked": current_date,
                "intervals": {},
            }

        ticker_entry = self._tickers[ticker]
        intervals = ticker_entry.setdefault("intervals", {})
        interval_entry = intervals.setdefault(interval, {})

        if found_data:
            interval_entry["status"] = "active"
            interval_entry["last_found_date"] = current_date
            interval_entry["last_checked"] = current_date
            interval_entry.pop("not_found_streak_days", None)
            interval_entry.pop("cooling_since", None)
            # permanently_dead is intentionally NOT cleared — only add_ticker() does that
            if last_date is not None:
                interval_entry["last_data_date"] = self._config.format_date(last_date)

            if storage_info is not None:
                interval_entry["storage"] = storage_info

            ticker_entry["status"] = "active"
            ticker_entry["last_checked"] = current_date
        else:
            today_str = current_date

            # Permanently dead intervals are never updated
            if interval_entry.get("permanently_dead"):
                return

            cooling_since = interval_entry.get("cooling_since")
            if cooling_since:
                now = self._config.get_now()
                if self._business_days_since(cooling_since, now) >= 7:
                    # Post-cooldown retry just failed → permanently dead
                    interval_entry["permanently_dead"] = True
                    interval_entry.pop("cooling_since", None)
                interval_entry["status"] = "not_found"
                interval_entry["last_not_found_date"] = today_str
                interval_entry["last_checked"] = today_str
                ticker_entry["last_checked"] = today_str
                return

            # Day-based streak: only increment once per calendar day
            if interval_entry.get("last_not_found_date") != today_str:
                streak = interval_entry.get("not_found_streak_days", 0) + 1
                interval_entry["not_found_streak_days"] = streak
                if streak >= 3:
                    interval_entry["cooling_since"] = today_str

            interval_entry["status"] = "not_found"
            interval_entry["last_not_found_date"] = today_str
            interval_entry["last_checked"] = today_str
            ticker_entry["last_checked"] = today_str
            # Do NOT set global not_found — per-interval permanently_dead is authoritative

    def add_ticker(self, ticker: str) -> None:
        """Add or resurrect a ticker as manually managed (exempt from auto-death)."""
        today = self._config.format_date()
        if ticker not in self._tickers:
            self._tickers[ticker] = {
                "ticker": ticker,
                "added_date": today,
                "status": "active",
                "last_checked": today,
                "source": "manual",
                "intervals": {},
            }
            logger.info(f"Added new manual ticker: {ticker}")
        else:
            data = self._tickers[ticker]
            was_dead = data.get("manually_removed") or any(
                iv.get("permanently_dead") for iv in data.get("intervals", {}).values()
            )
            data["source"] = "manual"
            data.pop("manually_removed", None)
            data["status"] = "active"
            for iv in data.get("intervals", {}).values():
                iv.pop("permanently_dead", None)
                iv.pop("not_found_streak_days", None)
                iv.pop("cooling_since", None)
                if iv.get("status") == "not_found":
                    iv["status"] = "active"
            if was_dead:
                logger.warning(f"Resurrecting previously dead ticker: {ticker}")
            else:
                logger.info(f"Marked existing ticker as manual: {ticker}")
        self.save()

    def remove_ticker(self, ticker: str) -> None:
        """Permanently deactivate a ticker. Not reactivated by CSV updates."""
        if ticker not in self._tickers:
            logger.warning(f"Ticker {ticker} not in registry")
            return
        data = self._tickers[ticker]
        data["source"] = "manual"
        data["manually_removed"] = True
        for iv in data.get("intervals", {}).values():
            iv["permanently_dead"] = True
        logger.info(f"Manually deactivated ticker: {ticker}")
        self.save()

    @staticmethod
    def _all_intervals_not_found(intervals: Iterable[dict]) -> bool:
        return all(interval.get("status") == "not_found" for interval in intervals)

    @staticmethod
    def _business_days_since(start_str: str, now: datetime) -> int:
        """Count weekdays between start_str date and now (exclusive of start, inclusive of end)."""
        try:
            start = datetime.strptime(start_str, "%Y-%m-%d").date()
        except ValueError:
            return 0
        end = now.date()
        count, current = 0, start
        while current < end:
            current += timedelta(days=1)
            if current.weekday() < 5:
                count += 1
        return count

    def confirm_not_founds(self) -> None:
        """Re-check globally not-found tickers using the 1d interval."""
        if self._limiter is None or self._fetch_callback is None:
            raise RuntimeError(
                "confirm_not_founds requires limiter and fetch_callback to be provided"
            )

        logger.debug("Confirming not found tickers")
        not_found_tickers = {
            ticker: data
            for ticker, data in self._tickers.items()
            if data.get("status") == "not_found"
        }

        logger.info(f"Number of not found tickers: {len(not_found_tickers)}")
        for stock, meta_data in track(
            not_found_tickers.items(), "Re-checking not-founds..."
        ):
            self._limiter()
            current_date = self._config.format_date()
            meta_data["last_checked"] = current_date

            try:
                found_data, last_date = self._fetch_callback(stock, "1d", "1d")
                if found_data:
                    logger.debug(f"{stock} is found.")
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

        self.save()
        self.reparse_not_founds()

    def reparse_not_founds(self) -> None:
        """Reactivate not-found tickers if any interval has recent data (<90 days)."""
        not_found_tickers = {
            ticker: data
            for ticker, data in self._tickers.items()
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
                            days_since = (self._config.get_now() - last_date).days
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
                        "added_date", self._config.format_date()
                    ),
                    "status": "active",
                    "last_checked": self._config.format_date(),
                    "intervals": meta_data.get("intervals", {}),
                }
                logger.info(f"Reactivating {ticker} - found recent data in intervals.")
                self._tickers[ticker] = stock_meta

        self.save()
