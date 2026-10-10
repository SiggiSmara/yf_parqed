from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Callable
from urllib.error import HTTPError

from loguru import logger
from rich.progress import track

from ..common.config_service import ConfigService


# Per-interval keys written since the nightly collection (ADR 2026-10-03,
# Decision 6). "last_data_date" is the key of earlier releases: it is read as a
# fallback and never written, so that an earlier release reading a file saved
# by this one still fetches everything.
LAST_FETCH_KEY = "last_fetch_at"
NEWEST_BAR_KEY = "newest_bar_date"
LEGACY_LAST_DATA_KEY = "last_data_date"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def merge_entry(base: dict, ours: dict, theirs: dict) -> dict:
    """
    Apply the changes made here since ``base`` onto ``theirs``, key by key.

    A key changed, added or removed here takes this side's value; every other
    key keeps the other writer's. Nested dicts (the intervals) are merged the
    same way. So the daemon's fetch stamps and a remove-ticker's flags on the
    same ticker both survive, whichever was written first.
    """
    merged = dict(theirs)
    for key, value in ours.items():
        if key in base and base[key] == value:
            continue
        theirs_value = theirs.get(key)
        base_value = base.get(key)
        if (
            isinstance(value, dict)
            and isinstance(base_value, dict)
            and isinstance(theirs_value, dict)
        ):
            merged[key] = merge_entry(base_value, value, theirs_value)
        else:
            merged[key] = value
    for key in base:
        if key not in ours:
            merged.pop(key, None)
    return merged


class TickerRegistry:
    """Manage ticker metadata persistence and lifecycle transitions."""

    def __init__(
        self,
        config: ConfigService,
        initial_tickers: dict | None = None,
        limiter: Callable[[], None] | None = None,
        fetch_callback: Callable[[str, str, str], tuple[bool, datetime | None]]
        | None = None,
        clock: Callable[[], datetime] | None = None,
    ):
        self._config = config
        self._clock = clock or utc_now
        self._tickers: dict = {}
        self._limiter = limiter
        self._fetch_callback = fetch_callback
        # What tickers.json held per ticker when it was last read or written by
        # this instance; save_changes() writes only the entries that differ.
        self._baseline: dict[str, str] = {}
        self._replaced = False
        if initial_tickers is not None:
            self.replace(initial_tickers)
        else:
            self.load()

    @property
    def tickers(self) -> dict:
        return self._tickers

    def load(self) -> dict:
        self._tickers = self._config.load_tickers()
        self.mark_clean()
        return self._tickers

    def save(self) -> None:
        """Write the whole registry as it is in memory, replacing the file."""
        with self._config.tickers_lock():
            if self._config.save_tickers(self._tickers):
                self.mark_clean()

    def save_changes(self) -> bool:
        """
        Write only what this instance changed, onto a fresh read of the file.

        Another process may have written tickers.json since this instance read
        it (add-ticker next to a running daemon, or the daemon next to a
        command). Its work is kept: an entry is written only when it differs
        from what this instance last read or wrote; when the other process
        changed the same entry, the two are merged key by key (``merge_entry``);
        an entry it deleted is not brought back, and an entry it changed is not
        deleted. Afterwards the memory holds the merged registry. A registry
        set with ``replace`` is written whole, and so is one whose file has
        disappeared since it was read.
        Returns False when there was nothing to write or the write was refused;
        raises ``ValueError`` when the file cannot be decoded, and
        ``TimeoutError`` when the lock is not free.
        """
        if self._replaced:
            self.save()
            return not self._replaced

        changed, removed = self._changes()
        if not changed and not removed:
            return False

        if self._baseline and not self._config.tickers_path.is_file():
            # The file was there when it was read and is gone now (moved aside,
            # deleted). Merging onto "nothing" would drop every entry that was
            # not changed here; the registry in memory is the record.
            logger.warning(
                f"{self._config.tickers_path} has disappeared; "
                "writing the whole registry from memory"
            )
            self.save()
            return not self.has_unsaved_changes()

        with self._config.tickers_lock():
            stored = self._config.read_tickers_strict()
            for ticker in changed:
                ours = self._tickers[ticker]
                base_text = self._baseline.get(ticker)
                theirs = stored.get(ticker)
                if base_text is None:
                    # Added here. If another writer added it too (add-ticker,
                    # while the list update here found it as well), its keys
                    # win: an entry made by hand outranks one from the lists.
                    stored[ticker] = (
                        merge_entry({}, theirs, ours)
                        if isinstance(theirs, dict) and isinstance(ours, dict)
                        else ours
                    )
                elif theirs is None:
                    continue  # deleted by another writer since it was read here
                elif self._fingerprint(theirs) == base_text or not (
                    isinstance(theirs, dict) and isinstance(ours, dict)
                ):
                    stored[ticker] = ours
                else:
                    stored[ticker] = merge_entry(json.loads(base_text), ours, theirs)
            for ticker in removed:
                theirs = stored.get(ticker)
                if (
                    theirs is None
                    or self._fingerprint(theirs) == self._baseline[ticker]
                ):
                    stored.pop(ticker, None)
                # else: changed by another writer since it was read here (an
                # add-ticker that brought it back); it stays.
            if not self._config.save_tickers(stored):
                return False
            self._tickers = stored
            self.mark_clean()
        return True

    def has_unsaved_changes(self) -> bool:
        if self._replaced:
            return True
        changed, removed = self._changes()
        return bool(changed or removed)

    def mark_clean(self) -> None:
        """Take the registry in memory as what the file holds."""
        self._baseline = {
            ticker: self._fingerprint(entry) for ticker, entry in self._tickers.items()
        }
        self._replaced = False

    @staticmethod
    def _fingerprint(entry: object) -> str:
        return json.dumps(entry, sort_keys=True, default=str)

    def _changes(self) -> tuple[list[str], list[str]]:
        changed = [
            ticker
            for ticker, entry in self._tickers.items()
            if self._baseline.get(ticker) != self._fingerprint(entry)
        ]
        removed = [ticker for ticker in self._baseline if ticker not in self._tickers]
        return changed, removed

    def replace(self, tickers: dict) -> dict:
        self._tickers = tickers
        self._baseline = {}
        self._replaced = True
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
            t
            for t, data in self._tickers.items()
            if data.get("source") != "manual"
            and not data.get("manually_removed")
            and t not in new_symbols
            and data.get("intervals")
            and all(
                iv.get("permanently_dead", False) for iv in data["intervals"].values()
            )
        ]
        for t in to_remove:
            del self._tickers[t]
        if to_remove:
            logger.info(
                f"Pruned {len(to_remove)} permanently dead tickers no longer in CSV"
            )

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

        # Set by remove-ticker or tools/prune_registry.py. A cycle never sets
        # it, and an interval that returned no data is still asked every night.
        return not interval_data.get("permanently_dead")

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

        last_data = interval_meta.get(NEWEST_BAR_KEY) or interval_meta.get(
            LEGACY_LAST_DATA_KEY
        )
        if not last_data:
            return None

        try:
            return datetime.strptime(last_data, "%Y-%m-%d")
        except ValueError:
            return None

    def fetched_since(self, ticker: str, interval: str, moment: datetime) -> bool:
        """True when Yahoo last answered for this ticker and interval at or after ``moment``."""
        interval_meta = self.get_interval_metadata(ticker, interval)
        if not interval_meta:
            return False

        stamp = interval_meta.get(LAST_FETCH_KEY)
        if not isinstance(stamp, str):
            return False
        try:
            fetched = datetime.fromisoformat(stamp)
        except ValueError:
            return False
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return fetched >= moment

    def mark_done_for_the_night(self, ticker: str, interval: str) -> None:
        """
        Note that nothing more is to be fetched for this ticker tonight, without
        an answer from Yahoo: its stored bars are already up to date.
        """
        interval_entry = (
            self._tickers.setdefault(ticker, {"ticker": ticker, "status": "active"})
            .setdefault("intervals", {})
            .setdefault(interval, {})
        )
        interval_entry[LAST_FETCH_KEY] = self._clock().isoformat(timespec="seconds")

    def update_ticker_interval_status(
        self,
        ticker: str,
        interval: str,
        found_data: bool,
        last_date: datetime | None = None,
        storage_info: dict | None = None,
        fetched_at: datetime | None = None,
        record_fetch: bool = True,
    ) -> None:
        """
        Record Yahoo's answer for one ticker and interval.

        Call it only for an answer: bars, or "no data". A request that failed
        is not recorded, so the ticker is asked again. ``found_data=False``
        notes the empty answer and nothing else; no count of empty answers
        pauses a ticker or marks it dead. ``record_fetch=False`` leaves the
        fetch stamp alone, so the nightly schedule asks for the ticker again:
        for an answer that is not the night's collection.
        """
        current_date = self._config.format_date()
        fetch_stamp = (fetched_at or self._clock()).isoformat(timespec="seconds")

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
            if record_fetch:
                interval_entry[LAST_FETCH_KEY] = fetch_stamp
            interval_entry.pop("not_found_streak_days", None)
            interval_entry.pop("cooling_since", None)
            # permanently_dead is intentionally NOT cleared — only add_ticker() does that
            if last_date is not None:
                interval_entry[NEWEST_BAR_KEY] = self._config.format_date(last_date)
                interval_entry.pop(LEGACY_LAST_DATA_KEY, None)

            if storage_info is not None:
                interval_entry["storage"] = storage_info

            ticker_entry["status"] = "active"
            ticker_entry["last_checked"] = current_date
        else:
            today_str = current_date

            # Permanently dead intervals are never updated
            if interval_entry.get("permanently_dead"):
                return

            # Left over from the streak and pause of earlier releases.
            interval_entry.pop("not_found_streak_days", None)
            interval_entry.pop("cooling_since", None)

            interval_entry["status"] = "not_found"
            interval_entry["last_not_found_date"] = today_str
            interval_entry["last_checked"] = today_str
            if record_fetch:
                interval_entry[LAST_FETCH_KEY] = fetch_stamp
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
        self.save_changes()

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
        self.save_changes()

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
                found_data, _last_date = self._fetch_callback(stock, "1d", "1d")
                if found_data:
                    logger.debug(f"{stock} is found.")
                    # A probe, not the night's collection: no bars were stored.
                    # So neither the fetch stamp nor the newest bar date is
                    # written, and the cycle fetches the ticker in full.
                    self.update_ticker_interval_status(
                        stock, "1d", True, record_fetch=False
                    )
                else:
                    logger.debug(f"{stock} is not found.")

            except HTTPError as e:
                status_code = None
                if hasattr(e, "response"):
                    status_code = e.response.status_code
                logger.error(
                    f"Error getting data for {stock}: HTTP {status_code} - {str(e)}, most likely not available anymore."
                )

        self.save_changes()
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

        self.save_changes()
