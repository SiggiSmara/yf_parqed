from __future__ import annotations

from datetime import datetime
from typing import Callable

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFTickerMissingError, YFTzMissingError


# Intervals Yahoo keeps only for days; their full fetch is the last 7 days.
SHORT_LIVED_INTERVALS = frozenset({"1m", "2m", "5m", "15m", "30m"})
# Intervals Yahoo keeps for 730 days.
HOURLY_INTERVALS = frozenset({"60m", "90m", "1h"})

# Yahoo errors, as yfinance quotes them, that are answers and not failures:
# Yahoo has no bars for what was asked. Seen in live answers on 2026-10-10.
_NO_DATA_DESCRIPTIONS = (
    "No data found",  # "..., symbol may be delisted": unknown or delisted symbol
    "Data doesn't exist for startDate",  # a window before the first bar
    "The requested range must be within the last",  # older than Yahoo keeps
)


class FetchFailed(Exception):
    """Yahoo did not give a usable answer; the ticker has to be asked again."""


def is_no_data_answer(ticker: object, error: YFTickerMissingError) -> bool:
    """
    Tell Yahoo's "this ticker has no bars" from a request that went wrong.

    yfinance raises the same "possibly delisted; no price data found" for
    both, because it only treats status 429 as an error. Checked against
    yfinance 0.2.66 and live answers on 2026-10-10:

    - an unknown or delisted symbol, or a window Yahoo has no bars for: the
      message quotes one of Yahoo's errors in ``_NO_DATA_DESCRIPTIONS``;
    - a known symbol without a trade in the period: no error is quoted, and
      the response carried the symbol's metadata;
    - anything else is a failure: another quoted error (an invalid crumb, an
      internal error, a window that is too wide), a quoted status code, or a
      response without metadata.

    When the metadata cannot be looked at (a yfinance that keeps it
    elsewhere), the answer is a failure: a ticker asked again too often costs
    requests, a failure taken for "no data" can cost its bars. A test fails
    when the installed yfinance no longer has the attribute. A missing time
    zone is not judged here: see ``DataFetcher._missing_timezone``.
    """
    message = str(error)
    if "Yahoo status_code" in message:
        return False
    if 'Yahoo error = "' in message:
        return any(text in message for text in _NO_DATA_DESCRIPTIONS)
    metadata = getattr(
        getattr(ticker, "_price_history", None), "_history_metadata", None
    )
    return isinstance(metadata, dict) and bool(metadata)


class DataFetcher:
    """
    Wrap Yahoo Finance interactions with limiter and normalization helpers.

    An empty frame means Yahoo answered and has no bars for the ticker. A
    request that failed (rate limit, network, an error response) raises, so the
    caller can leave the ticker for a later attempt. yfinance is asked to raise
    its errors for that reason: by default it logs every failure as "possibly
    delisted" and returns an empty frame, which looks like an answer.
    """

    def __init__(
        self,
        limiter: Callable[[], None],
        today_provider: Callable[[], datetime],
        empty_frame_factory: Callable[[], pd.DataFrame],
        ticker_factory: Callable[[str], yf.Ticker] | None = None,
    ) -> None:
        self._limiter = limiter
        self._today_provider = today_provider
        self._empty_frame_factory = empty_frame_factory
        self._ticker_factory = ticker_factory or yf.Ticker

    def fetch(
        self,
        stock: str,
        start_date: datetime,
        end_date: datetime,
        interval: str,
        get_all: bool = False,
    ) -> pd.DataFrame:
        self._limiter()
        ticker = self._ticker_factory(stock)

        if get_all:
            df = self._fetch_all(ticker, stock, interval)
        else:
            df = self._fetch_window(ticker, stock, start_date, end_date, interval)

        if df.empty:
            return self._empty_frame_factory()
        return df

    def has_window(
        self, start_date: datetime, end_date: datetime, interval: str
    ) -> bool:
        """
        False when there is nothing to ask Yahoo for between the two dates
        (see ``_apply_interval_constraints``). ``fetch`` then returns an empty
        frame without a request, which is not an answer from Yahoo.
        """
        start, end = self._apply_interval_constraints(
            start_date, end_date, interval, self._today_provider()
        )
        return start != end

    def has_recent_bars(self, stock: str, interval: str) -> bool:
        """
        Ask Yahoo for the last days of one ticker and say whether bars came
        back. Used to find out whether Yahoo is answering properly, with a
        ticker that is known to have bars. Nothing is stored; a failed request
        raises.
        """
        self._limiter()
        ticker = self._ticker_factory(stock)
        try:
            df = ticker.history(period="5d", interval=interval, raise_errors=True)
        except YFTickerMissingError as exc:
            return not self._empty_or_failed(ticker, exc).empty
        return not df.empty

    def _fetch_window(
        self,
        ticker: yf.Ticker,
        stock: str,
        start_date: datetime,
        end_date: datetime,
        interval: str,
    ) -> pd.DataFrame:
        today = self._today_provider()
        start, end = self._apply_interval_constraints(
            start_date, end_date, interval, today
        )
        if start == end:
            # A window with nothing in it (see _apply_interval_constraints):
            # there is nothing to ask Yahoo for.
            return self._empty_frame_factory()

        try:
            df = ticker.history(
                start=start, end=end, interval=interval, raise_errors=True
            )
        except YFTickerMissingError as exc:
            return self._empty_or_failed(ticker, exc)

        return self._normalize_dataframe(df, stock)

    def _fetch_all(
        self,
        ticker: yf.Ticker,
        stock: str,
        interval: str,
    ) -> pd.DataFrame:
        period = "max"
        if interval in HOURLY_INTERVALS:
            period = "730d"
        elif interval in SHORT_LIVED_INTERVALS:
            period = "7d"

        try:
            df = ticker.history(period=period, interval=interval, raise_errors=True)
        except YFTickerMissingError as exc:
            return self._empty_or_failed(ticker, exc)

        return self._normalize_dataframe(df, stock)

    def _empty_or_failed(
        self, ticker: object, error: YFTickerMissingError
    ) -> pd.DataFrame:
        if isinstance(error, YFTzMissingError):
            return self._missing_timezone(ticker, error)
        if is_no_data_answer(ticker, error):
            return self._empty_frame_factory()
        raise FetchFailed(str(error)) from error

    def _missing_timezone(
        self, ticker: object, error: YFTzMissingError
    ) -> pd.DataFrame:
        """
        Decide what a missing time zone means by asking Yahoo once more.

        For a window or the full history yfinance first looks up the ticker's
        time zone. It reports "no timezone found" for a symbol Yahoo does not
        know, and also when the lookup itself failed: it swallows that error.
        A request by period needs no time zone, and its answer tells the two
        apart. Any other error of that request is a failure and propagates.
        """
        self._limiter()
        try:
            ticker.history(period="5d", interval="1d", raise_errors=True)
        except YFTzMissingError:
            return self._empty_frame_factory()  # nothing more can be learned
        except YFTickerMissingError as probe_error:
            if is_no_data_answer(ticker, probe_error):
                return self._empty_frame_factory()
            raise FetchFailed(str(probe_error)) from probe_error
        raise FetchFailed(
            f"{error}, but Yahoo knows the symbol: the time zone lookup failed"
        ) from error

    def _apply_interval_constraints(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: str,
        today: datetime,
    ) -> tuple[datetime, datetime]:
        start = start_date
        end = end_date

        if interval in HOURLY_INTERVALS:
            if (today - end).days >= 729:
                # The whole window is older than Yahoo keeps: nothing to ask.
                return end, end

            if (today - start).days >= 729:
                start = today - pd.Timedelta(days=729)
                start = start.replace(hour=8, minute=0, second=0, microsecond=0)

        if interval in SHORT_LIVED_INTERVALS:
            if (today - start).days >= 7:
                start = today - pd.Timedelta(days=7)
                start = start.replace(hour=0, minute=0, second=0, microsecond=0)

            if (today - end).days >= 7:
                end = today

        return start, end

    def _normalize_dataframe(self, df: pd.DataFrame, stock: str) -> pd.DataFrame:
        if df.empty:
            return self._empty_frame_factory()

        normalized = df.rename_axis("date").reset_index()
        normalized["date"] = pd.to_datetime(normalized["date"]).dt.tz_localize(None)
        normalized.columns = [col.lower() for col in normalized.columns]
        normalized["stock"] = stock

        columns = ["date", "open", "high", "low", "close", "volume", "stock"]
        normalized = normalized[columns]
        normalized.set_index(["stock", "date"], inplace=True)
        return normalized
