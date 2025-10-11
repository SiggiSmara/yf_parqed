from __future__ import annotations

from datetime import datetime
from typing import Callable

import pandas as pd
import yfinance as yf
from curl_cffi.requests.exceptions import HTTPError


class DataFetcher:
    """Wrap Yahoo Finance interactions with limiter and normalization helpers."""

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

        try:
            df = ticker.history(start=start, end=end, interval=interval)
        except Exception:
            return self._empty_frame_factory()

        return self._normalize_dataframe(df, stock)

    def _fetch_all(
        self,
        ticker: yf.Ticker,
        stock: str,
        interval: str,
    ) -> pd.DataFrame:
        period = "10y"
        if interval in ("60m", "90m", "1h"):
            period = "729d"
        elif interval in ("1m", "2m", "5m", "15m", "30m"):
            period = "8d"

        try:
            df = ticker.history(period=period, interval=interval)
        except HTTPError:
            return self._empty_frame_factory()

        return self._normalize_dataframe(df, stock)

    def _apply_interval_constraints(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: str,
        today: datetime,
    ) -> tuple[datetime, datetime]:
        start = start_date
        end = end_date

        if interval in ("60m", "90m", "1h"):
            if (today - start).days >= 729:
                start = today - pd.Timedelta(days=729)
                start = start.replace(hour=8, minute=0, second=0, microsecond=0)

            if (today - end).days >= 729:
                end = today

            if (end - start).days >= 729:
                return end, end  # force empty window

        if interval in ("1m", "2m", "5m", "15m", "30m"):
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
