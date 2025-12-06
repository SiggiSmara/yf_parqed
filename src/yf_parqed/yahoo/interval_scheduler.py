from __future__ import annotations

from datetime import datetime
from typing import Callable, Iterable, Sequence

import os
from loguru import logger
from rich.progress import track

from .ticker_registry import TickerRegistry

IntervalProvider = Callable[[], Sequence[str]]
LimitHandler = Callable[[], None] | None
LoadRegistry = Callable[[], None]
DateProvider = Callable[[], datetime]
ProcessStock = Callable[[str, datetime | None, datetime | None, str], None]


class IntervalScheduler:
    """Coordinate which tickers run for which intervals."""

    def __init__(
        self,
        registry: TickerRegistry,
        intervals: IntervalProvider,
        loader: LoadRegistry,
        limiter: LimitHandler,
        processor: ProcessStock,
        today_provider: DateProvider,
        progress_factory: Callable[[Iterable[str], str, bool], Iterable[str]]
        | None = None,
    ) -> None:
        self._registry = registry
        self._intervals_provider = intervals
        self._load_registry = loader
        self._limit = limiter
        self._process_stock = processor
        self._today_provider = today_provider
        self._progress_factory = progress_factory or self._default_progress

    @staticmethod
    def _default_progress(
        stocks: Iterable[str], description: str, disable: bool
    ) -> Iterable[str]:
        return track(stocks, description=description, disable=disable)

    def run(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> None:
        self._load_registry()

        active_tickers = [
            ticker
            for ticker, data in self._registry.tickers.items()
            if data.get("status", "active") == "active"
        ]
        not_found_count = sum(
            1
            for _ticker, data in self._registry.tickers.items()
            if data.get("status") == "not_found"
        )

        logger.info(f"Number of tickers to process: {len(active_tickers)}")
        logger.info(f"Number of tickers in exclude list: {not_found_count}")

        disable_track = not (os.getenv("YF_PARQED_LOG_LEVEL", "INFO") == "INFO")
        resolved_end = end_date or self._today_provider()

        for interval in self._intervals_provider():
            interval_stocks = [
                ticker
                for ticker in active_tickers
                if self._registry.is_active_for_interval(ticker, interval)
            ]

            logger.info(
                f"Processing {len(interval_stocks)} tickers for interval {interval}"
            )

            for ticker in self._progress_factory(
                interval_stocks,
                description=f"Processing stocks for interval:{interval}",
                disable=disable_track,
            ):
                if self._limit is not None:
                    self._limit()
                self._process_stock(
                    stock=ticker,
                    start_date=start_date,
                    end_date=resolved_end,
                    interval=interval,
                )
