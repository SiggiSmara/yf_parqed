from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterable, Sequence

import os
from curl_cffi.requests.exceptions import RequestException
from loguru import logger
from rich.progress import track
from yfinance.exceptions import YFException

from ..common.shutdown import StopCheck
from .data_fetcher import FetchFailed
from .ticker_registry import TickerRegistry

IntervalProvider = Callable[[], Sequence[str]]
LimitHandler = Callable[[], None] | None
LoadRegistry = Callable[[], None]
DateProvider = Callable[[], datetime]
ProcessStock = Callable[[str, datetime | None, datetime | None, str], str | None]
#: Answers "is this ticker still to be fetched for this interval?".
DueCheck = Callable[[str, str], bool]

# After this many tickers in a row that failed, the cycle ends. The rule is
# judged only for an interval of which at least half the active tickers are
# due: in a later cycle of a night only the failed tickers are still due, so
# they always come in a row.
MAX_CONSECUTIVE_FAILURES = 20

# Failures that say what went wrong in their message: Yahoo, the network, the
# disk, a refused write. Anything else is a bug and is logged with its
# traceback, once per kind of error in a cycle.
EXPECTED_FAILURES = (
    FetchFailed,
    YFException,
    RequestException,
    OSError,
    RuntimeError,
)

# A checkpoint (the registry save) runs after this many processed tickers.
CHECKPOINT_EVERY = 500

# What a processor may report for a ticker.
BARS = "bars"
EMPTY = "empty"


@dataclass
class CycleResult:
    """What one run of the scheduler did."""

    processed: int = 0
    skipped: int = 0
    with_bars: int = 0
    empty: int = 0
    failed: list[str] = field(default_factory=list)
    stopped: bool = False
    aborted: bool = False

    @property
    def ran_to_end(self) -> bool:
        return not self.stopped and not self.aborted


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
        is_due: DueCheck | None = None,
        checkpoint: Callable[[], None] | None = None,
        checkpoint_every: int = CHECKPOINT_EVERY,
    ) -> None:
        self._registry = registry
        self._intervals_provider = intervals
        self._load_registry = loader
        self._limit = limiter
        self._process_stock = processor
        self._today_provider = today_provider
        self._progress_factory = progress_factory or self._default_progress
        self.is_due = is_due
        self.checkpoint = checkpoint
        self._checkpoint_every = checkpoint_every

    @staticmethod
    def _default_progress(
        stocks: Iterable[str], description: str, disable: bool
    ) -> Iterable[str]:
        return track(stocks, description=description, disable=disable)

    def run(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        should_stop: StopCheck | None = None,
    ) -> CycleResult:
        """
        Process every active ticker that is due; with ``should_stop``, end after the current ticker.

        A ticker whose processing raises is logged and left for a later cycle;
        the tickers after it still run. ``MAX_CONSECUTIVE_FAILURES`` failures
        in a row end the cycle, in an interval of which at least half the
        active tickers are due.
        """
        self._load_registry()
        result = CycleResult()

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

        def stopping() -> bool:
            if should_stop is not None and should_stop():
                logger.info("Stop requested, ending the update cycle")
                result.stopped = True
                return True
            return False

        failures_in_a_row = 0
        since_checkpoint = 0
        traced: set[type] = set()

        for interval in self._intervals_provider():
            if stopping():
                return result
            interval_stocks = [
                ticker
                for ticker in active_tickers
                if self._registry.is_active_for_interval(ticker, interval)
            ]
            # A run of failures is counted within one interval.
            failures_in_a_row = 0
            judge_failure_runs = True
            if self.is_due is not None:
                due = [t for t in interval_stocks if self.is_due(t, interval)]
                already_done = len(interval_stocks) - len(due)
                judge_failure_runs = 2 * len(due) >= len(interval_stocks)
                result.skipped += already_done
                interval_stocks = due
                logger.info(
                    f"Processing {len(interval_stocks)} tickers for interval {interval} "
                    f"({already_done} already fetched tonight)"
                )
            else:
                logger.info(
                    f"Processing {len(interval_stocks)} tickers for interval {interval}"
                )

            for ticker in self._progress_factory(
                interval_stocks,
                description=f"Processing stocks for interval:{interval}",
                disable=disable_track,
            ):
                if stopping():
                    return result
                if self._limit is not None:
                    self._limit()
                try:
                    outcome = self._process_stock(
                        stock=ticker,
                        start_date=start_date,
                        end_date=resolved_end,
                        interval=interval,
                    )
                except Exception as exc:
                    # Nothing was recorded for this ticker, so a later cycle
                    # asks for it again.
                    message = (
                        f"{ticker} failed for interval {interval}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    if isinstance(exc, EXPECTED_FAILURES) or type(exc) in traced:
                        logger.error(message)
                    else:
                        traced.add(type(exc))
                        logger.opt(exception=exc).error(message)
                    result.failed.append(ticker)
                    failures_in_a_row += 1
                    if (
                        judge_failure_runs
                        and failures_in_a_row >= MAX_CONSECUTIVE_FAILURES
                    ):
                        logger.error(
                            f"{failures_in_a_row} tickers failed in a row; "
                            "ending the update cycle"
                        )
                        result.aborted = True
                        return result
                    continue

                failures_in_a_row = 0
                result.processed += 1
                if outcome == BARS:
                    result.with_bars += 1
                elif outcome == EMPTY:
                    result.empty += 1
                since_checkpoint += 1
                if (
                    self.checkpoint is not None
                    and since_checkpoint >= self._checkpoint_every
                ):
                    since_checkpoint = 0
                    self.checkpoint()

        return result
