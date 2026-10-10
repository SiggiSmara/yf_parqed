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
#: Answers "does Yahoo return bars for a ticker that is known to have them?",
#: for an interval and, when the cycle has one, the last ticker that had bars.
HealthCheck = Callable[[str, str | None], bool]

# After this many tickers in a row that failed, or that returned nothing after
# having had bars, the cycle finds out whether Yahoo is answering (the health
# check) and ends if it is not. Without a health check it ends right away. A
# run of that length is normal in a later cycle of a night, where only the
# tickers that failed or went quiet are still due.
MAX_CONSECUTIVE_FAILURES = 20

# Failures that go on for this many tickers end the cycle even when Yahoo
# answers: then it is the host (disk full, permissions), not Yahoo.
MAX_FAILURES_WITHOUT_SUCCESS = 100

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

# What a processor may report for a ticker. WENT_QUIET is an empty answer from
# a ticker that had bars at its last answer: normal now and then, but this many
# of them one after the other means Yahoo is not answering properly, in a way
# that cannot be told from "no data" one ticker at a time.
BARS = "bars"
EMPTY = "empty"
WENT_QUIET = "went_quiet"


@dataclass
class CycleResult:
    """What one run of the scheduler did."""

    processed: int = 0
    skipped: int = 0
    went_quiet: int = 0
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
        health_check: HealthCheck | None = None,
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
        self.health_check = health_check
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
        the tickers after it still run. After ``MAX_CONSECUTIVE_FAILURES``
        failures in a row, or that many tickers in a row that had bars and
        return nothing now, the cycle ends unless the health check says Yahoo
        is answering.
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
        quiet_in_a_row = 0
        since_checkpoint = 0
        last_with_bars: str | None = None
        traced: set[type] = set()

        def yahoo_answers(interval: str) -> bool:
            if self.health_check is None:
                return False
            try:
                return bool(self.health_check(interval, last_with_bars))
            except Exception as exc:
                logger.warning(f"The health check failed: {type(exc).__name__}: {exc}")
                return False

        for interval in self._intervals_provider():
            if stopping():
                return result
            interval_stocks = [
                ticker
                for ticker in active_tickers
                if self._registry.is_active_for_interval(ticker, interval)
            ]
            if self.is_due is not None:
                due = [t for t in interval_stocks if self.is_due(t, interval)]
                already_done = len(interval_stocks) - len(due)
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
                    if failures_in_a_row % MAX_CONSECUTIVE_FAILURES == 0:
                        if (
                            failures_in_a_row < MAX_FAILURES_WITHOUT_SUCCESS
                            and yahoo_answers(interval)
                        ):
                            logger.warning(
                                f"{failures_in_a_row} tickers failed in a row, but "
                                "Yahoo answers for a ticker that has bars; going on"
                            )
                        else:
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
                    quiet_in_a_row = 0
                    last_with_bars = ticker
                elif outcome == WENT_QUIET:
                    result.went_quiet += 1
                    quiet_in_a_row += 1
                    if quiet_in_a_row >= MAX_CONSECUTIVE_FAILURES:
                        if yahoo_answers(interval):
                            quiet_in_a_row = 0
                        else:
                            logger.error(
                                f"{quiet_in_a_row} tickers that had bars at their "
                                "last fetch returned nothing, one after the other, "
                                "and Yahoo does not answer for a ticker that has "
                                "bars. Ending the update cycle"
                            )
                            result.aborted = True
                            return result
                since_checkpoint += 1
                if (
                    self.checkpoint is not None
                    and since_checkpoint >= self._checkpoint_every
                ):
                    since_checkpoint = 0
                    self.checkpoint()

        return result
