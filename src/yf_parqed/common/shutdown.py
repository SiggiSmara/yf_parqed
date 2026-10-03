"""Helpers for loops that must notice a daemon stop request promptly."""

import signal
import time
from typing import Callable

from loguru import logger

#: Answers "has a stop been requested?". Signal handlers only set a flag; the
#: loops that do the work ask this between items.
StopCheck = Callable[[], bool]

# Longest an idle daemon waits before it notices a stop request. This must stay
# well below the units' TimeoutStopSec (30 seconds for the Xetra units).
IDLE_CHECK_SECONDS = 10

# How often a long wait looks at the stop check. A signal does not cut
# time.sleep short, so a long wait would otherwise outlast the stop timeout.
STOP_CHECK_STEP_SECONDS = 1.0


class ShutdownRequested(Exception):
    """A wait or fetch was abandoned because the daemon was asked to stop."""


def sleep_unless_stopped(
    seconds: float,
    should_stop: StopCheck | None,
    step: float = STOP_CHECK_STEP_SECONDS,
) -> bool:
    """
    Sleep for ``seconds``, looking at ``should_stop`` every ``step`` seconds.

    Returns True when the full time passed and False when a stop request cut it
    short. Without a stop check this is a plain ``time.sleep``.
    """
    if should_stop is None:
        time.sleep(seconds)
        return True

    remaining = seconds
    while remaining > 0:
        if should_stop():
            return False
        chunk = min(step, remaining)
        time.sleep(chunk)
        remaining -= chunk
    return not should_stop()


class StopFlag:
    """
    A daemon's stop request: set by SIGTERM or SIGINT, read by the work loops.

    The handler only records the signal. loguru's handlers are not re-entrant,
    so a log call there can hit the lock of the line the main thread is writing;
    the main loop logs the request later with ``log_request``. An instance is
    itself a ``StopCheck``: ``stop()`` says whether a stop was requested.
    """

    def __init__(self) -> None:
        self.signum: int | None = None
        self._requested = False

    def __call__(self) -> bool:
        return self._requested

    def handle(self, signum: int, frame: object) -> None:
        self.signum = signum
        self._requested = True

    def install(self) -> None:
        """Register this flag as the handler for SIGTERM and SIGINT."""
        signal.signal(signal.SIGTERM, self.handle)
        signal.signal(signal.SIGINT, self.handle)

    def log_request(self) -> None:
        if self.signum is not None:
            logger.info(f"Received signal {self.signum}, shutting down gracefully...")

    def sleep(self, seconds: float, step: float = IDLE_CHECK_SECONDS) -> bool:
        """Sleep, noticing a stop request within ``step`` seconds. See ``sleep_unless_stopped``."""
        return sleep_unless_stopped(seconds, self, step)
