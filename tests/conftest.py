import re
import signal
import sys
import time

import pytest
from loguru import logger


def strip_ansi(text: str) -> str:
    """Strip ANSI escape sequences from CLI output before asserting on content."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


@pytest.fixture(autouse=True)
def daemon_exit_calls(monkeypatch):
    """
    Keep a daemon's final exit from ending the test run.

    In production a daemon ends with os._exit (see common/process_exit.py).
    Here the call is recorded instead: the list holds the exit codes passed.
    """
    calls: list[int] = []
    monkeypatch.setattr("yf_parqed.common.process_exit._terminate", calls.append)
    yield calls
    # The exit also removes every loguru sink, which a real process never
    # notices. Give the next test a default sink back. It looks sys.stderr up
    # on each write because pytest swaps that stream for every test.
    logger.remove()
    logger.add(lambda message: sys.stderr.write(message), level="INFO")


@pytest.fixture
def stop_on_first_sleep(monkeypatch):
    """
    Deliver a stop request the way systemd does: the daemon's own SIGTERM
    handler runs while the daemon sleeps.

    Every time.sleep in the process returns at once, and from the moment the
    daemon has registered its handler each one also delivers the signal. Use
    it only with the cycle's work mocked out, so that the first sleep reached
    is the wait between cycles.
    """
    handlers = {}
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.__setitem__(signum, handler)
    )

    def sleep(_seconds):
        handler = handlers.get(signal.SIGTERM)
        if handler is not None:
            handler(signal.SIGTERM, None)

    monkeypatch.setattr(time, "sleep", sleep)


def pytest_sessionfinish(session, exitstatus):
    """Drop the default sink so atexit handlers left by PID-file tests end the run quietly."""
    logger.remove()
