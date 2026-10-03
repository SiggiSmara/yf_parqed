"""Ending a daemon process without Python's interpreter teardown."""

import os
import sys

from loguru import logger

# Indirection so tests can replace the call that ends the process.
_terminate = os._exit


def exit_daemon_process(code: int = 0) -> None:
    """
    End the process now, skipping Python's interpreter teardown.

    A daemon that sits idle between cycles has most of its memory moved to
    swap when the host runs short of RAM. A normal exit walks every object to
    free it, which first reads all of that back from disk. On a hard disk this
    takes longer than systemd's stop timeout, and the process is killed. The
    kernel frees the same memory without reading it.

    Call this only after the daemon's own cleanup is done (files closed, PID
    file removed): atexit handlers and ``finally`` blocks further up the stack
    do not run. Queued log messages are written and log files closed here.
    """
    logger.remove()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except (OSError, ValueError):
            pass
    _terminate(code)
