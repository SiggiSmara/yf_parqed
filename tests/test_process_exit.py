"""Tests for the daemon exit helper (common/process_exit.py)."""

import subprocess
import sys
import textwrap

from loguru import logger

from yf_parqed.common.process_exit import exit_daemon_process


def test_queued_log_messages_are_written_before_exit(tmp_path, daemon_exit_calls):
    """Messages still in the logging queue reach the file before the process ends."""
    log_file = tmp_path / "daemon.log"
    logger.remove()
    logger.add(log_file, enqueue=True, format="{message}")
    logger.info("last words")

    exit_daemon_process()

    assert "last words" in log_file.read_text()
    assert daemon_exit_calls == [0]


def test_exit_code_is_passed_on(daemon_exit_calls):
    exit_daemon_process(3)

    assert daemon_exit_calls == [3]


def test_process_ends_without_interpreter_teardown(tmp_path):
    """
    In a real process nothing runs after the call: no atexit handler, no later
    statement. The atexit handler stands in for the interpreter teardown that
    took longer than systemd's stop timeout when the daemon was in swap.
    """
    log_file = tmp_path / "daemon.log"
    teardown_marker = tmp_path / "teardown_ran"
    script = textwrap.dedent(
        f"""
        import atexit
        from pathlib import Path
        from loguru import logger
        from yf_parqed.common.process_exit import exit_daemon_process

        atexit.register(lambda: Path({str(teardown_marker)!r}).touch())
        logger.remove()
        logger.add({str(log_file)!r}, enqueue=True, format="{{message}}")
        logger.info("last words")
        exit_daemon_process()
        Path({str(teardown_marker)!r}).touch()
        """
    )

    result = subprocess.run([sys.executable, "-c", script], timeout=120)

    assert result.returncode == 0
    assert "last words" in log_file.read_text()
    assert not teardown_marker.exists()
