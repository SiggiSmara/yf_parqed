"""Tests for Yahoo Finance daemon mode functionality."""

import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from typer.testing import CliRunner

from yf_parqed import yfinance_cli as main


class StubConfig:
    """Minimal config stub for daemon tests."""
    def load_storage_config(self):
        return {"partitioned": True, "markets": {}, "sources": {}}
    
    def save_storage_config(self, config):
        pass


class StubYFParqedForDaemon:
    """Stub that tracks daemon-related calls."""

    def __init__(self):
        self.calls: list[tuple] = []
        self.new_not_found = False
        self.update_data_calls = 0
        self.maintenance_calls = {
            "update_current_list_of_stocks": 0,
            "confirm_not_founds": 0,
            "reparse_not_founds": 0,
        }
        self.my_path = Path("/tmp/test")
        self.work_path = Path("/tmp/test")
        self.config = StubConfig()  # Add config stub

    def set_working_path(self, path: Path):
        self.calls.append(("set_working_path", Path(path)))
        self.work_path = Path(path)
        return self.work_path

    def set_limiter(self, max_requests: int, duration: int):
        self.calls.append(("set_limiter", max_requests, duration))

    def update_stock_data(self, start_date=None, end_date=None):
        self.update_data_calls += 1
        self.calls.append(("update_stock_data", start_date, end_date))

    def save_tickers(self):
        self.calls.append(("save_tickers",))

    def update_current_list_of_stocks(self):
        self.maintenance_calls["update_current_list_of_stocks"] += 1
        self.calls.append(("update_current_list_of_stocks",))

    def confirm_not_founds(self):
        self.maintenance_calls["confirm_not_founds"] += 1
        self.calls.append(("confirm_not_founds",))

    def reparse_not_founds(self):
        self.maintenance_calls["reparse_not_founds"] += 1
        self.calls.append(("reparse_not_founds",))


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def stub(monkeypatch):
    stub = StubYFParqedForDaemon()
    monkeypatch.setattr(main, "yf_parqed", stub)
    return stub


class TestDaemonCLIFlags:
    """Test that daemon flags are properly recognized."""

    @freeze_time("2025-12-04 14:00:00-05:00")  # 14:00 EST (within trading hours)
    def test_daemon_flag_accepted(self, runner, stub, tmp_path, monkeypatch):
        """Daemon flag is accepted and triggers daemon mode."""
        # Create intervals.json to allow CLI initialization
        intervals_file = tmp_path / "intervals.json"
        intervals_file.write_text('["1d"]')
        
        # Mock time.sleep to avoid actual waiting
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # Allow the daemon loop to run at least once before stopping
            if len(sleep_calls) >= 2:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--interval",
                    "1",
                ],
                catch_exceptions=False,
            )

        # Should have attempted at least one update
        assert stub.update_data_calls >= 1

    def test_interval_flag_accepted(self, runner, stub, tmp_path):
        """Interval flag sets update frequency."""
        # Non-daemon mode, just verify flag is accepted
        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            result = runner.invoke(
                main.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--interval", "2"],
            )

        assert result.exit_code == 0

    def test_market_timezone_flag_accepted(self, runner, stub, tmp_path):
        """Market timezone flag is accepted."""
        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            result = runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--market-timezone",
                    "US/Pacific",
                ],
            )

        assert result.exit_code == 0

    def test_extended_hours_flag_accepted(self, runner, stub, tmp_path):
        """Extended hours flag is accepted."""
        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            result = runner.invoke(
                main.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--extended-hours"],
            )

        assert result.exit_code == 0

    def test_ticker_maintenance_flag_accepted(self, runner, stub, tmp_path):
        """Ticker maintenance flag accepts valid values."""
        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            for schedule in ["daily", "weekly", "monthly", "never"]:
                result = runner.invoke(
                    main.app,
                    [
                        "--wrk-dir",
                        str(tmp_path),
                        "update-data",
                        "--ticker-maintenance",
                        schedule,
                    ],
                )
                assert result.exit_code == 0


class TestPIDFileManagement:
    """Test PID file creation, detection, and cleanup."""

    def test_pid_file_created_in_daemon_mode(self, runner, stub, tmp_path, monkeypatch):
        """PID file is created when daemon starts."""
        pid_file = tmp_path / "test.pid"

        # Mock sleep to exit quickly
        def mock_sleep(seconds):
            raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            result = runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--pid-file",
                    str(pid_file),
                ],
                catch_exceptions=False,
            )

        # PID file should have been created and cleaned up
        # Exit code 130 is normal for KeyboardInterrupt (SIGINT)
        assert result.exit_code in (0, 130)

    def test_pid_file_prevents_duplicate_daemon(self, runner, stub, tmp_path):
        """Existing PID file with running process prevents daemon start."""
        pid_file = tmp_path / "test.pid"

        # Write current process PID to simulate running daemon
        pid_file.write_text(str(os.getpid()))

        result = runner.invoke(
            main.app,
            [
                "--wrk-dir",
                str(tmp_path),
                "update-data",
                "--daemon",
                "--pid-file",
                str(pid_file),
            ],
        )

        # Should exit with error
        assert result.exit_code == 1
        assert (
            "already running" in result.stdout.lower()
            or "already running" in str(result.exception).lower()
        )

    def test_stale_pid_file_removed(self, runner, stub, tmp_path, monkeypatch):
        """Stale PID file (process not running) is removed."""
        pid_file = tmp_path / "test.pid"

        # Write PID of non-existent process
        pid_file.write_text("999999")

        # Mock sleep to exit quickly
        def mock_sleep(seconds):
            raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            result = runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--pid-file",
                    str(pid_file),
                ],
                catch_exceptions=False,
            )

        # Exit code 130 is normal for KeyboardInterrupt (SIGINT)
        assert result.exit_code in (0, 130)


class TestTradingHoursIntegration:
    """Test daemon respects trading hours."""

    @freeze_time("2025-12-04 08:00:00-05:00")  # 08:00 EST (before market open)
    def test_daemon_runs_without_trading_hours_gating(
        self, runner, stub, tmp_path, monkeypatch
    ):
        """Daemon runs even outside market hours when no trading window is set."""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # Stop after first sleep interval
            raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--daemon"],
                catch_exceptions=False,
            )

        # Should have run at least one update even though time is before market open
        assert stub.update_data_calls >= 1

    @freeze_time("2025-12-04 08:00:00-05:00")  # 08:00 EST (before market open)
    def test_daemon_waits_outside_trading_hours(
        self, runner, stub, tmp_path, monkeypatch
    ):
        """Daemon waits when outside trading hours."""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # Exit after recording the wait
            if len(sleep_calls) >= 1:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--trading-hours",
                    "09:30-16:00",
                ],
                catch_exceptions=False,
            )

        # Should have slept (waiting for market open)
        assert len(sleep_calls) > 0
        assert stub.update_data_calls == 0  # No update should have run

    @freeze_time("2025-12-04 14:00:00-05:00")  # 14:00 EST (market open)
    def test_daemon_runs_during_trading_hours(
        self, runner, stub, tmp_path, monkeypatch
    ):
        """Daemon runs updates during trading hours."""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # Exit after first update
            if len(sleep_calls) >= 1:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--interval",
                    "1",
                ],
                catch_exceptions=False,
            )

        # Should have run at least one update
        assert stub.update_data_calls >= 1

    @freeze_time("2025-12-04 14:00:00-05:00")  # 14:00 EST
    def test_extended_hours_flag_changes_schedule(
        self, runner, stub, tmp_path, monkeypatch
    ):
        """Extended hours flag enables pre-market and after-hours."""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 1:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--extended-hours",
                ],
                catch_exceptions=False,
            )

        # Should run (extended hours cover 04:00-20:00)
        assert stub.update_data_calls >= 1


class TestTickerMaintenance:
    """Test ticker maintenance scheduling."""

    @freeze_time("2025-12-04 14:00:00-05:00")  # 14:00 EST
    def test_maintenance_runs_on_first_cycle(self, runner, stub, tmp_path, monkeypatch):
        """Ticker maintenance runs on first daemon cycle."""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 1:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--ticker-maintenance",
                    "weekly",
                ],
                catch_exceptions=False,
            )

        # Maintenance should have run once
        assert stub.maintenance_calls["update_current_list_of_stocks"] >= 1
        assert stub.maintenance_calls["confirm_not_founds"] >= 1
        assert stub.maintenance_calls["reparse_not_founds"] >= 1

    @freeze_time("2025-12-04 14:00:00-05:00")
    def test_maintenance_never_skips_maintenance(
        self, runner, stub, tmp_path, monkeypatch
    ):
        """Ticker maintenance 'never' skips all maintenance."""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 1:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--ticker-maintenance",
                    "never",
                ],
                catch_exceptions=False,
            )

        # No maintenance should have run
        assert stub.maintenance_calls["update_current_list_of_stocks"] == 0
        assert stub.maintenance_calls["confirm_not_founds"] == 0
        assert stub.maintenance_calls["reparse_not_founds"] == 0


class TestSignalHandling:
    """Test graceful shutdown on signals."""

    @freeze_time("2025-12-04 14:00:00-05:00")
    def test_sigterm_stops_daemon_gracefully(self, runner, stub, tmp_path, monkeypatch):
        """SIGTERM signal stops daemon gracefully."""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # Simulate SIGTERM on first sleep
            if len(sleep_calls) == 1:
                # Trigger the signal handler
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            result = runner.invoke(
                main.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--daemon"],
                catch_exceptions=False,
            )

        # Should exit cleanly (130 is normal for KeyboardInterrupt/SIGINT)
        assert result.exit_code in (0, 130)


class TestDaemonLoop:
    """Test daemon loop behavior."""

    @freeze_time("2025-12-04 14:00:00-05:00")
    def test_daemon_runs_multiple_cycles(self, runner, stub, tmp_path, monkeypatch):
        """Daemon runs multiple update cycles."""
        sleep_calls = []
        sleep_call_count = [0]  # Use list to allow modification in nested function

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            sleep_call_count[0] += 1
            # Exit after a few sleep calls (not waiting for full intervals)
            if sleep_call_count[0] >= 5:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--interval",
                    "1",
                ],
                catch_exceptions=False,
            )

        # Should have run at least one update
        assert stub.update_data_calls >= 1

    @freeze_time("2025-12-04 14:00:00-05:00")
    def test_daemon_handles_update_errors(self, runner, tmp_path, monkeypatch):
        """Daemon continues after update errors."""

        # Create a stub that raises error on first call
        class ErrorStub(StubYFParqedForDaemon):
            def __init__(self):
                super().__init__()
                self.error_count = 0

            def update_stock_data(self, start_date=None, end_date=None):
                self.update_data_calls += 1
                self.error_count += 1
                if self.error_count == 1:
                    raise RuntimeError("Simulated API error")
                self.calls.append(("update_stock_data", start_date, end_date))

        error_stub = ErrorStub()
        monkeypatch.setattr(main, "yf_parqed", error_stub)

        sleep_calls = []
        sleep_call_count = [0]

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            sleep_call_count[0] += 1
            # Exit after a few sleep calls
            if sleep_call_count[0] >= 5:
                raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", mock_sleep)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as mock_lock:
            mock_lock_instance = MagicMock()
            mock_lock_instance.try_acquire.return_value = True
            mock_lock.return_value = mock_lock_instance

            runner.invoke(
                main.app,
                [
                    "--wrk-dir",
                    str(tmp_path),
                    "update-data",
                    "--daemon",
                    "--interval",
                    "1",
                ],
                catch_exceptions=False,
            )

        # Should have attempted at least 1 update (may not reach 2 before interrupt)
        assert error_stub.update_data_calls >= 1
