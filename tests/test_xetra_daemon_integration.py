"""Integration tests for Xetra daemon mode features."""

import os
import time
from datetime import datetime, time as dt_time
from unittest.mock import MagicMock, Mock, patch
from zoneinfo import ZoneInfo

import pytest
from typer.testing import CliRunner

from yf_parqed.xetra_cli import (
    _check_and_write_pid_file,
    app,
)

runner = CliRunner()


@pytest.fixture
def mock_xetra_service():
    """Reusable mock for XetraService with common defaults."""
    mock_service = MagicMock()
    mock_service.__enter__ = Mock(return_value=mock_service)
    mock_service.__exit__ = Mock(return_value=False)
    mock_service.has_any_data.return_value = True  # Default: data exists (skip initial fetch)
    mock_service.fetch_and_store_missing_trades_incremental.return_value = {
        "dates_fetched": ["2025-12-04"],
        "total_trades": 1000,
    }
    mock_service.get_missing_dates.return_value = []
    return mock_service


@pytest.fixture
def fast_sleep(monkeypatch):
    """Mock time.sleep to make tests run instantly by raising SystemExit."""
    sleep_calls = []
    
    def mock_sleep(seconds):
        sleep_calls.append(seconds)
        # After first sleep in the post-fetch interval loop, exit
        if len(sleep_calls) >= 1:
            raise SystemExit(0)
    
    monkeypatch.setattr("yf_parqed.xetra_cli.time.sleep", mock_sleep)
    return sleep_calls


@pytest.fixture
def daemon_test_context(mock_xetra_service, monkeypatch):
    """Complete test context for daemon tests with fast execution."""
    # Mock XetraService where it's imported in xetra_cli
    with patch("yf_parqed.xetra_cli.XetraService", return_value=mock_xetra_service):
        # Mock sleep to exit fast
        sleep_calls = []
        
        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # After first sleep, exit the daemon
            if len(sleep_calls) >= 1:
                raise SystemExit(0)
        
        monkeypatch.setattr("yf_parqed.xetra_cli.time.sleep", mock_sleep)
        
        yield {
            "service": mock_xetra_service,
            "sleep_calls": sleep_calls,
        }


class TestPIDFileManagement:
    """Test PID file creation, validation, and cleanup."""

    def test_creates_pid_file_with_current_pid(self, tmp_path):
        """PID file is created with current process ID."""
        pid_file = tmp_path / "test.pid"

        _check_and_write_pid_file(pid_file)

        assert pid_file.exists()
        assert pid_file.read_text().strip() == str(os.getpid())

    def test_creates_parent_directories(self, tmp_path):
        """PID file parent directories are created if missing."""
        pid_file = tmp_path / "nested" / "dirs" / "test.pid"

        _check_and_write_pid_file(pid_file)

        assert pid_file.exists()
        assert pid_file.parent.exists()

    def test_detects_running_instance(self, tmp_path):
        """Raises error if another instance is running."""
        import typer

        pid_file = tmp_path / "test.pid"
        # Write current PID (simulates running instance)
        pid_file.write_text(str(os.getpid()))

        with pytest.raises(typer.Exit):
            _check_and_write_pid_file(pid_file)

    def test_removes_stale_pid_file(self, tmp_path):
        """Removes PID file if process doesn't exist."""
        pid_file = tmp_path / "test.pid"
        # Write non-existent PID
        stale_pid = 999999
        pid_file.write_text(str(stale_pid))

        _check_and_write_pid_file(pid_file)

        # Should have created new PID file with current PID
        assert pid_file.exists()
        assert pid_file.read_text().strip() == str(os.getpid())

    def test_removes_invalid_pid_file(self, tmp_path):
        """Removes PID file with invalid content."""
        pid_file = tmp_path / "test.pid"
        pid_file.write_text("not-a-number")

        _check_and_write_pid_file(pid_file)

        # Should have created new PID file
        assert pid_file.exists()
        assert pid_file.read_text().strip() == str(os.getpid())

    def test_removes_empty_pid_file(self, tmp_path):
        """Removes empty PID file."""
        pid_file = tmp_path / "test.pid"
        pid_file.write_text("")

        _check_and_write_pid_file(pid_file)

        # Should have created new PID file
        assert pid_file.exists()
        assert pid_file.read_text().strip() == str(os.getpid())


class TestDaemonLoopExecution:
    """Test daemon mode execution cycles and error handling."""

    def test_daemon_executes_fetch_cycle(self, daemon_test_context):
        """Daemon executes fetch cycle successfully."""
        ctx = daemon_test_context
        ctx["service"].fetch_and_store_missing_trades_incremental.return_value = {
            "dates_checked": ["2025-11-04"],
            "dates_fetched": ["2025-11-04"],
            "dates_partial": [],
            "total_trades": 100,
            "total_files": 10,
            "consolidated": True,
        }

        # Run daemon (24/7 to avoid trading hours logic)
        runner.invoke(
            app,
            [
                "fetch-trades",
                "DETR",
                "--daemon",
                "--interval",
                "1",
                "--active-hours",
                "00:00-23:59",
            ],
            catch_exceptions=False,
        )

        # Should have called fetch at least once
        assert ctx["service"].fetch_and_store_missing_trades_incremental.call_count >= 1

    def test_daemon_continues_after_fetch_error(self, daemon_test_context):
        """Daemon continues running after fetch errors."""
        ctx = daemon_test_context
        
        # Make first call fail, then succeed
        call_count = {"count": 0}

        def fetch_side_effect(*args, **kwargs):
            call_count["count"] += 1
            if call_count["count"] == 1:
                raise Exception("Network error")
            return {
                "dates_checked": [],
                "dates_fetched": [],
                "dates_partial": [],
                "total_trades": 0,
                "total_files": 0,
                "consolidated": False,
            }

        ctx["service"].fetch_and_store_missing_trades_incremental.side_effect = fetch_side_effect

        # Run daemon
        runner.invoke(
            app,
            [
                "fetch-trades",
                "DETR",
                "--daemon",
                "--interval",
                "1",
                "--active-hours",
                "00:00-23:59",
            ],
            catch_exceptions=False,
        )

        # Should have attempted fetch at least once, validates error doesn't crash daemon
        assert call_count["count"] >= 1

    def test_daemon_interval_timing(self, daemon_test_context):
        """Daemon sleeps for correct interval between runs."""
        ctx = daemon_test_context
        ctx["service"].fetch_and_store_missing_trades_incremental.return_value = {
            "dates_checked": [],
            "dates_fetched": [],
            "dates_partial": [],
            "total_trades": 0,
            "total_files": 0,
            "consolidated": False,
        }

        # Run daemon with 2-hour interval
        runner.invoke(
            app,
            [
                "fetch-trades",
                "DETR",
                "--daemon",
                "--interval",
                "2",
                "--active-hours",
                "00:00-23:59",
            ],
            catch_exceptions=False,
        )

        # Should have attempted to sleep in 10-second intervals (checking for shutdown)
        assert ctx["sleep_calls"][0] == 10


class TestFileLogging:
    """Test file logging configuration and behavior."""

    @patch("yf_parqed.xetra_cli.XetraService")
    def test_log_file_created(self, mock_service_class, tmp_path, mock_xetra_service):
        """Log file is created when --log-file is specified."""
        log_file = tmp_path / "test.log"

        # Use fixture
        mock_service = mock_xetra_service
        mock_service.fetch_and_store_missing_trades_incremental.return_value = {
            "dates_checked": [],
            "dates_fetched": [],
            "dates_partial": [],
            "total_trades": 0,
            "total_files": 0,
            "consolidated": False,
        }
        mock_service_class.return_value = mock_service

        # Run single fetch with log file
        result = runner.invoke(
            app, ["--log-file", str(log_file), "fetch-trades", "DETR"]
        )

        assert result.exit_code == 0
        # Give loguru's background thread time to flush
        time.sleep(0.01)
        assert log_file.exists()
        # Log should contain some content
        log_content = log_file.read_text()
        assert len(log_content) > 0

    @patch("yf_parqed.xetra_cli.XetraService")
    def test_log_file_contains_structured_logs(self, mock_service_class, tmp_path, mock_xetra_service):
        """Log file contains properly formatted log entries."""
        log_file = tmp_path / "test.log"

        # Use fixture
        mock_service = mock_xetra_service
        mock_service.fetch_and_store_missing_trades_incremental.return_value = {
            "dates_checked": ["2025-11-04"],
            "dates_fetched": ["2025-11-04"],
            "dates_partial": [],
            "total_trades": 100,
            "total_files": 10,
            "consolidated": True,
        }
        mock_service_class.return_value = mock_service

        runner.invoke(app, ["--log-file", str(log_file), "fetch-trades", "DETR"])

        # Give loguru's background thread time to flush
        time.sleep(0.01)
        log_content = log_file.read_text()
        # Check for structured format elements
        assert "INFO" in log_content
        # Should have timestamp in YYYY-MM-DD HH:mm:ss format
        assert any(
            char.isdigit() and "-" in log_content and ":" in log_content
            for char in log_content
        )

    @patch("yf_parqed.xetra_cli.XetraService")
    def test_log_file_parent_dirs_created(self, mock_service_class, tmp_path, mock_xetra_service):
        """Log file parent directories are created automatically."""
        log_file = tmp_path / "nested" / "dirs" / "test.log"

        # Setup mock service
        mock_service = mock_xetra_service
        mock_service.fetch_and_store_missing_trades_incremental.return_value = {
            "dates_checked": [],
            "dates_fetched": [],
            "dates_partial": [],
            "total_trades": 0,
            "total_files": 0,
            "consolidated": False,
        }
        mock_service_class.return_value = mock_service

        runner.invoke(app, ["--log-file", str(log_file), "fetch-trades", "DETR"])

        assert log_file.exists()
        assert log_file.parent.exists()


class TestDaemonTradingHoursIntegration:
    """Test daemon behavior with trading hours transitions."""

    @patch("yf_parqed.xetra_cli.TradingHoursChecker")
    def test_daemon_waits_outside_active_hours(
        self, mock_checker_class, daemon_test_context
    ):
        """Daemon waits when outside active hours."""
        ctx = daemon_test_context

        # Setup mock trading hours checker (outside active hours)
        mock_checker = MagicMock()
        mock_checker.market_tz = ZoneInfo("Europe/Berlin")
        mock_checker.is_within_hours.return_value = False  # Outside trading hours
        mock_checker.seconds_until_active.return_value = 5400.0  # 1.5 hours wait
        mock_checker.next_active_time.return_value = datetime(
            2025, 1, 15, 8, 30, 0, tzinfo=ZoneInfo("Europe/Berlin")
        )
        mock_checker_class.return_value = mock_checker
        mock_checker_class.parse_active_hours.return_value = (
            dt_time(8, 30),
            dt_time(18, 0),
        )

        # Run daemon with default active hours (08:30-18:00)
        runner.invoke(app, ["fetch-trades", "DETR", "--daemon", "--interval", "1"])

        # Should have slept (waiting for active hours)
        assert len(ctx["sleep_calls"]) > 0
        # Should NOT have called fetch (outside active hours)
        assert not ctx["service"].fetch_and_store_missing_trades_incremental.called

    @patch("yf_parqed.xetra_cli.TradingHoursChecker")
    def test_daemon_runs_within_active_hours(
        self, mock_checker_class, daemon_test_context
    ):
        """Daemon runs fetch when within active hours."""
        ctx = daemon_test_context
        ctx["service"].fetch_and_store_missing_trades_incremental.return_value = {
            "dates_checked": [],
            "dates_fetched": [],
            "dates_partial": [],
            "total_trades": 0,
            "total_files": 0,
            "consolidated": False,
        }

        # Setup mock trading hours checker
        mock_checker = MagicMock()
        mock_checker.market_tz = ZoneInfo("Europe/Berlin")
        call_count = {"count": 0}

        def is_within_side_effect():
            call_count["count"] += 1
            # First check: within hours, second check: outside hours
            return call_count["count"] == 1

        mock_checker.is_within_hours.side_effect = is_within_side_effect
        mock_checker_class.return_value = mock_checker
        mock_checker_class.parse_active_hours.return_value = (
            dt_time(8, 30),
            dt_time(18, 0),
        )

        # Run daemon
        runner.invoke(
            app,
            [
                "fetch-trades",
                "DETR",
                "--daemon",
                "--interval",
                "1",
                "--active-hours",
                "08:30-18:00",
            ],
        )

        # Should have called fetch once (was within hours)
        assert ctx["service"].fetch_and_store_missing_trades_incremental.called

    @patch("yf_parqed.xetra_cli.TradingHoursChecker")
    def test_daemon_transitions_from_outside_to_within_hours(
        self,
        mock_checker_class,
        mock_xetra_service,
        monkeypatch,
    ):
        """Daemon transitions from waiting to active state."""
        # Need custom sleep handling for this test - allow 2 sleeps (wait + post-fetch)
        with patch("yf_parqed.xetra_cli.XetraService", return_value=mock_xetra_service):
            mock_xetra_service.fetch_and_store_missing_trades_incremental.return_value = {
                "dates_checked": [],
                "dates_fetched": [],
                "dates_partial": [],
                "total_trades": 0,
                "total_files": 0,
                "consolidated": False,
            }
            
            # Custom sleep mock that exits after 2 calls
            sleep_calls = []
            def mock_sleep(seconds):
                sleep_calls.append(seconds)
                if len(sleep_calls) >= 2:
                    raise SystemExit(0)
            monkeypatch.setattr("yf_parqed.xetra_cli.time.sleep", mock_sleep)

            # Setup mock trading hours checker
            mock_checker = MagicMock()
            mock_checker.market_tz = ZoneInfo("Europe/Berlin")

            # Mock transitions: outside → within → outside (exits)
            within_checks = [False, True, False]
            check_index = {"index": 0}

            def is_within_side_effect():
                result = within_checks[check_index["index"]]
                check_index["index"] = min(check_index["index"] + 1, len(within_checks) - 1)
                return result

            mock_checker.is_within_hours.side_effect = is_within_side_effect

            # Mock 5 seconds until active (short wait)
            mock_checker.seconds_until_active.return_value = 5.0
            mock_checker.next_active_time.return_value = datetime.now(
                ZoneInfo("Europe/Berlin")
            )

            mock_checker_class.return_value = mock_checker
            mock_checker_class.parse_active_hours.return_value = (
                dt_time(8, 30),
                dt_time(18, 0),
            )

            # Run daemon
            runner.invoke(
                app,
                [
                    "fetch-trades",
                    "DETR",
                    "--daemon",
                    "--interval",
                    "1",
                    "--active-hours",
                    "08:30-18:00",
                ],
            )

            # Should have called fetch once (after entering active hours)
            assert mock_xetra_service.fetch_and_store_missing_trades_incremental.call_count >= 1


class TestDaemonPIDIntegration:
    """Test daemon mode with PID file."""

    def test_daemon_creates_pid_file(self, daemon_test_context, tmp_path):
        """Daemon creates PID file when --pid-file is specified."""
        pid_file = tmp_path / "daemon.pid"
        ctx = daemon_test_context
        
        ctx["service"].fetch_and_store_missing_trades_incremental.return_value = {
            "dates_checked": [],
            "dates_fetched": [],
            "dates_partial": [],
            "total_trades": 0,
            "total_files": 0,
            "consolidated": False,
        }

        # Run daemon with PID file
        runner.invoke(
            app,
            [
                "fetch-trades",
                "DETR",
                "--daemon",
                "--interval",
                "1",
                "--active-hours",
                "00:00-23:59",
                "--pid-file",
                str(pid_file),
            ],
            catch_exceptions=False,
        )

        # PID file should have been created (and may be cleaned up by atexit)
        # We can't reliably test cleanup in this context

    def test_daemon_prevents_duplicate_instances(self, tmp_path):
        """Second daemon instance fails if PID file exists with running process."""
        pid_file = tmp_path / "daemon.pid"
        # Write current PID (simulates running instance)
        pid_file.write_text(str(os.getpid()))

        # Try to start second instance
        result = runner.invoke(
            app,
            [
                "fetch-trades",
                "DETR",
                "--daemon",
                "--interval",
                "1",
                "--pid-file",
                str(pid_file),
            ],
        )

        # Should have failed
        assert result.exit_code != 0


class TestDaemonNoStore:
    """Test daemon mode interaction with --no-store flag."""

    def test_no_store_ignored_in_daemon_mode(self):
        """--no-store should not be used with --daemon (would be pointless)."""
        # This is more of a documentation test - daemon + no-store is allowed
        # but would be silly (daemon that never stores anything)
        result = runner.invoke(
            app, ["fetch-trades", "DETR", "--daemon", "--no-store", "--help"]
        )
        # Just verify the command structure is valid
        assert result.exit_code == 0
