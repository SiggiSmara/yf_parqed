import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from yf_parqed import yfinance_cli as main
from yf_parqed.primary_class import all_intervals


class StubConfig:
    """Minimal config stub for CLI tests."""
    def load_storage_config(self):
        return {"partitioned": True, "markets": {}, "sources": {}}
    
    def save_storage_config(self, config):
        pass


class StubYFParqed:
    """Minimal stub capturing CLI interactions."""

    def __init__(self):
        self.calls: list[tuple] = []
        self.new_not_found = False
        self.saved_intervals = None
        self.added_intervals: list[str] = []
        self.removed_intervals: list[str] = []
        self.partition_overrides: list[tuple] = []
        self.cleared_overrides: list[tuple] = []
        self.config = StubConfig()  # Add config stub

    def set_working_path(self, path: Path):
        self.calls.append(("set_working_path", Path(path)))
        self.work_path = Path(path)
        return self.work_path

    def set_limiter(self, max_requests: int, duration: int):
        self.calls.append(("set_limiter", max_requests, duration))

    def get_new_list_of_stocks(self):
        self.calls.append(("get_new_list_of_stocks",))
        return {}

    def save_intervals(self, intervals):
        self.calls.append(("save_intervals", tuple(intervals)))
        self.saved_intervals = list(intervals)

    def update_current_list_of_stocks(self):
        self.calls.append(("update_current_list_of_stocks",))

    def save_tickers(self):
        self.calls.append(("save_tickers",))

    def add_interval(self, interval: str):
        self.calls.append(("add_interval", interval))
        self.added_intervals.append(interval)

    def remove_interval(self, interval: str):
        self.calls.append(("remove_interval", interval))
        self.removed_intervals.append(interval)

    def update_stock_data(self, start_date=None, end_date=None):
        self.calls.append(("update_stock_data", start_date, end_date))

    def confirm_not_founds(self):
        self.calls.append(("confirm_not_founds",))

    def reparse_not_founds(self):
        self.calls.append(("reparse_not_founds",))

    def set_partition_override(self, *, enabled: bool, market=None, source=None):
        self.calls.append(("set_partition_override", enabled, market, source))
        self.partition_overrides.append((enabled, market, source))

    def clear_partition_override(self, *, market=None, source=None):
        self.calls.append(("clear_partition_override", market, source))
        self.cleared_overrides.append((market, source))


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def stub(monkeypatch):
    stub = StubYFParqed()
    monkeypatch.setattr(main, "yf_parqed", stub)
    return stub


def test_initialize_command_invokes_expected_workflow(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(main.app, ["--wrk-dir", tmp_dir, "initialize"])
        expected_path = Path(tmp_dir)

    assert result.exit_code == 0
    call_names = [name for name, *_ in stub.calls]
    assert ("set_working_path", expected_path) in stub.calls
    assert "get_new_list_of_stocks" in call_names
    assert "save_intervals" in call_names
    # initialize now defaults to ["1m"] instead of all intervals
    assert stub.saved_intervals == ["1m"]
    assert "update_current_list_of_stocks" in call_names
    assert "save_tickers" in call_names


def test_add_interval_respects_limits_option(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            ["--wrk-dir", tmp_dir, "--limits", "5", "10", "add-interval", "15m"],
        )
        expected_path = Path(tmp_dir)

    assert result.exit_code == 0
    assert ("set_working_path", expected_path) in stub.calls
    assert ("set_limiter", 5, 10) in stub.calls
    assert ("add_interval", "15m") in stub.calls


def test_remove_interval_command_invokes_stub(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            ["--wrk-dir", tmp_dir, "remove-interval", "30m"],
        )
        expected_path = Path(tmp_dir)

    assert result.exit_code == 0
    assert ("set_working_path", expected_path) in stub.calls
    assert ("remove_interval", "30m") in stub.calls


def test_update_data_basic_flow(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        stub.new_not_found = False
        result = runner.invoke(main.app, ["--wrk-dir", tmp_dir, "update-data"])

    assert result.exit_code == 0
    assert ("update_stock_data", None, None) in stub.calls
    assert "save_tickers" not in [name for name, *_ in stub.calls]


def test_update_data_non_interactive_saves_not_found(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        stub.new_not_found = True
        result = runner.invoke(
            main.app,
            [
                "--wrk-dir",
                tmp_dir,
                "update-data",
                "--non-interactive",
                "--save-not-founds",
            ],
        )

    assert result.exit_code == 0
    assert ("update_stock_data", None, None) in stub.calls
    assert ("save_tickers",) in stub.calls


def test_update_data_accepts_date_range(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            [
                "--wrk-dir",
                tmp_dir,
                "update-data",
                "--start-date",
                "2024-01-01T00:00:00",
                "--end-date",
                "2024-02-01T00:00:00",
            ],
        )

    assert result.exit_code == 0
    update_calls = [call for call in stub.calls if call[0] == "update_stock_data"]
    assert update_calls
    call_name, start_arg, end_arg = update_calls[-1]
    assert start_arg == datetime(2024, 1, 1)
    assert end_arg == datetime(2024, 2, 1)


def test_update_data_requires_both_dates(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            [
                "--wrk-dir",
                tmp_dir,
                "update-data",
                "--start-date",
                "2024-01-01T00:00:00",
            ],
        )

    assert result.exit_code == 0
    assert "Both start and end date must be provided" in result.stdout
    date_calls = [call for call in stub.calls if call[0] == "update_stock_data"]
    assert date_calls == []


def test_update_tickers_command_calls_update_list(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(main.app, ["--wrk-dir", tmp_dir, "update-tickers"])

    assert result.exit_code == 0
    assert ("update_current_list_of_stocks",) in stub.calls


def test_confirm_not_founds_command_calls_handler(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(main.app, ["--wrk-dir", tmp_dir, "confirm-not-founds"])

    assert result.exit_code == 0
    assert ("confirm_not_founds",) in stub.calls


def test_reparse_not_founds_calls_handler_twice(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(main.app, ["--wrk-dir", tmp_dir, "reparse-not-founds"])

    assert result.exit_code == 0
    reparse_calls = [call for call in stub.calls if call[0] == "reparse_not_founds"]
    assert len(reparse_calls) == 2


def test_global_options_apply_log_level_env(runner, stub, monkeypatch):
    monkeypatch.delenv("YF_PARQED_LOG_LEVEL", raising=False)

    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            ["--wrk-dir", tmp_dir, "--log-level", "DEBUG", "initialize"],
        )

    assert result.exit_code == 0
    assert os.environ["YF_PARQED_LOG_LEVEL"] == "DEBUG"
    assert ("set_working_path", Path(tmp_dir)) in stub.calls


def test_partition_toggle_enables_global_by_default(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(main.app, ["--wrk-dir", tmp_dir, "partition-toggle"])

    assert result.exit_code == 0
    assert ("set_partition_override", True, None, None) in stub.calls


def test_partition_toggle_disables_market(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            [
                "--wrk-dir",
                tmp_dir,
                "partition-toggle",
                "--market",
                "US",
                "--disable",
            ],
        )

    assert result.exit_code == 0
    assert ("set_partition_override", False, "US", None) in stub.calls


def test_partition_toggle_requires_market_when_source_provided(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            [
                "--wrk-dir",
                tmp_dir,
                "partition-toggle",
                "--source",
                "yahoo",
            ],
        )

    assert result.exit_code == 1
    assert "Provide --market" in result.stdout


def test_partition_toggle_clears_override(runner, stub):
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            main.app,
            [
                "--wrk-dir",
                tmp_dir,
                "partition-toggle",
                "--market",
                "US",
                "--source",
                "yahoo",
                "--clear",
            ],
        )

    assert result.exit_code == 0
    assert ("clear_partition_override", "US", "yahoo") in stub.calls
