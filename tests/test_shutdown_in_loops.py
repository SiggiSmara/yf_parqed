"""
Stop requests are honoured inside the work loops (ADR 2026-10-03, Step E).

A daemon that is told to stop must end its current cycle after the item it is
working on, not at the end of the cycle: a Xetra cycle can last an hour and a
Yahoo cycle four, but the stop timeout is 30 to 60 seconds.
"""

import signal
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from freezegun import freeze_time
from loguru import logger
from typer.testing import CliRunner

from yf_parqed import yfinance_cli
from yf_parqed.common.config_service import ConfigService
from yf_parqed.common.partition_path_builder import PartitionPathBuilder
from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.common.shutdown import (
    ShutdownRequested,
    StopFlag,
    sleep_unless_stopped,
)
from yf_parqed.xetra.xetra_fetcher import XetraFetcher
from yf_parqed.xetra.xetra_service import XetraService
from yf_parqed.xetra_cli import app as xetra_app
from yf_parqed.yahoo.interval_scheduler import IntervalScheduler
from yf_parqed.yahoo.ticker_registry import TickerRegistry


class TestSleepUnlessStopped:
    def test_without_a_check_it_is_one_plain_sleep(self):
        with patch("time.sleep") as sleep:
            assert sleep_unless_stopped(35, None) is True
        sleep.assert_called_once_with(35)

    def test_full_time_passes_when_no_stop_is_requested(self):
        with patch("time.sleep") as sleep:
            assert sleep_unless_stopped(3, lambda: False, step=1.0) is True
        assert [c.args[0] for c in sleep.call_args_list] == [1.0, 1.0, 1.0]

    def test_a_stop_request_ends_the_wait_within_one_step(self):
        stop = {"flag": False}

        def sleep(_seconds):
            stop["flag"] = True

        with patch("time.sleep", side_effect=sleep) as slept:
            assert sleep_unless_stopped(35, lambda: stop["flag"]) is False
        assert slept.call_count == 1


class TestStopFlag:
    def test_handler_sets_the_flag_without_logging(self):
        flag = StopFlag()
        assert flag() is False
        with patch("yf_parqed.common.shutdown.logger") as log:
            flag.handle(signal.SIGTERM, None)
        assert flag() is True
        assert flag.signum == signal.SIGTERM
        log.info.assert_not_called()

    def test_log_request_names_the_signal_and_is_silent_without_one(self):
        flag = StopFlag()
        with patch("yf_parqed.common.shutdown.logger") as log:
            flag.log_request()
            log.info.assert_not_called()
            flag.handle(signal.SIGINT, None)
            flag.log_request()
        assert "signal 2" in log.info.call_args.args[0]

    def test_install_registers_sigterm_and_sigint(self, monkeypatch):
        registered = {}
        monkeypatch.setattr(
            signal, "signal", lambda signum, h: registered.__setitem__(signum, h)
        )
        flag = StopFlag()
        flag.install()
        assert registered == {signal.SIGTERM: flag.handle, signal.SIGINT: flag.handle}

    def test_sleep_waits_in_ten_second_steps_and_stops_early(self):
        flag = StopFlag()
        with patch("time.sleep", side_effect=lambda _s: flag.handle(15, None)) as slept:
            assert flag.sleep(3600) is False
        slept.assert_called_once_with(10)

    def test_sleep_runs_the_remainder_after_whole_steps(self):
        with patch("time.sleep") as slept:
            assert StopFlag().sleep(25) is True
        assert [c.args[0] for c in slept.call_args_list] == [10, 10, 5]


class TestFetcherCooldown:
    def test_burst_cooldown_is_cut_short_by_a_stop_request(self):
        fetcher = XetraFetcher(burst_size=2, burst_cooldown=35)
        fetcher.request_count = 2  # the next call starts the cooldown
        stop = {"flag": False}
        fetcher.should_stop = lambda: stop["flag"]

        slept = []

        def sleep(seconds):
            slept.append(seconds)
            stop["flag"] = True

        with patch("time.sleep", side_effect=sleep):
            with pytest.raises(ShutdownRequested):
                fetcher.enforce_limits()

        assert sum(slept) < 35
        fetcher.close()

    def test_cooldown_runs_in_full_without_a_stop_request(self):
        fetcher = XetraFetcher(burst_size=2, burst_cooldown=5)
        fetcher.request_count = 2
        fetcher.should_stop = lambda: False

        with patch("time.sleep") as sleep:
            fetcher.enforce_limits()

        assert sum(c.args[0] for c in sleep.call_args_list) == pytest.approx(5)
        fetcher.close()


@pytest.fixture
def stop():
    return {"flag": False}


@pytest.fixture
def temp_root(tmp_path):
    return tmp_path / "data"


@pytest.fixture
def service(temp_root, stop):
    backend = PartitionedStorageBackend(
        empty_frame_factory=lambda: pd.DataFrame(),
        normalizer=lambda df: df,
        column_provider=lambda: [],
        path_builder=PartitionPathBuilder(root=temp_root),
    )
    return XetraService(
        fetcher=Mock(),
        parser=Mock(),
        backend=backend,
        root_path=temp_root,
        should_stop=lambda: stop["flag"],
    )


@pytest.fixture
def trades():
    return pd.DataFrame(
        {
            "isin": ["DE0005140008", "DE0008469008"],
            "price": [10.0, 20.0],
            "quantity": [1, 2],
            "trading_date_time": pd.to_datetime(["2025-11-04 09:00:00"] * 2),
        }
    )


def _files(date: str, count: int) -> list[str]:
    return [f"DETR-posttrade-{date}T09_{i:02d}.json.gz" for i in range(count)]


class TestXetraFetchLoops:
    def test_stop_during_a_date_ends_the_cycle_after_the_current_file(
        self, service, stop, trades
    ):
        fetched = []

        def fetch(venue, date, filename):
            fetched.append(filename)
            stop["flag"] = True  # the stop arrives while this file is processed
            return trades.copy()

        with (
            patch.object(service, "get_missing_dates", return_value=["2025-11-04"]),
            patch.object(service, "list_files", return_value=_files("2025-11-04", 5)),
            patch.object(service, "fetch_and_parse_trades", side_effect=fetch),
        ):
            summary = service.fetch_and_store_missing_trades_incremental("DETR")

        assert len(fetched) == 1
        assert summary["total_files"] == 1  # the file in hand was stored
        assert summary["dates_fetched"] == []
        assert summary["dates_partial"] == ["2025-11-04"]
        assert summary["consolidated"] is False

    def test_stop_between_dates_leaves_later_dates_untouched(
        self, service, stop, trades
    ):
        listed = []

        def list_files(venue, date):
            listed.append(date)
            return _files(date, 1)

        def fetch(venue, date, filename):
            stop["flag"] = True
            return trades.copy()

        with (
            patch.object(
                service,
                "get_missing_dates",
                return_value=["2025-11-04", "2025-11-05", "2025-11-06"],
            ),
            patch.object(service, "list_files", side_effect=list_files),
            patch.object(service, "fetch_and_parse_trades", side_effect=fetch),
        ):
            summary = service.fetch_and_store_missing_trades_incremental("DETR")

        assert listed == ["2025-11-04"]
        assert summary["dates_fetched"] == ["2025-11-04"]  # that date was complete

    def test_stop_skips_the_month_end_consolidation(self, service, stop, trades):
        """A stopped cycle does not start a minute-long consolidation on its way out."""
        stop["flag"] = True
        with (
            patch.object(service, "get_missing_dates", return_value=["2025-09-30"]),
            patch.object(service, "_consolidate_to_monthly") as consolidate,
        ):
            service.fetch_and_store_missing_trades_incremental("DETR")
        consolidate.assert_not_called()

    def test_stop_during_the_fetcher_cooldown_ends_the_loop_quietly(
        self, service, stop, trades
    ):
        """ShutdownRequested from the fetcher is a stop, not a failed file."""
        calls = []

        def fetch(venue, date, filename):
            calls.append(filename)
            stop["flag"] = True
            raise ShutdownRequested("stop requested during burst cooldown")

        messages = []
        sink = logger.add(lambda m: messages.append(m.record["message"]), level="ERROR")
        try:
            with (
                patch.object(service, "get_missing_dates", return_value=["2025-11-04"]),
                patch.object(
                    service, "list_files", return_value=_files("2025-11-04", 5)
                ),
                patch.object(service, "fetch_and_parse_trades", side_effect=fetch),
            ):
                summary = service.fetch_and_store_missing_trades_incremental("DETR")
        finally:
            logger.remove(sink)

        assert len(calls) == 1
        assert summary["total_files"] == 0
        assert not [m for m in messages if "Failed to" in m]

    def test_without_a_stop_check_nothing_changes(self, temp_root, trades):
        backend = PartitionedStorageBackend(
            empty_frame_factory=lambda: pd.DataFrame(),
            normalizer=lambda df: df,
            column_provider=lambda: [],
            path_builder=PartitionPathBuilder(root=temp_root),
        )
        plain = XetraService(
            fetcher=Mock(), parser=Mock(), backend=backend, root_path=temp_root
        )
        with (
            patch.object(plain, "get_missing_dates", return_value=["2025-11-04"]),
            patch.object(plain, "list_files", return_value=_files("2025-11-04", 3)),
            patch.object(plain, "fetch_and_parse_trades", return_value=trades.copy()),
        ):
            summary = plain.fetch_and_store_missing_trades_incremental(
                "DETR", consolidate=False
            )
        assert summary["total_files"] == 3
        assert summary["dates_fetched"] == ["2025-11-04"]


class TestXetraConsolidationStop:
    def _store_days(self, service, trades, days):
        for day in days:
            service.store_trades(trades, "DETR", datetime(2025, 11, day))

    def _monthly_dir(self, temp_root: Path) -> Path:
        return temp_root / "de/xetra/trades_monthly/venue=DETR/year=2025/month=11"

    def test_stop_between_days_abandons_the_monthly_file_and_cleans_up(
        self, service, stop, trades, temp_root
    ):
        self._store_days(service, trades, [3, 4, 5])
        original_conform = service._conform_table

        def conform(table, schema):
            stop["flag"] = True  # arrives while the first day is being written
            return original_conform(table, schema)

        with patch.object(service, "_conform_table", side_effect=conform):
            service._consolidate_to_monthly("DETR", 2025, 11)

        monthly = self._monthly_dir(temp_root)
        assert not (monthly / "trades.parquet").exists()
        assert not list(monthly.glob("*.tmp"))

    def test_the_abandoned_month_is_built_by_the_next_call(
        self, service, stop, trades, temp_root
    ):
        self._store_days(service, trades, [3, 4])
        stop["flag"] = True
        service._consolidate_to_monthly("DETR", 2025, 11)
        assert not (self._monthly_dir(temp_root) / "trades.parquet").exists()

        stop["flag"] = False
        service._consolidate_to_monthly("DETR", 2025, 11)
        assert (self._monthly_dir(temp_root) / "trades.parquet").exists()


class TestYahooScheduler:
    def _scheduler(self, tmp_path, processed, tickers=("AAA", "BBB", "CCC")):
        registry = TickerRegistry(
            ConfigService(tmp_path),
            initial_tickers={t: {"status": "active", "intervals": {}} for t in tickers},
        )
        return IntervalScheduler(
            registry=registry,
            intervals=lambda: ["1d", "1h"],
            loader=lambda: None,
            limiter=lambda: None,
            processor=lambda stock, start_date, end_date, interval: processed.append(
                (stock, interval)
            ),
            today_provider=lambda: datetime(2025, 1, 20),
            progress_factory=lambda stocks, description, disable: list(stocks),
        )

    def test_stop_request_ends_the_cycle_after_the_current_ticker(self, tmp_path):
        processed = []
        scheduler = self._scheduler(tmp_path, processed)

        scheduler.run(should_stop=lambda: len(processed) >= 1)

        assert processed == [("AAA", "1d")]

    def test_stop_request_skips_the_remaining_intervals(self, tmp_path):
        processed = []
        scheduler = self._scheduler(tmp_path, processed, tickers=("AAA",))

        scheduler.run(should_stop=lambda: len(processed) >= 1)

        assert processed == [("AAA", "1d")]

    def test_without_a_stop_check_every_ticker_runs(self, tmp_path):
        processed = []
        self._scheduler(tmp_path, processed).run()
        assert len(processed) == 6


@pytest.fixture
def handlers(monkeypatch):
    """Record the signal handlers a daemon registers instead of installing them."""
    registered = {}
    monkeypatch.setattr(
        signal,
        "signal",
        lambda signum, handler: registered.__setitem__(signum, handler),
    )
    return registered


def _logged(*logs: MagicMock) -> list[str]:
    return [str(c.args[0]) for log in logs for c in log.info.call_args_list]


class TestSignalHandlers:
    """The handlers only set a flag; the main loop does the logging (loguru is not re-entrant)."""

    @freeze_time("2025-12-04 14:00:00-05:00")
    def test_yahoo_handler_does_not_log_and_loop_reports_the_signal(
        self, handlers, tmp_path, monkeypatch
    ):
        seen = {}

        class Stub:
            new_not_found = False
            my_path = tmp_path
            work_path = tmp_path
            config = MagicMock()

            def set_working_path(self, path):
                return path

            def update_stock_data(
                self, start_date=None, end_date=None, should_stop=None
            ):
                before = len(_logged(log, cli_log))
                handlers[signal.SIGTERM](signal.SIGTERM, None)
                seen["logged_in_handler"] = len(_logged(log, cli_log)) - before
                seen["stop_seen"] = should_stop()

        monkeypatch.setattr(yfinance_cli, "yf_parqed", Stub())
        with (
            patch("yf_parqed.common.shutdown.logger") as log,
            patch("yf_parqed.yfinance_cli.logger") as cli_log,
            patch("yf_parqed.yfinance_cli.GlobalRunLock") as lock,
        ):
            lock.return_value.try_acquire.return_value = True
            result = CliRunner(env={"NO_COLOR": "1"}).invoke(
                yfinance_cli.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--daemon"],
                catch_exceptions=False,
            )

        messages = _logged(log, cli_log)
        assert result.exit_code == 0
        assert seen == {"logged_in_handler": 0, "stop_seen": True}
        assert any("Received signal 15" in m for m in messages)
        assert any("Stop requested during the update cycle" in m for m in messages)
        assert not any("All tickers were processed" in m for m in messages)

    def test_xetra_handler_does_not_log_and_service_gets_the_stop_check(self, handlers):
        service = MagicMock()
        service.__enter__.return_value = service
        service.has_any_data.return_value = True
        service.find_unmigrated_files.return_value = []
        seen = {}

        def cycle(venue, market, source):
            before = len(_logged(log, cli_log))
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            seen["logged_in_handler"] = len(_logged(log, cli_log)) - before
            seen["stop_seen"] = factory.call_args.kwargs["should_stop"]()
            return {"dates_fetched": [], "total_trades": 0}

        service.fetch_and_store_missing_trades_incremental.side_effect = cycle

        with (
            patch("yf_parqed.common.shutdown.logger") as log,
            patch("yf_parqed.xetra_cli.logger") as cli_log,
            patch("yf_parqed.xetra_cli.XetraService", return_value=service) as factory,
        ):
            result = CliRunner().invoke(
                xetra_app, ["fetch-trades", "DETR", "--daemon"], catch_exceptions=False
            )

        messages = _logged(log, cli_log)
        assert result.exit_code == 0
        assert seen == {"logged_in_handler": 0, "stop_seen": True}
        assert any("Received signal 15" in m for m in messages)
        assert any("Daemon shutting down gracefully" in m for m in messages)
