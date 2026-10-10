"""
Step D of ADR 2026-10-03 (Decision 6): the Yahoo daemon saves its ticker
registry and fetches every ticker once per collection night.

Yahoo serves 1-minute bars for 7 days only, so the tests here guard the rules
that keep a ticker from going unfetched: a failed request is retried the same
night, an empty answer never pauses a ticker, a save never loses another
writer's change, and the saved file lets the previous release fetch everything.
"""

from __future__ import annotations

import copy
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from typer.testing import CliRunner
from yfinance.exceptions import (
    YFPricesMissingError,
    YFRateLimitError,
    YFTzMissingError,
)

from yf_parqed import yfinance_cli
from yf_parqed.common import config_service
from yf_parqed.common.config_service import ConfigService
from yf_parqed.yahoo.data_fetcher import DataFetcher, FetchFailed, is_no_data_answer
from yf_parqed.yahoo.interval_scheduler import (
    MAX_CONSECUTIVE_FAILURES,
    MAX_FAILURES_WITHOUT_SUCCESS,
    CycleResult,
    IntervalScheduler,
)
from yf_parqed.yahoo.primary_class import NIGHT_START_HOUR_UTC, YFParqed
from yf_parqed.yahoo.ticker_registry import TickerRegistry, merge_entry

UTC = timezone.utc
NIGHT = datetime(2026, 10, 19, 22, 5, tzinfo=UTC)  # a Monday, just after the start


def entry(ticker: str, **interval_meta) -> dict:
    return {
        "ticker": ticker,
        "added_date": "2026-01-01",
        "status": "active",
        "last_checked": None,
        "intervals": {"1m": dict(interval_meta)} if interval_meta else {},
    }


def write_registry(path: Path, *tickers: str, **entries: dict) -> None:
    registry = {ticker: entry(ticker) for ticker in tickers}
    registry.update(entries)
    (path / "tickers.json").write_text(json.dumps(registry))


def stored(path: Path) -> dict:
    return json.loads((path / "tickers.json").read_text())


def bars(stock: str, day: str = "2026-10-19", count: int = 3) -> pd.DataFrame:
    dates = pd.date_range(f"{day} 09:30", periods=count, freq="min")
    frame = pd.DataFrame(
        {
            "stock": stock,
            "date": dates,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
        }
    )
    return frame.set_index(["stock", "date"])


class FakeFetcher:
    """Stands in for DataFetcher: answers per ticker with bars, nothing, or an error."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.get_all: list[bool] = []
        self.empty: set[str] = set()
        self.failing: dict[str, BaseException] = {}
        self.before_fetch = None
        self.reference_calls: list[str] = []
        self.yahoo_down = False

    def fetch(self, stock, start_date, end_date, interval, get_all=False):
        if self.before_fetch is not None:
            self.before_fetch(stock)
        self.calls.append(stock)
        self.get_all.append(get_all)
        if stock in self.failing:
            raise self.failing[stock]
        if stock in self.empty:
            return YFParqed._empty_price_frame()
        return bars(stock)

    def asked_since(self, mark: int) -> list[str]:
        return self.calls[mark:]

    def has_window(self, start_date, end_date, interval) -> bool:
        return True

    def has_recent_bars(self, stock, interval) -> bool:
        """The health check: does Yahoo return bars for a ticker known to have them?"""
        self.reference_calls.append(stock)
        if self.yahoo_down:
            raise ConnectionError("Yahoo is not answering")
        return stock not in self.empty and stock not in self.failing


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture()
def clock() -> Clock:
    return Clock(NIGHT)


def make(path: Path, clock: Clock, monkeypatch, nightly: bool = True):
    instance = YFParqed(my_path=path, my_intervals=["1m"])
    fetcher = FakeFetcher()
    instance.data_fetcher = fetcher
    monkeypatch.setattr(instance, "enforce_limits", lambda: None)
    monkeypatch.setattr(instance, "utc_now", clock)
    if nightly:
        instance.use_nightly_schedule()
    return instance, fetcher


# ── the collection night ─────────────────────────────────────────────────────


class TestNightBoundary:
    def test_the_night_starts_at_22_utc(self, tmp_path, clock, monkeypatch):
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch)

        assert NIGHT_START_HOUR_UTC == 22
        assert instance.night_start(datetime(2026, 10, 19, 22, 0, tzinfo=UTC)) == (
            datetime(2026, 10, 19, 22, 0, tzinfo=UTC)
        )
        assert instance.night_start(datetime(2026, 10, 19, 21, 59, 59, tzinfo=UTC)) == (
            datetime(2026, 10, 18, 22, 0, tzinfo=UTC)
        )
        assert instance.night_start(datetime(2026, 10, 20, 3, 0, tzinfo=UTC)) == (
            datetime(2026, 10, 19, 22, 0, tzinfo=UTC)
        )

    def test_seconds_until_the_next_night(self, tmp_path, clock, monkeypatch):
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch)

        at = datetime(2026, 10, 19, 21, 0, tzinfo=UTC)
        assert instance.seconds_until_next_night(at) == 3600
        at = datetime(2026, 10, 19, 22, 0, tzinfo=UTC)
        assert instance.seconds_until_next_night(at) == 24 * 3600

    def test_a_second_cycle_in_the_same_night_fetches_nothing(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA", "BBB", "CCC")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        first = instance.update_stock_data()
        instance.save_ticker_changes()
        clock.now = NIGHT + timedelta(hours=5)
        second = instance.update_stock_data()

        assert fetcher.calls == ["AAA", "BBB", "CCC"]
        assert (first.processed, first.skipped) == (3, 0)
        assert (second.processed, second.skipped) == (0, 3)

    def test_the_first_cycle_after_22_utc_fetches_every_ticker_again(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA", "BBB")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        clock.now = datetime(2026, 10, 19, 21, 0, tzinfo=UTC)  # before the night
        instance.update_stock_data()
        instance.save_ticker_changes()
        clock.now = datetime(2026, 10, 19, 22, 0, 1, tzinfo=UTC)
        instance.update_stock_data()
        instance.save_ticker_changes()
        clock.now = datetime(2026, 10, 20, 21, 59, tzinfo=UTC)  # still that night
        instance.update_stock_data()
        instance.save_ticker_changes()
        clock.now = datetime(2026, 10, 20, 22, 0, tzinfo=UTC)
        instance.update_stock_data()

        assert fetcher.calls == ["AAA", "BBB"] * 3

    def test_weekend_nights_run_like_any_other(self, tmp_path, clock, monkeypatch):
        write_registry(tmp_path, "AAA")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        for day in (24, 25):  # Saturday and Sunday
            clock.now = datetime(2026, 10, day, 22, 30, tzinfo=UTC)
            instance.update_stock_data()
            instance.save_ticker_changes()

        assert fetcher.calls == ["AAA", "AAA"]

    def test_the_nightly_fetch_is_always_the_full_period(
        self, tmp_path, clock, monkeypatch
    ):
        """Neither bar date makes the fetch a window: every bar is asked for each night."""
        write_registry(
            tmp_path,
            NEW=entry("NEW", status="active", newest_bar_date="2026-10-16"),
            OLD=entry("OLD", status="active", last_data_date="2026-10-16"),
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        instance.update_stock_data()

        assert fetcher.calls == ["NEW", "OLD"]
        assert fetcher.get_all == [True, True]

    def test_without_the_nightly_schedule_every_run_fetches(
        self, tmp_path, clock, monkeypatch
    ):
        """A single run from the command line is not held back by the night."""
        write_registry(tmp_path, "AAA")
        instance, fetcher = make(tmp_path, clock, monkeypatch, nightly=False)

        first = instance.update_stock_data()
        instance.save_tickers()
        instance.update_stock_data()

        assert first.skipped == 0
        assert instance.scheduler.is_due is None
        assert instance.scheduler.checkpoint is None


# ── answers, empty answers and failures ──────────────────────────────────────


class TestAnswersAndFailures:
    def test_a_failed_ticker_is_fetched_by_the_next_cycle_and_its_state_is_unchanged(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(
            tmp_path,
            "AAA",
            "CCC",
            BBB=entry("BBB", status="active", newest_bar_date="2026-10-16"),
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        before = copy.deepcopy(stored(tmp_path)["BBB"])
        fetcher.failing["BBB"] = YFRateLimitError()

        first = instance.update_stock_data()
        instance.save_ticker_changes()

        assert first.failed == ["BBB"]
        assert first.processed == 2
        assert first.ran_to_end
        assert stored(tmp_path)["BBB"] == before
        assert "last_fetch_at" in stored(tmp_path)["CCC"]["intervals"]["1m"]

        # two hours later, same night: only the failed ticker is asked
        del fetcher.failing["BBB"]
        mark = len(fetcher.calls)
        clock.now = NIGHT + timedelta(hours=2)
        second = instance.update_stock_data()

        assert fetcher.asked_since(mark) == ["BBB"]
        assert (second.processed, second.skipped, second.failed) == (1, 2, [])

    def test_a_storage_failure_is_a_failure_too(self, tmp_path, clock, monkeypatch):
        write_registry(tmp_path, "AAA", "BBB")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        def merge(frame, request):
            if request.ticker == "AAA":
                raise OSError("disk says no")

        monkeypatch.setattr(instance, "merge_yf", merge)
        result = instance.update_stock_data()

        assert result.failed == ["AAA"]
        assert result.processed == 1
        assert instance.tickers["AAA"]["intervals"] == {}
        assert instance.is_due_tonight("AAA", "1m")
        assert not instance.is_due_tonight("BBB", "1m")

    def test_an_empty_answer_ends_the_night_and_never_pauses_the_ticker(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA", "QUIET")
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.add("QUIET")

        instance.update_stock_data()
        instance.save_ticker_changes()
        clock.now = NIGHT + timedelta(hours=2)
        instance.update_stock_data()
        assert fetcher.calls == ["AAA", "QUIET"]  # not asked twice in one night

        # a month of nights without data: asked every night, never marked dead
        for night in range(1, 31):
            clock.now = NIGHT + timedelta(days=night)
            instance.update_stock_data()
            instance.save_ticker_changes()

        assert fetcher.calls.count("QUIET") == 31
        meta = stored(tmp_path)["QUIET"]["intervals"]["1m"]
        assert meta["status"] == "not_found"
        assert "permanently_dead" not in meta
        assert "cooling_since" not in meta
        assert "not_found_streak_days" not in meta

    def test_a_ticker_marked_dead_by_hand_is_still_left_alone(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(
            tmp_path,
            "AAA",
            GONE=entry("GONE", status="not_found", permanently_dead=True),
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        instance.update_stock_data()

        assert fetcher.calls == ["AAA"]

    def test_twenty_failures_in_a_row_end_the_cycle(self, tmp_path, clock, monkeypatch):
        names = [f"T{i:02d}" for i in range(30)]
        write_registry(tmp_path, *names)
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        for name in names:
            fetcher.failing[name] = ConnectionError("no network")

        result = instance.update_stock_data()

        assert MAX_CONSECUTIVE_FAILURES == 20
        assert result.aborted and not result.ran_to_end
        assert result.failed == names[:20]
        assert fetcher.calls == names[:20]

    def test_failures_that_are_not_in_a_row_do_not_end_the_cycle(
        self, tmp_path, clock, monkeypatch
    ):
        names = [f"T{i:02d}" for i in range(60)]
        write_registry(tmp_path, *names)
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        for name in names[::2]:  # every other ticker fails: 30 failures
            fetcher.failing[name] = ConnectionError("flaky")

        result = instance.update_stock_data()

        assert result.ran_to_end
        assert len(result.failed) == 30
        assert result.processed == 30


def test_scheduler_goes_on_after_a_ticker_that_raises(tmp_path):
    registry = TickerRegistry(
        ConfigService(tmp_path),
        initial_tickers={t: {"status": "active", "intervals": {}} for t in "ABC"},
    )
    done: list[str] = []

    def processor(stock, start_date, end_date, interval):
        if stock == "B":
            raise RuntimeError("bad file")
        done.append(stock)

    scheduler = IntervalScheduler(
        registry=registry,
        intervals=lambda: ["1m"],
        loader=lambda: None,
        limiter=None,
        processor=processor,
        today_provider=lambda: datetime(2026, 10, 19),
        progress_factory=lambda stocks, description, disable: stocks,
    )

    result = scheduler.run()

    assert done == ["A", "C"]
    assert result == CycleResult(processed=2, failed=["B"])


# ── saving the registry ──────────────────────────────────────────────────────


class TestRegistrySave:
    def test_a_kill_in_mid_cycle_keeps_the_last_periodic_save(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA", "BBB", "CCC", "DDD", "EEE")
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        instance.scheduler._checkpoint_every = 2
        fetcher.failing["DDD"] = KeyboardInterrupt()  # the process dies here

        with pytest.raises(KeyboardInterrupt):
            instance.update_stock_data()

        on_disk = stored(tmp_path)
        saved = [t for t, e in on_disk.items() if e["intervals"].get("1m")]
        assert saved == ["AAA", "BBB"]  # CCC was fetched after the last save

        # the next start, same night: only the tickers not saved are fetched
        restarted, again = make(tmp_path, clock, monkeypatch)
        result = restarted.update_stock_data()

        assert again.calls == ["CCC", "DDD", "EEE"]
        assert result.skipped == 2

    def test_add_ticker_during_a_cycle_survives_the_daemons_save(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA", "BBB", "CCC")
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        instance.scheduler._checkpoint_every = 1

        def another_process(stock):
            if stock == "BBB":
                other = TickerRegistry(ConfigService(tmp_path))
                other.add_ticker("NEWCO")
                other.remove_ticker("CCC")

        fetcher.before_fetch = another_process
        instance.update_stock_data()
        instance.save_ticker_changes()

        on_disk = stored(tmp_path)
        assert on_disk["NEWCO"]["source"] == "manual"
        assert on_disk["CCC"]["manually_removed"] is True
        assert "last_fetch_at" in on_disk["AAA"]["intervals"]["1m"]
        assert "last_fetch_at" in on_disk["BBB"]["intervals"]["1m"]
        # the removal took effect in the running cycle, at its next save
        assert fetcher.calls == ["AAA", "BBB"]
        # and the next cycle knows the new ticker
        clock.now = NIGHT + timedelta(days=1)
        instance.update_stock_data()
        assert "NEWCO" in fetcher.calls

    def test_the_daemons_save_does_not_undo_a_later_add_ticker(
        self, tmp_path, clock, monkeypatch
    ):
        """The daemon read the file before the command ran; its save must not write that old state back."""
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch)
        instance.load_tickers()

        TickerRegistry(ConfigService(tmp_path)).add_ticker("NEWCO")
        instance.update_ticker_interval_status(
            "AAA", "1m", True, datetime(2026, 10, 19)
        )
        assert instance.save_ticker_changes() is True

        assert set(stored(tmp_path)) == {"AAA", "NEWCO"}
        assert set(instance.tickers) == {"AAA", "NEWCO"}

    def test_nothing_changed_means_nothing_is_written(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch)
        instance.load_tickers()
        before = (tmp_path / "tickers.json").stat().st_mtime_ns

        assert instance.save_ticker_changes() is False
        assert (tmp_path / "tickers.json").stat().st_mtime_ns == before

    def test_the_saved_file_contains_no_last_data_date(
        self, tmp_path, clock, monkeypatch
    ):
        """
        The previous release fetches everything only while it finds no
        last_data_date. A revert within Yahoo's 7 days depends on that.
        """
        write_registry(
            tmp_path,
            "AAA",
            OLD=entry("OLD", status="active", last_data_date="2026-10-16"),
        )
        instance, _ = make(tmp_path, clock, monkeypatch)

        instance.update_stock_data()
        instance.save_ticker_changes()

        text = (tmp_path / "tickers.json").read_text()
        assert "last_data_date" not in text
        meta = stored(tmp_path)["OLD"]["intervals"]["1m"]
        assert meta["newest_bar_date"] == "2026-10-19"
        assert meta["last_fetch_at"] == "2026-10-19T22:05:00+00:00"

    def test_an_unreadable_registry_is_never_written_over(
        self, tmp_path, clock, monkeypatch
    ):
        (tmp_path / "tickers.json").write_text('{"AAA": {"ticker": "AAA", ')  # cut off
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        result = instance.update_stock_data()  # loads as empty: nothing to fetch
        assert result.processed == 0 and fetcher.calls == []
        assert instance.save_ticker_changes() is False
        instance.save_tickers()  # the whole-file save refuses an empty registry

        # even with something to save, the file that cannot be read is kept
        instance.update_ticker_interval_status(
            "BBB", "1m", True, datetime(2026, 10, 19)
        )
        assert instance.save_ticker_changes() is False

        assert (tmp_path / "tickers.json").read_text() == '{"AAA": {"ticker": "AAA", '

    def test_an_empty_registry_can_start_a_new_working_directory(self, tmp_path):
        config = ConfigService(tmp_path)

        assert config.save_tickers({}) is True
        assert config.save_tickers({"AAA": entry("AAA")}) is True
        assert config.save_tickers({}) is False
        assert list(stored(tmp_path)) == ["AAA"]
        assert not (tmp_path / "tickers.tmp").exists()

    def test_a_failed_save_keeps_the_changes_for_the_next_one(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA", "BBB")
        instance, _ = make(tmp_path, clock, monkeypatch)
        instance.update_stock_data()

        with patch.object(
            ConfigService, "save_tickers", side_effect=OSError("disk full")
        ):
            assert instance.save_ticker_changes() is False
        assert stored(tmp_path)["AAA"]["intervals"] == {}

        assert instance.save_ticker_changes() is True
        assert "last_fetch_at" in stored(tmp_path)["AAA"]["intervals"]["1m"]


class TestRegistryLock:
    def test_a_second_writer_waits_for_the_lock(self, tmp_path):
        config = ConfigService(tmp_path)

        with config.tickers_lock():
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                with ConfigService(tmp_path).tickers_lock(timeout=0.2):
                    pass
            assert time.monotonic() - started >= 0.2

        with config.tickers_lock(timeout=0.2):  # free again
            pass

    def test_a_lock_file_owned_by_someone_else_can_still_be_locked(self, tmp_path):
        """A tool run with sudo may create the lock file; the daemon only needs to read it."""
        config = ConfigService(tmp_path)
        config.tickers_lock_path.write_text("")
        config.tickers_lock_path.chmod(0o444)

        with config.tickers_lock(timeout=0.2):
            pass

    def test_a_save_that_cannot_get_the_lock_is_reported_not_raised(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch)
        instance.update_stock_data()

        def busy(self, timeout=30.0):
            raise TimeoutError("held by another process")

        with patch.object(ConfigService, "tickers_lock", busy):
            assert instance.save_ticker_changes() is False
        assert instance.save_ticker_changes() is True


class TestFetchedSince:
    def test_compares_the_saved_stamp_with_the_moment(self, tmp_path):
        registry = TickerRegistry(ConfigService(tmp_path))
        registry.update_ticker_interval_status(
            "AAA", "1m", False, fetched_at=datetime(2026, 10, 19, 22, 5, tzinfo=UTC)
        )
        night = datetime(2026, 10, 19, 22, 0, tzinfo=UTC)

        assert registry.fetched_since("AAA", "1m", night) is True
        assert registry.fetched_since("AAA", "1m", night + timedelta(days=1)) is False
        assert registry.fetched_since("AAA", "1h", night) is False
        assert registry.fetched_since("NOPE", "1m", night) is False

    @pytest.mark.parametrize("stamp", ["not a time", "", None, 20261019])
    def test_a_stamp_that_cannot_be_read_means_not_fetched(self, tmp_path, stamp):
        registry = TickerRegistry(
            ConfigService(tmp_path),
            initial_tickers={"AAA": entry("AAA", last_fetch_at=stamp)},
        )

        assert registry.fetched_since("AAA", "1m", NIGHT) is False

    def test_a_stamp_without_a_zone_is_read_as_utc(self, tmp_path):
        registry = TickerRegistry(
            ConfigService(tmp_path),
            initial_tickers={"AAA": entry("AAA", last_fetch_at="2026-10-19T22:30:00")},
        )

        assert registry.fetched_since("AAA", "1m", NIGHT) is True


# ── the daemon loop ──────────────────────────────────────────────────────────


class DaemonStub:
    new_not_found = False
    config = MagicMock()
    night_start_hour_utc = 22

    def __init__(self, path: Path, cycle: CycleResult | None = None) -> None:
        self.my_path = path
        self.cycle = cycle
        self.calls: list[str] = []
        self.until_night = 10**9

    def set_working_path(self, path):
        return path

    def use_nightly_schedule(self):
        self.calls.append("nightly")

    def seconds_until_next_night(self):
        return self.until_night

    def update_stock_data(self, start_date=None, end_date=None, should_stop=None):
        self.calls.append("cycle")
        return self.cycle

    def save_ticker_changes(self):
        self.calls.append("save")
        return True

    def save_tickers(self):
        self.calls.append("save_whole")

    def check_last_closed_month(self, should_stop=None):
        self.calls.append("check")

    def update_current_list_of_stocks(self):
        self.calls.append("maintenance")

    def confirm_not_founds(self):
        pass

    def reparse_not_founds(self):
        pass


def run_daemon(stub, tmp_path, monkeypatch, *extra: str):
    monkeypatch.setattr(yfinance_cli, "yf_parqed", stub)
    with patch("yf_parqed.yfinance_cli.GlobalRunLock") as lock:
        lock.return_value.try_acquire.return_value = True
        return CliRunner(env={"NO_COLOR": "1"}).invoke(
            yfinance_cli.app,
            ["--wrk-dir", str(tmp_path), "update-data", "--daemon", *extra],
            catch_exceptions=False,
        )


class TestDaemonLoop:
    def test_the_daemon_collects_nightly_and_saves_after_every_cycle(
        self, tmp_path, monkeypatch, stop_on_first_sleep
    ):
        """No --save-not-founds and nothing "not found": the registry is saved all the same."""
        stub = DaemonStub(tmp_path, CycleResult(processed=3))

        result = run_daemon(
            stub, tmp_path, monkeypatch, "--ticker-maintenance", "never"
        )

        assert result.exit_code == 0
        assert stub.calls == ["nightly", "cycle", "save", "check"]

    def test_a_cycle_with_failed_tickers_still_counts_as_finished(
        self, tmp_path, monkeypatch, stop_on_first_sleep
    ):
        stub = DaemonStub(tmp_path, CycleResult(processed=2, failed=["BBB"]))

        run_daemon(stub, tmp_path, monkeypatch, "--ticker-maintenance", "never")

        assert stub.calls == ["nightly", "cycle", "save", "check"]

    def test_a_cycle_ended_by_failures_is_saved_but_not_followed_by_the_check(
        self, tmp_path, monkeypatch, stop_on_first_sleep
    ):
        stub = DaemonStub(
            tmp_path, CycleResult(failed=[f"T{i}" for i in range(20)], aborted=True)
        )

        run_daemon(stub, tmp_path, monkeypatch, "--ticker-maintenance", "never")

        assert stub.calls == ["nightly", "cycle", "save"]

    def test_the_wait_between_cycles_ends_when_the_night_starts(
        self, tmp_path, monkeypatch
    ):
        stub = DaemonStub(tmp_path, CycleResult(processed=1))
        stub.until_night = 4  # the night starts in 4 seconds; the interval is 2 hours
        sleeps: list[float] = []

        def sleep(seconds):
            sleeps.append(seconds)
            raise KeyboardInterrupt()

        monkeypatch.setattr(time, "sleep", sleep)
        try:
            run_daemon(
                stub,
                tmp_path,
                monkeypatch,
                "--interval",
                "2",
                "--ticker-maintenance",
                "never",
            )
        except KeyboardInterrupt:
            pass

        assert sleeps == [5]  # 4 seconds, plus one to land after the boundary

    def test_a_single_run_does_not_switch_to_the_nightly_schedule(
        self, tmp_path, monkeypatch
    ):
        stub = DaemonStub(tmp_path, CycleResult(processed=1))
        monkeypatch.setattr(yfinance_cli, "yf_parqed", stub)

        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as lock:
            lock.return_value.try_acquire.return_value = True
            result = CliRunner(env={"NO_COLOR": "1"}).invoke(
                yfinance_cli.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--non-interactive"],
                catch_exceptions=False,
            )

        assert result.exit_code == 0
        assert stub.calls == ["cycle"]


# ── found by the code review of 2026-10-10 ───────────────────────────────────


class TestMergeWithAnotherWritersChangeToTheSameTicker:
    """
    The daemon saves every 500 tickers. A command that changes a ticker the
    daemon then fetches, before that save, must not be undone by it.
    """

    def test_remove_ticker_survives_the_daemon_fetching_that_ticker(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(
            tmp_path,
            "AAA",
            CCC=entry("CCC", status="active", newest_bar_date="2026-10-16"),
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        def another_process(stock):
            if stock == "AAA":
                TickerRegistry(ConfigService(tmp_path)).remove_ticker("CCC")

        fetcher.before_fetch = another_process
        instance.update_stock_data()  # no save in between: CCC is fetched from stale memory
        assert fetcher.calls == ["AAA", "CCC"]
        instance.save_ticker_changes()

        ccc = stored(tmp_path)["CCC"]
        assert ccc["manually_removed"] is True
        assert ccc["source"] == "manual"
        assert ccc["intervals"]["1m"]["permanently_dead"] is True
        assert (
            ccc["intervals"]["1m"]["newest_bar_date"] == "2026-10-19"
        )  # the daemon's part
        assert "last_fetch_at" in ccc["intervals"]["1m"]
        assert instance.is_ticker_active_for_interval("CCC", "1m") is False

    def test_add_ticker_survives_the_daemon_fetching_that_ticker(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(
            tmp_path,
            "AAA",
            GONE=entry("GONE", status="not_found"),
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.add("GONE")

        def another_process(stock):
            if stock == "AAA":
                TickerRegistry(ConfigService(tmp_path)).add_ticker("GONE")

        fetcher.before_fetch = another_process
        instance.update_stock_data()
        instance.save_ticker_changes()

        gone = stored(tmp_path)["GONE"]
        assert gone["source"] == "manual"
        assert "last_fetch_at" in gone["intervals"]["1m"]

    def test_a_ticker_deleted_by_another_writer_is_not_brought_back(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA", "BBB")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        def another_process(stock):
            if stock == "AAA":
                write_registry(tmp_path, "AAA")  # the prune tool removed BBB

        fetcher.before_fetch = another_process
        instance.update_stock_data()
        instance.save_ticker_changes()

        assert list(stored(tmp_path)) == ["AAA"]
        assert list(instance.tickers) == ["AAA"]

    def test_merge_entry_takes_each_sides_changes(self):
        base = {"status": "active", "intervals": {"1m": {"status": "active", "x": 1}}}
        ours = {
            "status": "active",
            "last_checked": "today",
            "intervals": {"1m": {"status": "active", "last_fetch_at": "t"}},
        }
        theirs = {
            "status": "active",
            "source": "manual",
            "intervals": {"1m": {"status": "active", "x": 1, "permanently_dead": True}},
        }

        assert merge_entry(base, ours, theirs) == {
            "status": "active",
            "source": "manual",
            "last_checked": "today",
            "intervals": {
                "1m": {
                    "status": "active",
                    "last_fetch_at": "t",
                    "permanently_dead": True,
                }
            },
        }

    def test_a_single_runs_save_keeps_another_writers_change(
        self, tmp_path, clock, monkeypatch
    ):
        """update-data without --daemon reads the file, fetches for hours, then saves."""
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch, nightly=False)

        instance.update_stock_data()
        TickerRegistry(ConfigService(tmp_path)).add_ticker("NEWCO")
        instance.save_tickers()

        assert set(stored(tmp_path)) == {"AAA", "NEWCO"}
        assert (
            stored(tmp_path)["AAA"]["intervals"]["1m"]["newest_bar_date"]
            == "2026-10-19"
        )


class TestNoDataOrFailure:
    """yfinance reports an error response as "no price data found", like a real empty answer."""

    @staticmethod
    def ticker(metadata):
        fake = MagicMock()
        fake._price_history._history_metadata = metadata
        return fake

    def test_yahoos_not_found_answer_is_no_data(self):
        error = YFPricesMissingError(
            "ZZZZ",
            ' (period=7d) (Yahoo error = "No data found, symbol may be delisted")',
        )
        assert is_no_data_answer(self.ticker({}), error) is True

    def test_a_known_symbol_without_bars_is_no_data(self):
        error = YFPricesMissingError("AACIU", " (period=7d)")
        assert is_no_data_answer(self.ticker({"symbol": "AACIU"}), error) is True

    @pytest.mark.parametrize(
        "debug_info, metadata",
        [
            (' (period=7d) (Yahoo error = "Invalid Crumb")', {}),
            (' (period=7d) (Yahoo error = "Internal server error")', {"symbol": "X"}),
            (" (period=7d)(Yahoo status_code = 502)", {"symbol": "X"}),
            (
                " (period=7d)",
                {},
            ),  # a response without a chart: nothing about the symbol
        ],
    )
    def test_anything_else_is_a_failed_request(self, debug_info, metadata):
        error = YFPricesMissingError("AAPL", debug_info)
        assert is_no_data_answer(self.ticker(metadata), error) is False

    def test_metadata_that_cannot_be_looked_at_is_a_failure(self):
        """Asked again too often costs requests; a failure taken for "no data" can cost bars."""
        error = YFPricesMissingError("AAPL", " (period=7d)")
        assert is_no_data_answer(object(), error) is False

    def test_the_installed_yfinance_keeps_the_metadata_where_it_is_looked_for(self):
        """Fails after a yfinance upgrade that moves it; then is_no_data_answer needs a look."""
        import yfinance

        history = yfinance.Ticker("AAPL")._lazy_load_price_history()
        assert hasattr(history, "_history_metadata")
        assert yfinance.Ticker("AAPL")._price_history is None  # set on first use

    def test_the_fetcher_raises_for_an_error_response(self):
        fake = self.ticker({})
        fake.history.side_effect = YFPricesMissingError(
            "AAPL", ' (period=7d) (Yahoo error = "Invalid Crumb")'
        )
        fetcher = DataFetcher(
            limiter=lambda: None,
            today_provider=lambda: datetime(2026, 10, 19),
            empty_frame_factory=pd.DataFrame,
            ticker_factory=lambda symbol: fake,
        )

        with pytest.raises(FetchFailed, match="Invalid Crumb"):
            fetcher.fetch(
                "AAPL", datetime(2026, 10, 19), datetime(2026, 10, 19), "1m", True
            )


class TestTickersThatGoQuiet:
    def test_an_empty_answer_after_bars_does_not_close_the_night(
        self, tmp_path, clock, monkeypatch
    ):
        """Asked again in every later cycle of that night; from the next night its empty answer stands."""
        write_registry(
            tmp_path,
            "AAA",
            WAS=entry("WAS", status="active", newest_bar_date="2026-10-12"),
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.add("WAS")

        first = instance.update_stock_data()
        instance.save_ticker_changes()
        meta = stored(tmp_path)["WAS"]["intervals"]["1m"]
        assert first.went_quiet == 1
        assert "last_fetch_at" not in meta
        assert meta["status"] == "not_found"
        assert meta["quiet_since"] == "2026-10-19T22:00:00+00:00"

        for hours in (2, 4):
            clock.now = NIGHT + timedelta(hours=hours)
            later = instance.update_stock_data()
            instance.save_ticker_changes()
            assert (later.processed, later.went_quiet) == (1, 1)
        assert fetcher.calls == ["AAA", "WAS", "WAS", "WAS"]
        assert "last_fetch_at" not in stored(tmp_path)["WAS"]["intervals"]["1m"]

        # the next night: one empty answer, and the night is closed for it
        clock.now = NIGHT + timedelta(days=1)
        next_night = instance.update_stock_data()
        instance.save_ticker_changes()
        clock.now = NIGHT + timedelta(days=1, hours=2)
        instance.update_stock_data()

        assert next_night.went_quiet == 0
        assert fetcher.calls.count("WAS") == 4
        meta = stored(tmp_path)["WAS"]["intervals"]["1m"]
        assert "quiet_since" not in meta and "last_fetch_at" in meta

    def test_the_second_opinion_can_bring_the_bars(self, tmp_path, clock, monkeypatch):
        write_registry(tmp_path, WAS=entry("WAS", status="active"))
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.add("WAS")
        instance.update_stock_data()
        instance.save_ticker_changes()

        fetcher.empty.clear()  # Yahoo answers properly again
        clock.now = NIGHT + timedelta(hours=2)
        instance.update_stock_data()
        instance.save_ticker_changes()

        meta = stored(tmp_path)["WAS"]["intervals"]["1m"]
        assert meta["status"] == "active"
        assert meta["newest_bar_date"] == "2026-10-19"

    def test_twenty_tickers_going_quiet_in_a_row_end_the_cycle(
        self, tmp_path, clock, monkeypatch
    ):
        """An outage that looks like "no data" for everyone must not close the night."""
        names = [f"T{i:02d}" for i in range(40)]
        write_registry(tmp_path, **{n: entry(n, status="active") for n in names})
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.update(names)

        result = instance.update_stock_data()
        instance.save_ticker_changes()

        assert result.aborted and result.went_quiet == 20
        assert fetcher.calls == names[:20]
        assert "last_fetch_at" not in (tmp_path / "tickers.json").read_text()

        # Yahoo is back two hours later: everything is fetched, nothing was lost
        fetcher.empty.clear()
        clock.now = NIGHT + timedelta(hours=2)
        again = instance.update_stock_data()
        assert again.ran_to_end and again.processed == 40

    def test_quiet_tickers_among_ones_with_bars_do_not_end_the_cycle(
        self, tmp_path, clock, monkeypatch
    ):
        names = [f"T{i:02d}" for i in range(60)]
        write_registry(tmp_path, **{n: entry(n, status="active") for n in names})
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.update(n for i, n in enumerate(names) if i % 20 != 0)  # 57 quiet

        result = instance.update_stock_data()

        assert result.ran_to_end
        assert result.went_quiet == 57


class TestOtherIntervals:
    def test_only_short_lived_intervals_are_fetched_in_full_every_night(
        self, tmp_path, clock, monkeypatch
    ):
        """Daily bars can be fetched again at any time: no full history every night."""
        registry = {
            "AAA": {
                "ticker": "AAA",
                "status": "active",
                "intervals": {
                    "1m": {"status": "active", "newest_bar_date": "2026-10-16"},
                    "1d": {"status": "active", "newest_bar_date": "2026-10-16"},
                },
            }
        }
        (tmp_path / "tickers.json").write_text(json.dumps(registry))
        instance = YFParqed(my_path=tmp_path, my_intervals=["1m", "1d"])
        fetcher = FakeFetcher()
        instance.data_fetcher = fetcher
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance, "utc_now", clock)
        monkeypatch.setattr(instance, "get_today", lambda: datetime(2026, 10, 19, 17))
        instance.use_nightly_schedule()

        instance.update_stock_data()

        assert fetcher.calls == ["AAA", "AAA"]
        assert fetcher.get_all == [True, False]  # 1m in full, 1d from its newest bar


class TestSaveFailuresAcrossCycles:
    def test_an_unsaved_registry_is_not_forgotten_at_the_next_cycle(
        self, tmp_path, clock, monkeypatch
    ):
        """Reading the file again would forget tonight's fetches and fetch everything again."""
        write_registry(tmp_path, "AAA", "BBB")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        with patch.object(
            ConfigService, "save_tickers", side_effect=OSError("disk full")
        ):
            instance.update_stock_data()
            assert instance.save_ticker_changes() is False
            clock.now = NIGHT + timedelta(hours=2)
            second = instance.update_stock_data()  # the save fails again at its start

        assert fetcher.calls == ["AAA", "BBB"]
        assert (second.processed, second.skipped) == (0, 2)

        clock.now = NIGHT + timedelta(hours=4)
        third = instance.update_stock_data()  # the disk has room again

        assert third.skipped == 2 and fetcher.calls == ["AAA", "BBB"]
        assert "last_fetch_at" in stored(tmp_path)["AAA"]["intervals"]["1m"]


def test_the_not_found_probe_does_not_count_as_the_nights_fetch(tmp_path):
    """confirm_not_founds asks Yahoo for one day and stores nothing."""
    config = ConfigService(tmp_path)
    registry = TickerRegistry(
        config,
        initial_tickers={
            "OLD": {"ticker": "OLD", "status": "not_found", "intervals": {}}
        },
        limiter=lambda: None,
        fetch_callback=lambda ticker, interval, period: (True, datetime(2026, 10, 19)),
    )

    registry.confirm_not_founds()

    assert registry.tickers["OLD"]["intervals"]["1d"]["status"] == "active"
    assert (
        registry.fetched_since("OLD", "1d", datetime(2000, 1, 1, tzinfo=UTC)) is False
    )


def test_without_flock_the_lock_lets_the_writer_through(tmp_path, monkeypatch):
    monkeypatch.setattr(config_service, "fcntl", None)
    config = ConfigService(tmp_path)

    with config.tickers_lock():
        with config.tickers_lock(timeout=0.1):
            assert config.save_tickers({"AAA": entry("AAA")}) is True


class TestSingleRunExitCode:
    """A failing ticker no longer ends a single run with a traceback, so the exit code has to tell."""

    @staticmethod
    def run(stub, tmp_path, monkeypatch):
        monkeypatch.setattr(yfinance_cli, "yf_parqed", stub)
        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as lock:
            lock.return_value.try_acquire.return_value = True
            return CliRunner(env={"NO_COLOR": "1"}).invoke(
                yfinance_cli.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--non-interactive"],
            )

    def test_a_clean_run_exits_0(self, tmp_path, monkeypatch):
        stub = DaemonStub(tmp_path, CycleResult(processed=3))
        assert self.run(stub, tmp_path, monkeypatch).exit_code == 0

    def test_a_run_with_failed_tickers_exits_1(self, tmp_path, monkeypatch):
        stub = DaemonStub(tmp_path, CycleResult(processed=2, failed=["BBB"]))
        assert self.run(stub, tmp_path, monkeypatch).exit_code == 1

    def test_a_run_ended_early_exits_1(self, tmp_path, monkeypatch):
        stub = DaemonStub(tmp_path, CycleResult(aborted=True))
        assert self.run(stub, tmp_path, monkeypatch).exit_code == 1


class TestPruneTool:
    @staticmethod
    def tool():
        import importlib.util

        path = Path(__file__).parent.parent / "tools" / "prune_registry.py"
        spec = importlib.util.spec_from_file_location("prune_registry", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_apply_marks_dead_suffixes_through_the_shared_save(self, tmp_path):
        write_registry(
            tmp_path, "AAA", **{"XYZ.WS": entry("XYZ.WS", status="not_found")}
        )

        self.tool().migrate(tmp_path, apply=True)

        on_disk = stored(tmp_path)
        assert on_disk["XYZ.WS"]["intervals"]["1m"]["permanently_dead"] is True
        assert "permanently_dead" not in json.dumps(on_disk["AAA"])
        assert (tmp_path / "tickers.json.lock").exists()

    def test_an_unreadable_registry_stops_the_tool(self, tmp_path):
        (tmp_path / "tickers.json").write_text("{cut off")

        with pytest.raises(ValueError):
            self.tool().migrate(tmp_path, apply=True)

        assert (tmp_path / "tickers.json").read_text() == "{cut off"


# ── found by the second code review of 2026-10-10 ────────────────────────────


class TestRegistryFileEdgeCases:
    def test_a_file_that_disappeared_is_written_again_from_memory(
        self, tmp_path, clock, monkeypatch
    ):
        """Merging onto a missing file would drop every ticker that was not just changed."""
        write_registry(
            tmp_path,
            "AAA",
            "BBB",
            KEPT=entry("KEPT", status="not_found", permanently_dead=True),
        )
        instance, _ = make(tmp_path, clock, monkeypatch)
        instance.load_tickers()
        (tmp_path / "tickers.json").unlink()  # moved aside by someone repairing it

        instance.update_ticker_interval_status(
            "AAA", "1m", True, datetime(2026, 10, 19)
        )
        assert instance.save_ticker_changes() is True

        assert set(stored(tmp_path)) == {"AAA", "BBB", "KEPT"}
        assert set(instance.tickers) == {"AAA", "BBB", "KEPT"}
        assert stored(tmp_path)["KEPT"]["intervals"]["1m"]["permanently_dead"] is True

    def test_a_ticker_pruned_here_but_brought_back_by_add_ticker_stays(self, tmp_path):
        """The daemon's list update works on a registry that may be hours old."""
        dead = entry("DEAD", status="not_found", permanently_dead=True)
        dead["source"] = "csv"
        write_registry(tmp_path, "AAA", DEAD=dead)
        daemon = TickerRegistry(ConfigService(tmp_path))

        TickerRegistry(ConfigService(tmp_path)).add_ticker("DEAD")
        daemon.update_current_list({"AAA": entry("AAA")})  # DEAD is not on the lists
        assert "DEAD" not in daemon.tickers
        daemon.save_changes()

        assert stored(tmp_path)["DEAD"]["source"] == "manual"
        assert "permanently_dead" not in stored(tmp_path)["DEAD"]["intervals"]["1m"]
        assert "DEAD" in daemon.tickers

    def test_a_ticker_pruned_here_and_untouched_elsewhere_is_removed(self, tmp_path):
        dead = entry("DEAD", status="not_found", permanently_dead=True)
        write_registry(tmp_path, "AAA", DEAD=dead)
        daemon = TickerRegistry(ConfigService(tmp_path))

        daemon.update_current_list({"AAA": entry("AAA")})
        daemon.save_changes()

        assert list(stored(tmp_path)) == ["AAA"]

    def test_a_ticker_added_by_hand_outranks_the_same_ticker_from_the_lists(
        self, tmp_path
    ):
        write_registry(tmp_path, "AAA")
        daemon = TickerRegistry(ConfigService(tmp_path))

        TickerRegistry(ConfigService(tmp_path)).add_ticker("NEWCO")
        daemon.update_current_list({"AAA": entry("AAA"), "NEWCO": entry("NEWCO")})
        daemon.save_changes()

        assert stored(tmp_path)["NEWCO"]["source"] == "manual"

    def test_ticker_maintenance_starts_from_the_file_not_from_old_memory(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch)
        instance.update_stock_data()
        instance.save_ticker_changes()

        TickerRegistry(ConfigService(tmp_path)).remove_ticker("AAA")  # hours later
        monkeypatch.setattr(
            instance, "get_new_list_of_stocks", lambda: {"AAA": entry("AAA")}
        )
        instance.update_current_list_of_stocks()

        assert instance.tickers["AAA"]["manually_removed"] is True
        assert stored(tmp_path)["AAA"]["manually_removed"] is True


class TestRangeAnswersAndTimezones:
    @pytest.mark.parametrize(
        "description",
        [
            "Data doesn't exist for startDate = 1420088400, endDate = 1451538000",
            "1m data not available for startTime=1786395600 and endTime=1786568400. "
            "The requested range must be within the last 30 days.",
            "1h data not available for startTime=1722459600 and endTime=1723323600. "
            "The requested range must be within the last 730 days.",
        ],
    )
    def test_yahoo_having_nothing_for_the_window_is_no_data(self, description):
        """Quoted from live answers on 2026-10-10."""
        error = YFPricesMissingError(
            "AAPL", f' (1d a -> b) (Yahoo error = "{description}")'
        )
        assert is_no_data_answer(MagicMock(), error) is True

    def test_a_window_that_is_too_wide_is_a_failed_request(self):
        error = YFPricesMissingError(
            "AAPL",
            ' (1m a -> b) (Yahoo error = "1m data not available for startTime=1 and '
            "endTime=2. Only 8 days worth of 1m granularity data are allowed to be "
            'fetched per request.")',
        )
        assert is_no_data_answer(MagicMock(), error) is False

    @staticmethod
    def fetcher(fake, limiter=lambda: None):
        return DataFetcher(
            limiter=limiter,
            today_provider=lambda: datetime(2026, 10, 19),
            empty_frame_factory=pd.DataFrame,
            ticker_factory=lambda symbol: fake,
        )

    def test_an_empty_window_is_not_sent_to_yahoo(self):
        fake = MagicMock()
        at = datetime(2026, 10, 19)

        result = self.fetcher(fake).fetch("AAPL", at, at, "1d")

        assert result.empty
        fake.history.assert_not_called()

    def test_a_missing_time_zone_for_an_unknown_symbol_is_no_data(self):
        fake = MagicMock()
        fake.history.side_effect = [
            YFTzMissingError("ZZZZ"),
            YFPricesMissingError(
                "ZZZZ",
                ' (period=5d) (Yahoo error = "No data found, symbol may be delisted")',
            ),
        ]
        limiter_calls = []

        result = self.fetcher(fake, lambda: limiter_calls.append(1)).fetch(
            "ZZZZ", datetime(2026, 10, 1), datetime(2026, 10, 9), "1d"
        )

        assert result.empty
        assert fake.history.call_args_list[1].kwargs["period"] == "5d"
        assert len(limiter_calls) == 2  # the second request waits its turn too

    def test_a_missing_time_zone_for_a_symbol_yahoo_knows_is_a_failure(self):
        fake = MagicMock()
        fake.history.side_effect = [YFTzMissingError("AAPL"), bars("AAPL")]

        with pytest.raises(FetchFailed, match="time zone lookup failed"):
            self.fetcher(fake).fetch(
                "AAPL", datetime(2026, 10, 1), datetime(2026, 10, 9), "1d"
            )

    def test_a_failed_second_request_is_a_failure(self):
        fake = MagicMock()
        fake.history.side_effect = [YFTzMissingError("AAPL"), ConnectionError("down")]

        with pytest.raises(ConnectionError):
            self.fetcher(fake).fetch(
                "AAPL", datetime(2026, 10, 1), datetime(2026, 10, 9), "1d"
            )


class TestWhatClosesANight:
    def test_a_single_run_does_not_close_the_night_for_the_daemon(
        self, tmp_path, clock, monkeypatch
    ):
        """A run from the command line fetches a window; the daemon still owes the 7 days."""
        write_registry(tmp_path, "AAA", "QUIET")
        single, fetcher = make(tmp_path, clock, monkeypatch, nightly=False)
        fetcher.empty.add("QUIET")
        single.update_stock_data()
        single.save_tickers()

        assert "last_fetch_at" not in (tmp_path / "tickers.json").read_text()

        daemon, asked = make(tmp_path, clock, monkeypatch)
        daemon.update_stock_data()
        assert asked.calls == ["AAA", "QUIET"]

    def test_a_window_given_by_hand_does_not_close_the_night(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        instance.update_stock_data(
            start_date=datetime(2026, 10, 1), end_date=datetime(2026, 10, 2)
        )
        instance.save_ticker_changes()
        instance.update_stock_data()

        assert fetcher.get_all == [False, True]  # the backfill, then the night's fetch

    def test_an_up_to_date_ticker_is_done_for_the_night(
        self, tmp_path, clock, monkeypatch
    ):
        """Daily bars already stored: later cycles must not walk the ticker again."""
        registry = {
            "AAA": {
                "ticker": "AAA",
                "status": "active",
                "intervals": {
                    "1d": {"status": "active", "newest_bar_date": "2026-10-19"}
                },
            }
        }
        (tmp_path / "tickers.json").write_text(json.dumps(registry))
        instance = YFParqed(my_path=tmp_path, my_intervals=["1d"])
        fetcher = FakeFetcher()
        instance.data_fetcher = fetcher
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance, "utc_now", clock)
        monkeypatch.setattr(instance, "get_today", lambda: datetime(2026, 10, 19, 17))
        instance.use_nightly_schedule()

        first = instance.update_stock_data()
        instance.save_ticker_changes()
        second = instance.update_stock_data()

        assert fetcher.calls == []
        assert (first.skipped, second.skipped) == (0, 1)


# ── found by the third code review of 2026-10-10 ─────────────────────────────


def active(*names: str) -> dict:
    return {name: entry(name, status="active") for name in names}


class TestOutagesThatLookLikeNoData:
    def test_an_outage_over_several_cycles_closes_nobodys_night(
        self, tmp_path, clock, monkeypatch
    ):
        """Every ticker returns nothing for two cycles; when Yahoo is back all are fetched."""
        names = [f"T{i:02d}" for i in range(60)]
        write_registry(tmp_path, **active(*names))
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.update(names)
        fetcher.yahoo_down = True

        for hours in (0, 2):
            clock.now = NIGHT + timedelta(hours=hours)
            result = instance.update_stock_data()
            instance.save_ticker_changes()
            assert result.aborted and result.went_quiet == 20
        assert "last_fetch_at" not in (tmp_path / "tickers.json").read_text()

        fetcher.empty.clear()
        fetcher.yahoo_down = False
        clock.now = NIGHT + timedelta(hours=4)
        back = instance.update_stock_data()

        assert back.ran_to_end and back.processed == 60 and back.went_quiet == 0

    def test_many_quiet_tickers_do_not_end_the_cycle_while_yahoo_answers(
        self, tmp_path, clock, monkeypatch
    ):
        """In a later cycle only the quiet tickers are due, so they always come in a row."""
        quiet = [f"Q{i:02d}" for i in range(45)]
        write_registry(tmp_path, **active("GOOD", *quiet))
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.empty.update(quiet)

        first = instance.update_stock_data()
        instance.save_ticker_changes()
        clock.now = NIGHT + timedelta(hours=2)
        second = instance.update_stock_data()

        assert first.ran_to_end and second.ran_to_end
        assert second.processed == 45 and second.went_quiet == 45
        # Yahoo was asked for a ticker that had bars, every 20 quiet ones
        assert fetcher.reference_calls == ["GOOD"] * 4

    def test_the_health_check_uses_a_ticker_that_had_bars_tonight(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(
            tmp_path,
            OLD=entry("OLD", status="active", newest_bar_date="2026-01-05"),
            RECENT=entry("RECENT", status="active", newest_bar_date="2026-10-16"),
            **active("AAA"),
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        monkeypatch.setattr(instance, "get_today", lambda: datetime(2026, 10, 19, 17))

        assert instance.yahoo_answers("1m") is True
        assert fetcher.reference_calls == ["RECENT"]  # bars in the last days

        instance.update_ticker_interval_status(
            "AAA", "1m", True, datetime(2026, 10, 19)
        )
        instance.yahoo_answers("1m")
        assert fetcher.reference_calls[-1] == "AAA"  # bars tonight

        fetcher.yahoo_down = True
        assert instance.yahoo_answers("1m") is False

    def test_without_a_ticker_known_to_have_bars_nothing_can_be_said(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, "AAA")
        instance, fetcher = make(tmp_path, clock, monkeypatch)

        assert instance.yahoo_answers("1m") is False
        assert fetcher.reference_calls == []


class TestFailuresInLaterCycles:
    def test_tickers_that_keep_failing_do_not_end_the_retry_cycle(
        self, tmp_path, clock, monkeypatch
    ):
        """25 failing tickers are 25 in a row once everything else is done for the night."""
        good = [f"G{i:02d}" for i in range(30)]
        bad = [f"X{i:02d}" for i in range(25)]
        names = [n for pair in zip(good, bad + good[:5]) for n in pair][:55]
        write_registry(tmp_path, *dict.fromkeys(good + bad))
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        instance.my_intervals = ["1m", "5m"]
        for name in bad:
            fetcher.failing[name] = FetchFailed("Yahoo error")
        del names

        first = instance.update_stock_data()
        instance.save_ticker_changes()
        assert first.ran_to_end and len(first.failed) == 50  # 25 per interval

        mark = len(fetcher.calls)
        clock.now = NIGHT + timedelta(hours=2)
        second = instance.update_stock_data()

        assert second.ran_to_end
        assert fetcher.asked_since(mark) == bad + bad  # both intervals reached
        assert len(second.failed) == 50

    def test_a_hundred_failures_in_a_row_end_the_cycle_even_when_yahoo_answers(
        self, tmp_path, clock, monkeypatch
    ):
        """Then it is the host, not Yahoo: a full disk fails every write."""
        names = [f"T{i:03d}" for i in range(150)]
        write_registry(
            tmp_path,
            **{
                n: entry(n, status="active", newest_bar_date="2026-10-16")
                for n in names
            },
        )
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        monkeypatch.setattr(instance, "get_today", lambda: datetime(2026, 10, 19, 17))
        monkeypatch.setattr(
            instance, "merge_yf", MagicMock(side_effect=OSError("No space left"))
        )

        result = instance.update_stock_data()

        assert MAX_FAILURES_WITHOUT_SUCCESS == 100
        assert result.aborted and len(result.failed) == 100

    def test_an_unexpected_error_is_logged_with_its_traceback(
        self, tmp_path, clock, monkeypatch
    ):
        from loguru import logger

        write_registry(tmp_path, "AAA", "BBB", "CCC")
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        fetcher.failing["AAA"] = KeyError("a bug")
        fetcher.failing["BBB"] = FetchFailed("Yahoo said no")
        fetcher.failing["CCC"] = KeyError("the same bug again")
        records = []
        sink = logger.add(lambda message: records.append(message.record), level="ERROR")
        try:
            instance.update_stock_data()
        finally:
            logger.remove(sink)

        by_ticker = {r["message"].split(" ")[0]: r["exception"] for r in records}
        assert by_ticker["AAA"] is not None  # a bug: where it was raised matters
        assert by_ticker["BBB"] is None  # Yahoo's failure says it all in one line
        assert by_ticker["CCC"] is None  # the same kind of bug is not traced twice


class TestWindowsGivenByHand:
    def test_empty_answers_for_an_old_window_do_not_end_the_run(
        self, tmp_path, clock, monkeypatch
    ):
        """A backfill of a time before most tickers were listed: empty answers in a row are normal."""
        names = [f"T{i:02d}" for i in range(40)]
        write_registry(tmp_path, **active(*names))
        for nightly in (False, True):
            instance, fetcher = make(tmp_path, clock, monkeypatch, nightly=nightly)
            fetcher.empty.update(names)

            result = instance.update_stock_data(
                start_date=datetime(2005, 1, 3), end_date=datetime(2005, 3, 1)
            )

            assert result.ran_to_end and result.processed == 40
            assert result.went_quiet == 0

    def test_a_stale_hourly_ticker_is_fetched_for_what_yahoo_still_keeps(self):
        """Before, a start older than 729 days with an end of today asked for nothing at all."""
        fake = MagicMock()
        fake.history.return_value = pd.DataFrame()
        today = datetime(2026, 10, 19, 17)
        fetcher = DataFetcher(
            limiter=lambda: None,
            today_provider=lambda: today,
            empty_frame_factory=pd.DataFrame,
            ticker_factory=lambda symbol: fake,
        )

        assert fetcher.has_window(datetime(2024, 1, 1), today, "1h") is True
        fetcher.fetch("AAA", datetime(2024, 1, 1), today, "1h")
        asked = fake.history.call_args.kwargs
        assert (today - asked["start"]).days in (728, 729) and asked["end"] == today

        # a window that is older than Yahoo keeps, start to end: nothing to ask
        assert (
            fetcher.has_window(datetime(2023, 1, 1), datetime(2023, 2, 1), "1h")
            is False
        )

    def test_a_window_with_nothing_to_ask_is_not_recorded_as_an_answer(
        self, tmp_path, clock, monkeypatch
    ):
        write_registry(tmp_path, **active("AAA"))
        instance, fetcher = make(tmp_path, clock, monkeypatch)
        monkeypatch.setattr(fetcher, "has_window", lambda *args: False)

        outcome = instance.save_single_stock_data(
            "AAA", datetime(2023, 1, 2), datetime(2023, 2, 1), "1m"
        )

        assert outcome is None and fetcher.calls == []
        assert instance.tickers["AAA"]["intervals"]["1m"] == {"status": "active"}


class TestWhatCountsAsStored:
    def test_a_month_the_backend_refused_to_write_is_a_failure(
        self, tmp_path, clock, monkeypatch
    ):
        """Its bars are not on disk, so the night must not be closed for the ticker."""
        write_registry(tmp_path, "AAA", "BBB")
        instance, _ = make(tmp_path, clock, monkeypatch)

        def merge(request, new_data, refused=None):
            if request.ticker == "AAA":
                refused.append(pd.Period("2026-10", freq="M"))
            return []

        monkeypatch.setattr(instance._partition_storage, "merge", merge)
        result = instance.update_stock_data()

        assert result.failed == ["AAA"] and result.processed == 1
        assert instance.tickers["AAA"]["intervals"] == {}
        assert instance.is_due_tonight("AAA", "1m")

    def test_the_backend_names_the_month_it_refused(self, tmp_path, clock, monkeypatch):
        write_registry(tmp_path, "AAA")
        instance, _ = make(tmp_path, clock, monkeypatch)
        instance.update_stock_data()  # stores three bars of 2026-10-19
        backend = instance._partition_storage
        request = instance._build_storage_request("AAA", "1m")
        dedupe = backend._dedupe

        def lossy(frame):  # a merge that loses the earliest stored bar
            deduped = dedupe(frame)
            return deduped[deduped["date"] > deduped["date"].min()]

        monkeypatch.setattr(backend, "_dedupe", lossy)
        refused: list = []
        written = backend.merge(request, bars("AAA", count=4), refused=refused)

        assert written == []
        assert [str(month) for month in refused] == ["2026-10"]

        with pytest.raises(RuntimeError, match="2026-10 not written"):
            instance.merge_yf(bars("AAA", count=4), request)

    def test_the_not_found_probe_writes_no_bar_date(self, tmp_path):
        """No bars were stored, so the cycle must still fetch the full history."""
        registry = TickerRegistry(
            ConfigService(tmp_path),
            initial_tickers={
                "OLD": {"ticker": "OLD", "status": "not_found", "intervals": {}}
            },
            limiter=lambda: None,
            fetch_callback=lambda ticker, interval, period: (
                True,
                datetime(2026, 10, 19),
            ),
        )

        registry.confirm_not_founds()

        assert registry.get_last_data_date("OLD", "1d") is None
