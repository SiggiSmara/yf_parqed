"""Step J of the daemon resource footprint ADR: find, record and keep damaged files.

The capture cannot be repeated and the data has no backup. So an unreadable
file is renamed, never deleted; every damaged file is written to
``damaged_partitions.jsonl``; a freshly written file is read back before it
replaces the stored one; and the files of a closed month are read once, with
the result noted in ``partition_checks.json``.
"""

import json
import os
import signal
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest
from freezegun import freeze_time
from typer.testing import CliRunner

from yf_parqed import yfinance_cli
from yf_parqed.common import partitioned_storage_backend
from yf_parqed.common.damage_log import DamageLog, describe_data_file
from yf_parqed.common.parquet_recovery import (
    ParquetRecoveryError,
    move_aside,
    safe_read_parquet,
)
from yf_parqed.common.partition_check import (
    PartitionCheckState,
    check_partitions,
    last_closed_month,
)
from yf_parqed.yahoo.primary_class import YFParqed

GARBAGE = b"not a parquet file"


def _bars(dates: list[str], ticker: str = "AAPL", close: float = 1.0) -> pd.DataFrame:
    """Bars shaped like the fetcher's output: no ``sequence`` column."""
    index = pd.MultiIndex.from_tuples(
        [(ticker, pd.Timestamp(d)) for d in dates], names=["stock", "date"]
    )
    return pd.DataFrame(
        {"open": close, "high": close, "low": close, "close": close, "volume": 10},
        index=index,
    )


@pytest.fixture()
def yf(tmp_path):
    return YFParqed(my_path=tmp_path, my_intervals=["1d"])


def _store(yf: YFParqed, ticker: str, dates: list[str]) -> None:
    yf.merge_yf(_bars(dates, ticker), yf._build_storage_request(ticker, "1d"))


def _partition(tmp_path: Path, ticker: str, month: str) -> Path:
    year, number = month.split("-")
    return (
        tmp_path
        / f"data/us/yahoo/stocks_1d/ticker={ticker}/year={year}/month={number}"
        / "data.parquet"
    )


def _read(path: Path, on_damaged=None) -> pd.DataFrame:
    return safe_read_parquet(
        path=path,
        required_columns=set(),
        normalizer=lambda df: df,
        empty_frame_factory=pd.DataFrame,
        on_damaged=on_damaged,
    )


def _snapshot(root: Path) -> dict[str, tuple[int, bytes]]:
    return {
        str(path.relative_to(root)): (path.stat().st_mtime_ns, path.read_bytes())
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


# --- J1: an unreadable file is moved aside, never deleted -------------------


def test_unreadable_file_is_renamed_and_keeps_its_bytes(tmp_path):
    path = tmp_path / "data.parquet"
    path.write_bytes(GARBAGE)
    seen = []

    with pytest.raises(ParquetRecoveryError, match="moved aside"):
        _read(path, on_damaged=lambda *args: seen.append(args))

    assert not path.exists()
    (moved,) = tmp_path.glob("data.parquet.damaged-*")
    assert moved.read_bytes() == GARBAGE
    assert not moved.name.endswith(".parquet")
    ((seen_path, seen_moved, seen_error),) = seen
    assert (seen_path, seen_moved) == (path, moved)
    assert isinstance(seen_error, Exception)


def test_missing_file_has_nothing_to_move(tmp_path):
    seen = []

    with pytest.raises(ParquetRecoveryError, match="does not exist"):
        _read(tmp_path / "data.parquet", on_damaged=lambda *args: seen.append(args))

    assert seen == []
    assert list(tmp_path.iterdir()) == []


@freeze_time("2026-10-04 10:15:00")
def test_two_files_moved_in_the_same_second_keep_both(tmp_path):
    path = tmp_path / "data.parquet"
    path.write_bytes(b"first")
    first = move_aside(path)
    path.write_bytes(b"second")
    second = move_aside(path)

    assert first.name == "data.parquet.damaged-20261004T101500Z"
    assert second != first
    assert (first.read_bytes(), second.read_bytes()) == (b"first", b"second")


def test_file_that_cannot_be_renamed_stays_and_is_reported(tmp_path, monkeypatch):
    path = tmp_path / "data.parquet"
    path.write_bytes(GARBAGE)
    seen = []

    def refuse(self, target):
        raise OSError("read-only file system")

    monkeypatch.setattr(Path, "rename", refuse)

    with pytest.raises(ParquetRecoveryError, match="left in place"):
        _read(path, on_damaged=lambda *args: seen.append(args))

    assert path.read_bytes() == GARBAGE
    assert [(p, moved) for p, moved, _ in seen] == [(path, None)]


def test_a_passing_error_does_not_move_the_file(tmp_path, monkeypatch):
    """The read is tried twice; an error that goes away has no consequences."""
    path = tmp_path / "data.parquet"
    pd.DataFrame({"a": [1, 2]}).to_parquet(path)
    real_read = pd.read_parquet
    attempts = []
    seen = []

    def read(target, *args, **kwargs):
        attempts.append(target)
        if len(attempts) == 1:
            raise OSError(24, "Too many open files")
        return real_read(target, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", read)

    assert len(_read(path, on_damaged=lambda *args: seen.append(args))) == 2
    assert len(attempts) == 2
    assert seen == []
    assert [p.name for p in tmp_path.iterdir()] == ["data.parquet"]


def test_a_failing_recorder_does_not_hide_the_read_error(tmp_path):
    path = tmp_path / "data.parquet"
    path.write_bytes(GARBAGE)

    def recorder(*args):
        raise OSError("disk full")

    with pytest.raises(ParquetRecoveryError, match="moved aside"):
        _read(path, on_damaged=recorder)

    assert len(list(tmp_path.glob("data.parquet.damaged-*"))) == 1


def test_capture_continues_into_a_new_file_after_a_damaged_month(yf, tmp_path):
    """The cycle that finds the file fails for that ticker; the next one starts a new file."""
    _store(yf, "AAPL", ["2024-02-12", "2024-03-04"])
    march = _partition(tmp_path, "AAPL", "2024-03")
    february = _partition(tmp_path, "AAPL", "2024-02")
    march.write_bytes(GARBAGE)
    february_before = february.read_bytes()

    with pytest.raises(RuntimeError, match="2024-03"):
        _store(yf, "AAPL", ["2024-03-05"])

    (moved,) = march.parent.glob("data.parquet.damaged-*")
    assert moved.read_bytes() == GARBAGE
    assert not march.exists()

    _store(yf, "AAPL", ["2024-03-05"])

    assert list(pd.read_parquet(march)["date"]) == [pd.Timestamp("2024-03-05")]
    assert moved.read_bytes() == GARBAGE
    assert february.read_bytes() == february_before
    # the full read ignores the file that was moved aside
    assert len(yf.read_yf(yf._build_storage_request("AAPL", "1d"))) == 2


# --- J2: the damage record --------------------------------------------------


def test_damaged_file_found_by_the_daemon_is_written_to_the_record(yf, tmp_path):
    _store(yf, "AAPL", ["2024-03-04"])
    march = _partition(tmp_path, "AAPL", "2024-03")
    march.write_bytes(GARBAGE)

    with pytest.raises(RuntimeError):
        _store(yf, "AAPL", ["2024-03-05"])

    (line,) = (tmp_path / "damaged_partitions.jsonl").read_text().splitlines()
    entry = json.loads(line)
    (moved,) = march.parent.glob("data.parquet.damaged-*")
    assert entry["path"] == str(march)
    assert entry["moved_to"] == str(moved)
    assert (entry["ticker"], entry["interval"], entry["month"]) == (
        "AAPL",
        "1d",
        "2024-03",
    )
    assert entry["action"] == "moved aside"
    assert entry["found_by"] == "read"
    assert entry["error"].startswith("ArrowInvalid: ")
    assert entry["size"] == len(GARBAGE)
    assert datetime.fromisoformat(entry["modified"]).tzinfo is not None
    assert datetime.fromisoformat(entry["when"]).tzinfo is not None


def test_record_follows_the_working_directory(yf, tmp_path):
    """The CLI builds the instance first and sets the working directory afterwards."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    yf.set_working_path(elsewhere)

    yf._record_damaged_file(Path("x/data.parquet"), None, OSError("boom"))

    assert not (tmp_path / "damaged_partitions.jsonl").exists()
    assert len(DamageLog(elsewhere / "damaged_partitions.jsonl").entries()) == 1


def test_record_survives_a_half_written_line(tmp_path):
    log = DamageLog(tmp_path / "damaged_partitions.jsonl")
    log.record(path=Path("a/data.parquet"), error="first", found_by="read")
    with open(log.path, "a") as handle:
        handle.write('{"when": "2026-10-04T10:00:00+00:00", "path": "b/da')

    log.record(path=Path("c/data.parquet"), error="third", found_by="read")

    assert [entry["error"] for entry in log.entries()] == ["first", "third"]
    assert log.path.read_text().endswith("\n")


def test_record_survives_bytes_that_are_not_text(tmp_path):
    log = DamageLog(tmp_path / "damaged_partitions.jsonl")
    log.record(path=Path("a/data.parquet"), error="first", found_by="read")
    with open(log.path, "ab") as handle:
        handle.write(b"\xff\xfe broken line\n")
    log.record(path=Path("c/data.parquet"), error="third", found_by="read")

    assert [entry["error"] for entry in log.entries()] == ["first", "third"]


def test_describe_data_file_reads_both_layouts():
    partitioned = Path(
        "/w/data/us/yahoo/stocks_1m/ticker=BRK.B/year=2026/month=09/data.parquet"
    )
    legacy = Path("/w/stocks_1d/AAPL.parquet")

    assert describe_data_file(partitioned) == {
        "ticker": "BRK.B",
        "interval": "1m",
        "month": "2026-09",
    }
    assert describe_data_file(legacy) == {
        "ticker": "AAPL",
        "interval": "1d",
        "month": None,
    }


def test_legacy_backend_records_a_damaged_file(tmp_path):
    yf = YFParqed(my_path=tmp_path, my_intervals=["1d"])
    path = tmp_path / "stocks_1d" / "AAPL.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(GARBAGE)

    assert yf.read_yf(path).empty

    (entry,) = yf.damage_log.entries()
    assert (entry["ticker"], entry["interval"]) == ("AAPL", "1d")
    assert Path(entry["moved_to"]).read_bytes() == GARBAGE


def test_legacy_file_that_cannot_be_moved_is_not_overwritten(tmp_path, monkeypatch):
    """Returning "no data" here would let the legacy save write a new file over it."""
    yf = YFParqed(my_path=tmp_path, my_intervals=["1d"])
    path = tmp_path / "stocks_1d" / "AAPL.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(GARBAGE)

    def refuse(self, target):
        raise OSError("read-only file system")

    monkeypatch.setattr(Path, "rename", refuse)

    with pytest.raises(RuntimeError, match="left in place"):
        yf.merge_yf(_bars(["2024-03-05"]), path)

    assert path.read_bytes() == GARBAGE
    (entry,) = yf.damage_log.entries()
    assert entry["action"] == "left in place"


def test_migration_tool_records_a_damaged_file(tmp_path):
    from yf_parqed.common.config_service import ConfigService
    from yf_parqed.common.storage import StorageRequest
    from yf_parqed.partition_migration_service import PartitionMigrationService

    service = PartitionMigrationService(ConfigService(tmp_path))
    path = tmp_path / "stocks_1d" / "AAPL.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(GARBAGE)

    service._legacy_backend.read(
        StorageRequest(root=tmp_path, interval="1d", ticker="AAPL")
    )

    (entry,) = DamageLog(tmp_path / "damaged_partitions.jsonl").entries()
    assert (entry["found_by"], entry["action"]) == ("migration", "moved aside")


# --- J4: a new file is read back before it replaces the stored one ----------


def test_write_with_a_wrong_row_count_does_not_replace_the_file(
    yf, tmp_path, monkeypatch
):
    _store(yf, "AAPL", ["2024-03-04"])
    march = _partition(tmp_path, "AAPL", "2024-03")
    before = march.read_bytes()
    monkeypatch.setattr(
        partitioned_storage_backend.pq,
        "read_metadata",
        lambda path: MagicMock(num_rows=1),
    )

    with pytest.raises(RuntimeError, match="holds 1 rows after writing, 2 were meant"):
        _store(yf, "AAPL", ["2024-03-05"])

    assert march.read_bytes() == before
    assert [p.name for p in march.parent.iterdir()] == ["data.parquet"]


def test_write_that_cannot_be_read_back_does_not_replace_the_file(
    yf, tmp_path, monkeypatch
):
    _store(yf, "AAPL", ["2024-03-04"])
    march = _partition(tmp_path, "AAPL", "2024-03")
    before = march.read_bytes()
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, **kwargs: Path(path).write_bytes(GARBAGE),
    )

    with pytest.raises(RuntimeError, match="cannot be read back"):
        _store(yf, "AAPL", ["2024-03-05"])

    assert march.read_bytes() == before
    assert [p.name for p in march.parent.iterdir()] == ["data.parquet"]


# --- J3: the check of a closed month ----------------------------------------


@pytest.fixture()
def stored(yf, tmp_path):
    """Three tickers with February and March 2024; returns the dataset directory."""
    for ticker in ("AAPL", "MSFT", "ZTS"):
        _store(yf, ticker, ["2024-02-12", "2024-03-04", "2024-03-05"])
    _store(yf, "NEW", ["2024-03-05"])
    return tmp_path / "data/us/yahoo/stocks_1d"


def test_check_reads_one_month_and_changes_nothing(stored, tmp_path):
    _partition(tmp_path, "MSFT", "2024-03").write_bytes(GARBAGE)
    _partition(tmp_path, "AAPL", "2024-02").write_bytes(GARBAGE)  # another month
    before = _snapshot(tmp_path)

    result = check_partitions([stored], "2024-03")

    check = result.months["2024-03"]
    assert list(result.months) == ["2024-03"]
    assert (check.files, check.rows) == (4, 5)
    assert [path for path, _ in check.damaged] == [
        _partition(tmp_path, "MSFT", "2024-03")
    ]
    assert result.complete
    assert _snapshot(tmp_path) == before


def test_check_reports_a_file_without_rows(stored, tmp_path):
    path = _partition(tmp_path, "ZTS", "2024-03")
    pd.read_parquet(path).iloc[0:0].to_parquet(path, index=False)

    result = check_partitions([stored], "2024-03")

    assert result.damaged == [(path, "the file holds no rows")]


def test_check_finds_damage_inside_a_file_with_a_good_footer(stored, tmp_path):
    """The footer still reads; only a full decode notices the broken data page."""
    path = _partition(tmp_path, "ZTS", "2024-03")
    content = bytearray(path.read_bytes())
    for offset in range(4, 60):
        content[offset] ^= 0xFF
    path.write_bytes(bytes(content))
    assert pq.read_metadata(path).num_rows == 2

    result = check_partitions([stored], "2024-03")

    assert [p for p, _ in result.damaged] == [path]


def test_check_of_all_months_groups_by_month(stored, tmp_path):
    _partition(tmp_path, "AAPL", "2024-02").write_bytes(GARBAGE)

    result = check_partitions([stored])

    assert {key: check.files for key, check in result.months.items()} == {
        "2024-02": 3,
        "2024-03": 4,
    }
    assert [p for p, _ in result.damaged] == [_partition(tmp_path, "AAPL", "2024-02")]


def test_check_stops_between_tickers(stored):
    asked = []

    def should_stop() -> bool:
        asked.append(1)
        return len(asked) > 2

    result = check_partitions([stored], "2024-03", should_stop)

    assert not result.complete
    assert result.files == 2


def test_month_without_files_is_an_empty_result(tmp_path):
    result = check_partitions([], "2024-03")

    assert (result.files, result.complete) == (0, True)
    assert list(result.months) == ["2024-03"]


def test_last_closed_month():
    assert last_closed_month(datetime(2026, 10, 4)) == "2026-09"
    assert last_closed_month(datetime(2026, 1, 1)) == "2025-12"


@freeze_time("2024-04-02 09:00:00")
def test_verify_records_damage_once_and_notes_the_month(yf, stored, tmp_path):
    damaged = _partition(tmp_path, "MSFT", "2024-03")
    damaged.write_bytes(GARBAGE)

    yf.verify_partitions("2024-03")
    yf.verify_partitions("2024-03")

    (entry,) = yf.damage_log.entries()
    assert entry["path"] == str(damaged)
    assert entry["action"] == "left in place"
    assert entry["moved_to"] is None
    assert entry["found_by"] == "verify-partitions"
    assert damaged.read_bytes() == GARBAGE
    state = json.loads((tmp_path / "partition_checks.json").read_text())
    assert list(state) == ["2024-03"]
    assert (state["2024-03"]["files"], state["2024-03"]["damaged"]) == (4, 1)


@freeze_time("2024-03-20 09:00:00")
def test_an_open_month_is_checked_but_not_noted(yf, stored, tmp_path):
    """An entry for the running month would hide it from the check after it closes."""
    result = yf.verify_partitions(None)

    assert sorted(result.months) == ["2024-02", "2024-03"]
    assert list(PartitionCheckState(tmp_path / "partition_checks.json").load()) == [
        "2024-02"
    ]


@freeze_time("2024-04-02 09:00:00")
def test_last_closed_month_is_checked_once(yf, stored, tmp_path):
    _partition(tmp_path, "MSFT", "2024-03").write_bytes(GARBAGE)

    first = yf.check_last_closed_month()
    second = yf.check_last_closed_month()

    assert first.months["2024-03"].files == 4
    assert second is None
    (entry,) = yf.damage_log.entries()
    assert entry["found_by"] == "month-close check"


@freeze_time("2024-04-02 09:00:00")
def test_a_stopped_check_is_not_noted_and_runs_again(yf, stored, tmp_path):
    stopped = yf.check_last_closed_month(should_stop=lambda: True)

    assert not stopped.complete
    assert not (tmp_path / "partition_checks.json").exists()
    assert yf.check_last_closed_month().complete


@freeze_time("2024-04-02 09:00:00")
def test_a_record_that_cannot_be_written_does_not_repeat_the_check(
    yf, stored, tmp_path, monkeypatch
):
    """For example a record file left behind by root. The month is still noted as checked."""
    _partition(tmp_path, "MSFT", "2024-03").write_bytes(GARBAGE)
    _partition(tmp_path, "ZTS", "2024-03").write_bytes(GARBAGE)

    def refuse(*args, **kwargs):
        raise PermissionError("damaged_partitions.jsonl is not writable")

    monkeypatch.setattr(DamageLog, "record", refuse)

    result = yf.check_last_closed_month()

    assert len(result.damaged) == 2
    state = json.loads((tmp_path / "partition_checks.json").read_text())
    assert state["2024-03"]["damaged"] == 2
    assert yf.check_last_closed_month() is None


@freeze_time("2024-04-02 09:00:00")
def test_result_is_returned_when_nothing_can_be_written_down(
    yf, stored, tmp_path, monkeypatch
):
    """For example the command run by a user who may not write the working directory."""
    _partition(tmp_path, "MSFT", "2024-03").write_bytes(GARBAGE)

    def refuse(*args, **kwargs):
        raise PermissionError("read-only working directory")

    monkeypatch.setattr(DamageLog, "record", refuse)
    monkeypatch.setattr(PartitionCheckState, "record", refuse)

    assert len(yf.verify_partitions("2024-03").damaged) == 1


@freeze_time("2024-04-02 09:00:00")
def test_a_file_damaged_again_after_being_replaced_is_recorded_again(
    yf, stored, tmp_path
):
    path = _partition(tmp_path, "MSFT", "2024-03")
    path.write_bytes(GARBAGE)
    yf.verify_partitions("2024-03")
    yf.verify_partitions("2024-03")
    assert len(yf.damage_log.entries()) == 1

    path.write_bytes(GARBAGE + b", another one")
    yf.verify_partitions("2024-03")

    assert [entry["size"] for entry in yf.damage_log.entries()] == [
        len(GARBAGE),
        len(GARBAGE) + 13,
    ]


@freeze_time("2024-04-02 09:00:00")
def test_bad_bytes_in_the_state_file_mean_check_again(yf, stored, tmp_path):
    (tmp_path / "partition_checks.json").write_bytes(b'{"2024-03": \xff\xfe')

    assert yf.check_last_closed_month().complete
    assert "2024-03" in json.loads((tmp_path / "partition_checks.json").read_text())


@freeze_time("2024-04-02 09:00:00")
def test_an_unreadable_state_file_means_check_again(yf, stored, tmp_path):
    (tmp_path / "partition_checks.json").write_text("{ half")

    assert yf.check_last_closed_month().complete
    assert "2024-03" in json.loads((tmp_path / "partition_checks.json").read_text())


# --- the daemon runs the check after the cycle -------------------------------


class _DaemonStub:
    new_not_found = False
    config = MagicMock()

    def __init__(
        self, path: Path, stop_in_cycle: bool = False, fail_in_cycle: bool = False
    ) -> None:
        self.my_path = path
        self.stop_in_cycle = stop_in_cycle
        self.fail_in_cycle = fail_in_cycle
        self.calls: list[str] = []

    # The nightly schedule the daemon switches on (ADR 2026-10-03, Decision 6).
    night_start_hour_utc = 22

    def use_nightly_schedule(self):
        pass

    def seconds_until_next_night(self):
        return 10**9

    def save_ticker_changes(self):
        return False

    def set_working_path(self, path):
        return path

    def update_stock_data(self, start_date=None, end_date=None, should_stop=None):
        self.calls.append("cycle")
        if self.stop_in_cycle:
            os.kill(os.getpid(), 15)
        if self.fail_in_cycle:
            raise RuntimeError("a ticker failed and ended the cycle")

    def check_last_closed_month(self, should_stop=None):
        self.calls.append("check")


def _run_daemon(stub, tmp_path, monkeypatch):
    monkeypatch.setattr(yfinance_cli, "yf_parqed", stub)
    # the daemon installs its own handlers; put the previous ones back afterwards
    previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        with patch("yf_parqed.yfinance_cli.GlobalRunLock") as lock:
            lock.return_value.try_acquire.return_value = True
            return CliRunner(env={"NO_COLOR": "1"}).invoke(
                yfinance_cli.app,
                ["--wrk-dir", str(tmp_path), "update-data", "--daemon"],
                catch_exceptions=False,
            )
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def test_daemon_checks_the_closed_month_after_the_cycle(
    tmp_path, stop_on_first_sleep, monkeypatch
):
    stub = _DaemonStub(tmp_path)

    result = _run_daemon(stub, tmp_path, monkeypatch)

    assert result.exit_code == 0
    assert stub.calls == ["cycle", "check"]


def test_daemon_survives_a_failing_check(tmp_path, stop_on_first_sleep, monkeypatch):
    stub = _DaemonStub(tmp_path)
    stub.check_last_closed_month = MagicMock(side_effect=OSError("disk gone"))

    result = _run_daemon(stub, tmp_path, monkeypatch)

    assert result.exit_code == 0
    assert stub.check_last_closed_month.call_count == 1


def test_daemon_does_not_check_after_a_cycle_that_failed(
    tmp_path, stop_on_first_sleep, monkeypatch
):
    """The tickers the cycle did not reach may still get bars of the closed month."""
    stub = _DaemonStub(tmp_path, fail_in_cycle=True)

    result = _run_daemon(stub, tmp_path, monkeypatch)

    assert result.exit_code == 0
    assert stub.calls == ["cycle"]


def test_daemon_does_not_start_the_check_when_asked_to_stop(tmp_path, monkeypatch):
    stub = _DaemonStub(tmp_path, stop_in_cycle=True)

    result = _run_daemon(stub, tmp_path, monkeypatch)

    assert result.exit_code == 0
    assert stub.calls == ["cycle"]


# --- J5: yf-parqed verify-partitions ----------------------------------------


@pytest.fixture()
def cli(yf, stored, tmp_path, monkeypatch):
    monkeypatch.setattr(yfinance_cli, "yf_parqed", yf)

    def run(*args: str):
        return CliRunner(env={"NO_COLOR": "1"}).invoke(
            yfinance_cli.app, ["--wrk-dir", str(tmp_path), "verify-partitions", *args]
        )

    return run


@freeze_time("2024-04-02 09:00:00")
def test_command_checks_the_last_closed_month_by_default(cli, tmp_path):
    result = cli()

    assert result.exit_code == 0
    assert "2024-03: 4 files, 7 rows, 0 damaged" in result.output
    assert "2024-02" not in result.output
    assert "2024-03" in json.loads((tmp_path / "partition_checks.json").read_text())


@freeze_time("2024-04-02 09:00:00")
def test_command_reports_damage_and_exits_with_1(cli, tmp_path):
    damaged = _partition(tmp_path, "AAPL", "2024-02")
    damaged.write_bytes(GARBAGE)

    result = cli("--month", "2024-02")

    assert result.exit_code == 1
    assert "2024-02: 3 files, 2 rows, 1 damaged" in result.output
    assert f"DAMAGED {damaged}" in result.output.replace("\n", "")
    assert damaged.read_bytes() == GARBAGE


@freeze_time("2024-04-02 09:00:00")
def test_command_checks_every_month_with_all(cli):
    result = cli("--all")

    assert result.exit_code == 0
    assert "2024-02: 3 files" in result.output
    assert "2024-03: 4 files" in result.output


@pytest.mark.parametrize(
    "args",
    [("--month", "2024-13"), ("--month", "March"), ("--month", "2024-03", "--all")],
)
def test_command_rejects_bad_arguments(cli, tmp_path, args):
    result = cli(*args)

    assert result.exit_code == 2
    assert not (tmp_path / "partition_checks.json").exists()
