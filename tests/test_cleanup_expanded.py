from pathlib import Path
import os

import pandas as pd

from yf_parqed.common.run_lock import GlobalRunLock
from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.common.partition_path_builder import PartitionPathBuilder
from yf_parqed.common.storage_backend import StorageRequest


def test_cleanup_recovers_tmp_when_final_missing(tmp_path: Path):
    data_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=CCC/year=2024/month=04"
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = data_dir / f"data.parquet.tmp-{os.getpid()}-recover"
    pd.DataFrame({"stock": ["CCC"], "open": [1.0]}).to_parquet(tmp_file, index=False)
    written = tmp_file.read_bytes()

    lock = GlobalRunLock(tmp_path)
    processed = lock.cleanup_tmp_files()
    assert processed >= 1
    final = data_dir / "data.parquet"
    assert final.exists()
    assert final.read_bytes() == written


def test_cleanup_keeps_an_incomplete_tmp_but_does_not_promote_it(tmp_path: Path):
    """A write that was cut short must not become the month's data file, nor be deleted."""
    data_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=CCC/year=2024/month=04"
    data_dir.mkdir(parents=True, exist_ok=True)
    whole = data_dir / "whole.parquet"
    pd.DataFrame({"stock": ["CCC"] * 50, "open": [1.0] * 50}).to_parquet(whole)
    tmp_file = data_dir / f"data.parquet.tmp-{os.getpid()}-cut"
    cut_short = whole.read_bytes()[:-20]
    tmp_file.write_bytes(cut_short)
    # what a quarantined data file leaves behind: it must be left alone
    moved_aside = data_dir / "data.parquet.damaged-20240401T000000Z"
    moved_aside.write_text("kept")
    lock = GlobalRunLock(tmp_path)

    processed = lock.cleanup_tmp_files()

    assert processed == 1
    assert not tmp_file.exists()
    assert not (data_dir / "data.parquet").exists()
    assert moved_aside.read_text() == "kept"
    (kept,) = data_dir.glob(f"{tmp_file.name}.damaged-*")
    assert kept.read_bytes() == cut_short

    # a second cleanup leaves the kept file alone
    assert lock.cleanup_tmp_files() == 0
    assert kept.read_bytes() == cut_short


def test_cleanup_does_not_judge_a_tmp_when_memory_runs_out(tmp_path: Path, monkeypatch):
    data_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=CCC/year=2024/month=04"
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = data_dir / f"data.parquet.tmp-{os.getpid()}-whole"
    pd.DataFrame({"stock": ["CCC"], "open": [1.0]}).to_parquet(tmp_file, index=False)

    def out_of_memory(path):
        raise MemoryError()

    monkeypatch.setattr("yf_parqed.common.run_lock.read_completely", out_of_memory)

    GlobalRunLock(tmp_path).cleanup_tmp_files()

    # the loop logs the error and goes on; the file is neither promoted nor renamed
    assert [p.name for p in data_dir.iterdir()] == [tmp_file.name]


def test_cleanup_removes_tmp_when_final_present(tmp_path: Path):
    data_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=CCC/year=2024/month=04"
    data_dir.mkdir(parents=True, exist_ok=True)
    final = data_dir / "data.parquet"
    final.write_text("final-content")
    tmp_file = data_dir / f"data.parquet.tmp-{os.getpid()}-remove"
    tmp_file.write_text("tmp-content")

    lock = GlobalRunLock(tmp_path)
    processed = lock.cleanup_tmp_files()
    assert processed >= 1
    assert final.exists()
    assert final.read_text() == "final-content"
    assert not tmp_file.exists()


def test_fsync_failure_during_partition_write(tmp_path: Path, monkeypatch):
    # Simulate os.fsync() raising during backend.save; ensure final exists and no tmp remain
    def empty_frame():
        return (
            pd.DataFrame(
                {
                    "stock": pd.Series(dtype="string"),
                    "date": pd.Series(dtype="datetime64[ns]"),
                    "sequence": pd.Series(dtype="int64"),
                }
            ).set_index(["stock", "date"])  # type: ignore
        )

    def normalizer(df):
        return df

    backend = PartitionedStorageBackend(
        empty_frame_factory=empty_frame,
        normalizer=normalizer,
        column_provider=lambda: ["stock", "date"],
        path_builder=PartitionPathBuilder(root=tmp_path / "data"),
    )

    df = pd.DataFrame(
        {"stock": ["CCC"], "date": [pd.Timestamp("2024-04-01")], "open": [1.0]}
    )
    df["sequence"] = 0

    # Make os.fsync raise
    def raise_fsync(fd):
        raise OSError("simulated fsync failure")

    monkeypatch.setattr("os.fsync", raise_fsync)

    request = StorageRequest(
        root=tmp_path / "data",
        market="US",
        source="yahoo",
        dataset="stocks",
        interval="1d",
        ticker="CCC",
    )

    # Should not raise: backend handles fsync failures (best-effort) and still writes
    backend.save(request, df, empty_frame())

    # Ensure no tmp files remain and final exists
    tmp_files = list((tmp_path / "data").rglob("data.parquet.tmp-*"))
    assert not tmp_files
    final = (
        tmp_path
        / "data"
        / "us"
        / "yahoo"
        / "stocks_1d"
        / "ticker=CCC"
        / "year=2024"
        / "month=04"
        / "data.parquet"
    )
    assert final.exists()
