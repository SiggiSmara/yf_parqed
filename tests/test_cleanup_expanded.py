from pathlib import Path
import os

import pandas as pd

from yf_parqed.run_lock import GlobalRunLock
from yf_parqed.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.partition_path_builder import PartitionPathBuilder
from yf_parqed.storage_backend import StorageRequest


def test_cleanup_recovers_tmp_when_final_missing(tmp_path: Path):
    data_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=CCC/year=2024/month=04"
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = data_dir / f"data.parquet.tmp-{os.getpid()}-recover"
    tmp_file.write_text("recovered")

    lock = GlobalRunLock(tmp_path)
    processed = lock.cleanup_tmp_files()
    assert processed >= 1
    final = data_dir / "data.parquet"
    assert final.exists()
    assert final.read_text() == "recovered"


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
