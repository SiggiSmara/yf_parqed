from pathlib import Path
import os

import pandas as pd

from yf_parqed.common.run_lock import GlobalRunLock


def _create_tmp_parquet(tmp_dir: Path):
    # create a valid parquet file under a ticker partition and then a tmp variant
    partition_dir = tmp_dir / "data/us/yahoo/stocks_1d/ticker=AAA/year=2024/month=01"
    partition_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {"stock": ["AAA"], "date": [pd.Timestamp("2024-01-01")], "open": [1.0]}
    )
    final = partition_dir / "data.parquet"
    df.to_parquet(final, index=False)
    tmp = partition_dir / f"data.parquet.tmp-{os.getpid()}-1"
    df.to_parquet(tmp, index=False)
    return tmp, final


def test_lock_acquire_and_release(tmp_path: Path):
    lock = GlobalRunLock(tmp_path)
    assert lock.try_acquire() is True
    info = lock.owner_info()
    assert isinstance(info, dict)
    lock.release()
    # lock dir should be removed
    assert not (tmp_path / ".run_lock").exists()


def test_cleanup_tmp_files(tmp_path: Path):
    # create a tmp file with no final then run cleanup
    partition_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=AAA/year=2024/month=02"
    partition_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {"stock": ["AAA"], "date": [pd.Timestamp("2024-02-01")], "open": [2.0]}
    )
    tmp = partition_dir / f"data.parquet.tmp-{os.getpid()}-2"
    df.to_parquet(tmp, index=False)

    lock = GlobalRunLock(tmp_path)
    processed = lock.cleanup_tmp_files()
    assert processed >= 1
    final = partition_dir / "data.parquet"
    assert final.exists()


def test_partial_write_failure_and_recovery(tmp_path: Path, monkeypatch):
    # Setup partition with existing final file
    partition_dir = tmp_path / "data/us/yahoo/stocks_1d/ticker=BBB/year=2024/month=05"
    partition_dir.mkdir(parents=True, exist_ok=True)
    final = partition_dir / "data.parquet"
    final.write_text("final-content")

    # Our simulated write should create a tmp file then raise
    created_tmp = None

    def fake_to_parquet(self, path, index=True, compression=None):
        nonlocal created_tmp
        # write a tmp file path-like
        tmp_path = Path(path)
        tmp_path.write_text("incomplete")
        created_tmp = tmp_path
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr("pandas.DataFrame.to_parquet", fake_to_parquet)

    # Call the backend write logic via PartitionedStorageBackend.save
    from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend
    from yf_parqed.common.partition_path_builder import PartitionPathBuilder
    from yf_parqed.common.storage_backend import StorageRequest

    def empty_frame():
        import pandas as pd

        # provide both 'stock' and 'date' columns so set_index([...]) succeeds
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
        # path builder should use the same data root as the StorageRequest
        path_builder=PartitionPathBuilder(root=tmp_path / "data"),
    )

    # Create a DataFrame target for same ticker/month
    import pandas as pd

    df = pd.DataFrame(
        {"stock": ["BBB"], "date": [pd.Timestamp("2024-05-01")], "open": [1.0]}
    )
    df["sequence"] = 0

    request = StorageRequest(
        root=tmp_path / "data",
        market="US",
        source="yahoo",
        dataset="stocks",
        interval="1d",
        ticker="BBB",
    )

    # existing_data empty

    try:
        backend.save(request, df, empty_frame())
    except RuntimeError:
        # write failed as expected
        pass

    # Original final should still exist unchanged
    assert final.exists()
    assert final.read_text() == "final-content"

    # There should be a tmp file created; cleanup should recover or remove it
    lock = GlobalRunLock(tmp_path)
    processed = lock.cleanup_tmp_files()
    assert processed >= 1
    # After cleanup, ensure no tmp files remain
    tmp_files = list((tmp_path / "data").rglob("data.parquet.tmp-*"))
    assert not tmp_files
