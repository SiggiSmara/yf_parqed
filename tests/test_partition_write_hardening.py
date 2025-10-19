from pathlib import Path

import pandas as pd

from yf_parqed.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.partition_path_builder import PartitionPathBuilder
from yf_parqed.storage_backend import StorageRequest


def test_month_start_not_persisted(tmp_path: Path):
    # Setup backend with data root under tmp_path/data
    data_root = tmp_path / "data"

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
        path_builder=PartitionPathBuilder(root=data_root),
    )

    df = pd.DataFrame(
        {"stock": ["DDD"], "date": [pd.Timestamp("2024-06-02")], "open": [2.0]}
    )
    df["sequence"] = 0

    request = StorageRequest(
        root=data_root,
        market="US",
        source="yahoo",
        dataset="stocks",
        interval="1d",
        ticker="DDD",
    )

    # Save — this will write a partition file under data_root
    backend.save(request, df, empty_frame())

    # Locate final file and read back
    final = (
        data_root
        / "us"
        / "yahoo"
        / "stocks_1d"
        / "ticker=DDD"
        / "year=2024"
        / "month=06"
        / "data.parquet"
    )
    assert final.exists()
    read_df = pd.read_parquet(final)
    # month_start should not be persisted
    assert "month_start" not in read_df.columns
