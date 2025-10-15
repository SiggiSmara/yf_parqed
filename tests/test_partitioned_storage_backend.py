from pathlib import Path

import pandas as pd
import pytest

from yf_parqed.partition_path_builder import PartitionPathBuilder
from yf_parqed.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.storage_backend import StorageRequest


@pytest.fixture()
def empty_frame():
    def factory():
        return pd.DataFrame(
            {
                "stock": pd.Series(dtype="string"),
                "date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="Int64"),
                "sequence": pd.Series(dtype="Int64"),
            }
        ).set_index(["stock", "date"])

    return factory


@pytest.fixture()
def normalizer():
    def normalize(df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = [
            "stock",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "sequence",
        ]
        normalized = df.copy()
        for column in expected_cols:
            if column not in normalized.columns:
                if column in {"open", "high", "low", "close"}:
                    normalized[column] = pd.Series(dtype="float64")
                elif column in {"volume", "sequence"}:
                    normalized[column] = pd.Series(dtype="Int64")
                elif column == "date":
                    normalized[column] = pd.Series(dtype="datetime64[ns]")
                else:
                    normalized[column] = pd.Series(dtype="string")
        normalized["stock"] = normalized["stock"].astype("string")
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
        for price_col in ["open", "high", "low", "close"]:
            normalized[price_col] = pd.to_numeric(
                normalized[price_col], errors="coerce"
            ).astype("float64")
        for int_col in ["volume", "sequence"]:
            normalized[int_col] = (
                pd.to_numeric(normalized[int_col], errors="coerce")
                .round()
                .astype("Int64")
            )
        return normalized[expected_cols]

    return normalize


@pytest.fixture()
def columns():
    return [
        "stock",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sequence",
    ]


@pytest.fixture()
def backend(tmp_path: Path, empty_frame, normalizer, columns):
    builder = PartitionPathBuilder(root=tmp_path)
    return PartitionedStorageBackend(
        empty_frame_factory=empty_frame,
        normalizer=normalizer,
        column_provider=lambda: columns,
        path_builder=builder,
    )


def make_request(root: Path, interval: str = "1d") -> StorageRequest:
    return StorageRequest(
        root=root,
        market="us",
        source="yahoo",
        dataset="stocks",
        interval=interval,
        ticker="AAPL",
    )


def make_sample_df(dates: list[str]) -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(
        [("AAPL", pd.Timestamp(date)) for date in dates], names=["stock", "date"]
    )
    return pd.DataFrame(
        {
            "open": [100.0 + i for i in range(len(dates))],
            "high": [101.0 + i for i in range(len(dates))],
            "low": [99.0 + i for i in range(len(dates))],
            "close": [100.5 + i for i in range(len(dates))],
            "volume": [1_000 + i for i in range(len(dates))],
            "sequence": [i for i in range(len(dates))],
        },
        index=index,
    )


def test_save_requires_market_and_source(backend, tmp_path, empty_frame):
    request = StorageRequest(
        root=tmp_path,
        market=None,
        source=None,
        dataset="stocks",
        interval="1d",
        ticker="AAPL",
    )
    df = make_sample_df(["2024-01-05"])
    with pytest.raises(ValueError):
        backend.save(request, df, empty_frame())


def test_save_writes_partition_files(backend, tmp_path, empty_frame):
    df = make_sample_df(["2024-01-05", "2024-01-06"])
    request = make_request(tmp_path)

    backend.save(request, df, empty_frame())

    base = tmp_path / "us/yahoo/stocks_1d/ticker=AAPL"
    first = base / "year=2024/month=01/day=05/data.parquet"
    second = base / "year=2024/month=01/day=06/data.parquet"
    assert first.exists()
    assert second.exists()

    reloaded = backend.read(request)
    assert not reloaded.empty
    assert len(reloaded) == 2
    assert reloaded.index.get_level_values("date").min() == pd.Timestamp("2024-01-05")


def test_read_returns_empty_when_no_partitions(backend, tmp_path):
    request = make_request(tmp_path)
    result = backend.read(request)
    assert result.empty


def test_read_removes_corrupt_partition_and_fails(backend, tmp_path, empty_frame):
    request = make_request(tmp_path)
    df = make_sample_df(["2024-01-05"])
    backend.save(request, df, empty_frame())

    corrupt_path = (
        tmp_path
        / "us/yahoo/stocks_1d/ticker=AAPL/year=2024/month=01/day=05/data.parquet"
    )
    corrupt_path.write_text("not parquet")

    with pytest.raises(RuntimeError):
        backend.read(request)

    assert not corrupt_path.exists()
