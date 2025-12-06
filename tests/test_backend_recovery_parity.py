"""Shared test suite to validate recovery behavior across all storage backends.

This module ensures both StorageBackend and PartitionedStorageBackend implement
identical recovery and fail-safe behavior:
- Only delete truly corrupt/unreadable files
- Preserve files with schema mismatches
- Attempt safe column recovery before failing
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from yf_parqed.common.partition_path_builder import PartitionPathBuilder
from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.common.storage_backend import StorageBackend, StorageRequest


@pytest.fixture
def empty_frame_factory():
    """Factory for creating empty price DataFrames."""

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


@pytest.fixture
def normalizer():
    """Normalizer that ensures consistent column types."""

    def normalize(df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()

        # Ensure all required columns exist
        for col in [
            "stock",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "sequence",
        ]:
            if col not in normalized.columns:
                if col in {"open", "high", "low", "close"}:
                    normalized[col] = pd.Series(dtype="float64")
                elif col in {"volume", "sequence"}:
                    normalized[col] = pd.Series(dtype="Int64")
                elif col == "date":
                    normalized[col] = pd.Series(dtype="datetime64[ns]")
                else:
                    normalized[col] = pd.Series(dtype="string")

        # Normalize types
        normalized["stock"] = normalized["stock"].astype("string")
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")

        for price_col in ["open", "high", "low", "close"]:
            normalized[price_col] = pd.to_numeric(
                normalized[price_col], errors="coerce"
            ).astype("float64")

        for int_col in ["volume", "sequence"]:
            numeric_series = pd.to_numeric(normalized[int_col], errors="coerce")
            normalized[int_col] = numeric_series.round().astype("Int64")

        # Ensure column order
        normalized = normalized[
            ["stock", "date", "open", "high", "low", "close", "volume", "sequence"]
        ]
        return normalized

    return normalize


@pytest.fixture
def column_provider():
    """Provides list of required columns."""
    return lambda: [
        "stock",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sequence",
    ]


@pytest.fixture
def legacy_backend(empty_frame_factory, normalizer, column_provider):
    """Create a StorageBackend instance."""
    return StorageBackend(
        empty_frame_factory=empty_frame_factory,
        normalizer=normalizer,
        column_provider=column_provider,
    )


@pytest.fixture
def partitioned_backend(tmp_path, empty_frame_factory, normalizer, column_provider):
    """Create a PartitionedStorageBackend instance."""
    builder = PartitionPathBuilder(root=tmp_path)
    return PartitionedStorageBackend(
        empty_frame_factory=empty_frame_factory,
        normalizer=normalizer,
        column_provider=column_provider,
        path_builder=builder,
    )


def make_legacy_request(root: Path, ticker: str = "TEST") -> StorageRequest:
    """Create a legacy storage request."""
    return StorageRequest(root=root, interval="1d", ticker=ticker)


def make_partition_request(root: Path, ticker: str = "TEST") -> StorageRequest:
    """Create a partitioned storage request."""
    return StorageRequest(
        root=root,
        market="us",
        source="yahoo",
        dataset="stocks",
        interval="1d",
        ticker=ticker,
    )


class TestCorruptFileHandling:
    """Test that both backends delete truly corrupt files."""

    def test_legacy_backend_deletes_corrupt_file(
        self, tmp_path, legacy_backend, empty_frame_factory
    ):
        """Legacy backend should delete unreadable corrupt files."""
        request = make_legacy_request(tmp_path, ticker="CORRUPT")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create a corrupt file (not valid parquet)
        path.write_text("this is not a parquet file")

        result = legacy_backend.read(request)

        # Should return empty and delete corrupt file
        assert result.empty
        assert not path.exists()

    def test_partitioned_backend_deletes_corrupt_file(
        self, tmp_path, partitioned_backend, empty_frame_factory
    ):
        """Partitioned backend should delete unreadable corrupt files."""
        request = make_partition_request(tmp_path, ticker="CORRUPT")

        # First save valid data
        df = pd.DataFrame(
            {
                "stock": ["CORRUPT"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
                "sequence": [1],
            }
        ).set_index(["stock", "date"])

        partitioned_backend.save(request, df, empty_frame_factory())

        # Corrupt the partition file
        corrupt_path = (
            tmp_path
            / "us/yahoo/stocks_1d/ticker=CORRUPT/year=2024/month=01/data.parquet"
        )
        corrupt_path.write_text("this is not a parquet file")

        # Should fail with RuntimeError and delete corrupt file
        with pytest.raises(RuntimeError, match="Failed to read.*partition"):
            partitioned_backend.read(request)

        assert not corrupt_path.exists()


class TestSchemaMismatchPreservation:
    """Test that both backends preserve files with schema mismatches."""

    def test_legacy_backend_preserves_schema_mismatch(
        self, tmp_path, legacy_backend, empty_frame_factory
    ):
        """Legacy backend should preserve files with missing columns."""
        request = make_legacy_request(tmp_path, ticker="INCOMPLETE")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create parquet with incomplete schema
        df = pd.DataFrame(
            {
                "stock": ["INCOMPLETE"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                # Missing: high, low, close, volume, sequence
            }
        )
        df.to_parquet(path, index=False)

        result = legacy_backend.read(request)

        # Should return empty but PRESERVE file
        assert result.empty
        assert path.exists()

    def test_partitioned_backend_preserves_schema_mismatch(
        self, tmp_path, partitioned_backend, empty_frame_factory
    ):
        """Partitioned backend should preserve files with missing columns."""
        request = make_partition_request(tmp_path, ticker="INCOMPLETE")

        # First save valid data
        df = pd.DataFrame(
            {
                "stock": ["INCOMPLETE"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
                "sequence": [1],
            }
        ).set_index(["stock", "date"])

        partitioned_backend.save(request, df, empty_frame_factory())

        # Replace with incomplete schema
        bad_path = (
            tmp_path
            / "us/yahoo/stocks_1d/ticker=INCOMPLETE/year=2024/month=01/data.parquet"
        )
        bad_df = pd.DataFrame(
            {
                "stock": ["INCOMPLETE"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                # Missing: high, low, close, volume, sequence
            }
        )
        bad_df.to_parquet(bad_path, index=False)

        # Should fail with RuntimeError but PRESERVE file
        with pytest.raises(RuntimeError, match="Failed to read.*partition"):
            partitioned_backend.read(request)

        assert bad_path.exists()


class TestEmptyFileHandling:
    """Test that both backends preserve empty files."""

    def test_legacy_backend_preserves_empty_file(
        self, tmp_path, legacy_backend, empty_frame_factory
    ):
        """Legacy backend should preserve empty parquet files."""
        request = make_legacy_request(tmp_path, ticker="EMPTY")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create empty but valid parquet
        df = pd.DataFrame(
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
        )
        df.to_parquet(path, index=False)

        result = legacy_backend.read(request)

        # Should return empty but PRESERVE file
        assert result.empty
        assert path.exists()

    def test_partitioned_backend_preserves_empty_file(
        self, tmp_path, partitioned_backend, empty_frame_factory
    ):
        """Partitioned backend should preserve empty parquet files."""
        request = make_partition_request(tmp_path, ticker="EMPTY")

        # Create a partition directory with an empty file
        partition_path = tmp_path / "us/yahoo/stocks_1d/ticker=EMPTY/year=2024/month=01"
        partition_path.mkdir(parents=True, exist_ok=True)

        empty_file = partition_path / "data.parquet"
        df = pd.DataFrame(
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
        )
        df.to_parquet(empty_file, index=False)

        # Should fail with RuntimeError but PRESERVE file
        with pytest.raises(RuntimeError, match="Failed to read.*partition"):
            partitioned_backend.read(request)

        assert empty_file.exists()


class TestRecoveryStrategies:
    """Test that both backends attempt the same recovery strategies."""

    def test_legacy_backend_promotes_unnamed_index(
        self, tmp_path, legacy_backend, empty_frame_factory
    ):
        """Legacy backend should promote unnamed numeric index to sequence."""
        request = make_legacy_request(tmp_path, ticker="INDEX")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create DataFrame with unnamed index (will become 'index' column)
        df = pd.DataFrame(
            {
                "stock": ["INDEX"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
            }
        )
        df.index = pd.Index([5])
        df.reset_index().to_parquet(path, index=False)

        result = legacy_backend.read(request)

        # Should successfully recover
        assert not result.empty
        assert "sequence" in result.reset_index().columns

    def test_legacy_backend_promotes_index_column(
        self, tmp_path, legacy_backend, empty_frame_factory
    ):
        """Legacy backend should promote 'index' column to sequence."""
        request = make_legacy_request(tmp_path, ticker="INDEXCOL")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create DataFrame with 'index' column
        df = pd.DataFrame(
            {
                "index": [7],
                "stock": ["INDEXCOL"],
                "date": [datetime(2024, 1, 2)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
            }
        )
        df.to_parquet(path, index=False)

        result = legacy_backend.read(request)

        # Should successfully recover
        assert not result.empty
        assert "sequence" in result.reset_index().columns

    def test_legacy_backend_rejects_datetime_index(
        self, tmp_path, legacy_backend, empty_frame_factory
    ):
        """Legacy backend should NOT promote datetime index to sequence."""
        request = make_legacy_request(tmp_path, ticker="DATETIME")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create DataFrame with datetime index
        df = pd.DataFrame(
            {
                "stock": ["DATETIME"],
                "date": [datetime(2024, 1, 3)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
            }
        )
        df.index = pd.DatetimeIndex([datetime(2024, 1, 3)])
        df.reset_index().to_parquet(path, index=False)

        result = legacy_backend.read(request)

        # Should return empty (cannot recover) but PRESERVE file
        assert result.empty
        assert path.exists()

    def test_partitioned_backend_promotes_unnamed_index(
        self, tmp_path, partitioned_backend, empty_frame_factory
    ):
        """Partitioned backend should promote unnamed numeric index to sequence."""
        request = make_partition_request(tmp_path, ticker="INDEX")

        # Create partition directory
        partition_path = tmp_path / "us/yahoo/stocks_1d/ticker=INDEX/year=2024/month=01"
        partition_path.mkdir(parents=True, exist_ok=True)

        # Create DataFrame with unnamed index
        df = pd.DataFrame(
            {
                "stock": ["INDEX"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
            }
        )
        df.index = pd.Index([5])
        df.reset_index().to_parquet(partition_path / "data.parquet", index=False)

        result = partitioned_backend.read(request)

        # Should successfully recover
        assert not result.empty
        assert "sequence" in result.reset_index().columns

    def test_partitioned_backend_promotes_index_column(
        self, tmp_path, partitioned_backend, empty_frame_factory
    ):
        """Partitioned backend should promote 'index' column to sequence."""
        request = make_partition_request(tmp_path, ticker="INDEXCOL")

        # Create partition directory
        partition_path = (
            tmp_path / "us/yahoo/stocks_1d/ticker=INDEXCOL/year=2024/month=01"
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        # Create DataFrame with 'index' column
        df = pd.DataFrame(
            {
                "index": [7],
                "stock": ["INDEXCOL"],
                "date": [datetime(2024, 1, 2)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
            }
        )
        df.to_parquet(partition_path / "data.parquet", index=False)

        result = partitioned_backend.read(request)

        # Should successfully recover
        assert not result.empty
        assert "sequence" in result.reset_index().columns

    def test_partitioned_backend_rejects_datetime_index(
        self, tmp_path, partitioned_backend, empty_frame_factory
    ):
        """Partitioned backend should NOT promote datetime index to sequence."""
        request = make_partition_request(tmp_path, ticker="DATETIME")

        # Create partition directory
        partition_path = (
            tmp_path / "us/yahoo/stocks_1d/ticker=DATETIME/year=2024/month=01"
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        # Create DataFrame with datetime index
        df = pd.DataFrame(
            {
                "stock": ["DATETIME"],
                "date": [datetime(2024, 1, 3)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
            }
        )
        df.index = pd.DatetimeIndex([datetime(2024, 1, 3)])
        partition_file = partition_path / "data.parquet"
        df.reset_index().to_parquet(partition_file, index=False)

        # Should fail but PRESERVE file
        with pytest.raises(RuntimeError, match="Failed to read.*partition"):
            partitioned_backend.read(request)

        assert partition_file.exists()
