import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

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
def storage(empty_frame_factory, normalizer, column_provider):
    """Create a StorageBackend instance with test dependencies."""
    return StorageBackend(
        empty_frame_factory=empty_frame_factory,
        normalizer=normalizer,
        column_provider=column_provider,
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def make_request(
    root: Path, ticker: str = "AAPL", interval: str = "1d"
) -> StorageRequest:
    return StorageRequest(root=root, interval=interval, ticker=ticker)


class TestStorageBackendRead:
    """Test StorageBackend read operations."""

    def test_read_returns_empty_when_file_missing(self, storage, temp_dir):
        """read() should return empty DataFrame when file doesn't exist."""
        request = make_request(temp_dir, ticker="MISSING")
        result = storage.read(request)

        assert result.empty
        assert list(result.index.names) == ["stock", "date"]

    def test_read_loads_valid_parquet(self, storage, temp_dir):
        """read() should successfully load a valid parquet file."""
        request = make_request(temp_dir, ticker="VALID")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(
            {
                "stock": ["TEST"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
                "sequence": [1],
            }
        )
        df.to_parquet(path, index=False)

        result = storage.read(request)

        assert not result.empty
        assert len(result) == 1
        assert result.index.names == ["stock", "date"]
        assert result.loc[("TEST", pd.Timestamp("2024-01-01")), "close"] == 100.5

    def test_read_deletes_corrupt_file(self, storage, temp_dir):
        """read() should delete corrupt parquet files."""
        request = make_request(temp_dir, ticker="CORRUPT")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create a corrupt file
        path.write_text("not a parquet file")

        result = storage.read(request)

        assert result.empty
        assert not path.exists()

    def test_read_deletes_file_with_missing_columns(self, storage, temp_dir):
        """read() should preserve files with missing columns for operator inspection."""
        request = make_request(temp_dir, ticker="INCOMPLETE")
        path = request.legacy_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Create parquet with incomplete schema
        df = pd.DataFrame(
            {
                "stock": ["BAD"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                # Missing other required columns
            }
        )
        df.to_parquet(path, index=False)

        result = storage.read(request)

        assert result.empty
        # File should be PRESERVED for operator inspection (not deleted)
        assert path.exists()


class TestStorageBackendSave:
    """Test StorageBackend save operations."""

    def test_save_returns_empty_when_both_empty(self, storage, temp_dir):
        """save() should return empty DataFrame when both inputs are empty."""
        empty_df = storage._empty_frame_factory()
        request = make_request(temp_dir, ticker="EMPTY")

        result = storage.save(request, empty_df, empty_df)

        assert result.empty
        assert not request.legacy_path().exists()

    def test_save_returns_existing_when_new_empty(self, storage, temp_dir):
        """save() should return existing data when new data is empty."""
        empty_df = storage._empty_frame_factory()
        existing_df = pd.DataFrame(
            {
                "stock": ["KEEP"],
                "date": [datetime(2024, 1, 1)],
                "open": [50.0],
                "high": [51.0],
                "low": [49.0],
                "close": [50.5],
                "volume": [500],
                "sequence": [1],
            }
        ).set_index(["stock", "date"])

        request = make_request(temp_dir, ticker="KEEP")

        result = storage.save(request, empty_df, existing_df)

        assert len(result) == 1
        assert result.loc[("KEEP", pd.Timestamp("2024-01-01")), "close"] == 50.5

    def test_save_merges_and_deduplicates(self, storage, temp_dir):
        """save() should merge new and existing data, keeping latest for duplicates."""
        existing_df = pd.DataFrame(
            {
                "stock": ["MERGE"],
                "date": [datetime(2024, 1, 1)],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.0],
                "volume": [1000],
                "sequence": [1],
            }
        ).set_index(["stock", "date"])

        new_df = pd.DataFrame(
            {
                "stock": ["MERGE", "MERGE"],
                "date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
                "open": [10.0, 11.0],
                "high": [12.0, 13.0],
                "low": [9.0, 10.0],
                "close": [11.0, 12.0],
                "volume": [1100, 1200],
                "sequence": [2, 3],
            }
        ).set_index(["stock", "date"])

        request = make_request(temp_dir, ticker="MERGE")

        result = storage.save(request, new_df, existing_df)

        # Should have 2 rows: updated Jan 1 and new Jan 2
        assert len(result) == 2
        # Jan 1 should have the newer data (sequence 2)
        assert result.loc[("MERGE", pd.Timestamp("2024-01-01")), "close"] == 11.0
        assert result.loc[("MERGE", pd.Timestamp("2024-01-01")), "sequence"] == 2
        # Jan 2 should exist
        assert result.loc[("MERGE", pd.Timestamp("2024-01-02")), "close"] == 12.0

    def test_save_persists_to_parquet(self, storage, temp_dir):
        """save() should write data to parquet file."""
        new_df = pd.DataFrame(
            {
                "stock": ["SAVE"],
                "date": [datetime(2024, 1, 1)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000],
                "sequence": [1],
            }
        ).set_index(["stock", "date"])

        request = make_request(temp_dir, ticker="SAVE")
        empty_df = storage._empty_frame_factory()

        storage.save(request, new_df, empty_df)

        # Verify file was created
        path = request.legacy_path()
        assert path.exists()

        # Verify can be read back
        reloaded = pd.read_parquet(path)
        assert len(reloaded) == 1
        assert reloaded.loc[0, "stock"] == "SAVE"

    def test_save_preserves_higher_sequence_values(self, storage, temp_dir):
        """save() should keep row with highest sequence when deduplicating."""
        existing_df = pd.DataFrame(
            {
                "stock": ["SEQ"],
                "date": [datetime(2024, 1, 1)],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.0],
                "volume": [1000],
                "sequence": [5],  # Higher sequence
            }
        ).set_index(["stock", "date"])

        new_df = pd.DataFrame(
            {
                "stock": ["SEQ"],
                "date": [datetime(2024, 1, 1)],
                "open": [20.0],
                "high": [21.0],
                "low": [19.0],
                "close": [20.0],
                "volume": [2000],
                "sequence": [1],  # Lower sequence
            }
        ).set_index(["stock", "date"])

        request = make_request(temp_dir, ticker="SEQ")

        result = storage.save(request, new_df, existing_df)

        # Should keep the higher sequence value (5)
        assert len(result) == 1
        assert result.loc[("SEQ", pd.Timestamp("2024-01-01")), "sequence"] == 5
        assert result.loc[("SEQ", pd.Timestamp("2024-01-01")), "close"] == 10.0


class TestStorageBackendEdgeCases:
    """Test edge cases and error handling."""

    def test_handles_empty_dataframe_with_schema(self, storage, temp_dir):
        """Storage should handle empty DataFrames that have correct schema."""
        request = make_request(temp_dir, ticker="EMPTY_SCHEMA")
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

        result = storage.read(request)

        # Should handle gracefully and delete
        assert result.empty

    def test_normalizer_called_during_save(self, storage, temp_dir):
        """save() should normalize data types during merge."""
        new_df = pd.DataFrame(
            {
                "stock": ["NORM"],
                "date": [datetime(2024, 1, 1)],
                "open": ["100.5"],  # String instead of float
                "high": [101],  # Int instead of float
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.5],  # Float instead of Int
                "sequence": ["1"],  # String instead of Int
            }
        ).set_index(["stock", "date"])

        empty_df = storage._empty_frame_factory()
        request = make_request(temp_dir, ticker="NORM")

        result = storage.save(request, new_df, empty_df)

        # Types should be normalized
        assert result.loc[("NORM", pd.Timestamp("2024-01-01")), "open"] == 100.5
        assert isinstance(
            result.loc[("NORM", pd.Timestamp("2024-01-01")), "open"], float
        )
        assert result.loc[("NORM", pd.Timestamp("2024-01-01")), "volume"] == 1000
