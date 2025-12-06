import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from yf_parqed.yahoo.primary_class import YFParqed


class TestStorageOperations:
    """Tests focusing on parquet persistence helpers."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def create_instance(self):
        return YFParqed(my_path=self.temp_dir, my_intervals=["1d"])

    def test_save_yf_merges_and_deduplicates_rows(self):
        """save_yf replaces duplicate rows with the newest values and preserves distinct dates."""
        yf_parqed = self.create_instance()
        data_path = self.temp_dir / "stocks_1d" / "MERGE.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)

        base_df = pd.DataFrame(
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

        result = yf_parqed.save_yf(new_df, base_df, data_path)

        assert data_path.exists()
        assert set(result.index.get_level_values("date")) == {
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
        }

        merged_close = result.loc[("MERGE", datetime(2024, 1, 1)), "close"]
        merged_volume = result.loc[("MERGE", datetime(2024, 1, 1)), "volume"]
        assert merged_close == 11.0
        assert merged_volume == 1100

    def test_save_yf_handles_empty_existing_dataframe(self):
        """save_yf writes new rows when no prior data exists."""
        yf_parqed = self.create_instance()
        data_path = self.temp_dir / "stocks_1d" / "FRESH.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)

        empty_existing = pd.DataFrame(
            {
                "stock": pd.Series(dtype=str),
                "date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="int64"),
                "sequence": pd.Series(dtype="int64"),
            }
        ).set_index(["stock", "date"])

        new_rows = pd.DataFrame(
            {
                "stock": ["FRESH"],
                "date": [datetime(2024, 2, 1)],
                "open": [20.0],
                "high": [22.0],
                "low": [19.0],
                "close": [21.0],
                "volume": [1500],
                "sequence": [1],
            }
        ).set_index(["stock", "date"])

        result = yf_parqed.save_yf(
            new_rows,
            empty_existing,
            data_path,
        )

        assert result.index.tolist() == [("FRESH", datetime(2024, 2, 1))]
        close_value = result.loc[("FRESH", datetime(2024, 2, 1)), "close"]
        assert close_value == 21.0

    def test_read_yf_returns_empty_structure_when_file_missing(self):
        """read_yf should provide the expected empty schema if parquet file is absent."""
        yf_parqed = self.create_instance()
        data_path = self.temp_dir / "stocks_1d" / "MISSING.parquet"

        df = yf_parqed.read_yf(data_path)
        assert df.empty
        assert df.index.names == ["stock", "date"]
        assert list(df.columns) == [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "sequence",
        ]

    def test_read_yf_deletes_corrupt_empty_file(self):
        """read_yf removes empty parquet files and returns clean structure."""
        yf_parqed = self.create_instance()
        data_path = self.temp_dir / "stocks_1d" / "CORRUPT.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)

        empty_df = pd.DataFrame(
            {
                "stock": pd.Series(dtype=str),
                "date": pd.Series(dtype="datetime64[ns]"),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="int64"),
                "sequence": pd.Series(dtype="int64"),
            }
        )
        empty_df.to_parquet(data_path, index=False)

        df = yf_parqed.read_yf(data_path)

        assert df.empty
        # Empty files are now PRESERVED for operator inspection (not deleted)
        assert data_path.exists()

    def test_save_yf_preserves_higher_sequence_values(self):
        """When new rows provide lower sequence numbers, keep the highest-sequence snapshot."""
        yf_parqed = self.create_instance()
        data_path = self.temp_dir / "stocks_1d" / "SEQ.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)

        existing_df = pd.DataFrame(
            {
                "stock": ["SEQ"],
                "date": [datetime(2024, 3, 1)],
                "open": [30.0],
                "high": [31.0],
                "low": [29.5],
                "close": [30.5],
                "volume": [2500],
                "sequence": [9],
            }
        ).set_index(["stock", "date"])

        stale_update = pd.DataFrame(
            {
                "stock": ["SEQ"],
                "date": [datetime(2024, 3, 1)],
                "open": [28.0],
                "high": [29.0],
                "low": [27.0],
                "close": [28.5],
                "volume": [2000],
                "sequence": [4],
            }
        ).set_index(["stock", "date"])

        result = yf_parqed.save_yf(stale_update, existing_df, data_path)

        latest_sequence = result.loc[("SEQ", datetime(2024, 3, 1)), "sequence"]
        latest_close = result.loc[("SEQ", datetime(2024, 3, 1)), "close"]
        assert latest_sequence == 9
        assert latest_close == 30.5

    def test_read_yf_resets_partial_files_missing_columns(self):
        """Files missing required columns are treated as corrupt and replaced with an empty frame."""
        yf_parqed = self.create_instance()
        data_path = self.temp_dir / "stocks_1d" / "PARTIAL.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)

        partial_df = pd.DataFrame(
            {
                "stock": ["PARTIAL"],
                "date": [datetime(2024, 4, 1)],
                "close": [15.0],
            }
        )
        partial_df.to_parquet(data_path, index=False)
        partial_df.to_parquet(data_path, index=False)

        df = yf_parqed.read_yf(data_path)

        assert df.empty
        # Preserve partial files that are readable but missing columns so operators
        # can inspect and remediate them. Only unreadable/corrupt files are deleted.
        assert data_path.exists()

    def test_save_yf_normalizes_numeric_types(self):
        """save_yf coerces numeric columns to stable dtypes to avoid drift between writes."""
        yf_parqed = self.create_instance()
        data_path = self.temp_dir / "stocks_1d" / "DTYPE.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)

        existing_df = pd.DataFrame(
            {
                "stock": ["DTYPE"],
                "date": [datetime(2024, 5, 1)],
                "open": [40.0],
                "high": [41.0],
                "low": [39.5],
                "close": [40.5],
                "volume": [3000],
                "sequence": [5],
            }
        ).set_index(["stock", "date"])

        drifted_df = pd.DataFrame(
            {
                "stock": ["DTYPE"],
                "date": [datetime(2024, 5, 2)],
                "open": ["42.0"],
                "high": ["43.0"],
                "low": ["41.5"],
                "close": ["42.5"],
                "volume": ["3100"],
                "sequence": ["6"],
            }
        ).set_index(["stock", "date"])

        result = yf_parqed.save_yf(drifted_df, existing_df, data_path)

        assert str(result.dtypes["open"]) == "float64"
        assert str(result.dtypes["volume"]) == "Int64"
        assert str(result.dtypes["sequence"]) == "Int64"
