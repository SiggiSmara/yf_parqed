"""
Tests for Xetra incremental storage and monthly consolidation.

Tests cover:
1. Incremental per-file storage (interrupt safety)
2. Monthly consolidation from daily files
3. CLI commands for checking partial downloads and consolidating
4. Auto-detection of years/months from stored data
"""

import pandas as pd
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from src.yf_parqed.xetra.xetra_service import XetraService
from src.yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend
from src.yf_parqed.common.partition_path_builder import PartitionPathBuilder


@pytest.fixture
def temp_root(tmp_path):
    """Create temporary root directory for test data."""
    return tmp_path / "data"


@pytest.fixture
def service(temp_root):
    """Create XetraService with temp storage."""
    path_builder = PartitionPathBuilder(root=temp_root)
    backend = PartitionedStorageBackend(
        empty_frame_factory=lambda: pd.DataFrame(),
        normalizer=lambda df: df,
        column_provider=lambda: [],
        path_builder=path_builder,
    )

    # Mock fetcher and parser
    fetcher = Mock()
    parser = Mock()

    return XetraService(
        fetcher=fetcher,
        parser=parser,
        backend=backend,
        root_path=temp_root,
    )


@pytest.fixture
def sample_trades_df():
    """Create sample trades DataFrame matching Xetra schema."""
    return pd.DataFrame(
        {
            "isin": ["DE0005140008", "DE0008469008", "DE0005140008"],
            "mnemonic": ["DBK", "VOW3", "DBK"],
            "time": pd.to_datetime(
                ["2025-11-04 09:00:00", "2025-11-04 09:00:01", "2025-11-04 09:00:02"]
            ),
            "price": [12.50, 105.30, 12.51],
            "volume": [100, 50, 200],
        }
    )


class TestIncrementalStorage:
    """Test per-file incremental storage (interrupt safety)."""

    def test_store_trades_creates_daily_file(
        self, service, temp_root, sample_trades_df
    ):
        """Test that store_trades creates file in daily partition structure."""
        venue = "DETR"
        trade_date = datetime(2025, 11, 4)

        service.store_trades(sample_trades_df, venue, trade_date)

        # Check file exists in expected location
        expected_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / "year=2025"
            / "month=11"
            / "day=04"
            / "trades.parquet"
        )
        assert expected_path.exists()

        # Verify data
        stored_df = pd.read_parquet(expected_path)
        assert len(stored_df) == 3
        assert list(stored_df["isin"]) == [
            "DE0005140008",
            "DE0008469008",
            "DE0005140008",
        ]

    def test_multiple_stores_to_same_date_merge(
        self, service, temp_root, sample_trades_df
    ):
        """Test that multiple stores to same date append/merge data."""
        venue = "DETR"
        trade_date = datetime(2025, 11, 4)

        # Store first batch
        service.store_trades(sample_trades_df, venue, trade_date)

        # Store second batch
        more_trades = pd.DataFrame(
            {
                "isin": ["DE0007164600"],
                "mnemonic": ["SAP"],
                "time": pd.to_datetime(["2025-11-04 09:00:03"]),
                "price": [150.20],
                "volume": [300],
            }
        )
        service.store_trades(more_trades, venue, trade_date)

        # Check merged data
        expected_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / "year=2025"
            / "month=11"
            / "day=04"
            / "trades.parquet"
        )
        stored_df = pd.read_parquet(expected_path)
        assert len(stored_df) == 4  # 3 + 1

    def test_empty_dataframe_does_not_create_file(self, service, temp_root):
        """Test that empty DataFrame doesn't create files."""
        venue = "DETR"
        trade_date = datetime(2025, 11, 4)
        empty_df = pd.DataFrame()

        service.store_trades(empty_df, venue, trade_date)

        # File should not exist
        expected_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / "year=2025"
            / "month=11"
            / "day=04"
            / "trades.parquet"
        )
        assert not expected_path.exists()


class TestMonthlyConsolidation:
    """Test monthly consolidation from daily files."""

    def test_consolidate_month_combines_daily_files(
        self, service, temp_root, sample_trades_df
    ):
        """Test that consolidation combines multiple daily files into one monthly file."""
        venue = "DETR"
        year = 2025
        month = 11

        # Create multiple daily files
        for day in [4, 5, 6]:
            trade_date = datetime(year, month, day)
            df = sample_trades_df.copy()
            df["time"] = pd.to_datetime(f"{year}-{month:02d}-{day:02d} 09:00:00")
            service.store_trades(df, venue, trade_date)

        # Consolidate
        service._consolidate_to_monthly(venue, year, month)

        # Check monthly file exists
        monthly_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades_monthly"
            / "venue=DETR"
            / f"year={year}"
            / f"month={month:02d}"
            / "trades.parquet"
        )
        assert monthly_path.exists()

        # Verify combined data
        monthly_df = pd.read_parquet(monthly_path)
        assert len(monthly_df) == 9  # 3 trades × 3 days

    def test_consolidate_sorts_by_time(self, service, temp_root, sample_trades_df):
        """Test that consolidated data is sorted by time."""
        venue = "DETR"
        year = 2025
        month = 11

        # Create daily files in reverse order
        for day in [6, 5, 4]:
            trade_date = datetime(year, month, day)
            df = sample_trades_df.copy()
            df["time"] = pd.to_datetime(f"{year}-{month:02d}-{day:02d} 09:00:00")
            service.store_trades(df, venue, trade_date)

        # Consolidate
        service._consolidate_to_monthly(venue, year, month)

        # Check sorting
        monthly_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades_monthly"
            / "venue=DETR"
            / f"year={year}"
            / f"month={month:02d}"
            / "trades.parquet"
        )
        monthly_df = pd.read_parquet(monthly_path)

        # Times should be in ascending order
        assert monthly_df["time"].is_monotonic_increasing

    def test_consolidate_empty_month_logs_warning(self, service, temp_root):
        """Test that consolidating non-existent month returns without error."""
        venue = "DETR"
        year = 2025
        month = 12

        # Should not raise, just return
        service._consolidate_to_monthly(venue, year, month)

        # Verify no monthly file was created
        monthly_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades_monthly"
            / "venue=DETR"
            / f"year={year}"
            / f"month={month:02d}"
            / "trades.parquet"
        )
        assert not monthly_path.exists()

    def test_consolidate_preserves_daily_files(
        self, service, temp_root, sample_trades_df
    ):
        """Test that consolidation preserves original daily files."""
        venue = "DETR"
        year = 2025
        month = 11

        # Create daily file
        trade_date = datetime(year, month, 4)
        service.store_trades(sample_trades_df, venue, trade_date)

        daily_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / f"year={year}"
            / f"month={month:02d}"
            / "day=04"
            / "trades.parquet"
        )

        # Consolidate
        service._consolidate_to_monthly(venue, year, month)

        # Daily file should still exist
        assert daily_path.exists()


class TestCheckPartialDownloads:
    """Test detection of partial/incomplete downloads."""

    def test_detect_complete_dates(self, service, temp_root, sample_trades_df):
        """Test detection of dates with complete parquet files."""
        venue = "DETR"

        # Create files for multiple dates
        for day in [4, 5, 6]:
            trade_date = datetime(2025, 11, day)
            service.store_trades(sample_trades_df, venue, trade_date)

        status = service.check_partial_downloads(venue)

        assert len(status["complete_dates"]) == 3
        assert "2025-11-04" in status["complete_dates"]
        assert "2025-11-05" in status["complete_dates"]
        assert "2025-11-06" in status["complete_dates"]

    def test_detect_months_ready_for_consolidation(
        self, service, temp_root, sample_trades_df
    ):
        """Test detection of months with data ready for consolidation."""
        venue = "DETR"

        # Create files in Nov and Oct
        service.store_trades(sample_trades_df, venue, datetime(2025, 11, 4))
        service.store_trades(sample_trades_df, venue, datetime(2025, 10, 15))

        status = service.check_partial_downloads(venue)

        assert (2025, 11) in status["months_ready"]
        assert (2025, 10) in status["months_ready"]
        assert len(status["months_ready"]) == 2

    def test_empty_directory_returns_empty_status(self, service, temp_root):
        """Test that empty data directory returns empty status."""
        venue = "DETR"

        status = service.check_partial_downloads(venue)

        assert status["complete_dates"] == []
        assert status["partial_dates"] == []
        assert status["months_ready"] == []

    def test_multiple_venues_isolated(self, service, temp_root, sample_trades_df):
        """Test that different venues are tracked separately."""
        # Store data for DETR
        service.store_trades(sample_trades_df, "DETR", datetime(2025, 11, 4))

        # Store data for DFRA
        service.store_trades(sample_trades_df, "DFRA", datetime(2025, 11, 5))

        # Check DETR
        detr_status = service.check_partial_downloads("DETR")
        assert len(detr_status["complete_dates"]) == 1
        assert "2025-11-04" in detr_status["complete_dates"]

        # Check DFRA
        dfra_status = service.check_partial_downloads("DFRA")
        assert len(dfra_status["complete_dates"]) == 1
        assert "2025-11-05" in dfra_status["complete_dates"]


class TestIncrementalFetchWithConsolidation:
    """Test the full incremental fetch + consolidation workflow."""

    @patch("src.yf_parqed.xetra.xetra_service.XetraService.list_files")
    @patch("src.yf_parqed.xetra.xetra_service.XetraService.fetch_and_parse_trades")
    def test_incremental_fetch_stores_per_file(
        self, mock_fetch, mock_list, service, temp_root, sample_trades_df
    ):
        """Test that incremental fetch stores after each file."""
        venue = "DETR"

        # Mock 3 files available
        mock_list.return_value = [
            "DETR-posttrade-2025-11-04T09_00.json.gz",
            "DETR-posttrade-2025-11-04T09_01.json.gz",
            "DETR-posttrade-2025-11-04T09_02.json.gz",
        ]

        # Each file returns some trades
        mock_fetch.return_value = sample_trades_df.copy()

        # Mock get_missing_dates to return one date
        with patch.object(service, "get_missing_dates", return_value=["2025-11-04"]):
            summary = service.fetch_and_store_missing_trades_incremental(
                venue,
                consolidate=False,  # Skip consolidation for this test
            )

        assert summary["total_files"] == 3
        assert summary["dates_fetched"] == ["2025-11-04"]

        # Verify file was created
        daily_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / "year=2025"
            / "month=11"
            / "day=04"
            / "trades.parquet"
        )
        assert daily_path.exists()

    @patch("src.yf_parqed.xetra.xetra_service.XetraService.list_files")
    @patch("src.yf_parqed.xetra.xetra_service.XetraService.fetch_and_parse_trades")
    def test_consolidation_runs_after_complete_date(
        self, mock_fetch, mock_list, service, temp_root, sample_trades_df
    ):
        """Test that monthly consolidation runs after completing a date."""
        venue = "DETR"

        mock_list.return_value = ["DETR-posttrade-2025-11-04T09_00.json.gz"]
        mock_fetch.return_value = sample_trades_df.copy()

        with patch.object(service, "get_missing_dates", return_value=["2025-11-04"]):
            summary = service.fetch_and_store_missing_trades_incremental(
                venue,
                consolidate=True,  # Enable consolidation
            )

        assert summary["consolidated"] is True

        # Check monthly file was created
        monthly_path = (
            temp_root
            / "de"
            / "xetra"
            / "trades_monthly"
            / "venue=DETR"
            / "year=2025"
            / "month=11"
            / "trades.parquet"
        )
        assert monthly_path.exists()

    @patch("src.yf_parqed.xetra.xetra_service.XetraService.list_files")
    @patch("src.yf_parqed.xetra.xetra_service.XetraService.fetch_and_parse_trades")
    def test_partial_download_tracked_correctly(
        self, mock_fetch, mock_list, service, temp_root, sample_trades_df
    ):
        """Test that partial downloads are tracked when files < expected."""
        venue = "DETR"

        # Simulate only 2 files available (partial day)
        mock_list.return_value = [
            "DETR-posttrade-2025-11-04T09_00.json.gz",
            "DETR-posttrade-2025-11-04T09_01.json.gz",
        ]
        mock_fetch.return_value = sample_trades_df.copy()

        with patch.object(service, "get_missing_dates", return_value=["2025-11-04"]):
            summary = service.fetch_and_store_missing_trades_incremental(venue)

        # Date should be marked as fetched (we completed all available files)
        assert "2025-11-04" in summary["dates_fetched"]


class TestCLIIntegration:
    """Test CLI commands for consolidation."""

    def test_consolidate_month_cli_detects_months(
        self, service, temp_root, sample_trades_df
    ):
        """Test that CLI auto-detects months from stored data."""
        # Store data for Nov 2025
        service.store_trades(sample_trades_df, "DETR", datetime(2025, 11, 4))
        service.store_trades(sample_trades_df, "DETR", datetime(2025, 11, 5))

        # Check detection
        status = service.check_partial_downloads("DETR")
        assert (2025, 11) in status["months_ready"]

    def test_check_partial_shows_complete_and_ready(
        self, service, temp_root, sample_trades_df
    ):
        """Test that check_partial returns both complete dates and months ready."""
        # Store data
        service.store_trades(sample_trades_df, "DETR", datetime(2025, 11, 4))
        service.store_trades(sample_trades_df, "DETR", datetime(2025, 11, 5))

        status = service.check_partial_downloads("DETR")

        assert len(status["complete_dates"]) == 2
        assert (2025, 11) in status["months_ready"]
