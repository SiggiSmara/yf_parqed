"""Tests for XetraService orchestration layer."""

import gzip
import json
from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from yf_parqed.xetra.xetra_service import XetraService


class TestXetraService:
    """Test suite for XetraService orchestration."""

    @pytest.fixture
    def sample_trade(self):
        """Single trade record matching Deutsche Börse schema."""
        return {
            "messageId": "posttrade",
            "sourceName": "ETR",
            "isin": "DE0007100000",
            "currency": "EUR",
            "tickActionIndicator": "I",
            "instrumentIdCode": "I",
            "mmtMarketMechanism": "8",
            "mmtTradingMode": "2",
            "mmtNegotTransPretrdWaivInd": "-",
            "mmtModificationInd": "-",
            "mmtBenchmarkRefprcInd": "-",
            "mmtPubModeDefReason": "-",
            "mmtAlgoInd": "H",
            "quotationType": 1,
            "lastQty": 159.00,
            "lastTrade": 56.20,
            "lastTradeTime": "2025-10-31T13:54:00.042457058Z",
            "distributionDateTime": "2025-10-31T13:54:00.052903000Z",
            "tickId": 33976320,
            "instrumentId": "DE0007100000",
            "transIdCode": "1000000000000025050760176191884004245705800000006636",
            "executionVenueId": "XETA",
        }

    @pytest.fixture
    def sample_jsonl(self, sample_trade):
        """JSONL format with 3 trades."""
        trade1 = sample_trade.copy()
        trade2 = sample_trade.copy()
        trade2["isin"] = "DE000A3H2200"
        trade2["lastTrade"] = 48.04
        trade2["tickId"] = 49699840

        trade3 = sample_trade.copy()
        trade3["isin"] = "DE000SHA0100"
        trade3["lastTrade"] = 7.06
        trade3["tickId"] = 49704960

        return (
            json.dumps(trade1) + "\n" + json.dumps(trade2) + "\n" + json.dumps(trade3)
        )

    def test_initialization_default_dependencies(self):
        """Test service initializes with default dependencies."""
        service = XetraService()

        assert service.fetcher is not None
        assert service.parser is not None
        assert service.backend is not None

    def test_initialization_injected_dependencies(self):
        """Test service accepts injected dependencies."""
        mock_fetcher = Mock()
        mock_parser = Mock()
        mock_backend = Mock()

        service = XetraService(
            fetcher=mock_fetcher, parser=mock_parser, backend=mock_backend
        )

        assert service.fetcher is mock_fetcher
        assert service.parser is mock_parser
        assert service.backend is mock_backend

    @patch("yf_parqed.xetra.xetra_fetcher.XetraFetcher.list_available_files")
    def test_list_files_delegates_to_fetcher(self, mock_list):
        """Test list_files calls fetcher and filters by date."""
        # Mock returns files with dates in filenames
        mock_list.return_value = [
            "DETR-posttrade-2025-10-31T09_00.json.gz",
            "DETR-posttrade-2025-10-31T10_00.json.gz",
            "DETR-posttrade-2025-11-01T09_00.json.gz",  # Different date
        ]

        service = XetraService()
        files = service.list_files("DETR", "2025-10-31")

        # Should only return files matching the requested date
        assert len(files) == 2
        assert all("2025-10-31" in f for f in files)
        mock_list.assert_called_once_with("DETR")

    @patch("yf_parqed.xetra.xetra_fetcher.XetraFetcher.download_file")
    @patch("yf_parqed.xetra.xetra_fetcher.XetraFetcher.decompress_gzip")
    @patch("yf_parqed.xetra.xetra_parser.XetraParser.parse")
    def test_fetch_and_parse_trades_full_workflow(
        self, mock_parse, mock_decompress, mock_download, sample_jsonl
    ):
        """Test complete fetch → decompress → parse workflow."""
        # Mock compressed data
        compressed = gzip.compress(sample_jsonl.encode("utf-8"))
        mock_download.return_value = compressed
        mock_decompress.return_value = sample_jsonl

        # Mock parsed DataFrame
        mock_df = pd.DataFrame(
            {
                "isin": ["DE0007100000", "DE000A3H2200", "DE000SHA0100"],
                "price": [56.20, 48.04, 7.06],
                "volume": [159.0, 3.0, 40.0],
            }
        )
        mock_parse.return_value = mock_df

        service = XetraService()
        df = service.fetch_and_parse_trades(
            "DETR", "2025-10-31", "DETR-posttrade-2025-10-31T13_54.json.gz"
        )

        # Verify workflow steps
        mock_download.assert_called_once_with(
            "DETR", "2025-10-31", "DETR-posttrade-2025-10-31T13_54.json.gz"
        )
        mock_decompress.assert_called_once_with(compressed)
        mock_parse.assert_called_once_with(sample_jsonl)

        # Verify result
        assert len(df) == 3
        assert df["isin"].tolist() == ["DE0007100000", "DE000A3H2200", "DE000SHA0100"]

    @patch("yf_parqed.xetra.xetra_fetcher.XetraFetcher.list_available_files")
    @patch("yf_parqed.xetra.xetra_service.XetraService.fetch_and_parse_trades")
    def test_fetch_all_trades_for_date_combines_files(
        self, mock_fetch_parse, mock_list
    ):
        """Test fetching and combining multiple files."""
        # Mock returns files with dates in filenames
        mock_list.return_value = [
            "DETR-posttrade-2025-10-31T09_00.json.gz",
            "DETR-posttrade-2025-10-31T10_00.json.gz",
        ]

        # Mock DataFrames from each file
        df1 = pd.DataFrame({"isin": ["DE001"], "price": [100.0], "volume": [10.0]})
        df2 = pd.DataFrame({"isin": ["DE002"], "price": [200.0], "volume": [20.0]})

        mock_fetch_parse.side_effect = [df1, df2]

        service = XetraService()
        combined = service.fetch_all_trades_for_date("DETR", "2025-10-31")

        # Verify combined DataFrame
        assert len(combined) == 2
        assert combined["isin"].tolist() == ["DE001", "DE002"]
        assert combined["price"].tolist() == [100.0, 200.0]

    @patch("yf_parqed.xetra.xetra_fetcher.XetraFetcher.list_available_files")
    def test_fetch_all_trades_for_date_no_files(self, mock_list):
        """Test handling when no files found."""
        mock_list.return_value = []

        service = XetraService()
        df = service.fetch_all_trades_for_date("DETR", "2025-10-31")

        assert df.empty
        assert isinstance(df, pd.DataFrame)

    @patch("yf_parqed.xetra.xetra_fetcher.XetraFetcher.list_available_files")
    @patch("yf_parqed.xetra.xetra_service.XetraService.fetch_and_parse_trades")
    def test_fetch_all_trades_continues_on_error(self, mock_fetch_parse, mock_list):
        """Test that errors in one file don't stop processing others."""
        mock_list.return_value = [
            "DETR-posttrade-2025-10-31T09_00.json.gz",
            "DETR-posttrade-2025-10-31T10_00.json.gz",
            "DETR-posttrade-2025-10-31T11_00.json.gz",
        ]

        # Second file fails, others succeed
        df1 = pd.DataFrame({"isin": ["DE001"], "price": [100.0]})
        df3 = pd.DataFrame({"isin": ["DE003"], "price": [300.0]})

        mock_fetch_parse.side_effect = [
            df1,
            Exception("Network error"),
            df3,
        ]

        service = XetraService()
        combined = service.fetch_all_trades_for_date("DETR", "2025-10-31")

        # Should have 2 successful files
        assert len(combined) == 2
        assert combined["isin"].tolist() == ["DE001", "DE003"]

    def test_store_trades_calls_backend(self):
        """Test store_trades delegates to backend correctly."""
        mock_backend = Mock()
        service = XetraService(backend=mock_backend)

        df = pd.DataFrame(
            {
                "isin": ["DE001", "DE002"],
                "price": [100.0, 200.0],
                "volume": [10.0, 20.0],
            }
        )
        trade_date = datetime(2025, 10, 31)

        service.store_trades(df, "DETR", trade_date, market="de", source="xetra")

        mock_backend.save_xetra_trades.assert_called_once_with(
            df, "DETR", trade_date, "de", "xetra"
        )

    def test_store_trades_empty_dataframe(self):
        """Test store_trades handles empty DataFrame gracefully."""
        mock_backend = Mock()
        service = XetraService(backend=mock_backend)

        df = pd.DataFrame()
        trade_date = datetime(2025, 10, 31)

        service.store_trades(df, "DETR", trade_date)

        # Should not call backend for empty DataFrame
        mock_backend.save_xetra_trades.assert_not_called()

    def test_context_manager_support(self):
        """Test service works as context manager."""
        mock_fetcher = Mock()

        with XetraService(fetcher=mock_fetcher) as service:
            assert service.fetcher is mock_fetcher

        # Verify cleanup called
        mock_fetcher.close.assert_called_once()

    def test_close_closes_fetcher(self):
        """Test close method closes fetcher."""
        mock_fetcher = Mock()
        service = XetraService(fetcher=mock_fetcher)

        service.close()

        mock_fetcher.close.assert_called_once()

    @patch("yf_parqed.xetra.xetra_fetcher.XetraFetcher.list_available_files")
    @patch("yf_parqed.xetra.xetra_service.XetraService.fetch_and_parse_trades")
    def test_fetch_all_trades_all_files_fail(self, mock_fetch_parse, mock_list):
        """Test handling when all files fail to parse."""
        mock_list.return_value = ["file1.json.gz", "file2.json.gz"]

        # All files fail
        mock_fetch_parse.side_effect = [
            Exception("Parse error 1"),
            Exception("Parse error 2"),
        ]

        service = XetraService()
        df = service.fetch_all_trades_for_date("DETR", "2025-10-31")

        # Should return empty DataFrame
        assert df.empty
        assert isinstance(df, pd.DataFrame)

    def test_store_trades_default_market_and_source(self):
        """Test store_trades uses default market/source parameters."""
        mock_backend = Mock()
        service = XetraService(backend=mock_backend)

        df = pd.DataFrame({"isin": ["DE001"], "price": [100.0], "volume": [10.0]})
        trade_date = datetime(2025, 10, 31)

        # Call without market/source
        service.store_trades(df, "DETR", trade_date)

        # Should use defaults
        mock_backend.save_xetra_trades.assert_called_once_with(
            df, "DETR", trade_date, "de", "xetra"
        )

    def test_get_missing_dates_both_missing(self, tmp_path):
        """Test get_missing_dates when both today and yesterday are missing."""
        from datetime import timedelta

        mock_fetcher = Mock()
        mock_backend = Mock()
        mock_backend._path_builder._root = tmp_path

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        # Mock API returning files for both dates (all files in rolling window)
        mock_fetcher.list_available_files.return_value = [
            f"DETR-posttrade-{today.strftime('%Y-%m-%d')}T09_00.json.gz",
            f"DETR-posttrade-{today.strftime('%Y-%m-%d')}T10_00.json.gz",
            f"DETR-posttrade-{yesterday.strftime('%Y-%m-%d')}T09_00.json.gz",
        ]

        service = XetraService(fetcher=mock_fetcher, backend=mock_backend)
        missing = service.get_missing_dates("DETR")

        # Both should be missing (not stored locally)
        assert len(missing) == 2
        assert all(isinstance(d, str) for d in missing)

    def test_get_missing_dates_one_stored(self, tmp_path):
        """Test get_missing_dates when one date is already stored.

        Note: get_missing_dates now returns ALL dates from API (including stored ones)
        to enable incremental checking. The actual filtering happens in
        fetch_and_store_missing_trades_incremental.
        """
        from datetime import timedelta

        mock_fetcher = Mock()
        mock_backend = Mock()
        mock_backend._path_builder._root = tmp_path

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        # Mock API returning files for both dates
        mock_fetcher.list_available_files.return_value = [
            f"DETR-posttrade-{today.strftime('%Y-%m-%d')}T09_00.json.gz",
            f"DETR-posttrade-{yesterday.strftime('%Y-%m-%d')}T09_00.json.gz",
        ]

        # Create storage for today only
        year = today.year
        month = f"{today.month:02d}"
        day = f"{today.day:02d}"

        storage_dir = (
            tmp_path
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
        )
        storage_dir.mkdir(parents=True)
        (storage_dir / "trades.parquet").touch()

        # Create download log showing today's file is complete
        log_path = tmp_path / "de" / "xetra" / ".download_log.parquet"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_data = pd.DataFrame(
            {
                "venue": ["DETR"],
                "date": [today.strftime("%Y-%m-%d")],
                "timestamp": [f"{today.strftime('%Y-%m-%d')}T09_00"],
                "has_data": [True],
                "trade_count": [100],
                "downloaded_at": [pd.Timestamp.now()],
            }
        )
        log_data.to_parquet(log_path, index=False)

        service = XetraService(fetcher=mock_fetcher, backend=mock_backend)
        missing = service.get_missing_dates("DETR")

        # Both dates should be returned (for incremental checking)
        assert len(missing) == 2
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")
        assert yesterday_str in missing
        assert today_str in missing

    def test_get_missing_dates_all_stored(self, tmp_path):
        """Test get_missing_dates when all dates are already stored."""
        from datetime import timedelta

        mock_fetcher = Mock()
        mock_backend = Mock()
        mock_backend._path_builder._root = tmp_path

        # Mock API returning files
        mock_fetcher.list_available_files.return_value = ["file1.json.gz"]

        # Create storage for both today and yesterday
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        for check_date in [today, yesterday]:
            year = check_date.year
            month = f"{check_date.month:02d}"
            day = f"{check_date.day:02d}"

            storage_dir = (
                tmp_path
                / "de"
                / "xetra"
                / "trades"
                / "venue=DETR"
                / f"year={year}"
                / f"month={month}"
                / f"day={day}"
            )
            storage_dir.mkdir(parents=True)
            (storage_dir / "trades.parquet").touch()

        service = XetraService(fetcher=mock_fetcher, backend=mock_backend)
        missing = service.get_missing_dates("DETR")

        # Nothing should be missing
        assert len(missing) == 0

    @patch("yf_parqed.xetra.xetra_service.XetraService.fetch_all_trades_for_date")
    @patch("yf_parqed.xetra.xetra_service.XetraService.store_trades")
    @patch("yf_parqed.xetra.xetra_service.XetraService.get_missing_dates")
    def test_fetch_and_store_missing_trades_success(
        self, mock_get_missing, mock_store, mock_fetch_all
    ):
        """Test fetch_and_store_missing_trades with successful fetch."""
        # Mock missing dates
        mock_get_missing.return_value = ["2025-11-04", "2025-11-03"]

        # Mock fetch returning data
        mock_df = pd.DataFrame(
            {
                "isin": ["DE001", "DE002", "DE003"],
                "price": [100.0, 200.0, 300.0],
                "volume": [10.0, 20.0, 30.0],
            }
        )
        mock_fetch_all.return_value = mock_df

        service = XetraService()
        summary = service.fetch_and_store_missing_trades("DETR")

        # Verify summary
        assert summary["dates_checked"] == ["2025-11-04", "2025-11-03"]
        assert len(summary["dates_fetched"]) == 2
        assert summary["total_trades"] == 6  # 3 trades × 2 dates
        assert summary["total_isins"] == 3

    @patch("yf_parqed.xetra.xetra_service.XetraService.get_missing_dates")
    def test_fetch_and_store_missing_trades_nothing_missing(self, mock_get_missing):
        """Test fetch_and_store_missing_trades when nothing is missing."""
        # Mock no missing dates
        mock_get_missing.return_value = []

        service = XetraService()
        summary = service.fetch_and_store_missing_trades("DETR")

        # Verify empty summary
        assert summary["total_trades"] == 0
        assert len(summary["dates_fetched"]) == 0


class TestDownloadLogTracking:
    """Test suite for download log and partial download recovery."""

    def test_download_log_tracks_empty_files(self, tmp_path):
        """Test that download log tracks files even when they're empty."""
        from yf_parqed.xetra.xetra_service import XetraService
        from yf_parqed.common.partition_path_builder import PartitionPathBuilder
        from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend

        # Setup service with temp directory
        path_builder = PartitionPathBuilder(tmp_path)
        backend = PartitionedStorageBackend(
            empty_frame_factory=lambda: pd.DataFrame(),
            normalizer=lambda df: df,
            column_provider=lambda: [],
            path_builder=path_builder,
        )

        mock_fetcher = Mock()
        mock_parser = Mock()

        # Mock list_available_files to return 3 files
        mock_fetcher.list_available_files.return_value = [
            "DETR-posttrade-2025-11-28T08_00.json.gz",
            "DETR-posttrade-2025-11-28T08_01.json.gz",
            "DETR-posttrade-2025-11-28T08_02.json.gz",
        ]

        # Mock downloads: first has data, others are empty
        mock_fetcher.download_file.side_effect = [
            b"mock_gzip_data_1",
            b"mock_gzip_data_2",
            b"mock_gzip_data_3",
        ]
        mock_fetcher.decompress_gzip.side_effect = [
            '{"trades":[{"isin":"DE001"}]}',  # Has data
            '{"trades":[]}',  # Empty
            '{"trades":[]}',  # Empty
        ]

        # Mock parser to return data for first, empty for others
        mock_parser.parse.side_effect = [
            pd.DataFrame(
                {
                    "isin": ["DE001"],
                    "trade_time": [pd.Timestamp("2025-11-28 08:00:00")],
                    "price": [100.0],
                }
            ),
            pd.DataFrame(
                columns=["isin", "trade_time", "price"]
            ),  # Empty but with columns
            pd.DataFrame(
                columns=["isin", "trade_time", "price"]
            ),  # Empty but with columns
        ]

        service = XetraService(
            fetcher=mock_fetcher,
            parser=mock_parser,
            backend=backend,
            root_path=tmp_path,
        )

        # Run incremental fetch
        service.fetch_and_store_missing_trades_incremental(
            "DETR", market="de", source="xetra"
        )

        # Verify download log was created
        log_path = tmp_path / "de" / "xetra" / ".download_log.parquet"
        assert log_path.exists(), "Download log should be created"

        # Read and verify log contents
        log_df = pd.read_parquet(log_path)
        assert len(log_df) == 3, "Should track all 3 downloads"
        assert log_df["venue"].iloc[0] == "DETR"
        assert log_df["date"].iloc[0] == "2025-11-28"

        # Verify empty files are tracked
        empty_entries = log_df[~log_df["has_data"]]
        assert len(empty_entries) == 2, "Should track 2 empty files"
        assert all(empty_entries["trade_count"] == 0), (
            "Empty files should have 0 trades"
        )

    def test_partial_download_recovery(self, tmp_path):
        """Test that interrupted downloads can resume from where they left off."""
        from yf_parqed.xetra.xetra_service import XetraService
        from yf_parqed.common.partition_path_builder import PartitionPathBuilder
        from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend

        # Setup service
        path_builder = PartitionPathBuilder(tmp_path)
        backend = PartitionedStorageBackend(
            empty_frame_factory=lambda: pd.DataFrame(),
            normalizer=lambda df: df,
            column_provider=lambda: [],
            path_builder=path_builder,
        )

        mock_fetcher = Mock()
        mock_parser = Mock()

        # Simulate 5 files available, but only 2 are in the log (partial download)
        mock_fetcher.list_available_files.return_value = [
            "DETR-posttrade-2025-11-28T08_00.json.gz",
            "DETR-posttrade-2025-11-28T08_01.json.gz",
            "DETR-posttrade-2025-11-28T08_02.json.gz",
            "DETR-posttrade-2025-11-28T08_03.json.gz",
            "DETR-posttrade-2025-11-28T08_04.json.gz",
        ]

        # Create a partial download log (2 files already downloaded)
        log_path = tmp_path / "de" / "xetra" / ".download_log.parquet"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        partial_log = pd.DataFrame(
            {
                "venue": ["DETR", "DETR"],
                "date": ["2025-11-28", "2025-11-28"],
                "timestamp": ["2025-11-28T08_00", "2025-11-28T08_01"],
                "has_data": [True, False],
                "trade_count": [10, 0],
                "downloaded_at": [pd.Timestamp.now(), pd.Timestamp.now()],
            }
        )
        partial_log.to_parquet(log_path, index=False)

        # Mock downloads for remaining 3 files
        mock_fetcher.download_file.side_effect = [
            b"mock_data_3",
            b"mock_data_4",
            b"mock_data_5",
        ]
        mock_fetcher.decompress_gzip.side_effect = [
            '{"trades":[]}',
            '{"trades":[{"isin":"DE002"}]}',
            '{"trades":[]}',
        ]
        mock_parser.parse.side_effect = [
            pd.DataFrame(
                columns=["isin", "trade_time", "price"]
            ),  # Empty but with columns
            pd.DataFrame(
                {
                    "isin": ["DE002"],
                    "trade_time": [pd.Timestamp("2025-11-28 08:03:00")],
                    "price": [200.0],
                }
            ),
            pd.DataFrame(
                columns=["isin", "trade_time", "price"]
            ),  # Empty but with columns
        ]

        service = XetraService(
            fetcher=mock_fetcher,
            parser=mock_parser,
            backend=backend,
            root_path=tmp_path,
        )

        # Run incremental fetch - should only download 3 remaining files
        summary = service.fetch_and_store_missing_trades_incremental(
            "DETR", market="de", source="xetra"
        )

        # Verify only 3 files were fetched
        assert mock_fetcher.download_file.call_count == 3, (
            "Should only download 3 remaining files"
        )
        assert summary["total_files"] == 3, "Should report 3 files processed"

        # Verify log now has all 5 entries
        updated_log = pd.read_parquet(log_path)
        assert len(updated_log) == 5, "Log should have all 5 entries after resume"

    def test_download_log_merges_with_parquet_data(self, tmp_path):
        """Test that download log merges with existing parquet data timestamps."""
        from yf_parqed.xetra.xetra_service import XetraService
        from yf_parqed.common.partition_path_builder import PartitionPathBuilder
        from yf_parqed.common.partitioned_storage_backend import PartitionedStorageBackend

        # Setup service
        path_builder = PartitionPathBuilder(tmp_path)
        backend = PartitionedStorageBackend(
            empty_frame_factory=lambda: pd.DataFrame(),
            normalizer=lambda df: df,
            column_provider=lambda: [],
            path_builder=path_builder,
        )

        # Create existing parquet data (simulating data downloaded before log was implemented)
        parquet_path = (
            tmp_path
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / "year=2025"
            / "month=11"
            / "day=28"
            / "trades.parquet"
        )
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        existing_data = pd.DataFrame(
            {
                "isin": ["DE001", "DE002"],
                "trade_time": [
                    pd.Timestamp("2025-11-28 08:00:00"),
                    pd.Timestamp("2025-11-28 08:01:00"),
                ],
                "price": [100.0, 200.0],
            }
        )
        existing_data.to_parquet(parquet_path, index=False)

        # Create download log with different timestamps (empty files)
        log_path = tmp_path / "de" / "xetra" / ".download_log.parquet"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_data = pd.DataFrame(
            {
                "venue": ["DETR"],
                "date": ["2025-11-28"],
                "timestamp": ["2025-11-28T08_02"],  # Different from parquet
                "has_data": [False],
                "trade_count": [0],
                "downloaded_at": [pd.Timestamp.now()],
            }
        )
        log_data.to_parquet(log_path, index=False)

        mock_fetcher = Mock()
        mock_parser = Mock()

        # Mock 4 files available
        mock_fetcher.list_available_files.return_value = [
            "DETR-posttrade-2025-11-28T08_00.json.gz",  # In parquet
            "DETR-posttrade-2025-11-28T08_01.json.gz",  # In parquet
            "DETR-posttrade-2025-11-28T08_02.json.gz",  # In log
            "DETR-posttrade-2025-11-28T08_03.json.gz",  # Missing - should download
        ]

        service = XetraService(
            fetcher=mock_fetcher,
            parser=mock_parser,
            backend=backend,
            root_path=tmp_path,
        )

        # Mock the download for the missing file
        mock_fetcher.download_file.return_value = b"mock_data"
        mock_fetcher.decompress_gzip.return_value = '{"trades":[]}'
        mock_parser.parse.return_value = pd.DataFrame(
            columns=["isin", "trade_time", "price"]
        )  # Empty but with columns

        # Run incremental fetch
        summary = service.fetch_and_store_missing_trades_incremental(
            "DETR", market="de", source="xetra"
        )

        # Should only download 1 file (08:03), not the ones in parquet or log
        assert mock_fetcher.download_file.call_count == 1, (
            "Should only download 1 missing file"
        )
        assert summary["total_files"] == 1
