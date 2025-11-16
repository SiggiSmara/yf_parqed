"""Tests for XetraService orchestration layer."""

import gzip
import json
from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from yf_parqed.xetra_service import XetraService


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

    @patch("yf_parqed.xetra_fetcher.XetraFetcher.list_available_files")
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

    @patch("yf_parqed.xetra_fetcher.XetraFetcher.download_file")
    @patch("yf_parqed.xetra_fetcher.XetraFetcher.decompress_gzip")
    @patch("yf_parqed.xetra_parser.XetraParser.parse")
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

    @patch("yf_parqed.xetra_fetcher.XetraFetcher.list_available_files")
    @patch("yf_parqed.xetra_service.XetraService.fetch_and_parse_trades")
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

    @patch("yf_parqed.xetra_fetcher.XetraFetcher.list_available_files")
    def test_fetch_all_trades_for_date_no_files(self, mock_list):
        """Test handling when no files found."""
        mock_list.return_value = []

        service = XetraService()
        df = service.fetch_all_trades_for_date("DETR", "2025-10-31")

        assert df.empty
        assert isinstance(df, pd.DataFrame)

    @patch("yf_parqed.xetra_fetcher.XetraFetcher.list_available_files")
    @patch("yf_parqed.xetra_service.XetraService.fetch_and_parse_trades")
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

    @patch("yf_parqed.xetra_fetcher.XetraFetcher.list_available_files")
    @patch("yf_parqed.xetra_service.XetraService.fetch_and_parse_trades")
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
        """Test get_missing_dates when one date is already stored."""
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

        service = XetraService(fetcher=mock_fetcher, backend=mock_backend)
        missing = service.get_missing_dates("DETR")

        # Only yesterday should be missing
        assert len(missing) == 1
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        assert missing[0] == yesterday_str

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

    @patch("yf_parqed.xetra_service.XetraService.fetch_all_trades_for_date")
    @patch("yf_parqed.xetra_service.XetraService.store_trades")
    @patch("yf_parqed.xetra_service.XetraService.get_missing_dates")
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

    @patch("yf_parqed.xetra_service.XetraService.get_missing_dates")
    def test_fetch_and_store_missing_trades_nothing_missing(self, mock_get_missing):
        """Test fetch_and_store_missing_trades when nothing is missing."""
        # Mock no missing dates
        mock_get_missing.return_value = []

        service = XetraService()
        summary = service.fetch_and_store_missing_trades("DETR")

        # Verify empty summary
        assert summary["total_trades"] == 0
        assert len(summary["dates_fetched"]) == 0
