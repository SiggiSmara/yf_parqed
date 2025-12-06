"""End-to-end integration tests for Xetra workflow with mocked API."""

import gzip
import json
from datetime import datetime

import pandas as pd
import pytest

from yf_parqed.xetra.xetra_service import XetraService


class TestXetraIntegrationMocked:
    """Integration tests with mocked Deutsche Börse API responses."""

    @pytest.fixture
    def sample_trades(self):
        """Generate realistic trade data matching Deutsche Börse schema."""
        trades = []
        isins = [
            "DE0007100000",  # Mercedes-Benz
            "DE000BASF111",  # BASF
            "DE000BAY0017",  # Bayer
            "DE0005140008",  # Deutsche Bank
            "DE000A1ML7J1",  # Vonovia
        ]

        base_time = "2025-11-01T09:00:00"
        for i in range(100):
            isin = isins[i % len(isins)]
            trade = {
                "messageId": "posttrade",
                "sourceName": "ETR",
                "isin": isin,
                "currency": "EUR",
                "tickActionIndicator": "I",
                "instrumentIdCode": "I",
                "mmtMarketMechanism": "8",
                "mmtTradingMode": "2",
                "mmtNegotTransPretrdWaivInd": "-",
                "mmtModificationInd": "-",
                "mmtBenchmarkRefprcInd": "-",
                "mmtPubModeDefReason": "-",
                "mmtAlgoInd": "H" if i % 3 == 0 else "-",
                "quotationType": 1,
                "lastQty": float(10 + i * 2),
                "lastTrade": 100.0 + i * 0.5,
                "lastTradeTime": f"{base_time}.{i:09d}Z",
                "distributionDateTime": f"{base_time}.{i + 1000000:09d}Z",
                "tickId": 1000000 + i,
                "instrumentId": isin,
                "transIdCode": f"100000000000002504{i:020d}",
                "executionVenueId": "XETA",
            }
            trades.append(trade)

        return trades

    @pytest.fixture
    def sample_jsonl(self, sample_trades):
        """Convert trades to JSONL format."""
        return "\n".join(json.dumps(trade) for trade in sample_trades)

    @pytest.fixture
    def compressed_sample(self, sample_jsonl):
        """Gzip compress the JSONL data."""
        return gzip.compress(sample_jsonl.encode("utf-8"))

    def test_full_workflow_with_mocks(
        self, tmp_path, sample_trades, sample_jsonl, compressed_sample, monkeypatch
    ):
        """Test complete fetch → parse → store → read workflow with mocked API."""
        import time

        # Mock the HTTP fetcher methods
        from yf_parqed.xetra.xetra_fetcher import XetraFetcher

        def mock_list_files(self, venue):
            return ["DETR-posttrade-2025-11-01T09_00.json.gz"]

        def mock_download(self, venue, date, filename):
            return compressed_sample

        def mock_decompress(self, data):
            return gzip.decompress(data).decode("utf-8")

        monkeypatch.setattr(XetraFetcher, "list_available_files", mock_list_files)
        monkeypatch.setattr(XetraFetcher, "download_file", mock_download)
        monkeypatch.setattr(XetraFetcher, "decompress_gzip", mock_decompress)

        # Create service with temp path as root
        service = XetraService(root_path=tmp_path)

        # Step 1: List files
        files = service.list_files("DETR", "2025-11-01")
        assert len(files) == 1
        assert "DETR-posttrade-2025-11-01T09_00.json.gz" in files

        # Step 2: Fetch and parse trades
        start_time = time.time()
        df = service.fetch_all_trades_for_date("DETR", "2025-11-01")
        fetch_time = time.time() - start_time

        # Validate parsed data
        assert len(df) == 100, f"Expected 100 trades, got {len(df)}"
        assert len(df["isin"].unique()) == 5, "Expected 5 unique ISINs"

        # Check schema
        required_columns = [
            "isin",
            "price",
            "volume",
            "currency",
            "trade_time",
            "venue",
        ]
        for col in required_columns:
            assert col in df.columns, f"Missing column: {col}"

        # Validate data types
        assert df["price"].dtype == "float64"
        assert df["volume"].dtype == "float64"
        assert pd.api.types.is_datetime64_any_dtype(df["trade_time"])

        # Step 3: Store to parquet
        trade_date = datetime(2025, 11, 1)
        service.store_trades(df, "DETR", trade_date, market="de", source="xetra")

        # Step 4: Verify parquet files exist
        # Path: {tmp_path}/de/xetra/trades/venue=DETR/year=2025/month=11/day=01/trades.parquet
        expected_path = (
            tmp_path
            / "de"
            / "xetra"
            / "trades"
            / "venue=DETR"
            / "year=2025"
            / "month=11"
            / "day=01"
            / "trades.parquet"
        )
        assert expected_path.exists(), f"Parquet file not found at {expected_path}"

        # Step 5: Read back and validate
        df_read = pd.read_parquet(expected_path)
        assert len(df_read) == 100
        assert len(df_read["isin"].unique()) == 5

        # Verify data integrity (sample checks)
        assert df_read["isin"].tolist()[0] == "DE0007100000"
        assert df_read["price"].tolist()[0] == 100.0
        assert df_read["volume"].tolist()[0] == 10.0

        # Performance check (should be fast with 100 trades)
        assert fetch_time < 5.0, (
            f"Fetch took {fetch_time:.2f}s, should be <5s for 100 trades"
        )

        print(f"\n✓ Integration test passed in {fetch_time:.3f}s")
        print(f"  - Fetched and parsed {len(df)} trades")
        print(f"  - {len(df['isin'].unique())} unique ISINs")
        print(f"  - Stored to {expected_path}")
        print(f"  - Read back {len(df_read)} trades successfully")

    def test_multiple_files_workflow(self, tmp_path, sample_jsonl, monkeypatch):
        """Test workflow with multiple files for same venue/date."""
        from yf_parqed.xetra.xetra_fetcher import XetraFetcher

        # Create two different files
        trades1 = [
            {
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
                "lastQty": 100.0,
                "lastTrade": 50.0,
                "lastTradeTime": "2025-11-01T09:00:00.000000000Z",
                "distributionDateTime": "2025-11-01T09:00:01.000000000Z",
                "tickId": 1000000,
                "instrumentId": "DE0007100000",
                "transIdCode": "1000000000000001",
                "executionVenueId": "XETA",
            }
        ]

        trades2 = [
            {
                "messageId": "posttrade",
                "sourceName": "ETR",
                "isin": "DE000BASF111",
                "currency": "EUR",
                "tickActionIndicator": "I",
                "instrumentIdCode": "I",
                "mmtMarketMechanism": "8",
                "mmtTradingMode": "2",
                "mmtNegotTransPretrdWaivInd": "-",
                "mmtModificationInd": "-",
                "mmtBenchmarkRefprcInd": "-",
                "mmtPubModeDefReason": "-",
                "mmtAlgoInd": "-",
                "quotationType": 1,
                "lastQty": 200.0,
                "lastTrade": 75.0,
                "lastTradeTime": "2025-11-01T10:00:00.000000000Z",
                "distributionDateTime": "2025-11-01T10:00:01.000000000Z",
                "tickId": 2000000,
                "instrumentId": "DE000BASF111",
                "transIdCode": "2000000000000002",
                "executionVenueId": "XETA",
            }
        ]

        jsonl1 = json.dumps(trades1[0])
        jsonl2 = json.dumps(trades2[0])
        compressed1 = gzip.compress(jsonl1.encode("utf-8"))
        compressed2 = gzip.compress(jsonl2.encode("utf-8"))

        # Mock to return different data per file
        download_map = {
            "DETR-posttrade-2025-11-01T09_00.json.gz": compressed1,
            "DETR-posttrade-2025-11-01T10_00.json.gz": compressed2,
        }

        def mock_list_files(self, venue):
            return list(download_map.keys())

        def mock_download(self, venue, date, filename):
            return download_map[filename]

        def mock_decompress(self, data):
            return gzip.decompress(data).decode("utf-8")

        monkeypatch.setattr(XetraFetcher, "list_available_files", mock_list_files)
        monkeypatch.setattr(XetraFetcher, "download_file", mock_download)
        monkeypatch.setattr(XetraFetcher, "decompress_gzip", mock_decompress)

        # Fetch all trades
        service = XetraService()
        df = service.fetch_all_trades_for_date("DETR", "2025-11-01")

        # Should have combined both files
        assert len(df) == 2
        assert set(df["isin"].unique()) == {"DE0007100000", "DE000BASF111"}
        assert df["price"].tolist() == [50.0, 75.0]

    def test_empty_response_workflow(self, tmp_path, monkeypatch):
        """Test workflow when API returns no files."""
        from yf_parqed.xetra.xetra_fetcher import XetraFetcher

        def mock_list_files(self, venue):
            return []

        monkeypatch.setattr(XetraFetcher, "list_available_files", mock_list_files)

        service = XetraService()
        df = service.fetch_all_trades_for_date("DETR", "2025-11-01")

        assert df.empty
        assert isinstance(df, pd.DataFrame)

    def test_partial_file_failure_workflow(self, tmp_path, sample_jsonl, monkeypatch):
        """Test workflow when some files fail to download/parse."""
        from yf_parqed.xetra.xetra_fetcher import XetraFetcher

        files = [
            "DETR-posttrade-2025-11-01T09_00.json.gz",  # Will succeed
            "DETR-posttrade-2025-11-01T10_00.json.gz",  # Will fail
            "DETR-posttrade-2025-11-01T11_00.json.gz",  # Will succeed
        ]

        def mock_list_files(self, venue):
            return files

        def mock_download(self, venue, date, filename):
            if "10_00" in filename:
                raise Exception("Network error")
            # Return minimal valid data
            trade = {
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
                "lastQty": 100.0,
                "lastTrade": 50.0,
                "lastTradeTime": "2025-11-01T09:00:00.000000000Z",
                "distributionDateTime": "2025-11-01T09:00:01.000000000Z",
                "tickId": 1000000,
                "instrumentId": "DE0007100000",
                "transIdCode": "1000000000000001",
                "executionVenueId": "XETA",
            }
            return gzip.compress(json.dumps(trade).encode("utf-8"))

        def mock_decompress(self, data):
            return gzip.decompress(data).decode("utf-8")

        monkeypatch.setattr(XetraFetcher, "list_available_files", mock_list_files)
        monkeypatch.setattr(XetraFetcher, "download_file", mock_download)
        monkeypatch.setattr(XetraFetcher, "decompress_gzip", mock_decompress)

        service = XetraService()
        df = service.fetch_all_trades_for_date("DETR", "2025-11-01")

        # Should have 2 successful files (09_00 and 11_00), 1 failed (10_00)
        assert len(df) == 2
        assert not df.empty
