"""Test that partitioned storage is the default for new installations."""

import json
from pathlib import Path
import pytest
from yf_parqed.config_service import ConfigService
from yf_parqed.primary_class import YFParqed


def test_default_storage_config_is_partitioned(tmp_path):
    """Test that ConfigService defaults to partitioned storage."""
    config = ConfigService(tmp_path)
    storage_config = config.load_storage_config()
    
    assert storage_config["partitioned"] is True, "Default storage should be partitioned"
    assert isinstance(storage_config["markets"], dict)
    assert isinstance(storage_config["sources"], dict)


def test_storage_config_created_on_initialize(tmp_path):
    """Test that initialize command creates storage_config.json with partitioned mode."""
    # Create intervals.json first (required for YFParqed initialization)
    intervals_file = tmp_path / "intervals.json"
    intervals_file.write_text('["1d"]')
    
    config = ConfigService(tmp_path)
    
    # Simulate what initialize does - load and save storage config
    storage_config = config.load_storage_config()
    config.save_storage_config(storage_config)
    
    # Verify file was created
    storage_config_file = tmp_path / "storage_config.json"
    assert storage_config_file.exists(), "storage_config.json should be created"
    
    # Verify content
    saved_config = json.loads(storage_config_file.read_text())
    assert saved_config["partitioned"] is True, "Saved config should have partitioned=True"


def test_ticker_gets_storage_info_on_first_save(tmp_path):
    """Test that storage backend info is recorded when ticker data is first saved."""
    # This test verifies the flow:
    # 1. Ticker is processed
    # 2. Data is saved with partitioned backend
    # 3. Storage info is recorded in ticker metadata
    
    from yf_parqed.ticker_registry import TickerRegistry
    from datetime import datetime
    
    config = ConfigService(tmp_path)
    registry = TickerRegistry(config=config)
    
    # Simulate saving data with partitioned storage
    storage_info = {
        "mode": "partitioned",
        "market": "us",
        "source": "yahoo",
        "dataset": "stocks_1d",
    }
    
    registry.update_ticker_interval_status(
        ticker="AAPL",
        interval="1d",
        found_data=True,
        last_date=datetime(2025, 12, 4),
        storage_info=storage_info,
    )
    
    # Verify storage info was recorded
    ticker_data = registry.tickers.get("AAPL")
    assert ticker_data is not None, "Ticker should exist"
    
    interval_data = ticker_data.get("intervals", {}).get("1d", {})
    assert interval_data.get("storage") == storage_info, "Storage info should be recorded"
    assert interval_data.get("storage", {}).get("mode") == "partitioned"


def test_ticker_without_storage_info_uses_default(tmp_path):
    """Test that tickers without storage info default to partitioned when config says so."""
    from yf_parqed.ticker_registry import TickerRegistry
    
    # Create a ticker without storage info (simulating old data)
    tickers_file = tmp_path / "tickers.json"
    tickers_data = {
        "AAPL": {
            "ticker": "AAPL",
            "added_date": "2025-12-01",
            "status": "active",
            "last_checked": "2025-12-04",
            "intervals": {
                "1d": {
                    "status": "active",
                    "last_found_date": "2025-12-04",
                    "last_data_date": "2025-12-03",
                    "last_checked": "2025-12-04",
                    # Note: no "storage" key
                }
            }
        }
    }
    tickers_file.write_text(json.dumps(tickers_data, indent=2))
    
    config = ConfigService(tmp_path)
    registry = TickerRegistry(config=config, initial_tickers=tickers_data)
    
    # Verify ticker loaded without storage info
    ticker_data = registry.tickers.get("AAPL")
    interval_data = ticker_data.get("intervals", {}).get("1d", {})
    assert "storage" not in interval_data, "Old ticker should not have storage info"
    
    # Get storage info (should be None, triggering default behavior)
    storage_info = registry.get_interval_storage("AAPL", "1d")
    assert storage_info is None, "Old ticker should return None for storage info"


def test_storage_backend_selection_with_config(tmp_path):
    """Test that _build_storage_request uses partitioned backend when configured."""
    # Create intervals.json
    intervals_file = tmp_path / "intervals.json"
    intervals_file.write_text('["1d"]')
    
    # Create storage_config.json with partitioned mode
    storage_config_file = tmp_path / "storage_config.json"
    storage_config_file.write_text(json.dumps({
        "partitioned": True,
        "markets": {"us": True},
        "sources": {"yahoo": True}
    }))
    
    # Create tickers.json with a ticker that has storage info
    tickers_file = tmp_path / "tickers.json"
    tickers_data = {
        "AAPL": {
            "ticker": "AAPL",
            "added_date": "2025-12-01",
            "status": "active",
            "last_checked": "2025-12-04",
            "intervals": {
                "1d": {
                    "status": "active",
                    "last_found_date": "2025-12-04",
                    "last_data_date": "2025-12-03",
                    "last_checked": "2025-12-04",
                    "storage": {
                        "mode": "partitioned",
                        "market": "us",
                        "source": "yahoo",
                        "dataset": "stocks_1d"
                    }
                }
            }
        }
    }
    tickers_file.write_text(json.dumps(tickers_data, indent=2))
    
    # Initialize YFParqed
    yf = YFParqed(my_path=tmp_path, my_intervals=["1d"])
    
    # Build storage request for ticker with partitioned storage info
    storage_request = yf._build_storage_request("AAPL", "1d")
    
    # Verify it's a partitioned request
    assert storage_request.market == "us", "Should use US market"
    assert storage_request.source == "yahoo", "Should use yahoo source"
    assert storage_request.root == tmp_path / "data", "Should use data/ subdirectory"


def test_initialize_creates_correct_config_files(tmp_path):
    """Integration test: verify initialize creates all required config files correctly."""
    from unittest.mock import Mock, patch
    
    # Mock the API calls - return dict format expected by update_current_list
    mock_tickers = {
        "AAPL": {"ticker": "AAPL"},
        "GOOGL": {"ticker": "GOOGL"},
        "MSFT": {"ticker": "MSFT"}
    }
    
    with patch('yf_parqed.primary_class.YFParqed.get_new_list_of_stocks') as mock_get_stocks:
        mock_get_stocks.return_value = mock_tickers
        
        # Initialize (with minimal intervals to avoid full ticker download)
        yf = YFParqed(my_path=tmp_path, my_intervals=["1d"])
        
        # Simulate what initialize command does
        yf.get_new_list_of_stocks()
        yf.save_intervals(["1m"])
        yf.update_current_list_of_stocks()
        yf.save_tickers()
        
        # Ensure storage_config.json exists
        storage_config = yf.config.load_storage_config()
        yf.config.save_storage_config(storage_config)
    
    # Verify all config files created
    assert (tmp_path / "intervals.json").exists(), "intervals.json should exist"
    assert (tmp_path / "tickers.json").exists(), "tickers.json should exist"
    assert (tmp_path / "storage_config.json").exists(), "storage_config.json should exist"
    
    # Verify storage_config.json has partitioned mode
    storage_config_file = tmp_path / "storage_config.json"
    storage_config = json.loads(storage_config_file.read_text())
    assert storage_config["partitioned"] is True, "Should default to partitioned storage"


def test_legacy_storage_not_used_for_new_tickers(tmp_path):
    """Test that new tickers don't use legacy storage paths when partitioned is default."""
    from yf_parqed.ticker_registry import TickerRegistry
    from datetime import datetime
    
    config = ConfigService(tmp_path)
    registry = TickerRegistry(config=config)
    
    # Add a new ticker with partitioned storage
    storage_info = {
        "mode": "partitioned",
        "market": "us",
        "source": "yahoo",
        "dataset": "stocks_1m",
    }
    
    registry.update_ticker_interval_status(
        ticker="NVDA",
        interval="1m",
        found_data=True,
        last_date=datetime(2025, 12, 4),
        storage_info=storage_info,
    )
    
    # Verify storage mode is partitioned, not legacy
    ticker_data = registry.tickers.get("NVDA")
    interval_data = ticker_data["intervals"]["1m"]
    
    assert interval_data.get("storage", {}).get("mode") == "partitioned"
    assert interval_data.get("storage", {}).get("market") == "us"
    assert interval_data.get("storage", {}).get("source") == "yahoo"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
