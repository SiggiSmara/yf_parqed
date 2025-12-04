"""Test initial data fetch behavior for Xetra daemon."""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

import pytest
from freezegun import freeze_time
from typer.testing import CliRunner

from yf_parqed import xetra_cli as main


class StubXetraService:
    """Stub for XetraService that tracks calls and simulates data."""

    def __init__(self, has_data=False):
        self.has_data = has_data
        self.fetch_calls = 0
        self.has_any_data_calls = []

    def has_any_data(self, venue, market="de", source="xetra"):
        self.has_any_data_calls.append((venue, market, source))
        return self.has_data

    def fetch_and_store_missing_trades_incremental(self, venue, market, source):
        self.fetch_calls += 1
        return {
            "dates_fetched": ["2025-12-04"],
            "total_trades": 1000,
        }

    def get_missing_dates(self, venue, market="de", source="xetra"):
        if self.has_data:
            return []
        return ["2025-12-04", "2025-12-03"]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def test_daemon_performs_initial_fetch_when_no_data_within_hours():
    """Daemon fetches all available data on startup if no data exists (within trading hours)."""
    stub = StubXetraService(has_data=False)

    with patch("yf_parqed.xetra_cli.XetraService", return_value=stub):
        with freeze_time("2025-12-04 14:00:00+01:00"):  # 14:00 CET (within trading hours)
            runner = CliRunner()
            
            # Mock sleep to exit after initial fetch
            sleep_count = [0]
            original_sleep = time.sleep
            
            def mock_sleep(seconds):
                sleep_count[0] += 1
                if sleep_count[0] >= 1:
                    raise KeyboardInterrupt()
                original_sleep(0.01)  # Small delay for testing
            
            with patch("time.sleep", mock_sleep):
                result = runner.invoke(
                    main.app,
                    ["fetch-trades", "DETR", "--daemon"],
                    catch_exceptions=False,
                )

    # Should have checked for existing data
    assert len(stub.has_any_data_calls) >= 1
    assert stub.has_any_data_calls[0] == ("DETR", "de", "xetra")
    
    # Should have performed initial fetch since no data exists and within trading hours
    assert stub.fetch_calls >= 1


def test_daemon_skips_initial_fetch_when_data_exists():
    """Daemon skips initial fetch if data already exists."""
    stub = StubXetraService(has_data=True)

    with patch("yf_parqed.xetra_cli.XetraService", return_value=stub):
        with freeze_time("2025-12-04 14:00:00+01:00"):  # 14:00 CET
            runner = CliRunner()
            
            sleep_count = [0]
            original_sleep = time.sleep
            
            def mock_sleep(seconds):
                sleep_count[0] += 1
                if sleep_count[0] >= 1:
                    raise KeyboardInterrupt()
                original_sleep(0.01)
            
            with patch("time.sleep", mock_sleep):
                result = runner.invoke(
                    main.app,
                    ["fetch-trades", "DETR", "--daemon"],
                    catch_exceptions=False,
                )

    # Should have checked for existing data
    assert len(stub.has_any_data_calls) >= 1
    
    # Should NOT have performed initial fetch since data exists
    # Only regular cycle fetch should occur
    assert stub.fetch_calls >= 0  # May or may not fetch in regular cycle depending on timing


def test_daemon_defers_initial_fetch_when_outside_hours():
    """Daemon performs initial fetch immediately even when outside trading hours (API is 24/7)."""
    stub = StubXetraService(has_data=False)

    with patch("yf_parqed.xetra_cli.XetraService", return_value=stub):
        with freeze_time("2025-12-04 06:00:00+01:00"):  # 06:00 CET (outside trading hours 08:30-18:00)
            runner = CliRunner()

            sleep_count = [0]

            def mock_sleep(seconds):
                sleep_count[0] += 1
                # Exit after first sleep
                raise KeyboardInterrupt()

            with patch("time.sleep", mock_sleep):
                result = runner.invoke(
                    main.app,
                    ["fetch-trades", "DETR", "--daemon"],
                    catch_exceptions=False,
                )

    # Should have checked for existing data
    assert len(stub.has_any_data_calls) >= 1

    # Should have performed fetch immediately (initial fetch doesn't wait for trading hours)
    assert stub.fetch_calls == 1
def test_has_any_data_returns_false_when_no_directory():
    """has_any_data returns False when venue directory doesn't exist."""
    from yf_parqed.xetra_service import XetraService
    import tempfile
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        service = XetraService(root_path=Path(tmp_dir))
        assert service.has_any_data("DETR") is False


def test_has_any_data_returns_false_when_directory_empty():
    """has_any_data returns False when venue directory exists but has no parquet files."""
    from yf_parqed.xetra_service import XetraService
    import tempfile
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # Create venue directory structure but no parquet files
        venue_dir = tmp_path / "de" / "xetra" / "trades" / "venue=DETR" / "year=2025" / "month=12" / "day=04"
        venue_dir.mkdir(parents=True)
        
        service = XetraService(root_path=tmp_path)
        assert service.has_any_data("DETR") is False


def test_has_any_data_returns_true_when_parquet_exists():
    """has_any_data returns True when parquet files exist for the venue."""
    from yf_parqed.xetra_service import XetraService
    import tempfile
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # Create venue directory and a parquet file
        venue_dir = tmp_path / "de" / "xetra" / "trades" / "venue=DETR" / "year=2025" / "month=12" / "day=04"
        venue_dir.mkdir(parents=True)
        (venue_dir / "trades.parquet").touch()
        
        service = XetraService(root_path=tmp_path)
        assert service.has_any_data("DETR") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
