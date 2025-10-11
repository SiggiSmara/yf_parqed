import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import pytest
from unittest.mock import patch

# Import the class we're testing
from yf_parqed.primary_class import YFParqed


class TestTickerOperations:
    """Test suite for unified ticker read/write operations."""

    def setup_method(self):
        """Set up test environment before each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_date = "2024-01-15"

        # Mock intervals to avoid file operations
        self.test_intervals = ["1d", "1h"]

        # Create test data with new interval-aware structure - updated to match migration
        self.sample_tickers = {
            "AAPL": {
                "ticker": "AAPL",
                "added_date": self.test_date,
                "status": "active",
                "last_checked": self.test_date,
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": self.test_date,
                        "last_data_date": "2024-01-14",
                        "last_checked": self.test_date,
                    }
                },
            },
            "GOOGL": {
                "ticker": "GOOGL",
                "added_date": self.test_date,
                "status": "active",
                "last_checked": self.test_date,
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": self.test_date,
                        "last_data_date": self.test_date,
                        "last_checked": self.test_date,
                    }
                },
            },
            "INVALID": {
                "ticker": "INVALID",
                "added_date": self.test_date,
                "status": "not_found",
                "last_checked": self.test_date,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": self.test_date,
                        "last_checked": self.test_date,
                    },
                    "1h": {
                        "status": "not_found",
                        "last_not_found_date": self.test_date,
                        "last_checked": self.test_date,
                    },
                },
            },
        }

    def teardown_method(self):
        """Clean up after each test."""
        shutil.rmtree(self.temp_dir)

    def create_yf_parqed_instance(self):
        """Create a YFParqed instance with mocked intervals."""
        with patch.object(YFParqed, "load_intervals") as mock_load:
            mock_load.return_value = None
            instance = YFParqed(my_path=self.temp_dir, my_intervals=self.test_intervals)
            return instance

    def test_load_tickers_empty_file(self):
        """Test loading tickers when no file exists."""
        yf_parqed = self.create_yf_parqed_instance()

        # Should create empty dict when no file exists
        assert yf_parqed.tickers == {}
        assert yf_parqed.tickers_path == self.temp_dir / "tickers.json"

    def test_save_and_load_tickers(self):
        """Test saving and loading tickers."""
        yf_parqed = self.create_yf_parqed_instance()

        # Set test data and save
        yf_parqed.tickers = self.sample_tickers.copy()
        yf_parqed.save_tickers()

        # Verify file was created
        assert yf_parqed.tickers_path.exists()

        # Load fresh instance and verify data
        yf_parqed2 = self.create_yf_parqed_instance()
        assert yf_parqed2.tickers == self.sample_tickers

    def test_update_current_list_of_stocks_new_tickers(self):
        """Test updating ticker list with new tickers."""
        yf_parqed = self.create_yf_parqed_instance()

        # Mock get_new_list_of_stocks to return test data with new structure
        new_tickers = {
            "NEWTICKER": {
                "ticker": "NEWTICKER",
                "added_date": self.test_date,
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        with patch.object(
            yf_parqed, "get_new_list_of_stocks", return_value=new_tickers
        ):
            yf_parqed.update_current_list_of_stocks()

        # Should add new ticker
        assert "NEWTICKER" in yf_parqed.tickers
        assert yf_parqed.tickers["NEWTICKER"]["status"] == "active"
        assert "intervals" in yf_parqed.tickers["NEWTICKER"]

    def test_update_current_list_of_stocks_reactivate_not_found(self):
        """Test reactivating previously not found tickers."""
        yf_parqed = self.create_yf_parqed_instance()
        yf_parqed.tickers = self.sample_tickers.copy()

        # Mock get_new_list_of_stocks to return a previously not found ticker
        new_tickers = {
            "INVALID": {
                "ticker": "INVALID",
                "added_date": self.test_date,
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        with patch.object(
            yf_parqed, "get_new_list_of_stocks", return_value=new_tickers
        ):
            yf_parqed.update_current_list_of_stocks()

        # Should reactivate the ticker and preserve interval data
        assert yf_parqed.tickers["INVALID"]["status"] == "active"
        assert "intervals" in yf_parqed.tickers["INVALID"]

    def test_is_ticker_active_for_interval(self):
        """Test interval-specific ticker activity check."""
        yf_parqed = self.create_yf_parqed_instance()
        yf_parqed.tickers = self.sample_tickers.copy()

        # Active ticker should be processable
        assert yf_parqed.is_ticker_active_for_interval("AAPL", "1d")

        # New ticker should be processable
        assert yf_parqed.is_ticker_active_for_interval("NEWTICKER", "1d")

        # Globally not found ticker should not be processable
        assert not yf_parqed.is_ticker_active_for_interval("INVALID", "1d")

        # Test interval-specific not found (recent)
        with patch("yf_parqed.primary_class.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 20)  # 5 days later
            mock_datetime.strptime = datetime.strptime
            assert not yf_parqed.is_ticker_active_for_interval("INVALID", "1d")

    def test_is_ticker_active_for_interval_respects_cooldown_window(self):
        """Tickers should remain skipped if recent interval failure is within cooldown."""
        yf_parqed = self.create_yf_parqed_instance()
        current_date = datetime(2024, 2, 1)
        last_failure = (current_date - timedelta(days=10)).strftime("%Y-%m-%d")
        yf_parqed.tickers = {
            "COOLDOWN": {
                "ticker": "COOLDOWN",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": "2024-01-01",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": last_failure,
                        "last_checked": last_failure,
                    }
                },
            }
        }

        with patch("yf_parqed.primary_class.datetime") as mock_datetime:
            mock_datetime.now.return_value = current_date
            mock_datetime.strptime = datetime.strptime
            assert yf_parqed.is_ticker_active_for_interval("COOLDOWN", "1d") is False

    def test_is_ticker_active_for_interval_allows_retry_after_cooldown(self):
        """Ticker should become eligible once cooldown window has elapsed."""
        yf_parqed = self.create_yf_parqed_instance()
        current_date = datetime(2024, 3, 1)
        last_failure = (current_date - timedelta(days=45)).strftime("%Y-%m-%d")
        yf_parqed.tickers = {
            "RETRY": {
                "ticker": "RETRY",
                "added_date": "2023-12-01",
                "status": "active",
                "last_checked": last_failure,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": last_failure,
                        "last_checked": last_failure,
                    }
                },
            }
        }

        with patch("yf_parqed.primary_class.datetime") as mock_datetime:
            mock_datetime.now.return_value = current_date
            mock_datetime.strptime = datetime.strptime
            assert yf_parqed.is_ticker_active_for_interval("RETRY", "1d") is True

    def test_update_ticker_interval_status_found_data(self):
        """Test updating ticker status when data is found."""
        yf_parqed = self.create_yf_parqed_instance()
        test_date = datetime(2024, 1, 15)

        with patch("yf_parqed.primary_class.datetime") as mock_datetime:
            mock_datetime.now.return_value = test_date
            mock_datetime.strftime = datetime.strftime

            yf_parqed.update_ticker_interval_status("TESTTICKER", "1d", True, test_date)

            # Should create ticker with active status
            assert "TESTTICKER" in yf_parqed.tickers
            ticker_data = yf_parqed.tickers["TESTTICKER"]
            assert ticker_data["status"] == "active"
            assert ticker_data["last_checked"] == self.test_date
            assert ticker_data["intervals"]["1d"]["status"] == "active"
            assert ticker_data["intervals"]["1d"]["last_found_date"] == self.test_date

    def test_update_ticker_interval_status_no_data(self):
        """Test updating ticker status when no data is found."""
        yf_parqed = self.create_yf_parqed_instance()
        test_date = datetime(2024, 1, 15)

        with patch("yf_parqed.primary_class.datetime") as mock_datetime:
            mock_datetime.now.return_value = test_date
            mock_datetime.strftime = datetime.strftime

            yf_parqed.update_ticker_interval_status("TESTTICKER", "1d", False)

            # Should create ticker but mark interval as not found
            assert "TESTTICKER" in yf_parqed.tickers
            ticker_data = yf_parqed.tickers["TESTTICKER"]
            assert ticker_data["last_checked"] == self.test_date
            assert ticker_data["intervals"]["1d"]["status"] == "not_found"
            assert (
                ticker_data["intervals"]["1d"]["last_not_found_date"] == self.test_date
            )

    def test_update_ticker_interval_status_all_intervals_not_found(self):
        """Test global status change when all intervals are not found."""
        yf_parqed = self.create_yf_parqed_instance()
        test_date = datetime(2024, 1, 15)

        with patch("yf_parqed.primary_class.datetime") as mock_datetime:
            mock_datetime.now.return_value = test_date
            mock_datetime.strftime = datetime.strftime

            # Mark multiple intervals as not found
            yf_parqed.update_ticker_interval_status("TESTTICKER", "1d", False)
            yf_parqed.update_ticker_interval_status("TESTTICKER", "1h", False)

            # Global status should be not_found when all intervals fail
            ticker_data = yf_parqed.tickers["TESTTICKER"]
            assert ticker_data["status"] == "not_found"
            assert ticker_data["last_checked"] == self.test_date

    def test_data_structure_integrity(self):
        """Test that data structure maintains integrity across operations."""
        yf_parqed = self.create_yf_parqed_instance()
        yf_parqed.tickers = self.sample_tickers.copy()

        # Save and reload
        yf_parqed.save_tickers()
        yf_parqed.load_tickers()

        # Verify all required fields are present (updated for migration structure)
        for ticker, data in yf_parqed.tickers.items():
            assert "ticker" in data
            assert "added_date" in data
            assert "status" in data
            assert "last_checked" in data
            assert "intervals" in data
            assert data["status"] in ["active", "not_found"]
            assert data["ticker"] == ticker
            assert isinstance(data["intervals"], dict)

    def test_interval_data_structure(self):
        """Test that interval data has correct structure."""
        yf_parqed = self.create_yf_parqed_instance()
        yf_parqed.tickers = self.sample_tickers.copy()

        # Check AAPL's interval data
        aapl_interval_data = yf_parqed.tickers["AAPL"]["intervals"]["1d"]
        assert "status" in aapl_interval_data
        assert "last_found_date" in aapl_interval_data
        assert "last_data_date" in aapl_interval_data
        assert "last_checked" in aapl_interval_data

        # Check INVALID's interval data
        invalid_interval_data = yf_parqed.tickers["INVALID"]["intervals"]["1d"]
        assert "status" in invalid_interval_data
        assert "last_not_found_date" in invalid_interval_data
        assert "last_checked" in invalid_interval_data

    def test_file_format_validation(self):
        """Test that saved file has correct JSON format."""
        yf_parqed = self.create_yf_parqed_instance()
        yf_parqed.tickers = self.sample_tickers.copy()
        yf_parqed.save_tickers()

        # Read file directly and validate JSON
        file_content = yf_parqed.tickers_path.read_text()
        loaded_data = json.loads(file_content)

        assert isinstance(loaded_data, dict)
        assert loaded_data == self.sample_tickers

    def test_concurrent_access_simulation(self):
        """Test behavior when file is modified between operations."""
        yf_parqed = self.create_yf_parqed_instance()
        yf_parqed.tickers = {
            "TICKER1": {
                "ticker": "TICKER1",
                "added_date": self.test_date,
                "status": "active",
                "last_checked": self.test_date,
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": self.test_date,
                        "last_data_date": self.test_date,
                        "last_checked": self.test_date,
                    }
                },
            }
        }
        yf_parqed.save_tickers()

        # Simulate external modification
        external_data = {
            "TICKER2": {
                "ticker": "TICKER2",
                "added_date": self.test_date,
                "status": "active",
                "last_checked": self.test_date,
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": self.test_date,
                        "last_data_date": self.test_date,
                        "last_checked": self.test_date,
                    }
                },
            }
        }
        yf_parqed.tickers_path.write_text(json.dumps(external_data, indent=4))

        # Reload should get external changes
        yf_parqed.load_tickers()
        assert yf_parqed.tickers == external_data
        assert "TICKER1" not in yf_parqed.tickers
        assert "TICKER2" in yf_parqed.tickers

    def test_update_ticker_interval_status_keeps_active_if_any_interval_succeeds(self):
        """Global ticker status stays active when at least one interval succeeds."""
        yf_parqed = self.create_yf_parqed_instance()
        test_date = datetime(2024, 1, 31)

        with patch("yf_parqed.primary_class.datetime") as mock_datetime:
            mock_datetime.now.return_value = test_date
            mock_datetime.strftime = datetime.strftime

            # First, mark daily interval successful
            yf_parqed.update_ticker_interval_status("BLEND", "1d", True, test_date)

            # Then mark hourly interval as failing
            yf_parqed.update_ticker_interval_status("BLEND", "1h", False)

            ticker_data = yf_parqed.tickers["BLEND"]
            assert ticker_data["status"] == "active"
            assert ticker_data["intervals"]["1d"]["status"] == "active"
            assert ticker_data["intervals"]["1h"]["status"] == "not_found"

    def test_not_found_lifecycle_global_transition_after_repeated_failures(self):
        """Multiple interval failures should demote ticker to global not_found."""
        yf_parqed = self.create_yf_parqed_instance()
        yf_parqed.tickers = {
            "LIFE": {
                "ticker": "LIFE",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": "2024-01-01",
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": "2024-01-01",
                        "last_data_date": "2024-01-01",
                        "last_checked": "2024-01-01",
                    },
                    "1h": {
                        "status": "active",
                        "last_found_date": "2024-01-01",
                        "last_data_date": "2024-01-01",
                        "last_checked": "2024-01-01",
                    },
                },
            }
        }

        with patch.object(yf_parqed, "get_yfinance_data", return_value=pd.DataFrame()):
            yf_parqed.save_single_stock_data("LIFE", interval="1d")
            assert yf_parqed.tickers["LIFE"]["status"] == "active"
            assert yf_parqed.tickers["LIFE"]["intervals"]["1d"]["status"] == "not_found"

            yf_parqed.save_single_stock_data("LIFE", interval="1h")

        ticker_data = yf_parqed.tickers["LIFE"]
        assert ticker_data["status"] == "not_found"
        assert all(
            interval_info["status"] == "not_found"
            for interval_info in ticker_data["intervals"].values()
        )

    def test_not_found_lifecycle_reactivation_after_interval_success(self):
        """A successful interval fetch should reactivate a previously not_found ticker."""
        yf_parqed = self.create_yf_parqed_instance()
        stale_date = "2024-01-01"
        yf_parqed.tickers = {
            "BOUNCE": {
                "ticker": "BOUNCE",
                "added_date": "2023-12-01",
                "status": "active",
                "last_checked": stale_date,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": stale_date,
                        "last_checked": stale_date,
                    },
                    "1h": {
                        "status": "active",
                        "last_found_date": stale_date,
                        "last_data_date": stale_date,
                        "last_checked": stale_date,
                    },
                },
            }
        }

        success_index = pd.MultiIndex.from_tuples(
            [("BOUNCE", pd.Timestamp("2024-02-15"))], names=["stock", "date"]
        )
        success_df = pd.DataFrame(
            {
                "open": [40.0],
                "high": [41.0],
                "low": [39.5],
                "close": [40.5],
                "volume": [1500],
                "sequence": [1],
            },
            index=success_index,
        )

        with (
            patch.object(yf_parqed, "get_yfinance_data", return_value=success_df),
            patch.object(yf_parqed, "save_yf", return_value=success_df),
        ):
            yf_parqed.save_single_stock_data("BOUNCE", interval="1d")

        ticker_data = yf_parqed.tickers["BOUNCE"]
        assert ticker_data["status"] == "active"
        assert ticker_data["intervals"]["1d"]["status"] == "active"
        assert ticker_data["intervals"]["1h"]["status"] == "active"
        assert (
            ticker_data["intervals"]["1d"]["last_found_date"]
            == ticker_data["last_checked"]
        )

    def test_confirm_not_founds_reactivates_when_history_returns_data(self):
        """confirm_not_founds should reactivate tickers when fresh data exists."""
        yf_parqed = self.create_yf_parqed_instance()
        not_found_date = "2024-01-10"
        yf_parqed.tickers = {
            "RECOVER": {
                "ticker": "RECOVER",
                "added_date": "2023-12-01",
                "status": "not_found",
                "last_checked": not_found_date,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": not_found_date,
                        "last_checked": not_found_date,
                    }
                },
            }
        }

        history_df = pd.DataFrame(
            {"Close": [200.0]}, index=pd.to_datetime(["2024-01-20"])
        )
        current_now = datetime(2024, 1, 21)

        with (
            patch("yf_parqed.primary_class.track", side_effect=lambda it, *_, **__: it),
            patch("yf_parqed.primary_class.yf.Ticker") as mock_ticker,
            patch("yf_parqed.primary_class.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = current_now
            mock_datetime.strftime = datetime.strftime
            mock_datetime.strptime = datetime.strptime

            mock_ticker.return_value.history.return_value = history_df

            yf_parqed.confirm_not_founds()

        ticker_data = yf_parqed.tickers["RECOVER"]
        assert ticker_data["status"] == "active"
        interval_data = ticker_data["intervals"]["1d"]
        assert interval_data["status"] == "active"
        assert interval_data["last_found_date"] == current_now.strftime("%Y-%m-%d")
        assert ticker_data["last_checked"] == current_now.strftime("%Y-%m-%d")

    def test_confirm_not_founds_keeps_not_found_when_no_history(self):
        """Ticker remains not_found if no data returned."""
        yf_parqed = self.create_yf_parqed_instance()
        not_found_date = "2024-01-01"
        yf_parqed.tickers = {
            "STILL_MISSING": {
                "ticker": "STILL_MISSING",
                "added_date": "2023-11-01",
                "status": "not_found",
                "last_checked": not_found_date,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": not_found_date,
                        "last_checked": not_found_date,
                    }
                },
            }
        }

        empty_df = pd.DataFrame()
        current_now = datetime(2024, 1, 5)

        with (
            patch("yf_parqed.primary_class.track", side_effect=lambda it, *_, **__: it),
            patch("yf_parqed.primary_class.yf.Ticker") as mock_ticker,
            patch("yf_parqed.primary_class.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = current_now
            mock_datetime.strftime = datetime.strftime
            mock_datetime.strptime = datetime.strptime

            mock_ticker.return_value.history.return_value = empty_df

            yf_parqed.confirm_not_founds()

        ticker_data = yf_parqed.tickers["STILL_MISSING"]
        assert ticker_data["status"] == "not_found"
        interval_data = ticker_data["intervals"]["1d"]
        assert interval_data["status"] == "not_found"
        # Interval metadata should remain unchanged when no data is found
        assert interval_data["last_not_found_date"] == not_found_date
        assert ticker_data["last_checked"] == current_now.strftime("%Y-%m-%d")

    def test_confirm_not_founds_repeated_runs_update_last_checked(self):
        """Repeated confirm_not_founds calls should keep metadata fresh."""
        yf_parqed = self.create_yf_parqed_instance()
        original_not_found = "2024-01-01"
        yf_parqed.tickers = {
            "PERSIST": {
                "ticker": "PERSIST",
                "added_date": "2023-11-01",
                "status": "not_found",
                "last_checked": original_not_found,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": original_not_found,
                        "last_checked": original_not_found,
                    }
                },
            }
        }

        def run_confirm(current_now: datetime):
            with (
                patch(
                    "yf_parqed.primary_class.track",
                    side_effect=lambda it, *_, **__: it,
                ),
                patch("yf_parqed.primary_class.yf.Ticker") as mock_ticker,
                patch("yf_parqed.primary_class.datetime") as mock_datetime,
            ):
                mock_datetime.now.return_value = current_now
                mock_datetime.strftime = datetime.strftime
                mock_datetime.strptime = datetime.strptime

                mock_ticker.return_value.history.return_value = pd.DataFrame()

                yf_parqed.confirm_not_founds()

        first_check = datetime(2024, 1, 5)
        second_check = datetime(2024, 2, 10)

        run_confirm(first_check)
        assert yf_parqed.tickers["PERSIST"]["last_checked"] == first_check.strftime(
            "%Y-%m-%d"
        )

        run_confirm(second_check)
        ticker_meta = yf_parqed.tickers["PERSIST"]
        assert ticker_meta["last_checked"] == second_check.strftime("%Y-%m-%d")
        interval_meta = ticker_meta["intervals"]["1d"]
        assert interval_meta["last_not_found_date"] == original_not_found

    def test_reparse_not_founds_reactivates_recently_found_interval(self):
        """reparse_not_founds promotes tickers back to active if recent interval data exists."""
        yf_parqed = self.create_yf_parqed_instance()
        recent_date = datetime(2024, 3, 1)
        last_found = (recent_date - timedelta(days=30)).strftime("%Y-%m-%d")
        yf_parqed.tickers = {
            "WAKEUP": {
                "ticker": "WAKEUP",
                "added_date": "2023-01-01",
                "status": "not_found",
                "last_checked": recent_date.strftime("%Y-%m-%d"),
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": last_found,
                        "last_data_date": last_found,
                        "last_checked": last_found,
                    }
                },
            }
        }

        with (
            patch("yf_parqed.primary_class.track", side_effect=lambda it, *_, **__: it),
            patch("yf_parqed.primary_class.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = recent_date
            mock_datetime.strftime = datetime.strftime
            mock_datetime.strptime = datetime.strptime

            yf_parqed.reparse_not_founds()

        ticker_data = yf_parqed.tickers["WAKEUP"]
        assert ticker_data["status"] == "active"
        assert ticker_data["last_checked"] == recent_date.strftime("%Y-%m-%d")

    def test_reparse_not_founds_respects_ninety_day_window(self):
        """Tickers remain excluded when last interval success is beyond the 90-day window."""
        yf_parqed = self.create_yf_parqed_instance()
        current_date = datetime(2024, 6, 1)
        stale_found = (current_date - timedelta(days=120)).strftime("%Y-%m-%d")
        yf_parqed.tickers = {
            "STALE": {
                "ticker": "STALE",
                "added_date": "2023-01-01",
                "status": "not_found",
                "last_checked": stale_found,
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_found_date": stale_found,
                        "last_data_date": stale_found,
                        "last_checked": stale_found,
                    }
                },
            }
        }

        with (
            patch("yf_parqed.primary_class.track", side_effect=lambda it, *_, **__: it),
            patch("yf_parqed.primary_class.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = current_date
            mock_datetime.strftime = datetime.strftime
            mock_datetime.strptime = datetime.strptime

            yf_parqed.reparse_not_founds()

        ticker_data = yf_parqed.tickers["STALE"]
        assert ticker_data["status"] == "not_found"
        assert ticker_data["last_checked"] == stale_found

    def test_metadata_preservation(self):
        """Test that additional metadata fields are preserved."""
        yf_parqed = self.create_yf_parqed_instance()

        # Create ticker with extra metadata including intervals
        ticker_with_metadata = {
            "METATICKER": {
                "ticker": "METATICKER",
                "added_date": self.test_date,
                "status": "not_found",
                "last_checked": self.test_date,
                "error_count": 3,
                "custom_field": "custom_value",
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": self.test_date,
                        "last_checked": self.test_date,
                        "retry_count": 5,
                    }
                },
            }
        }
        yf_parqed.tickers = ticker_with_metadata

        # Save and reload
        yf_parqed.save_tickers()
        yf_parqed.load_tickers()

        # All metadata should be preserved
        ticker_data = yf_parqed.tickers["METATICKER"]
        assert ticker_data["error_count"] == 3
        assert ticker_data["custom_field"] == "custom_value"
        assert ticker_data["last_checked"] == self.test_date
        assert ticker_data["intervals"]["1d"]["retry_count"] == 5


def run_tests():
    """Run all tests manually without pytest."""
    test_instance = TestTickerOperations()

    # List of test methods - updated to reflect removed backward compatibility tests
    test_methods = [
        "test_load_tickers_empty_file",
        "test_save_and_load_tickers",
        "test_update_current_list_of_stocks_new_tickers",
        "test_update_current_list_of_stocks_reactivate_not_found",
        "test_is_ticker_active_for_interval",
        "test_is_ticker_active_for_interval_respects_cooldown_window",
        "test_is_ticker_active_for_interval_allows_retry_after_cooldown",
        "test_update_ticker_interval_status_found_data",
        "test_update_ticker_interval_status_no_data",
        "test_update_ticker_interval_status_all_intervals_not_found",
        "test_update_ticker_interval_status_keeps_active_if_any_interval_succeeds",
        "test_not_found_lifecycle_global_transition_after_repeated_failures",
        "test_not_found_lifecycle_reactivation_after_interval_success",
        "test_confirm_not_founds_reactivates_when_history_returns_data",
        "test_confirm_not_founds_keeps_not_found_when_no_history",
        "test_confirm_not_founds_repeated_runs_update_last_checked",
        "test_reparse_not_founds_reactivates_recently_found_interval",
        "test_reparse_not_founds_respects_ninety_day_window",
        "test_data_structure_integrity",
        "test_interval_data_structure",
        "test_file_format_validation",
        "test_concurrent_access_simulation",
        "test_metadata_preservation",
    ]

    passed = 0
    failed = 0

    for method_name in test_methods:
        try:
            test_instance.setup_method()
            method = getattr(test_instance, method_name)
            method()
            test_instance.teardown_method()
            print(f"✅ {method_name}")
            passed += 1
        except Exception as e:
            print(f"❌ {method_name}: {str(e)}")
            failed += 1
            test_instance.teardown_method()

    print(f"\nTest Results: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    # Can be run with or without pytest
    try:
        import pytest

        print("Running with pytest...")
        pytest.main([__file__, "-v"])
    except ImportError:
        print("Running without pytest...")
        success = run_tests()
        exit(0 if success else 1)
