import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from yf_parqed.primary_class import YFParqed


class TestUpdateLoopHarness:
    """Sanity checks for orchestrating update_stock_data with mocks."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_update_stock_data_runs_with_basic_mocks(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        instance.tickers = {
            "DUMMY": {
                "ticker": "DUMMY",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance, "save_single_stock_data", lambda **kwargs: None)
        monkeypatch.setattr(instance, "save_tickers", lambda: None)
        monkeypatch.setattr(instance, "load_tickers", lambda: None)

        instance.update_stock_data()

        # If no exception was raised, the harness works
        assert True

    def test_active_interval_updates_metadata_and_calls_fetch(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        instance.tickers = {
            "ACTIVE": {
                "ticker": "ACTIVE",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        sample_index = pd.MultiIndex.from_tuples(
            [("ACTIVE", pd.Timestamp("2024-01-02"))], names=["stock", "date"]
        )
        sample_df = pd.DataFrame(
            {
                "open": [15.0],
                "high": [16.0],
                "low": [14.5],
                "close": [15.5],
                "volume": [2000],
                "sequence": [1],
            },
            index=sample_index,
        )

        fetch_calls = {"count": 0}

        def fake_get_yfinance_data(**kwargs):
            fetch_calls["count"] += 1
            return sample_df

        saved_payloads = {}

        def fake_save_yf(df1, df2, data_path):
            saved_payloads["df1"] = df1
            saved_payloads["df2"] = df2
            saved_payloads["path"] = data_path
            return df1

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance, "get_yfinance_data", fake_get_yfinance_data)
        monkeypatch.setattr(instance, "save_yf", fake_save_yf)
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        assert fetch_calls["count"] == 1
        assert saved_payloads["path"].name == "ACTIVE.parquet"
        assert saved_payloads["path"].parent.name == "stocks_1d"
        assert list(saved_payloads["df1"].index.names) == ["stock", "date"]
        assert saved_payloads["df2"].empty

        interval_meta = instance.tickers["ACTIVE"]["intervals"]["1d"]
        assert interval_meta["status"] == "active"
        assert interval_meta["last_data_date"] == "2024-01-02"

    def test_cooldown_interval_skips_fetch_calls(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        today_str = datetime.now().strftime("%Y-%m-%d")
        instance.tickers = {
            "COOLDOWN": {
                "ticker": "COOLDOWN",
                "status": "active",
                "last_checked": None,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": today_str,
                        "last_checked": today_str,
                    }
                },
            }
        }

        save_single_calls = {"count": 0}

        def stub_save_single_stock_data(**kwargs):
            save_single_calls["count"] += 1

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(
            instance, "save_single_stock_data", stub_save_single_stock_data
        )
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        assert save_single_calls["count"] == 0
        interval_meta = instance.tickers["COOLDOWN"]["intervals"]["1d"]
        assert interval_meta["status"] == "not_found"
        assert interval_meta["last_not_found_date"] == today_str

    @pytest.mark.parametrize(
        "days_since, expected_invocations",
        [
            (29, 0),  # still inside cooldown window
            (30, 1),  # threshold day should allow retry
            (45, 1),  # comfortably outside window
        ],
    )
    def test_cooldown_boundary_behavior(
        self, monkeypatch, days_since, expected_invocations
    ):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        last_not_found = (datetime.now() - timedelta(days=days_since)).strftime(
            "%Y-%m-%d"
        )
        instance.tickers = {
            "BOUNDARY": {
                "ticker": "BOUNDARY",
                "status": "active",
                "last_checked": None,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": last_not_found,
                        "last_checked": last_not_found,
                    }
                },
            }
        }

        save_calls = {"count": 0}

        def track_save_single_stock_data(**_kwargs):
            save_calls["count"] += 1

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(
            instance, "save_single_stock_data", track_save_single_stock_data
        )
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        assert save_calls["count"] == expected_invocations

    def test_cooldown_malformed_timestamp_allows_retry(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        instance.tickers = {
            "MALFORMED": {
                "ticker": "MALFORMED",
                "status": "active",
                "last_checked": None,
                "intervals": {
                    "1d": {
                        "status": "not_found",
                        "last_not_found_date": "20-02-2024",  # invalid format
                        "last_checked": "20-02-2024",
                    }
                },
            }
        }

        save_calls = {"count": 0}

        def track_save_single_stock_data(**_kwargs):
            save_calls["count"] += 1

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(
            instance, "save_single_stock_data", track_save_single_stock_data
        )
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        assert save_calls["count"] == 1

    def test_empty_fetch_marks_interval_not_found(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        instance.tickers = {
            "EMPTY": {
                "ticker": "EMPTY",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        def fake_get_yfinance_data(**kwargs):
            return pd.DataFrame()

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance, "get_yfinance_data", fake_get_yfinance_data)
        monkeypatch.setattr(instance, "save_yf", lambda df1, df2, path: df2)
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        interval_meta = instance.tickers["EMPTY"]["intervals"]["1d"]
        assert interval_meta["status"] == "not_found"
        assert instance.new_not_found is True

    def test_multi_interval_sequencing_handles_mixed_outcomes(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d", "1h"])
        instance.tickers = {
            "MULTI": {
                "ticker": "MULTI",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        sample_index = pd.MultiIndex.from_tuples(
            [("MULTI", pd.Timestamp("2024-03-01"))], names=["stock", "date"]
        )
        success_df = pd.DataFrame(
            {
                "open": [30.0],
                "high": [31.0],
                "low": [29.5],
                "close": [30.5],
                "volume": [5000],
                "sequence": [1],
            },
            index=sample_index,
        )

        call_order: list[str] = []

        def fake_get_yfinance_data(interval, **kwargs):
            call_order.append(interval)
            if interval == "1d":
                return success_df
            return pd.DataFrame()

        save_calls: list[str] = []

        def fake_save_yf(df1, df2, data_path):
            save_calls.append(data_path.name)
            return df1

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance, "get_yfinance_data", fake_get_yfinance_data)
        monkeypatch.setattr(instance, "save_yf", fake_save_yf)
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        assert call_order == ["1d", "1h"]
        assert save_calls == ["MULTI.parquet"]

        intervals = instance.tickers["MULTI"]["intervals"]
        assert intervals["1d"]["status"] == "active"
        assert intervals["1d"]["last_data_date"] == "2024-03-01"
        assert intervals["1h"]["status"] == "not_found"

    def test_save_tickers_not_called_during_update_loop(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        instance.tickers = {
            "PERSIST": {
                "ticker": "PERSIST",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        sample_index = pd.MultiIndex.from_tuples(
            [("PERSIST", pd.Timestamp("2024-04-01"))], names=["stock", "date"]
        )
        sample_df = pd.DataFrame(
            {
                "open": [40.0],
                "high": [41.0],
                "low": [39.5],
                "close": [40.5],
                "volume": [6000],
                "sequence": [1],
            },
            index=sample_index,
        )

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance, "get_yfinance_data", lambda **_: sample_df)
        monkeypatch.setattr(instance, "save_yf", lambda df1, df2, path: df1)

        save_ticker_calls = {"count": 0}

        def track_save_tickers():
            save_ticker_calls["count"] += 1

        monkeypatch.setattr(instance, "save_tickers", track_save_tickers)

        instance.update_stock_data()

        assert save_ticker_calls["count"] == 0

    def test_enforce_limits_runs_for_each_processed_ticker(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d", "1h"])
        instance.tickers = {
            "AAA": {
                "ticker": "AAA",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            },
            "BBB": {
                "ticker": "BBB",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            },
        }

        def make_df(symbol: str, open_val: float):
            idx = pd.MultiIndex.from_tuples(
                [(symbol, pd.Timestamp("2024-05-01"))], names=["stock", "date"]
            )
            return pd.DataFrame(
                {
                    "open": [open_val],
                    "high": [open_val + 1],
                    "low": [open_val - 0.5],
                    "close": [open_val + 0.5],
                    "volume": [7000],
                    "sequence": [1],
                },
                index=idx,
            )

        data_map = {"AAA": make_df("AAA", 50.0), "BBB": make_df("BBB", 60.0)}

        limit_calls = {"count": 0}

        def track_limits():
            limit_calls["count"] += 1

        def fake_get_yfinance_data(stock, interval, **kwargs):
            if interval == "1d":
                return data_map[stock]
            return pd.DataFrame()

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", track_limits)
        monkeypatch.setattr(instance, "get_yfinance_data", fake_get_yfinance_data)
        monkeypatch.setattr(instance, "save_yf", lambda df1, df2, path: df1)
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        # Two tickers across two intervals => enforce_limits called four times
        assert limit_calls["count"] == 4
