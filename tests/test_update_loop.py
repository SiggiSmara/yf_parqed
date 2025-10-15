import json
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from yf_parqed.partitioned_storage_backend import PartitionedStorageBackend
from yf_parqed.primary_class import YFParqed
from yf_parqed.storage_backend import StorageBackend, StorageRequest


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

    def test_default_storage_backend_is_legacy(self):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        assert isinstance(instance.storage, StorageBackend)

    def test_partitioned_backend_selected_when_flag_enabled(self):
        config_path = self.temp_dir / "storage_config.json"
        config_path.write_text(
            json.dumps(
                {
                    "partitioned": True,
                    "markets": {},
                    "sources": {},
                }
            )
        )

        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        assert isinstance(instance._partition_storage, PartitionedStorageBackend)

    def test_partitioned_interval_uses_partition_backend(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        instance.tickers = {
            "PART": {
                "ticker": "PART",
                "status": "active",
                "intervals": {
                    "1d": {
                        "status": "active",
                        "storage": {
                            "mode": "partitioned",
                            "market": "us",
                            "source": "yahoo",
                            "dataset": "stocks",
                            "root": "data",
                            "verified_at": "2025-10-15T00:00:00Z",
                        },
                    }
                },
            }
        }

        sample_index = pd.MultiIndex.from_tuples(
            [("PART", pd.Timestamp("2024-04-01"))], names=["stock", "date"]
        )
        sample_df = pd.DataFrame(
            {
                "open": [25.0],
                "high": [26.0],
                "low": [24.5],
                "close": [25.5],
                "volume": [4000],
                "sequence": [0],
            },
            index=sample_index,
        )

        captured_reads = []
        captured_saves = []

        def record_read(request):
            captured_reads.append(request)
            return YFParqed._empty_price_frame()

        def record_save(request, new_data, existing_data):
            captured_saves.append(request)
            return new_data

        def fail_legacy_read(_request):
            raise AssertionError(
                "Legacy backend should not be used for partitioned interval"
            )

        def fail_legacy_save(_request, _new, _existing):
            raise AssertionError(
                "Legacy backend should not be used for partitioned interval"
            )

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance.data_fetcher, "fetch", lambda **__: sample_df)
        monkeypatch.setattr(instance._partition_storage, "read", record_read)
        monkeypatch.setattr(instance._partition_storage, "save", record_save)
        monkeypatch.setattr(instance._legacy_storage, "read", fail_legacy_read)
        monkeypatch.setattr(instance._legacy_storage, "save", fail_legacy_save)
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.save_single_stock_data("PART", interval="1d")

        assert captured_reads and captured_saves
        request = captured_saves[0]
        assert request.market == "us"
        assert request.source == "yahoo"

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

        def fake_fetch(**kwargs):
            fetch_calls["count"] += 1
            return sample_df

        saved_payloads = {}

        def fake_save_yf(df1, df2, request):
            saved_payloads["df1"] = df1
            saved_payloads["df2"] = df2
            saved_payloads["request"] = request
            saved_payloads["path"] = request.legacy_path()
            return df1

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance.data_fetcher, "fetch", fake_fetch)
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

        def fake_fetch(**kwargs):
            return pd.DataFrame()

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance.data_fetcher, "fetch", fake_fetch)
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

        def fake_fetch(interval, **kwargs):
            call_order.append(interval)
            if interval == "1d":
                return success_df
            return pd.DataFrame()

        save_calls: list[str] = []

        def fake_save_yf(df1, df2, request):
            save_calls.append(request.legacy_path().name)
            return df1

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", lambda: None)
        monkeypatch.setattr(instance.data_fetcher, "fetch", fake_fetch)
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
        monkeypatch.setattr(instance.data_fetcher, "fetch", lambda **_: sample_df)
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

        def fake_fetch(stock, interval, **kwargs):
            if interval == "1d":
                return data_map[stock]
            return pd.DataFrame()

        monkeypatch.setattr(instance, "load_tickers", lambda: None)
        monkeypatch.setattr(instance, "enforce_limits", track_limits)
        monkeypatch.setattr(instance.data_fetcher, "fetch", fake_fetch)
        monkeypatch.setattr(instance, "save_yf", lambda df1, df2, path: df1)
        monkeypatch.setattr(instance, "save_tickers", lambda: None)

        instance.update_stock_data()

        # Two tickers across two intervals => enforce_limits called four times
        assert limit_calls["count"] == 4

    def test_save_single_stock_data_uses_registry_last_data_date(self, monkeypatch):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        metadata_date = "2024-02-05"
        instance.tickers = {
            "REG": {
                "ticker": "REG",
                "status": "active",
                "last_checked": None,
                "intervals": {
                    "1d": {
                        "status": "active",
                        "last_data_date": metadata_date,
                        "last_found_date": "2024-02-05",
                    }
                },
            }
        }

        sample_index = pd.MultiIndex.from_tuples(
            [("REG", pd.Timestamp("2024-02-06"))], names=["stock", "date"]
        )
        sample_df = pd.DataFrame(
            {
                "open": [25.0],
                "high": [26.0],
                "low": [24.5],
                "close": [25.5],
                "volume": [4000],
                "sequence": [1],
            },
            index=sample_index,
        )

        fetch_args = {}

        def fake_fetch(**kwargs):
            fetch_args.update(kwargs)
            return sample_df

        monkeypatch.setattr(instance, "get_today", lambda: datetime(2024, 2, 6, 17, 0))
        monkeypatch.setattr(
            instance, "read_yf", lambda _path: instance._empty_price_frame()
        )
        monkeypatch.setattr(instance.data_fetcher, "fetch", fake_fetch)
        monkeypatch.setattr(instance, "save_yf", lambda df1, df2, path: df1)

        instance.save_single_stock_data("REG", interval="1d")

        assert fetch_args["get_all"] is False
        assert fetch_args["start_date"].strftime("%Y-%m-%d") == metadata_date

    def test_save_single_stock_data_fetches_all_when_metadata_missing(
        self, monkeypatch
    ):
        instance = YFParqed(my_path=self.temp_dir, my_intervals=["1d"])
        instance.tickers = {
            "NEW": {
                "ticker": "NEW",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        sample_index = pd.MultiIndex.from_tuples(
            [("NEW", pd.Timestamp("2024-02-10"))], names=["stock", "date"]
        )
        sample_df = pd.DataFrame(
            {
                "open": [35.0],
                "high": [36.0],
                "low": [34.5],
                "close": [35.5],
                "volume": [5000],
                "sequence": [1],
            },
            index=sample_index,
        )

        fetch_args = {}

        def fake_fetch(**kwargs):
            fetch_args.update(kwargs)
            return sample_df

        monkeypatch.setattr(instance, "get_today", lambda: datetime(2024, 2, 11, 17, 0))
        monkeypatch.setattr(
            instance, "read_yf", lambda _path: instance._empty_price_frame()
        )
        monkeypatch.setattr(instance, "save_yf", lambda df1, df2, path: df1)
        monkeypatch.setattr(instance.data_fetcher, "fetch", fake_fetch)

        instance.save_single_stock_data("NEW", interval="1d")

        assert fetch_args["get_all"] is True
        assert fetch_args["start_date"] is not None

    def test_storage_backend_can_be_injected(self, monkeypatch):
        class DummyStorage:
            def __init__(self, empty_df: pd.DataFrame):
                self.empty = empty_df
                self.read_calls: list[StorageRequest] = []
                self.save_calls: list[
                    tuple[StorageRequest, pd.DataFrame, pd.DataFrame]
                ] = []

            def read(self, request: StorageRequest) -> pd.DataFrame:
                self.read_calls.append(request)
                return self.empty

            def save(
                self,
                request: StorageRequest,
                new_data: pd.DataFrame,
                existing_data: pd.DataFrame,
            ) -> pd.DataFrame:
                self.save_calls.append((request, new_data, existing_data))
                return new_data

        dummy_storage = DummyStorage(YFParqed._empty_price_frame())
        instance = YFParqed(
            my_path=self.temp_dir,
            my_intervals=["1d"],
            storage_backend=dummy_storage,
        )
        instance.tickers = {
            "INJECT": {
                "ticker": "INJECT",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

        sample_index = pd.MultiIndex.from_tuples(
            [("INJECT", pd.Timestamp("2024-02-20"))], names=["stock", "date"]
        )
        sample_df = pd.DataFrame(
            {
                "open": [45.0],
                "high": [46.0],
                "low": [44.5],
                "close": [45.5],
                "volume": [6000],
                "sequence": [1],
            },
            index=sample_index,
        )

        monkeypatch.setattr(instance, "get_today", lambda: datetime(2024, 2, 21, 17, 0))
        monkeypatch.setattr(instance.data_fetcher, "fetch", lambda **_: sample_df)

        instance.save_single_stock_data("INJECT", interval="1d")

        assert dummy_storage.read_calls
        assert dummy_storage.save_calls
