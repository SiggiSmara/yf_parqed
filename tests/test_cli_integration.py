import json
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from yf_parqed import yfinance_cli as main
from yf_parqed.primary_class import YFParqed


@pytest.fixture
def cli_environment(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory()
    tmp_path = Path(temp_dir.name)

    runner = CliRunner()

    original_instance = main.yf_parqed
    original_intervals = main.all_intervals
    instance = YFParqed(my_path=tmp_path, my_intervals=["1m"])
    main.yf_parqed = instance

    monkeypatch.setattr(main, "all_intervals", ["1m"])

    def fake_get_new_list_of_stocks(self, download_tickers=True):
        return {
            "SYN": {
                "ticker": "SYN",
                "added_date": "2024-01-01",
                "status": "active",
                "last_checked": None,
                "intervals": {},
            }
        }

    def fake_fetch(
        stock: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1m",
        get_all: bool = False,
    ):
        date_index = pd.MultiIndex.from_tuples(
            [(stock, pd.Timestamp("2024-01-02"))], names=["stock", "date"]
        )
        return pd.DataFrame(
            {
                "open": [10.0],
                "high": [11.0],
                "low": [9.5],
                "close": [10.5],
                "volume": [1000],
                "sequence": [1],
            },
            index=date_index,
        )

    monkeypatch.setattr(
        YFParqed, "get_new_list_of_stocks", fake_get_new_list_of_stocks, raising=False
    )
    # Patch the data_fetcher.fetch method on the instance
    monkeypatch.setattr(instance.data_fetcher, "fetch", fake_fetch)
    monkeypatch.setattr(YFParqed, "enforce_limits", lambda self: None, raising=False)

    try:
        yield runner, instance, tmp_path
    finally:
        main.yf_parqed = original_instance
        main.all_intervals = original_intervals
        temp_dir.cleanup()


def test_cli_initialize_and_update_flow(cli_environment):
    runner, instance, tmp_path = cli_environment

    init_result = runner.invoke(main.app, ["--wrk-dir", str(tmp_path), "initialize"])
    assert init_result.exit_code == 0

    tickers_path = tmp_path / "tickers.json"
    assert tickers_path.exists()
    tickers = json.loads(tickers_path.read_text())
    assert "SYN" in tickers

    update_result = runner.invoke(
        main.app,
        [
            "--wrk-dir",
            str(tmp_path),
            "update-data",
            "--non-interactive",
            "--save-not-founds",
        ],
    )
    assert update_result.exit_code == 0

    # Verify ticker was updated successfully
    interval_meta = instance.tickers["SYN"]["intervals"]["1m"]
    assert interval_meta["status"] == "active"
    assert interval_meta["last_data_date"] == "2024-01-02"
    
    # Verify storage_config.json was created with partitioned mode
    storage_config_path = tmp_path / "storage_config.json"
    assert storage_config_path.exists()
    storage_config = json.loads(storage_config_path.read_text())
    assert storage_config["partitioned"] is True

    assert instance.my_intervals == ["1m"]
    assert instance.new_not_found is False
