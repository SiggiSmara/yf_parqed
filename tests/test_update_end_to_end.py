import json
from pathlib import Path

import pandas as pd
import pytest

from yf_parqed.yahoo.primary_class import YFParqed


@pytest.fixture
def seeded_workspace(tmp_path: Path) -> Path:
    """Prepare a temporary workspace with baseline config and tickers."""
    intervals_path = tmp_path / "intervals.json"
    tickers_path = tmp_path / "tickers.json"

    intervals_path.write_text(json.dumps(["1d", "1h"]))

    tickers = {
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
    tickers_path.write_text(json.dumps(tickers, indent=4))

    return tmp_path


def build_history_frame(
    symbol: str, dates: list[str], base_price: float
) -> pd.DataFrame:
    index = pd.Index(pd.to_datetime(dates), name="Date")
    data = {
        "Open": [base_price for _ in dates],
        "High": [base_price + 1 for _ in dates],
        "Low": [base_price - 1 for _ in dates],
        "Close": [base_price + 0.5 for _ in dates],
        "Volume": [1_000 * (idx + 1) for idx, _ in enumerate(dates)],
    }
    return pd.DataFrame(data, index=index)


class FakeTicker:
    def __init__(self, symbol: str, history_map: dict[tuple[str, str], pd.DataFrame]):
        self.symbol = symbol
        self.history_map = history_map

    def history(self, *_, interval: str = "1d", **__):
        frame = self.history_map.get((self.symbol, interval))
        if frame is None:
            return pd.DataFrame()
        return frame.copy()


def test_update_stock_data_end_to_end(monkeypatch, seeded_workspace: Path):
    """Exercise the real update loop with deterministic Yahoo responses."""

    history_map = {
        ("AAA", "1d"): build_history_frame("AAA", ["2024-01-01", "2024-01-02"], 100.0),
        ("AAA", "1h"): pd.DataFrame(),  # Force interval-specific not_found path
        ("BBB", "1d"): pd.DataFrame(),
        ("BBB", "1h"): pd.DataFrame(),
    }

    def fake_ticker(symbol: str, *_args, **_kwargs):
        return FakeTicker(symbol, history_map)

    monkeypatch.setattr("yf_parqed.yahoo.primary_class.yf.Ticker", fake_ticker)

    instance = YFParqed(my_path=seeded_workspace)

    instance.update_stock_data()

    # AAA should have parquet output for 1d interval (in partitioned storage)
    # Find the parquet file in the partitioned structure
    parquet_files = list((seeded_workspace / "data" / "us" / "yahoo" / "stocks_1d" / "ticker=AAA").rglob("*.parquet"))
    assert len(parquet_files) > 0, "Expected at least one parquet file for AAA"
    saved_df = pd.read_parquet(parquet_files[0])
    assert saved_df.shape[0] == 2
    assert set(saved_df["date"].dt.strftime("%Y-%m-%d")) == {"2024-01-01", "2024-01-02"}

    # Interval metadata reflects mixed outcomes
    aaa_meta = instance.tickers["AAA"]
    assert aaa_meta["status"] == "active"
    aaa_1d = aaa_meta["intervals"]["1d"]
    assert aaa_1d["status"] == "active"
    assert aaa_1d["last_data_date"] == "2024-01-02"

    aaa_1h = aaa_meta["intervals"]["1h"]
    assert aaa_1h["status"] == "not_found"
    assert aaa_1h["last_checked"] is not None

    # BBB should transition to globally not_found with no parquet artifacts
    bbb_meta = instance.tickers["BBB"]
    assert bbb_meta["status"] == "not_found"
    assert all(
        interval_info["status"] == "not_found"
        for interval_info in bbb_meta["intervals"].values()
    )
    assert not (seeded_workspace / "stocks_1d" / "BBB.parquet").exists()
    assert not (seeded_workspace / "stocks_1h" / "BBB.parquet").exists()

    # The run should mark that at least one ticker hit a not_found outcome
    assert instance.new_not_found is True
