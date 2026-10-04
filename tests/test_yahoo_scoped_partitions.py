"""Steps B and C of the daemon resource footprint ADR, through the Yahoo service.

A cycle fetches a few recent days. With partitioned storage it should open and
rewrite only the monthly files those days fall into, rewrite nothing when the
fetched bars are already stored, and open nothing when the fetch is empty.
"""

import os
from datetime import datetime

import pandas as pd
import pytest

from yf_parqed.yahoo.primary_class import YFParqed


def _bars(dates: list[str], ticker: str = "AAPL", close: float = 1.0) -> pd.DataFrame:
    """Bars shaped like the fetcher's output: no ``sequence`` column."""
    index = pd.MultiIndex.from_tuples(
        [(ticker, pd.Timestamp(d)) for d in dates], names=["stock", "date"]
    )
    return pd.DataFrame(
        {"open": close, "high": close, "low": close, "close": close, "volume": 10},
        index=index,
    )


@pytest.fixture()
def yf(tmp_path, monkeypatch):
    instance = YFParqed(my_path=tmp_path, my_intervals=["1d"])
    instance.tickers = {
        "AAPL": {
            "ticker": "AAPL",
            "status": "active",
            "last_checked": None,
            "intervals": {},
        }
    }
    monkeypatch.setattr(instance, "get_today", lambda: datetime(2024, 3, 6, 17, 0))
    return instance


def _fetch_returns(yf, monkeypatch, frame):
    monkeypatch.setattr(yf.data_fetcher, "fetch", lambda **kwargs: frame)


@pytest.fixture()
def stored(yf, tmp_path, monkeypatch):
    """January to March on disk, files aged by an hour; returns a 'changed' check."""
    _fetch_returns(yf, monkeypatch, _bars(["2024-01-10", "2024-02-12", "2024-03-04"]))
    yf.save_single_stock_data("AAPL", interval="1d")
    root = tmp_path / "data/us/yahoo/stocks_1d/ticker=AAPL/year=2024"
    files = {m: root / f"month={m:02d}/data.parquet" for m in (1, 2, 3)}
    for path in files.values():
        aged = path.stat().st_mtime - 3600
        os.utime(path, (aged, aged))
    before = {m: (p.stat().st_mtime_ns, p.read_bytes()) for m, p in files.items()}

    def changed() -> list[int]:
        return [
            m
            for m, p in files.items()
            if (p.stat().st_mtime_ns, p.read_bytes()) != before[m]
        ]

    return files, changed


def test_cycle_rewrites_only_the_month_of_the_new_bars(yf, stored, monkeypatch):
    files, changed = stored
    _fetch_returns(yf, monkeypatch, _bars(["2024-03-04", "2024-03-05"], close=2.0))

    yf.save_single_stock_data("AAPL", interval="1d")

    assert changed() == [3]
    march = pd.read_parquet(files[3]).set_index("date")
    assert sorted(march.index) == [
        pd.Timestamp("2024-03-04"),
        pd.Timestamp("2024-03-05"),
    ]
    assert (march["close"] == 2.0).all()


def test_cycle_that_refetches_stored_bars_rewrites_nothing(yf, stored, monkeypatch):
    """What every cycle does outside trading hours until Step D lands."""
    _, changed = stored
    _fetch_returns(yf, monkeypatch, _bars(["2024-02-12", "2024-03-04"]))

    yf.save_single_stock_data("AAPL", interval="1d")

    assert changed() == []


def test_cycle_opens_only_the_months_of_the_new_bars(yf, stored, monkeypatch):
    opened = []
    backend = yf._partition_storage
    original = backend._read_month
    monkeypatch.setattr(
        backend,
        "_read_month",
        lambda request, month: opened.append(month) or original(request, month),
    )
    monkeypatch.setattr(
        backend, "read", lambda *a, **k: pytest.fail("the whole ticker was read")
    )
    _fetch_returns(yf, monkeypatch, _bars(["2024-02-29", "2024-03-01"]))

    yf.save_single_stock_data("AAPL", interval="1d")

    assert opened == [pd.Period("2024-02"), pd.Period("2024-03")]


def test_cycle_opens_nothing_when_the_fetch_returns_nothing(yf, stored, monkeypatch):
    backend = yf._partition_storage
    for name in ("read", "merge", "save"):
        monkeypatch.setattr(
            backend, name, lambda *a, _n=name, **k: pytest.fail(f"{_n} was called")
        )
    _fetch_returns(yf, monkeypatch, yf._empty_price_frame())

    yf.save_single_stock_data("AAPL", interval="1d")
