"""Tests for ISINMapper lookup service."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from yf_parqed.xetra.isin_mapper import ISINMapper


def _write_cache(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _cache_row(isin: str, ticker: str, status: str = "active") -> dict:
    return {
        "isin": isin,
        "ticker": ticker,
        "name": f"Company {ticker}",
        "currency": "EUR",
        "wkn": "000000",
        "status": status,
        "first_seen": date(2025, 10, 1),
        "last_seen": date.today(),
        "source": "deutsche_boerse_csv",
    }


def test_get_ticker_hit(tmp_path):
    cache_path = tmp_path / "isin_mapping.parquet"
    _write_cache(cache_path, [
        _cache_row("DE0005140008", "DBK"),
        _cache_row("DE0007236101", "SIE"),
    ])

    mapper = ISINMapper(cache_path)
    assert mapper.get_ticker("DE0005140008") == "DBK"
    assert mapper.get_ticker("DE0007236101") == "SIE"


def test_get_ticker_miss(tmp_path):
    cache_path = tmp_path / "isin_mapping.parquet"
    _write_cache(cache_path, [_cache_row("DE0005140008", "DBK")])

    mapper = ISINMapper(cache_path)
    assert mapper.get_ticker("US0378331005") is None


def test_get_ticker_inactive_not_returned(tmp_path):
    """Inactive ISINs must not be returned by get_ticker."""
    cache_path = tmp_path / "isin_mapping.parquet"
    _write_cache(cache_path, [
        _cache_row("DE0005140008", "DBK", status="active"),
        _cache_row("DE9999999999", "OLD", status="inactive"),
    ])

    mapper = ISINMapper(cache_path)
    assert mapper.get_ticker("DE0005140008") == "DBK"
    assert mapper.get_ticker("DE9999999999") is None


def test_missing_cache_returns_none(tmp_path):
    cache_path = tmp_path / "does_not_exist.parquet"
    mapper = ISINMapper(cache_path)
    assert mapper.get_ticker("DE0005140008") is None


def test_reload_picks_up_new_mapping(tmp_path):
    """reload() makes newly added ISINs visible without re-instantiating."""
    cache_path = tmp_path / "isin_mapping.parquet"
    _write_cache(cache_path, [_cache_row("DE0005140008", "DBK")])

    mapper = ISINMapper(cache_path)
    assert mapper.get_ticker("DE0007236101") is None

    # Now add SIE to the cache
    _write_cache(cache_path, [
        _cache_row("DE0005140008", "DBK"),
        _cache_row("DE0007236101", "SIE"),
    ])
    mapper.reload()
    assert mapper.get_ticker("DE0007236101") == "SIE"


def test_reload_from_missing_cache_clears_mapping(tmp_path):
    """If cache disappears between load and reload, mapping is cleared."""
    cache_path = tmp_path / "isin_mapping.parquet"
    _write_cache(cache_path, [_cache_row("DE0005140008", "DBK")])

    mapper = ISINMapper(cache_path)
    assert mapper.get_ticker("DE0005140008") == "DBK"

    cache_path.unlink()
    mapper.reload()
    assert mapper.get_ticker("DE0005140008") is None
