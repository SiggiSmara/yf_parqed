from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import patch

import pytest

from yf_parqed.yahoo.primary_class import YFParqed


@pytest.fixture()
def yfp(tmp_path: Path) -> YFParqed:
    return YFParqed(my_path=tmp_path, my_intervals=["1d", "1h"])


def _write_nasdaq_csv(path: Path, rows: list[tuple[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Symbol", "Security Name"])
        for symbol, name in rows:
            writer.writerow([symbol, name])


def _write_nyse_csv(path: Path, rows: list[tuple[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ACT Symbol", "Company Name"])
        for symbol, name in rows:
            writer.writerow([symbol, name])



@pytest.mark.parametrize("symbol,name", [
    ("ACMEW", "Acme Corp - Warrants"),
    ("ACME", "Acme Corp - Warrant"),
    ("ACMEU", "Acme Corp - Units"),
    ("ACME", "Acme Corp - Unit"),
    ("ACME", "Acme Corp - Rights"),
    ("ACME", "Acme Corp - Right"),
    ("ACME", "American Depositary Shares"),
    ("ACME", "Depositary Shares representing interest"),
    ("ACME", "Depositary Receipts"),
    ("ACME", "American Depositary"),
    ("ACME.WS", "Acme Warrant"),
    ("ACME.RT", "Acme Rights"),
    ("ACME.U", "Acme Units"),
])
def test_is_dead_instrument_detects_dead(symbol: str, name: str) -> None:
    assert YFParqed._is_dead_instrument(symbol, name) is True


@pytest.mark.parametrize("symbol,name", [
    ("AAPL", "Apple Inc."),
    ("MSFT", "Microsoft Corporation"),
    ("BRK.A", "Berkshire Hathaway Inc. Class A"),  # single-letter class designator — not a derivative
    ("BRK.B", "Berkshire Hathaway Inc. Class B"),
    ("AMZN", "Amazon.com Inc."),
    ("META", "Meta Platforms Inc."),
    ("GOOG", "Alphabet Inc."),
])
def test_is_dead_instrument_passes_live_stocks(symbol: str, name: str) -> None:
    assert YFParqed._is_dead_instrument(symbol, name) is False


# ── _parse_csv_tickers / get_new_list_of_stocks ───────────────────────────────

def test_nasdaq_csv_warrants_filtered(yfp: YFParqed, tmp_path: Path) -> None:
    nasdaq = tmp_path / "nasdaq-listed.csv"
    nyse = tmp_path / "nyse-listed.csv"
    _write_nasdaq_csv(nasdaq, [
        ("AAPL", "Apple Inc."),
        ("ACMEW", "Acme Corp - Warrants"),
        ("ACMEU", "Acme Corp - Units"),
    ])
    _write_nyse_csv(nyse, [])

    result = yfp.get_new_list_of_stocks(download_tickers=False)

    assert "AAPL" in result
    assert "ACMEW" not in result
    assert "ACMEU" not in result


def test_nyse_csv_rights_filtered(yfp: YFParqed, tmp_path: Path) -> None:
    nasdaq = tmp_path / "nasdaq-listed.csv"
    nyse = tmp_path / "nyse-listed.csv"
    _write_nasdaq_csv(nasdaq, [])
    _write_nyse_csv(nyse, [
        ("IBM", "International Business Machines Corp."),
        ("IBMR", "IBM Corp - Rights"),
        ("IBMD", "IBM Corp American Depositary Shares"),
    ])

    result = yfp.get_new_list_of_stocks(download_tickers=False)

    assert "IBM" in result
    assert "IBMR" not in result
    assert "IBMD" not in result


def test_dot_symbol_filtered(yfp: YFParqed, tmp_path: Path) -> None:
    nasdaq = tmp_path / "nasdaq-listed.csv"
    nyse = tmp_path / "nyse-listed.csv"
    _write_nasdaq_csv(nasdaq, [
        ("SPAC.WS", "Some SPAC Warrants"),
        ("SPAC.U", "Some SPAC Units"),
        ("GOOD", "Good Company Inc"),
    ])
    _write_nyse_csv(nyse, [])

    result = yfp.get_new_list_of_stocks(download_tickers=False)

    assert "SPAC.WS" not in result
    assert "SPAC.U" not in result
    assert "GOOD" in result


def test_result_has_source_csv_after_update_current_list(yfp: YFParqed, tmp_path: Path) -> None:
    nasdaq = tmp_path / "nasdaq-listed.csv"
    nyse = tmp_path / "nyse-listed.csv"
    _write_nasdaq_csv(nasdaq, [("AAPL", "Apple Inc.")])
    _write_nyse_csv(nyse, [])

    new_tickers = yfp.get_new_list_of_stocks(download_tickers=False)
    yfp.registry.update_current_list(new_tickers)

    assert yfp.registry.tickers["AAPL"]["source"] == "csv"


def test_missing_csv_returns_empty(yfp: YFParqed, tmp_path: Path) -> None:
    result = yfp.get_new_list_of_stocks(download_tickers=False)
    assert result == {}
