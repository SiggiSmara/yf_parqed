"""Tests for ISINMappingUpdater: scraper, parser, and cache merger."""

import io
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from yf_parqed.xetra.isin_mapping_updater import ISINMappingUpdater, _make_absolute


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_HTML_WITH_ABSOLUTE_LINK = """
<html><body>
<a href="https://www.xetra.com/resource/blob/1528/abc123/data/t7-xetr-allTradableInstruments.csv">
  Download all tradable instruments
</a>
</body></html>
"""

_HTML_WITH_RELATIVE_LINK = """
<html><body>
<a href="/resource/blob/9999/def456/data/t7-xetr-allTradableInstruments.csv">All instruments</a>
</body></html>
"""

_HTML_WITH_PROTOCOL_RELATIVE_LINK = """
<html><body>
<a href="//www.xetra.com/resource/blob/7777/ghi789/data/t7-xetr-allTradableInstruments.csv">CSV</a>
</body></html>
"""

_HTML_NO_CSV_LINK = """
<html><body>
<a href="/some/other/page.html">Other link</a>
<a href="/instruments-statistics/overview">Overview</a>
</body></html>
"""

_MINIMAL_CSV = """Market:;XETR
Date Last Update:;26.04.2026
Product Status;Instrument Status;Instrument;ISIN;Product ID;Instrument ID;WKN;Mnemonic;MIC Code;Currency
Active;Active;DEUTSCHE BANK AG NA O.N.;DE0005140008;;;514000;DBK;XETR;EUR
Active;Active;SIEMENS AG NA;DE0007236101;;;723610;SIE;XETR;EUR
Inactive;Active;OLD CORP;DE0000000099;;;000099;OLD;XETR;EUR
Active;Inactive;INACTIVE CORP;DE0000000098;;;000098;INA;XETR;EUR
Active;Active;  TRIMMED CORP  ;AT000000STR1;;;AB1234;XD4;XETR;EUR
"""


def _make_response(content: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = content
    resp.content = content.encode("utf-8")
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# _make_absolute helper
# ---------------------------------------------------------------------------


def test_make_absolute_keeps_absolute():
    url = "https://www.xetra.com/resource/blob/123/abc/data/t7-xetr-allTradableInstruments.csv"
    assert _make_absolute(url) == url


def test_make_absolute_expands_relative():
    assert _make_absolute("/resource/blob/1/a/data/t7-xetr-allTradableInstruments.csv") == (
        "https://www.xetra.com/resource/blob/1/a/data/t7-xetr-allTradableInstruments.csv"
    )


def test_make_absolute_expands_protocol_relative():
    result = _make_absolute("//www.xetra.com/path/t7-xetr-allTradableInstruments.csv")
    assert result.startswith("https:")


# ---------------------------------------------------------------------------
# get_csv_url
# ---------------------------------------------------------------------------


def test_get_csv_url_absolute_link():
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(_HTML_WITH_ABSOLUTE_LINK)

    url = updater.get_csv_url()
    assert "alltradableinstruments.csv" in url.lower()
    assert url.startswith("https://")


def test_get_csv_url_relative_link():
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(_HTML_WITH_RELATIVE_LINK)

    url = updater.get_csv_url()
    assert url.startswith("https://www.xetra.com/")
    assert "alltradableinstruments.csv" in url.lower()


def test_get_csv_url_protocol_relative_link():
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(_HTML_WITH_PROTOCOL_RELATIVE_LINK)

    url = updater.get_csv_url()
    assert url.startswith("https:")


def test_get_csv_url_raises_when_no_link():
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(_HTML_NO_CSV_LINK)

    with pytest.raises(ValueError, match="Could not find CSV download link"):
        updater.get_csv_url()


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


def test_download_and_parse_returns_only_active():
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(_MINIMAL_CSV)

    df = updater.download_and_parse("https://example.com/fake.csv")

    isins = set(df["isin"])
    # Active+Active rows only
    assert "DE0005140008" in isins  # DBK
    assert "DE0007236101" in isins  # SIE
    assert "AT000000STR1" in isins  # TRIMMED
    # Inactive rows excluded
    assert "DE0000000099" not in isins  # Product Status = Inactive
    assert "DE0000000098" not in isins  # Instrument Status = Inactive


def test_download_and_parse_strips_whitespace():
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(_MINIMAL_CSV)

    df = updater.download_and_parse("https://example.com/fake.csv")
    trimmed_row = df[df["isin"] == "AT000000STR1"]
    assert len(trimmed_row) == 1
    assert trimmed_row.iloc[0]["name"] == "TRIMMED CORP"


def test_download_and_parse_sets_status_source_last_seen():
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(_MINIMAL_CSV)

    df = updater.download_and_parse("https://example.com/fake.csv")
    assert (df["status"] == "active").all()
    assert (df["source"] == "deutsche_boerse_csv").all()
    assert (df["last_seen"] == date.today()).all()


def test_download_and_parse_raises_on_missing_columns():
    bad_csv = "Market:;XETR\nDate Last Update:;26.04.2026\nOnlyColumn\nvalue\n"
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(bad_csv)

    with pytest.raises(ValueError, match="missing expected columns"):
        updater.download_and_parse("https://example.com/fake.csv")


def test_download_and_parse_filters_invalid_isin_length():
    short_isin_csv = """Market:;XETR
Date Last Update:;26.04.2026
Product Status;Instrument Status;Instrument;ISIN;Product ID;Instrument ID;WKN;Mnemonic;MIC Code;Currency
Active;Active;VALID CO;DE0005140008;;;111111;VLD;XETR;EUR
Active;Active;SHORT CO;SHORT123;;;222222;SHT;XETR;EUR
"""
    updater = ISINMappingUpdater()
    updater.client = MagicMock()
    updater.client.get.return_value = _make_response(short_isin_csv)

    df = updater.download_and_parse("https://example.com/fake.csv")
    assert "DE0005140008" in set(df["isin"])
    assert "SHORT123" not in set(df["isin"])


# ---------------------------------------------------------------------------
# merge_with_cache
# ---------------------------------------------------------------------------


def _make_cache(tmp_path: Path, rows: list[dict]) -> Path:
    cache_path = tmp_path / "data" / "reference" / "isin_mapping.parquet"
    cache_path.parent.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(cache_path, index=False)
    return cache_path


def _make_new_data(isins: list[str]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "isin": isin,
            "ticker": f"T{i}",
            "name": f"Company {i}",
            "currency": "EUR",
            "wkn": f"WKN{i:06d}",
            "status": "active",
            "last_seen": date.today(),
            "source": "deutsche_boerse_csv",
        }
        for i, isin in enumerate(isins, 1)
    ])


def test_merge_first_run_all_new(tmp_path):
    """No existing cache — every ISIN is new."""
    cache_path = tmp_path / "data" / "reference" / "isin_mapping.parquet"

    updater = ISINMappingUpdater()
    new_data = _make_new_data(["DE0005140008", "DE0007236101"])
    result = updater.merge_with_cache(new_data, cache_path)

    assert len(result) == 2
    assert set(result["isin"]) == {"DE0005140008", "DE0007236101"}
    assert (result["first_seen"] == date.today()).all()
    assert (result["last_seen"] == date.today()).all()
    assert (result["status"] == "active").all()


def test_merge_new_isin_gets_first_seen_today(tmp_path):
    """A genuinely new ISIN gets first_seen = today."""
    cache_path = _make_cache(tmp_path, [
        {
            "isin": "DE0005140008",
            "ticker": "DBK",
            "name": "DEUTSCHE BANK",
            "currency": "EUR",
            "wkn": "514000",
            "status": "active",
            "first_seen": date(2025, 10, 1),
            "last_seen": date(2025, 10, 1),
            "source": "deutsche_boerse_csv",
        }
    ])

    updater = ISINMappingUpdater()
    new_data = _make_new_data(["DE0005140008", "DE0007236101"])  # SIE is new
    result = updater.merge_with_cache(new_data, cache_path)

    sie_row = result[result["isin"] == "DE0007236101"].iloc[0]
    assert sie_row["first_seen"] == date.today()


def test_merge_existing_isin_keeps_first_seen(tmp_path):
    """Existing ISINs preserve their original first_seen."""
    old_first_seen = date(2025, 10, 1)
    cache_path = _make_cache(tmp_path, [
        {
            "isin": "DE0005140008",
            "ticker": "DBK",
            "name": "DEUTSCHE BANK",
            "currency": "EUR",
            "wkn": "514000",
            "status": "active",
            "first_seen": old_first_seen,
            "last_seen": date(2025, 10, 1),
            "source": "deutsche_boerse_csv",
        }
    ])

    updater = ISINMappingUpdater()
    new_data = _make_new_data(["DE0005140008"])
    result = updater.merge_with_cache(new_data, cache_path)

    dbk_row = result[result["isin"] == "DE0005140008"].iloc[0]
    assert dbk_row["first_seen"] == old_first_seen
    assert dbk_row["last_seen"] == date.today()
    assert dbk_row["status"] == "active"


def test_merge_delisted_isin_marked_inactive(tmp_path):
    """ISINs present in cache but absent from new CSV are marked inactive."""
    cache_path = _make_cache(tmp_path, [
        {
            "isin": "DE0005140008",
            "ticker": "DBK",
            "name": "DEUTSCHE BANK",
            "currency": "EUR",
            "wkn": "514000",
            "status": "active",
            "first_seen": date(2025, 10, 1),
            "last_seen": date(2025, 10, 1),
            "source": "deutsche_boerse_csv",
        },
        {
            "isin": "DE9999999999",
            "ticker": "OLD",
            "name": "OLD CORP",
            "currency": "EUR",
            "wkn": "999999",
            "status": "active",
            "first_seen": date(2025, 10, 1),
            "last_seen": date(2025, 10, 1),
            "source": "deutsche_boerse_csv",
        },
    ])

    updater = ISINMappingUpdater()
    # New CSV only has DBK, not OLD
    new_data = _make_new_data(["DE0005140008"])
    result = updater.merge_with_cache(new_data, cache_path)

    old_row = result[result["isin"] == "DE9999999999"].iloc[0]
    assert old_row["status"] == "inactive"

    dbk_row = result[result["isin"] == "DE0005140008"].iloc[0]
    assert dbk_row["status"] == "active"


def test_merge_reactivates_previously_inactive(tmp_path):
    """An ISIN that was inactive and reappears in the CSV becomes active again."""
    cache_path = _make_cache(tmp_path, [
        {
            "isin": "DE0005140008",
            "ticker": "DBK",
            "name": "DEUTSCHE BANK",
            "currency": "EUR",
            "wkn": "514000",
            "status": "inactive",
            "first_seen": date(2025, 10, 1),
            "last_seen": date(2025, 11, 1),
            "source": "deutsche_boerse_csv",
        }
    ])

    updater = ISINMappingUpdater()
    new_data = _make_new_data(["DE0005140008"])
    result = updater.merge_with_cache(new_data, cache_path)

    dbk_row = result[result["isin"] == "DE0005140008"].iloc[0]
    assert dbk_row["status"] == "active"
    assert dbk_row["last_seen"] == date.today()


def test_merge_result_sorted_by_isin(tmp_path):
    """Merged result is sorted by ISIN ascending."""
    cache_path = tmp_path / "data" / "reference" / "isin_mapping.parquet"
    updater = ISINMappingUpdater()
    new_data = _make_new_data(["DE0007236101", "AT000000STR1", "DE0005140008"])
    result = updater.merge_with_cache(new_data, cache_path)

    isins = result["isin"].tolist()
    assert isins == sorted(isins)


# ---------------------------------------------------------------------------
# run (end-to-end integration of all steps)
# ---------------------------------------------------------------------------


def test_run_writes_parquet(tmp_path):
    updater = ISINMappingUpdater()
    updater.client = MagicMock()

    page_resp = _make_response(_HTML_WITH_ABSOLUTE_LINK)
    csv_resp = _make_response(_MINIMAL_CSV)
    updater.client.get.side_effect = [page_resp, csv_resp]

    cache_path = tmp_path / "data" / "reference" / "isin_mapping.parquet"
    updater.run(cache_path)

    assert cache_path.exists()
    df = pd.read_parquet(cache_path)
    assert len(df) > 0
    assert "isin" in df.columns
    assert "ticker" in df.columns
