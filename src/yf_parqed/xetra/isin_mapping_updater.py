import io
from datetime import date
from pathlib import Path

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

_INSTRUMENTS_PAGE = "https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments"

_REQUIRED_COLUMNS = {
    "Product Status",
    "Instrument Status",
    "Instrument",
    "ISIN",
    "Mnemonic",
    "Currency",
    "WKN",
}

_CACHE_COLUMNS = [
    "isin",
    "ticker",
    "name",
    "currency",
    "wkn",
    "status",
    "first_seen",
    "last_seen",
    "source",
]


def _empty_cache() -> pd.DataFrame:
    return pd.DataFrame(columns=_CACHE_COLUMNS)


class ISINMappingUpdater:
    """Scrape, download, parse and merge the Deutsche Börse ISIN→ticker CSV."""

    def __init__(self, timeout: int = 60):
        self.client = httpx.Client(timeout=timeout)

    # ------------------------------------------------------------------
    # Step 1: scrape the instruments page to find the current CSV URL
    # ------------------------------------------------------------------

    def get_csv_url(self) -> str:
        response = self.client.get(_INSTRUMENTS_PAGE)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        for link in soup.find_all("a", href=True):
            href: str = link["href"]
            if "alltradableinstruments.csv" in href.lower():
                return _make_absolute(href)
        raise ValueError(
            "Could not find CSV download link on Deutsche Börse instruments page"
        )

    # ------------------------------------------------------------------
    # Step 2: download the CSV and parse it into a DataFrame
    # ------------------------------------------------------------------

    def download_and_parse(self, url: str) -> pd.DataFrame:
        response = self.client.get(url)
        response.raise_for_status()

        content = response.content.decode("utf-8", errors="replace")
        # First 2 rows are metadata ("Market: XETR", "Date Last Update: …")
        df = pd.read_csv(io.StringIO(content), sep=";", skiprows=2, dtype=str).fillna(
            ""
        )

        missing = _REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing expected columns: {missing}")

        df = df.rename(
            columns={
                "ISIN": "isin",
                "Mnemonic": "ticker",
                "Instrument": "name",
                "Currency": "currency",
                "WKN": "wkn",
            }
        )
        for col in ["isin", "ticker", "name", "currency", "wkn"]:
            df[col] = df[col].str.strip()

        mask = (
            (df["Product Status"].str.strip() == "Active")
            & (df["Instrument Status"].str.strip() == "Active")
            & (df["isin"].str.len() == 12)
            & (df["ticker"].str.len() > 0)
        )
        df = df.loc[mask, ["isin", "ticker", "name", "currency", "wkn"]].copy()
        df["status"] = "active"
        df["last_seen"] = date.today()
        df["source"] = "deutsche_boerse_csv"

        logger.info(f"Parsed {len(df)} active instruments from CSV")
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Step 3: merge with the existing Parquet cache
    # ------------------------------------------------------------------

    def merge_with_cache(
        self, new_data: pd.DataFrame, cache_path: Path
    ) -> pd.DataFrame:
        today = date.today()

        if cache_path.exists():
            cache = pd.read_parquet(cache_path)
        else:
            cache = _empty_cache()

        new_isins = set(new_data["isin"])
        cached_isins = set(cache["isin"]) if len(cache) > 0 else set()

        # Existing ISINs: take updated fields from new CSV, keep first_seen from cache
        cache_first_seen = (
            cache[["isin", "first_seen"]].drop_duplicates("isin")
            if len(cache) > 0
            else pd.DataFrame(columns=["isin", "first_seen"])
        )
        existing_new = new_data[new_data["isin"].isin(cached_isins)].copy()
        existing = existing_new.merge(cache_first_seen, on="isin", how="left")
        existing["last_seen"] = today
        existing["status"] = "active"

        # Check for ticker changes and log warnings
        if len(existing) > 0 and len(cache) > 0:
            cache_tickers = cache[cache["isin"].isin(cached_isins)].set_index("isin")[
                "ticker"
            ]
            for _, row in existing.iterrows():
                old_ticker = cache_tickers.get(row["isin"])
                if old_ticker is not None and old_ticker != row["ticker"]:
                    logger.warning(
                        f"Ticker changed for ISIN {row['isin']}: {old_ticker!r} → {row['ticker']!r}"
                    )

        # New ISINs: first_seen = last_seen = today
        truly_new = new_isins - cached_isins
        new_entries = new_data[new_data["isin"].isin(truly_new)].copy()
        new_entries["first_seen"] = today
        if truly_new:
            logger.info(f"New ISINs added: {len(truly_new)}")

        # Delisted ISINs: mark inactive, preserve all other fields
        delisted_isins = cached_isins - new_isins
        delisted = cache[cache["isin"].isin(delisted_isins)].copy()
        delisted["status"] = "inactive"
        if delisted_isins:
            logger.info(f"ISINs marked inactive: {len(delisted_isins)}")

        parts = [df for df in [existing, new_entries, delisted] if len(df) > 0]
        if not parts:
            return _empty_cache()

        merged = (
            pd.concat(parts, ignore_index=True)[_CACHE_COLUMNS]
            .sort_values("isin")
            .reset_index(drop=True)
        )
        return merged

    # ------------------------------------------------------------------
    # Step 4: run the full update cycle
    # ------------------------------------------------------------------

    def run(self, cache_path: Path) -> None:
        logger.info("Scraping Deutsche Börse instruments page for CSV URL...")
        csv_url = self.get_csv_url()
        logger.info(f"CSV URL: {csv_url}")

        logger.info("Downloading and parsing CSV...")
        new_data = self.download_and_parse(csv_url)

        logger.info("Merging with existing cache...")
        merged = self.merge_with_cache(new_data, cache_path)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(cache_path, index=False)
        logger.info(f"ISIN mapping updated: {len(merged)} entries → {cache_path}")

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _make_absolute(href: str) -> str:
    if href.startswith("//"):
        return f"https:{href}"
    if href.startswith("/"):
        return f"https://www.xetra.com{href}"
    if not href.startswith("http"):
        return f"https://www.xetra.com/{href}"
    return href
