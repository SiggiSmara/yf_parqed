# Xetra ISIN Mapping Strategy: Deutsche Börse CSV

## Executive Summary

**Decision**: Use Deutsche Börse's official "All Tradable Instruments" CSV as primary ISIN→ticker mapping source instead of OpenFIGI API.

**Rationale**:
- **Official source**: Authoritative data directly from Deutsche Börse
- **Free & unlimited**: No API rate limits or costs (vs OpenFIGI $500/month for production)
- **Comprehensive**: 4,280+ XETRA instruments with ISIN, mnemonic (ticker), name, metadata
- **Daily updates**: Refreshed nightly at ~11:54 PM CET
- **Alignment**: Mirrors existing yfinance pattern (local Parquet cache with timestamps)

**Trade-off**: CSV URL is dynamic (blob ID + hash change daily), requires web scraping to discover current link.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ ISIN Mapping Update Flow                                        │
└─────────────────────────────────────────────────────────────────┘

1. Scrape Deutsche Börse webpage
   https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments
   ↓
2. Extract current CSV download link
   https://www.xetra.com/resource/blob/{id}/{hash}/data/t7-xetr-allTradableInstruments.csv
   ↓
3. Download CSV (semicolon-delimited, ~4,280 rows)
   ↓
4. Parse and normalize:
   - Extract: ISIN, Mnemonic (ticker), Instrument (name), Currency, Status
   - Filter: Active instruments only (Product Status = "Active")
   - Transform: Lowercase tickers, strip whitespace
   ↓
5. Merge with existing isin_mapping.parquet:
   - Add new ISINs with first_seen timestamp
   - Update existing ISINs with last_seen timestamp
   - Mark missing ISINs as delisted (status = "inactive")
   ↓
6. Write updated isin_mapping.parquet
   - Schema: isin, ticker, name, currency, status, first_seen, last_seen, source
   - Sorted by ISIN for efficient lookups
   - Indexed on both isin and ticker columns

┌─────────────────────────────────────────────────────────────────┐
│ Runtime ISIN Lookup Flow                                        │
└─────────────────────────────────────────────────────────────────┘

1. XetraParqed receives trade data with ISIN (e.g., "DE0005140008")
   ↓
2. Check cache: isin_mapping.parquet
   ↓
3. Cache HIT → use mapped ticker (e.g., "DBK")
   ↓
4. Cache MISS → write to __UNMAPPED__ partition
   ↓
5. Log warning for manual review/investigation
```

---

## CSV Schema Specification

### Source CSV Format

**URL Pattern**: `https://www.xetra.com/resource/blob/{dynamic_id}/{dynamic_hash}/data/t7-xetr-allTradableInstruments.csv`

**Example URL** (2025-11-03):
```
https://www.xetra.com/resource/blob/1528/28190824d27b9267af27bcd473d89c6d/data/t7-xetr-allTradableInstruments.csv
```

**Delimiter**: Semicolon (`;`)

**Header Structure**:
```csv
Market:;XETR
Date Last Update:;03.11.2025
Product Status;Instrument Status;Instrument;ISIN;Product ID;Instrument ID;WKN;Mnemonic;MIC Code;...
```

**Relevant Columns** (positions 0-based):
| Position | Field Name         | Example Value              | Usage                          |
|----------|-------------------|----------------------------|--------------------------------|
| 0        | Product Status     | `Active`                   | Filter criterion               |
| 1        | Instrument Status  | `Active`                   | Secondary filter               |
| 2        | Instrument         | `DEUTSCHE BANK AG NA O.N.` | Company name                   |
| 3        | ISIN               | `DE0005140008`             | Primary key for mapping        |
| 6        | WKN                | `514000`                   | German securities ID (backup)  |
| 7        | Mnemonic           | `DBK`                      | Ticker symbol                  |
| 122      | Currency           | `EUR`                      | Trading currency               |
| 129      | First Trading Date | `2018-07-16`               | Listing date                   |
| 130      | Last Trading Date  | (empty if active)          | Delisting date (if applicable) |

**Sample Rows**:
```csv
Active;Active;DEUTSCHE BANK AG NA O.N.;DE0005140008;...;DBK;XETR;...;EUR;...;2018-07-16;;...
Active;Active;SIEMENS AG NA;DE0007236101;...;SIE;XETR;...;EUR;...;2007-03-05;;...
Active;Active;VOLKSWAGEN AG VZO O.N.;DE0007664039;...;VOW3;XETR;...;EUR;...;2007-03-05;;...
```

---

## Parquet Cache Schema

### isin_mapping.parquet

**Purpose**: Local cache of ISIN→ticker mappings for fast runtime lookups

**Storage Location**: `data/reference/isin_mapping.parquet`

**Schema**:
```python
{
    "isin": pl.Utf8,           # ISO 6166 ISIN code (12 chars) — PRIMARY KEY
    "ticker": pl.Utf8,         # Xetra mnemonic (1-5 chars typically)
    "name": pl.Utf8,           # Full instrument name
    "currency": pl.Utf8,       # ISO 4217 currency code (3 chars)
    "wkn": pl.Utf8,            # Optional: German WKN (6 chars)
    "status": pl.Utf8,         # "active" | "inactive" (delisted)
    "first_seen": pl.Date,     # Date first discovered in CSV
    "last_seen": pl.Date,      # Date last confirmed in CSV
    "source": pl.Utf8,         # Always "deutsche_boerse_csv"
}
```

**Indexes**: 
- Primary: ISIN (for trade data lookups)
- Secondary: ticker (for reverse lookups)

**Sorting**: By ISIN ascending (enables binary search)

**Example Rows**:
| isin           | ticker | name                      | currency | wkn    | status  | first_seen | last_seen  | source              |
|----------------|--------|---------------------------|----------|--------|---------|------------|------------|---------------------|
| AT000000STR1   | XD4    | STRABAG SE                | EUR      | 00A0M23V | active  | 2025-10-12 | 2025-11-03 | deutsche_boerse_csv |
| DE0005140008   | DBK    | DEUTSCHE BANK AG NA O.N.  | EUR      | 514000 | active  | 2025-10-12 | 2025-11-03 | deutsche_boerse_csv |
| DE0007236101   | SIE    | SIEMENS AG NA             | EUR      | 723610 | active  | 2025-10-12 | 2025-11-03 | deutsche_boerse_csv |
| US0378331005   | AAPL34 | APPLE INC. DL-,00001      | EUR      | 865985 | inactive | 2025-10-12 | 2025-10-20 | deutsche_boerse_csv |

---

## Implementation Components

### 1. Web Scraper (`ISINMappingUpdater`)

**Responsibility**: Extract current CSV download URL from Deutsche Börse webpage

**Logic**:
```python
import httpx
from bs4 import BeautifulSoup

class ISINMappingUpdater:
    INSTRUMENTS_PAGE = "https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments"
    
    def get_csv_download_url(self) -> str:
        """Scrape current CSV download link from instruments page."""
        response = httpx.get(self.INSTRUMENTS_PAGE, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Find link with text containing "All tradable instruments"
        for link in soup.find_all("a", href=True):
            if "alltradableinstruments.csv" in link["href"].lower():
                href = link["href"]
                # Handle relative vs absolute URLs
                if href.startswith("/"):
                    return f"https://www.xetra.com{href}"
                elif not href.startswith("http"):
                    return f"https://www.xetra.com/{href}"
                return href
        
        raise ValueError("Could not find CSV download link on Deutsche Börse page")
```

**Dependencies**: `httpx`, `beautifulsoup4` (add to `pyproject.toml`)

**Error Handling**:
- HTTP errors → retry with exponential backoff (3 attempts)
- Parsing failures → log error, use cached mapping if available
- Missing link → raise exception (require manual intervention)

---

### 2. CSV Downloader & Parser

**Responsibility**: Download CSV, parse semicolon-delimited format, extract relevant fields

**Logic**:
```python
import polars as pl
from pathlib import Path
from datetime import date

class ISINMappingUpdater:
    def download_and_parse_csv(self, url: str) -> pl.DataFrame:
        """Download CSV and parse into normalized DataFrame."""
        response = httpx.get(url, timeout=60)
        response.raise_for_status()
        
        # Save to temp file for Polars to read
        temp_csv = Path("/tmp/xetra_instruments.csv")
        temp_csv.write_bytes(response.content)
        
        # Read CSV (skip metadata header rows)
        df = pl.read_csv(
            temp_csv,
            separator=";",
            skip_rows=2,  # Skip "Market: XETR" and "Date Last Update" rows
            has_header=True,
            encoding="utf-8",
        )
        
        # Extract and normalize relevant columns
        normalized = df.select([
            pl.col("ISIN").str.strip_chars().alias("isin"),
            pl.col("Mnemonic").str.strip_chars().str.to_lowercase().alias("ticker"),
            pl.col("Instrument").str.strip_chars().alias("name"),
            pl.col("Currency").str.strip_chars().alias("currency"),
            pl.col("WKN").str.strip_chars().alias("wkn"),
            pl.col("Product Status").str.strip_chars().alias("product_status"),
            pl.col("Instrument Status").str.strip_chars().alias("instrument_status"),
        ]).filter(
            # Only keep active instruments
            (pl.col("product_status") == "Active") &
            (pl.col("instrument_status") == "Active")
        ).select([
            "isin", "ticker", "name", "currency", "wkn"
        ]).with_columns([
            pl.lit("active").alias("status"),
            pl.lit(date.today()).alias("last_seen"),
            pl.lit("deutsche_boerse_csv").alias("source"),
        ])
        
        temp_csv.unlink()  # Cleanup
        return normalized
```

**Validation**:
- Assert ISIN is 12 characters (ISO 6166 standard)
- Assert ticker is non-empty and 1-10 characters
- Assert currency is 3 characters (ISO 4217)
- Log warning for duplicate ISINs (shouldn't happen)

---

### 3. Cache Merger

**Responsibility**: Merge new CSV data with existing Parquet cache, track lifecycle

**Logic**:
```python
class ISINMappingUpdater:
    def merge_with_cache(
        self,
        new_data: pl.DataFrame,
        cache_path: Path,
    ) -> pl.DataFrame:
        """Merge new CSV data with existing cache, updating timestamps."""
        today = date.today()
        
        # Load existing cache (or create empty if first run)
        if cache_path.exists():
            cache = pl.read_parquet(cache_path)
        else:
            cache = pl.DataFrame(schema={
                "isin": pl.Utf8,
                "ticker": pl.Utf8,
                "name": pl.Utf8,
                "currency": pl.Utf8,
                "wkn": pl.Utf8,
                "status": pl.Utf8,
                "first_seen": pl.Date,
                "last_seen": pl.Date,
                "source": pl.Utf8,
            })
        
        # Categorize ISINs
        new_isins = set(new_data["isin"])
        cached_isins = set(cache["isin"])
        
        # 1. Existing ISINs (update last_seen)
        existing = cache.filter(pl.col("isin").is_in(new_isins)).with_columns([
            pl.lit(today).alias("last_seen"),
            pl.lit("active").alias("status"),  # Reactivate if was delisted
        ])
        
        # 2. New ISINs (add with first_seen = last_seen = today)
        truly_new_isins = new_isins - cached_isins
        new_entries = new_data.filter(
            pl.col("isin").is_in(list(truly_new_isins))
        ).with_columns([
            pl.lit(today).alias("first_seen"),
        ])
        
        # 3. Delisted ISINs (mark inactive, preserve last_seen)
        delisted_isins = cached_isins - new_isins
        delisted = cache.filter(pl.col("isin").is_in(list(delisted_isins))).with_columns([
            pl.lit("inactive").alias("status"),
        ])
        
        # Combine and sort
        merged = pl.concat([existing, new_entries, delisted]).sort("isin")
        
        # Log summary
        logger.info(f"ISIN mapping updated: {len(truly_new_isins)} new, {len(existing)} existing, {len(delisted_isins)} delisted")
        
        return merged
```

**Edge Cases**:
- Ticker changes for same ISIN → update ticker, log warning
- ISIN reappears after delisting → reactivate with new last_seen
- First run (no cache) → all ISINs are "new"

---

### 4. CLI Command

**Command**: `xetra-parqed update-isin-mapping`

**Options**:
- `--force`: Force re-download even if cache is fresh (<24 hours old)
- `--dry-run`: Show what would change without writing

**Implementation**:
```python
@app.command()
def update_isin_mapping(
    force: bool = typer.Option(False, "--force", help="Force update even if cache is recent"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show changes without saving"),
):
    """Update ISIN→ticker mapping from Deutsche Börse CSV."""
    cache_path = Path("data/reference/isin_mapping.parquet")
    
    # Check cache age
    if cache_path.exists() and not force:
        age_hours = (datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)).total_seconds() / 3600
        if age_hours < 24:
            logger.info(f"Cache is fresh ({age_hours:.1f} hours old), skipping update. Use --force to override.")
            return
    
    updater = ISINMappingUpdater()
    
    # Step 1: Scrape current CSV URL
    logger.info("Scraping Deutsche Börse instruments page...")
    csv_url = updater.get_csv_download_url()
    logger.info(f"Found CSV URL: {csv_url}")
    
    # Step 2: Download and parse
    logger.info("Downloading and parsing CSV...")
    new_data = updater.download_and_parse_csv(csv_url)
    logger.info(f"Parsed {len(new_data)} active instruments")
    
    # Step 3: Merge with cache
    logger.info("Merging with existing cache...")
    merged = updater.merge_with_cache(new_data, cache_path)
    
    if dry_run:
        logger.info("[DRY RUN] Would write:")
        logger.info(merged.head(10))
        return
    
    # Step 4: Write updated cache
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    merged.write_parquet(cache_path)
    logger.info(f"Updated cache written to {cache_path}")
```

**Scheduling**: Run daily via cron/systemd timer before main data fetch

---

### 5. Runtime Lookup Service

**Responsibility**: Fast ISIN→ticker lookups during trade data processing

**Logic**:
```python
class ISINMapper:
    def __init__(self, cache_path: Path):
        """Load ISIN mapping cache into memory for fast lookups."""
        self.cache_path = cache_path
        self._mapping = self._load_cache()
    
    def _load_cache(self) -> dict[str, str]:
        """Load active ISIN→ticker mappings into dict."""
        if not self.cache_path.exists():
            logger.warning("ISIN mapping cache not found, returning empty mapping")
            return {}
        
        df = pl.read_parquet(self.cache_path).filter(
            pl.col("status") == "active"
        )
        return dict(zip(df["isin"], df["ticker"]))
    
    def get_ticker(self, isin: str) -> str | None:
        """Look up ticker for ISIN. Returns None if not found."""
        return self._mapping.get(isin)
    
    def reload(self):
        """Reload cache from disk (call after update_isin_mapping)."""
        self._mapping = self._load_cache()
```

**Integration** in `XetraParqed`:
```python
class XetraParqed:
    def __init__(self, working_path: Path):
        self.isin_mapper = ISINMapper(working_path / "data/reference/isin_mapping.parquet")
    
    def _partition_trade_data(self, trades: pl.DataFrame) -> dict[str, pl.DataFrame]:
        """Partition trades by ticker, with __UNMAPPED__ fallback."""
        partitions = {}
        
        for isin in trades["ISIN"].unique():
            ticker = self.isin_mapper.get_ticker(isin)
            
            if ticker is None:
                logger.warning(f"Unmapped ISIN: {isin}")
                ticker = "__UNMAPPED__"
            
            partition = trades.filter(pl.col("ISIN") == isin)
            partitions[ticker] = partition
        
        return partitions
```

---

## Comparison: Deutsche Börse CSV vs OpenFIGI

| Aspect                 | Deutsche Börse CSV                          | OpenFIGI API                              |
|------------------------|---------------------------------------------|-------------------------------------------|
| **Authoritative**      | ✅ Official exchange source                  | ⚠️ Third-party aggregator (Bloomberg)    |
| **Coverage**           | ✅ 4,280+ XETRA instruments                  | ✅ Global coverage (100M+ instruments)    |
| **Cost**               | ✅ Free, unlimited                           | ❌ $500/month for production (25 req/s free) |
| **Latency**            | ✅ Local cache lookup (<1ms)                 | ❌ Network request (~50-200ms per lookup) |
| **Reliability**        | ✅ Download once daily, cache locally        | ⚠️ Rate limits, potential downtime       |
| **Update Frequency**   | ⚠️ Daily (~11:54 PM CET)                     | ✅ Real-time via API                      |
| **Implementation**     | ⚠️ Requires web scraping + CSV parsing      | ✅ Simple REST API                        |
| **Non-Xetra Support**  | ❌ XETRA only                                | ✅ Global exchanges                       |
| **Maintenance**        | ⚠️ Scraper may break if webpage changes     | ✅ Stable API contract                    |

**Decision**: Use Deutsche Börse CSV as **primary**, OpenFIGI as **optional fallback** for non-Xetra ISINs (e.g., ADRs, international listings).

---

## Migration from OpenFIGI Design

### Changes Required

1. **Remove OpenFIGI dependency** from primary ISIN mapping flow
2. **Update ADR** (docs/adr/2025-10-12-xetra-delayed-data.md):
   - Replace AD-8 (ISIN Mapping) to specify Deutsche Börse CSV as primary
   - Keep OpenFIGI as "Future Enhancement" for multi-exchange support
3. **Update implementation addendum** (docs/xetra_implementation_addendum.md):
   - Replace OpenFIGI integration code with CSV scraper/parser
   - Update cost analysis (remove $500/month API cost)
4. **Update key findings** (docs/xetra_key_findings_summary.md):
   - Critical Decision #4: Change from OpenFIGI to Deutsche Börse CSV
   - Update Phase 2 roadmap to include CSV scraper implementation

### Backward Compatibility

If future use case requires non-Xetra ISINs (e.g., Frankfurt, Stuttgart):
1. Check Deutsche Börse CSV first (local cache)
2. On miss → fallback to OpenFIGI API (with rate limiting)
3. Cache OpenFIGI results in same `isin_mapping.parquet` with `source = "openfigi"`

---

## Testing Strategy

### Unit Tests

1. **CSV Scraper** (`test_isin_mapping_scraper.py`):
   - Mock webpage HTML with valid/invalid link structures
   - Test link extraction (absolute, relative, missing)
   - Test HTTP error handling (404, timeout, network error)

2. **CSV Parser** (`test_isin_mapping_parser.py`):
   - Fixture CSV with known ISINs/tickers
   - Test semicolon delimiter parsing
   - Test header row skipping (metadata rows)
   - Test active/inactive filtering
   - Test column normalization (strip, lowercase)
   - Test validation (ISIN length, ticker non-empty)

3. **Cache Merger** (`test_isin_mapping_merger.py`):
   - Test new ISIN insertion (first_seen = today)
   - Test existing ISIN update (last_seen = today)
   - Test delisted ISIN marking (status = inactive)
   - Test ticker change detection (log warning)
   - Test reactivation (inactive → active)

4. **Lookup Service** (`test_isin_mapper.py`):
   - Test cache loading (active only)
   - Test ISIN→ticker lookup (hit/miss)
   - Test reload after update
   - Test empty cache handling

### Integration Tests

1. **End-to-End Update** (`test_isin_mapping_e2e.py`):
   - Mock Deutsche Börse webpage + CSV download
   - Run `update-isin-mapping` command
   - Verify Parquet cache written correctly
   - Verify logging output (new/existing/delisted counts)

2. **CLI Dry Run** (`test_isin_mapping_cli.py`):
   - Test `--dry-run` flag (no file write)
   - Test `--force` flag (ignores cache age)
   - Test cache age check (<24 hours = skip)

### Regression Tests

1. **Schema Stability**:
   - Assert Parquet schema matches spec
   - Assert CSV column positions stable (detect Deutsche Börse changes)

2. **Scraper Resilience**:
   - Test with archived Deutsche Börse page HTML (prevent breakage)
   - Alert on link pattern changes

---

## Deployment Checklist

- [ ] Add dependencies: `httpx`, `beautifulsoup4`, `lxml` to `pyproject.toml`
- [ ] Implement `ISINMappingUpdater` class in `src/yf_parqed/isin_mapping_updater.py`
- [ ] Implement `ISINMapper` class in `src/yf_parqed/isin_mapper.py`
- [ ] Add CLI command `update-isin-mapping` to `src/yf_parqed/tools/xetra_cli.py`
- [ ] Integrate `ISINMapper` into `XetraParqed.__init__()`
- [ ] Create test fixtures (sample CSV, webpage HTML)
- [ ] Write unit tests (4 test files, ~25 tests total)
- [ ] Write integration tests (2 test files, ~8 tests total)
- [ ] Update ADR with Deutsche Börse CSV decision
- [ ] Update implementation addendum to remove OpenFIGI code
- [ ] Update key findings summary with cost savings
- [ ] Create cron job template for daily `update-isin-mapping`
- [ ] Document manual fallback process (if scraper breaks)

---

## Future Enhancements

1. **Multi-Exchange Support**:
   - Deutsche Börse also publishes CSVs for Frankfurt (XFRA), Stuttgart (XSTU)
   - Same scraping pattern, different URLs
   - Extend `isin_mapping.parquet` with `exchange` column

2. **Ticker History Tracking**:
   - Add `ticker_history` JSONB column to track ticker changes over time
   - Example: `{"2024-01-01": "DBK1", "2025-01-01": "DBK"}`
   - Useful for historical analysis

3. **OpenFIGI Hybrid Mode**:
   - Auto-fallback to OpenFIGI for unmapped ISINs
   - Cache OpenFIGI results locally
   - Reduce API calls over time as cache grows

4. **Monitoring & Alerts**:
   - Track daily new/delisted counts
   - Alert if >10% of ISINs delisted in single day (data quality issue)
   - Alert if scraper fails 3 consecutive days

---

## Questions for Review

1. **Scraper Fragility**: Webpage structure could change. Acceptable risk given cost savings?
   - Mitigation: Version-controlled HTML fixtures for regression testing
   - Fallback: Manual CSV download + local import command

2. **Update Frequency**: Daily updates sufficient, or need intraday?
   - Current: CSV updates nightly (~11:54 PM CET)
   - Consideration: New IPOs would be delayed 0-24 hours
   - Decision: **Daily is sufficient** (IPOs are rare events, can handle manually)

3. **Non-Xetra ISINs**: Should we support Frankfurt/Stuttgart from day 1?
   - Current scope: XETRA only
   - Recommendation: **Defer to Phase 2** (keep XETRA simple first)

4. **Cache Storage**: Parquet vs SQLite for ISIN mapping?
   - Current: Parquet (aligns with project pattern)
   - Alternative: SQLite with indexes (faster lookups, more complex)
   - Decision: **Parquet** (simpler, ~4K rows is trivial for in-memory dict)

---

## Approval Checklist

Before proceeding with implementation:
- [ ] Confirm Deutsche Börse CSV as primary mapping source
- [ ] Approve web scraping approach (vs manual CSV downloads)
- [ ] Confirm daily update frequency (vs intraday)
- [ ] Approve `__UNMAPPED__` partition strategy for unknown ISINs
- [ ] Approve deferral of multi-exchange support to Phase 2
- [ ] Approve removal of OpenFIGI from Phase 1 scope
