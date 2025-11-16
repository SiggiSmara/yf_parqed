# Xetra Implementation Addendum

**Date**: 2025-11-02  
**Purpose**: Address additional implementation details discovered from actual Xetra data samples and API analysis

## 1. Actual Per-Trade Schema (From Sample Data)

### 1.1 Complete Field Inventory

Based on `docs/data/DETR-posttrade-2025-10-31T13_54.json`, each trade record contains **23 fields**:

```python
# Core identifiers
"messageId": "posttrade"          # Always "posttrade" for trade data
"sourceName": "ETR"                # Exchange code (ETR = Xetra)
"isin": "DE0007100000"             # ISIN identifier (PRIMARY KEY for partitioning)
"instrumentId": "DE0007100000"     # Duplicate of ISIN (redundant)
"transIdCode": "1000...6636"       # Unique transaction ID (64 chars)
"tickId": 33976320                 # Sequential tick identifier

# Trade details
"lastTrade": 56.20                 # PRICE - the actual trade price
"lastQty": 159.00                  # VOLUME - shares/units traded
"currency": "EUR"                  # Trading currency
"quotationType": 1                 # Quote type (1 = standard)

# Timestamps (nanosecond precision)
"lastTradeTime": "2025-10-31T13:54:00.042457058Z"      # Trade execution time
"distributionDateTime": "2025-10-31T13:54:00.052903000Z"  # Data publication time

# Execution venue
"executionVenueId": "XETA"         # XETA = Xetra, XETB = other venue segment

# MiFID II transparency fields (regulatory metadata)
"tickActionIndicator": "I"         # Action type (I = Insert)
"instrumentIdCode": "I"            # Instrument code type
"mmtMarketMechanism": "8"          # Market mechanism
"mmtTradingMode": "2"              # Trading mode
"mmtNegotTransPretrdWaivInd": "-"  # Negotiated transaction indicator
"mmtModificationInd": "-"          # Modification indicator
"mmtBenchmarkRefprcInd": "-"       # Benchmark reference price indicator
"mmtPubModeDefReason": "-"         # Publication mode deferral reason
"mmtAlgoInd": "H"                  # Algorithmic trading indicator (H = yes, - = no)
```

### 1.2 Field Categories and Utility

**CRITICAL for users** (12 fields):
- `isin` - Security identifier (partition key)
- `lastTrade` - Price
- `lastQty` - Volume
- `currency` - Currency
- `lastTradeTime` - Execution timestamp
- `distributionDateTime` - Publication timestamp
- `executionVenueId` - Venue (XETA vs XETB)
- `transIdCode` - Unique trade ID (deduplication)
- `tickId` - Sequential order
- `sourceName` - Exchange
- `mmtAlgoInd` - Algorithmic vs manual trade
- `quotationType` - Quote type

**Regulatory/metadata** (9 fields - optional for advanced users):
- `mmtMarketMechanism` - Market mechanism code
- `mmtTradingMode` - Trading mode code
- `mmtNegotTransPretrdWaivInd` - Negotiated trade flag
- `mmtModificationInd` - Modification flag
- `mmtBenchmarkRefprcInd` - Benchmark reference flag
- `mmtPubModeDefReason` - Publication deferral reason
- `tickActionIndicator` - Action type (always "I" for inserts)
- `instrumentIdCode` - Instrument code (always "I")
- `messageId` - Message type (always "posttrade")

**Redundant** (2 fields - can be dropped):
- `instrumentId` - Duplicate of `isin`
- `messageId` - Always "posttrade" in trade files

### 1.3 Revised Per-Trade Storage Schema

```python
# Minimum viable schema (10 fields - 95% use case)
XETRA_TRADE_CORE_SCHEMA = {
    "isin": "string",              # Partition key
    "price": "float64",            # lastTrade renamed for clarity
    "volume": "float64",           # lastQty renamed for clarity
    "currency": "string",
    "trade_time": "timestamp[ns]", # lastTradeTime renamed
    "venue": "string",             # executionVenueId
    "trans_id": "string",          # transIdCode for deduplication
    "tick_id": "int64",           # tickId for sequencing
    "algo_trade": "bool",          # mmtAlgoInd converted to boolean
    "source": "string",            # sourceName (ETR)
}

# Extended schema for regulatory compliance (add 8 more fields)
XETRA_TRADE_EXTENDED_SCHEMA = {
    **XETRA_TRADE_CORE_SCHEMA,
    "distribution_time": "timestamp[ns]",  # distributionDateTime
    "quote_type": "int8",                   # quotationType
    "market_mechanism": "string",           # mmtMarketMechanism
    "trading_mode": "string",               # mmtTradingMode
    "negotiated_flag": "string",            # mmtNegotTransPretrdWaivInd
    "modification_flag": "string",          # mmtModificationInd
    "benchmark_flag": "string",             # mmtBenchmarkRefprcInd
    "pub_deferral": "string",              # mmtPubModeDefReason
}
```

**Recommendation**: 
- **Default**: Use core schema (10 fields) for 95% of users
- **Optional**: CLI flag `--extended-metadata` to enable full 18-field schema
- **Never store**: `instrumentId`, `messageId`, `tickActionIndicator`, `instrumentIdCode` (redundant/constant)

---

## 2. Deutsche Börse File Listing API

### 2.1 API Discovery

**URL**: `https://mfs.deutsche-boerse.com/api/`

**Response Format**: 
- **With `Accept: application/json` header**: Returns pure JSON ✅
- **Without header**: Returns HTML with embedded JSON

**Sample Response**:
```json
{
  "SourcePrefix": "",
  "DaysToKeepOnWebpage": "1",
  "FileCount": "1321",
  "GenerationDatetime": "2025-11-02 15:10:17.910 UTC",
  "CurrentFiles": [
    "-2025-10-31T22_00.json.gz",
    "-2025-10-31T21_59.json.gz",
    ...
  ]
}
```

### 2.2 Key Findings

**Problem**: The API returns **suffix-only** file names (e.g., `-2025-10-31T13_54.json.gz`), missing the **venue/type prefix** (e.g., `DETR-posttrade`).

**Implication**: To construct full URLs, we must **enumerate all possible prefix combinations**:

**Venue prefixes** (4):
- `DETR` - Deutsche Börse XETRA
- `DFRA` - Frankfurt Stock Exchange
- `DGAT` - Börse Stuttgart (GETTEX)
- `DEUR` - Eurex (derivatives)

**Type prefixes** (2):
- `posttrade` - Executed trades (our primary target)
- `pretrade` - Order book snapshots (optional future feature)

**Full prefix matrix** (8 combinations):
```
DETR-posttrade-
DETR-pretrade-
DFRA-posttrade-
DFRA-pretrade-
DGAT-posttrade-
DGAT-pretrade-
DEUR-posttrade-
DEUR-pretrade-
```

### 2.3 File Availability Orchestration Strategy

**Option A: Prefix enumeration** (recommended for MVP)
```python
async def list_available_files(
    venues: list[str] = ["DETR"],  # Default to XETRA only
    types: list[str] = ["posttrade"],
) -> dict[str, list[str]]:
    """
    Fetch available files by enumerating venue/type prefixes.
    
    Returns dict mapping full URL to metadata:
    {
        "https://...DETR-posttrade-2025-10-31T13_54.json.gz": {
            "venue": "DETR",
            "type": "posttrade",
            "datetime": "2025-10-31T13:54",
            "suffix": "-2025-10-31T13_54.json.gz"
        }
    }
    """
    api_response = await fetch_json("https://mfs.deutsche-boerse.com/api/")
    available = {}
    
    for venue in venues:
        for file_type in types:
            prefix = f"{venue}-{file_type}"
            base_url = f"https://mfs.deutsche-boerse.com/api/{prefix}"
            
            for suffix in api_response["CurrentFiles"]:
                full_url = f"{base_url}{suffix}"
                # Test if file exists (HEAD request or check against local index)
                available[full_url] = {
                    "venue": venue,
                    "type": file_type,
                    "datetime": parse_datetime_from_suffix(suffix),
                    "suffix": suffix
                }
    
    return available
```

**Option B: Probe and cache** (more efficient for production)
```python
async def discover_available_files(
    target_datetime: datetime,
    venues: list[str] = ["DETR"],
) -> dict[str, str]:
    """
    Probe specific datetime for each venue/type combo.
    Cache results to avoid redundant HEAD requests.
    """
    suffix = format_suffix(target_datetime)  # e.g., "-2025-10-31T13_54.json.gz"
    available = {}
    
    tasks = []
    for venue in venues:
        for file_type in ["posttrade", "pretrade"]:
            url = f"https://mfs.deutsche-boerse.com/api/{venue}-{file_type}{suffix}"
            tasks.append(check_file_exists(url))  # async HEAD request
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for url, exists in zip(tasks, results):
        if exists:
            available[url] = parse_metadata(url)
    
    return available
```

### 2.4 CLI Design for Venue/Type Selection

**Proposed CLI flags**:
```bash
# Download specific venues (default: DETR only)
xetra-parqed download --venue DETR --venue DFRA --type posttrade

# Download all venues for a venue prefix
xetra-parqed download --all-venues --type posttrade

# Download specific datetime range
xetra-parqed download --venue DETR --type posttrade \
    --start-date 2025-10-31 --start-time 09:00 \
    --end-date 2025-10-31 --end-time 17:30

# Download last N hours (for backfill)
xetra-parqed download --venue DETR --type posttrade --last-hours 24

# Skip already downloaded files (idempotent)
xetra-parqed download --venue DETR --type posttrade --skip-existing
```

**Config file support** (`xetra_config.json`):
```json
{
  "venues": {
    "DETR": {"enabled": true, "priority": 1},
    "DFRA": {"enabled": false, "priority": 2},
    "DGAT": {"enabled": false, "priority": 3},
    "DEUR": {"enabled": false, "priority": 4}
  },
  "types": {
    "posttrade": {"enabled": true},
    "pretrade": {"enabled": false}
  },
  "download": {
    "concurrent_downloads": 3,
    "retry_attempts": 3,
    "verify_checksums": true
  }
}
```

---

## 3. ISIN to Ticker Mapping Challenge

### 3.1 The Problem

**Key observation**: Xetra data contains **only ISIN identifiers**, not ticker symbols.

**Examples from sample data**:
- ISIN `DE0005140008` → Xetra mnemonic `DBK` (Deutsche Bank)
- ISIN `DE0007236101` → Xetra mnemonic `SIE` (Siemens)
- ISIN `AT000000STR1` → Xetra mnemonic `XD4` (Strabag, Austrian)

**Challenge**: Need authoritative, cost-effective ISIN→ticker mapping source.

### 3.2 **APPROVED** Solution: Deutsche Börse Official CSV

**Decision**: Use Deutsche Börse's official "All Tradable Instruments" CSV as primary mapping source.

**Data Source**:
- **Webpage**: https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments
- **CSV URL Pattern**: `https://www.xetra.com/resource/blob/{ID}/{HASH}/data/t7-xetr-allTradableInstruments.csv`
- **Format**: Semicolon-delimited, 130+ columns, ~4,280 active XETRA instruments
- **Update Schedule**: Daily at ~11:54 PM CET
- **Cost**: **FREE** (official exchange data)

**Relevant CSV Fields**:
- Column 3: `ISIN` (e.g., `DE0005140008`)
- Column 7: `Mnemonic` (ticker, e.g., `DBK`)
- Column 2: `Instrument` (full name, e.g., `DEUTSCHE BANK AG NA O.N.`)
- Column 122: `Currency` (e.g., `EUR`)
- Column 6: `WKN` (German securities ID, e.g., `514000`)

**Implementation Strategy**:

```python
from pathlib import Path
import httpx
from bs4 import BeautifulSoup
import polars as pl
from datetime import date

class ISINMappingUpdater:
    """Updates ISIN→ticker mapping from Deutsche Börse CSV."""
    
    INSTRUMENTS_PAGE = "https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments"
    
    def get_csv_download_url(self) -> str:
        """Scrape current CSV download link from instruments page."""
        response = httpx.get(self.INSTRUMENTS_PAGE, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Find link containing "alltradableinstruments.csv"
        for link in soup.find_all("a", href=True):
            if "alltradableinstruments.csv" in link["href"].lower():
                href = link["href"]
                if href.startswith("/"):
                    return f"https://www.xetra.com{href}"
                elif not href.startswith("http"):
                    return f"https://www.xetra.com/{href}"
                return href
        
        raise ValueError("Could not find CSV download link")
    
    def download_and_parse_csv(self, url: str) -> pl.DataFrame:
        """Download CSV and parse into normalized DataFrame."""
        response = httpx.get(url, timeout=60)
        response.raise_for_status()
        
        # Save to temp file for Polars
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
        ]).filter(
            pl.col("product_status") == "Active"
        ).select([
            "isin", "ticker", "name", "currency", "wkn"
        ]).with_columns([
            pl.lit("active").alias("status"),
            pl.lit(date.today()).alias("last_seen"),
            pl.lit("deutsche_boerse_csv").alias("source"),
        ])
        
        temp_csv.unlink()  # Cleanup
        return normalized

class ISINMapper:
    """Runtime ISIN→ticker lookup service."""
    
    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self._mapping = self._load_cache()
    
    def _load_cache(self) -> dict[str, str]:
        """Load active ISIN→ticker mappings into dict."""
        if not self.cache_path.exists():
            logger.warning("ISIN mapping cache not found")
            return {}
        
        df = pl.read_parquet(self.cache_path).filter(
            pl.col("status") == "active"
        )
        return dict(zip(df["isin"], df["ticker"]))
    
    def get_ticker(self, isin: str) -> str | None:
        """Look up ticker for ISIN. Returns None if not found."""
        return self._mapping.get(isin)
    
    def reload(self):
        """Reload cache from disk (call after update)."""
        self._mapping = self._load_cache()
```

**Cache Schema** (`data/reference/isin_mapping.parquet`):
```python
{
    "isin": pl.Utf8,           # ISO 6166 ISIN code (12 chars)
    "ticker": pl.Utf8,         # Xetra mnemonic (1-5 chars)
    "name": pl.Utf8,           # Full instrument name
    "currency": pl.Utf8,       # ISO 4217 currency code
    "wkn": pl.Utf8,            # German WKN (6 chars)
    "status": pl.Utf8,         # "active" | "inactive" (delisted)
    "first_seen": pl.Date,     # Date first discovered
    "last_seen": pl.Date,      # Date last confirmed in CSV
    "source": pl.Utf8,         # "deutsche_boerse_csv"
}
```

**CLI Command**:
```bash
# Daily automated update (cron job)
xetra-parqed update-isin-mapping

# Force refresh (ignore cache age)
xetra-parqed update-isin-mapping --force
```

**Advantages Over OpenFIGI**:
- ✅ **FREE** (vs $500/month for OpenFIGI production tier)
- ✅ **Authoritative** (official exchange source vs third-party)
- ✅ **Fast** (<1ms local cache vs 50-200ms API)
- ✅ **Unlimited** (no rate limits)
- ✅ **Reliable** (100% uptime via local cache)

**Trade-offs**:
- ⚠️ Daily update lag (0-24 hours for new IPOs)
- ⚠️ Web scraping complexity (dynamic CSV URL requires scraping)
- ⚠️ Maintenance burden (quarterly scraper reviews)

**Mitigation**: Unknown ISINs written to venue-specific `isin={isin}/` partition for manual review. OpenFIGI deferred to Phase 3 as optional fallback for non-German ISINs.

**Related Documentation**: 
- `/docs/xetra/xetra_isin_mapping_strategy.md` - Full implementation spec
- `/docs/xetra/xetra_isin_mapping_decision.md` - Cost-benefit analysis
- `/docs/adr/2025-10-12-xetra-delayed-data.md` - AD-8 decision

### 3.3 Aggregation Strategy with Venue-First Dual-Partitioning

**Problem**: Some ISINs may not have mappings initially (new IPOs, foreign listings, delisted stocks).

**Solution**: Store aggregated data with **venue-first dual-partitioning** (venue first, then ticker for mapped or ISIN for unmapped):

```python
# Partition structure for aggregated OHLCV
data/de/xetra/stocks_1m/
  venue=DETR/                    # Xetra venue
    ticker=dbk/                  # Mapped ISINs (lowercase ticker)
      year=2025/month=11/data.parquet
    ticker=sie/
      year=2025/month=11/data.parquet
    isin=LU1234567890/           # Unmapped ISINs (ISIN as partition key)
      year=2025/month=11/data.parquet
    isin=CH0012345678/
      year=2025/month=11/data.parquet
  venue=DFRA/                    # Frankfurt venue
    ticker=dbk/
      year=2025/month=11/data.parquet
```

**Schema for `ticker=` partitions** (mapped ISINs):
```python
{
    "date": "date",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "trades": "int64",          # Xetra-specific: trade count
    "currency": "string",       # Xetra-specific: trading currency
    "venue": "string",          # Xetra-specific: XETA, XETB
    # Note: No ISIN column (ticker is partition key)
}
```

**Schema for `isin=` partitions** (unmapped ISINs):
```python
{
    "isin": "string",           # Included for clarity (also partition key)
    "date": "date",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "trades": "int64",
    "currency": "string",
    "venue": "string",
}
```

**Rationale**:
- **Venue-first partitioning**: Low cardinality (5 venues) before high cardinality (4,280 tickers) follows Hive best practice
- **DuckDB optimization**: `WHERE venue = 'DETR'` uses partition pruning on first partition level
- **Schema consistency**: Raw trades and aggregated OHLCV both use venue-first ordering
- **Clear separation**: `ticker=X` means "we know this ticker", `isin=Y` means "unmapped"
- **No data loss**: Preserve unmapped trades with full ISIN identifier
- **Queryable**: Users can query by ISIN if they know it (`stocks_1m/venue=DETR/isin=LU1234567890/`)
- **Backfill-friendly**: When mapping discovered later, migrate `isin=X/` → `ticker=Y/`
- **Better than `__UNMAPPED__`**: No need to read parquet to find which ISIN

### 3.4 Implementation Phases for Mapping

**Phase 1** (MVP): 
- Store raw trades in venue-first time-partitioned layout (`venue=VENUE/year=YYYY/month=MM/day=DD/`)
- No ticker mapping yet (optional for raw trades)
- Users can manually map ISINs for analysis

**Phase 2** (Basic aggregation):
- Implement Deutsche Börse CSV scraper + parser
- Build local Parquet cache (`isin_mapping.parquet`)
- Aggregate to OHLCV for **mapped** ISINs → `venue=VENUE/ticker={ticker}/`
- Aggregate to OHLCV for **unmapped** ISINs → `venue=VENUE/isin={isin}/`
- Track new/delisted ISINs with lifecycle timestamps

**Phase 3** (Production):
- Daily cron job: `xetra-parqed update-isin-mapping`
- Backfill historical aggregations when new mappings discovered
- CLI command: `xetra-parqed map-isin <ISIN>` to query cache
- Optional: ISIN-reorganized archive for "all trades for ISIN" queries

**Phase 4** (Multi-Exchange - Future):
- Extend to Frankfurt (XFRA), Stuttgart (XSTU) CSVs
- Add `exchange` column to mapping schema
- Support OpenFIGI as fallback for non-German ISINs (ADRs, international)

---

## 4. Updated Schema Recommendations

### 4.1 Raw Trade Storage (Venue-First Time-Partitioned Landing Zone)

**Storage Path**:
```python
XETRA_TRADE_PATH = (
    "data/{market}/{source}/trades/"
    "venue={venue}/"
    "year={year}/month={month:02d}/day={day:02d}/"
    "trades.parquet"
)

# Example:
# data/de/xetra/trades/venue=DETR/year=2025/month=11/day=01/trades.parquet
# data/de/xetra/trades/venue=DFRA/year=2025/month=11/day=02/trades.parquet
```

**Rationale**:
- **Venue-first partitioning**: Low cardinality (5 venues) before high cardinality (time), follows Hive best practice
- **DuckDB optimization**: `WHERE venue = 'DETR'` uses partition pruning on first partition level
- **Source alignment**: Deutsche Börse serves one file per venue/date (all ISINs) → storage mirrors this 1:1
- **Idempotent updates**: List existing venue/dates, fetch missing dates (simple set difference)
- **Write efficiency**: 1 atomic write per venue/date vs 100+ per date (ISIN-partitioned)
- **Deduplication**: Check ~365 date directories per venue per year vs 4,280 ISIN directories
- **Query optimization**: "All trades for venue+date" (common) reads 1 file; "all trades for ISIN" (rare) scans venue/date range
- **Schema consistency**: Raw trades and aggregated OHLCV both use venue-first ordering

**Core fields** (10 - default):
```python
{
    "isin": "string",             # Security identifier (NOT partition key, stored in data)
    "price": "float64",
    "volume": "float64",
    "currency": "string",
    "trade_time": "timestamp[ns]",
    "venue": "string",            # XETA, XETB, etc.
    "trans_id": "string",
    "tick_id": "int64",
    "algo_trade": "bool",
    "source": "string",           # ETR, FRA, etc.
}
```

**Extended fields** (add 8 with `--extended-metadata`):
```python
{
    "distribution_time": "timestamp[ns]",
    "quote_type": "int8",
    "market_mechanism": "string",
    "trading_mode": "string",
    "negotiated_flag": "string",
    "modification_flag": "string",
    "pub_deferral": "string",
}
```

### 4.2 Aggregated OHLCV Schema (Venue-First Dual-Partitioned)

**For Mapped ISINs** (`venue={venue}/ticker={ticker}/year=YYYY/month=MM/`):

```python
XETRA_OHLCV_TICKER_PATH = (
    "data/{market}/{source}/stocks_{interval}/"
    "venue={venue}/"
    "ticker={ticker}/"
    "year={year}/month={month:02d}/"
    "data.parquet"
)

# Example: data/de/xetra/stocks_1m/venue=DETR/ticker=dbk/year=2025/month=11/data.parquet

# Schema (no ISIN column, ticker is partition key):
{
    "date": "date",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "vwap": "float64",            # Volume-weighted average price
    "trade_count": "int64",       # Number of trades in period
    "sequence": "int64",
    "currency": "string",
    "venue": "string",            # XETA, XETB
    "source": "string",           # xetra
    "market": "string",           # de
}
```

**For Unmapped ISINs** (`venue={venue}/isin={isin}/year=YYYY/month=MM/`):

```python
XETRA_OHLCV_ISIN_PATH = (
    "data/{market}/{source}/stocks_{interval}/"
    "venue={venue}/"
    "isin={isin}/"
    "year={year}/month={month:02d}/"
    "data.parquet"
)

# Example: data/de/xetra/stocks_1m/venue=DETR/isin=LU1234567890/year=2025/month=11/data.parquet

# Schema (includes ISIN column for clarity):
{
    "isin": "string",             # Included for clarity (also partition key)
    "date": "date",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "vwap": "float64",
    "trade_count": "int64",
    "sequence": "int64",
    "currency": "string",
    "venue": "string",
    "source": "string",
    "market": "string",
}
```

---

## 5. Updated CLI Design

### 5.1 Download Command

```bash
xetra-parqed download [OPTIONS]

Options:
  --venue TEXT          Venue code (DETR, DFRA, DGAT, DEUR) [default: DETR]
  --type TEXT           Data type (posttrade, pretrade) [default: posttrade]
  --extended-metadata   Store full 18-field schema (default: core 10 fields)
  --start-date DATE     Start date (YYYY-MM-DD)
  --end-date DATE       End date (YYYY-MM-DD)
  --last-days INTEGER   Download last N days
  --skip-existing       Skip dates already downloaded
  --concurrent INTEGER  Concurrent downloads [default: 3]
```

**Example**:
```bash
# Download missing dates (idempotent)
xetra-parqed download --venue DETR --skip-existing

# Download last 7 days
xetra-parqed download --venue DETR --last-days 7

# Download date range
xetra-parqed download --venue DETR --start-date 2025-11-01 --end-date 2025-11-30
```

### 5.2 Mapping Commands

```bash
# Build initial ISIN cache from Yahoo Finance tickers
xetra-parqed build-isin-cache --from-yf-data

# Lookup single ISIN
xetra-parqed map-isin DE0005140008

# Update ISIN→ticker mapping from Deutsche Börse CSV
xetra-parqed update-isin-mapping

# Force refresh (ignore cache age)
xetra-parqed update-isin-mapping --force

# Import custom ISIN→Ticker mapping (override)
xetra-parqed import-mapping isin_ticker_map.csv

# Show mapping statistics
xetra-parqed mapping-stats
```

### 5.3 Aggregate Command

```bash
xetra-parqed aggregate [OPTIONS]

Options:
  --interval TEXT       Aggregation interval (1m, 5m, 1h, 1d)
  --start-date DATE     Start date
  --end-date DATE       End date
  --venue TEXT          Venue code
  --require-mapping     Skip ISINs without ticker mapping (store mapped only)
  --update-existing     Re-aggregate existing partitions
  --dual-partition      Use dual-partitioning (ticker= for mapped, isin= for unmapped) [default: True]
```

---

## 6. Implementation Priority Updates

### Phase 1: Raw Trade Storage (Venue-First Time-Partitioned Landing Zone)
- ✅ Download DETR-posttrade files by date range
- ✅ Parse JSON.gz to core 10-field schema
- ✅ Store in `trades/venue=VENUE/year=YYYY/month=MM/day=DD/trades.parquet`
- ✅ Implement idempotent date-based update logic
- ✅ Store partitioned by venue+date (no ticker mapping yet)
- ✅ Implement basic CLI (`download`, `list`)

### Phase 2: ISIN Mapping & Aggregation (Venue-First Dual-Partitioned)

- 🆕 Implement Deutsche Börse CSV scraper + parser
- 🆕 Build persistent ISIN→Ticker Parquet cache (`isin_mapping.parquet`)
- 🆕 Daily cron job: `xetra-parqed update-isin-mapping`
- ✅ Aggregate raw trades to 1m OHLCV
- 🆕 Store aggregated data with `venue=VENUE/ticker={ticker}/` partition (mapped) or `venue=VENUE/isin={isin}/` (unmapped)
- ✅ Add `map-isin` and `aggregate` commands

### Phase 3: Multi-Venue Support

- ✅ Extend to DFRA, DGAT, DEUR venues
- ✅ Add `pretrade` data type support
- 🆕 Venue-specific mapping (some ISINs may differ)
- ✅ Update partition structure to `data/{venue}/xetra/...`

### Phase 4: Production Hardening

- 🆕 Track ISIN lifecycle (new/delisted) with timestamps
- 🆕 Multi-exchange CSV support (Frankfurt XFRA, Stuttgart XSTU)
- ✅ Automated backfill for newly mapped ISINs
- ✅ Mapping validation and conflict resolution
- 🆕 OpenFIGI fallback for non-German ISINs (Phase 3 enhancement)

---

## 7. Open Questions & Decisions Needed

1. **Default schema**: Core 10 fields or extended 18 fields?
   - **✅ DECIDED**: Core 10 by default, `--extended-metadata` flag for full schema

2. **ISIN mapping strategy**: ~~OpenFIGI free tier sufficient for MVP?~~
   - **✅ DECIDED**: Deutsche Börse official CSV (FREE, authoritative, unlimited)
   - See `/docs/xetra_isin_mapping_decision.md` for cost-benefit analysis

3. **Unmapped ISIN handling**: Store in separate partition or skip aggregation?
   - **✅ DECIDED**: Store in venue-first dual-partitioning with `venue=VENUE/isin={isin}/` for unmapped ISINs

4. **Venue priority**: Start with DETR only or support all 4 venues from start?
   - **✅ DECIDED**: DETR only for MVP, add multi-venue in Phase 3

5. **File naming conflict**: API returns suffix-only, need prefix enumeration?
   - **✅ DECIDED**: Enumerate known prefixes, cache discovered patterns

6. **Ticker suffix convention**: ~~Always append `.DE` for German stocks?~~
   - **✅ DECIDED**: Use Deutsche Börse CSV `Mnemonic` field directly (lowercase)
   - No suffix needed (Xetra mnemonics are unique: `dbk`, `sie`, `xd4`)

---

## 8. Updated ADR Impact

**Changes to existing ADR**:

1. **AD-2 (Dual-schema storage)**: 
   - Update per-trade schema from 15 fields to 10 (core) or 18 (extended)
   - Clarify field naming conventions (rename `lastTrade` → `price`)

2. **AD-3 (Partitioning)**:
   - Add venue-first dual-partitioning with `venue=VENUE/isin={isin}/` for unmapped ISINs
   - Partition aggregated data by `venue=VENUE/ticker={ticker}/` when mapping exists

3. **AD-5 (CLI design)**:
   - Add venue/type selection flags (`--venue`, `--type`)
   - Add ISIN mapping commands (`map-isin`, `update-isin-mapping`, etc.)
   - Add `--extended-metadata` flag for full schema

4. **✅ ADDED AD-8: ISIN→Ticker Mapping Strategy**:
   - **Decision**: Use Deutsche Börse official CSV as primary source
   - **Rationale**: FREE ($6K/year savings), authoritative, <1ms local cache lookups
   - **Alternative**: OpenFIGI API (deferred to Phase 3 for non-German ISINs)
   - **Implementation**: Web scraper + daily CSV download + Parquet cache
   - See ADR Section AD-8 for full specification

5. **NEW AD-9: API File Discovery**:
   - **Decision**: Enumerate venue/type prefixes + API suffix list
   - **Rationale**: API returns suffix-only, must construct full URLs
   - **Alternative**: Probe all combos with HEAD requests (slower)
   - **Risk**: New venue codes require code updates

---

## 9. Next Steps

1. **✅ Update ADR** with new decisions (AD-8, AD-9) and schema changes
2. **Implement Deutsche Börse CSV scraper** (BeautifulSoup + httpx)
3. **Build ISIN mapper service** (Parquet cache + in-memory lookups)
4. **Test API** file enumeration with actual downloads
5. **Validate schema** with sample aggregation (10 ISINs → OHLCV)
6. **Define mapping cache format** (Parquet with ISIN, ticker, first_seen, last_seen columns)
7. **Implement CLI** commands (`update-isin-mapping`, `map-isin`, `mapping-stats`)
8. **Create daily cron job** template for CSV updates

---

## Appendix A: Field Meaning Reference

**MiFID II Transparency Fields** (for reference):

- `mmtMarketMechanism`: Market mechanism type (1-9)
  - 8 = Continuous auction order book
- `mmtTradingMode`: Trading mode (1-6)
  - 2 = Continuous trading
- `mmtNegotTransPretrdWaivInd`: Negotiated transaction indicator
  - `-` = Not negotiated
  - `W` = Negotiated under waiver
- `mmtAlgoInd`: Algorithmic trading indicator
  - `H` = Algorithmic trade
  - `-` = Non-algorithmic trade
- `tickActionIndicator`: Tick action
  - `I` = Insert
  - `D` = Delete
  - `M` = Modify

**Recommendation**: Store these as strings (not enums) to future-proof against new codes.

---

## Appendix B: Sample API Integration Code

```python
import httpx
import asyncio
from datetime import datetime, timedelta

class XetraFileDiscovery:
    BASE_URL = "https://mfs.deutsche-boerse.com/api"
    VENUES = ["DETR", "DFRA", "DGAT", "DEUR"]
    TYPES = ["posttrade", "pretrade"]
    
    async def list_available_files(
        self,
        venues: list[str] = ["DETR"],
        types: list[str] = ["posttrade"],
        since_hours: int = 24
    ) -> list[dict]:
        """
        Discover available files from API.
        
        Returns list of file metadata:
        [{
            "url": "https://...",
            "venue": "DETR",
            "type": "posttrade",
            "datetime": datetime(...),
            "filename": "DETR-posttrade-2025-10-31T13_54.json.gz"
        }]
        """
        async with httpx.AsyncClient() as client:
            # Fetch suffix list
            resp = await client.get(
                self.BASE_URL,
                headers={"Accept": "application/json"}
            )
            api_data = resp.json()
            suffixes = api_data["CurrentFiles"]
            
            # Parse datetime from suffixes
            cutoff = datetime.utcnow() - timedelta(hours=since_hours)
            recent_suffixes = [
                s for s in suffixes
                if self._parse_datetime(s) >= cutoff
            ]
            
            # Enumerate venue/type combinations
            files = []
            for venue in venues:
                for file_type in types:
                    prefix = f"{venue}-{file_type}"
                    for suffix in recent_suffixes:
                        files.append({
                            "url": f"{self.BASE_URL}/{prefix}{suffix}",
                            "venue": venue,
                            "type": file_type,
                            "datetime": self._parse_datetime(suffix),
                            "filename": f"{prefix}{suffix}"
                        })
            
            return files
    
    def _parse_datetime(self, suffix: str) -> datetime:
        """Parse datetime from suffix like '-2025-10-31T13_54.json.gz'"""
        # Remove leading '-' and trailing '.json.gz'
        dt_str = suffix[1:].replace(".json.gz", "").replace("_", ":")
        return datetime.fromisoformat(dt_str)
```

---

**End of Addendum**
