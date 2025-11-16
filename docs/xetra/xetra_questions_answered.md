# Answers to Xetra Implementation Questions

**Date**: 2025-11-02

---

## Question 1: Accurate Schema for Per-Trade Data

### Answer
The actual Xetra JSON contains **23 fields** per trade record (not the 15 fields we initially assumed).

**Sample record structure** from `DETR-posttrade-2025-10-31T13_54.json`:

```json
{
  "messageId": "posttrade",
  "sourceName": "ETR",
  "isin": "DE0007100000",
  "currency": "EUR",
  "tickActionIndicator": "I",
  "instrumentIdCode": "I",
  "mmtMarketMechanism": "8",
  "mmtTradingMode": "2",
  "mmtNegotTransPretrdWaivInd": "-",
  "mmtModificationInd": "-",
  "mmtBenchmarkRefprcInd": "-",
  "mmtPubModeDefReason": "-",
  "mmtAlgoInd": "H",
  "quotationType": 1,
  "lastQty": 159.00,
  "lastTrade": 56.20,
  "lastTradeTime": "2025-10-31T13:54:00.042457058Z",
  "distributionDateTime": "2025-10-31T13:54:00.052903000Z",
  "tickId": 33976320,
  "instrumentId": "DE0007100000",
  "transIdCode": "1000000000000025050760176191884004245705800000006636",
  "executionVenueId": "XETA"
}
```

### Recommended Storage Schema

**Core schema** (10 fields - default):
```python
{
    "isin": "string",              # Primary identifier for partitioning
    "price": "float64",            # lastTrade renamed for clarity
    "volume": "float64",           # lastQty renamed
    "currency": "string",          # EUR, USD, etc.
    "trade_time": "timestamp[ns]", # lastTradeTime with nanosecond precision
    "venue": "string",             # executionVenueId (XETA, XETB)
    "trans_id": "string",          # transIdCode for deduplication
    "tick_id": "int64",           # Sequential order within minute
    "algo_trade": "bool",          # mmtAlgoInd: H→True, -→False
    "source": "string",            # sourceName (ETR, FRA, etc.)
}
```

**Extended schema** (add 8 regulatory fields with `--extended-metadata` flag):
- `distribution_time` - Publication timestamp
- `quote_type` - Quote type code
- `market_mechanism` - MiFID II market mechanism code
- `trading_mode` - MiFID II trading mode
- `negotiated_flag` - Negotiated transaction indicator
- `modification_flag` - Modification indicator
- `benchmark_flag` - Benchmark reference indicator
- `pub_deferral` - Publication deferral reason

**Fields to drop** (5 redundant):
- `instrumentId` - Duplicate of `isin`
- `messageId` - Always "posttrade" in trade files
- `tickActionIndicator` - Always "I" (insert) in posttrade files
- `instrumentIdCode` - Always "I"

### Field Meanings (Key Metadata)

**Core trading fields**:
- `lastTrade` → `price`: Actual execution price
- `lastQty` → `volume`: Number of shares/units traded
- `lastTradeTime`: Execution timestamp (nanosecond precision)
- `distributionDateTime`: When Deutsche Börse published the trade data

**MiFID II transparency fields** (regulatory requirements):
- `mmtMarketMechanism`: Market mechanism type (8 = continuous auction)
- `mmtTradingMode`: Trading mode (2 = continuous trading)
- `mmtAlgoInd`: Algorithmic trading flag (H = algo, - = manual)
- `mmtNegotTransPretrdWaivInd`: Negotiated trade flag (- = not negotiated)

**Identifiers**:
- `isin`: International Securities Identification Number (e.g., DE0007100000)
- `transIdCode`: Unique 64-character transaction ID for deduplication
- `tickId`: Sequential tick number within the minute bucket
- `executionVenueId`: Trading venue (XETA = Xetra main, XETB = other segment)

**Recommendation**: Use core 10-field schema by default (saves ~44% storage), offer `--extended-metadata` CLI flag for full 18-field schema for regulatory compliance use cases.

---

## Question 2: API Listing of Available Data

### How to Retrieve File List as JSON

**Discovery**: The API **does** support JSON responses!

**Method**:
```bash
curl -H "Accept: application/json" "https://mfs.deutsche-boerse.com/api/"
```

**Response format**:
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

### The Problem: Suffix-Only File Names

**Critical finding**: The API returns **only the datetime suffix**, not the full filename. The venue/type prefix is missing!

**What we get**: `-2025-10-31T13_54.json.gz`  
**What we need**: `DETR-posttrade-2025-10-31T13_54.json.gz`

### Solution: Prefix Enumeration

We must enumerate all **venue × type** combinations:

**Known venue prefixes** (4):
- `DETR` - Deutsche Börse XETRA (primary focus)
- `DFRA` - Frankfurt Stock Exchange
- `DGAT` - Börse Stuttgart (GETTEX)
- `DEUR` - Eurex (derivatives)

**Known type prefixes** (2):
- `posttrade` - Executed trades (our target)
- `pretrade` - Order book snapshots (future feature)

**Full URL construction**:
```
https://mfs.deutsche-boerse.com/api/{VENUE}-{TYPE}{SUFFIX}

Example:
https://mfs.deutsche-boerse.com/api/DETR-posttrade-2025-10-31T13_54.json.gz
                                    └──┬──┘ └───┬───┘ └────────┬────────┘
                                    venue    type        API suffix
```

### Recommended Implementation

```python
async def list_available_files(
    venues: list[str] = ["DETR"],
    types: list[str] = ["posttrade"],
    since_hours: int = 24
) -> list[dict]:
    """
    Fetch available files from Deutsche Börse API.
    
    Returns list of:
    [{
        "url": "https://mfs.deutsche-boerse.com/api/DETR-posttrade-2025-10-31T13_54.json.gz",
        "venue": "DETR",
        "type": "posttrade",
        "datetime": datetime(2025, 10, 31, 13, 54),
        "filename": "DETR-posttrade-2025-10-31T13_54.json.gz"
    }]
    """
    # 1. Fetch suffix list from API
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://mfs.deutsche-boerse.com/api/",
            headers={"Accept": "application/json"}
        )
        api_data = resp.json()
    
    suffixes = api_data["CurrentFiles"]  # ["-2025-10-31T13_54.json.gz", ...]
    
    # 2. Filter by time window
    cutoff = datetime.utcnow() - timedelta(hours=since_hours)
    recent_suffixes = [
        s for s in suffixes
        if parse_datetime_from_suffix(s) >= cutoff
    ]
    
    # 3. Enumerate venue/type combinations
    base_url = "https://mfs.deutsche-boerse.com/api"
    files = []
    
    for venue in venues:
        for file_type in types:
            prefix = f"{venue}-{file_type}"
            
            for suffix in recent_suffixes:
                files.append({
                    "url": f"{base_url}/{prefix}{suffix}",
                    "venue": venue,
                    "type": file_type,
                    "datetime": parse_datetime_from_suffix(suffix),
                    "filename": f"{prefix}{suffix}"
                })
    
    return files

def parse_datetime_from_suffix(suffix: str) -> datetime:
    """
    Parse '-2025-10-31T13_54.json.gz' → datetime(2025, 10, 31, 13, 54)
    """
    # Remove leading '-' and trailing '.json.gz'
    dt_str = suffix[1:].replace(".json.gz", "")
    # Replace '_' with ':' in time part
    dt_str = dt_str.replace("_", ":")
    # Parse ISO 8601 format
    return datetime.fromisoformat(dt_str)
```

### CLI Design for Venue/Type Selection

```bash
# Download specific venue/type (default: DETR posttrade)
xetra-parqed download --venue DETR --type posttrade

# Download multiple venues
xetra-parqed download --venue DETR --venue DFRA --type posttrade

# Download last 24 hours
xetra-parqed download --venue DETR --type posttrade --last-hours 24

# List available files without downloading
xetra-parqed list-files --venue DETR --type posttrade
```

---

## Question 3: ISIN to Ticker Mapping

> **🎯 UPDATE (2025-11-02)**: **Deutsche Börse CSV approach approved** as primary solution.  
> See new strategy documents for implementation details:
> - `/docs/xetra_isin_mapping_strategy.md` - Full technical specification
> - `/docs/xetra_isin_mapping_decision.md` - Cost-benefit analysis ($6K/year savings)
> - `/docs/xetra_isin_mapping_comparison.md` - Before/after comparison
>
> **Key advantages**: FREE (vs $500/month), authoritative, <1ms lookups, unlimited rate.  
> OpenFIGI information below kept for reference as Phase 3 fallback option.

### The Problem

**Xetra data contains ONLY ISINs**, not ticker symbols.

**Example mappings needed**:
- ISIN `DE0005140008` → Xetra mnemonic `DBK` (Deutsche Bank)
- ISIN `DE0007236101` → Xetra mnemonic `SIE` (Siemens)
- ISIN `AT000000STR1` → Xetra mnemonic `XD4` (Strabag, Austrian)
- ISIN `US0378331005` → Xetra mnemonic (if traded on XETRA)

**Where to find the mapping?** Unfortunately, **not in the Xetra trade data itself**. The JSON records only contain:
- `isin` - International identifier
- `instrumentId` - Duplicate of ISIN
- No ticker symbol field at all

### **APPROVED** Solution: Deutsche Börse Official CSV

**✅ PRIMARY APPROACH**: Use Deutsche Börse's free "All Tradable Instruments" CSV

**Data Source**:
- **URL**: https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments
- **Format**: Semicolon-delimited CSV, 4,280+ XETRA instruments
- **Update**: Daily at ~11:54 PM CET
- **Cost**: **FREE** (official exchange data)

**CSV Fields**:
- Column 3: `ISIN` (e.g., `DE0005140008`)
- Column 7: `Mnemonic` (ticker, e.g., `DBK`)
- Column 2: `Instrument` (full name)
- Column 122: `Currency` (e.g., `EUR`)

**Implementation**:
1. Web scraper extracts dynamic CSV download URL
2. Daily cron job: `xetra-parqed update-isin-mapping`
3. Parquet cache: `data/reference/isin_mapping.parquet`
4. Runtime lookups: In-memory dict (<1ms)

**Advantages**:
- ✅ **FREE** (vs $500/month for OpenFIGI = **$6,000/year savings**)
- ✅ **Authoritative**: Official exchange source
- ✅ **Fast**: <1ms local cache lookups
- ✅ **Unlimited**: No rate limits
- ✅ **Reliable**: 100% uptime via local cache

**Trade-offs**:
- ⚠️ Daily update lag (0-24 hours for new IPOs)
- ⚠️ Web scraping complexity (mitigated with monitoring)

---

### Alternative Solution: OpenFIGI API (Phase 3 Fallback)

> **Note**: Originally planned as primary solution, now deferred to Phase 3 as optional fallback for non-German ISINs (ADRs, international listings).

**Provider**: Bloomberg (free + paid tiers)  
**API Endpoint**: `https://api.openfigi.com/v3/mapping`  
**Documentation**: https://www.openfigi.com/api

**Free tier limits**:
- 25 requests per second
- 5 ISINs per request (batching)
- No API key required (but recommended for higher reliability)

**Paid tier limits**:
- 250 requests per second
- API key required
- Cost: ~$500/month

**Coverage**: 300M+ securities worldwide including:
- Equities, ETFs, bonds, derivatives
- Multiple exchanges per ISIN
- Historical mappings

**Example API request**:
```python
import httpx
import asyncio

async def isin_to_ticker(isin: str, exchange: str = "GR") -> dict | None:
    """
    Map ISIN to ticker using OpenFIGI API.
    
    Args:
        isin: ISIN code (e.g., "DE0007100000")
        exchange: Exchange code (GR = Germany, US = United States)
    
    Returns:
        {
            "isin": "DE0007100000",
            "ticker": "MBG",
            "exchange_code": "GR",
            "yahoo_ticker": "MBG.DE",
            "name": "Mercedes-Benz Group AG",
            "market_sector": "Equity"
        }
    """
    payload = [{
        "idType": "ID_ISIN",
        "idValue": isin,
        "exchCode": exchange,  # GR = Germany
    }]
    
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.openfigi.com/v3/mapping",
            json=payload,
            headers={
                "X-OPENFIGI-APIKEY": api_key,  # Optional for free tier
                "Content-Type": "application/json"
            }
        )
        
        if resp.status_code != 200:
            return None
        
        data = resp.json()
        
        if not data or not data[0].get("data"):
            return None
        
        # Extract first equity result
        for result in data[0]["data"]:
            if result.get("marketSector") == "Equity":
                ticker = result.get("ticker")
                exchange_code = result.get("exchCode")
                
                # Construct Yahoo ticker (append exchange suffix)
                yahoo_suffix = EXCHANGE_SUFFIX_MAP.get(exchange_code, ".DE")
                yahoo_ticker = f"{ticker}{yahoo_suffix}"
                
                return {
                    "isin": isin,
                    "ticker": ticker,
                    "exchange_code": exchange_code,
                    "yahoo_ticker": yahoo_ticker,
                    "name": result.get("name"),
                    "market_sector": result.get("marketSector")
                }
    
    return None

# Exchange code to Yahoo suffix mapping
EXCHANGE_SUFFIX_MAP = {
    "GR": ".DE",  # Germany (XETRA)
    "GF": ".F",   # Frankfurt Stock Exchange
    "US": "",     # US stocks (no suffix)
    "LN": ".L",   # London Stock Exchange
    "AS": ".AS",  # Euronext Amsterdam
}
```

### Alternative Solutions

**Option 1: Local cache with yfinance reverse lookup**
- Only works for tickers you already have in yf-parqed
- One-time job: iterate existing Yahoo tickers, fetch `.info["isin"]`, build cache
- Limitation: Won't discover new ISINs from Xetra data

**Option 2: Deutsche Börse reference data** (official but complex)
- Provider: Deutsche Börse (free via FTP)
- Format: Daily CSV snapshots
- Contains: ISIN + local ticker (e.g., "MBG" not "MBG.DE")
- Post-processing: Must still append exchange suffix and verify with Yahoo

**Option 3: Manual CSV import** (for custom/unlisted securities)
```bash
# User provides custom mapping file
xetra-parqed import-mapping custom_isin_ticker.csv

# CSV format:
# isin,ticker,exchange,notes
# DE0007100000,MBG.DE,XETRA,Mercedes-Benz
# DE000BASF111,BAS.DE,XETRA,BASF
```

### Recommended Hybrid Approach

```python
class ISINMapper:
    """
    Multi-tier ISIN to ticker mapping with persistent cache.
    """
    
    def __init__(self, cache_path: str = "isin_cache.parquet"):
        self.cache_path = cache_path
        self.cache = self._load_cache()
        self.openfigi_client = OpenFIGIClient(rate_limit=25)
        self.unmapped = set()
        
        # Stats
        self.cache_hits = 0
        self.cache_misses = 0
        self.api_calls = 0
    
    async def get_ticker(self, isin: str) -> str | None:
        """
        Resolve ISIN to Yahoo ticker.
        
        Lookup order:
        1. Local cache (instant)
        2. OpenFIGI API (rate-limited)
        3. Return None if unmapped
        """
        # Tier 1: Check local cache
        if ticker := self.cache.get(isin):
            self.cache_hits += 1
            return ticker
        
        self.cache_misses += 1
        
        # Tier 2: Query OpenFIGI
        try:
            result = await self.openfigi_client.lookup(isin)
            if result:
                ticker = result["yahoo_ticker"]
                
                # Update cache
                self.cache[isin] = ticker
                self._save_cache()
                
                self.api_calls += 1
                return ticker
        
        except RateLimitError:
            logger.warning(f"OpenFIGI rate limit hit, will retry later for {isin}")
            return None
        
        except Exception as e:
            logger.error(f"OpenFIGI error for {isin}: {e}")
        
        # Tier 3: Mark as unmapped
        self.unmapped.add(isin)
        logger.warning(f"No ticker found for ISIN {isin}")
        return None
    
    def _load_cache(self) -> dict[str, str]:
        """Load cache from parquet file."""
        if not Path(self.cache_path).exists():
            return {}
        
        df = pd.read_parquet(self.cache_path)
        return dict(zip(df["isin"], df["ticker"]))
    
    def _save_cache(self):
        """Persist cache to parquet file."""
        df = pd.DataFrame([
            {"isin": isin, "ticker": ticker, "last_updated": datetime.utcnow()}
            for isin, ticker in self.cache.items()
        ])
        df.to_parquet(self.cache_path, index=False)
    
    def get_stats(self) -> dict:
        """Return mapping statistics."""
        total_lookups = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total_lookups if total_lookups > 0 else 0
        
        return {
            "cached_isins": len(self.cache),
            "unmapped_isins": len(self.unmapped),
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_rate": f"{hit_rate:.1%}",
            "api_calls": self.api_calls
        }
```

### Handling Unmapped ISINs

**Problem**: Some ISINs may never map to Yahoo tickers:
- Delisted securities
- Regional-only instruments not on Yahoo
- Bonds/derivatives (if we expand beyond equities)

**Solution**: Store aggregated data with special partition:

```
data/de/xetra/stocks_1m/
  ticker=MBG.DE/          # Mapped ISINs
    year=2025/month=10/data.parquet
  ticker=BAS.DE/
    year=2025/month=10/data.parquet
  ticker=__UNMAPPED__/    # Fallback for unmapped ISINs
    year=2025/month=10/data.parquet  # Contains ISIN column
```

**Schema for unmapped partition**:
```python
{
    "isin": "string",           # Primary identifier
    "ticker": "string",         # Always "__UNMAPPED__"
    "date": "date",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "sequence": "int64",
    "currency": "string",
    # ... other OHLCV fields
}
```

**Benefit**: When mapping is discovered later, can backfill historical data:
```bash
# Discover new mapping
xetra-parqed map-isin DE0007100000
# Output: Mapped DE0007100000 → MBG.DE

# Backfill historical aggregations
xetra-parqed backfill-mapped --isin DE0007100000 --start 2025-01-01
# Reads from __UNMAPPED__ partition, re-aggregates to ticker=MBG.DE partition
```

### CLI Commands for ISIN Mapping

```bash
# Lookup single ISIN
xetra-parqed map-isin DE0007100000
# Output:
# ISIN: DE0007100000
# Ticker: MBG.DE
# Name: Mercedes-Benz Group AG
# Exchange: XETRA (GR)
# Source: OpenFIGI API

# Build initial cache from existing Yahoo Finance data
xetra-parqed build-isin-cache --from-yf-data
# Iterates existing yf-parqed tickers, extracts ISINs, populates cache

# Batch map all unmapped ISINs (requires OpenFIGI API key for large batches)
xetra-parqed map-all-isins --api-key YOUR_API_KEY --batch-size 100
# Processes unmapped ISINs in batches, respects rate limits

# Import custom ISIN→Ticker mapping CSV
xetra-parqed import-mapping custom_mappings.csv
# Format: isin,ticker,exchange

# Show mapping statistics
xetra-parqed mapping-stats
# Output:
# Cached ISINs: 5,423
# Unmapped ISINs: 127
# Cache hit rate: 97.7%
# API calls today: 234

# Export cache for backup/sharing
xetra-parqed export-cache --output isin_cache_backup.csv
```

---

## Summary of Answers

1. **Accurate schema**: 23 fields in source data, recommend storing 10 core fields by default (18 with `--extended-metadata` flag)

2. **API file listing**: Yes, API supports JSON with `Accept: application/json` header, but returns suffix-only (must enumerate venue×type prefixes)

3. **ISIN to ticker mapping**: **✅ UPDATED** - Use Deutsche Börse official CSV (FREE, authoritative, <1ms lookups) instead of OpenFIGI API ($500/month). Store unmapped ISINs in `__UNMAPPED__` partition for later resolution. **$6,000/year cost savings.**

**Next steps**: See `docs/xetra_key_findings_summary.md` and `docs/xetra_implementation_addendum.md` for complete implementation details.

**New strategy docs**:
- `docs/xetra_isin_mapping_strategy.md` - Full implementation specification
- `docs/xetra_isin_mapping_decision.md` - Cost-benefit analysis
- `docs/xetra_isin_mapping_comparison.md` - Before/after comparison
