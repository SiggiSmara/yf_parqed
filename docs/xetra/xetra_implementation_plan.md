# Xetra Data Integration - Detailed Implementation Plan

## Executive Summary

This document provides a comprehensive architectural design for integrating Deutsche Börse Xetra delayed trade data into the yf_parqed system, maintaining alignment with existing Yahoo Finance data patterns while accommodating the unique characteristics of per-trade data and European market requirements.

## Current Architecture Analysis

### Yahoo Finance Data Flow

**Schema (OHLCV + metadata)**:
```python
{
    "stock": string,        # Ticker symbol
    "date": datetime64[ns], # Timestamp (index)
    "open": float64,
    "high": float64,
    "low": float64,
    "close": float64,
    "volume": Int64,
    "sequence": Int64       # Monotonic ID for deduplication
}
```

**Intervals**: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo

**Storage Pattern** (Partitioned):
```
data/
  {market}/           # e.g., "us"
    {source}/         # e.g., "yahoo"
      stocks_{interval}/
        ticker={TICKER}/
          year=YYYY/
            month=MM/
              data.parquet
```

**Key Observations**:
1. **No split/dividend adjustment tracking** - Yahoo Finance returns split-adjusted prices by default, but we don't track when splits occur or store adjustment factors
2. **Interval-based aggregation** - All data is pre-aggregated into time buckets
3. **Single-level schema** - Uniform OHLCV structure across all intervals
4. **Per-ticker partitioning** - Year/month partitions within each ticker
5. **Rate limiting required** - Yahoo API enforces strict limits (default: 3 req/2s)

## Xetra Data Characteristics

### Deutsche Börse Delayed Data Feed

**Source**: `https://mfs.deutsche-boerse.com/api/download/DETR-posttrade-{YYYY-MM-DD}T{HH_MM}.json.gz`

**Venues**:
- `DETR` - Xetra (primary German exchange)
- `DETG` - Tradegate (retail platform)

**Data Retention**: ~24 hours rolling window

**Record Structure** (per-trade):
```json
{
  "ISIN": "DE0005810055",
  "Mnemonic": "DBK",
  "SecurityDesc": "DEUTSCHE BANK AG",
  "SecurityType": "Common stock",
  "Currency": "EUR",
  "SecurityID": 123456,
  "Date": "2025-10-10",
  "Time": "09:00:01.123",
  "StartPrice": 10.50,
  "MaxPrice": 10.52,
  "MinPrice": 10.48,
  "EndPrice": 10.51,
  "TradedVolume": 1000,
  "NumberOfTrades": 5
}
```

**Key Differences from Yahoo**:
1. **Per-trade granularity** - Individual transactions, not pre-aggregated
2. **ISIN-based identification** - Need mapping to ticker symbols
3. **Multiple identifiers** - ISIN, Mnemonic, SecurityID all present
4. **Minute-level buckets** - Data already aggregated to 1-minute windows in source
5. **No rate limiting needed** - Static file downloads, no API quota
6. **Short retention** - Must backfill quickly before data expires

## Schema Design

### Xetra Trade-Level Schema

For raw per-trade data storage:

```python
{
    "isin": string,              # Primary identifier (DE0005810055)
    "mnemonic": string,          # Ticker symbol (DBK)
    "security_desc": string,     # Company name
    "security_type": string,     # "Common stock", "ETF", etc.
    "currency": string,          # EUR, USD, etc.
    "security_id": Int64,        # Exchange-assigned ID
    "date": datetime64[ns],      # Trade timestamp (UTC)
    "start_price": float64,      # Opening price for minute bucket
    "max_price": float64,        # High
    "min_price": float64,        # Low
    "end_price": float64,        # Closing price
    "traded_volume": Int64,      # Volume
    "number_of_trades": Int64,   # Trade count
    "venue": string,             # XETR, TGAT, etc.
    "sequence": Int64            # Monotonic ID (added by us)
}
```

### Xetra Aggregated Schema (1m/1h/1d alignment)

For compatibility with existing analytics expecting OHLCV format:

```python
{
    "stock": string,        # Mnemonic (DBK) - for compatibility
    "isin": string,         # DE0005810055 - additional identifier
    "date": datetime64[ns], # Timestamp
    "open": float64,        # start_price
    "high": float64,        # max_price
    "low": float64,         # min_price
    "close": float64,       # end_price
    "volume": Int64,        # traded_volume
    "trades": Int64,        # number_of_trades (Xetra-specific)
    "venue": string,        # XETR, TGAT (Xetra-specific)
    "sequence": Int64       # Monotonic ID
}
```

**Rationale**: Maintain `stock`/`date`/OHLCV core for drop-in compatibility with existing YFParqed workflows, add Xetra-specific fields (`trades`, `venue`, `isin`) for richer analysis.

## Partition Storage Layout

### Proposed Structure

```
data/
  us/
    yahoo/
      stocks_1d/
        ticker=AAPL/
          year=2024/
            month=11/
              data.parquet
              
  de/
    xetra/
      trades/                    # Raw per-trade data (minute buckets from source)
        isin=DE0005810055/
          year=2024/
            month=11/
              data.parquet
      stocks_1m/                 # Aggregated 1-minute bars (passthrough from source)
        ticker=DBK/
          year=2024/
            month=11/
              data.parquet
      stocks_1h/                 # Aggregated hourly bars
        ticker=DBK/
          year=2024/
            month=11/
              data.parquet
      stocks_1d/                 # Aggregated daily bars
        ticker=DBK/
          year=2024/
            month=11/
              data.parquet
              
    tradegate/
      trades/                    # Tradegate trades
        isin=DE0005810055/
          year=2024/
            month=11/
              data.parquet
      stocks_1m/
        ticker=DBK/
          year=2024/
            month=11/
              data.parquet
      # ... 1h, 1d
```

**Design Decisions**:

1. **Venue-first hierarchy** - Market → Source → Dataset pattern
   - Prevents accidental mixing of Xetra/Tradegate data
   - Allows per-venue configuration and retention policies
   
2. **Separate `trades/` dataset** - Distinct from `stocks_*` aggregations
   - Different schema (richer metadata)
   - Different partition key (ISIN vs ticker/mnemonic)
   - Enables raw data preservation for re-aggregation
   
3. **Ticker-based aggregations** - Use `mnemonic` for `stocks_*` datasets
   - Compatibility with existing `StorageRequest` API
   - Analyst-friendly (DBK vs DE0005810055)
   - ISIN available in schema for cross-referencing
   
4. **Month partitions** - Consistent with Yahoo data
   - Balances file count vs partition size
   - Aligns with typical analysis windows

## Split Handling Strategy

### Current Gap

**Yahoo Finance**: Returns split-adjusted prices by default, but:
- No tracking of when splits occurred
- No storage of adjustment factors
- No mechanism to detect new splits post-ingestion
- Potential data continuity issues if historical data is re-fetched

**Problem Scenarios**:
1. **Forward fill after split** - New data post-split has different price scale
2. **Historical refetch** - Yahoo returns adjusted prices, creating duplicates with different values
3. **Analysis requirements** - Some models need unadjusted prices + adjustment factors

### Proposed Solution: Split Metadata Tracking

#### Detection Strategy

**For Yahoo Data**:
1. **Ticker actions API** - Use `yfinance.Ticker.actions` to fetch split/dividend history
2. **Periodic check** - Query actions during ticker updates (daily for active tickers)
3. **Price discontinuity detection** - Flag large overnight gaps (>20%) for manual review

**For Xetra Data**:
1. **External feed** - Deutsche Börse publishes corporate actions separately
2. **Manual configuration** - Initially require operator input for known splits
3. **Future enhancement** - Integrate with corporate actions feed when available

#### Metadata Schema

Add to `tickers.json`:

```python
{
  "AAPL": {
    "ticker": "AAPL",
    "status": "active",
    "added_date": "2024-01-15",
    "intervals": { ... },
    "corporate_actions": {           # NEW
      "splits": [
        {
          "date": "2024-08-30",
          "ratio": 4.0,              # 4-for-1 split
          "detected_at": "2024-08-31",
          "applied_to_historical": true
        }
      ],
      "dividends": [
        {
          "date": "2024-11-15",
          "amount": 0.25,
          "currency": "USD",
          "ex_date": "2024-11-13"
        }
      ]
    }
  }
}
```

#### Adjustment Process

**Option 1: Store Unadjusted + Factors** (Recommended)
- Store raw unadjusted prices in parquet
- Add `split_factor` and `div_factor` columns
- Compute adjusted prices on-the-fly during analysis
- Enables both adjusted and unadjusted analysis

**Option 2: Dual Storage** (Higher overhead)
- Store both adjusted and unadjusted datasets
- `stocks_1d/` - adjusted (default)
- `stocks_1d_unadjusted/` - raw prices
- Clear separation, higher storage cost

**Recommendation**: Start with Option 1 for new Xetra data (source provides unadjusted), add factors to schema. For Yahoo data, continue with adjusted-by-default but add split tracking to detect refetch issues.

#### Implementation Phases

**Phase 1 (MVP)** - Detection only:
- Track splits in metadata
- Log warnings when splits detected
- No automatic adjustment

**Phase 2** - Adjustment factors:
- Add `split_factor` column to schema
- Populate for new data post-split
- Provide helper function for adjustment

**Phase 3** - Historical reconstruction:
- Tool to backfill adjustment factors
- Re-aggregate with unadjusted prices if needed

## Service Architecture

### Separate Primary Class: `XetraParqed`

**Rationale for New Class**:

1. **Different data source characteristics**:
   - No API rate limiting needed (static file downloads)
   - Short retention window (24h) vs indefinite Yahoo access
   - Different error handling (404 = data expired vs ticker not found)

2. **Different scheduling requirements**:
   - Time-sensitive backfilling (data expires)
   - Per-minute fetches vs per-interval batches
   - Venue-specific fetch schedules

3. **Different schema management**:
   - Two schema types (trades + aggregated)
   - ISIN-based partitioning for trades
   - Additional metadata fields

4. **Separation of concerns**:
   - YFParqed remains focused on Yahoo Finance
   - XetraParqed owns Xetra-specific logic
   - Shared infrastructure via services

**Shared Components** (via dependency injection):
- `ConfigService` - Configuration management
- `StorageBackend` - Legacy storage (if needed)
- `PartitionedStorageBackend` - Partitioned storage
- `PartitionPathBuilder` - Path construction
- `parquet_recovery` module - Error recovery

**Xetra-Specific Components**:
- `XetraFetcher` - Download/decompress/parse JSON.gz files
- `XetraTickerRegistry` - ISIN→Mnemonic mapping, corporate actions
- `XetraScheduler` - Time-sensitive backfill orchestration
- `XetraAggregator` - Build 1h/1d bars from 1m trades

### Proposed Class Structure

```python
class XetraParqed:
    def __init__(
        self,
        my_path: Path = Path.cwd(),
        venues: Sequence[str] | None = None,  # ["xetra", "tradegate"]
        my_intervals: Sequence[str] | None = None,  # ["1m", "1h", "1d"]
        store_trades: bool = True,  # Also store raw trade data
    ):
        self.config = ConfigService(my_path)
        self.venues = venues or ["xetra"]
        self.my_intervals = my_intervals or ["1m", "1h", "1d"]
        self.store_trades = store_trades
        
        # Shared infrastructure
        self.partition_storage = PartitionedStorageBackend(
            empty_frame_factory=self._empty_trade_frame,  # OR _empty_stock_frame
            normalizer=self._normalize_trade_frame,
            column_provider=self._trade_frame_columns,
            path_builder=PartitionPathBuilder(root=my_path / "data"),
        )
        
        # Xetra-specific services
        self.ticker_registry = XetraTickerRegistry(config=self.config)
        self.fetcher = XetraFetcher(
            session_factory=lambda: httpx.Client(),
            decompressor=gzip.decompress,
        )
        self.aggregator = XetraAggregator(
            intervals=self.my_intervals,
        )
        self.scheduler = XetraScheduler(
            registry=self.ticker_registry,
            fetcher=self.fetcher,
            storage=self.partition_storage,
            venues=self.venues,
            intervals=self.my_intervals,
        )
```

### Unified CLI vs Separate Binary

**Option 1: Extend `yf-parqed` CLI**:
```bash
yf-parqed initialize --source yahoo
yf-parqed initialize --source xetra --venue xetra
yf-parqed update-data --source yahoo
yf-parqed update-data --source xetra --backfill-hours 24
```

**Option 2: Separate `xetra-parqed` CLI**:
```bash
xetra-parqed initialize --venue xetra
xetra-parqed update-data --backfill-hours 24
xetra-parqed verify-isin-mapping
```

**Recommendation**: **Option 2** (Separate CLI)
- Clearer responsibility boundaries
- Different operational concerns (time-sensitive backfill vs periodic updates)
- Avoids bloating yf-parqed with Xetra-specific flags
- Both can coexist in same workspace (`data/` root)

## Recovery Process Adaptations

### Schema Differences

**Xetra Trade Schema** adds:
- `isin`, `mnemonic`, `security_desc`, `security_type`, `currency`, `security_id`
- `start_price`, `max_price`, `min_price`, `end_price` (vs `open`, `high`, `low`, `close`)
- `traded_volume`, `number_of_trades`
- `venue`

**Xetra Stock Schema** (aggregated) adds:
- `isin`, `trades`, `venue`

### Recovery Module Adaptation

Current `parquet_recovery.py` expects:
```python
required_columns = {"stock", "date", "open", "high", "low", "close", "volume", "sequence"}
```

**Proposed Enhancement**: Schema-aware recovery

```python
class SchemaDefinition:
    """Define expected schema for recovery validation."""
    def __init__(
        self,
        required_columns: set[str],
        optional_columns: set[str] | None = None,
        column_aliases: dict[str, str] | None = None,  # NEW
    ):
        self.required_columns = required_columns
        self.optional_columns = optional_columns or set()
        self.column_aliases = column_aliases or {}
        
    def validate(self, df: pd.DataFrame) -> bool:
        # Check aliases first
        for alias, canonical in self.column_aliases.items():
            if alias in df.columns and canonical not in df.columns:
                df.rename(columns={alias: canonical}, inplace=True)
        return self.required_columns.issubset(df.columns)

# Usage in XetraParqed
XETRA_TRADE_SCHEMA = SchemaDefinition(
    required_columns={"isin", "date", "start_price", "max_price", "min_price", 
                      "end_price", "traded_volume", "sequence"},
    optional_columns={"mnemonic", "security_desc", "currency", "venue"},
    column_aliases={"Date": "date", "ISIN": "isin"},  # Handle source inconsistencies
)

XETRA_STOCK_SCHEMA = SchemaDefinition(
    required_columns={"stock", "date", "open", "high", "low", "close", "volume", "sequence"},
    optional_columns={"isin", "trades", "venue"},
)
```

**Alternative**: Multiple recovery normalizers
- `_normalize_trade_frame()` - Handles Xetra trade schema
- `_normalize_stock_frame()` - Handles aggregated OHLCV schema
- Inject appropriate normalizer based on dataset type

**Recommendation**: Use column aliases in normalizer functions, keep recovery module schema-agnostic. Each primary class (YFParqed, XetraParqed) provides its own normalizer that handles source-specific quirks.

### Index Promotion Logic

Current logic promotes unnamed `index` column to `sequence`. 

**For Xetra**:
- Source data has no `sequence` column
- Need to generate monotonic ID during ingestion
- Add `sequence` column in normalizer: `df['sequence'] = range(len(df))`
- Recovery module continues to work unchanged

## Rate Limiting Assessment

### Yahoo Finance

**Requirement**: **YES**
- API enforces quotas (~2000 requests/hour)
- 429 errors if exceeded
- Current implementation: 3 requests per 2 seconds

### Deutsche Börse Delayed Data

**Requirement**: **NO**

**Rationale**:
1. **Static file downloads** - Not an API, just HTTP GET on gzip files
2. **No authentication** - Anonymous access
3. **No documented rate limits** - Designed for batch download
4. **Short retention** - Download quickly then process locally
5. **Predictable URLs** - Generate URL, fetch file, done

**Implementation**:
- Use `httpx` with default retry logic
- Handle 404 gracefully (data expired/not yet available)
- Add exponential backoff for transient errors (503, timeout)
- No inter-request delays needed

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)

**Goal**: Establish core Xetra ingestion without aggregation

1. **XetraFetcher service**:
   - URL generation from venue/timestamp
   - Download + decompress JSON.gz
   - Parse to DataFrame
   - Unit tests with fixtures

2. **XetraParqed skeleton**:
   - Initialize config
   - Load ticker registry (ISIN mapping)
   - Wire fetcher + storage

3. **Trade schema storage**:
   - Define `_empty_trade_frame()` and `_normalize_trade_frame()`
   - Extend PartitionedStorageBackend with trade dataset support
   - Write trades to `data/de/xetra/trades/isin=.../`

4. **CLI basics**:
   - `xetra-parqed initialize --venue xetra`
   - `xetra-parqed backfill --hours 24`
   - Logging and progress display

**Acceptance Criteria**:
- Fetch and store 1-minute trade data for single ISIN
- Verify partition layout correct
- Handle missing files (404) gracefully

### Phase 2: Aggregation & Multi-Interval (Weeks 3-4)

**Goal**: Generate 1h/1d aggregations from 1m data

1. **XetraAggregator service**:
   - Load 1m trades for ticker/date range
   - Resample to 1h bins (OHLC logic)
   - Resample to 1d bins
   - Preserve `trades` count, `venue`

2. **Aggregated schema storage**:
   - Define `_empty_stock_frame()` with Xetra extensions
   - Write to `data/de/xetra/stocks_1h/`, `stocks_1d/`

3. **Scheduler integration**:
   - XetraScheduler orchestrates: fetch → store trades → aggregate → store stocks
   - Handle missing minutes (gaps in source data)

4. **CLI enhancements**:
   - `xetra-parqed update-data --intervals 1m,1h,1d`
   - `xetra-parqed verify --interval 1d`

**Acceptance Criteria**:
- Fetch 24h of 1m data
- Generate matching 1h and 1d aggregations
- Verify row counts consistent

### Phase 3: Split Tracking (Weeks 5-6)

**Goal**: Detect and track corporate actions

1. **Corporate actions schema**:
   - Extend `tickers.json` with `corporate_actions`
   - Add split/dividend metadata

2. **Yahoo split detection**:
   - Use `yfinance.Ticker.actions` API
   - Populate `corporate_actions.splits`
   - Log warnings for new splits

3. **Xetra split handling**:
   - Manual configuration initially
   - Store in `de_tickers.json` or similar

4. **Adjustment factors** (optional):
   - Add `split_factor` column to schema
   - Populate for new data post-detection
   - Helper function for adjusted prices

**Acceptance Criteria**:
- Detect AAPL 4-for-1 split in test data
- Store split metadata
- Log clear warnings when refetching historical data post-split

### Phase 4: Production Hardening (Weeks 7-8)

**Goal**: Multi-venue, error recovery, operational tooling

1. **Tradegate support**:
   - Add `DETG` venue to fetcher
   - Separate partition paths
   - Test mixed Xetra/Tradegate workflow

2. **ISIN mapping**:
   - Download/parse German ticker lists
   - Build ISIN→Mnemonic index
   - CLI command to refresh mappings

3. **Recovery & validation**:
   - Corrupt parquet handling (existing module works)
   - Missing minute detection and alerts
   - Row count verification tools

4. **Documentation**:
   - Usage guide
   - Schema reference
   - Migration plan for existing Yahoo users

**Acceptance Criteria**:
- Both Xetra and Tradegate working
- Missing data gaps logged clearly
- Comprehensive test coverage

### Phase 5: Advanced Features (Future)

1. **DuckDB integration**:
   - Query trades and aggregations without loading full dataset
   - Zero-copy analytics
   
2. **Corporate actions feed**:
   - Integrate Deutsche Börse corporate actions API
   - Automatic split detection
   
3. **Cross-venue analysis**:
   - Compare Xetra vs Tradegate prices
   - Detect arbitrage opportunities
   
4. **Real-time upgrade path**:
   - WebSocket feed integration
   - Live tick storage

## Testing Strategy

### Unit Tests

**XetraFetcher**:
- URL generation (various venues/timestamps)
- JSON parsing (valid/malformed data)
- Decompression errors
- HTTP error handling (404, 503, timeout)

**XetraAggregator**:
- 1m → 1h resampling (OHLC logic)
- 1h → 1d resampling
- Gaps/missing minutes handling
- Trade count preservation

**XetraParqed**:
- Initialization
- Config management
- Storage backend wiring

### Integration Tests

**End-to-end flow**:
1. Fetch 1-minute file (mocked HTTP)
2. Parse and store trades
3. Aggregate to 1h/1d
4. Verify parquet files exist
5. Read back and validate schema

**Multi-venue**:
1. Fetch Xetra + Tradegate data for same ISIN
2. Verify separate partition paths
3. Aggregate independently

### Fixtures

```python
# tests/fixtures/xetra_sample.json.gz
# Contains 60 minutes of DBK trades (2024-11-02 09:00-10:00)

# tests/fixtures/tradegate_sample.json.gz  
# Contains 60 minutes of DBK trades (same period)
```

## Open Questions & Decisions Needed

1. **ISIN vs Mnemonic for primary key**:
   - Trades: Use ISIN (stable across venues)
   - Aggregations: Use Mnemonic (analyst-friendly)
   - **Decision**: Proceed as proposed

2. **Storage of raw JSON**:
   - Keep original .json.gz files for audit trail?
   - Or trust parquet as source of truth?
   - **Decision**: Parquet only (can re-download from Deutsche Börse if needed within 24h)

3. **Multi-currency handling**:
   - Store all prices in native currency (EUR for German stocks)
   - Add `currency` column
   - Conversion to USD happens in analysis layer
   - **Decision**: Store native currency, document in schema

4. **Backward compatibility**:
   - Should `yf-parqed update-data` trigger Xetra updates?
   - Or always require separate `xetra-parqed` invocation?
   - **Decision**: Separate CLIs, no automatic cross-triggering

5. **Data retention policy**:
   - Yahoo: Keep indefinitely
   - Xetra: Keep how long?
   - **Decision**: Document recommended retention, leave to operator (no automatic deletion)

## Success Metrics

1. **Functional**: Fetch and store 24 hours of Xetra 1m data for 100 ISINs
2. **Performance**: Process 24h of data for 100 ISINs in <5 minutes
3. **Reliability**: Handle 404s (expired data) gracefully, no crashes
4. **Correctness**: 1h/1d aggregations match manual OHLC calculation
5. **Usability**: Clear CLI commands, good error messages

## Conclusion

This design provides a **parallel implementation** to YFParqed that:
- Reuses proven infrastructure (partition storage, recovery, config)
- Accommodates unique Xetra requirements (per-trade data, short retention, ISIN mapping)
- Maintains compatibility with existing workflows via consistent schemas
- Enables future enhancements (splits, DuckDB, real-time)

**Key Architectural Decision**: Separate `XetraParqed` class enables clean separation of concerns while sharing battle-tested components. The investment in dual-schema storage (trades + aggregations) pays off with flexibility for both raw data analysis and drop-in OHLCV compatibility.
