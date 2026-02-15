# ADR: OHLCV Aggregation Service

## Status: To-Do (agreed; Phase 2a normalization prioritized before aggregation)

**Context**: Xetra Phase 2 Implementation - Revised Approach

**Last Updated**: 2026-02-15

**Key Design Decisions**:
- **CLI Interface**: Single `update` command with auto-detection (primary interface)
- **Debugging**: Use `--dry-run` and `--verbose` flags for visibility, not separate manual commands
- **Simplicity**: System determines what needs processing, user controls pipeline stages via flags
- **Phased Approach**: Split into 2a (normalization) and 2b (aggregation) for incremental testing

## Problem Statement

We store raw tick-level data from two sources:
1. **Yahoo Finance**: Pre-aggregated OHLCV at 1m/1h/1d intervals
2. **Xetra**: Raw per-trade data (price, volume, timestamp per transaction)

Xetra raw trades need aggregation to OHLCV format for:
- Consistency with Yahoo Finance data model
- Efficient time-series analysis (daily/hourly patterns)
- Reduced data volume for common queries
- Enable cross-source analytics (compare US vs German equities)

**Key Challenge**: Current Xetra storage is venue-first (all ISINs mixed in daily files). Need ISIN-first partitioning for efficient queries and aggregation.

## Revised Implementation Approach: Phased Storage Optimization

**Decision**: Split Phase 2 into two sequential sub-phases:
- **Phase 2a**: Normalize raw trade storage (venue → ISIN partitioning, no aggregation)
- **Phase 2b**: Aggregate normalized trades to OHLCV

### Phase 2a: Raw Trade Normalization (Prioritized)

**Goal**: Re-partition raw trades from venue-first to ISIN-first layout, preserving all raw data.

**Rationale**:
1. **Separation of concerns**: Storage optimization independent of aggregation logic
2. **Incremental testing**: Validate partitioning before adding aggregation complexity
3. **DuckDB optimization**: Enable efficient ISIN-filtered queries on raw trades
4. **Reusable foundation**: Both ad-hoc analysis and automated aggregation benefit

**Storage Transformation**:
```python
# Current (Phase 1):
data/de/xetra/trades/venue=DETR/year=2025/month=12/day=05/trades.parquet
  → All ISINs mixed, ~500K trades/file, ~50MB/day

# Phase 2a Target:
data/de/xetra/trades_by_isin/venue=DETR/isin=DE0005190003/year=2025/month=12/trades.parquet
  → One file per ISIN per month per venue, still raw trades (no aggregation)
  → ~100KB/ISIN/month, ~50MB total for 500 ISINs
```

**Processing Flow**:
```
1. Load all daily files for a month: trades/venue=DETR/year=2025/month=12/day=*/
2. Group by ISIN
3. For each ISIN:
   - Extract all trades
   - Sort by trade_time
   - Write to trades_by_isin/venue=DETR/isin=<ISIN>/year=2025/month=12/trades.parquet
4. Preserve ALL columns (no schema changes, no aggregation)
```

**CLI Command**:
```bash
# Intelligent update (auto-detects what needs processing) - PRIMARY INTERFACE
xetra-parqed update DETR                    # Full pipeline: normalize → 1m → 1h → 1d
xetra-parqed update DETR --dry-run          # Preview what would be processed (no execution)
xetra-parqed update DETR --verbose          # Show detailed progress and statistics
xetra-parqed update DETR --normalize-only   # Phase 2a only (stop before aggregation)

# Advanced flags for specific control
xetra-parqed update DETR --intervals 1m,1h  # Only aggregate to specific intervals
xetra-parqed update DETR --force            # Reprocess even if up-to-date
```

**Recommended Workflow**: Use `update` command with `--dry-run` or `--verbose` for visibility. Manual commands removed to keep interface simple.

**DuckDB Query Benefits**:
```sql
-- Before: Scan 1.5 GB to find one ISIN's trades
SELECT * FROM read_parquet('data/de/xetra/trades/venue=DETR/**/trades.parquet')
WHERE isin = 'DE0005190003' AND date >= '2025-12-01';

-- After: Scan 100 KB (ISIN partition pruning)
SELECT * FROM read_parquet('data/de/xetra/trades_by_isin/venue=DETR/isin=DE0005190003/year=2025/month=12/trades.parquet')
WHERE trade_time >= '2025-12-01';
```

**Success Criteria for Phase 2a**:
- ✅ Re-partitioned data preserves all raw columns (no data loss)
- ✅ Row counts match between source (venue/day) and target (ISIN/month)
- ✅ DuckDB queries 10-50x faster for single-ISIN queries
- ✅ File sizes per ISIN reasonable (~50-500KB/month)
- ✅ CLI command idempotent (can re-run without issues)

### Phase 2b: OHLCV Aggregation (Follows Phase 2a)

**Prerequisite**: Phase 2a normalized trades available in `trades_by_isin/`

**Goal**: Aggregate ISIN-partitioned raw trades to 1m/1h/1d OHLCV bars.

## Single Source of Truth Architecture

Implement aggregation where:
1. Fetch and store **only raw data** from APIs (raw trades for Xetra, 1m for Yahoo)
2. Aggregate locally using pandas to produce derived intervals (1h, 1d from 1m/raw)
3. Create reusable `OHLCVAggregator` service that works for both data sources

### Design Decisions (Phase 2b: Aggregation)

#### 1. Storage Structure

**Decision**: Use `stocks_<interval>` naming to match Yahoo Finance convention.

**Phase 2b Aggregated OHLCV Storage**:
```
# ISIN-partitioned OHLCV (monthly files):
data/de/xetra/stocks_1m/ticker=DE0005190003/year=2025/month=12/data.parquet
data/de/xetra/stocks_1h/ticker=DE0005190003/year=2025/month=12/data.parquet
data/de/xetra/stocks_1d/ticker=DE0005190003/year=2025/month=12/data.parquet

# Source data for aggregation (Phase 2a normalized trades):
data/de/xetra/trades_by_isin/venue=DETR/isin=DE0005190003/year=2025/month=12/trades.parquet
```

**Note on Partition Naming**:
- **Raw trades**: `venue=DETR` (venue-first, all ISINs mixed)
- **Normalized trades**: `venue=DETR/isin=DE0005190003` (ISIN-first per venue, still raw data)
- **OHLCV**: `ticker=DE0005190003` (matches Yahoo convention, values are ISINs or ticker symbols)

**Rationale**:
- Consistency with existing Yahoo Finance storage (`data/us/yahoo/stocks_1d/...`)
- Simplifies cross-source queries (same path pattern)
- Clear separation: `trades_by_isin/` for raw, `stocks_<interval>/` for OHLCV
- Future DuckDB analytics can query both sources uniformly
- Monthly partitions balance file count vs. update granularity

**Alternative Considered**: `ohlcv_<interval>` naming to make aggregation explicit. Rejected for consistency with Yahoo naming.

#### 2. Aggregation Source Data

**Decision**: Aggregate from Phase 2a normalized trades (`trades_by_isin/`), not from venue/day landing zone.

**Rationale**:
- **Simpler logic**: Read one ISIN at a time (already partitioned)
- **Better performance**: Skip ISIN filtering step (partition pruning)
- **Incremental processing**: Process new months as normalized data arrives
- **Consistent inputs**: Normalized trades have uniform schema

**Data Flow**:
```
Raw landing → Phase 2a normalize → Phase 2b aggregate → OHLCV
  (venue/day)     (ISIN/month raw)    (ISIN/month bars)
```

#### 2a. Trading Hours Filtering

**Decision**: Filter to continuous trading hours (09:00-17:30 CET/CEST), exclude pre-market and post-trade filings.

**Real Data Analysis** (Xetra DETR, Feb 13, 2026):
- **Total trades**: 535,733
- **Continuous trading** (09:00-17:30 CET): 530,533 trades (99.0%)
- **Pre-market** (08:00-08:59 CET): 655 trades (0.12%)
- **Post-trade filings** (18:00-21:59 CET): 802 trades (0.15%)
- **Closing auction spike**: 17:17 CET (3,436 trades = 0.64%)

**Trade Distribution by Hour (CET)**:

| CET Hour | Trades | % | Notes |
|----------|--------|---|-------|
| 08:00-08:59 | 655 | 0.12% | Pre-market |
| 09:00-16:59 | 527,097 | 98.39% | Continuous trading |
| 17:00-17:30 | 3,743 | 0.70% | Closing auction + settlement |
| 18:00-21:59 | 802 | 0.15% | Late trade reporting |

**Rationale for 09:00-17:30 CET filter**:
- **Official Xetra hours**: 09:00-17:30 CET (continuous trading + auctions)
- **Captures 99%+ of volume**: Pre-market and post-trade filing are negligible
- **Clean data**: Pre-market trades may be test data or corrections
- **Consistent with exchange definition**: Matches Xetra's official trading session

**Implementation Note**: Timestamps stored in **UTC**, must convert filter boundaries to UTC (not individual timestamps).

**DST Handling**:
- **CET (winter)**: UTC+1 → 09:00 CET = 08:00 UTC, 17:30 CET = 16:30 UTC
- **CEST (summer)**: UTC+2 → 09:00 CEST = 07:00 UTC, 17:30 CEST = 15:30 UTC
- **2026 transitions**: CET→CEST on March 29, CEST→CET on October 25
- **Tool advantage**: `pytz` handles DST automatically, SQL requires manual date logic

**Alternative Considered**: Include all trades (00:00-23:59 UTC). **Rejected**: Pre-market and post-trade filings represent <0.3% of volume and may contain corrections/test data that shouldn't affect OHLCV bars.

#### 2b. Efficient Timestamp Filtering

**Decision**: Convert filter boundaries (2 timestamps) to UTC, not all trade timestamps (480K+ per ISIN).

**Performance Impact**:

| Approach | Conversions per ISIN | Cost |
|----------|----------------------|------|
| **Convert all timestamps** | 2,700-480,000 | High CPU overhead |
| **Convert boundaries** (✅ chosen) | 2 | Negligible overhead |

**Implementation Pattern**:

```python
# EFFICIENT: Convert boundaries once (Pandas)
berlin = pytz.timezone('Europe/Berlin')
start_local = berlin.localize(datetime.combine(date, time(9, 0)))  # Handles DST
end_local = berlin.localize(datetime.combine(date, time(17, 30)))
start_utc = start_local.astimezone(pytz.UTC)
end_utc = end_local.astimezone(pytz.UTC)

# Filter on raw UTC timestamps (no per-row conversion)
df = df[(df['Time'] >= start_utc) & (df['Time'] <= end_utc)]
```

```sql
-- EFFICIENT: Compute UTC bounds once (DuckDB - requires manual DST)
-- Feb 13, 2026 is CET (UTC+1): 09:00 CET = 08:00 UTC
WHERE EXTRACT(HOUR FROM trade_time) BETWEEN 8 AND 16
   OR (EXTRACT(HOUR FROM trade_time) = 16 AND EXTRACT(MINUTE FROM trade_time) <= 30)
```

**Rationale**: Timestamps are stored in UTC (timezone-naive). Converting millions of timestamps to CET just to filter is wasteful when we can convert the constant filter boundaries instead.

#### 3. Pipeline Orchestration

**Decision**: Single intelligent `update` command with debugging flags for visibility.

```bash
# Primary interface: Intelligent update with auto-detection
xetra-parqed update DETR                      # Full pipeline (normalize → 1m → 1h → 1d)
xetra-parqed update DETR --dry-run            # Preview execution plan, no changes
xetra-parqed update DETR --verbose            # Detailed progress and statistics

# Memory/performance control
xetra-parqed update DETR --workers 4          # Parallel aggregation (4 workers, ~500MB)
xetra-parqed update DETR --workers 1          # Sequential (low memory, ~200MB, default)

# Control flags for specific scenarios
xetra-parqed update DETR --normalize-only     # Stop after Phase 2a
xetra-parqed update DETR --intervals 1m,1h    # Aggregate only specific intervals
xetra-parqed update DETR --no-cascade         # Disable automatic 1h/1d updates
xetra-parqed update DETR --force              # Reprocess even if up-to-date
```

**Rationale**:
- **Simplicity**: Single command interface, no manual date/month management
- **Visibility**: `--dry-run` shows execution plan, `--verbose` shows detailed progress
- **Efficiency**: Auto-detection skips up-to-date data automatically
- **Memory control**: `--workers` lets users choose memory/speed trade-off (1=low memory, 4=balanced, 8=fast)
- **Debugging**: Flags provide visibility without requiring separate manual commands
- **Resource control**: `--normalize-only` and `--intervals` control pipeline stages

**User Experience Focus**: User requested debugging flags over manual CLI commands - prefer detailed summaries (--verbose) rather than low-level manual control.

**See Section 3a below for detailed auto-detection logic.**

**Alternatives Considered**:
- **Separate commands** (normalize-trades, aggregate-ohlcv): Rejected - too granular, requires manual date/month management
- **On-demand (lazy)**: Aggregate during reads. Rejected: unpredictable latency, cache invalidation complexity
- **Eager (automatic)**: Aggregate after every fetch. Rejected: couples fetching and aggregation, harder to debug
- **Always reprocess**: No staleness checks. Rejected: wastes CPU, slow for large datasets

#### 3a. Intelligent Update System (Auto-Detection)

**Decision**: CLI commands auto-detect what needs processing, with optional user control over pipeline stages.

**Problem**: Users shouldn't need to manually specify date ranges or figure out which aggregations are stale. The system should:
1. Detect which months/days have new raw data that hasn't been normalized
2. Detect which normalized data hasn't been aggregated to OHLCV
3. Detect when higher-interval aggregations (1h, 1d) need updates due to new 1m data
4. Allow users to control whether to run normalization-only or full pipeline

**Solution: Smart Update Command**

```bash
# Auto-detect everything that needs processing and run full pipeline
xetra-parqed update DETR

# Preview what would be processed without executing (debugging)
xetra-parqed update DETR --dry-run

# Show detailed progress during execution (debugging)
xetra-parqed update DETR --verbose

# Control pipeline stages
xetra-parqed update DETR --normalize-only   # Stop before aggregation
xetra-parqed update DETR --intervals 1m,1h  # Only specific intervals
xetra-parqed update DETR --no-cascade       # Disable 1h/1d auto-updates
xetra-parqed update DETR --force            # Reprocess even if up-to-date
```

**Auto-Detection Logic**:

1. **Phase 2a Detection (Normalization)**:
   ```python
   # Find months with raw venue/day data but missing/incomplete ISIN/month data
   
   for month in raw_trade_months:
       raw_days = get_days_in_month(f"trades/venue={venue}/year={year}/month={month}")
       normalized_file = f"trades_by_isin/venue={venue}/isin=*/year={year}/month={month}/trades.parquet"
       
       if not normalized_file.exists():
           # Full month needs normalization
           needs_normalization.append((year, month))
       else:
           # Check if all days are present in normalized data
           normalized_row_count = count_rows(normalized_file)
           raw_row_count = sum(count_rows(day) for day in raw_days)
           
           if abs(normalized_row_count - raw_row_count) > threshold:
               # Counts don't match, re-normalize
               needs_normalization.append((year, month))
   ```

2. **Phase 2b Detection (OHLCV Aggregation)**:
   ```python
   # For each interval (1m, 1h, 1d), check if normalized data is newer than OHLCV
   
   for interval in ["1m", "1h", "1d"]:
       for (year, month) in months_with_normalized_data:
           normalized_mtime = get_mtime(f"trades_by_isin/.../year={year}/month={month}/")
           ohlcv_mtime = get_mtime(f"stocks_{interval}/.../year={year}/month={month}/")
           
           if ohlcv_mtime is None or normalized_mtime > ohlcv_mtime:
               # Normalized data is newer, need to aggregate
               needs_aggregation[interval].append((year, month))
   ```

3. **Cascade Detection (Higher Intervals)**:
   ```python
   # When 1m OHLCV is updated, check if 1h/1d need updates
   
   if "1m" in updated_intervals:
       for month in updated_1m_months:
           # 1h aggregation reads from 1m bars, check if stale
           if needs_1h_update(month):
               cascade_aggregation["1h"].append(month)
           
           # 1d aggregation also reads from 1m, check if stale
           if needs_1d_update(month):
               cascade_aggregation["1d"].append(month)
   ```

**User Control Flags**:

| Flag | Behavior |
|------|----------|
| (none) | Full pipeline: normalize → 1m → 1h → 1d (auto-cascade, sequential) |
| `--dry-run` | Preview execution plan without making changes (debugging) |
| `--verbose` | Show detailed progress, row counts, timing during execution (debugging) |
| `--workers N` | Parallel aggregation with N workers (1=sequential/200MB, 4=balanced/500MB, 8=fast/1GB) |
| `--normalize-only` | Stop after Phase 2a (no OHLCV aggregation) |
| `--intervals 1m` | Normalize + 1m only (no 1h/1d cascade) |
| `--intervals 1h,1d` | Normalize + 1m → 1h → 1d (skip intermediate if not in list) |
| `--no-cascade` | Disable automatic 1h/1d updates (explicit intervals only) |
| `--force` | Reprocess even if timestamps indicate up-to-date |

**Rationale**:
- **Usability**: "Just run `update DETR`" should Do The Right Thing™
- **Efficiency**: Don't reprocess data that's already up-to-date
- **Flexibility**: Power users can control pipeline stages for debugging/testing
- **Visibility**: Dry-run mode shows exactly what will happen
- **Incremental**: Works naturally with daemon mode (process new data as it arrives)

**Implementation Considerations**:
- **Atomic updates**: Use staging directories + atomic rename to avoid partial writes
- **Parallel processing**: ISINs can be normalized/aggregated independently (future optimization)
- **Progress tracking**: Log which months/ISINs are being processed
- **Error handling**: If one ISIN fails, continue with others (collect errors, report at end)
- **Idempotence**: Re-running `update` after partial failure should resume cleanly

**Output Example (--dry-run)**:
```
$ xetra-parqed update DETR --dry-run

🔍 Scanning for updates...

Phase 2a: Normalization (venue → ISIN partitioning)
  ✓ 2025-11: Already normalized (500 ISINs, 12.3M trades)
  ⚠ 2025-12: Needs normalization (48 days collected, 0 ISINs normalized)
  ⚠ 2026-01: Needs normalization (31 days collected, 0 ISINs normalized)
  → Would normalize: 2 months, ~25M trades

Phase 2b: OHLCV Aggregation
  Interval 1m:
    ⚠ 2025-12: source newer than OHLCV (would aggregate 500 ISINs)
    ⚠ 2026-01: OHLCV missing (would aggregate 500 ISINs)
  
  Interval 1h (cascade from 1m):
    ⚠ 2025-12: 1m data updated (would aggregate 500 ISINs)
    ⚠ 2026-01: 1m data updated (would aggregate 500 ISINs)
  
  Interval 1d (cascade from 1m):
    ⚠ 2025-12: 1m data updated (would aggregate 500 ISINs)
    ⚠ 2026-01: 1m data updated (would aggregate 500 ISINs)

📊 Summary:
  - Normalize: 2 months
  - Aggregate 1m: 1,000 ISIN-months
  - Aggregate 1h: 1,000 ISIN-months (cascade)
  - Aggregate 1d: 1,000 ISIN-months (cascade)
  - Estimated time: ~5 minutes

Run without --dry-run to execute.
```

**Output Example (--verbose)**:
```
$ xetra-parqed update DETR --verbose

🔍 Scanning for updates...
  Found 2 months needing normalization
  Found 3 intervals needing aggregation

📦 Phase 2a: Normalizing 2025-12 (48 days, 12.5M trades)
  Loading venue/day files... 48 files found
  Grouping by ISIN... 523 unique ISINs detected
  [1/523] DE0005190003 (BMW): 45,234 trades → trades_by_isin/ ✓
  [2/523] DE0008469008 (DAI): 38,912 trades → trades_by_isin/ ✓
  [...]
  [523/523] DE0008467416: 1,234 trades → trades_by_isin/ ✓
  ✅ Normalized 2025-12: 523 ISINs, 12,500,000 trades (45.2s)

📊 Phase 2b: Aggregating 2025-12 to 1m bars
  [1/523] DE0005190003: 45,234 trades → 2,145 bars ✓
  [2/523] DE0008469008: 38,912 trades → 1,892 bars ✓
  [...]
  [523/523] DE0008467416: 1,234 trades → 78 bars ✓
  ✅ Aggregated 2025-12 to 1m: 523 ISINs, 1,123,456 bars (23.1s)

📊 Phase 2b: Aggregating 2025-12 to 1h bars (cascade)
  [1/523] DE0005190003: 2,145 bars → 48 bars ✓
  [...]
  ✅ Aggregated 2025-12 to 1h: 523 ISINs, 25,104 bars (8.3s)

📊 Phase 2b: Aggregating 2025-12 to 1d bars (cascade)
  [1/523] DE0005190003: 2,145 bars → 1 bar ✓
  [...]
  ✅ Aggregated 2025-12 to 1d: 523 ISINs, 523 bars (2.1s)

✅ Update complete!
  - Normalized: 1 month (12.5M trades)
  - Aggregated: 3 intervals (523 ISINs)
  - Total time: 78.7s
```

**Phase 2.5 Evolution**: Add `--auto-update` flag to `fetch-trades` daemon to automatically run normalization + aggregation after fetching.

#### 4. Ticker Identification (Hybrid Approach)

**Decision**: Store by ISIN, query by ticker via DuckDB joins.

**Storage**: ISIN as partition key (stable, authoritative)
```
data/de/xetra/stocks_1d/ticker=DE0005190003/year=2025/month=12/data.parquet
# Note: partition directory named "ticker=" for consistency, but stores ISIN value
```

**Query Layer**: Optional ISIN→Ticker mapping for user-friendly queries
```
# Reference table (manual curated or Deutsche Börse CSV):
data/reference/xetra_isin_ticker_map.parquet
  Columns: isin, ticker, name, currency

# DuckDB query with on-the-fly join:
SELECT ohlcv.*, map.ticker, map.name
FROM read_parquet('data/de/xetra/stocks_1d/ticker=*/...', hive_partitioning=1) ohlcv
JOIN read_parquet('data/reference/xetra_isin_ticker_map.parquet') map
  ON ohlcv.ticker = map.isin
WHERE map.ticker IN ('BMW', 'DAI', 'DB1');
```

**Rationale**:
- **Storage stability**: ISIN never changes, ticker symbols can (rare but possible)
- **No blocking dependencies**: Aggregation works without ticker mapping
- **Progressive enhancement**: Start with top 40 DAX tickers, expand later
- **Query flexibility**: DuckDB handles joins efficiently (zero-copy, partition pruning)
- **Manual override**: Users can add custom ISIN→Ticker mappings

**Implementation Phases**:
- **Phase 2a/2b**: Store by ISIN only, document ISIN in query results
- **Phase 2.5**: Add curated ticker reference table (top 40 DAX stocks)
- **Phase 3**: Optional Deutsche Börse CSV scraper for full ~4,280 instruments

**Ticker Mapping Options**:

| Approach | Coverage | Maintenance | Complexity |
|----------|----------|-------------|------------|
| **Manual curated** (Phase 2.5) | Top 40 DAX | Low (static list) | Simple CSV |
| **Scrape Deutsche Börse** (Phase 3) | All ~4,280 | Medium (daily sync) | Web scraper |
| **Query-time only** (Phase 2) | N/A (show ISIN) | None | None |

**Recommendation for Phase 2**: Start with ISIN-only storage, add optional ticker mapping in Phase 2.5.

#### 5. Corporate Actions

**Decision**: Store unadjusted prices in OHLCV aggregates. Adjustment handled downstream in normalized analytics layer.

**Rationale**:
- **Phase 2 Goal**: Prove aggregation pipeline works end-to-end with unadjusted data
- **Data availability**: Xetra raw trades don't include split/dividend metadata
- **External dependency**: Requires additional data source for corporate actions
- **⚡ Key Insight**: Log returns (normalized analytics layer) **eliminate the need for split adjustments** in most analytical workflows

**Why Log Returns Solve the Split Problem**:

```python
# Example: 2-for-1 stock split on Day 2
# Unadjusted prices: [100, 50, 51, 52]
# After split: price halves but shares double (value preserved)

# Traditional approach: Adjust historical prices backward
# Adjusted prices: [50, 50, 51, 52]  # ← Requires tracking splits

# Normalized analytics approach: Use log returns directly
log_returns = ln([100, 50, 51, 52] / [NaN, 100, 50, 51])
            = [NaN, -0.693, 0.0198, 0.0196]

# The split creates a -69.3% "return" that is clearly an outlier
# Detection: if |log_return| > 0.5 (>50%), flag as corporate action
# Then: exclude or interpolate that day's return

# Result: Returns-based analysis works without adjustment metadata!
```

**Corporate Action Detection via Returns**:

1. **Splits**: Overnight price changes >30% (|log_return| > 0.26) → likely split/reverse split
2. **Dividends**: Moderate drops (~1-5%) on ex-dividend dates → can be ignored or flagged
3. **Mergers**: Extreme price changes + volume spikes → flag for manual review

**Workflow**:

```python
# In OHLCVAggregator: Store raw unadjusted prices
ohlcv = aggregate_trades_to_ohlcv(raw_trades)  # No adjustment
save_to_storage(ohlcv, dataset='stocks_1m')    # Raw data preserved

# In NormalizedAnalyticsService: Detect and handle corporate actions
log_returns = compute_log_returns(ohlcv['close'])
corporate_action_days = detect_outliers(log_returns, threshold=0.26)  # >30% overnight
log_returns_cleaned = interpolate_or_exclude(log_returns, corporate_action_days)

# Analyst uses cleaned returns, never sees the split
volatility = log_returns_cleaned.std()
```

**Trade-offs**:

| Approach | Pros | Cons |
|----------|------|------|
| **Traditional (adjust prices)** | Continuous price series, simple charting | Requires split metadata, backfill historical data, cannot reconstruct unadjusted |
| **Normalized (detect via returns)** | No metadata needed, preserves raw data, automated detection | Charts show discontinuities, requires outlier handling |

**Decision for Phase 2**: Use normalized analytics approach
- Store unadjusted prices in OHLCV (simple, no external dependencies)
- Detect corporate actions via return outliers in normalized layer
- Optional Phase 3: Integrate split metadata for improved accuracy (if user needs adjusted price charts)

**Consequence**: Most analytical use cases (volatility, correlation, risk metrics) work immediately without split tracking infrastructure
- **Complexity**: Adjustment logic is intricate, needs separate ADR and implementation phase

**Phase 3 Roadmap**:
1. Implement `CorporateActionService` to fetch split/dividend data
2. Add `AdjustmentEngine` to apply adjustments to OHLCV data
3. Store both adjusted and unadjusted OHLCV (like Yahoo Finance)
4. Document adjustment methodology in separate ADR

## Implementation Architecture

### UpdateOrchestrator Service

```python
class UpdateOrchestrator:
    """
    Intelligent pipeline orchestrator that auto-detects what needs processing.
    
    Design Principles:
    - Auto-detection: Scans filesystem to determine stale data
    - Pipeline control: User specifies stages (normalize-only, specific intervals)
    - Cascade logic: Automatically updates higher intervals when lower intervals change
    - Dry-run support: Preview execution plan without running
    - Idempotent: Safe to re-run, skips up-to-date data
    """
    
    def __init__(
        self,
        config: ConfigService,
        normalizer: TradeNormalizer,
        aggregator: OHLCVAggregator,
        storage: PartitionedStorageBackend
    ):
        self.config = config
        self.normalizer = normalizer
        self.aggregator = aggregator
        self.storage = storage
    
    def plan_updates(
        self,
        venue: str,
        intervals: list[str] = ["1m", "1h", "1d"],
        normalize_only: bool = False,
        no_cascade: bool = False,
        force: bool = False,
        dry_run: bool = False
    ) -> dict:
        """
        Detect what needs processing and build execution plan.
        
        Returns:
            {
                "normalize": [(year, month), ...],
                "aggregate": {
                    "1m": [(year, month), ...],
                    "1h": [(year, month), ...],
                    "1d": [(year, month), ...]
                }
            }
        """
        plan = {"normalize": [], "aggregate": {interval: [] for interval in intervals}}
        
        # Phase 2a detection: Find months needing normalization
        raw_months = self._scan_raw_trades(venue)
        for year, month in raw_months:
            if force or self._needs_normalization(venue, year, month):
                plan["normalize"].append((year, month))
        
        if normalize_only:
            return plan  # Skip aggregation detection
        
        # Phase 2b detection: Find months needing aggregation
        normalized_months = self._scan_normalized_trades(venue)
        for interval in intervals:
            for year, month in normalized_months:
                if force or self._needs_aggregation(venue, interval, year, month):
                    plan["aggregate"][interval].append((year, month))
        
        # Cascade detection: If 1m updated, check 1h/1d
        if not no_cascade and "1m" in plan["aggregate"]:
            for interval in ["1h", "1d"]:
                if interval in intervals:
                    for year, month in plan["aggregate"]["1m"]:
                        if (year, month) not in plan["aggregate"][interval]:
                            plan["aggregate"][interval].append((year, month))
        
        return plan
    
    def execute_updates(
        self,
        venue: str,
        intervals: list[str] = ["1m", "1h", "1d"],
        normalize_only: bool = False,
        no_cascade: bool = False,
        force: bool = False
    ) -> dict:
        """Execute planned updates and return summary."""
        plan = self.plan_updates(venue, intervals, normalize_only, no_cascade, force)
        
        results = {"normalized": 0, "aggregated": {}}
        
        # Execute Phase 2a: Normalization
        for year, month in plan["normalize"]:
            self._normalize_month(venue, year, month)
            results["normalized"] += 1
        
        if normalize_only:
            return results
        
        # Execute Phase 2b: Aggregation
        for interval in intervals:
            results["aggregated"][interval] = 0
            for year, month in plan["aggregate"][interval]:
                self._aggregate_month(venue, interval, year, month)
                results["aggregated"][interval] += 1
        
        return results
    
    def _needs_normalization(self, venue: str, year: int, month: int) -> bool:
        """Check if month needs normalization by comparing row counts."""
        raw_count = self._count_raw_trades(venue, year, month)
        normalized_count = self._count_normalized_trades(venue, year, month)
        
        # Allow 0.1% tolerance for rounding
        return abs(raw_count - normalized_count) > (raw_count * 0.001)
    
    def _needs_aggregation(self, venue: str, interval: str, year: int, month: int) -> bool:
        """Check if month needs aggregation by comparing timestamps."""
        normalized_mtime = self._get_normalized_mtime(venue, year, month)
        ohlcv_mtime = self._get_ohlcv_mtime(venue, interval, year, month)
        
        if ohlcv_mtime is None:
            return True  # OHLCV doesn't exist
        
        return normalized_mtime > ohlcv_mtime  # Normalized data is newer
```

### TradeNormalizer Service

```python
class TradeNormalizer:
    """
    Phase 2a: Re-partition venue/day trades to ISIN/month layout.
    
    Design Principles:
    - Preserve all columns (no aggregation, schema unchanged)
    - Monthly batching for efficient file sizes
    - Sort by timestamp for deterministic output
    - Validate row counts before/after
    
    **Tool Choice: DuckDB (recommended) or Pandas**
    - DuckDB preferred for production (parallel, memory-efficient, faster)
    - Pandas acceptable for development/testing (simpler debugging)
    """
    
    def normalize_month(
        self,
        venue: str,
        year: int,
        month: int,
        staging_dir: Path | None = None
    ) -> dict:
        """
        Normalize one month of venue/day trades to ISIN/month layout.
        
        Returns:
            {
                "isins_processed": 500,
                "rows_in": 12345678,
                "rows_out": 12345678,
                "duration_sec": 45.2
            }
        """
        # Implementation details...
        pass
```

## Tool Selection Analysis

### Phase 2a: Consolidation/Normalization

**Task**: Re-partition ~21 daily files to ~4,000 ISIN-specific monthly files.

**Real Data Sizing** (Feb 2026, actual Xetra data):
- **One day**: 23MB, 535K trades across 4,168 ISINs
- **One month** (21 trading days): 483MB raw, ~11.25M trades
- **Per-ISIN distribution**:
  - Top 1% (42 ISINs): 5-20MB/month each (20-480K trades)
  - Average (3,700 ISINs): ~115KB/month each (~2,700 trades)
  - Bottom 10% (417 ISINs): <1KB/month each (<100 trades)

#### DuckDB Approach

```python
# Read all daily files, group by ISIN, write partitioned output
import duckdb

def normalize_with_duckdb(venue: str, year: int, month: int):
    con = duckdb.connect()
    
    # Single query to read, group, and write
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet(
                'data/de/xetra/trades/venue={venue}/year={year}/month={month:02d}/day=*/trades.parquet',
                hive_partitioning=1
            )
            ORDER BY isin, Time
        ) TO 'data/de/xetra/trades_by_isin' (
            FORMAT PARQUET,
            PARTITION_BY (venue, isin, year, month),
            OVERWRITE_OR_IGNORE
        )
    """)
```

**DuckDB Pros**:
- ✅ **Parallel execution**: Multi-threaded by default (4-8x faster)
- ✅ **Memory efficient**: Streaming execution, doesn't load all data into RAM
- ✅ **Native partitioning**: Can write Hive-style partitions directly
- ✅ **Simple code**: Single SQL query handles entire pipeline
- ✅ **Partition pruning**: Efficient reading of source files
- ✅ **Zero-copy**: Direct parquet→parquet without intermediate conversions

**DuckDB Cons**:
- ❌ **Less debuggable**: Can't easily inspect intermediate state
- ❌ **SQL verbosity**: More complex for conditional logic
- ❌ **Error handling**: Harder to handle per-ISIN failures gracefully

**Estimated Performance** (483MB, 11.25M trades):
- **DuckDB**: 15-30 seconds (parallel, streaming)
- **Pandas**: 60-120 seconds (single-threaded, memory-bound)

#### Pandas Approach

```python
# Read daily files, group, iterate ISINs, write individually
import pandas as pd

def normalize_with_pandas(venue: str, year: int, month: int):
    # Read all daily files
    daily_files = glob(f"data/de/xetra/trades/venue={venue}/year={year}/month={month:02d}/day=*/trades.parquet")
    df = pd.concat([pd.read_parquet(f) for f in daily_files])
    
    # Group by ISIN
    for isin, group in df.groupby('isin'):
        group = group.sort_values('Time')
        
        # Write per-ISIN file
        output_path = f"data/de/xetra/trades_by_isin/venue={venue}/isin={isin}/year={year}/month={month:02d}/trades.parquet"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        group.to_parquet(output_path, index=False)
```

**Pandas Pros**:
- ✅ **Familiar API**: Easier for Python developers to understand
- ✅ **Debuggable**: Can inspect DataFrames with `.head()`, `.info()`
- ✅ **Error handling**: Easy to try/except per-ISIN and continue
- ✅ **Flexible**: Simple to add custom transformations or filters
- ✅ **Development speed**: Faster to prototype and test

**Pandas Cons**:
- ❌ **Memory intensive**: Loads entire month (1.5GB) into RAM at once
- ❌ **Single-threaded**: No parallel processing by default
- ❌ **Slower**: 2-4x slower than DuckDB for large datasets
- ❌ **Manual partitioning**: Need to construct paths and create directories manually

**Memory Requirements**:
- DuckDB: ~150-200MB (streaming)
- Pandas: ~2GB (483MB raw + overhead + copies during groupby)

#### Recommendation for Phase 2a: **DuckDB**

**Rationale**:
- Consolidation is primarily I/O and grouping (DuckDB's strength)
- 2-4x performance improvement matters for monthly batch jobs
- Memory efficiency enables processing on smaller machines
- Production workload benefits from parallelism

**Fallback**: Use pandas for initial implementation/testing, migrate to DuckDB after proving logic correct.

---

### Phase 2b: OHLCV Aggregation

**Task**: Read ISIN/month file, resample to 1m/1h/1d bars with OHLC calculations.

**Real Data Sizing** (based on actual Xetra data analysis):
- **Average ISIN**: ~115KB/month (~2,700 trades)
- **High-liquidity ISINs** (top 100): 5-20MB/month (~100K-480K trades)
  - Example: DE0007164600 (22,943 trades/day → ~480K trades/month)
- **Low-liquidity ISINs**: <1KB/month (<100 trades)

**After 1m aggregation:**
- Average: ~50K/month (50-100x compression)
- High-liquidity: 100-500K/month

#### Pandas Approach

```python
def aggregate_with_pandas(isin_file: Path, interval: str):
    import pytz
    from datetime import datetime, time
    
    # Read single ISIN's trades
    df = pd.read_parquet(isin_file)
    df['Time'] = pd.to_datetime(df['Time'], utc=True)  # Parse as UTC
    
    # EFFICIENT: Convert filter boundaries once, not all timestamps
    # Determine UTC bounds for trading hours (09:00-17:30 CET/CEST)
    date = df['Time'].iloc[0].date()
    berlin = pytz.timezone('Europe/Berlin')
    
    # Create Berlin-time boundaries (handles DST automatically)
    start_local = berlin.localize(datetime.combine(date, time(9, 0)))
    end_local = berlin.localize(datetime.combine(date, time(17, 30)))
    
    # Convert to UTC for filtering (2 conversions vs 480K)
    start_utc = start_local.astimezone(pytz.UTC)
    end_utc = end_local.astimezone(pytz.UTC)
    
    # Filter on UTC timestamps (no per-row conversion needed)
    df = df[(df['Time'] >= start_utc) & (df['Time'] <= end_utc)]
    
    # Set index for resampling (still in UTC, no conversion needed)
    df = df.set_index('Time')
    
    # Resample to target interval with OHLC
    ohlcv = df.resample(interval).agg({
        'price': ['first', 'max', 'min', 'last'],  # Open, High, Low, Close
        'volume': 'sum',
        'trades': 'sum'
    })
    
    # Flatten column names
    ohlcv.columns = ['open', 'high', 'low', 'close', 'volume', 'trades']
    
    # Calculate VWAP
    df['pv'] = df['price'] * df['volume']
    vwap = df.resample(interval)['pv'].sum() / df.resample(interval)['volume'].sum()
    ohlcv['vwap'] = vwap
    
    # Filter zero-volume bars
    ohlcv = ohlcv[ohlcv['volume'] > 0]
    
    return ohlcv
```

**Pandas Pros**:
- ✅ **`.resample()` is perfect**: Literally designed for time-series OHLC aggregation
- ✅ **Timezone handling**: `pytz` handles CET/CEST DST transitions automatically
- ✅ **Efficient filtering**: Convert 2 boundary values, not 480K timestamps
- ✅ **Natural syntax**: `df.resample('1m').agg({'price': 'ohlc'})` is intuitive
- ✅ **Custom aggregations**: Easy to add VWAP, weighted averages, percentiles
- ✅ **Trading hours**: DST-aware filtering with automatic offset calculation
- ✅ **Small memory footprint**: Each ISIN file is 115KB avg, 20MB max
- ✅ **Mature ecosystem**: Extensive documentation and examples for financial data

**Pandas Cons**:
- ❌ **Single-threaded**: Can't parallelize across ISINs natively (but we use multiprocessing.Pool)
- ❌ **DST dependency**: Need `pytz` for reliable DST handling (but already in dependencies)
- ❌ **Iterative**: Need loop to process multiple ISINs

**Estimated Performance** (per ISIN):
- **Average ISIN** (115KB, 2.7K trades): 50-100ms
- **High-liquidity** (20MB, 480K trades): 2-5 seconds
- **Low-liquidity** (<1KB, <100 trades): 10-20ms
- **Total for 4,168 ISINs**: 
  - Sequential: ~3-5 minutes (dominated by top 42 ISINs)
  - Parallel (4 workers): ~1-2 minutes
  - Parallel (8 workers): ~45-90 seconds

**Memory Requirements (Parallel Mode):**

| Mode | Workers | Peak Memory | Notes |
|------|---------|-------------|-------|
| Sequential | 1 | ~60MB | 20MB largest ISIN × 3x pandas overhead |
| Parallel | 4 | ~500MB | 4 × (50MB Python + 60MB data peak) |
| Parallel | 8 | ~1GB | 8 × (50MB Python + 60MB data peak) |

**Memory Profile Details:**
- Each worker loads **one ISIN at a time** (not all ISINs)
- Worst case: All workers hit high-liquidity ISINs simultaneously
- Pandas overhead: ~2-3x file size (DataFrame + index + copies during resample)
- Python interpreter per worker: ~50MB base
- **Recommended**: 4 workers for <500MB memory profile, 8 workers if >2GB available

#### DuckDB Approach

```python
def aggregate_with_duckdb(isin_file: Path, interval: str, date: datetime.date):
    con = duckdb.connect()
    
    # Convert interval to DuckDB syntax
    interval_map = {'1m': '1 minute', '1h': '1 hour', '1d': '1 day'}
    
    # EFFICIENT: Determine UTC bounds for trading hours once (manual DST handling)
    # CET (UTC+1): Oct-Mar, CEST (UTC+2): Mar-Oct
    # 2026 DST: CEST starts Mar 29, ends Oct 25
    is_cest = (date >= datetime.date(2026, 3, 29) and date < datetime.date(2026, 10, 25))
    
    if is_cest:
        # CEST: 09:00-17:30 CEST = 07:00-15:30 UTC
        start_hour, end_hour = 7, 15
        end_minute = 30
    else:
        # CET: 09:00-17:30 CET = 08:00-16:30 UTC
        start_hour, end_hour = 8, 16
        end_minute = 30
    
    query = f"""
        SELECT
            time_bucket(INTERVAL '{interval_map[interval]}', trade_time) as time,
            FIRST(price ORDER BY trade_time) as open,
            MAX(price) as high,
            MIN(price) as low,
            LAST(price ORDER BY trade_time) as close,
            SUM(volume) as volume,
            COUNT(*) as trades,
            SUM(price * volume) / NULLIF(SUM(volume), 0) as vwap
        FROM read_parquet('{isin_file}')
        WHERE volume > 0
          AND (
              EXTRACT(HOUR FROM trade_time) > {start_hour}
              OR (EXTRACT(HOUR FROM trade_time) = {start_hour} AND EXTRACT(MINUTE FROM trade_time) >= 0)
          )
          AND (
              EXTRACT(HOUR FROM trade_time) < {end_hour}
              OR (EXTRACT(HOUR FROM trade_time) = {end_hour} AND EXTRACT(MINUTE FROM trade_time) <= {end_minute})
          )
        GROUP BY time_bucket(INTERVAL '{interval_map[interval]}', trade_time)
        ORDER BY time
    """
    
    return con.execute(query).df()
```

**DuckDB Pros**:
- ✅ **Parallel ISIN processing**: Can process multiple ISINs in parallel with single query
- ✅ **Window functions**: FIRST_VALUE, LAST_VALUE, aggregations in single pass
- ✅ **Fast for large ISINs**: Better than pandas for highly-liquid stocks (20MB files)
- ✅ **Batch processing**: Can aggregate all ISINs in one query with UNION ALL
- ✅ **Efficient filtering**: Boundary-based filtering (same as pandas)

**DuckDB Cons**:
- ❌ **Manual DST handling**: Must hardcode CET/CEST transition dates (Mar 29, Oct 25)
- ❌ **DST maintenance burden**: Need to update dates yearly or implement calendar logic
- ❌ **Time-bucket complexity**: Requires `time_bucket()` function, less intuitive than `.resample()`
- ❌ **Less intuitive**: SQL window functions less natural for OHLC than pandas `.resample()`
- ❌ **OHLC verbosity**: `FIRST(... ORDER BY)`, `LAST(... ORDER BY)` more verbose
- ❌ **Debugging difficulty**: Opaque SQL query harder to debug than pandas step-by-step

**DST Handling Comparison**:
```python
# Pandas: Automatic DST handling
berlin = pytz.timezone('Europe/Berlin')
start_local = berlin.localize(datetime.combine(date, time(9, 0)))  # Handles DST automatically
start_utc = start_local.astimezone(pytz.UTC)

# DuckDB: Manual DST logic
is_cest = (date >= datetime.date(2026, 3, 29) and date < datetime.date(2026, 10, 25))
start_hour = 7 if is_cest else 8  # Hardcoded dates, must update annually
```

#### Recommendation for Phase 2b: **Pandas**

**Rationale**:
1. **`.resample()` is purpose-built** for time-series OHLC aggregation
2. **Automatic DST handling** via `pytz` - no hardcoded transition dates
3. **Efficient filtering** - convert 2 boundaries vs 480K timestamps (same efficiency as DuckDB)
4. **Simpler VWAP** - natural Python expressions vs SQL CTEs
5. **Better debugging** - inspect DataFrames step-by-step with `.head()`, `.info()`
6. **Code readability** - financial analysts can understand and maintain
7. **Mature ecosystem** - extensive pandas + financial data documentation

**Key Advantages Over DuckDB for This Use Case:**
- **DST**: Pandas auto-detects CET/CEST, DuckDB requires manual date updates
- **Syntax**: `df.resample('1m').agg({'price': 'ohlc'})` vs verbose SQL window functions
- **Debugging**: Step-by-step inspection vs opaque SQL query
- **Maintenance**: No annual DST date updates needed

**Important Note on Performance:**
- Real data shows **highly skewed distribution**: 42 ISINs (1%) account for ~60% of runtime
- High-liquidity ISINs (20MB files, 480K trades) take 2-5s each
- Average ISINs (115KB files, 2.7K trades) take 50-100ms each
- **Parallelization recommended** for production: Process ISINs concurrently with `multiprocessing.Pool`

**When to use DuckDB**: 
- If aggregating ALL ISINs in a single batch query (Phase 3 optimization)
- Batch processing with `UNION ALL` across ISINs for cross-sectional analysis
- When DST handling is externalized (e.g., pre-filtered data)

---

### Hybrid Approach (Recommended)

**Best of both worlds**:

```python
class UpdateOrchestrator:
    def __init__(self):
        self.normalizer = DuckDBNormalizer()    # Phase 2a: DuckDB
        self.aggregator = PandasAggregator()      # Phase 2b: Pandas
    
    def execute_updates(self, venue: str, max_workers: int = 1):
        """
        Execute Phase 2a + 2b updates.
        
        Args:
            venue: Trading venue (e.g., 'DETR')
            max_workers: Parallelism for Phase 2b (1=sequential, 4=balanced, 8=fast)
                         Controls memory/speed trade-off:
                         - 1 worker:  ~200MB peak, 3-5min (daemon-friendly)
                         - 4 workers: ~500MB peak, 1-2min (balanced)
                         - 8 workers: ~1GB peak, 45-90s (interactive)
        """
        # Phase 2a: Consolidation with DuckDB (fast, parallel, memory-efficient)
        self.normalizer.normalize_month(venue, 2025, 12)  # ~20 seconds, 200MB
        
        # Phase 2b: Aggregation with Pandas (per-ISIN, simple, maintainable)
        isin_files = glob("data/de/xetra/trades_by_isin/venue=DETR/isin=*/...")
        
        if max_workers == 1:
            # Sequential: Low memory, longer runtime
            for isin_file in isin_files:
                ohlcv = self.aggregator.aggregate(isin_file, interval="1m")
                self.storage.save(ohlcv, ...)
        else:
            # Parallel: Higher memory, faster runtime
            with multiprocessing.Pool(max_workers) as pool:
                pool.map(self._aggregate_isin, isin_files)
```

**Performance Comparison** (4,168 ISINs, 483MB/month, 11.25M trades):

| Phase | Task | Tool | Time | Memory |
|-------|------|------|------|--------|
| 2a | Consolidation | DuckDB | ~20s | ~200MB |
| 2a | Consolidation | Pandas | ~90s | ~2GB |
| 2b | OHLCV 1m (seq) | Pandas | ~3-5min | ~60MB |
| 2b | OHLCV 1m (4x) | Pandas | ~1-2min | ~500MB |
| 2b | OHLCV 1m (8x) | Pandas | ~45-90s | ~1GB |
| 2b | OHLCV 1m | DuckDB | ~1-2min* | ~100MB |
| 2b | OHLCV 1h | Pandas | ~30s (seq) | ~60MB |
| 2b | OHLCV 1d | Pandas | ~15s (seq) | ~60MB |
| **Total (Hybrid 4x)** | **Both 1m** | **DuckDB + Pandas** | **~1.5-2min** | **~500MB peak** |
| **Total (Hybrid seq)** | **Both 1m** | **DuckDB + Pandas** | **~3.5-5min** | **~200MB peak** |
| **Total (Pandas seq)** | **Both 1m** | **All Pandas** | **~4.5-6min** | **~2GB peak** |

_* DuckDB would need single-query batch mode with UNION ALL; per-ISIN queries have high overhead._
_4x = 4 parallel workers, 8x = 8 parallel workers, seq = sequential_

**Conclusion**: 
- **Phase 2a**: Use DuckDB (4x faster, 90% less memory than pandas, handles 483MB efficiently)
- **Phase 2b**: Use Pandas with **configurable parallelism**
  - **Low memory** (daemon/cron): Sequential mode (~200MB total pipeline)
  - **Interactive** (user-triggered): 4 workers (~500MB total pipeline)
  - **Performance** (powerful machine): 8 workers (~1GB total pipeline)
- **Key Insight**: Real Xetra data shows highly skewed distribution (1% of ISINs = 60% of work)
- **Rationale**: Optimize bulk I/O (DuckDB), parallelize per-ISIN work (Pandas), let user control memory/speed trade-off

**Memory Profile Comparison:**
```
Sequential:  Phase 2a (200MB) → Phase 2b (60MB)   = 200MB peak  ✅ Low memory
Parallel 4x: Phase 2a (200MB) → Phase 2b (500MB)  = 500MB peak  ✅ Balanced  
Parallel 8x: Phase 2a (200MB) → Phase 2b (1GB)    = 1GB peak    ⚠️  High memory
Pandas-only: Phase 2a (2GB)   → Phase 2b (60MB)   = 2GB peak    ❌ Memory intensive
```

### OHLCVAggregator Service

```python
class OHLCVAggregator:
    """
    Reusable aggregation service for converting raw trades to OHLCV bars.
    
    Design Principles:
    - Efficient: Convert filter boundaries (2 values), not all timestamps
    - DST-aware: Automatic CET/CEST handling via pytz
    - Stateless: No internal state, pure transformation
    - Configurable: Trading hours filter configurable per venue
    
    Real Data Context (Xetra):
    - Timestamps stored in UTC (timezone-naive)
    - Trading hours: 09:00-17:30 CET/CEST (99%+ of volume)
    - DST transitions: CET (UTC+1) ↔ CEST (UTC+2) on Mar 29 and Oct 25
    """
    
    def __init__(self, trading_start: time = time(9, 0), trading_end: time = time(17, 30)):
        self.trading_start = trading_start  # Default: 09:00 (Xetra)
        self.trading_end = trading_end      # Default: 17:30 (Xetra)
    
    def aggregate_from_file(
        self,
        file_path: Path,
        target_interval: str,  # "1m", "1h", "1d"
        market_tz: str = "Europe/Berlin"
    ) -> pd.DataFrame:
        """
        Aggregate raw trades from parquet file to OHLCV bars.
        
        Args:
            file_path: Path to ISIN/month parquet file (normalized trades)
            target_interval: Desired output interval ("1m", "1h", "1d")
            market_tz: Market timezone for trading hours (default: Europe/Berlin)
        
        Returns:
            DataFrame with OHLCV columns: open, high, low, close, volume, trades, vwap
            Index: DatetimeIndex in UTC
        
        Implementation Notes:
        - Filters to trading hours (09:00-17:30 local time) by converting boundaries to UTC
        - Handles DST automatically via pytz (CET/CEST transitions)
        - Resamples on UTC timestamps (no per-row timezone conversion)
        - Returns UTC-indexed result (caller can convert if needed)
        """
        import pytz
        
        # 1. Read raw trades (UTC timestamps, columns: trade_time, price, volume)
        df = pd.read_parquet(file_path)
        
        if df.empty:
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'trades', 'vwap'])
        
        # 2. Parse timestamps as UTC
        df['trade_time'] = pd.to_datetime(df['trade_time'], utc=True)
        
        # 3. EFFICIENT: Convert filter boundaries once (not all timestamps)
        #    Determine UTC bounds for trading hours on this date
        date = df['trade_time'].iloc[0].date()
        tz = pytz.timezone(market_tz)
        
        # Localize naive datetime to market timezone (handles DST automatically)
        start_local = tz.localize(datetime.combine(date, self.trading_start))
        end_local = tz.localize(datetime.combine(date, self.trading_end))
        
        # Convert to UTC for filtering (2 conversions vs 480K)
        start_utc = start_local.astimezone(pytz.UTC)
        end_utc = end_local.astimezone(pytz.UTC)
        
        # 4. Filter to trading hours (on UTC timestamps, no per-row conversion)
        df = df[(df['trade_time'] >= start_utc) & (df['trade_time'] <= end_utc)]
        
        if df.empty:
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'trades', 'vwap'])
        
        # 5. Set index for resampling (still in UTC, resample works on any timezone)
        df = df.set_index('trade_time')
        
        # 6. Resample to target interval with OHLC aggregation
        ohlcv = df.resample(target_interval).agg({
            'price': ['first', 'max', 'min', 'last'],  # OHLC
            'volume': 'sum'
        })
        
        # Flatten multi-index columns
        ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
        
        # 7. Calculate VWAP (volume-weighted average price)
        df['pv'] = df['price'] * df['volume']
        vwap = df.resample(target_interval)['pv'].sum() / df.resample(target_interval)['volume'].sum()
        ohlcv['vwap'] = vwap
        
        # 8. Add trade count
        trade_count = df.resample(target_interval).size()
        ohlcv['trades'] = trade_count
        
        # 9. Remove zero-volume bars
        ohlcv = ohlcv[ohlcv['volume'] > 0].copy()

        # Add provenance and metadata columns
        ohlcv['aggregated_at'] = datetime.now(pytz.UTC)
        ohlcv['aggregated_by'] = 'OHLCVAggregator'
        ohlcv['aggregation_version'] = '1.0'

        return ohlcv
```

### DataNormalizer

```python
class DataNormalizer:
    """
    Normalize heterogeneous data sources to common schema.
    
    Handles:
    - Different column names (Price vs price vs Close)
    - Different timestamp formats (Unix, ISO8601, datetime)
    - Different data types (int64 vs float64)
    """
    
    def normalize(self, df: pd.DataFrame, source: str = "xetra") -> pd.DataFrame:
        """
        Normalize to standard schema: index=datetime, columns=[price, volume]
        """
        if source == "xetra":
            return self._normalize_xetra_trades(df)
        elif source == "yahoo":
            return self._normalize_yahoo_minute(df)
        else:
            raise ValueError(f"Unknown source: {source}")
    
    def _normalize_xetra_trades(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Xetra raw trades schema:
        - Time (Unix timestamp)
        - StartPrice (first trade price)
        - EndPrice (last trade price)
        - TradedVolume (total volume)
        """
        normalized = pd.DataFrame({
            'price': df['EndPrice'],  # Use last trade price
            'volume': df['TradedVolume']
        })
        normalized.index = pd.to_datetime(df['Time'], unit='ms')
        return normalized
    
    def _normalize_yahoo_minute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Yahoo Finance 1m data already in OHLCV format.
        Extract close price for tick-level aggregation.
        """
        normalized = pd.DataFrame({
            'price': df['close'],
            'volume': df['volume']
        })
        normalized.index = df.index
        return normalized
```

### CLI Integration

New command in `xetra_cli.py`:

```python
@app.command()
def update(
    venue: str = typer.Argument(..., help="Trading venue (e.g., DETR)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview execution plan without running"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed progress and statistics"),
    workers: int = typer.Option(1, "--workers", "-j", help="Parallel workers (1=seq/200MB, 4=balanced/500MB, 8=fast/1GB)"),
    normalize_only: bool = typer.Option(False, "--normalize-only", help="Stop after normalization (Phase 2a)"),
    intervals: Optional[str] = typer.Option(None, help="Comma-separated intervals (1m,1h,1d)"),
    no_cascade: bool = typer.Option(False, "--no-cascade", help="Disable automatic 1h/1d updates"),
    force: bool = typer.Option(False, "--force", help="Reprocess even if up-to-date"),
):
    """
    Intelligently update Xetra data pipeline (auto-detects what needs processing).
    
    This command scans your data and automatically determines:
    - Which months need normalization (venue/day → ISIN/month)
    - Which months need aggregation to OHLCV (raw trades → 1m/1h/1d bars)
    - Whether higher intervals (1h, 1d) need updates after 1m changes
    
    Examples:
        # Full pipeline with auto-detection (most common)
        xetra-parqed update DETR
        
        # Preview what would be processed (debugging)
        xetra-parqed update DETR --dry-run
        
        # Show detailed progress (debugging)
        xetra-parqed update DETR --verbose
        
        # Parallel aggregation for faster processing (requires more memory)
        xetra-parqed update DETR --workers 4    # Balanced: ~500MB, 1-2 min
        xetra-parqed update DETR --workers 8    # Fast: ~1GB, 45-90 sec
        
        # Only normalize, skip aggregation
        xetra-parqed update DETR --normalize-only
        
        # Control which intervals to aggregate
        xetra-parqed update DETR --intervals 1m,1h
    """
    # Initialize services
    config = ConfigService(working_path=Path.cwd())
    orchestrator = UpdateOrchestrator(config)
    
    # Parse intervals
    interval_list = intervals.split(",") if intervals else ["1m", "1h", "1d"]
    
    # Plan updates
    plan = orchestrator.plan_updates(
        venue=venue,
        intervals=interval_list,
        normalize_only=normalize_only,
        no_cascade=no_cascade,
        force=force,
        dry_run=dry_run
    )
    
    # Display plan
    display_update_plan(plan, dry_run=dry_run)
    
    if dry_run:
        typer.echo("\nRun without --dry-run to execute.")
        return
    
    # Execute updates
    results = orchestrator.execute_updates(
        venue=venue,
        intervals=interval_list,
        normalize_only=normalize_only,
        no_cascade=no_cascade,
        force=force,
        verbose=verbose,
        max_workers=workers
    )
    
    # Display summary
    display_update_summary(results, verbose=verbose)
```

## Data Flow

### Intelligent Update Command (Orchestrator)

```
┌─────────────────────────────────────────────────────────────────┐
│ User invokes: xetra-parqed update DETR                          │
└─────────────────────────────────────────────────────────────────┘
    │
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ UpdateOrchestrator                                              │
│   1. Scan: Compare venue/day files vs ISIN/month files         │
│   2. Detect: Which months need normalization (Phase 2a)         │
│   3. Scan: Compare ISIN/month files vs OHLCV files             │
│   4. Detect: Which months need aggregation per interval         │
│   5. Plan: Build execution graph (normalize → 1m → 1h → 1d)     │
│   6. Execute: Run pipeline stages with progress tracking        │
└─────────────────────────────────────────────────────────────────┘
    │
    ├─────────> Phase 2a: Normalize (if needed)
    │           ↓
    │           data/de/xetra/trades_by_isin/.../trades.parquet
    │
    ├─────────> Phase 2b: Aggregate 1m (if needed or cascade)
    │           ↓
    │           data/de/xetra/stocks_1m/.../data.parquet
    │
    ├─────────> Phase 2b: Aggregate 1h (if cascade enabled)
    │           ↓
    │           data/de/xetra/stocks_1h/.../data.parquet
    │
    └─────────> Phase 2b: Aggregate 1d (if cascade enabled)
                ↓
                data/de/xetra/stocks_1d/.../data.parquet
```

**Key Features**:
- Auto-detection: Scans filesystem to determine what's stale
- Dry-run mode: Preview execution plan without running
- Memory control: `--workers N` for configurable memory/speed trade-off (1/4/8 workers)
- Selective execution: `--normalize-only` or `--intervals 1m` to control stages
- Cascade control: `--no-cascade` to disable automatic 1h/1d updates
- Idempotent: Re-running after failure resumes cleanly

### Phase 2a: Raw Trade Normalization

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1 (Current): Raw Data Collection (Venue-First)           │
└─────────────────────────────────────────────────────────────────┘
    │
    │ xetra-parqed fetch-trades DETR
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ data/de/xetra/trades/venue=DETR/year=2025/month=12/            │
│   ├── day=01/trades.parquet  (all ISINs mixed)                  │
│   ├── day=02/trades.parquet                                     │
│   └── ...                                                        │
└─────────────────────────────────────────────────────────────────┘
    │
    │ xetra-parqed update DETR (auto-detects December needs normalization)
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ Normalization Service (Phase 2a)                               │
│   1. Load all day files for month                               │
│   2. Group by ISIN                                              │
│   3. Sort trades by timestamp                                   │
│   4. Preserve ALL columns (no aggregation)                      │
│   5. Write one file per ISIN per month                          │
└─────────────────────────────────────────────────────────────────┘
    │
    │ PartitionedStorageBackend.save()
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ data/de/xetra/trades_by_isin/                                   │
│   ├── venue=DETR/isin=DE0005190003/year=2025/month=12/trades.parquet │
│   ├── venue=DETR/isin=DE0008469008/year=2025/month=12/trades.parquet │
│   └── ...  (one file per ISIN per month per venue, ~100KB each) │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 2b: OHLCV Aggregation (Follows Phase 2a)

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2a Output: Normalized Trades (ISIN-First)                │
└─────────────────────────────────────────────────────────────────┘
    │
    │ data/de/xetra/trades_by_isin/venue=DETR/isin=*/year=2025/month=12/trades.parquet
    │
    │ xetra-parqed update DETR (auto-detects aggregation needed)
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ OHLCVAggregator Service (Phase 2b)                              │
│   1. Load normalized trades from trades_by_isin/                │
│   2. For each ISIN:                                             │
│      a. Load monthly parquet file                               │
│      b. Resample to target interval (1m/1h/1d)                  │
│      c. Generate OHLCV columns                                  │
│      d. Filter zero-volume bars                                 │
│      e. Write to stocks_<interval>/ticker=<ISIN>/               │
│   3. Cascade: If 1m updated, check if 1h/1d need updates        │
└─────────────────────────────────────────────────────────────────┘
    │
    │ PartitionedStorageBackend.save()
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ data/de/xetra/stocks_1m/ticker=DE0005190003/year=2025/month=12/ │
│   └── data.parquet  (1-minute OHLCV bars)                       │
│                                                                  │
│ data/de/xetra/stocks_1h/ticker=DE0005190003/year=2025/month=12/ │
│   └── data.parquet  (hourly OHLCV bars)                         │
│                                                                  │
│ data/de/xetra/stocks_1d/ticker=DE0005190003/year=2025/month=12/ │
│   └── data.parquet  (daily OHLCV bars)                          │
└─────────────────────────────────────────────────────────────────┘
```

## Correctness & Validation (Phase 2b)

To ensure aggregation correctness and detect regressions, the aggregator must implement the following validation and fallback behaviors:

- **Deduplication / ordering**: Before aggregation, normalize and deduplicate input trades.
    - Primary dedupe key: transaction id (if present). Fallback: `(timestamp, price, volume, isin)` tuple.
    - Sort by timestamp (then by transaction id) to determine open and close prices deterministically for colliding timestamps.

- **Volume conservation checks**: After aggregation, assert that the sum of `volume` across output bars equals the sum of input trade volumes for the same range. If mismatch > threshold (configurable, e.g., 0.01%), treat as failure.

- **Minute-coverage and completeness**: Report minutes covered vs expected trading minutes per session and fail or warn when coverage drops below configured thresholds. Emit metrics: `aggregation.minutes_covered`, `aggregation.capture_rate_pct`.

- **Provenance & checksums**: Record `source_files` or their checksums used to produce each partition and store them alongside the aggregated partition for audit and potential rollback.

- **Validation failure behavior**:
    - On non-critical validation warnings (e.g., minor coverage shortfall), write the aggregated partition to a staging location and mark `status=partial` in the manifest; alert operators.
    - On critical validation failures (volume mismatch, schema issues), do not promote staging file to active partition; keep staging file for investigation and emit an alert with details. Optionally store a failure artifact for diagnostics.

- **Idempotence & resumability**: Aggregations write to a staging path and produce a per-partition manifest file with progress checkpoints. Re-running aggregation in resume mode should continue from the last successful checkpoint without reprocessing already-verified partitions.

- **Atomic activation**: After successful validation, perform an atomic replace of staging → active partition (same-directory temp + fsync + replace). Update manifest to record `verified_at` and `checksum`.

- **Testing & edge-cases**: Add unit/integration tests that cover:
    - Duplicated trades and out-of-order arrivals
    - Late-arriving trades (ensure resume and merge semantics)
    - DST changes and half-day sessions
    - Empty intervals, single-trade minutes, and multiple trades per millisecond

These validations are expected to be executed by the aggregator service and its test harness prior to activating any aggregated partition.

## Testing Strategy

### Unit Tests

```python
# tests/test_ohlcv_aggregator.py
def test_aggregate_trades_to_1m_bars():
    """Test aggregation from tick data to 1-minute bars."""
    trades = create_mock_trades(num=100, timespan="1h")
    aggregator = OHLCVAggregator(...)
    
    result = aggregator.aggregate(trades, source_interval="tick", target_interval="1m")
    
    assert len(result) <= 60  # Max 60 1-minute bars in 1 hour
    assert result['volume'].sum() == trades['TradedVolume'].sum()  # Volume conservation
    assert result['high'].max() >= result['low'].min()  # OHLC consistency

def test_aggregate_1m_to_1h_bars():
    """Test aggregation from 1-minute to hourly bars."""
    minute_bars = create_mock_ohlcv(interval="1m", count=60)
    aggregator = OHLCVAggregator(...)
    
    result = aggregator.aggregate(minute_bars, source_interval="1m", target_interval="1h")
    
    assert len(result) == 1  # One hourly bar from 60 minute bars
    assert result['open'].iloc[0] == minute_bars['open'].iloc[0]  # First open
    assert result['close'].iloc[0] == minute_bars['close'].iloc[-1]  # Last close

def test_timezone_aware_aggregation():
    """Test that aggregation respects market timezone boundaries."""
    # Create trades spanning midnight UTC
    trades = create_trades_around_midnight(tz="UTC")
    aggregator = OHLCVAggregator(...)
    
    # Aggregate to daily bars in Berlin timezone
    result = aggregator.aggregate(trades, source_interval="tick", target_interval="1d", market_tz="Europe/Berlin")
    
    # Should create bars aligned to Berlin midnight, not UTC midnight
    assert result.index[0].tz == ZoneInfo("Europe/Berlin")
```

### Integration Tests

```python
# tests/test_xetra_aggregation_integration.py
def test_full_aggregation_pipeline(tmp_path):
    """Test complete workflow: fetch → store → aggregate → verify."""
    service = XetraService(working_path=tmp_path)
    
    # 1. Fetch raw trades (mocked API)
    with mock_xetra_api():
        service.fetch_trades("DETR", date(2025, 12, 5))
    
    # 2. Aggregate to 1-minute bars
    service.aggregate_ohlcv("DETR", interval="1m", date=date(2025, 12, 5))
    
    # 3. Verify output exists and is valid
    ohlcv_path = tmp_path / "data/de/xetra/stocks_1m/ticker=DE0005190003/year=2025/month=12/data.parquet"
    assert ohlcv_path.exists()
    
    df = pd.read_parquet(ohlcv_path)
    assert 'open' in df.columns
    assert 'high' in df.columns
    assert 'low' in df.columns
    assert 'close' in df.columns
    assert 'volume' in df.columns
    assert len(df) > 0

def test_intelligent_update_auto_detection(tmp_path):
    """Test update command auto-detects what needs processing."""
    service = XetraService(working_path=tmp_path)
    
    # 1. Setup: Create raw venue/day files for December 2025
    for day in range(1, 15):
        with mock_xetra_api():
            service.fetch_trades("DETR", date(2025, 12, day))
    
    # 2. Run update command (should detect normalization needed)
    orchestrator = UpdateOrchestrator(service)
    plan = orchestrator.plan_updates("DETR", dry_run=True)
    
    # Verify detection
    assert len(plan["normalize"]) == 1  # December 2025 needs normalization
    assert plan["normalize"][0] == (2025, 12)
    assert len(plan["aggregate"]["1m"]) == 0  # No normalization done yet, so no aggregation
    
    # 3. Execute normalization
    orchestrator.execute_updates("DETR", normalize_only=True)
    
    # 4. Re-plan (should now detect aggregation needed)
    plan = orchestrator.plan_updates("DETR", dry_run=True)
    assert len(plan["normalize"]) == 0  # Already normalized
    assert len(plan["aggregate"]["1m"]) == 1  # December 2025 needs 1m aggregation
    assert plan["aggregate"]["1m"][0] == (2025, 12)

def test_update_cascade_detection(tmp_path):
    """Test that 1h/1d updates cascade from 1m updates."""
    service = XetraService(working_path=tmp_path)
    orchestrator = UpdateOrchestrator(service)
    
    # Setup: Normalized data exists, no OHLCV exists
    create_normalized_data(tmp_path, month=(2025, 12))
    
    # Plan with cascade enabled (default)
    plan = orchestrator.plan_updates("DETR", intervals=["1m", "1h", "1d"], dry_run=True)
    
    # Should detect all three intervals need processing
    assert (2025, 12) in plan["aggregate"]["1m"]
    assert (2025, 12) in plan["aggregate"]["1h"]  # Cascaded
    assert (2025, 12) in plan["aggregate"]["1d"]  # Cascaded
    
    # Plan with cascade disabled
    plan = orchestrator.plan_updates("DETR", intervals=["1m"], no_cascade=True, dry_run=True)
    assert (2025, 12) in plan["aggregate"]["1m"]
    assert len(plan["aggregate"]["1h"]) == 0  # Cascade disabled
    assert len(plan["aggregate"]["1d"]) == 0  # Cascade disabled

def test_update_idempotence(tmp_path):
    """Test that re-running update after completion does nothing."""
    service = XetraService(working_path=tmp_path)
    orchestrator = UpdateOrchestrator(service)
    
    # Setup: Create raw data and run full pipeline
    create_raw_trades(tmp_path, month=(2025, 12))
    orchestrator.execute_updates("DETR")
    
    # Verify all outputs exist
    assert exists(tmp_path / "data/de/xetra/trades_by_isin/.../2025/month=12/")
    assert exists(tmp_path / "data/de/xetra/stocks_1m/.../2025/month=12/")
    assert exists(tmp_path / "data/de/xetra/stocks_1h/.../2025/month=12/")
    assert exists(tmp_path / "data/de/xetra/stocks_1d/.../2025/month=12/")
    
    # Re-plan (should detect nothing needs processing)
    plan = orchestrator.plan_updates("DETR", dry_run=True)
    assert len(plan["normalize"]) == 0
    assert len(plan["aggregate"]["1m"]) == 0
    assert len(plan["aggregate"]["1h"]) == 0
    assert len(plan["aggregate"]["1d"]) == 0

def test_update_force_reprocessing(tmp_path):
    """Test --force flag bypasses staleness checks."""
    service = XetraService(working_path=tmp_path)
    orchestrator = UpdateOrchestrator(service)
    
    # Setup: Everything already processed
    create_complete_pipeline(tmp_path, month=(2025, 12))
    
    # Without force: detects nothing
    plan = orchestrator.plan_updates("DETR", dry_run=True)
    assert len(plan["normalize"]) == 0
    
    # With force: reprocesses everything
    plan = orchestrator.plan_updates("DETR", force=True, dry_run=True)
    assert len(plan["normalize"]) == 1
    assert len(plan["aggregate"]["1m"]) == 1

def test_aggregation_preserves_data_integrity():
    """Verify that aggregation doesn't lose or corrupt data."""
    trades = load_real_trades("DETR", date(2025, 12, 5))
    total_volume = trades['TradedVolume'].sum()
    
    # Aggregate to 1m, 1h, 1d
    ohlcv_1m = aggregator.aggregate(trades, "tick", "1m")
    ohlcv_1h = aggregator.aggregate(trades, "tick", "1h")
    ohlcv_1d = aggregator.aggregate(trades, "tick", "1d")
    
    # Volume must be conserved across all intervals
    assert ohlcv_1m['volume'].sum() == total_volume
    assert ohlcv_1h['volume'].sum() == total_volume
    assert ohlcv_1d['volume'].sum() == total_volume
```

### End-to-End Tests

```python
# tests/test_xetra_cli_aggregation.py
def test_aggregate_ohlcv_cli_command():
    """Test CLI command execution."""
    runner = CliRunner()
    
    # Setup test data
    setup_mock_trades("DETR", date(2025, 12, 5))
    
    # Run CLI command
    result = runner.invoke(app, [
        "aggregate-ohlcv", "DETR",
        "--interval", "1m",
        "--date", "2025-12-05"
    ])
    
    assert result.exit_code == 0
    assert "Aggregated" in result.stdout
    assert "1m bars" in result.stdout
```

## Migration Path to Eager Aggregation

Once workflow is proven stable:

```python
# Phase 2.5: Add opt-in auto-aggregation
@app.command()
def fetch_trades(
    venue: str,
    date: Optional[str] = None,
    auto_aggregate: bool = typer.Option(False, help="Automatically aggregate to OHLCV after fetch")
):
    """Fetch raw trades with optional auto-aggregation."""
    service = XetraService()
    
    # Fetch raw trades
    service.fetch_trades(venue, parse_date(date))
    
    # Optionally aggregate
    if auto_aggregate:
        for interval in ["1m", "1h", "1d"]:
            service.aggregate_ohlcv(venue, interval, parse_date(date))
            typer.echo(f"✓ Auto-aggregated to {interval}")

# Phase 3: Enable by default in daemon mode
@app.command()
def daemon(
    venue: str,
    auto_aggregate: bool = typer.Option(True, help="Auto-aggregate after fetches (default: enabled)")
):
    """Run daemon with auto-aggregation."""
    # Default to eager aggregation in production
    ...
```

## Performance Considerations

### Pandas Resample Efficiency

- **Memory**: Resample operates on in-memory DataFrame, requires ~2x source data size
- **CPU**: Single-threaded, scales linearly with row count
- **I/O**: Read once (raw trades), write once (OHLCV)

**Benchmarks** (estimated for typical Xetra day):
- Raw trades: ~500K rows, ~50 MB
- Aggregate to 1m: ~2 seconds, output ~400 rows, ~50 KB
- Aggregate to 1h: ~1 second, output ~10 rows, ~2 KB
- Aggregate to 1d: <1 second, output ~1 row, <1 KB

### Storage Overhead

- Raw trades: 50 MB/day/venue
- OHLCV 1m: 50 KB/day/venue (~0.1% of raw)
- OHLCV 1h: 2 KB/day/venue (~0.004% of raw)
- OHLCV 1d: <1 KB/day/venue (~0.002% of raw)

**Total**: ~50 MB/day (dominated by raw trades)

### Scalability

Current architecture supports:
- **Tickers**: Unlimited (per-ticker partitioning)
- **Time range**: Years of history (monthly partitions)
- **Concurrent aggregation**: Parallelize by date or ticker if needed (future enhancement)

## Alternatives Considered

### 1. DuckDB for Aggregation

**Pros**: SQL interface, potential performance gains for large datasets
**Cons**: Additional dependency, more complex for simple resampling, overkill for Phase 2

**Decision**: Defer to Phase 3 for analytics use cases, not ETL.

### 2. Store Only Raw Data, Aggregate On-Demand

**Pros**: Single source of truth, no storage redundancy
**Cons**: Unpredictable latency, repeated computation, cache invalidation complexity

**Decision**: Pre-compute OHLCV, disk is cheap and queries are fast.

### 3. Fetch Pre-Aggregated OHLCV from Xetra API

**Pros**: No aggregation logic needed
**Cons**: Xetra API doesn't provide OHLCV, only raw trades

**Decision**: Not possible, must aggregate locally.

## Success Metrics

### Phase 2a: Raw Trade Normalization

Implementation is successful when:

1. ✅ **Data integrity**: Row counts match between venue/day source and ISIN/month target
2. ✅ **Schema preservation**: All 23 columns from raw trades preserved in normalized layout
3. ✅ **Query performance**: DuckDB single-ISIN queries 10-50x faster (scan ~100KB vs ~1GB)
4. ✅ **File size**: ISIN partition files reasonable (~50-500KB/month per ISIN)
5. ✅ **Idempotence**: CLI command can be re-run without data corruption
6. ✅ **Test coverage**: 100% pass rate on normalization test suite

### Phase 2b: OHLCV Aggregation

Implementation is successful when:

1. ✅ **Correctness**: Aggregated OHLCV matches manual verification (spot-check 10 ISINs)
2. ✅ **Volume conservation**: Sum of bar volumes equals sum of input trade volumes (±0.01%)
3. ✅ **Performance**: Aggregation completes in <5 seconds per ISIN per month
4. ✅ **Storage efficiency**: OHLCV files are <10% size of raw trades
5. ✅ **Usability**: Single `update` command with intuitive flags (`--dry-run`, `--verbose`) for debugging
6. ✅ **Testability**: 100% pass rate on aggregation test suite (unit + integration)
7. ✅ **Reusability**: OHLCVAggregator service works for both Yahoo Finance and Xetra data
8. ✅ **Auto-detection**: System correctly identifies what needs processing without manual date management

## Implementation Notes for Future Developers

This section consolidates key insights from real data analysis and design decisions to guide implementation.

### Real Data Characteristics (Xetra DETR, Feb 2026)

**File Sizes:**
- **Daily raw trades**: 23MB (535K trades across 4,168 ISINs)
- **Monthly raw trades**: 483MB (21 trading days, 11.25M trades)
- **Per-ISIN distribution**:
  - Average: 115KB/month (~2,700 trades)
  - Top 1% (42 ISINs): 5-20MB/month (100K-480K trades each)
  - Bottom 10%: <1KB/month (<100 trades)
- **After aggregation to 1m**: 50-100x compression (avg 50KB/month)

**Trading Hours (All times in CET/CEST):**
- **Continuous trading**: 09:00-17:30 (99.0% of volume)
- **Pre-market**: 08:00-08:59 (0.12% of volume)
- **Closing auction**: 17:17 (peak spike: 3,436 trades = 0.64%)
- **Post-trade filings**: 18:00-21:59 (0.15% of volume, late reporting)

**Timestamp Storage:**
- **Format**: UTC timezone-naive `TIMESTAMP_NS` (nanosecond precision)
- **Not CET**: Must convert filter boundaries, not individual timestamps
- **Example**: 08:00 UTC = 09:00 CET (winter) or 07:00 UTC = 09:00 CEST (summer)

**ISIN Skew (Critical for Performance):**
- **Top 1% ISINs**: SAP, Siemens, Deutsche Telekom (480K trades/month, 20MB files)
- **Processing time skew**: Top 42 ISINs = 60% of total runtime (2-5s each vs 50-100ms avg)
- **Parallelization impact**: 4 workers reduce 3-5min to 1-2min (4,168 ISINs)

### Critical Implementation Decisions

#### 1. Efficient Timestamp Filtering (DO THIS)

**❌ WRONG - Convert all timestamps:**
```python
# Converts 480K timestamps per high-liquidity ISIN
df['trade_time'] = pd.to_datetime(df['trade_time'], utc=True)
df = df.set_index('trade_time').tz_convert('Europe/Berlin')  # SLOW!
df = df.between_time('09:00', '17:30')
```

**✅ CORRECT - Convert boundaries only:**
```python
# Converts 2 timestamps per ISIN (99.9999% fewer conversions)
berlin = pytz.timezone('Europe/Berlin')
start_local = berlin.localize(datetime.combine(date, time(9, 0)))  # Handles DST!
end_local = berlin.localize(datetime.combine(date, time(17, 30)))
start_utc = start_local.astimezone(pytz.UTC)
end_utc = end_local.astimezone(pytz.UTC)

# Filter on raw UTC timestamps
df = df[(df['trade_time'] >= start_utc) & (df['trade_time'] <= end_utc)]
```

**Performance Impact**: 100-1000x fewer timezone conversions, especially critical for high-liquidity ISINs.

#### 2. Automatic DST Handling (Why Pandas > DuckDB for Phase 2b)

**Pandas with pytz:**
```python
# Automatically handles CET (UTC+1) vs CEST (UTC+2)
tz = pytz.timezone('Europe/Berlin')
start_local = tz.localize(datetime.combine(date, time(9, 0)))  # DST-aware
start_utc = start_local.astimezone(pytz.UTC)  # Correct offset automatically
```

**DuckDB requires manual DST logic:**
```python
# Must hardcode transition dates (MAINTENANCE BURDEN)
is_cest = (date >= datetime.date(2026, 3, 29) and date < datetime.date(2026, 10, 25))
start_hour = 7 if is_cest else 8  # Must update annually!
```

**2026 DST Transitions**:
- **CET → CEST**: March 29, 2026 at 02:00 (UTC+1 → UTC+2)
- **CEST → CET**: October 25, 2026 at 03:00 (UTC+2 → UTC+1)

**Decision**: Use pandas + pytz for Phase 2b to avoid manual DST maintenance.

#### 3. Memory Management for Parallel Aggregation

**Per-Worker Memory Usage:**
- Python interpreter: ~50MB
- Peak data: 20MB largest ISIN × 3x pandas overhead = ~60MB
- **Total per worker**: ~110MB worst case

**Configuration Guidelines**:

| Workers | Peak Memory | Processing Time | Use Case |
|---------|-------------|-----------------|----------|
| 1 | 200MB | 3-5 min | Daemon/cron (default) |
| 4 | 500MB | 1-2 min | Interactive (recommended) |
| 8 | 1GB | 45-90s | Powerful machines only |

**CLI Flag**: `--workers N` (default: 1 for safety)

#### 4. Tool Selection Rationale

**Phase 2a (Consolidation): DuckDB**
- Bulk I/O and re-partitioning (DuckDB's strength)
- 4x faster than pandas (20s vs 90s)
- 90% less memory (200MB vs 2GB)
- Native Hive partitioning support

**Phase 2b (Aggregation): Pandas**
- Time-series resampling (pandas' strength)
- `.resample()` purpose-built for OHLC
- Automatic DST handling (pytz)
- Easier debugging (step-by-step inspection)
- Simpler VWAP calculations

**Why not DuckDB for Phase 2b?**
1. Manual DST date maintenance (annual updates)
2. Verbose SQL for OHLC (`FIRST(...ORDER BY)`, `LAST(...ORDER BY)`)
3. Harder to debug (opaque SQL vs pandas `.head()`)
4. No performance advantage for small files (115KB avg)

### Common Pitfalls to Avoid

1. **Don't convert all timestamps to CET** - Convert boundaries instead (100-1000x faster)
2. **Don't forget DST transitions** - Use `pytz.localize()`, not naive datetime arithmetic
3. **Don't assume uniform ISIN sizes** - Top 1% are 100x larger than average
4. **Don't skip trading hours filtering** - Pre-market/post-trade filings are only 0.3% but may contain test data
5. **Don't use `.tz_convert()` on DataFrames** - Filter in UTC, resample in UTC, convert output if needed
6. **Don't hardcode DST dates** - Use `pytz` library for automatic handling

### Debugging Tips

**Check trading hours filter correctness:**
```python
# Verify UTC bounds are correct for the date
date = datetime.date(2026, 2, 13)  # Winter = CET (UTC+1)
berlin = pytz.timezone('Europe/Berlin')
start_local = berlin.localize(datetime.combine(date, time(9, 0)))
print(f"09:00 CET = {start_local.astimezone(pytz.UTC)} UTC")
# Expected: 08:00 UTC (winter) or 07:00 UTC (summer)
```

**Verify volume conservation:**
```python
# Input vs output volume must match (±0.01%)
input_volume = df['volume'].sum()
output_volume = ohlcv['volume'].sum()
assert abs(input_volume - output_volume) / input_volume < 0.0001
```

**Inspect high-liquidity ISINs:**
```python
# Check which ISINs are slowest to process
isin_sizes = [(isin, file.stat().st_size) for isin, file in isin_files.items()]
isin_sizes.sort(key=lambda x: x[1], reverse=True)
print("Top 10 largest ISINs:", isin_sizes[:10])
# These will dominate processing time in sequential mode
```

### Testing Checklist

Before implementation:
- [ ] Test DST transition dates (Mar 29, Oct 25, 2026)
- [ ] Test boundary filtering (09:00:00.000 and 17:30:00.000 should be included)
- [ ] Test high-liquidity ISIN (480K trades, 20MB file)
- [ ] Test low-liquidity ISIN (<100 trades, <1KB file)
- [ ] Test empty result (all trades outside trading hours)
- [ ] Verify volume conservation (input sum = output sum)
- [ ] Test parallel mode (4 workers, 8 workers)
- [ ] Test sequential mode (baseline memory usage)

## Future Enhancements (Phase 3+)


1. **Corporate Action Adjustments**: Implement split/dividend adjustments
2. **ISIN→Ticker Lookup**: Add user-friendly ticker symbol queries
3. **Automatic Aggregation**: Enable by default in daemon mode
4. **DuckDB Analytics**: Zero-copy SQL queries on OHLCV data
5. **Cross-Source Comparison**: Unified queries across Yahoo + Xetra
6. **Real-Time Aggregation**: Streaming aggregation for live trading data

## References

- [Xetra Phase 1 ADR](2025-10-12-xetra-delayed-data.md)
- [Partition Storage ADR](2025-10-12-partition-aware-storage.md)
- [DuckDB Query Layer ADR](2025-10-12-duckdb-query-layer.md)
- [Yahoo Finance Pipeline ADR](2025-10-10-yahoo-finance-data-pipeline.md)

---

**Approved By**: SiggiSmara  
**Implementation Start**: 2025-12-05
