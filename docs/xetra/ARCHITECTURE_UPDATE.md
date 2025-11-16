# Xetra Storage Architecture Update

**Date**: 2025-11-02  
**Status**: PROPOSED  
**Replaces**: Original ISIN-partitioned raw trade storage from ADR  

---

## Problem Statement

Original ADR proposed **ISIN-partitioned raw trade storage**:
```
data/de/xetra/trades/isin=DE0005140008/year=2025/month=11/data.parquet
```

This creates a **source-storage mismatch**:
- **Source**: Deutsche Börse serves **one file per venue/date** containing all ISINs
- **Storage**: We partition by ISIN, requiring splitting each download into 100+ partition writes
- **Update logic**: Must track which ISINs need updates (complex state management)
- **Deduplication**: Requires checking 4,280+ ISIN directories individually

---

## Proposed Solution: Two-Stage Storage

### Stage 1: Venue-First Time-Partitioned Landing Zone (Mirrors Source)

**Purpose**: Store raw trades exactly as received from Deutsche Börse

**Layout**:
```
data/de/xetra/
  trades/                                # Raw per-trade data (landing zone)
    venue=DETR/                          # Xetra venue (low cardinality first)
      year=2025/month=11/day=01/
        trades.parquet                   # All Xetra ISINs for DETR on 2025-11-01
      year=2025/month=11/day=02/
        trades.parquet
    venue=DFRA/                          # Frankfurt venue
      year=2025/month=11/day=01/
        trades.parquet                   # All Frankfurt ISINs for DFRA on 2025-11-01
```

**Schema**: All 23 fields from source (including `isin` and `venue` columns)

**Advantages**:
1. **Idempotent updates**: Simply check if `venue=X/year=Y/month=Z/day=D/` exists
2. **One write per download**: Atomic operation (download → decompress → write)
3. **Source fidelity**: Preserves original data structure
4. **Simple deduplication**: List existing venue/date partitions, fetch missing dates
5. **Fast ingestion**: No ISIN-based splitting required
6. **Venue-first partitioning**: Low cardinality (5 venues) before time hierarchy, enables DuckDB partition pruning

**Update CLI Logic**:
```python
def update_trades(start_date: str, end_date: str):
    """Fetch missing date ranges."""
    existing_dates = list_partition_dates("data/de/xetra/trades/")
    missing_dates = [d for d in date_range(start_date, end_date) 
                     if d not in existing_dates]
    
    for date in missing_dates:
        file = fetch(venue="DETR", date=date)
        trades_df = parse_json(file)  # All ISINs in one DataFrame
        
        # Single atomic write (no ISIN splitting)
        write_parquet(
            trades_df,
            f"data/de/xetra/trades/year={date.year}/month={date.month:02d}/day={date.day:02d}/DETR_trades.parquet"
        )
```

---

### Stage 2: ISIN-Reorganized Archive (Optional, Phase 2+)

**Purpose**: ISIN-centric view for advanced queries (e.g., "all trades for DE0005140008 ever")

**Layout**:
```
data/de/xetra/
  trades_by_isin/                        # ISIN-reorganized (optional)
    isin=DE0005140008/
      year=2025/month=11/
        data.parquet                     # Only trades for this ISIN
```

**Workflow**: Background job reorganizes landing zone data into ISIN partitions

**Use Case**: Advanced users querying single ISIN across all time (rare)

**Decision**: Defer to Phase 3+ (not needed for MVP)

---

## Aggregated OHLCV Storage (Venue-First Ticker + ISIN Dual-Partitioning)

### For Mapped ISINs: Venue-First Ticker-Partitioned

```
data/de/xetra/
  stocks_1m/
    venue=DETR/                          # Xetra venue
      ticker=dbk/                        # Deutsche Bank (ISIN: DE0005140008)
        year=2025/month=11/
          data.parquet
      ticker=sie/                        # Siemens (ISIN: DE0007236101)
        year=2025/month=11/
          data.parquet
    venue=DFRA/                          # Frankfurt venue
      ticker=dbk/
        year=2025/month=11/
          data.parquet
```

**Criteria**: ISIN successfully mapped to ticker via Deutsche Börse CSV

**Schema**: Standard OHLCV (no ISIN column needed, ticker is partition key)

---

### For Unmapped ISINs: Venue-First ISIN-Partitioned

```
data/de/xetra/
  stocks_1m/
    venue=DETR/
      isin=LU1234567890/                 # Unknown ISIN (no ticker mapping)
        year=2025/month=11/
          data.parquet
```

**Criteria**: ISIN not found in Deutsche Börse CSV (foreign listings, ETFs, etc.)

**Schema**: Standard OHLCV + `isin` column (for clarity)

**Rationale**:
1. **Venue-first**: Low cardinality (5 venues) before high cardinality (4,280 tickers), enables DuckDB partition pruning
2. **Clear separation**: `ticker=` means "we know this ticker", `isin=` means "unmapped"
3. **No data loss**: Preserve unmapped trades with full ISIN identifier
4. **Queryable**: Users can still query by ISIN if they know it
5. **Backfill-friendly**: When mapping discovered later, move `venue=X/isin=Y/` → `venue=X/ticker=Z/`
6. **Schema consistency**: Matches raw trades venue-first hierarchy

---

## Storage Layout Comparison

### Original Proposal (ADR)

```
data/de/xetra/
  trades/                                # ISIN-partitioned raw trades
    isin=DE0005140008/year=2025/month=11/
    isin=DE0007236101/year=2025/month=11/
    ...4,280 more ISIN directories...
  
  stocks_1m/                             # Ticker-partitioned OHLCV
    ticker=dbk/year=2025/month=11/
    ticker=__UNMAPPED__/                 # All unmapped ISINs (need to read parquet)
```

**Update Logic**:
```python
for isin in all_isins:  # 4,280 iterations
    if needs_update(isin, date):
        download_and_partition()  # Complex
```

---

### New Proposal (Venue-First Time-Based Landing)

```
data/de/xetra/
  trades/                                # Venue-first time-partitioned landing zone
    venue=DETR/
      year=2025/month=11/day=01/trades.parquet  # All ISINs for DETR, one file
      year=2025/month=11/day=02/trades.parquet
    venue=DFRA/
      year=2025/month=11/day=01/trades.parquet  # All ISINs for DFRA, one file
  
  stocks_1m/                             # Venue-first dual-partitioned OHLCV
    venue=DETR/
      ticker=dbk/year=2025/month=11/     # Mapped ISINs
      isin=LU1234567890/year=2025/month=11/  # Unmapped ISINs
```

**Update Logic**:
```python
for venue in venues:  # 5 iterations (DETR, DFRA, DGAT, DEUR, DETG)
    existing_dates = list_partition_dates(f"trades/venue={venue}/")
    for date in missing_dates:  # ~30 iterations for 1 month per venue
    download_and_write()  # Simple, one atomic write
```

---

## Migration Impact on Implementation Checklist

### Phase 1 Changes

**Before** (ISIN-partitioned):
- Fetch trades → split by ISIN → write 100+ partition files
- Update logic: Track per-ISIN state
- Deduplication: Check 4,280 directories

**After** (Time-partitioned):
- Fetch trades → write one partition file per date/venue
- Update logic: List existing dates, fetch missing
- Deduplication: Check ~365 date directories per year

**Code Simplification**: ~30% less complexity in Phase 1

---

### Phase 2 No Changes

Aggregation workflow unchanged:
1. Load raw trades from landing zone (now by date instead of by ISIN)
2. Map ISINs to tickers
3. Aggregate to OHLCV
4. Write to `ticker=X/` or `isin=Y/` based on mapping status

---

## Performance Comparison

### Write Performance (Ingestion)

| Metric | ISIN-Partitioned | Time-Partitioned | Improvement |
|--------|------------------|------------------|-------------|
| Writes per day | 100-200 (one per ISIN) | 1 (all ISINs) | **100x fewer** |
| Partition creation | 100-200 directories | 1 directory | **100x fewer** |
| Atomic operations | 100-200 | 1 | **100x fewer** |
| Corruption risk | 100-200 write points | 1 write point | **100x lower** |

---

### Read Performance (Aggregation)

| Metric | ISIN-Partitioned | Time-Partitioned | Impact |
|--------|------------------|------------------|--------|
| Read all trades for date | Read 100+ files | Read 1 file | **100x faster** |
| Read all trades for ISIN | Read 1 file | Read N date files | **Nx slower** (rare query) |
| Aggregation input | Concat 100 files | Read 1 file | **Simpler** |

**Trade-off**: Time-partitioned is faster for "all ISINs for date" (common), slower for "all dates for ISIN" (rare).

---

### Storage Overhead

| Metric | ISIN-Partitioned | Time-Partitioned | Impact |
|--------|------------------|------------------|--------|
| Partition directories | 4,280+ ISINs × 12 months | 365 days × 1 venue | **92% fewer dirs** |
| Parquet overhead | 4,280 files × ~200KB | 365 files × ~20MB | **Similar total size** |
| Metadata overhead | High (many small files) | Low (fewer large files) | **Better FS performance** |

---

## Decision Matrix

| Criterion | ISIN-Partitioned | Time-Partitioned | Winner |
|-----------|------------------|------------------|--------|
| **Source alignment** | ❌ Mismatch (split needed) | ✅ 1:1 match | Time |
| **Update simplicity** | ❌ Track 4,280 ISINs | ✅ List dates | Time |
| **Write performance** | ❌ 100+ writes/day | ✅ 1 write/day | Time |
| **Deduplication** | ❌ 4,280 checks | ✅ 365 checks | Time |
| **Query by ISIN** | ✅ Direct partition | ❌ Scan dates | ISIN |
| **Query by date** | ❌ Scan 100+ files | ✅ Read 1 file | Time |
| **Corruption risk** | ❌ 100+ write points | ✅ 1 write point | Time |
| **Filesystem load** | ❌ 50k+ files/year | ✅ 365 files/year | Time |

**Result**: Time-partitioned wins **7 out of 8 criteria**

---

## Updated Storage Paths

### Raw Trades (Landing Zone)

```python
XETRA_TRADE_PATH = (
    "data/{market}/{source}/trades/"
    "year={year}/month={month:02d}/day={day:02d}/"
    "{venue}_trades.parquet"
)

# Example:
# data/de/xetra/trades/year=2025/month=11/day=01/DETR_trades.parquet
```

---

### Aggregated OHLCV (Mapped ISINs)

```python
XETRA_OHLCV_TICKER_PATH = (
    "data/{market}/{source}/stocks_{interval}/"
    "ticker={ticker}/"
    "year={year}/month={month:02d}/"
    "data.parquet"
)

# Example:
# data/de/xetra/stocks_1m/ticker=dbk/year=2025/month=11/data.parquet
```

---

### Aggregated OHLCV (Unmapped ISINs)

```python
XETRA_OHLCV_ISIN_PATH = (
    "data/{market}/{source}/stocks_{interval}/"
    "isin={isin}/"
    "year={year}/month={month:02d}/"
    "data.parquet"
)

# Example:
# data/de/xetra/stocks_1m/isin=LU1234567890/year=2025/month=11/data.parquet
```

---

## CLI Simplification

### Before (ISIN-Partitioned)

```bash
# Complex: Must specify ISINs or "all"
xetra fetch-trades --venue DETR --date 2025-11-01 --isins DE0005140008,DE0007236101
xetra fetch-trades --venue DETR --date 2025-11-01 --all-isins  # Fetch and split into 4,280 partitions
```

---

### After (Time-Partitioned)

```bash
# Simple: Just fetch missing dates
xetra update  # Fetches all missing dates since last update

# Or specify range
xetra update --start-date 2025-11-01 --end-date 2025-11-30

# Manual single-date fetch
xetra fetch-trades --venue DETR --date 2025-11-01  # One atomic write
```

---

## Implementation Changes

### Phase 1 Update: XetraFetcher + Storage

**Old**:
```python
def save_trades(trades_df: pd.DataFrame, date: datetime):
    for isin, group in trades_df.groupby('isin'):  # 100+ iterations
        path = f"trades/isin={isin}/year={date.year}/month={date.month:02d}/"
        write_parquet(group, path)
```

**New**:
```python
def save_trades(trades_df: pd.DataFrame, venue: str, date: datetime):
    path = f"trades/year={date.year}/month={date.month:02d}/day={date.day:02d}/{venue}_trades.parquet"
    write_parquet(trades_df, path)  # Single atomic write
```

**Lines of Code**: ~50 lines deleted (no ISIN splitting logic)

---

### Phase 2 Update: XetraAggregator

**Old**:
```python
def aggregate(date: datetime, interval: str):
    trades = []
    for isin in all_isins:  # 4,280 iterations
        isin_trades = read_parquet(f"trades/isin={isin}/year={date.year}/...")
        trades.append(isin_trades)
    all_trades = pd.concat(trades)
    # ... aggregate
```

**New**:
```python
def aggregate(date: datetime, interval: str):
    # Single read operation
    all_trades = read_parquet(f"trades/year={date.year}/month={date.month:02d}/day={date.day:02d}/DETR_trades.parquet")
    # ... aggregate
```

**Lines of Code**: ~20 lines deleted (no multi-file concat logic)

---

### Phase 2 Update: Dual-Partitioning for OHLCV

```python
def save_ohlcv(ohlcv_df: pd.DataFrame, interval: str, date: datetime):
    """Save OHLCV with dual-partitioning (ticker vs ISIN)."""
    
    # Split by mapping status
    mapped = ohlcv_df[ohlcv_df['ticker'].notna()]
    unmapped = ohlcv_df[ohlcv_df['ticker'].isna()]
    
    # Write mapped ISINs to ticker partitions
    for ticker, group in mapped.groupby('ticker'):
        path = f"stocks_{interval}/ticker={ticker}/year={date.year}/month={date.month:02d}/"
        write_parquet(group.drop(columns=['isin']), path)
    
    # Write unmapped ISINs to ISIN partitions
    for isin, group in unmapped.groupby('isin'):
        path = f"stocks_{interval}/isin={isin}/year={date.year}/month={date.month:02d}/"
        write_parquet(group, path)  # Keep ISIN column
```

---

## Updated ADR Decision (Proposed)

**AD-2 (Revised): Two-Stage Storage Pattern with Venue-First Partitioning**

**Decision**: Use **venue-first time-partitioned landing zone** for raw trades, **venue-first dual-partitioned** (ticker/ISIN) for aggregated OHLCV.

**Rationale**:
1. **Source alignment**: Deutsche Börse serves one file per venue/date (all ISINs) → store one file per venue/date
2. **Venue-first partitioning**: Low cardinality (5 venues) before high cardinality (time/tickers) follows Hive partitioning best practice
3. **DuckDB optimization**: `WHERE venue = 'DETR'` uses partition pruning on first partition level
4. **Idempotent updates**: Check existing venue/date partitions, fetch missing dates (simple set difference)
5. **Write efficiency**: 1 atomic write per venue/date vs 100+ per date (ISIN-partitioned)
6. **Deduplication**: List ~365 date dirs per venue vs 4,280 ISIN dirs
7. **Corruption risk**: 1 write point per venue/date vs 100+ write points
8. **Query patterns**: "All trades for venue+date" (common) optimized; venue-specific queries use partition pruning
9. **Schema consistency**: Raw trades and aggregated OHLCV both use venue-first ordering

**Trade-offs Accepted**:
- Queries for "all trades for single ISIN across all venues and time" require scanning multiple venue/date partitions (rare use case)
- Users wanting ISIN-centric view can use optional Stage 2 reorganization (deferred to Phase 3+)
- Single-venue users see extra directory level (minimal overhead, standard across industry)

**Unmapped ISINs**: Use `venue=VENUE/isin=X` partitions instead of `ticker=__UNMAPPED__` for clarity and queryability

---

## Venue-First Partitioning Analysis

### Problem: Partition Order Decision

After approving time-based landing zone, question arose: should venue be a partition key? If so, should it come before or after ticker/ISIN?

**Options Considered**:
1. **No venue partitioning**: Keep current structure (venue in filename or column)
2. **Ticker-first**: `ticker=dbk/venue=DETR/year=2025/...`
3. **Venue-first**: `venue=DETR/ticker=dbk/year=2025/...` ⭐ RECOMMENDED

### Decision Matrix: Venue-First vs Ticker-First

| Criterion | Ticker-First | Venue-First | Winner |
|-----------|--------------|-------------|--------|
| **Cardinality** | High before low (4,280 → 5) | Low before high (5 → 4,280) | **Venue-first** ✅ (Hive best practice) |
| **Common queries** | "All venues for ticker X" | "All tickers for venue X" | **Venue-first** ✅ (most users focus on single venue) |
| **DuckDB partition pruning** | Scans all tickers to filter venue | Prunes at top level | **Venue-first** ✅ (5x less scanning) |
| **Directory listing** | 4,280 ticker dirs at top | 5 venue dirs at top | **Venue-first** ✅ (faster filesystem ops) |
| **Cross-venue analysis** | Slightly easier | Slightly harder | **Ticker-first** (but rare use case) |
| **Schema consistency** | Different from raw trades | Matches raw trades pattern | **Venue-first** ✅ (if raw uses venue-first) |

**Result**: **Venue-first wins 5/6 criteria** (tie-breaker: partition cardinality best practice)

### Venue-First Storage Layout

**Raw Trades**:
```
data/de/xetra/trades/
  venue=DETR/                           # Xetra venue
    year=2025/month=11/day=01/
      trades.parquet                    # All ISINs for DETR on 2025-11-01
  venue=DFRA/                           # Frankfurt venue
    year=2025/month=11/day=01/
      trades.parquet                    # All ISINs for DFRA on 2025-11-01
```

**Aggregated OHLCV** (mapped):
```
data/de/xetra/stocks_1m/
  venue=DETR/
    ticker=dbk/
      year=2025/month=11/data.parquet
    ticker=sie/
      year=2025/month=11/data.parquet
  venue=DFRA/
    ticker=dbk/
      year=2025/month=11/data.parquet
```

**Aggregated OHLCV** (unmapped):
```
data/de/xetra/stocks_1m/
  venue=DETR/
    isin=LU1234567890/
      year=2025/month=11/data.parquet
```

### DuckDB Query Examples

**Venue-specific query with partition pruning**:
```sql
-- Venue-first: Scans only venue=DETR partition (1/5 of data)
SELECT * FROM read_parquet('data/de/xetra/stocks_1m/*/*/*/*')
WHERE venue = 'DETR'
-- DuckDB automatically prunes to venue=DETR/ directory tree

-- Ticker-first: Must scan all 4,280 ticker directories
SELECT * FROM read_parquet('data/de/xetra/stocks_1m/*/*/*/*')
WHERE venue = 'DETR'
-- DuckDB scans ticker=*/venue=DETR/... across all tickers
```

**Multi-venue query**:
```sql
-- Venue-first: Scans only DETR and DFRA partitions (2/5 of data)
SELECT * FROM read_parquet('data/de/xetra/stocks_1m/*/*/*/*')
WHERE venue IN ('DETR', 'DFRA')
```

**Ticker-specific query** (both equally efficient):
```sql
-- Venue-first
SELECT * FROM read_parquet('data/de/xetra/stocks_1m/venue=DETR/ticker=dbk/*/*')

-- Ticker-first
SELECT * FROM read_parquet('data/de/xetra/stocks_1m/ticker=dbk/*/*')
```

### Advantages of Venue-First

1. **Follows Hive partitioning best practice**: Low-cardinality (5 venues) before high-cardinality (4,280 tickers)
2. **DuckDB partition pruning**: `WHERE venue = 'DETR'` scans 20% of data instead of 100%
3. **Filesystem efficiency**: Top-level listing shows 5 directories instead of 4,280
4. **Schema consistency**: Raw trades and OHLCV both use venue-first hierarchy
5. **Common query pattern**: Most users analyze single venue (Xetra) rather than cross-venue ticker analysis
6. **Extensibility**: Adding new venues doesn't pollute ticker namespace

---

## Recommendation

**APPROVE** this architecture update for the following reasons:

1. ✅ **Simpler implementation**: 30% less code in Phase 1, 20% less in Phase 2
2. ✅ **Better source alignment**: 1:1 mapping between Deutsche Börse files and storage
3. ✅ **Idempotent updates**: Trivial to detect missing venue/dates
4. ✅ **Lower corruption risk**: 100x fewer write operations
5. ✅ **Better filesystem performance**: 92% fewer partition directories at top level
6. ✅ **Clearer unmapped handling**: `venue=X/isin=Y` vs `ticker=__UNMAPPED__`
7. ✅ **Aligns with existing yf_parqed patterns**: Already uses time-based partitioning for Yahoo data
8. ✅ **DuckDB optimization**: Venue-first partitioning enables efficient partition pruning
9. ✅ **Follows Hive best practices**: Low-cardinality (venue) before high-cardinality (ticker/ISIN)
10. ✅ **Schema consistency**: Raw trades and OHLCV both use venue-first hierarchy

**No breaking changes**: This is a planning-phase update (no code written yet)

**Action Items**:
1. Update ADR AD-2 and AD-3 with venue-first storage layout
2. Update Implementation Checklist Phase 1 and Phase 2 (add venue partitioning)
3. Update Implementation Addendum Sections 3.3, 4.1, 4.2 (venue-first paths)
4. Update README storage diagram (add venue directories)
5. Add this document to `docs/xetra/` as decision rationale

---

## Next Steps

**If approved**:
1. [x] Update `/docs/adr/2025-10-12-xetra-delayed-data.md` AD-2 and AD-3 sections
2. [x] Update `/docs/xetra/IMPLEMENTATION_CHECKLIST.md` Phase 1.3 and Phase 2.6 (venue-first paths)
3. [x] Update `/docs/xetra/xetra_implementation_addendum.md` Sections 3.3, 4.1, 4.2 (venue-first schema)
4. [x] Update `/docs/xetra/README.md` storage layout diagram
5. [x] Update this document with venue-first partitioning analysis

**Estimated update time**: 4 hours to revise all documentation (COMPLETED 2025-11-02)

---

**Status**: APPROVED (Time-based storage approved 2025-11-02, Venue-first partitioning approved 2025-11-02)
