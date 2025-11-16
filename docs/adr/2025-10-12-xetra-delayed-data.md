# ADR 2025-10-12: Xetra Delayed Data Ingestion

## Status

**In Progress** (Phase 1 Complete, Phase 2 Partial) - Last updated 2025-11-17

- ✅ **Phase 1 Complete**: Foundation infrastructure operational with production-quality test coverage
- 🚧 **Phase 2 In Progress**: Raw trade storage working, OHLCV aggregation pending
- 🔲 **Phase 3-5**: Not started (split tracking, production hardening, advanced features)

## Context

### Business Requirements

- Users require delayed (15-minute) Deutsche Börse Xetra trading data to expand coverage beyond US markets
- Deutsche Börse publishes freely accessible, per-minute JSON snapshots as gzip-compressed files
- Data is available via anonymous HTTP download for approximately 24 hours rolling window
- URL pattern: `https://mfs.deutsche-boerse.com/api/download/{VENUE}-posttrade-{YYYY-MM-DD}T{HH_MM}.json.gz`
  - Venues: `DETR` (Xetra), `DETG` (Tradegate)
  - Feed type: `posttrade` (executed transactions)
  - Timestamp: minute-level granularity

### Technical Context

- Existing `yf_parqed` architecture supports US equities via Yahoo Finance API
- Partition-aware storage backend (Hive-style) provides proven scalability
- Current schema: OHLCV (open/high/low/close/volume) + metadata
- Yahoo Finance returns split-adjusted prices but lacks split tracking
- Integration requires ISIN-to-ticker mapping and corporate action handling

### Key Differences from Yahoo Finance

| Aspect | Yahoo Finance | Deutsche Börse |
|--------|--------------|----------------|
| **Granularity** | Pre-aggregated intervals (1m/1h/1d) | Per-trade data in 1-minute buckets |
| **Identifier** | Ticker symbol (AAPL) | ISIN (DE0005810055) + Mnemonic (DBK) |
| **Data Retention** | Indefinite API access | 24-hour rolling window |
| **Rate Limiting** | Strict (3 req/2s) | None (static file download) |
| **Split Adjustment** | Automatic (opaque) | Unadjusted (requires external tracking) |
| **Authentication** | None | None |

## Decision Drivers

1. **Market expansion** - Enable European equity analysis without licensing costs
2. **Architecture reuse** - Leverage existing partition storage, recovery, and configuration infrastructure
3. **Operational separation** - Xetra workflows must not disrupt existing Yahoo Finance operations
4. **Data fidelity** - Preserve raw trade data for re-aggregation and analysis flexibility
5. **Split handling** - Address gap in corporate action tracking for both Yahoo and Xetra data

## Architectural Decisions

> **Note**: Full implementation details available in `/docs/xetra_implementation_plan.md`

### AD-1: Separate Primary Class (`XetraParqed`)

**Decision**: Implement as standalone class, not extension of `YFParqed`.

**Rationale**: Different operational characteristics (no rate limiting needed, time-sensitive backfilling within 24h retention window, different error semantics where 404 = data expired rather than ticker not found) warrant clean separation of concerns. Share battle-tested infrastructure (ConfigService, PartitionedStorageBackend, parquet_recovery) via dependency injection.

**Shared Components**:
- ConfigService, PartitionedStorageBackend, PartitionPathBuilder, parquet_recovery module

**Xetra-Specific Components** (new):
- XetraFetcher, XetraTickerRegistry, XetraScheduler, XetraAggregator

### AD-2: Dual-Schema Storage with Time-Based Landing Zone

**Decision**: Store both raw per-trade data (`trades/` dataset) and pre-aggregated OHLCV intervals (`stocks_*/` datasets).

**Trade Schema** (raw): **Venue-first time-partitioned landing zone** (`venue=VENUE/year=YYYY/month=MM/day=DD/`) mirrors Deutsche Börse file organization. Each venue+date partition contains one parquet file with all ISINs for that venue/trading day. Preserves all 23 source fields including ISIN, security metadata, trade counts, venue identifier.

**Aggregated Schema** (1m/1h/1d): **Venue-first dual-partitioned** OHLCV format:
- **Mapped ISINs**: `venue=VENUE/ticker={ticker}/year=YYYY/month=MM/` - Compatible with Yahoo data structure, extended with Xetra-specific fields (trade counts, currency)
- **Unmapped ISINs**: `venue=VENUE/isin={isin}/year=YYYY/month=MM/` - Preserves data for securities without ticker mapping (foreign listings, ETFs), includes ISIN column for clarity

**Rationale**: 
- **Source alignment**: Deutsche Börse serves one file per venue/date (all ISINs) → storage mirrors this 1:1, avoiding ISIN-based splitting (100+ writes per day → 1 write per day)
- **Venue-first partitioning**: Low cardinality (5 venues) before high cardinality (4,280 tickers) follows Hive partitioning best practice, enables efficient DuckDB partition pruning on venue queries
- **Idempotent updates**: Simple date listing detects missing data (365 date checks/year vs 4,280 ISIN checks)
- **Write efficiency**: Single atomic operation per venue/date reduces corruption risk (1 write point vs 100+ write points)
- **Query optimization**: Common pattern "all trades for venue+date" reads 1 file; venue-specific queries use partition pruning
- **Unmapped clarity**: `isin=` partition key clearly signals unmapped status vs ambiguous `ticker=__UNMAPPED__`
- **Schema consistency**: Raw trades and aggregated OHLCV both use venue-first ordering
- **Preserves source fidelity** for re-aggregation and detailed analysis while maintaining drop-in compatibility with existing analytics workflows

**Trade-offs Accepted**: Queries for "all trades for single ISIN across all time and venues" require scanning multiple venue/date partitions (rare use case, can be optimized with optional ISIN-reorganized archive in Phase 3+).

### AD-3: Partition Layout (Venue-First Hive Partitioning)

**Decision**: Use **venue-first Hive-style partitioning** for both raw trades and aggregated OHLCV data.

**Directory Structure**:
```
data/de/deutsche-boerse/
├── trades/                              # Raw per-trade data (23 fields)
│   ├── venue=DETR/                      # Xetra venue
│   │   ├── year=2025/month=11/day=01/
│   │   │   └── trades.parquet           # All ISINs for DETR on 2025-11-01
│   │   └── year=2025/month=11/day=02/
│   │       └── trades.parquet
│   ├── venue=DFRA/                      # Frankfurt venue
│   │   └── year=2025/month=11/day=01/
│   │       └── trades.parquet           # All ISINs for DFRA on 2025-11-01
│   └── venue=DGAT/                      # Stuttgart venue
│       └── year=2025/month=11/day=01/
│           └── trades.parquet
│
├── stocks_1m/                           # 1-minute OHLCV aggregates
│   └── venue=DETR/
│       ├── ticker=dbk/                  # Deutsche Bank (mapped ISIN)
│       │   ├── year=2025/month=11/
│       │   │   └── data.parquet
│       │   └── year=2025/month=12/
│       │       └── data.parquet
│       └── isin=LU1234567890/           # Unmapped ETF (no ticker)
│           └── year=2025/month=11/
│               └── data.parquet
│
├── stocks_1h/                           # 1-hour OHLCV aggregates
│   └── venue=DETR/
│       ├── ticker=sap/
│       │   └── year=2025/month=11/
│       │       └── data.parquet
│       └── isin=DE0005140008/
│           └── year=2025/month=11/
│               └── data.parquet
│
└── stocks_1d/                           # 1-day OHLCV aggregates
    └── venue=DETR/
        ├── ticker=bmw/
        │   └── year=2025/month=11/
        │       └── data.parquet
        └── isin=FR0000120271/
            └── year=2025/month=11/
                └── data.parquet
```

**Rationale**:
- **Low-cardinality-first**: Venue (5 values) before ticker/ISIN (4,280 values) follows Hive partitioning best practice
- **DuckDB optimization**: `WHERE venue = 'DETR'` uses partition pruning on first partition level, scanning 1/5 of data
- **Directory efficiency**: Top-level listing shows 5 venue directories vs 4,280 ticker directories (reduces filesystem overhead)
- **Schema consistency**: Both raw trades and aggregated OHLCV use same venue-first hierarchy
- **Source alignment**: Mirrors Deutsche Börse API structure (venue → date → ISINs)
- **Multi-venue queries**: `WHERE venue IN ('DETR', 'DFRA')` scans 2/5 of data with partition pruning
- **Standard Hive conventions**: `key=value/` naming enables automatic schema discovery in DuckDB, Spark, Trino

**Trade-offs Accepted**: Single-venue users see extra directory level (minimal overhead, standard across industry)

### AD-4: Corporate Action Tracking (Phased Implementation)

**Decision**: Address split tracking gap for both Yahoo and Xetra data in three phases.

**Identified Gap**: Yahoo Finance returns split-adjusted prices by default but provides no metadata about adjustment timing. Re-fetching historical data post-split creates duplicates with different price scales.

**Phase 1** (MVP - Detection and metadata only):

- Query `yfinance.Ticker.actions` API for Yahoo data during ticker updates
- Manual configuration for Xetra (Deutsche Börse corporate actions feed integration deferred)
- Extend `tickers.json` with `corporate_actions` section containing splits and dividends arrays
- Log warnings when new splits detected, no automatic price adjustment

**Phase 2** (Adjustment factors):

- Add optional `split_factor` and `div_factor` columns to parquet schema
- Store unadjusted prices + factors for Xetra data (source provides unadjusted)
- Continue split-adjusted storage for Yahoo data (backward compatible)
- Provide helper functions for on-the-fly price adjustment in analysis layer

**Phase 3** (Historical reconstruction):

- Tool to backfill adjustment factors for existing Yahoo data
- Re-aggregation capability using unadjusted prices
- Automated split detection via price discontinuities (>20% overnight gaps)

**Rationale**: Immediate value (split awareness and logging) without blocking MVP delivery. Incremental complexity added based on user demand for unadjusted price analysis.

### AD-5: Separate CLI Binary (`xetra-parqed`)

**Decision**: Provide standalone CLI, not extensions to `yf-parqed`.

**Command Examples**:

```bash
# Initialization
xetra-parqed initialize --venue xetra --intervals 1m,1h,1d

# Data updates
xetra-parqed update-data --backfill-hours 24
xetra-parqed update-data --venue tradegate --intervals 1h,1d

# Both CLIs coexist in same workspace
yf-parqed update-data --intervals 1d
xetra-parqed update-data --backfill-hours 24
```

**Rationale**: Clearer operational responsibilities (time-sensitive backfilling vs flexible scheduling). Avoids bloating `yf-parqed` with Xetra-specific flags and conditional logic. Both CLIs write to shared `data/` root with non-overlapping partition paths.

### AD-6: No Rate Limiting Required for Xetra

**Decision**: XetraFetcher implements empirically validated rate limiting despite static file downloads.

**Original Rationale**: Static file downloads via HTTP GET (not a quota-limited API), no authentication tokens, no documented throttling policies.

**Implementation Update (2025-11-17)**: Empirical testing revealed Deutsche Börse **does enforce rate limits** on bulk downloads:
- **Discovered behavior**: Burst downloads trigger throttling/connection issues
- **Validated configuration**: 0.6s inter-request delay + 35s cooldown after 30 requests
- **Performance**: Linear relationship (R²=0.97), zero 429 errors over 810 consecutive files
- **Trading hours filtering**: Reduces file count by ~56.5% (08:30-18:00 CET/CEST window)

**Result**: XetraFetcher includes sophisticated rate limiting with burst management and trading hours optimization, contrary to original "no rate limiting needed" assumption. This prevented production issues during 24-hour backfills.

### AD-7: Schema-Agnostic Recovery Module

**Decision**: Keep `parquet_recovery.py` generic; each primary class (YFParqed, XetraParqed) injects its own normalizer function.

**Current Behavior**: Recovery module expects specific column set hardcoded for Yahoo data

**Enhanced Approach**: 

- Recovery module validates schema post-normalization, doesn't know column names
- Each data source provides normalizer handling source-specific quirks (column aliases, missing fields, type coercion)
- XetraParqed provides separate normalizers for trade schema vs aggregated schema
- Index promotion logic works unchanged (handles generic unnamed numeric index)

**Rationale**: Extensibility for future data sources (London Stock Exchange, Tokyo Exchange, etc.) without modifying core recovery logic. Single responsibility: recovery handles corruption detection, normalizers handle source-specific transformations.

### AD-8: ISIN→Ticker Mapping via Deutsche Börse CSV

**Decision**: Use Deutsche Börse's official "All Tradable Instruments" CSV as primary ISIN→ticker mapping source instead of third-party APIs (e.g., OpenFIGI).

**Data Source**: 
- **URL**: `https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments`
- **Format**: Semicolon-delimited CSV with 130+ columns, ~4,280 XETRA instruments
- **Update Frequency**: Daily (~11:54 PM CET)
- **Relevant Fields**: ISIN (column 3), Mnemonic/ticker (column 7), Instrument name, Currency, WKN

**Implementation**:
- Web scraper extracts dynamic CSV download URL from Deutsche Börse webpage
- Daily cron job downloads CSV and merges with local Parquet cache (`data/reference/isin_mapping.parquet`)
- Cache schema: `isin`, `ticker`, `name`, `currency`, `wkn`, `status`, `first_seen`, `last_seen`, `source`
- Runtime lookups via in-memory dictionary (<1ms latency)
- Unknown ISINs written to `__UNMAPPED__` partition for manual review

**Rationale**: 
- **Authoritative**: Official exchange data vs third-party aggregator
- **Cost**: FREE vs $500/month for OpenFIGI production tier
- **Performance**: Local cache (<1ms) vs network API (50-200ms)
- **Reliability**: No rate limits, 100% uptime via local cache
- **Trade-off**: Daily update lag (0-24 hours for new IPOs) acceptable given rarity and `__UNMAPPED__` fallback

**Alternative Considered**: OpenFIGI API provides global coverage (100M+ instruments) and real-time updates but requires paid subscription ($500/month for 250 req/sec) and introduces network dependency. Deferred to Phase 3 as optional fallback for non-German ISINs (ADRs, international listings).

**Related Documentation**: See `/docs/xetra_isin_mapping_strategy.md` for implementation details, `/docs/xetra_isin_mapping_decision.md` for cost-benefit analysis.

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2) ✅ **COMPLETE**

**Goal**: Core Xetra ingestion without aggregation

**Deliverables**:

- ✅ XetraFetcher service (416 lines, 97% coverage) - URL generation, download, gzip decompression, JSON parsing, rate limiting with burst management
- ✅ XetraParser service (266 lines, 100% coverage) - 22-field schema normalization, required field validation, complete schema enforcement
- ✅ XetraService orchestration (843 lines, 80% coverage) - Coordinates fetcher + parser + storage, incremental fetch with gap detection
- ✅ Trade schema storage - Daily partitioning (`venue=VENUE/year=YYYY/month=MM/day=DD/`), monthly consolidation working
- ✅ CLI basics (418 lines, 100% coverage) - `fetch-trades`, `check-status`, `list-files`, `check-partial`, `consolidate-month`

**Actual Results**: 
- **Test Coverage**: 129 Xetra-specific tests across 8 test files, exceeding quality targets
- **Rate Limiting**: Empirically validated (R²=0.97 linear model, zero 429 errors over 810 files)
- **Trading Hours Optimization**: 56.5% file reduction via timezone-aware filtering
- **Production Quality**: Error handling, retry logic, corruption recovery all working

**Acceptance**: ✅ Can fetch and store 24h of 1m trade data for unlimited ISINs, verified partition layout, handles 404s gracefully, monthly consolidation operational

**Completion Date**: 2025-11-17

### Phase 2: Aggregation & Multi-Interval (Weeks 3-4) 🚧 **IN PROGRESS**

**Goal**: Generate 1h/1d aggregations from 1m data

**Deliverables**:

- 🚧 XetraAggregator service (load 1m trades, resample to 1h/1d with OHLC logic, preserve trade counts and venue) - **Not yet implemented**
- 🚧 Aggregated schema storage (empty_stock_frame with Xetra extensions, write to stocks_1h/, stocks_1d/) - **Not yet implemented**
- ✅ Monthly consolidation working (raw trades only)
- 🚧 XetraScheduler integration (orchestrate fetch→store trades→aggregate→store stocks) - **Partial: fetch→store working, aggregation pending**
- 🚧 CLI enhancements (update-data with intervals flag, verify command) - **fetch-trades complete, intervals flag pending**

**Current Status**:
- ✅ **Working**: Raw per-trade data storage in daily partitions, monthly consolidation
- ❌ **Missing**: OHLCV resampling logic, `stocks_1m/`, `stocks_1h/`, `stocks_1d/` datasets, ticker/ISIN partitioning for aggregated data

**Acceptance**: Fetch 24h of 1m data, generate matching 1h and 1d aggregations, verify row count consistency - **Partial completion**

**Notes**: Phase 1 overdelivered on quality (100% CLI coverage, empirical rate limit validation), but Phase 2 aggregation is critical for MVP usability. Most users need OHLCV intervals, not raw per-trade data.

### Phase 3: Split Tracking (Weeks 5-6) 🔲 **NOT STARTED**

**Goal**: Detect and track corporate actions

**Deliverables**:

- Extend tickers.json with corporate_actions structure
- Yahoo split detection via yfinance.Ticker.actions, populate metadata, log warnings
- Xetra manual split configuration via de_tickers.json or similar
- Optional: Add split_factor column to schema, populate for new data, provide adjustment helper

**Acceptance**: Detect historical AAPL 4-for-1 split in test data, store metadata, log warnings on refetch

**Status**: Deferred pending Phase 2 completion

### Phase 4: Production Hardening (Weeks 7-8) 🔲 **NOT STARTED**

**Goal**: Multi-venue support, robust error handling, operational tooling

**Deliverables**:

- Tradegate venue support (DETG URL pattern, separate partition paths, mixed workflow testing)
- ISIN→ticker mapping via Deutsche Börse CSV (web scraper, daily update CLI, Parquet cache with lifecycle tracking)
- Recovery and validation (corrupt parquet handling via existing module, missing minute detection, row count verification)
- Documentation (usage guide, schema reference, migration plan for existing users)

**Acceptance**: Both Xetra and Tradegate working end-to-end, ISIN mapper handles 4,280+ instruments with <1ms lookups, missing data gaps logged, comprehensive test coverage

**Status**: Deferred pending Phase 2 completion. Current implementation focuses on DETR (Xetra) venue only.

### Phase 5: Advanced Features (Future) 🔮 **DEFERRED**

- DuckDB integration for zero-copy analytics on trades and aggregations (ADR exists: `/docs/adr/2025-10-12-duckdb-query-layer.md`)
- Deutsche Börse corporate actions feed integration (automatic split detection)
- Cross-venue analysis tools (compare Xetra vs Tradegate prices, arbitrage detection)
- Real-time WebSocket feed integration for low-latency tick storage

**Status**: Awaiting Phase 2-4 completion and user feedback on OHLCV aggregation workflows

## Testing Strategy

### Unit Tests

- **XetraFetcher**: URL generation, JSON parsing (valid/malformed), decompression errors, HTTP error handling
- **XetraAggregator**: 1m→1h/1d resampling, OHLC logic, gap handling, trade count preservation
- **XetraParqed**: Initialization, config management, storage backend wiring

### Integration Tests

- **End-to-end**: Fetch mocked 1m file → parse → store trades → aggregate → verify parquet files and schema
- **Multi-venue**: Fetch Xetra + Tradegate for same ISIN, verify separate partition paths, aggregate independently

### Fixtures

```python
# tests/fixtures/xetra_sample.json.gz
# 60 minutes of DBK trades (2024-11-02 09:00-10:00)

# tests/fixtures/tradegate_sample.json.gz  
# 60 minutes of DBK trades (same period, different venue)
```

## Alternatives Considered

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **Extend YFParqed** | Code reuse, single class | Conflated concerns, complex conditionals, harder to test | ❌ Rejected - operational differences too significant |
| **Real-time streaming** | Lower latency, live data | Higher licensing cost, infrastructure complexity, legal review | 🔮 Deferred - prove delayed pipeline first |
| **Third-party aggregators** | Simplified ingestion, managed service | Reduced control, additional costs, vendor lock-in | ❌ Rejected - maintain data sovereignty |
| **Single schema (OHLCV only)** | Simpler storage, less disk usage | Loss of source fidelity, cannot re-aggregate | ❌ Rejected - raw data valuable for research |
| **Unified CLI** | Single entry point, consistent UX | Bloated command surface, unclear operational boundaries | ❌ Rejected - separation of concerns clearer |

## Success Metrics

1. **Functional**: Fetch and store 24 hours of Xetra 1m data for 100 ISINs without data loss
2. **Performance**: Process 24h of data for 100 ISINs in <5 minutes wall-clock time
3. **Reliability**: Handle 404s (expired data) and transient network errors gracefully, zero crashes
4. **Correctness**: 1h/1d aggregations match manual OHLC calculation (verified via spot checks)
5. **Usability**: Clear CLI commands, informative error messages, comprehensive logging

## Consequences

### Positive

- **Enables European market analysis** without real-time licensing costs
- **Reuses proven infrastructure** (partition storage, recovery, config) reducing implementation risk
- **Preserves raw data** for re-aggregation and detailed research use cases
- **Compatible with existing workflows** via standard OHLCV schema for aggregated data
- **Addresses Yahoo split tracking gap** benefiting all data sources

### Negative

- **Higher storage footprint** due to dual raw + aggregated persistence (~2x vs aggregated-only)
- **Operational complexity** from managing two separate CLIs and data sources
- **Time-sensitive backfilling** requires monitoring to catch data before 24h expiry
- **Manual split handling** for Xetra initially (until corporate actions feed integrated)

### Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Deutsche Börse changes feed format | High | Version feed format in fixtures, add schema validation, monitor for breaking changes |
| 24h retention window too short | Medium | Implement alerting for fetch failures, document recovery procedures |
| ISIN mapping incomplete/stale | Medium | Provide refresh CLI command, log warnings for unmapped ISINs |
| Storage costs from raw data | Low | Implement retention policies, document DuckDB migration path |

## Work Log

| Date | Milestone | Status | Notes |
|------|-----------|--------|-------|
| 2025-10-12 | ADR drafted | ✅ Complete | Initial proposal |
| 2025-11-02 | Implementation plan complete | ✅ Complete | Detailed architecture doc created |
| 2025-11-03 | XetraParser implemented | ✅ Complete | 22-field schema, 100% test coverage |
| 2025-11-03 | XetraFetcher basic implementation | ✅ Complete | Download, decompress, parse JSON |
| 2025-11-04 | Rate limiting empirical validation | ✅ Complete | R²=0.97 linear model, 0.6s/30/35s config |
| 2025-11-04 | Trading hours filtering | ✅ Complete | 08:30-18:00 CET/CEST, 56.5% file reduction |
| 2025-11-05 | XetraService orchestration | ✅ Complete | Incremental fetch, gap detection, consolidation |
| 2025-11-05 | Integration tests | ✅ Complete | End-to-end fetch→parse→store workflow |
| 2025-11-17 | CLI implementation | ✅ Complete | 5 commands, 100% coverage, 22 tests |
| 2025-11-17 | Phase 1 complete | ✅ Complete | **Foundation operational, production quality** |
| TBD | Phase 2: OHLCV aggregation | 🚧 In Progress | XetraAggregator service pending |
| TBD | Phase 2: Multi-interval storage | 🔲 Pending | stocks_1m/, stocks_1h/, stocks_1d/ datasets |
| TBD | Phase 3: Split tracking | 🔲 Pending | Dependent on Phase 2 |
| TBD | Phase 4: Production hardening | 🔲 Pending | Multi-venue, ISIN mapping, documentation |

## Implementation Summary (2025-11-17)

### Completed Components

| Component | Lines | Coverage | Tests | Status |
|-----------|-------|----------|-------|--------|
| XetraFetcher | 416 | 97% | 37 | ✅ Production |
| XetraParser | 266 | 100% | 23 | ✅ Production |
| XetraService | 843 | 80% | 18 | ✅ Production |
| xetra-parqed CLI | 418 | 100% | 22 | ✅ Production |
| Integration Tests | - | - | 4 | ✅ Complete |
| Live API Tests | - | - | 7 | ✅ Complete |
| Consolidation Tests | - | - | 16 | ✅ Complete |
| **Total** | **1,943** | **91%** | **129** | **Phase 1 Complete** |

### Key Achievements

1. **Empirical Rate Limiting**: Discovered and validated Deutsche Börse rate limits through systematic testing (810 files, R²=0.97)
2. **Trading Hours Optimization**: Timezone-aware filtering reduces unnecessary downloads by 56.5%
3. **Test Quality**: 100% coverage on CLI and parser, exceeding industry standards
4. **Production Readiness**: Error handling, retry logic, corruption recovery all operational
5. **Monthly Consolidation**: Automatic daily→monthly aggregation for space efficiency

### Remaining Work for MVP

**Critical Path to Phase 2 Completion:**

1. **XetraAggregator Service** (~200 lines estimated)
   - Load daily raw trade parquet files
   - Resample to 1m/1h/1d intervals with OHLC logic
   - Preserve trade counts, venue metadata, handle gaps
   - Unit tests for resampling correctness

2. **Aggregated Storage** (~100 lines estimated)
   - Implement `stocks_1m/`, `stocks_1h/`, `stocks_1d/` dataset support
   - Ticker/ISIN partitioning: `venue=VENUE/ticker={ticker}/year=YYYY/month=MM/`
   - Schema compatibility with Yahoo Finance OHLCV structure

3. **CLI Integration** (~50 lines estimated)
   - Add `--intervals` flag to `fetch-trades` command
   - Optional aggregation step after raw trade storage
   - Progress reporting for aggregation operations

4. **Integration Tests** (~150 lines estimated)
   - End-to-end: fetch→store trades→aggregate→verify OHLCV
   - Row count consistency validation
   - OHLC math correctness (spot checks against manual calculations)

**Estimated Effort**: 2-3 weeks for experienced developer familiar with pandas resampling and the existing codebase.

## References

- [Xetra Implementation Plan](/docs/xetra_implementation_plan.md) - Detailed technical specification
- [ADR: Partition-Aware Storage](/docs/adr/2025-10-12-partition-aware-storage.md) - Storage backend architecture
- [Deutsche Börse Data Feed Documentation](https://www.deutsche-boerse.com/dbg-en/products-services/market-data-and-analytics) - Official data specification
- [yfinance Documentation](https://github.com/ranaroussi/yfinance) - Yahoo Finance API reference
