# Xetra Integration - Architecture Summary

## Executive Summary

Comprehensive architectural design complete for integrating Deutsche Börse Xetra delayed trade data into yf_parqed. Separate `XetraParqed` class will handle European market data while reusing proven infrastructure (partition storage, recovery, config).

## Key Architectural Decisions

### 1. Separate Primary Class: `XetraParqed`
- **Why**: Xetra has fundamentally different operational characteristics (no rate limiting, 24h retention window, different error semantics)
- **Shared**: ConfigService, PartitionedStorageBackend, parquet_recovery
- **New**: XetraFetcher, XetraScheduler, XetraAggregator, XetraTickerRegistry

### 2. Dual-Schema Storage
- **Trade Schema**: Raw per-trade data, ISIN-partitioned, full metadata
- **Aggregated Schema**: Standard OHLCV format (Yahoo-compatible) + Xetra extensions (isin, trades, venue)
- **Why**: Preserve source fidelity for re-aggregation while maintaining drop-in compatibility with existing analytics

### 3. Partition Layout
```
data/
  de/xetra/trades/isin=DE0005810055/year=2024/month=11/data.parquet
  de/xetra/stocks_1m/ticker=DBK/year=2024/month=11/data.parquet
  de/tradegate/trades/isin=DE0005810055/year=2024/month=11/data.parquet
```
- Market→Source→Dataset hierarchy prevents accidental data mixing
- Separate `trades/` (ISIN key) and `stocks_*/` (ticker key) datasets

### 4. Corporate Action Tracking (NEW for both Yahoo and Xetra)
**Identified Gap**: Yahoo Finance returns split-adjusted prices but doesn't track when splits occurred

**Solution (Phased)**:
- **Phase 1**: Detect splits via `yfinance.Ticker.actions`, store in `tickers.json` metadata, log warnings
- **Phase 2**: Add `split_factor` column, store unadjusted + factors for Xetra
- **Phase 3**: Historical backfill tools, automated price discontinuity detection

### 5. Separate CLI: `xetra-parqed`
```bash
xetra-parqed initialize --venue xetra --intervals 1m,1h,1d
xetra-parqed update-data --backfill-hours 24
```
- Clearer operational responsibilities
- Both CLIs coexist in same workspace

### 6. No Rate Limiting for Xetra
- Static file downloads (not an API)
- Use retry logic for transient errors only
- Handle 404 as "data expired" (not an error)

### 7. Schema-Agnostic Recovery
- Keep `parquet_recovery.py` generic
- Each primary class injects its own normalizer
- Extensibility for future data sources (LSE, TSE, etc.)

## Implementation Phases

| Phase | Duration | Goal | Key Deliverable |
|-------|----------|------|-----------------|
| **1: Foundation** | Weeks 1-2 | Core ingestion | Fetch & store 24h of 1m trades for 10 ISINs |
| **2: Aggregation** | Weeks 3-4 | Multi-interval | Generate 1h/1d from 1m data |
| **3: Split Tracking** | Weeks 5-6 | Corporate actions | Detect & log splits for Yahoo + Xetra |
| **4: Hardening** | Weeks 7-8 | Production-ready | Multi-venue, error handling, docs |
| **5: Advanced** | Future | Enhancements | DuckDB, real-time feed, cross-venue analysis |

## Schema Details

### Xetra Trade Schema (Raw Data)
```python
{
    "isin": string,              # DE0005810055
    "mnemonic": string,          # DBK
    "security_desc": string,     # DEUTSCHE BANK AG
    "currency": string,          # EUR
    "date": datetime64[ns],
    "start_price": float64,      # Open
    "max_price": float64,        # High
    "min_price": float64,        # Low
    "end_price": float64,        # Close
    "traded_volume": Int64,
    "number_of_trades": Int64,
    "venue": string,             # XETR, TGAT
    "sequence": Int64
}
```

### Xetra Aggregated Schema (Yahoo-Compatible)
```python
{
    "stock": string,        # DBK (for compatibility)
    "isin": string,         # DE0005810055 (Xetra-specific)
    "date": datetime64[ns],
    "open": float64,
    "high": float64,
    "low": float64,
    "close": float64,
    "volume": Int64,
    "trades": Int64,        # Xetra-specific
    "venue": string,        # Xetra-specific
    "sequence": Int64
}
```

## Service Architecture

```python
class XetraParqed:
    def __init__(
        self,
        my_path: Path = Path.cwd(),
        venues: Sequence[str] = ["xetra"],
        my_intervals: Sequence[str] = ["1m", "1h", "1d"],
        store_trades: bool = True,
    ):
        # Shared infrastructure
        self.config = ConfigService(my_path)
        self.partition_storage = PartitionedStorageBackend(...)
        
        # Xetra-specific
        self.fetcher = XetraFetcher(...)      # Download/decompress/parse
        self.ticker_registry = XetraTickerRegistry(...)  # ISIN mapping
        self.aggregator = XetraAggregator(...)  # 1m→1h/1d
        self.scheduler = XetraScheduler(...)  # Orchestration
```

## Testing Strategy

- **Unit Tests**: XetraFetcher (URL generation, parsing), XetraAggregator (OHLC logic)
- **Integration Tests**: End-to-end (fetch→store→aggregate), multi-venue
- **Fixtures**: `tests/fixtures/xetra_sample.json.gz`, `tests/fixtures/tradegate_sample.json.gz`

## Success Metrics

1. **Functional**: Fetch and store 24h of 1m data for 100 ISINs
2. **Performance**: Process 24h for 100 ISINs in <5 minutes
3. **Reliability**: Handle 404s and errors gracefully, zero crashes
4. **Correctness**: 1h/1d aggregations match manual calculations
5. **Usability**: Clear CLI, informative errors

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Feed format changes | Schema validation, monitor breaking changes |
| 24h retention too short | Alerting for fetch failures, recovery docs |
| Incomplete ISIN mapping | Refresh CLI command, log warnings |
| Storage costs | Retention policies, DuckDB migration path |

## Documents Created

1. **`/docs/xetra_implementation_plan.md`** (15 pages)
   - Detailed technical specification
   - Schema design rationale
   - Service architecture deep-dive
   - Phase-by-phase implementation roadmap
   - Testing strategy and fixtures
   - Open questions and decisions

2. **`/docs/adr/2025-10-12-xetra-delayed-data.md`** (Updated)
   - Business context and decision drivers
   - 7 architectural decisions with rationale
   - Implementation roadmap summary
   - Alternatives considered matrix
   - Success metrics and consequences
   - Work log tracking

## Next Steps

1. **Prioritization**: Stakeholder review of implementation plan
2. **Resource Allocation**: Assign developers for Phase 1 (Weeks 1-2)
3. **Fixture Creation**: Obtain sample Xetra data for test development
4. **ISIN Mapping**: Download German ticker lists for initial registry
5. **Kickoff**: Begin XetraFetcher service implementation

## Questions Resolved

✅ Need separate primary class? **Yes** - operational differences too significant  
✅ Need rate limiting? **No** - static file downloads, not API  
✅ Store raw trades? **Yes** - valuable for re-aggregation  
✅ How handle splits? **Phased** - detection first, factors later  
✅ Recovery module changes? **None** - use normalizer injection  
✅ Separate CLI? **Yes** - clearer operational boundaries  
✅ Compatible with existing data? **Yes** - shared OHLCV schema for aggregations
