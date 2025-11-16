# Xetra Implementation - Key Findings Summary

**Date**: 2025-11-02  
**Analysis**: Real Xetra data sample + Deutsche Börse API testing

---

## 1. ACTUAL SCHEMA - 23 Fields Discovered (Not 15!)

### Critical Discovery
Sample file `DETR-posttrade-2025-10-31T13_54.json` contains **23 fields** per trade, including 9 MiFID II regulatory fields not previously documented.

### Recommended Storage Strategy

**Default (10 fields - 95% use case)**:
```python
{
    "isin": "string",              # DE0007100000 
    "price": "float64",            # 56.20 (renamed from lastTrade)
    "volume": "float64",           # 159.00 (renamed from lastQty)
    "currency": "string",          # EUR
    "trade_time": "timestamp[ns]", # 2025-10-31T13:54:00.042457058Z
    "venue": "string",             # XETA, XETB
    "trans_id": "string",          # Unique 64-char transaction ID
    "tick_id": "int64",           # Sequential order
    "algo_trade": "bool",          # H→True, -→False
    "source": "string",            # ETR
}
```

**Extended (18 fields with `--extended-metadata` flag)**:
Add 8 more MiFID II compliance fields (market mechanism, trading mode, etc.)

**Drop (5 redundant fields)**:
- `instrumentId` - duplicate of ISIN
- `messageId` - always "posttrade"
- `tickActionIndicator`, `instrumentIdCode` - always "I"

---

## 2. FILE DISCOVERY API - Critical Gap Found

### The Problem
Deutsche Börse API returns **suffixes only**:
```json
{
  "CurrentFiles": [
    "-2025-10-31T13_54.json.gz",   # Missing venue/type prefix!
    "-2025-10-31T13_53.json.gz"
  ]
}
```

### The Solution
Must enumerate all **venue×type** combinations:

**Venue codes** (4): `DETR`, `DFRA`, `DGAT`, `DEUR`  
**Type codes** (2): `posttrade`, `pretrade`  
**Total combinations**: 8 prefix patterns

**Example full URL construction**:
```
https://mfs.deutsche-boerse.com/api/DETR-posttrade-2025-10-31T13_54.json.gz
                                    └──┬──┘ └───┬───┘ └────────┬────────┘
                                    venue   type        API suffix
```

### API Testing Results
✅ **JSON response works**: Setting `Accept: application/json` header returns pure JSON (not HTML)  
✅ **No authentication needed**: Anonymous access confirmed  
✅ **1321 files available**: ~22 hours of minute-level data (1440 min/day × venue combinations)

---

## 3. ISIN → TICKER MAPPING - Solved! ✅

### Problem Statement

Xetra data contains **only ISINs**, not ticker symbols:
- ISIN `DE0005140008` = Xetra mnemonic `DBK` (Deutsche Bank)
- ISIN `DE0007236101` = Xetra mnemonic `SIE` (Siemens)
- ISIN `AT000000STR1` = Xetra mnemonic `XD4` (Strabag, Austrian)

### **APPROVED** Solution: Deutsche Börse Official CSV

**✅ DECISION**: Use Deutsche Börse's free "All Tradable Instruments" CSV

**Why This Wins**:
- **FREE** (vs $500/month for OpenFIGI production tier = **$6,000/year savings**)
- **Authoritative**: Official exchange source (not third-party)
- **Fast**: <1ms local cache lookups (vs 50-200ms API calls)
- **Unlimited**: No rate limits
- **Reliable**: 100% uptime via local Parquet cache

**Data Source**:
- **URL**: https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments
- **Format**: Semicolon-delimited CSV, 130+ columns
- **Coverage**: 4,280+ active XETRA instruments
- **Update**: Daily at ~11:54 PM CET

**Integration Approach**:
1. Web scraper extracts dynamic CSV download URL
2. Daily cron job: `xetra-parqed update-isin-mapping`
3. Parquet cache: `data/reference/isin_mapping.parquet`
4. Runtime lookups: In-memory dict (<1ms)
5. Unknown ISINs → `__UNMAPPED__` partition

**Schema** (`isin_mapping.parquet`):
```python
{
    "isin": "DE0005140008",
    "ticker": "dbk",              # Lowercase Xetra mnemonic
    "name": "DEUTSCHE BANK AG",
    "currency": "EUR",
    "status": "active",
    "first_seen": "2025-10-12",
    "last_seen": "2025-11-02",
    "source": "deutsche_boerse_csv"
}
```

**Fallback for Unmapped ISINs**:
```
data/de/xetra/stocks_1m/ticker=__UNMAPPED__/
  ├── ISIN column preserved for later resolution
  └── Can backfill when mapping discovered
```

**Trade-offs Accepted**:
- 0-24 hour lag for new IPOs (rare, handled via `__UNMAPPED__`)
- Web scraping complexity (mitigated with HTML fixtures + monitoring)

**Related Docs**:
- `/docs/xetra_isin_mapping_strategy.md` - Full implementation spec
- `/docs/xetra_isin_mapping_decision.md` - Cost-benefit analysis
- `/docs/xetra_isin_mapping_comparison.md` - Before/after comparison

---

## 4. CLI DESIGN UPDATES

### New Download Flags
```bash
xetra-parqed download \
  --venue DETR \                  # DETR, DFRA, DGAT, DEUR
  --type posttrade \              # posttrade, pretrade
  --extended-metadata \           # Store 18 fields vs 10
  --last-hours 24 \               # Download last 24 hours
  --skip-existing                 # Idempotent downloads
```

### New Mapping Commands

```bash
# Daily automated ISIN mapping update
xetra-parqed update-isin-mapping

# Force refresh (ignore cache age)
xetra-parqed update-isin-mapping --force

# Lookup single ISIN
xetra-parqed map-isin DE0005140008

# Import custom ISIN→Ticker mapping (override)
xetra-parqed import-mapping isin_ticker_map.csv

# Show mapping stats
xetra-parqed mapping-stats
# Output: Cached: 4,280 | Unmapped: 23 | Active: 4,257 | Inactive: 46
```

---

## 5. IMPLEMENTATION ROADMAP UPDATES

### Phase 1: Raw Trade Storage (2 weeks)
- ✅ Download DETR-posttrade files via API enumeration
- ✅ Parse JSON.gz to **core 10-field schema** (default)
- ✅ Store partitioned by ISIN: `data/de/xetra/trades/isin=XXX/`
- 🆕 Implement `--extended-metadata` flag for 18-field schema
- ✅ Basic CLI: `download`, `list-venues`

### Phase 2: ISIN Mapping & Aggregation (3 weeks)

- 🆕 Implement Deutsche Börse CSV scraper (BeautifulSoup + httpx)
- 🆕 Build persistent cache: `isin_mapping.parquet`
- 🆕 Daily cron job: `xetra-parqed update-isin-mapping`
- ✅ Aggregate raw trades to 1m OHLCV
- 🆕 Partition by `ticker=XXX` (mapped) or `ticker=__UNMAPPED__` (unmapped)
- 🆕 Add CLI commands: `map-isin`, `update-isin-mapping`, `aggregate`, `mapping-stats`

### Phase 3: Multi-Venue Support (2 weeks)

- ✅ Extend to DFRA, DGAT, DEUR venues
- ✅ Add `pretrade` data type support
- 🆕 Venue-specific ISIN mappings (some ISINs differ by venue)
- ✅ Update partition structure: `data/{venue}/xetra/...`

### Phase 4: Production Hardening (2 weeks)

- 🆕 Track ISIN lifecycle (new/delisted) with timestamps
- 🆕 Daily batch job: download new files + update ISIN mappings
- 🆕 Automated backfill for newly mapped ISINs
- 🆕 Multi-exchange CSV support (Frankfurt XFRA, Stuttgart XSTU)
- ✅ Monitoring, alerting, mapping validation

---

## 6. CRITICAL DECISIONS NEEDED

### Decision 1: Default Schema

**Question**: Store 10 fields by default or 18?  
**✅ DECIDED**: 10 fields (core) by default, `--extended-metadata` for full schema  
**Rationale**: 95% of users don't need MiFID II compliance fields, saves 44% storage

### Decision 2: Unmapped ISIN Handling

**Question**: Skip aggregation for unmapped ISINs or store separately?  
**✅ DECIDED**: Store in `__UNMAPPED__` partition with ISIN column  
**Rationale**: Allows backfill when mappings discovered later, preserves data

### Decision 3: ~~OpenFIGI Tier~~ → ISIN Mapping Strategy

**Question**: ~~Free tier (25 req/sec) sufficient for MVP?~~  
**✅ DECIDED**: Use Deutsche Börse official CSV instead (FREE, unlimited)  
**Rationale**: **$6,000/year cost savings**, authoritative source, <1ms lookups, no rate limits  
**Impact**: OpenFIGI deferred to Phase 3 as optional fallback for non-German ISINs

### Decision 4: Venue Scope

**Question**: Support all 4 venues (DETR, DFRA, DGAT, DEUR) from start?  
**✅ DECIDED**: DETR only for MVP (Xetra = largest liquidity)  
**Rationale**: Reduces testing surface, add others in Phase 3

### Decision 5: API File Enumeration

**Question**: Probe all 8 venue×type combinations or enumerate discovered patterns?  
**✅ DECIDED**: Enumerate known 8 patterns (cached config), probe on failure  
**Rationale**: Faster, handles 99% of cases, graceful fallback for new venues

### Decision 6: Ticker ~~Suffix~~ Convention

**Question**: ~~Always append `.DE` for German stocks?~~  
**✅ DECIDED**: Use Deutsche Börse CSV `Mnemonic` field directly (lowercase)  
**Rationale**: Xetra mnemonics are unique (`dbk`, `sie`, `xd4`), no suffix needed

---

## 7. RISK MITIGATION

### Risk 1: Unmapped ISINs

**Impact**: Cannot aggregate to ticker-partitioned OHLCV  
**Mitigation**: Store in `__UNMAPPED__` partition, flag for manual review  
**Workaround**: Users can provide custom ISIN→Ticker CSV: `import-mapping custom.csv`

### Risk 2: ~~OpenFIGI Rate Limits~~ → CSV Scraper Fragility

**Impact**: Deutsche Börse webpage structure changes break CSV URL extraction  
**Mitigation**: Version-controlled HTML fixtures for regression tests, monitoring alerts after 3 failures  
**Escalation**: Manual CSV download + local import command (`import-mapping --from-csv`)

### Risk 3: Deutsche Börse API Changes

**Impact**: URL pattern or JSON schema changes break downloads  
**Mitigation**: Version API client, add schema validation on parse  
**Monitoring**: Daily health check: `download --last-hours 1 --dry-run`

### Risk 4: 24-Hour Data Retention

**Impact**: Missing backfill window = permanent data loss  
**Mitigation**: Run download every 12 hours (50% safety margin)  
**Alerting**: Email if last successful download > 18 hours ago

### Risk 5: Storage Growth (Per-Trade Data)

**Impact**: Raw trades ~100x larger than aggregated OHLCV  
**Mitigation**: Option 1 - Compress older partitions (gzip parquet)  
**Mitigation**: Option 2 - Retention policy (delete trades >90 days, keep aggregated)  
**Config**: `retention_days: {trades: 90, aggregated: 3650}`

---

## 8. UPDATED TECHNICAL DEBT

### New Items

1. **Deutsche Börse CSV Scraper** - Web scraping fragility, HTML fixture maintenance
2. **ISIN Mapping Cache** - Lifecycle tracking, conflict resolution, delistings
3. **Venue Enumeration** - Hardcoded list in config, needs update mechanism
4. **Schema Evolution** - 10-field vs 18-field migration path
5. **Unmapped Partition** - Backfill automation when mappings discovered

### Existing Items (from Yahoo integration)

1. Corporate action tracking (splits/dividends) - still Phase 4+
2. DuckDB query layer - deferred to future enhancement
3. Multi-interval aggregation consistency checks

---

## 9. TESTING STRATEGY ADDITIONS

### Unit Tests (New)

- `test_xetra_api_discovery.py` - API parsing, prefix enumeration
- `test_xetra_schema_parsing.py` - 10-field vs 18-field schema validation
- `test_isin_mapping_scraper.py` - CSV URL extraction, HTTP errors
- `test_isin_mapping_parser.py` - CSV parsing, normalization, validation
- `test_isin_mapping_merger.py` - New/existing/delisted ISIN handling
- `test_isin_mapper.py` - Cache loading, lookups, reloads

### Integration Tests (New)

- `test_xetra_download_end_to_end.py` - Real API call, file download, parse
- `test_xetra_aggregation.py` - Raw trades → OHLCV with ISIN mapping
- `test_unmapped_partition.py` - Store/retrieve from `__UNMAPPED__`
- `test_isin_mapping_e2e.py` - Scrape → download → parse → merge → write
- `test_isin_mapping_cli.py` - CLI flags (`--force`, `--dry-run`)

### Manual Test Plan

1. Download 1 hour of DETR-posttrade data
2. Verify 10-field schema parquet output
3. Run `xetra-parqed update-isin-mapping` (scrape Deutsche Börse CSV)
4. Verify 4,280+ ISINs cached in `isin_mapping.parquet`
5. Aggregate mapped ISINs to 1m OHLCV
6. Verify ticker partitions created (e.g., `ticker=dbk/`, `ticker=sie/`)
7. Verify unmapped ISINs in `__UNMAPPED__` partition
8. Import custom ISIN mapping, re-aggregate

---

## 10. DOCUMENTATION REQUIREMENTS

### User Documentation (New)

- **Xetra Quick Start Guide** - Download first hour of data
- **ISIN Mapping Guide** - Deutsche Börse CSV setup, cache management, troubleshooting
- **Schema Reference** - 10-field vs 18-field comparison table
- **CLI Command Reference** - All `xetra-parqed` subcommands

### Developer Documentation (New)
- **API Client Design** - File enumeration algorithm
- **Mapping Architecture** - Cache format, OpenFIGI integration
- **Partition Strategy** - `__UNMAPPED__` handling, backfill process
- **Testing Fixtures** - Sample JSON.gz files for unit tests

---

## 11. NEXT IMMEDIATE ACTIONS (Priority Order)

1. ✅ **COMPLETED**: Analyze real Xetra data sample → 23 fields discovered
2. ✅ **COMPLETED**: Test Deutsche Börse API → JSON response confirmed
3. 🔄 **IN PROGRESS**: Update ADR with new architectural decisions (AD-8, AD-9)
4. ⏭️ **NEXT**: Create POC for OpenFIGI integration (10 ISINs)
5. ⏭️ Implement API file enumeration (venue×type prefix matrix)
6. ⏭️ Define ISIN cache schema (Parquet: isin, ticker, exchange, last_updated)
7. ⏭️ Validate 10-field schema with sample aggregation
8. ⏭️ Implement CLI stubs: `map-isin`, `mapping-stats`, `download --venue`

---

## APPENDIX: Field Mapping Cheat Sheet

| Xetra JSON Field | Stored Field | Type | Default Schema |
|------------------|--------------|------|----------------|
| `isin` | `isin` | string | ✅ Core |
| `lastTrade` | `price` | float64 | ✅ Core |
| `lastQty` | `volume` | float64 | ✅ Core |
| `currency` | `currency` | string | ✅ Core |
| `lastTradeTime` | `trade_time` | timestamp[ns] | ✅ Core |
| `executionVenueId` | `venue` | string | ✅ Core |
| `transIdCode` | `trans_id` | string | ✅ Core |
| `tickId` | `tick_id` | int64 | ✅ Core |
| `mmtAlgoInd` | `algo_trade` | bool | ✅ Core (H→True) |
| `sourceName` | `source` | string | ✅ Core |
| `distributionDateTime` | `distribution_time` | timestamp[ns] | 🔧 Extended |
| `quotationType` | `quote_type` | int8 | 🔧 Extended |
| `mmtMarketMechanism` | `market_mechanism` | string | 🔧 Extended |
| `mmtTradingMode` | `trading_mode` | string | 🔧 Extended |
| `mmtNegotTransPretrdWaivInd` | `negotiated_flag` | string | 🔧 Extended |
| `mmtModificationInd` | `modification_flag` | string | 🔧 Extended |
| `mmtBenchmarkRefprcInd` | `benchmark_flag` | string | 🔧 Extended |
| `mmtPubModeDefReason` | `pub_deferral` | string | 🔧 Extended |
| `instrumentId` | *dropped* | - | ❌ Redundant |
| `messageId` | *dropped* | - | ❌ Constant |
| `tickActionIndicator` | *dropped* | - | ❌ Constant |
| `instrumentIdCode` | *dropped* | - | ❌ Constant |

---

**Document Status**: Ready for ADR update and Phase 1 implementation  
**Contact**: See `docs/xetra_implementation_addendum.md` for full technical details
