# Xetra Delayed Data Integration - Documentation Hub

**Last Updated**: 2025-11-02  
**Status**: Planning & Architecture Complete, Implementation Pending  
**Lead Decision**: Deutsche Börse CSV for ISIN mapping (**$6K/year cost savings**)

---

## 📚 Documentation Map

### Start Here

1. **[ADR: Xetra Delayed Data Integration](../adr/2025-10-12-xetra-delayed-data.md)** ⭐ **MAIN REFERENCE**
   - **Purpose**: Architectural Decision Record with all 8 critical decisions
   - **Audience**: Technical leads, architects, future implementers
   - **Key Sections**: 
     - AD-1 through AD-8: Core architectural decisions
     - Implementation roadmap (4 phases, 8 weeks)
     - Success metrics and risk mitigation
   - **Start here if**: You need to understand the "why" behind design choices

2. **[Implementation Checklist](IMPLEMENTATION_CHECKLIST.md)** ⭐ **FOR IMPLEMENTATION**
   - **Purpose**: Step-by-step guide for future agents/developers
   - **Audience**: Implementers, coding agents, developers
   - **Key Sections**:
     - Phase-by-phase task breakdown with acceptance criteria
     - File creation checklist with exact paths
     - Testing requirements per phase
     - Validation commands
   - **Start here if**: You're ready to write code

### Deep Dives

3. **[Implementation Addendum](xetra_implementation_addendum.md)** (12,000 words)
   - **Purpose**: Comprehensive technical specification and code examples
   - **Covers**:
     - Section 1: Actual schema analysis (23 fields → 10 core + 8 extended)
     - Section 2: API file discovery (JSON response format, prefix enumeration)
     - Section 3: ISIN mapping with Deutsche Börse CSV (complete implementation)
     - Section 4: Updated schema recommendations
     - Section 5: CLI design
     - Sections 6-9: Roadmap, decisions, next steps, field reference
   - **Best for**: Detailed implementation questions, code patterns, schema choices

4. **[Key Findings Summary](xetra_key_findings_summary.md)** (4,000 words)
   - **Purpose**: Executive summary and quick reference
   - **Covers**:
     - 10 key findings from API/data analysis
     - 4-phase roadmap overview
     - 6 critical decisions (all ✅ DECIDED)
     - Risk mitigation strategies
     - Testing strategy (33+ tests planned)
   - **Best for**: Quick lookups, decision rationale, roadmap overview

5. **[Questions Answered](xetra_questions_answered.md)** (6,000 words)
   - **Purpose**: Direct answers to specific technical questions
   - **Covers**:
     - Question 1: Accurate schema (23 fields with examples)
     - Question 2: API file listing (JSON support confirmed)
     - Question 3: ISIN→ticker mapping (Deutsche Börse CSV strategy)
   - **Best for**: Specific questions about schema, API, mapping

### ISIN Mapping Strategy (NEW - 2025-11-02)

6. **[ISIN Mapping Strategy](xetra_isin_mapping_strategy.md)** (12,000 words)
   - **Purpose**: Complete technical specification for Deutsche Börse CSV integration
   - **Covers**:
     - Web scraper implementation (BeautifulSoup)
     - CSV parser (semicolon-delimited, 130+ columns)
     - Parquet cache schema (`isin_mapping.parquet`)
     - Merge logic (new/existing/delisted ISINs)
     - Runtime lookup service (<1ms)
     - CLI commands (`update-isin-mapping`, `map-isin`)
     - Testing strategy (33 tests)
   - **Best for**: Implementing ISIN mapping subsystem

7. **[ISIN Mapping Decision](xetra_isin_mapping_decision.md)** (5,000 words)
   - **Purpose**: Cost-benefit analysis and decision rationale
   - **Covers**:
     - Deutsche Börse CSV vs OpenFIGI comparison
     - Cost analysis (**$6,000/year savings**)
     - Before/after architecture diagrams
     - Implementation checklist
     - Risk assessment
     - Approval status
   - **Best for**: Understanding why CSV approach was chosen

8. **[ISIN Mapping Comparison](xetra_isin_mapping_comparison.md)** (4,000 words)
   - **Purpose**: Side-by-side comparison and migration path
   - **Covers**:
     - 10-dimension comparison table (CSV wins 7/10)
     - Before/after data flows
     - Decision matrix with weighted scoring (CSV: 9.25 vs OpenFIGI: 5.4)
     - 3-phase migration plan
     - Cost-benefit analysis
   - **Best for**: Comparing approaches, justifying decision to stakeholders

---

## 🎯 Quick Navigation by Role

### If you're a **Technical Lead / Architect**:
1. Read [ADR](../adr/2025-10-12-xetra-delayed-data.md) for decisions and rationale
2. Review [ISIN Mapping Decision](xetra_isin_mapping_decision.md) for cost analysis
3. Check [Key Findings Summary](xetra_key_findings_summary.md) for roadmap

### If you're an **Implementer / Developer**:
1. Start with [Implementation Checklist](IMPLEMENTATION_CHECKLIST.md)
2. Use [Implementation Addendum](xetra_implementation_addendum.md) for code patterns
3. Reference [ISIN Mapping Strategy](xetra_isin_mapping_strategy.md) for ISIN subsystem

### If you're a **Future AI Agent**:
1. **PRIMARY**: [Implementation Checklist](IMPLEMENTATION_CHECKLIST.md) - Your task list
2. **REFERENCE**: [Implementation Addendum](xetra_implementation_addendum.md) - Code examples
3. **VALIDATION**: [ADR](../adr/2025-10-12-xetra-delayed-data.md) - Design constraints

### If you're a **Stakeholder / PM**:
1. Read [Key Findings Summary](xetra_key_findings_summary.md) - Executive overview
2. Review [ISIN Mapping Decision](xetra_isin_mapping_decision.md) - Cost justification ($6K/year savings)
3. Check [ADR Success Metrics](../adr/2025-10-12-xetra-delayed-data.md#success-metrics) - Acceptance criteria

---

## 📊 Current Status

### ✅ Completed (Planning Phase)
- [x] Deutsche Börse API analysis (JSON support confirmed)
- [x] Actual schema analysis (23 fields → 10 core recommended)
- [x] ISIN mapping strategy (Deutsche Börse CSV approved)
- [x] Architectural Decision Record (8 decisions documented)
- [x] 4-phase implementation roadmap (8 weeks estimated)
- [x] Cost-benefit analysis ($6K/year savings vs OpenFIGI)
- [x] Testing strategy (33+ tests planned)

### 🔄 In Progress
- None (awaiting implementation start)

### ⏳ Pending (Implementation Phase)
- [ ] Phase 1: Raw Trade Storage (2 weeks)
- [ ] Phase 2: ISIN Mapping & Aggregation (3 weeks)
- [ ] Phase 3: Multi-Venue Support (2 weeks)
- [ ] Phase 4: Production Hardening (2 weeks)

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│ XetraParqed (Separate Primary Class)                        │
├─────────────────────────────────────────────────────────────┤
│ • XetraFetcher        → Download + decompress JSON.gz       │
│ • XetraAggregator     → Raw trades → OHLCV (1m/1h/1d)       │
│ • ISINMapper          → Deutsche Börse CSV → <1ms lookups   │
│ • XetraScheduler      → Orchestrate fetch→aggregate flow    │
│ • PartitionedStorage  → Hive-style partitions (reused)      │
└─────────────────────────────────────────────────────────────┘

Data Flow:
  Deutsche Börse API → XetraFetcher → Raw Trades (time-partitioned landing)
                                           ↓
  Deutsche Börse CSV → ISINMapper → Ticker Lookup (<1ms)
                                           ↓
  Raw Trades → XetraAggregator → OHLCV (ticker= or isin= partitions)
```

---

## 🗂️ Storage Layout

```
data/
  de/                              # Market: Germany
    xetra/                         # Source: Xetra
      trades/                      # Raw per-trade data (venue-first time-partitioned)
        venue=DETR/                # Xetra venue (low cardinality first)
          year=2025/month=11/day=01/
            trades.parquet         # All Xetra ISINs for DETR on 2025-11-01
          year=2025/month=11/day=02/
            trades.parquet         # All Xetra ISINs for DETR on 2025-11-02
        venue=DFRA/                # Frankfurt venue
          year=2025/month=11/day=01/
            trades.parquet         # All Frankfurt ISINs on 2025-11-01
      stocks_1m/                   # Aggregated 1-minute OHLCV (venue-first dual-partitioned)
        venue=DETR/                # Venue partitioning for DuckDB optimization
          ticker=dbk/              # Mapped ISINs use ticker partition
            year=2025/month=11/data.parquet
          ticker=sie/
            year=2025/month=11/data.parquet
          isin=LU1234567890/       # Unmapped ISINs use ISIN partition
            year=2025/month=11/data.parquet
      stocks_1h/                   # Aggregated 1-hour OHLCV (same venue-first dual-partition)
        venue=DETR/
          ticker=dbk/year=2025/month=11/data.parquet
          isin=LU1234567890/year=2025/month=11/data.parquet
      stocks_1d/                   # Aggregated 1-day OHLCV (same venue-first dual-partition)
        venue=DETR/
          ticker=dbk/year=2025/month=11/data.parquet
          isin=LU1234567890/year=2025/month=11/data.parquet
    tradegate/                     # Source: Tradegate (Phase 3)
      trades/
        venue=DETG/
          year=2025/month=11/day=01/trades.parquet
      stocks_1m/
        venue=DETG/
          ticker=dbk/year=2025/month=11/data.parquet

  reference/                       # Reference data (not time-series)
    isin_mapping.parquet           # ISIN→ticker cache (4,280+ instruments)
      # Schema: isin, ticker, name, currency, status, first_seen, last_seen
```

---

## 💡 Key Design Decisions

1. **Separate XetraParqed Class** (not extension of YFParqed)
   - **Why**: Different operational characteristics (no rate limiting, 24h retention window)
   - **Benefit**: Clean separation of concerns, easier testing

2. **Venue-First Time-Based Landing Zone + Dual-Schema Storage** (raw venue+time-partitioned + aggregated venue+dual-partitioned) ⭐
   - **Why**: 
     - **Low-cardinality-first**: 5 venues before 4,280 tickers follows Hive partitioning best practice
     - **DuckDB optimization**: `WHERE venue = 'DETR'` uses partition pruning on first partition level
     - **Source alignment**: Deutsche Börse serves one file per venue/date → storage mirrors this 1:1
     - **Schema consistency**: Raw trades and aggregated OHLCV both use venue-first ordering
   - **Benefit**: 100x fewer writes (1 per venue/date vs 100+ per date), lower corruption risk, simpler update logic, efficient venue-specific queries
   - **Trade-off**: "All trades for ISIN across all venues" queries scan multiple venue/date partitions (rare use case)

3. **Deutsche Börse CSV for ISIN Mapping** ⭐
   - **Why**: FREE, authoritative, <1ms lookups, unlimited
   - **Cost Savings**: **$6,000/year** vs OpenFIGI
   - **Trade-off**: 0-24 hour lag for new IPOs (acceptable)

4. **Venue-First Dual-Partition Strategy** (`venue=VENUE/ticker=` for mapped, `venue=VENUE/isin=` for unmapped)
   - **Why**: Clear separation, preserve data for ISINs without ticker mapping, queryable by ISIN, DuckDB partition pruning on venue
   - **Benefit**: No ambiguous `__UNMAPPED__` partition, enable backfill when mappings discovered later, efficient venue-specific queries

5. **10-Field Core Schema** (18 with `--extended-metadata`)
   - **Why**: 95% of users don't need MiFID II compliance fields
   - **Savings**: 44% storage reduction

6. **No Rate Limiting for Xetra**
   - **Why**: Static file downloads (not API with quotas)
   - **Benefit**: Simpler code, faster downloads

---

## 🧪 Testing Strategy

### Test Coverage Plan (33+ tests)

**Unit Tests** (25 tests):
- XetraFetcher: URL generation, decompression, JSON parsing
- XetraAggregator: OHLC logic, gap handling
- ISINMapper: Cache loading, lookups, lifecycle tracking
- CSV Scraper: URL extraction, HTTP errors
- CSV Parser: Semicolon delimiter, normalization, validation

**Integration Tests** (8 tests):
- End-to-end: Fetch → parse → store → aggregate
- Multi-venue: DETR + DFRA isolation
- ISIN mapping: Scrape → download → merge → cache
- CLI: All commands with real temp workspace

**Acceptance Criteria**:
- 100% test pass rate (match existing yf_parqed standard)
- Fetch + store 24h of data for 100 ISINs in <5 minutes
- ISIN cache: 4,280+ instruments with <1ms lookups
- OHLC aggregations match manual calculations (spot checks)

---

## 📋 Dependencies to Add

```toml
[project.dependencies]
# Already have: yfinance, pandas, pyarrow, typer, loguru, rich, httpx

# NEW for Xetra:
beautifulsoup4 = "^4.12.0"  # HTML parsing for CSV URL extraction
lxml = "^5.0.0"             # Fast HTML parser backend for BS4
```

---

## 🚨 Critical Reminders for Future Agents

### MUST DO Before Starting
1. ✅ Run `uv sync` to restore environment
2. ✅ Read [Implementation Checklist](IMPLEMENTATION_CHECKLIST.md) in full
3. ✅ Check all 183 existing tests pass: `uv run pytest`
4. ✅ Review [ADR Section AD-1 through AD-8](../adr/2025-10-12-xetra-delayed-data.md)

### MUST DO During Implementation
1. ✅ Follow test-driven development (write failing test first)
2. ✅ Run tests after each file creation: `uv run pytest`
3. ✅ Maintain 100% test pass rate (never commit failing tests)
4. ✅ Update [Implementation Checklist](IMPLEMENTATION_CHECKLIST.md) as you complete tasks

### MUST DO Before Committing
1. ✅ All 183+ tests passing: `uv run pytest`
2. ✅ Linting clean: `uv run ruff check . --fix && uv run ruff format .`
3. ✅ Documentation updated (if public API changed)
4. ✅ [Implementation Checklist](IMPLEMENTATION_CHECKLIST.md) updated with ✅

---

## 🔗 External References

- [Deutsche Börse Market Data API](https://mfs.deutsche-boerse.com/api/)
- [Deutsche Börse Instruments Page](https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments)
- [Xetra Trading Venue](https://www.xetra.com/)
- [yfinance Documentation](https://github.com/ranaroussi/yfinance)

---

## 📝 Version History

| Date       | Change                                           | Author/Agent |
|------------|--------------------------------------------------|--------------|
| 2025-10-12 | Initial ADR created                              | Agent        |
| 2025-11-02 | Implementation plan completed                    | Agent        |
| 2025-11-02 | ISIN mapping strategy changed to Deutsche Börse CSV | Agent + User |
| 2025-11-02 | Documentation reorganized into docs/xetra/       | Agent        |

---

## 🆘 Getting Help

**If you're stuck on**:
- **Architecture decisions** → Re-read [ADR](../adr/2025-10-12-xetra-delayed-data.md)
- **Implementation details** → Check [Implementation Addendum](xetra_implementation_addendum.md)
- **ISIN mapping** → See [ISIN Mapping Strategy](xetra_isin_mapping_strategy.md)
- **Testing** → Review existing tests in `tests/` directory
- **Cost justification** → Show [ISIN Mapping Decision](xetra_isin_mapping_decision.md) to stakeholders

**Questions to ask yourself before proceeding**:
1. Have I read the Implementation Checklist for this phase?
2. Do I understand which service is responsible for this functionality?
3. Have I written a failing test before implementing?
4. Will this change break any of the 183 existing tests?
5. Am I following the established patterns from yf_parqed?

---

**Next Step**: Open [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md) and start Phase 1! 🚀
