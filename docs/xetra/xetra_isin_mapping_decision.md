# ISIN Mapping Decision: Deutsche Börse CSV vs OpenFIGI

**Date**: 2025-11-03  
**Status**: APPROVED FOR IMPLEMENTATION  
**Supersedes**: OpenFIGI API design in `xetra_implementation_addendum.md`

---

## Decision

✅ **Use Deutsche Börse "All Tradable Instruments" CSV as primary ISIN→ticker mapping source**

❌ **Do NOT use OpenFIGI API for Phase 1**

---

## Rationale

### Why Deutsche Börse CSV Wins

| Factor | Deutsche Börse CSV | OpenFIGI API |
|--------|-------------------|--------------|
| **Official** | ✅ Direct from exchange | ⚠️ Third-party (Bloomberg) |
| **Cost** | ✅ **FREE** | ❌ **$500/month** for production |
| **Coverage** | ✅ 4,280+ XETRA instruments | ✅ 100M+ global (overkill) |
| **Speed** | ✅ Local cache (<1ms) | ❌ Network (50-200ms) |
| **Limits** | ✅ Unlimited | ❌ 25 req/sec (free tier) |
| **Reliability** | ✅ Download + cache | ⚠️ API downtime possible |
| **Maintenance** | ⚠️ Scraper fragility | ✅ Stable API |

**Bottom line**: Deutsche Börse CSV is **authoritative, free, and fast** via local caching. The only tradeoff is web scraping complexity (acceptable given cost savings).

---

## What Changes

### Remove from Scope
- OpenFIGI API integration
- $500/month recurring cost
- Rate limiter for ISIN lookups
- Network dependency for mappings

### Add to Scope
- Web scraper for Deutsche Börse instruments page
- CSV parser (semicolon-delimited, 130+ columns)
- Parquet cache (`isin_mapping.parquet`) with lifecycle tracking
- Daily update CLI command (`xetra-parqed update-isin-mapping`)

---

## Implementation Summary

### Components

1. **Web Scraper**: Extract dynamic CSV download URL from Deutsche Börse webpage
2. **CSV Downloader**: Fetch CSV via `httpx` (4,280 instruments, ~1MB gzipped)
3. **Parser**: Extract ISIN, Mnemonic (ticker), Name, Currency, WKN from semicolon-delimited CSV
4. **Cache Manager**: Merge with existing `isin_mapping.parquet`, track new/delisted ISINs
5. **Lookup Service**: In-memory dict for runtime ISIN→ticker resolution (<1ms)

### Data Flow

```
Deutsche Börse Page → Scrape CSV URL → Download CSV → Parse → 
Merge with Cache → Write Parquet → Load into Memory → 
Runtime Lookups (<1ms)
```

### Storage Schema

**File**: `data/reference/isin_mapping.parquet`

| Column     | Type   | Example          | Purpose                    |
|------------|--------|------------------|----------------------------|
| isin       | Utf8   | DE0005140008     | Primary key                |
| ticker     | Utf8   | DBK              | XETRA mnemonic             |
| name       | Utf8   | DEUTSCHE BANK AG | Full instrument name       |
| currency   | Utf8   | EUR              | Trading currency           |
| wkn        | Utf8   | 514000           | German securities ID       |
| status     | Utf8   | active           | active \| inactive         |
| first_seen | Date   | 2025-10-12       | Date first discovered      |
| last_seen  | Date   | 2025-11-03       | Date last confirmed        |
| source     | Utf8   | deutsche_boerse_csv | Data source             |

### CLI Usage

```bash
# Daily automated update (cron job)
uv run xetra-parqed update-isin-mapping

# Force refresh (ignore cache age)
uv run xetra-parqed update-isin-mapping --force

# Dry run (preview changes)
uv run xetra-parqed update-isin-mapping --dry-run
```

---

## CSV Source Details

### Current URL (2025-11-03)
```
https://www.xetra.com/resource/blob/1528/28190824d27b9267af27bcd473d89c6d/data/t7-xetr-allTradableInstruments.csv
```

### URL Pattern (Dynamic Components)
```
https://www.xetra.com/resource/blob/{ID}/{HASH}/data/t7-xetr-allTradableInstruments.csv
                                      ^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                      Changes daily (requires scraping)
```

### Webpage to Scrape
```
https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/all-tradable-instruments
```

### CSV Format

**Delimiter**: Semicolon (`;`)

**Header** (skip 2 metadata rows):
```csv
Market:;XETR
Date Last Update:;03.11.2025
Product Status;Instrument Status;Instrument;ISIN;...;Mnemonic;...;Currency;...
```

**Relevant Columns**:
- Column 3: `ISIN` (e.g., `DE0005140008`)
- Column 7: `Mnemonic` (e.g., `DBK`)
- Column 2: `Instrument` (e.g., `DEUTSCHE BANK AG NA O.N.`)
- Column 122: `Currency` (e.g., `EUR`)

**Row Count**: ~4,280 active instruments

---

## Risk Mitigation

### Risk: Webpage structure changes → scraper breaks

**Mitigations**:
1. Version-controlled HTML fixtures for regression tests
2. Monitoring: Alert if scraper fails 3+ consecutive days
3. Fallback: Manual CSV download + local import command
4. Maintenance: Review scraper quarterly for breakage

### Risk: CSV schema changes → parser breaks

**Mitigations**:
1. Column position validation in tests
2. Assert expected columns exist on parse
3. Graceful degradation: Skip unknown columns
4. Logging: Warn if unexpected columns appear

### Risk: Daily updates miss intraday IPOs

**Mitigations**:
1. Accept 0-24 hour delay for new listings (rare events)
2. `__UNMAPPED__` partition catches unknown ISINs
3. Manual override: `xetra-parqed add-isin DE123456789 XYZ` command (future)

---

## Testing Strategy

### Unit Tests (4 files, ~25 tests)
- `test_isin_mapping_scraper.py`: URL extraction, HTTP errors, link patterns
- `test_isin_mapping_parser.py`: CSV parsing, filtering, normalization, validation
- `test_isin_mapping_merger.py`: New/existing/delisted ISIN handling, ticker changes
- `test_isin_mapper.py`: Cache loading, lookups, reloads

### Integration Tests (2 files, ~8 tests)
- `test_isin_mapping_e2e.py`: Full update flow (scrape → download → parse → merge → write)
- `test_isin_mapping_cli.py`: CLI flags (`--force`, `--dry-run`), cache age checks

### Regression Tests
- Scraper resilience: Archived HTML fixtures
- Schema stability: Assert Parquet schema matches spec

---

## Documentation Updates Required

1. **ADR** (`docs/adr/2025-10-12-xetra-delayed-data.md`):
   - Update AD-8: Change from OpenFIGI to Deutsche Börse CSV
   - Add rationale: Cost savings, speed, authoritativeness
   - Defer OpenFIGI to future multi-exchange enhancement

2. **Implementation Addendum** (`docs/xetra_implementation_addendum.md`):
   - Remove OpenFIGI integration code (~200 lines)
   - Add CSV scraper/parser code (~150 lines)
   - Update cost analysis: Remove $500/month API cost

3. **Key Findings** (`docs/xetra_key_findings_summary.md`):
   - Critical Decision #4: Replace OpenFIGI with Deutsche Börse CSV
   - Update Phase 2 roadmap: Add CSV scraper implementation tasks
   - Update risk section: Add scraper fragility mitigation

---

## Dependencies to Add

```toml
[project.dependencies]
# ... existing dependencies ...
httpx = "^0.27.0"          # HTTP client for CSV download
beautifulsoup4 = "^4.12.0" # HTML parsing for URL extraction
lxml = "^5.0.0"            # Fast HTML parser backend for BS4
```

---

## Future Enhancements

### Phase 2: Multi-Exchange Support
- Deutsche Börse also publishes CSVs for:
  - Frankfurt (XFRA): Same pattern, different URL
  - Stuttgart (XSTU): Same pattern, different URL
- Extend schema with `exchange` column
- Single unified `isin_mapping.parquet` for all German exchanges

### Phase 3: OpenFIGI Hybrid (Optional)
- For non-German ISINs (ADRs, international)
- Auto-fallback to OpenFIGI on cache miss
- Cache OpenFIGI results locally (`source = "openfigi"`)
- Reduces API calls over time as cache grows

### Phase 4: Ticker History Tracking
- Add `ticker_history` JSONB column
- Track ticker symbol changes over time
- Example: `{"2024-01-01": "DBK1", "2025-01-01": "DBK"}`
- Useful for historical analysis post-merger/rebranding

---

## Approval Required

Before implementing, confirm:
- [x] ✅ Deutsche Börse CSV as primary mapping source
- [x] ✅ Web scraping approach (vs manual downloads)
- [x] ✅ Daily update frequency (vs intraday)
- [x] ✅ `__UNMAPPED__` partition for unknown ISINs
- [x] ✅ Defer multi-exchange to Phase 2
- [x] ✅ Remove OpenFIGI from Phase 1 scope

---

## Implementation Checklist

### Code Changes
- [ ] Add dependencies (`httpx`, `beautifulsoup4`, `lxml`)
- [ ] Implement `ISINMappingUpdater` class
- [ ] Implement `ISINMapper` class
- [ ] Add CLI command `update-isin-mapping`
- [ ] Integrate `ISINMapper` into `XetraParqed`

### Tests
- [ ] Unit tests: Scraper (5 tests)
- [ ] Unit tests: Parser (8 tests)
- [ ] Unit tests: Merger (7 tests)
- [ ] Unit tests: Mapper (5 tests)
- [ ] Integration: E2E update (4 tests)
- [ ] Integration: CLI (4 tests)

### Documentation
- [ ] Update ADR with Deutsche Börse CSV decision
- [ ] Update implementation addendum (remove OpenFIGI)
- [ ] Update key findings summary (cost savings)
- [ ] Create scraper maintenance guide
- [ ] Document manual fallback process

### Operations
- [ ] Create cron job template for daily updates
- [ ] Set up monitoring/alerting for scraper failures
- [ ] Create manual CSV import command (fallback)
- [ ] Document troubleshooting steps

---

## Cost Savings

**OpenFIGI API** (original plan):
- Free tier: 25 requests/sec (insufficient for 4,280 ISINs)
- Paid tier: $500/month for 250 requests/sec
- **Annual cost**: **$6,000**

**Deutsche Börse CSV** (new plan):
- Web scraping: FREE
- Storage: ~500KB Parquet (negligible)
- Compute: Daily update ~5 seconds CPU time
- **Annual cost**: **$0**

**Savings**: **$6,000/year** ✅

---

## Questions?

**Q: What if Deutsche Börse changes their webpage?**  
A: Scraper has version-controlled HTML fixtures for regression testing. Fallback to manual CSV download + local import.

**Q: What if CSV schema changes?**  
A: Parser validates column positions on startup. Tests alert to schema drift. Graceful degradation skips unknown columns.

**Q: What about real-time listings (IPOs)?**  
A: 0-24 hour delay acceptable (rare events). Unknown ISINs go to `__UNMAPPED__` partition for manual review.

**Q: Can we support other exchanges (Frankfurt, Stuttgart)?**  
A: Yes! Phase 2 enhancement. Same CSV pattern, just different URLs. Extend schema with `exchange` column.

**Q: Should we keep OpenFIGI as fallback?**  
A: Defer to Phase 3. Only needed for non-German ISINs (ADRs, international). XETRA-only scope for Phase 1.

---

**Next Steps**: Proceed with implementation per checklist above. Estimate 3-5 days development + testing.
