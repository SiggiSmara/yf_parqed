# Data Safety Strategy for yf_parqed

## 🚨 Critical Context: Limited Recovery Windows

**yf_parqed stores financial data with LIMITED RECOVERY WINDOWS:**
- **Yahoo Finance 1m data**: 7-day expiry (permanently lost after)
- **Xetra raw trades**: ~24-hour expiry (permanently lost after)

**ANY storage structure change that causes data loss is CATASTROPHIC and IRREVERSIBLE.**

---

## Mandatory Rules for Storage Changes

### 1. Never Destructive By Default

**Rule:** All storage structure changes MUST preserve existing data as the default behavior.

**Implementation:**
- New storage layouts should **coexist** with old layouts during transition
- Deletion/moving of data requires **explicit user confirmation** with warnings
- Default behavior: Read from both old and new, write to new only
- Provide `--dry-run` mode that shows what would change without executing

**Example (Partition Migration):**
```bash
# Good: Non-destructive by default
yf-parqed-migrate migrate --venue us:yahoo --interval 1d
# → Copies data to new layout, keeps original

# Requires explicit flag for destruction
yf-parqed-migrate migrate --venue us:yahoo --interval 1d --delete-legacy
# → Warns: "This will delete data from stocks_1d/. Continue? [y/N]"
```

---

### 2. Verification Before Activation

**Rule:** New storage layouts must be verified **byte-for-byte** before becoming active.

**Implementation:**
- Row count comparison (legacy vs new)
- SHA256 checksum per ticker
- Schema validation (column names, types, index)
- Date range coverage validation
- Reject migration if ANY ticker fails verification

**Example:**
```python
# Current implementation in partition_migration_service.py
def verify_migration(self, venue, interval):
    legacy_rows = count_rows(legacy_path)
    new_rows = count_rows(partitioned_path)
    
    if legacy_rows != new_rows:
        raise ValueError(f"Row count mismatch: {legacy_rows} != {new_rows}")
    
    legacy_checksum = compute_checksum(legacy_path)
    new_checksum = compute_checksum(partitioned_path)
    
    if legacy_checksum != new_checksum:
        raise ValueError("Checksum mismatch - data corruption detected")
```

---

### 3. Atomic Operations with Rollback

**Rule:** Storage changes must be atomic (all-or-nothing) with rollback capability.

**Implementation:**
- Use staging directories for new layouts
- Atomic rename/move only after full verification
- Maintain `.backup/` or `.migration-backup/` copies during transition
- Provide `rollback` command to restore from backup

**Directory Structure During Migration:**
```
data/
├── legacy/                    # Original data (preserved during migration)
│   └── stocks_1d/
│       ├── AAPL.parquet
│       └── MSFT.parquet
├── .migration-staging/        # Temp directory for new layout (hidden)
│   └── us/yahoo/stocks_1d/
│       └── ticker=AAPL/...
└── us/yahoo/stocks_1d/        # Active after verification passes
    └── ticker=AAPL/...
```

**Rollback Command:**
```bash
# If something goes wrong after migration
yf-parqed-migrate rollback --venue us:yahoo --interval 1d
# → Restores from data/legacy/, deletes new layout
```

---

### 4. Metadata-Driven Storage Routing

**Rule:** Storage location is determined by metadata, not code paths. This allows gradual migration per-ticker.

**Implementation:**
- `tickers.json` contains per-interval storage metadata
- Runtime checks metadata before every read/write
- Supports mixed-mode: Some tickers in legacy, others in partitioned

**Current Implementation:**
```json
{
  "AAPL": {
    "intervals": {
      "1d": {
        "status": "active",
        "storage": {
          "backend": "partitioned",
          "market": "us",
          "source": "yahoo"
        }
      }
    }
  },
  "MSFT": {
    "intervals": {
      "1d": {
        "status": "active"
        // No "storage" key = legacy backend
      }
    }
  }
}
```

**Benefit:** Rollback is just metadata change, no data movement needed.

---

### 5. Explicit Migration Plans

**Rule:** Storage migrations require a persistent plan file documenting intent, progress, and status.

**Implementation:**
- `migration_plan.json` persists migration state
- Tracks per-venue, per-interval completion
- Enables resume after interruption
- Documents original vs target layout

**Current Implementation:**
```json
{
  "version": "1.0",
  "created_at": "2025-12-05T10:30:00Z",
  "venues": [
    {
      "market": "us",
      "source": "yahoo",
      "intervals": [
        {
          "name": "1d",
          "status": "completed",
          "tickers_migrated": 5000,
          "verified_at": "2025-12-05T12:00:00Z",
          "checksum_verified": true
        },
        {
          "name": "1h",
          "status": "in_progress",
          "tickers_migrated": 2500,
          "tickers_total": 5000
        }
      ]
    }
  ]
}
```

---

### 6. Read-Compatibility with Old Layouts

**Rule:** Code must support reading from BOTH old and new layouts indefinitely (write-only to new).

**Implementation:**
- `StorageRequest` abstraction allows specifying legacy vs partitioned
- `PartitionPathBuilder` has `_legacy_path()` fallback
- Read operations check new location first, fall back to legacy
- Never delete legacy data until user explicitly confirms

**Current Implementation:**
```python
def read(self, ticker, interval, market=None, source=None):
    # Try new partitioned layout first
    if market and source:
        path = self.path_builder.build(
            market=market, source=source, 
            dataset="stocks", interval=interval, 
            ticker=ticker, timestamp=datetime.now()
        )
        if path.exists():
            return pd.read_parquet(path)
    
    # Fallback to legacy layout
    legacy_path = self.path_builder._legacy_path(interval, ticker)
    if legacy_path.exists():
        return pd.read_parquet(legacy_path)
    
    # No data found in either location
    return empty_dataframe()
```

---

### 7. Disk Space Validation

**Rule:** Migrations must validate sufficient disk space before starting (2x data size + overhead).

**Implementation:**
- Calculate source data size
- Check free disk space
- Require 2.5x source size available (source + copy + overhead)
- Abort if insufficient space

**Current Implementation:**
```python
def estimate_disk_requirements(self, venue, interval):
    legacy_size = get_directory_size(f"data/legacy/stocks_{interval}")
    required_space = legacy_size * 2.5  # Source + copy + overhead
    
    free_space = shutil.disk_usage(self.root_path).free
    
    if free_space < required_space:
        raise ValueError(
            f"Insufficient disk space. "
            f"Required: {required_space / 1e9:.1f} GB, "
            f"Available: {free_space / 1e9:.1f} GB"
        )
```

---

### 8. Incremental Migration with Progress Tracking

**Rule:** Large migrations should process data in batches with persistent progress tracking.

**Implementation:**
- Process N tickers at a time (default: 100)
- Save progress after each batch
- Resume from last checkpoint on failure/interrupt
- Show ETA based on current throughput

**Example:**
```bash
yf-parqed-migrate migrate --venue us:yahoo --interval 1d --batch-size 100

# Output:
# Migrating AAPL... ✓ (1/5000, ETA: 2h 30m)
# Migrating MSFT... ✓ (2/5000, ETA: 2h 28m)
# ...
# [Ctrl+C pressed]
# Progress saved: 2500/5000 tickers completed
# Resume with: yf-parqed-migrate migrate --venue us:yahoo --interval 1d --resume
```

---

### 9. Data Integrity Monitoring

**Rule:** Regular automated checks to detect corruption or inconsistencies.

**Implementation:**
- Daily cron job runs integrity checks
- Validates parquet file readability
- Compares row counts vs expected (from metadata)
- Alerts on anomalies (missing dates, corrupt files, unexpected size changes)

**Proposed Command:**
```bash
yf-parqed verify-integrity --interval 1d --last-n-days 7
# Checks:
# - All parquet files readable
# - No missing dates in coverage
# - Row counts match expected patterns
# - Schema consistency across tickers
```

---

### 10. Documentation of Current Layout

**Rule:** Always document the CURRENT storage structure so future changes are aware.

**Current Storage Structure (as of 2025-12-05):**

#### Legacy (Flat) Layout
```
data/
├── stocks_1m/              # Yahoo Finance 1-minute data
│   ├── AAPL.parquet
│   └── MSFT.parquet
├── stocks_1h/              # Yahoo Finance 1-hour data
├── stocks_1d/              # Yahoo Finance 1-day data
└── ...
```

#### Partitioned (Hive-Style) Layout
```
data/
├── us/yahoo/               # Market/source scoped
│   ├── stocks_1m/
│   │   └── ticker=AAPL/
│   │       └── year=2025/month=12/
│   │           └── data.parquet
│   ├── stocks_1h/
│   └── stocks_1d/
└── de/                     # Xetra data
    ├── xetra/
    │   ├── trades/         # Raw per-trade data
    │   │   └── venue=DETR/
    │   │       └── year=2025/month=12/day=05/
    │   │           └── trades.parquet
    │   ├── stocks_1m/      # Aggregated OHLCV (NOT YET IMPLEMENTED)
    │   ├── stocks_1h/
    │   └── stocks_1d/
    └── tradegate/          # Future: Tradegate venue
```

#### Migration Metadata Files
- **`storage_config.json`** - Global/market/source partition flags
- **`tickers.json`** - Per-ticker per-interval storage metadata
- **`migration_plan.json`** - Active migration tracking (if exists)

---

## Pre-Flight Checklist for Storage Changes

Before proposing ANY change to storage structure, verify:

- [ ] **Document current structure clearly**
- [ ] **Explain why change is needed** (what problem does it solve?)
- [ ] **Design coexistence strategy** (old + new simultaneously)
- [ ] **Specify verification method** (checksums, row counts)
- [ ] **Plan rollback procedure** (how to undo if things go wrong?)
- [ ] **Estimate disk space requirements** (2.5x source size minimum)
- [ ] **Draft migration command** with `--dry-run` option
- [ ] **Write tests** covering both old and new layouts
- [ ] **Update this documentation** with new layout
- [ ] **Consider failure scenarios:**
  - What happens if migration fails halfway?
  - What happens if user runs old code after migration?
  - Can we detect corrupted migration automatically?
  - What if disk fills up during migration?

---

## Examples: Right vs Wrong Approaches

### ❌ WRONG: Destructive Change

```python
# DON'T: Overwrites existing data without verification
def save_aggregated(df, interval, ticker):
    path = f"stocks_{interval}/{ticker}.parquet"  # Same path as raw data!
    df.to_parquet(path)  # Overwrites raw data - CATASTROPHIC ❌
```

**Problems:**
- Overwrites existing data without backup
- No verification of new data
- No rollback capability
- Silent data loss

---

### ✅ RIGHT: Non-Destructive Change

```python
# DO: New dataset name, separate path, metadata tracking, verification
def save_aggregated(df, interval, ticker):
    # 1. New dataset name to avoid collision
    staging_path = path_builder.build(
        market="us", source="yahoo",
        dataset="stocks_aggregated",  # Different from "stocks"
        interval=interval, ticker=ticker,
        timestamp=datetime.now()
    )
    
    # 2. Write to staging directory first
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(staging_path)
    
    # 3. Verify data integrity
    verify_parquet_readable(staging_path)
    verify_row_count(df, staging_path)
    verify_schema(staging_path, expected_schema)
    
    # 4. Atomic move to final location
    final_path = staging_path.parent / "data.parquet"
    staging_path.replace(final_path)  # Atomic operation
    
    # 5. Update metadata to track both raw and aggregated
    update_ticker_metadata(ticker, interval, {
        "raw_backend": "partitioned",
        "aggregated_backend": "partitioned",
        "aggregated_from": "1m",
        "aggregated_at": datetime.now().isoformat(),
        "source_checksum": compute_checksum(raw_data)
    })
    
    # 6. Log for audit trail
    logger.info(f"Aggregated {ticker} {interval} from 1m data: {len(df)} rows")
```

**Benefits:**
- Preserves original data
- Staging directory + atomic move
- Full verification before activation
- Metadata tracking for rollback
- Audit trail

---

### ❌ WRONG: No Migration Plan

```bash
# DON'T: Directly modify storage without plan
mv data/stocks_1d data/us/yahoo/stocks_1d  # Manual, error-prone ❌
```

**Problems:**
- No progress tracking
- Can't resume if interrupted
- No verification
- No rollback capability

---

### ✅ RIGHT: Explicit Migration with Plan

```bash
# DO: Create plan, migrate incrementally, verify, rollback if needed

# 1. Create migration plan
yf-parqed-migrate init --venue us:yahoo --interval 1d

# 2. Estimate requirements (non-destructive check)
yf-parqed-migrate estimate --venue us:yahoo --interval 1d
# Output: Required: 50 GB, Available: 100 GB ✓

# 3. Dry run (shows what would happen)
yf-parqed-migrate migrate --venue us:yahoo --interval 1d --dry-run

# 4. Incremental migration with checkpoints
yf-parqed-migrate migrate --venue us:yahoo --interval 1d --batch-size 100

# 5. Verify data integrity
yf-parqed-migrate verify --venue us:yahoo --interval 1d
# Output: ✓ Row counts match: 5000 tickers
#         ✓ Checksums verified: 5000 tickers

# 6. If verification passes, activate new layout (metadata change only)
yf-parqed-migrate activate --venue us:yahoo --interval 1d

# 7. If something goes wrong, rollback
yf-parqed-migrate rollback --venue us:yahoo --interval 1d
```

**Benefits:**
- Persistent plan tracking
- Incremental progress with resume
- Verification before activation
- Rollback capability
- Clear audit trail

---

## When to Escalate

**STOP and seek review** before implementing storage changes that involve:

1. **Changing partition key structure** (e.g., `ticker=` → `isin=`)
2. **Modifying parquet schema** (adding/removing columns)
3. **Changing file naming conventions** (e.g., `data.parquet` → `ohlcv.parquet`)
4. **Moving data between directories** (any `mv` or `rename` operations)
5. **Introducing new dataset types** (new top-level directories)
6. **Deprecating existing layouts** (removing support for old paths)
7. **Changing metadata structure** (tickers.json, storage_config.json schema)

**Golden Rule: When in doubt about data safety, STOP and ASK.**

Better to delay implementation and ask questions than to cause irreversible data loss.

---

## Testing Requirements for Storage Changes

All storage structure changes MUST include:

### Unit Tests
- Read from old layout ✓
- Read from new layout ✓
- Write to new layout ✓
- Fallback to old layout when new doesn't exist ✓
- Mixed-mode (some tickers old, some new) ✓

### Integration Tests
- Full migration workflow (init → migrate → verify → activate)
- Resume after interruption (simulate Ctrl+C)
- Rollback after partial migration
- Disk space validation (simulate full disk)
- Corrupted data detection (simulate bad parquet file)

### End-to-End Tests
- Real parquet files (not mocked)
- Multiple tickers (at least 10)
- Multiple intervals (at least 2)
- Verification passes ✓
- Can read from both layouts after migration ✓

---

## Current Implementation References

**Key Files:**
- `src/yf_parqed/partition_path_builder.py` - Path resolution logic
- `src/yf_parqed/partitioned_storage_backend.py` - Partitioned storage implementation
- `src/yf_parqed/storage_backend.py` - Legacy storage implementation
- `src/yf_parqed/partition_migration_service.py` - Migration orchestration
- `src/yf_parqed/tools/partition_migrate.py` - Migration CLI
- `src/yf_parqed/config_service.py` - Storage config management

**Test Files:**
- `tests/test_partitioned_storage_backend.py`
- `tests/test_partition_migration_service.py`
- `tests/test_partition_migrate_cli.py`
- `tests/test_storage_backend.py`

**Documentation:**
- `docs/adr/2025-10-12-partition-aware-storage.md` - Partition storage ADR
- `docs/release-notes.md` - Migration guidance for users
- `README.md` - User-facing migration instructions

---

## Maintenance and Updates

**This document should be updated when:**
- New storage layout is introduced
- Migration strategy changes
- New safety mechanisms are added
- Storage-related bugs are discovered and fixed
- User feedback reveals gaps in safety procedures

**Last Updated:** 2025-12-05  
**Next Review:** After any storage structure change or every 3 months
