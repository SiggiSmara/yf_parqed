# ADR 2025-10-10: Yahoo Finance Data Pipeline

## Status

**Implemented** - Core production system (service-oriented refactoring completed 2025-10-11)

## Context

### Business Requirements

- Users require persistent, interval-aware storage of US equity OHLCV data (Open/High/Low/Close/Volume)
- Yahoo Finance provides free, historical and real-time data via the `yfinance` library
- Data must survive application updates, support incremental updates, and handle ticker lifecycle (active → delisted → reactivated)
- System must be robust against API rate limits, parquet file corruption, and ticker metadata inconsistencies

### Technical Context

- Yahoo Finance returns split-adjusted prices by default but provides no split metadata
- API has strict rate limiting (default: 3 requests per 2 seconds)
- Historical data available going back decades for most US equities
- Ticker symbols can be delisted, merged, or reactivated over time
- Multiple intervals supported: 1m (7 days max), 5m, 15m, 30m, 1h (730 days max), 1d, 1wk, 1mo

## Architectural Decisions

### AD-1: Service-Oriented Architecture with Dependency Injection

**Decision**: Decompose monolithic `YFParqed` class (700+ lines) into specialized services with clear responsibilities.

**Architecture**:
```
YFParqed (façade, 485 lines)
├── ConfigService (79 lines) - Environment and configuration management
├── TickerRegistry (215 lines) - Ticker lifecycle and interval status tracking
├── IntervalScheduler (95 lines) - Update loop orchestration
├── DataFetcher (126 lines) - Yahoo Finance API abstraction with rate limiting
└── StorageBackend (116 lines) - Parquet I/O with corruption recovery
    └── PartitionedStorageBackend (extension) - Hive-style partitioning
```

**Service Responsibilities**:

**ConfigService**:
- Loads/saves `tickers.json` (ticker metadata), `intervals.json` (configured intervals), `storage_config.json` (backend selection)
- Provides working directory and file path resolution
- Single source of truth for configuration persistence

**TickerRegistry**:
- Manages ticker lifecycle: `active` (trading), `not_found` (delisted/invalid)
- Per-interval status tracking with cooldown periods (30 days after interval failure)
- Global exclusion when all intervals fail
- Reactivation logic for previously delisted tickers (90-day recent activity threshold)

**IntervalScheduler**:
- Orchestrates update loop: ticker filtering → per-ticker fetch → status updates
- Enforces per-interval cooldown (skip tickers in cooldown period)
- Handles empty responses (marks interval as `not_found`)
- Logs progress and errors

**DataFetcher**:
- Wraps `yfinance` API with normalized interface
- Enforces Yahoo Finance interval constraints (1m: 7 days, 1h: 730 days)
- Normalizes DataFrame schema (lowercase columns, timezone removal)
- Invokes rate limiter before each request

**StorageBackend**:
- Reads/writes parquet files with corruption detection and recovery
- Merges new data with existing data (deduplication by datetime index)
- Validates schema and data types
- Supports both legacy (flat) and partitioned (Hive-style) layouts

**Rationale**:
- **Single Responsibility**: Each service has one reason to change
- **Testability**: Services can be unit tested in isolation with mocks
- **Maintainability**: 100-200 lines per service vs 700+ line monolith
- **Reusability**: ConfigService and StorageBackend shared with Xetra pipeline
- **Backward Compatibility**: Façade pattern preserves existing CLI and API

**Testing**: 109 tests passing (up from 80 pre-refactoring), comprehensive coverage across unit, integration, and end-to-end levels.

### AD-2: Unified Ticker Registry with Per-Interval Status

**Decision**: Store all ticker metadata in single `tickers.json` file with nested per-interval status.

**Schema**:
```json
{
  "AAPL": {
    "ticker": "AAPL",
    "status": "active",
    "added_date": "2024-01-15",
    "last_checked": "2024-01-20",
    "intervals": {
      "1d": {
        "status": "active",
        "last_found_date": "2024-01-20",
        "last_data_date": "2024-01-19",
        "last_checked": "2024-01-20",
        "storage": {
          "backend": "partitioned",
          "market": "us",
          "source": "yahoo"
        }
      },
      "1h": {
        "status": "not_found",
        "last_not_found_date": "2024-01-20",
        "last_checked": "2024-01-20"
      }
    }
  }
}
```

**Lifecycle Logic**:
- **Per-interval tracking**: Each interval has independent `active`/`not_found` status
- **Cooldown enforcement**: 30-day cooldown after interval-specific `not_found` (prevents repeated API calls for unavailable data)
- **Global status**: Ticker marked globally `not_found` when all configured intervals fail
- **Reactivation**: `reparse_not_founds` command reactivates tickers with recent interval activity (<90 days)

**Rationale**:
- **Granular control**: 1m data may expire while 1d data remains available
- **API efficiency**: Skip intervals in cooldown to reduce unnecessary requests
- **Audit trail**: Tracks first seen, last seen, and state transitions for debugging
- **Storage routing**: Per-interval `storage` metadata enables backend migration

**Alternatives Considered**: Separate per-interval files (rejected - increases I/O and merge complexity), global cooldown (rejected - overly aggressive for multi-interval tickers).

### AD-3: Dual Storage Backend with Partition Migration

**Decision**: Support both legacy (flat) and partitioned (Hive-style) storage layouts with runtime selection.

**Legacy Layout** (`stocks_<interval>/`):
```
stocks_1d/
├── AAPL.parquet
├── MSFT.parquet
└── GOOGL.parquet
```

**Partitioned Layout** (`data/<market>/<source>/stocks_<interval>/`):
```
data/us/yahoo/stocks_1d/
├── ticker=AAPL/
│   ├── year=2024/month=11/
│   │   └── data.parquet
│   └── year=2024/month=12/
│       └── data.parquet
├── ticker=MSFT/
│   └── year=2024/month=11/
│       └── data.parquet
└── ticker=GOOGL/
    └── year=2024/month=12/
        └── data.parquet
```

**Backend Selection**:
- Runtime routing via `storage_config.json` (global/market/source flags)
- Per-ticker override via `tickers.json` interval-level `storage` metadata
- Migration CLI (`yf-parqed-migrate`) for bulk conversion

**Partitioned Storage Benefits**:
- **Corruption isolation**: Single corrupt file affects one ticker/month, not entire ticker history
- **Incremental backups**: rsync only changed months, not entire multi-GB files
- **Query optimization**: DuckDB partition pruning for time-range queries
- **Storage efficiency**: Compress historical partitions independently
- **Hive compatibility**: Standard `key=value/` layout works with Spark, Trino, DuckDB

**Migration Strategy**:
- Legacy data moved to `data/legacy/` for archival
- Migration CLI validates, transforms, and writes to partitioned layout
- Atomic directory moves ensure data safety
- Per-ticker migration tracking in `tickers.json`

**Rationale**: Incremental migration avoids big-bang cutover risk. Users can run both backends simultaneously during transition. Partitioned layout scales to thousands of tickers without performance degradation.

### AD-4: Rate Limiting with Configurable Burst Management

**Decision**: Enforce rate limiting via token bucket algorithm with CLI-configurable parameters.

**Implementation**:
- Default: 3 requests per 2 seconds (Yahoo Finance documented limit)
- CLI override: `yf-parqed --limits 5 2 update-data` (5 requests per 2 seconds)
- Token bucket: Allows bursts up to `max_calls` then enforces `period` delay
- Invoked by DataFetcher before every `yfinance` API call

**Rationale**:
- **API compliance**: Prevents 429 Too Many Requests errors and potential IP bans
- **Flexibility**: Power users can increase limits for faster backfills (at their own risk)
- **Testing**: Tests use mock limiter (`limiter=lambda: None`) for speed
- **Observability**: Logs when rate limiting triggers for debugging

**Alternatives Considered**: No rate limiting (rejected - causes 429 errors), exponential backoff (deferred - token bucket sufficient for steady-state operation).

### AD-5: Corruption Recovery with Schema Validation

**Decision**: Automatically detect and recover from corrupt parquet files during read operations.

**Recovery Process**:
1. Attempt to read parquet file
2. If read fails (ArrowInvalid, OSError), log warning and delete corrupt file
3. Retry read (returns empty DataFrame if file deleted)
4. Caller proceeds with merge logic (treats as new data write)

**Schema Validation**:
- Enforce lowercase column names (`open`, `high`, `low`, `close`, `volume`)
- Require datetime index named `datetime`
- Validate data types (float64 for prices, int64 for volume)
- Add `stock` column with ticker symbol

**Rationale**:
- **Self-healing**: System recovers from corruption without manual intervention
- **Data safety**: Deletion only after read failure (prevents false positives)
- **Idempotent**: Re-fetching data after deletion restores lost records
- **Observability**: Logs corruption events for monitoring

**Trade-offs**: Deleted data must be re-fetched (acceptable - Yahoo Finance has historical data), aggressive deletion policy (acceptable - corruption is rare, re-fetch is safe).

### AD-6: Ticker Lifecycle Automation

**Decision**: Automate ticker discovery, deactivation, and reactivation via CLI commands.

**Commands**:
- `initialize`: Download NASDAQ/NYSE ticker lists, populate `tickers.json`
- `update-tickers`: Merge new tickers, reactivate previously `not_found` tickers
- `update-data`: Fetch data for active tickers, update interval status
- `confirm-not-founds`: Re-check globally `not_found` tickers (via 1d interval)
- `reparse-not-founds`: Reactivate tickers with recent interval activity (<90 days)

**Lifecycle States**:
```
[New Ticker]
    ↓
  active (all intervals)
    ↓ (interval-specific failures)
  active (some intervals), not_found (other intervals)
    ↓ (all intervals fail)
  not_found (global)
    ↓ (confirm-not-founds finds data OR update-tickers reactivates)
  active (reactivated)
```

**Rationale**:
- **Hands-off operation**: System adapts to ticker lifecycle without manual intervention
- **Data freshness**: `update-tickers` adds IPOs, mergers, relistings automatically
- **Resource efficiency**: Cooldown periods prevent repeated API calls for inactive tickers
- **Audit trail**: All state transitions logged in `tickers.json`

**Alternatives Considered**: Manual ticker management (rejected - high operational burden), real-time ticker validation (deferred - batch updates sufficient for daily workflows).

### AD-7: Daemon Mode with Multi-Timezone Trading Hours Awareness

**Decision**: Support daemon mode for continuous data collection with timezone-aware US market hours (NYSE/NASDAQ).

**Status**: 🔲 **NOT IMPLEMENTED** (planned)

**Proposed Implementation**:

**Trading Hours**:
- **NYSE/NASDAQ regular hours**: 09:30-16:00 US/Eastern (6.5 hours, default)
- **Pre-market**: 04:00-09:30 US/Eastern (optional, `--extended-hours` flag)
- **After-hours**: 16:00-20:00 US/Eastern (optional, `--extended-hours` flag)
- **Timezone conversion**: Daemon must convert US/Eastern to local system timezone for scheduling

**Daemon Features** (similar to Xetra AD-9):
- `yf-parqed --daemon --interval 1 update-data` - Run every 1 hour during market hours (default)
- `--market-timezone "US/Eastern"` - Market timezone (defaults to `US/Eastern` for NYSE/NASDAQ)
- `--system-timezone auto` - System timezone (auto-detected from OS, or explicit like `Europe/Berlin`)
- `--extended-hours` - Enable pre-market and after-hours collection (still hourly interval)
- `--ticker-maintenance-interval weekly` - Periodic ticker list updates and not-found revalidation
- Smart sleeping outside market hours (check for shutdown every minute)
- PID file management (`--pid-file /run/yf-parqed/update.pid`)
- File logging with rotation (`--log-file /var/log/yf-parqed/update.log`)
- Signal handling (SIGTERM/SIGINT for graceful shutdown)

**Timezone Configuration**:

**Auto-detection (default)**:
```python
# System timezone detected via time.tzname, datetime.now().astimezone(), or /etc/timezone
# Examples:
#   - Linux: Reads /etc/timezone or $TZ environment variable
#   - Windows: Uses Windows Registry timezone settings
#   - Docker: Inherits from container TZ env var or host /etc/localtime

import datetime
system_tz = datetime.datetime.now().astimezone().tzinfo  # Auto-detected
# Result: tzfile('/usr/share/zoneinfo/Europe/Berlin') or similar
```

**Timezone Conversion Examples**:
```python
# System in Germany (CET/CEST) running NYSE daemon
# Auto-detected: Europe/Berlin
# Market opens: 09:30 US/Eastern = 15:30 CET (winter) / 16:30 CEST (summer)
# Market closes: 16:00 US/Eastern = 22:00 CET (winter) / 23:00 CEST (summer)
# Daemon logs: "NYSE active 09:30-16:00 US/Eastern (15:30-22:00 CET today)"

# System in California (US/Pacific) running NYSE daemon
# Auto-detected: US/Pacific  
# Market opens: 09:30 US/Eastern = 06:30 US/Pacific
# Market closes: 16:00 US/Eastern = 13:00 US/Pacific
# Daemon logs: "NYSE active 09:30-16:00 US/Eastern (06:30-13:00 PST today)"

# System in Tokyo (Asia/Tokyo) running NYSE daemon
# Auto-detected: Asia/Tokyo
# Market opens: 09:30 US/Eastern = 23:30 JST (previous day)
# Market closes: 16:00 US/Eastern = 06:00 JST (next day)
# Daemon logs: "NYSE active 09:30-16:00 US/Eastern (23:30 JST Dec 3 - 06:00 JST Dec 4)"
```

**Configuration Priority**:
1. `--system-timezone` flag (explicit override)
2. `TZ` environment variable
3. Auto-detection from OS (Linux: `/etc/timezone`, Windows: Registry)

**Key Differences from Xetra Daemon**:
| Aspect | Xetra | Yahoo Finance (NYSE/NASDAQ) |
|--------|-------|------------------------------|
| **Market timezone** | Europe/Berlin (CET/CEST) | US/Eastern (EST/EDT) |
| **Regular hours** | 08:30-18:00 (9.5h) | 09:30-16:00 (6.5h) |
| **Extended hours** | None (pre-market not published) | Pre-market 04:00-09:30, After-hours 16:00-20:00 |
| **Data availability** | 15-min delayed, expires after 24h | Real-time (15-min delayed for free tier), historical indefinite |
| **Update frequency** | Hourly (per-trade data) | Hourly (1m data only, aggregate offline) |
| **Data collected** | Raw trades (all ISINs) | 1m OHLCV only (1h/1d calculated separately) |
| **Timezone complexity** | Single timezone (Europe/Berlin) | Multi-timezone (US/Eastern vs system timezone) |

**Configuration Examples**:
```bash
# Minimal configuration (auto-detects system timezone)
ExecStart=/opt/yf_parqed/.venv/bin/yf-parqed \
    --wrk-dir /var/lib/yf-parqed \
    --log-file /var/log/yf-parqed/update.log \
    --daemon \
    --interval 1 \
    update-data \
    --intervals 1m \
    --pid-file /run/yf-parqed/update.pid

# Explicit system timezone (useful for Docker/containers)
ExecStart=/opt/yf_parqed/.venv/bin/yf-parqed \
    --daemon \
    --interval 1 \
    --system-timezone "Europe/Berlin" \
    update-data \
    --intervals 1m \
    --pid-file /run/yf-parqed/update.pid

# Override market timezone (non-US markets, future enhancement)
ExecStart=/opt/yf_parqed/.venv/bin/yf-parqed \
    --daemon \
    --interval 1 \
    --market-timezone "Europe/London" \
    update-data \
    --intervals 1m \
    --pid-file /run/yf-parqed/update.pid

# Extended hours variant (auto-detects system timezone)
ExecStart=/opt/yf_parqed/.venv/bin/yf-parqed \
    --daemon \
    --interval 1 \
    update-data \
    --intervals 1m \
    --extended-hours \
    --pid-file /run/yf-parqed/update.pid

# With periodic ticker maintenance (weekly, auto-detects system timezone)
ExecStart=/opt/yf_parqed/.venv/bin/yf-parqed \
    --daemon \
    --interval 1 \
    update-data \
    --intervals 1m \
    --ticker-maintenance-interval weekly \
    --pid-file /run/yf-parqed/update.pid

# Note: 1h, 1d, and other intervals calculated offline via separate aggregation job
```

**Ticker Maintenance Options**:

The daemon can automatically perform ticker list maintenance via `--ticker-maintenance-interval`:

| Interval | Description | Operations Performed | Recommended For |
|----------|-------------|---------------------|-----------------|
| `daily` | Every 24 hours | `update-tickers` (add new IPOs/relistings), `reparse-not-founds` (reactivate recent activity) | Active traders, IPO-heavy portfolios |
| `weekly` | Every 7 days (default) | `update-tickers`, `confirm-not-founds` (re-check globally not-found), `reparse-not-founds` | Most users, balanced maintenance |
| `monthly` | Every 30 days | `update-tickers`, `confirm-not-founds`, `reparse-not-founds` | Long-term investors, stable portfolios |
| `never` | Disabled | Manual ticker management only | Users who prefer manual control |

**Maintenance Operations**:
- **`update-tickers`**: Downloads fresh NASDAQ/NYSE listings, adds new tickers, reactivates previously not-found tickers
- **`confirm-not-founds`**: Re-checks globally not-found tickers via 1d interval (are they trading again?)
- **`reparse-not-founds`**: Reactivates tickers with recent interval activity (<90 days)

**Timing**:
- Maintenance runs **outside market hours** (after 16:00 US/Eastern or before 09:30)
- Prevents interference with live data collection
- Uses same rate limiting as regular updates (3 req/2s)

**Recommended Interval Discussion**:

**Weekly (recommended default)**:
- **Pros**: Catches new IPOs within 7 days, keeps not-found list fresh, low overhead (~500-1000 extra API calls/week)
- **Cons**: Slightly more API usage than monthly
- **Use case**: Most users, reasonable balance of freshness vs resource usage

**Daily**:
- **Pros**: Minimal lag for new listings, aggressive not-found revalidation
- **Cons**: Higher API usage (~3000-5000 extra calls/week), more aggressive on not-found tickers (30-day cooldown still applies)
- **Use case**: Users tracking IPO activity, high portfolio churn

**Monthly**:
- **Pros**: Minimal API overhead, respects long cooldown periods
- **Cons**: Up to 30-day lag for new IPOs, not-found list may grow stale
- **Use case**: Long-term buy-and-hold portfolios, stable ticker lists

**Never**:
- **Pros**: Total control, no automatic changes to ticker list
- **Cons**: Manual `update-tickers` required after IPOs/mergers, not-found list grows without manual intervention
- **Use case**: Fixed portfolios, users who prefer explicit control

**Rationale**:
- **Minute granularity only**: Daemon fetches only 1m data during trading hours (raw data source)
- **Aggregation decoupled**: 1h, 1d, and other intervals calculated outside daemon via batch jobs or DuckDB queries
- **Update frequency**: Hourly collection sufficient for 1m data (Yahoo Finance 1m data limited to last 7 days, not real-time stream)
- **Ticker maintenance**: Automated discovery of new IPOs, reactivation of relisted tickers, cleanup of not-found list
- **API compliance**: Minimizes API calls (6-7 requests per ticker per day vs 3 req/2s limit), maintenance adds ~500-1000/week
- **Separation of concerns**: Live collection (daemon) vs aggregation/maintenance (offline/scheduled)
- **Timezone portability**: Users in Europe/Asia can run daemon without manual hour calculation
- **Extended hours support**: Power users can capture pre-market/after-hours 1m data (still hourly)
- **DST handling**: `zoneinfo` handles EST/EDT transitions automatically
- **Consistent UX**: Same daemon patterns as Xetra (hourly interval, PID files, logging, systemd integration)

**Implementation Checklist**:
- [ ] Add `--daemon`, `--interval`, `--active-hours`, `--timezone`, `--extended-hours` flags to CLI
- [ ] Implement `TradingHoursChecker` service with timezone conversion (reusable for Xetra multi-timezone)
- [ ] Add PID file management (reuse Xetra implementation)
- [ ] Add file logging with rotation (reuse Xetra implementation)
- [ ] Add signal handlers (SIGTERM, SIGINT)
- [ ] Write daemon integration tests (similar to Xetra's 52 tests)
- [ ] Document in `DAEMON_MODE.md` with systemd examples
- [ ] Validate across timezones (US/Eastern, Europe/Berlin, Asia/Tokyo)

**Xetra Multi-Timezone Enhancement**:
The same `TradingHoursChecker` service can enhance Xetra daemon for users running outside Europe:
```bash
# User in California running Xetra daemon (auto-detects US/Pacific)
xetra-parqed --daemon fetch-trades DETR \
    --pid-file /run/xetra/detr.pid
# Daemon converts: 08:30 CET = 23:30 PST (previous day)
#                   18:00 CET = 09:00 PST

# Explicit system timezone (Docker/container)
xetra-parqed --daemon \
    --system-timezone "Asia/Tokyo" \
    fetch-trades DETR \
    --pid-file /run/xetra/detr.pid
# Daemon converts: 08:30 CET = 16:30 JST
#                   18:00 CET = 02:00 JST (next day)
```

**Testing Strategy**:
- Unit tests: Timezone conversion logic (EST→CET, PST→EST, etc.)
- Integration tests: Daemon lifecycle (start, sleep, wake, shutdown)
- End-to-end tests: Multi-day operation with DST transitions (March/November)
- Mock time: `freezegun` for reproducible timezone tests

**Alternatives Considered**:
| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| **System timezone only** | Simpler implementation, no timezone config | Users in Europe must manually calculate US hours, breaks on DST transitions | ❌ Rejected - poor UX |
| **UTC-only configuration** | Unambiguous, no DST issues | Users must convert local time to UTC, not intuitive | ❌ Rejected - poor UX |
| **Market timezone with auto-conversion** | Intuitive (configure in market timezone), portable (works anywhere) | Requires timezone library, more complex implementation | ✅ **Selected** |
| **24/7 operation with empty checks** | No scheduling logic needed | Wastes resources, 70%+ unnecessary API calls | ❌ Rejected - inefficient |

**Risks & Mitigations**:
- **Risk**: DST transition during daemon operation (clock jumps forward/back)
  - **Mitigation**: Re-calculate wake time on every sleep cycle, use UTC internally for comparisons
- **Risk**: Timezone database outdated (pytz/zoneinfo)
  - **Mitigation**: Document requirement for updated `tzdata` package, validate on startup
- **Risk**: User confusion about timezone conversion
  - **Mitigation**: Log converted hours in local timezone: "Active 09:30-16:00 US/Eastern (15:30-22:00 CET)"

**Dependencies**:
- `zoneinfo` (Python 3.9+, backport available for 3.8)
- `tzdata` package (Windows/containers, automatic on Linux with system timezone data)

**Documentation**: Will be added to `/docs/DAEMON_MODE.md` with timezone examples, DST handling, and troubleshooting.

## Data Schema

### OHLCV Parquet Schema
```
datetime (datetime64[ns], index): Trading timestamp
open (float64): Opening price
high (float64): Highest price
low (float64): Lowest price
close (float64): Closing price
volume (int64): Trading volume
stock (object): Ticker symbol
```

**Notes**:
- Prices are split-adjusted by Yahoo Finance (unadjusted prices not available)
- Timezone information removed for storage efficiency (assumed US/Eastern)
- Index must be sorted and deduplicated (enforced by StorageBackend)

## Implementation Status

### Completed (Production)
- ✅ Service-oriented architecture (109 tests passing, 100% backward compatible)
- ✅ ConfigService, TickerRegistry, IntervalScheduler, DataFetcher, StorageBackend
- ✅ Dual storage backends (legacy + partitioned)
- ✅ Partition migration CLI (`yf-parqed-migrate`)
- ✅ Rate limiting with burst management
- ✅ Corruption recovery
- ✅ Ticker lifecycle automation
- ✅ Comprehensive test suite (unit, integration, end-to-end)

### In Progress
- 🚧 Corporate action tracking (Phase 1 metadata only - see AD-4 in Xetra ADR)
- 🚧 DuckDB analytics layer (ADR draft exists, implementation pending)

### Future Enhancements
- 🔮 Split-adjusted price backfill for existing data
- 🔮 Historical split detection via price discontinuities
- 🔮 Automated data quality monitoring
- 🔮 Multi-exchange support (European markets via Xetra ADR)

## Consequences

### Positive
- **Proven stability**: 183 tests passing, production-ready architecture
- **Scalability**: Partitioned storage handles thousands of tickers without performance degradation
- **Maintainability**: Service-oriented design enables independent evolution of components
- **Extensibility**: Shared infrastructure (ConfigService, StorageBackend) reused for Xetra pipeline
- **Robustness**: Automatic recovery from API failures, rate limiting, corruption detection

### Negative
- **Storage overhead**: Partitioned layout uses more inodes (one file per ticker/month vs one file per ticker)
- **Migration complexity**: Users must run migration CLI to adopt partitioned layout
- **Split-adjustment opacity**: Yahoo Finance provides no split metadata, requires external tracking
- **US-centric**: Designed for US equities, international markets require separate pipeline (see Xetra ADR)

### Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Yahoo Finance API changes | High | Version schema validation, monitor for breaking changes, fallback to legacy backend |
| Rate limit changes | Medium | Configurable rate limiter, exponential backoff can be added if needed |
| Ticker symbol changes (mergers, relistings) | Medium | Automated reactivation via `update-tickers`, manual override supported |
| Parquet format evolution | Low | Schema validation enforces consistency, migration path for format upgrades |

## Related Documentation

- **Implementation Guide**: `ARCHITECTURE.md` - Service responsibilities, data flows, API reference
- **Development History**: `AGENTS.md` - Refactoring timeline, test coverage map
- **CLI Reference**: `README.md` - Command examples, usage patterns
- **Migration Guide**: `docs/release-notes.md` - Partition storage migration steps
- **Related ADRs**: 
  - `2025-10-12-partition-aware-storage.md` - Partitioned storage implementation details
  - `2025-10-12-xetra-delayed-data.md` - European market extension (shares ConfigService, StorageBackend)
  - `2025-10-12-duckdb-query-layer.md` - Analytics layer (draft)

## Decision History

- **2025-10-10**: Initial ADR created documenting existing production system
- **2025-10-11**: Service-oriented refactoring completed (109 tests passing)
- **2025-10-12**: Partition-aware storage ADR published, migration CLI released
- **2025-10-19**: Partition storage validated in production, atomic writes hardened
