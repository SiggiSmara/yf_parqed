# yf_parqed Architecture

**For development history and agent-specific context, see [AGENTS.md](AGENTS.md).**

## Service-Oriented Design

The codebase follows a service-oriented architecture with clear separation of concerns:

```text
┌─────────────────────────────────────────────────────────────┐
│                         CLI Layer                            │
│                      (main.py / Typer)                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      YFParqed Façade                         │
│              (primary_class.py - 522 lines)                  │
│                                                              │
│  • Wires dependencies via constructor injection             │
│  • Delegates to services for all operations                 │
│  • Maintains backward-compatible public API                 │
└──────────┬────────┬────────┬────────┬────────┬──────────────┘
           │        │        │        │        │
           ▼        ▼        ▼        ▼        ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
    │ Config   │ │ Ticker   │ │ Interval │ │ Data     │ │ Storage  │
    │ Service  │ │ Registry │ │Scheduler │ │ Fetcher  │ │ Layers   │
    └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘
```

## Service Responsibilities

### ConfigService

**Purpose**: Environment and configuration management  
**File**: `src/yf_parqed/config_service.py`

Key methods:

- `set_working_path()` changes the base directory.
- `load_intervals()` / `save_intervals()` manage interval configuration.
- `load_tickers()` / `save_tickers()` read and persist ticker metadata.
- `configure_limits()` stores rate limiting parameters.
- `get_now()` / `format_date()` provide deterministic timestamps.

**Dependencies**: None (bottom layer)  
**Tests**: `tests/test_config_service.py`

### TickerRegistry

**Purpose**: Ticker lifecycle and interval metadata management  
**File**: `src/yf_parqed/ticker_registry.py`

Key methods:

- `load()` / `save()` handle persistence.
- `update_current_list()` merges new tickers and reactivates not-founds.
- `is_active_for_interval()` evaluates ticker/interval eligibility.
- `get_interval_storage()` exposes interval storage metadata for routing.
- `update_ticker_interval_status()` records fetch success/failure.
- `confirm_not_founds()` and `reparse_not_founds()` maintain not-found lifecycles.

**Dependencies**: `ConfigService`, optional limiter and fetch callback  
**Tests**: `tests/test_ticker_registry.py`, `tests/test_ticker_operations.py`

### IntervalScheduler

**Purpose**: Orchestrate the update loop across intervals  
**File**: `src/yf_parqed/interval_scheduler.py`

Key methods:

- `run()` reloads tickers, filters by interval eligibility, and invokes the processor per ticker.

**Dependencies**: `TickerRegistry`, limiter, processor callback  
**Tests**: `tests/test_interval_scheduler.py`

### DataFetcher

**Purpose**: Yahoo Finance API abstraction  
**File**: `src/yf_parqed/data_fetcher.py`

Key methods:

- `fetch()` retrieves OHLCV data for a ticker/interval/date range.
- `_fetch_window()` is a bounded fetch while `_fetch_all()` requests maximum history.
- `_apply_interval_constraints()` enforces Yahoo limits (729-day hourly, 7-day minute, etc.).
- `_normalize_dataframe()` lowercases columns, removes timezone metadata, and sets the multi-index.

**Dependencies**: `yfinance`, limiter, date provider  
**Tests**: `tests/test_data_fetcher.py`

### StorageBackend

**Purpose**: Legacy parquet persistence with corruption recovery  
**File**: `src/yf_parqed/storage_backend.py`

Key methods:

- `read()` loads parquet files with schema validation and removes corrupt artifacts.
- `save()` merges, deduplicates, and persists data to `stocks_<interval>/<ticker>.parquet`.

**Dependencies**: Empty frame factory, normalizer, column provider  
**Tests**: `tests/test_storage_backend.py`, `tests/test_storage_operations.py`

### PartitionedStorageBackend

**Purpose**: Partition-aware parquet persistence using Hive-style directories  
**File**: `src/yf_parqed/partitioned_storage_backend.py`

Key methods:

- `save()` writes data under `data/<market>/<source>/stocks_<interval>/ticker=<TICKER>/year=...` partitions.
- `read()` globs partition directories, normalizes schema, and deletes corrupt files before retrying.

**Dependencies**: `PartitionPathBuilder`, empty frame factory, normalizer, column provider  
**Tests**: `tests/test_partitioned_storage_backend.py`

Status: Implemented and validated (2025-10-19). See ADR `docs/adr/2025-10-12-partition-aware-storage.md` and release notes for operational details.

### PartitionMigrationService

**Purpose**: Drive legacy-to-partition migrations and toggle runtime storage flags  
**File**: `src/yf_parqed/partition_migration_service.py`

Key methods:

- `initialize_plan()` persists a migration plan rooted at `data/legacy/...`.
- `estimate_disk_requirements()` surfaces disk usage estimates and limitations before copying.
- `migrate_interval()` copies and verifies ticker data, backfills interval storage metadata, and activates per-source partition flags once verification succeeds.

**Dependencies**: `ConfigService`, `StorageBackend`, `PartitionedStorageBackend`, `PartitionPathBuilder`  
**Tests**: `tests/test_partition_migration_service.py`, `tests/test_partition_migrate_cli.py`

Status: Implemented and validated (2025-10-19). Migration CLI supports plan persistence, parity verification, and per-venue toggles; operators should consult release notes before running in production.

## Data Flow

### Initialization Flow

```text
CLI → YFParqed.__init__
  ├─→ ConfigService(path)
  ├─→ TickerRegistry(config)
  ├─→ DataFetcher(limiter, today, empty_frame)
  ├─→ StorageBackend(empty_frame, normalizer, columns)
  ├─→ PartitionedStorageBackend(empty_frame, normalizer, columns, path_builder)
  └─→ IntervalScheduler(registry, intervals, limiter, processor)
```

### Update Data Flow

```text
update_stock_data(start_date, end_date)
  └─→ scheduler.run(start_date, end_date)
       ├─→ registry.load() [reload tickers]
       ├─→ For each interval:
       │    └─→ For each active ticker:
       │         ├─→ limiter() [rate limit]
       │         └─→ save_single_stock_data(ticker, dates, interval)
       │              ├─→ storage.read(...) [existing data via StorageRequest]
       │              ├─→ data_fetcher.fetch(...) [new data]
       │              ├─→ storage.save(new, existing, ...)
       │              └─→ registry.update_ticker_interval_status(...)
       └─→ [no save_tickers() during loop - deferred]
```

### Not-Found Maintenance Flow

```text
confirm_not_founds()
  └─→ registry.confirm_not_founds()
       ├─→ For each globally not-found ticker:
       │    ├─→ limiter() [rate limit]
       │    └─→ fetch_callback(ticker, "1d", "1d")
       │         └─→ If data found: update_ticker_interval_status(found=True)
       ├─→ save() [persist changes]
       └─→ reparse_not_founds() [chain to reactivation]

reparse_not_founds()
  └─→ registry.reparse_not_founds()
       ├─→ For each not-found ticker:
       │    └─→ If any interval has data < 90 days old:
       │         └─→ Reactivate ticker globally
       └─→ save() [persist changes]
```

### Partitioned Storage Workflow

```text
data/legacy/stocks_<interval> → partition-migrate CLI → partitioned layout → runtime toggle
```

1. Operators relocate legacy `stocks_<interval>/` folders under `data/legacy/`.
2. `yf-parqed-migrate init` writes a migration plan and enforces the directory layout.
3. `yf-parqed-migrate migrate` copies and verifies data, backfills `tickers.json` storage metadata, and flips per-source flags in `storage_config.json` once verification succeeds.
4. Subsequent `update-data` runs rely on `_build_storage_request` to route partitioned intervals through `PartitionedStorageBackend`; legacy intervals continue using `StorageBackend`.
5. Operators can override defaults with `yf-parqed partition-toggle` for global, market, or source scopes.

## Dependency Injection Patterns

All services use constructor injection for testability:

```python
# Example: DataFetcher
DataFetcher(
    limiter=lambda: self.enforce_limits(),        # Callable
    today_provider=lambda: self.get_today(),      # Callable
    empty_frame_factory=self._empty_price_frame,  # Callable
    ticker_factory=yf.Ticker                      # Optional override
)
```

This enables:

- **Mocking**: Tests inject fake implementations.
- **Flexibility**: Implementations are swappable without code changes.
- **Testing**: Services remain testable in isolation.

## Test Coverage

Total: **163 tests** across 17 test files.

- Integration: `tests/test_cli_integration.py`, `tests/test_update_end_to_end.py`.
- Services: `tests/test_config_service.py`, `tests/test_ticker_registry.py`, `tests/test_interval_scheduler.py`, `tests/test_data_fetcher.py`, `tests/test_partition_migration_service.py`.
- Storage Layers: `tests/test_storage_backend.py`, `tests/test_storage_operations.py`, `tests/test_partitioned_storage_backend.py`.
- Orchestration: `tests/test_update_loop.py`, `tests/test_partition_migrate_cli.py`.
- CLI: `tests/test_cli.py`.
- Infrastructure: `tests/test_rate_limits.py`.

## Future Enhancements

- **Migration UX**: Add dry-run/resume support and a guided rollback command for partition migrations.
- **DuckDB Query Layer**: Provide zero-copy analytics on the partitioned layout, for example:

  ```python
  import duckdb

  con = duckdb.connect()
  con.execute("SELECT * FROM 'data/us/yahoo/stocks_1d/**/*.parquet' WHERE ticker='AAPL'")
  ```

## Design Principles

1. **Single Responsibility**: Each service has one clear purpose.
2. **Dependency Inversion**: High-level modules depend on abstractions (callables).
3. **Open/Closed**: Extend behavior via new services rather than modifying callers.
4. **Testability**: Every service has dedicated unit tests.
5. **Backward Compatibility**: Public CLI and façade APIs remain stable.
6. **Fail-Safe**: Corruption recovery, schema validation, and cross-version support.
7. **Operational Guardrails**: Migration CLI validates disk space, directory layout, and checksum parity before activating partition mode.
