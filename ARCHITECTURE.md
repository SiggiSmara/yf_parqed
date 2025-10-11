# yf_parqed Architecture

**For development history and agent-specific context, see [AGENTS.md](AGENTS.md).**

## Service-Oriented Design

The codebase follows a service-oriented architecture with clear separation of concerns:

```
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
    │ Service  │ │ Registry │ │Scheduler │ │ Fetcher  │ │ Backend  │
    └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘
```

## Service Responsibilities

### ConfigService
**Purpose**: Environment and configuration management  
**File**: `src/yf_parqed/config_service.py`  
**Key Methods**:
- `set_working_path()`: Change base directory
- `load_intervals()` / `save_intervals()`: Interval configuration
- `load_tickers()` / `save_tickers()`: Ticker JSON I/O
- `configure_limits()`: Rate limiting parameters
- `get_now()` / `format_date()`: Deterministic timestamps

**Dependencies**: None (bottom layer)  
**Tests**: `tests/test_config_service.py` (12 tests)

### TickerRegistry
**Purpose**: Ticker lifecycle and interval metadata management  
**File**: `src/yf_parqed/ticker_registry.py`  
**Key Methods**:
- `load()` / `save()`: Ticker data persistence
- `update_current_list()`: Merge new tickers, reactivate not-founds
- `is_active_for_interval()`: Check ticker/interval eligibility
- `update_ticker_interval_status()`: Record fetch success/failure
- `replace()`: Bulk ticker updates
- `confirm_not_founds()`: Re-check globally not-found tickers
- `reparse_not_founds()`: Reactivate tickers with recent interval data

**Dependencies**: `ConfigService` (for timestamps), optional limiter and fetch_callback  
**Tests**: `tests/test_ticker_registry.py` (7 tests), `tests/test_ticker_operations.py` (23 tests)

### IntervalScheduler
**Purpose**: Orchestrate update loop across intervals  
**File**: `src/yf_parqed/interval_scheduler.py`  
**Key Methods**:
- `run()`: Execute update workflow for all intervals/tickers

**Dependencies**: `TickerRegistry`, limiter, processor callback  
**Tests**: `tests/test_interval_scheduler.py` (2 tests)

### DataFetcher
**Purpose**: Yahoo Finance API interactions  
**File**: `src/yf_parqed/data_fetcher.py`  
**Key Methods**:
- `fetch()`: Retrieve OHLCV data for ticker/interval/date range
- `_fetch_window()`: Specific date range
- `_fetch_all()`: Maximum available history
- `_apply_interval_constraints()`: Yahoo API limits (729d hourly, 7d minute)
- `_normalize_dataframe()`: Lowercase columns, timezone removal, multi-index

**Dependencies**: `yfinance`, limiter, date provider  
**Tests**: `tests/test_data_fetcher.py` (18 tests)

### StorageBackend
**Purpose**: Parquet persistence with corruption recovery  
**File**: `src/yf_parqed/storage_backend.py`  
**Key Methods**:
- `read()`: Load parquet with schema validation
- `save()`: Merge, deduplicate, persist
- `_remove_file()`: Cross-version pathlib cleanup

**Dependencies**: Empty frame factory, normalizer, column provider  
**Tests**: `tests/test_storage_backend.py` (11 tests)

## Data Flow

### Initialization Flow
```
CLI → YFParqed.__init__
  ├─→ ConfigService(path)
  ├─→ TickerRegistry(config)
  ├─→ DataFetcher(limiter, today, empty_frame)
  ├─→ StorageBackend(empty_frame, normalizer, columns)
  └─→ IntervalScheduler(registry, intervals, limiter, processor)
```

### Update Data Flow
```
update_stock_data(start_date, end_date)
  └─→ scheduler.run(start_date, end_date)
       ├─→ registry.load() [reload tickers]
       ├─→ For each interval:
       │    └─→ For each active ticker:
       │         ├─→ limiter() [rate limit]
       │         └─→ save_single_stock_data(ticker, dates, interval)
       │              ├─→ storage.read(parquet_path) [existing data]
       │              ├─→ data_fetcher.fetch(...) [new data]
       │              ├─→ storage.save(new, existing, path)
       │              └─→ registry.update_ticker_interval_status(...)
       └─→ [no save_tickers() during loop - deferred]
```

### Not-Found Maintenance Flow
```
confirm_not_founds()
  └─→ registry.confirm_not_founds()
       ├─→ For each globally not-found ticker:
       │    ├─→ limiter() [rate limit]
       │    └─→ fetch_callback(ticker, "1d", "1d") [try to fetch data]
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
- **Mocking**: Tests inject fake implementations
- **Flexibility**: Swap implementations without code changes
- **Testing**: Services testable in isolation

## Test Coverage

Total: **109 tests** across 14 test files

- Integration: `test_cli_integration.py`, `test_update_end_to_end.py`
- Services: `test_config_service.py`, `test_ticker_registry.py`, `test_interval_scheduler.py`, `test_data_fetcher.py`, `test_storage_backend.py`
- Operations: `test_ticker_operations.py`, `test_storage_operations.py`, `test_update_loop.py`
- CLI: `test_cli.py`
- Infrastructure: `test_rate_limits.py`

## Future Enhancements

### Partition-Aware Storage
The current `StorageBackend` can be swapped for a partitioned version:
```
stocks_1d/
  ticker=AAPL/
    year=2025/
      month=10/
        day=11/
          data.parquet
```

Benefits:
- Minimize corruption blast radius
- Enable incremental backups
- Simplify selective rewrites
- Better parallelization

### DuckDB Query Layer
Add zero-copy analytics on partitioned parquet:
```python
import duckdb
con = duckdb.connect()
con.execute("SELECT * FROM 'stocks_1d/**/*.parquet' WHERE ticker='AAPL'")
```

## Design Principles

1. **Single Responsibility**: Each service has one clear purpose
2. **Dependency Inversion**: High-level modules depend on abstractions (callables)
3. **Open/Closed**: Extend via new services, not modifications
4. **Testability**: Every service has comprehensive unit tests
5. **Backward Compatibility**: Public API unchanged during refactor
6. **Fail-Safe**: Corruption recovery, schema validation, cross-version support

