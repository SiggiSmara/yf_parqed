# yf_parqed Technical Overview

## Purpose

`yf_parqed` is a Python package for persistent, interval-aware storage and management of stock ticker data retrieved from Yahoo Finance. It is designed for robust, scalable, and auditable financial data collection, supporting both historical and live updates.

## Core Features

- **Unified Ticker Management**: All tickers are stored in a single `tickers.json` file, with metadata and interval-specific status tracking.
- **Interval-Aware Data**: Each ticker tracks its status and last data availability per interval (e.g., `1d`, `1h`).
- **Status Lifecycle**:
  - `active`: Ticker is valid and being traded (with per-interval last checked/found dates).
  - `not_found`: Ticker is globally excluded (e.g., delisted or never traded), with interval-specific not found tracking.
- **Data Storage**: Price and OHLCV data for each ticker/interval is stored in Parquet files under `stocks_<interval>/` directories.
- **Migration Support**: Includes scripts to migrate legacy JSON formats to the new unified, interval-aware format.
- **CLI and Automation**: Typer-based CLI for initialization, updating tickers, updating data, and confirming/reparsing not found tickers.
- **Testing**: Pytest-based test suite validates read/write logic, interval status management, and data integrity.

## Key Data Structures

### tickers.json

```json
{
  "AAPL": {
    "ticker": "AAPL",
    "added_date": "2024-01-15",
    "status": "active",
    "last_checked": "2024-01-20",
    "intervals": {
      "1d": {
        "status": "active",
        "last_found_date": "2024-01-20",
        "last_data_date": "2024-01-19",
        "last_checked": "2024-01-20"
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

### Parquet Data

- Stored in `stocks_<interval>/TICKER.parquet`
- Contains OHLCV and other time series data for each ticker/interval

- `src/yf_parqed/primary_class.py`: Main logic for ticker management, interval status, and data updates
- `src/yf_parqed/interval_scheduler.py`: Coordinates per-interval processing using injected services
- `migrate_ticker_files.py`: Migration script for legacy JSON formats
- `fix_migration.py`: Script to fix incomplete migrated data
- `tests/test_ticker_operations.py`: Test suite for ticker logic and data integrity

## Workflow

1. **Initialize**: Download and store tickers from exchanges
2. **Update Data**: For each interval, fetch and store new data, updating ticker/interval status
3. **Confirm Not Founds**: Periodically re-check globally not found tickers
4. **Reparse Not Founds**: Reactivate tickers if new data is found
5. **Migration**: Convert old formats to new structure as needed

## Core Execution Flow

- **Startup (`YFParqed.__init__`)**: Sets working path, loads/saves interval config (`intervals.json`), loads `tickers.json`, and configures API rate limiting.
- **Ticker Lifecycle**: `get_new_list_of_stocks` downloads NASDAQ/NYSE lists; `update_current_list_of_stocks` merges new tickers and reactivates previously not found entries.
- **Update Loop (`update_stock_data`)**: Reloads tickers, filters global active tickers, then for each configured interval processes only those allowed by `is_ticker_active_for_interval` (enforces 30-day cooldown after interval failures).
- **Per-Ticker Processing (`save_single_stock_data`)**: Reads existing parquet, determines fetch window, downloads fresh data via `get_yfinance_data`, merges with `save_yf`, and updates interval metadata with `update_ticker_interval_status` (marking success/failure per interval and adjusting global status when all intervals fail).
- **Not Found Maintenance**: `confirm_not_founds` rechecks globally not found tickers via the 1d interval, then `reparse_not_founds` reactivates any ticker with recent interval activity (<90 days).
- **Utility Helpers**: `get_yfinance_data` enforces Yahoo interval limits, `process_yfinance_data` normalizes responses, and `save_yf`/`read_yf` manage Parquet persistence.

## Extensibility

- Easily add new intervals or metadata fields
- Designed for integration with other financial data pipelines
- CLI and Python API for automation

## Dependencies

- `yfinance`, `pyarrow`, `loguru`, `rich`, `typer`, `pytest`

## Notes for Future Agents

- All ticker state and history lives in `tickers.json`; interval metadata drives every orchestration decision.
- Package management runs through `uv`; rely on `uv sync` / `uv run pytest` for restores and validation, and make dependency changes with `uv add` / `uv remove` rather than editing `pyproject.toml` by hand.
- Current automated coverage map:
  - `tests/test_ticker_operations.py`: ticker registry lifecycle, interval status transitions, not-found maintenance, metadata integrity.
  - `tests/test_storage_operations.py`: parquet merge/read helpers using Polars-backed fixtures, covering dedupe, empty files, and corruption cleanup.
  - `tests/test_update_loop.py`: full update-loop harness with mocks—happy path, cooldown skips, empty-fetch failure, multi-interval sequencing, persistence guards, and rate-limiter invocation.
  - `tests/test_cli.py`: Typer command smoke tests using a stubbed `YFParqed`, confirming wiring, limiter handoff, and not-found flags.
  - `tests/test_cli_integration.py`: end-to-end CLI run (real `YFParqed` in a temp workspace) exercising `initialize` + `update-data` with mocked Yahoo responses, verifying parquet output and interval metadata.
  - `tests/test_data_fetcher.py`: comprehensive DataFetcher unit tests covering initialization, window/period fetching, interval constraints (hourly 729-day limit, minute 7-day limit), DataFrame normalization (lowercase columns, stock symbol, timezone removal), error handling (exceptions, HTTPError), and rate limiter invocation.
  - `tests/test_storage_backend.py`: isolated StorageBackend unit tests covering read/save operations, corruption recovery, schema validation, deduplication logic, sequence preservation, and type normalization.
- These suites implement the planned bottom-up → top-down strategy; future additions should extend this matrix rather than replace it.

## Service-Oriented Refactoring (Steps 1.1-1.7) — COMPLETED ✅

Successfully transformed `primary_class.py` from a 700+ line monolith into a composable service-oriented architecture with 5 specialized services and a thin façade. All work completed on 2025-10-11.

**For detailed service specifications, data flows, and dependency injection patterns, see [ARCHITECTURE.md](ARCHITECTURE.md).**

### Services Overview

The refactoring extracted these services (see ARCHITECTURE.md for detailed API reference):

1. **ConfigService** (79 lines) - Environment and configuration management
2. **TickerRegistry** (215 lines) - Ticker lifecycle and not-found maintenance  
3. **IntervalScheduler** (95 lines) - Update loop orchestration
4. **DataFetcher** (126 lines) - Yahoo Finance API abstraction
5. **StorageBackend** (116 lines) - Parquet I/O with corruption recovery
6. **YFParqed** (485 lines) - Thin façade wiring dependencies

### Achievements

- **109 tests passing** (up from 80 at project start)
- **100% backward compatibility** — no breaking changes to CLI or API
- **Clean architecture** — single responsibility, dependency injection, testability
- **Comprehensive coverage** — unit, integration, and end-to-end tests
- **Documentation** — `ARCHITECTURE.md` added with diagrams and service responsibilities

## Recently Completed (Other Enhancements)

- **Rate-limiter stress testing**: Bursty workload simulation with enforced delays. ✅ `tests/test_rate_limits.py` (2025-10-11)
- **CLI option coverage**: Date range flags and environment variable handling. ✅ `tests/test_cli.py` expansions (2025-10-11)
- **Storage edge cases**: Descending sequences, partial writes, dtype normalization. ✅ `tests/test_storage_operations.py` (2025-10-11)

## Future Improvements

**For detailed enhancement proposals with code examples, see [ARCHITECTURE.md](ARCHITECTURE.md#future-enhancements).**

1. **Partition-aware storage**: Organize parquet files by ticker/date hierarchy to minimize corruption risk and enable incremental backups
2. **DuckDB analytics layer**: Add zero-copy querying on partitioned parquet for interactive analysis without ETL

## Process Commitments

- Adopt test-driven development across new work going forward; use failing tests to codify desired behavior before implementation while keeping the expanded regression suite green.
