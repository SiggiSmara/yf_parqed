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

## Main Components

- `src/yf_parqed/primary_class.py`: Main logic for ticker management, interval status, and data updates
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
- These suites implement the planned bottom-up → top-down strategy; future additions should extend this matrix rather than replace it.

## Work In Progress

1. Gradually refactor `primary_class.py` into composable services (config/environment, ticker registry, interval scheduler, data fetcher, storage backend, not-found maintenance) so the current workflow is orchestrated by a thin façade, easing the later switch to partition-aware storage.

  1. Extract configuration and environment concerns into a `ConfigService` responsible for path management, intervals loading/saving, and rate limiter wiring.
  1. Introduce a `TickerRegistry` module handling JSON serialization, ticker lifecycle mutations, and interval metadata updates, leaving orchestration to lean call sites.
  1. Isolate the update loop into an `IntervalScheduler` that decides which tickers/intervals run, relying on injected dependencies (`Limiter`, `TickerRegistry`, `DataFetcher`).
  1. Create a `DataFetcher` abstraction wrapping Yahoo interactions (`get_yfinance_data`, `process_yfinance_data`, rate-limit enforcement) with injectable stubs for tests.
  1. Move parquet I/O (`read_yf`, `save_yf`) to a `StorageBackend` that exposes typed operations, enabling future partitioning strategies without touching business logic.
  1. Compose these services through a thin `YFParqed` façade that wires dependencies, keeping CLI/API surface untouched while enabling incremental rewrites and focused tests.

## Recently Completed

- Rate-limiter stress scenario that simulates bursty workloads and asserts enforced delays via patched clocks. ✅ Covered by `tests/test_rate_limits.py::test_enforce_limits_handles_bursty_sequence` on 2025-10-11.
- CLI smoke permutations covering option flags not yet exercised in `tests/test_cli.py`. ✅ Covered by `tests/test_cli.py::test_update_data_accepts_date_range`, `tests/test_cli.py::test_update_data_requires_both_dates`, and `tests/test_cli.py::test_global_options_apply_log_level_env` on 2025-10-11.
- Storage edge cases around descending sequence numbers, partial parquet writes, and dtype drift. ✅ Covered by `tests/test_storage_operations.py::test_save_yf_preserves_higher_sequence_values`, `tests/test_storage_operations.py::test_read_yf_resets_partial_files_missing_columns`, and `tests/test_storage_operations.py::test_save_yf_normalizes_numeric_types` on 2025-10-11.
- Expand automated coverage: add end-to-end `update_stock_data` tests, interval cooldown edge cases, not-found lifecycle mocks, rate-limiter behavior, storage dedupe edge cases, and CLI command smoke tests. ✅ Covered by the combined additions to `tests/test_update_end_to_end.py`, `tests/test_update_loop.py`, `tests/test_ticker_operations.py`, `tests/test_rate_limits.py`, `tests/test_storage_operations.py`, and `tests/test_cli.py` on 2025-10-11.

## Future Improvements

1. Partition per ticker/interval data by transaction date (e.g., `stocks_1d/ticker=XYZ/year=2025/month=10/day=11/`) to minimize corruption risk, enable incremental backups, and simplify selective rewrites.
2. Add a DuckDB query layer on top of the partitioned parquet store for zero-copy analytics, predicate pushdown, and interactive reporting without bespoke ETL.

## Process Commitments

- Adopt test-driven development across new work going forward; use failing tests to codify desired behavior before implementation while keeping the expanded regression suite green.
