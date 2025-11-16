# GitHub Copilot Instructions for yf_parqed

## Repository Overview

**yf_parqed** is a Python package for persistent, interval-aware storage of stock ticker data from Yahoo Finance. It stores OHLCV data in Parquet files with robust ticker lifecycle management, rate limiting, and corruption recovery.

- **Size**: ~600 KB source code, 183 tests across 17 test files
- **Language**: Python 3.12+
- **Package Manager**: `uv` (Astral's fast Python package manager)
- **Key Dependencies**: `yfinance`, `pandas`, `pyarrow`, `typer`, `loguru`, `rich`, `httpx`
- **Testing**: `pytest` with 183 passing tests (100% pass rate required)
- **Linting**: `ruff` (linter + formatter)
- **Pre-commit**: `uv-pre-commit` with `ruff` hooks

## Critical Build & Test Instructions

### Environment Setup

**ALWAYS run `uv sync` before any other command.** This is mandatory for restoring the virtual environment and dependencies.

```bash
uv sync
```

- Takes ~25ms when dependencies are cached
- Creates/updates `.venv/` with all production and dev dependencies
- Must be run after fresh clone or after pulling dependency changes

### Dependency Management

**NEVER edit `pyproject.toml` or `uv.lock` manually.** Use `uv` commands:

```bash
# Add a new dependency
uv add <package-name>

# Add a dev dependency
uv add --dev <package-name>

# Remove a dependency
uv remove <package-name>

# Update dependencies
uv lock --upgrade
```

### Running Tests

**ALWAYS run the full test suite before submitting changes.** All 183 tests must pass.

```bash
# Run all tests (required before committing)
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_ticker_operations.py

# Run with coverage
uv run pytest --cov=yf_parqed --cov-report=term-missing
```

- Test execution time: ~11 seconds for full suite
- **Zero failures tolerated** - fix all failures before proceeding
- Tests automatically clean up temp files and directories

### Linting & Formatting

**ALWAYS run linting before committing.** Ruff enforces code style.

```bash
# Check for lint errors (do this first)
uv run ruff check .

# Auto-fix lint errors
uv run ruff check . --fix

# Check formatting
uv run ruff format --check .

# Apply formatting
uv run ruff format .
```

**Common workflow:**
```bash
uv run ruff check . --fix && uv run ruff format .
```

### Pre-commit Hooks

Pre-commit hooks run `uv lock` validation and `ruff` checks. Install with:

```bash
uv tool install pre-commit --with pre-commit-uv --force-reinstall
pre-commit install
```

Hooks defined in `.pre-commit-config.yaml`:
- `uv-lock` - validates `uv.lock` is up to date
- `ruff` - linter with auto-fix
- `ruff-format` - code formatter

### Running the CLI

The package provides two CLI entry points:

```bash
# Main CLI for data operations
uv run yf-parqed --help

# Migration CLI for partition storage
uv run yf-parqed-migrate --help
```

**Note**: The CLI initializes logging on startup, producing INFO-level output by default.

## Project Architecture

### Service-Oriented Design

The codebase follows a clean service-oriented architecture with dependency injection:

```
YFParqed (façade) → ConfigService, TickerRegistry, IntervalScheduler, 
                     DataFetcher, StorageBackend, PartitionedStorageBackend
```

**Key Service Files** (`src/yf_parqed/`):
- `primary_class.py` - Main façade (586 lines), wires all services
- `config_service.py` - Environment and configuration management
- `ticker_registry.py` - Ticker lifecycle, interval status tracking
- `interval_scheduler.py` - Update loop orchestration
- `data_fetcher.py` - Yahoo Finance API abstraction with rate limiting
- `storage_backend.py` - Legacy parquet I/O with corruption recovery
- `partitioned_storage_backend.py` - Hive-style partition storage
- `partition_migration_service.py` - Migration between storage backends
- `partition_path_builder.py` - Path construction for partitioned storage
- `run_lock.py` - Global run-lock for concurrent safety
- `main.py` - Typer CLI definitions

**Entry Points**:
- `src/yf_parqed/main.py` - Primary CLI (`yf-parqed`)
- `src/yf_parqed/tools/partition_migrate.py` - Migration CLI (`yf-parqed-migrate`)

### Key Data Structures

**tickers.json** - Single source of truth for ticker state:
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
        "storage": {"backend": "partitioned", "market": "us", "source": "yahoo"}
      }
    }
  }
}
```

**intervals.json** - Configured intervals to fetch (e.g., `["1d", "1h", "1m"]`)

**storage_config.json** - Backend selection (legacy vs partitioned)

### Storage Backends

**Legacy**: `stocks_<interval>/<TICKER>.parquet` (flat structure)

**Partitioned**: `data/<market>/<source>/stocks_<interval>/ticker=<TICKER>/year=YYYY/month=MM/<file>.parquet` (Hive-style)

Migration flow: Legacy → `data/legacy/` → Migration CLI → Partitioned layout

### Test Organization

**Test Structure** (`tests/`):
- `test_config_service.py` - Configuration management (11 tests)
- `test_ticker_registry.py` - Ticker lifecycle (10 tests)
- `test_ticker_operations.py` - Interval status, not-found maintenance (15 tests)
- `test_interval_scheduler.py` - Update orchestration (5 tests)
- `test_data_fetcher.py` - Yahoo API abstraction (12 tests)
- `test_storage_backend.py` - Legacy storage (15 tests)
- `test_storage_operations.py` - Parquet merge/dedup (8 tests)
- `test_partitioned_storage_backend.py` - Partition storage (20 tests)
- `test_partition_migration_service.py` - Migration logic (12 tests)
- `test_update_loop.py` - Full update harness (16 tests)
- `test_cli.py` - CLI command smoke tests (10 tests)
- `test_cli_integration.py` - End-to-end CLI (5 tests)
- `test_rate_limits.py` - Rate limiter (6 tests)
- Additional: locks, partition writes, cleanup, migration CLI

**Testing Strategy**: Bottom-up (unit) → top-down (integration) → end-to-end

## Common Workflows

### Making Code Changes

1. **Before starting**: `uv sync`
2. **Make changes** to source files
3. **Run tests**: `uv run pytest`
4. **Fix linting**: `uv run ruff check . --fix && uv run ruff format .`
5. **Verify**: `uv run pytest` again
6. **Commit** changes

### Adding a New Service

1. Create service file in `src/yf_parqed/`
2. Use constructor injection for dependencies
3. Add unit tests in `tests/test_<service>.py`
4. Wire into `YFParqed` façade in `primary_class.py`
5. Update `ARCHITECTURE.md` with service description
6. Run full test suite to ensure integration works

### Modifying Ticker Logic

- **State changes**: Edit `ticker_registry.py`
- **Tests**: Update `test_ticker_registry.py` or `test_ticker_operations.py`
- **Execution flow**: Check `interval_scheduler.py` for orchestration
- **Persistence**: Handled automatically by `ConfigService`

### Storage Backend Changes

- **Legacy**: Modify `storage_backend.py`
- **Partitioned**: Modify `partitioned_storage_backend.py`
- **Path logic**: Edit `partition_path_builder.py`
- **Migration**: Update `partition_migration_service.py`
- **Tests**: Add cases to `test_storage_backend.py` or `test_partitioned_storage_backend.py`

## Known Gotchas & Workarounds

### 1. Rate Limiting is Always Active

The Yahoo Finance API has strict rate limits. Default: 3 requests per 2 seconds.

- **Do NOT** disable rate limiting in tests or production
- Mock the limiter in unit tests instead: `limiter=lambda: None`
- Adjust via CLI: `yf-parqed --limits 3 2 update-data`

### 2. Ticker State is Global

All ticker metadata lives in `tickers.json`. Interval-specific state is nested under `intervals.<interval_name>`.

- **Do NOT** create separate per-interval ticker files
- **Do** use `TickerRegistry.is_active_for_interval()` for eligibility checks
- Cooldown period: 30 days after interval-specific failures

### 3. Test Isolation Requires Temp Directories

Most tests use `tmp_path` fixtures to avoid cross-test pollution.

- **Always** use `tmp_path` for file I/O in tests
- **Never** write to the repo root during tests
- Cleanup is automatic via pytest fixtures

### 4. Parquet Corruption Recovery

Both storage backends handle corrupt parquet files by deleting them and retrying.

- **Expected behavior**: Warning log + file deletion + retry
- **Do NOT** disable corruption recovery
- **Do** test with `StorageBackend._create_corrupt_file()` helper in tests

### 5. Storage Backend Selection

Runtime storage backend is determined by:
1. Interval-specific `storage` metadata in `tickers.json`
2. Global/market/source flags in `storage_config.json`
3. Fallback to legacy backend

- **Check** `_build_storage_request()` in `primary_class.py` for routing logic
- **Test** both backends when modifying storage code

### 6. Migration CLI Directory Layout

Migration CLI **requires** legacy data under `data/legacy/`:

```bash
# WRONG - will fail
stocks_1d/

# CORRECT - required layout
data/legacy/stocks_1d/
```

Move legacy folders manually before running `yf-parqed-migrate init`.

### 7. uv Tool Installation

Pre-commit requires a specific installation command:

```bash
# Correct installation
uv tool install pre-commit --with pre-commit-uv --force-reinstall

# Then install hooks
pre-commit install
```

### 8. Python Version

**Requires Python 3.12+**. Check with:

```bash
python --version  # Should be 3.12 or higher
uv python list    # Shows available Python versions
```

## Validation Checklist

Before submitting changes, verify:

- [ ] `uv sync` completes successfully
- [ ] `uv run pytest` - all 183 tests pass
- [ ] `uv run ruff check . --fix` - no lint errors
- [ ] `uv run ruff format .` - code is formatted
- [ ] No temp files or test artifacts in repo
- [ ] Documentation updated if public API changed
- [ ] Test coverage maintained for new code

## Additional Resources

- **Architecture Details**: See `ARCHITECTURE.md` for service responsibilities and data flows
- **Development History**: See `AGENTS.md` for refactoring timeline and test coverage map
- **Release Notes**: See `docs/release-notes.md` for version history
- **ADRs**: See `docs/adr/` for architectural decisions (partition storage, DuckDB, etc.)

## Trust These Instructions

**These instructions have been validated against the actual codebase.** Commands have been tested and confirmed working. Only search the codebase if:
- Information here is incomplete or unclear
- You need implementation details beyond what's documented
- You encounter errors not described in the gotchas section

When in doubt, start with `uv sync` and `uv run pytest` to verify your environment is correct.
