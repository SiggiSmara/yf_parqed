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

## 🚨 CRITICAL: Data Safety for Storage Changes

**yf_parqed stores financial data with LIMITED RECOVERY WINDOWS:**
- Yahoo Finance 1m data: **7-day expiry** (permanently lost after)
- Xetra raw trades: **24-hour expiry** (permanently lost after)

**Before making ANY changes to data folder structure or storage paths:**

1. **READ THE COMPREHENSIVE GUIDE**: [`DATA_SAFETY_STRATEGY.md`](DATA_SAFETY_STRATEGY.md)
2. **Follow the 10 mandatory rules** (non-destructive by default, verification before activation, atomic operations, etc.)
3. **Use the pre-flight checklist** before proposing storage changes
4. **When in doubt, STOP and ASK** - data loss is irreversible

**Quick safety rules:**
- Never destructive by default (coexist old + new layouts)
- Verify checksums/row counts before activating new layout
- Support reading from both old and new layouts forever
- Use staging directories + atomic operations
- Provide rollback capability
- Validate disk space (require 2.5x source size)

**Golden Rule: Storage structure changes require explicit migration strategy with rollback capability.**

---

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

## Development Workflows

**For detailed workflows, see [`DEVELOPMENT_GUIDE.md`](DEVELOPMENT_GUIDE.md)**

Quick reference:
- **Standard workflow**: `uv sync` → make changes → `uv run pytest` → `uv run ruff check . --fix && uv run ruff format .` → commit
- **Adding services**: Create in `src/yf_parqed/` → inject dependencies → add tests → wire into `primary_class.py` → update `ARCHITECTURE.md`
- **Ticker logic**: Edit `ticker_registry.py`, test in `test_ticker_registry.py`, orchestration in `interval_scheduler.py`
- **Storage changes**: Follow [DATA_SAFETY_STRATEGY.md](DATA_SAFETY_STRATEGY.md) mandatory rules

## Testing

**For comprehensive testing guide, see [`TESTING_GUIDE.md`](TESTING_GUIDE.md)**

**Test Coverage**: 183 Yahoo Finance tests + 129 Xetra tests = 312 total, 100% pass rate required

Quick reference:
- Run all: `uv run pytest`
- Specific file: `uv run pytest tests/test_ticker_operations.py`
- With coverage: `uv run pytest --cov=yf_parqed --cov-report=term-missing`
- Always use `tmp_path` fixture for file I/O
- Mock external APIs: `limiter=lambda: None` for rate limiter
- Test both storage backends when modifying storage code

## Troubleshooting

**For common issues and solutions, see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md)**

Quick fixes:
- **Tests fail after pull**: `uv sync` then `rm -rf .pytest_cache && uv run pytest`
- **Import errors**: `uv sync` then verify with `uv run python -c "import yf_parqed"`
- **Rate limit (429)**: Increase delay `--limits 2 3` or wait 15-30 minutes
- **Corrupt parquet**: Auto-recovers (check logs), or manually delete and re-fetch
- **Daemon won't start**: Check for stale PID `cat /tmp/yf-parqed.pid` and remove if process not running

## Pre-Commit Checklist

Before submitting changes:

- [ ] `uv sync` completes successfully
- [ ] `uv run pytest` - all 312 tests pass (183 Yahoo + 129 Xetra)
- [ ] `uv run ruff check . --fix` - no lint errors
- [ ] `uv run ruff format .` - code is formatted
- [ ] No temp files or test artifacts in repo
- [ ] Documentation updated if public API changed
- [ ] Test coverage maintained for new code

## Additional Resources

### Core Documentation
- **Architecture**: `ARCHITECTURE.md` - Service responsibilities, data flows, dependency injection patterns
- **Development**: `.github/DEVELOPMENT_GUIDE.md` - Workflows, adding services, CLI commands, git process
- **Testing**: `.github/TESTING_GUIDE.md` - Test structure, patterns, debugging, coverage
- **Troubleshooting**: `.github/TROUBLESHOOTING.md` - Common issues and solutions
- **Data Safety**: `.github/DATA_SAFETY_STRATEGY.md` - Mandatory rules for storage changes

### Project History & Planning
- **History**: `AGENTS.md` - Refactoring timeline, test coverage evolution
- **Release Notes**: `docs/release-notes.md` - Version history and migration guidance
- **Roadmap**: `docs/roadmap.md` - Completed and upcoming features
- **ADRs**: `docs/adr/` - Architectural decision records

### Quick Start
When in doubt: `uv sync && uv run pytest` to verify environment.
For storage changes: Read DATA_SAFETY_STRATEGY.md first.
For development tasks: Refer to DEVELOPMENT_GUIDE.md workflows.
