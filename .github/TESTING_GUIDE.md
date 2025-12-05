# Testing Guide for yf_parqed

Comprehensive guide to the test suite structure, patterns, and best practices.

## Test Suite Overview

- **Total Tests**: 183 (Yahoo Finance) + 129 (Xetra) = 312 tests
- **Execution Time**: ~11 seconds (full suite)
- **Pass Rate**: 100% required before commits
- **Coverage Target**: >80% for new code

## Test Organization

### Test File Structure

Tests are organized by component responsibility, following a bottom-up → top-down → end-to-end strategy.

#### Service Layer Tests (Unit)

| Test File | Component | Tests | Focus |
|-----------|-----------|-------|-------|
| `test_config_service.py` | ConfigService | 11 | Config loading, saving, path management |
| `test_ticker_registry.py` | TickerRegistry | 10 | Ticker lifecycle, state transitions |
| `test_ticker_operations.py` | Ticker logic | 15 | Interval status, not-found maintenance |
| `test_interval_scheduler.py` | IntervalScheduler | 5 | Update orchestration, filtering |
| `test_data_fetcher.py` | DataFetcher | 12 | Yahoo API abstraction, rate limiting |
| `test_storage_backend.py` | StorageBackend | 15 | Legacy storage, corruption recovery |
| `test_storage_operations.py` | Storage helpers | 8 | Parquet merge, deduplication |
| `test_partitioned_storage_backend.py` | PartitionedStorage | 20 | Hive-style storage, partition logic |
| `test_partition_migration_service.py` | Migration | 12 | Legacy→Partitioned migration |

#### Orchestration Layer Tests (Integration)

| Test File | Component | Tests | Focus |
|-----------|-----------|-------|-------|
| `test_update_loop.py` | Update pipeline | 16 | Full update harness with mocks |
| `test_partition_migrate_cli.py` | Migration CLI | 12 | CLI workflow, plan management |
| `test_rate_limits.py` | Rate limiter | 6 | Burst workloads, delay enforcement |

#### End-to-End Tests

| Test File | Component | Tests | Focus |
|-----------|-----------|-------|-------|
| `test_cli.py` | CLI commands | 10 | Command smoke tests |
| `test_cli_integration.py` | Full workflow | 5 | Real YFParqed with temp workspace |
| `test_update_end_to_end.py` | Update flow | 8 | Initialize → update → verify |

#### Infrastructure Tests

| Test File | Component | Tests | Focus |
|-----------|-----------|-------|-------|
| `test_run_lock.py` | GlobalRunLock | 8 | Lock acquisition, cleanup |
| `test_run_lock_cli.py` | Lock CLI | 4 | CLI lock integration |
| `test_partition_write_hardening.py` | Atomic writes | 6 | fsync, partial writes, recovery |
| `test_cleanup_expanded.py` | Cleanup logic | 8 | Temp file removal, corruption |

#### Xetra-Specific Tests

| Test File | Component | Tests | Focus |
|-----------|-----------|-------|-------|
| `test_xetra_fetcher.py` | XetraFetcher | 37 | HTTP downloads, rate limiting |
| `test_xetra_parser.py` | XetraParser | 23 | JSON→DataFrame, schema validation |
| `test_xetra_service.py` | XetraService | 18 | Orchestration, consolidation |
| `test_xetra_cli.py` | Xetra CLI | 22 | CLI commands, intelligent date detection |
| `test_xetra_integration.py` | End-to-end | 4 | Full fetch→parse→store workflow |
| `test_xetra_consolidation.py` | Consolidation | 16 | Daily→monthly aggregation |
| `test_xetra_daemon_integration.py` | Daemon mode | 18 | PID management, signal handling |
| `test_trading_hours_checker.py` | Trading hours | 34 | Timezone conversion, DST |

---

## Running Tests

### Basic Commands

```bash
# Run all tests (required before commits)
uv run pytest

# Verbose output
uv run pytest -v

# Specific test file
uv run pytest tests/test_ticker_operations.py

# Specific test function
uv run pytest tests/test_ticker_operations.py::test_ticker_activation

# With coverage report
uv run pytest --cov=yf_parqed --cov-report=term-missing
```

### Advanced Options

```bash
# Stop on first failure
uv run pytest -x

# Last failed tests only
uv run pytest --lf

# Parallel execution (requires pytest-xdist)
uv run pytest -n auto

# Show print statements
uv run pytest -s

# Show slowest tests
uv run pytest --durations=10
```

### Filtering by Markers

```bash
# Run only unit tests
uv run pytest -m "not integration"

# Run only integration tests
uv run pytest -m integration

# Skip slow tests
uv run pytest -m "not slow"
```

---

## Test Patterns

### 1. Temporary Directory Isolation

**Always use `tmp_path` for file I/O to avoid test pollution.**

```python
def test_with_temp_dir(tmp_path):
    # tmp_path is a pytest fixture providing isolated directory
    config = ConfigService(working_path=tmp_path)
    config.save_tickers({"AAPL": {"status": "active"}})
    
    # Verify file creation
    assert (tmp_path / "tickers.json").exists()
    
    # Cleanup is automatic - no need to delete files
```

### 2. Mocking External Dependencies

**Mock external APIs and services to avoid network calls and flaky tests.**

```python
from unittest.mock import MagicMock
import pandas as pd

def test_data_fetcher_with_mock():
    # Mock yfinance.Ticker
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame({
        "Open": [100.0],
        "High": [105.0],
        "Low": [99.0],
        "Close": [103.0],
        "Volume": [1000000]
    })
    
    # Inject mock via constructor
    fetcher = DataFetcher(
        limiter=lambda: None,  # Disable rate limiting
        ticker_factory=lambda symbol: mock_ticker
    )
    
    result = fetcher.fetch("AAPL", "1d")
    assert not result.empty
    assert result["close"].iloc[0] == 103.0
```

### 3. Testing Both Storage Backends

**Use parametrize to test both legacy and partitioned storage.**

```python
import pytest

@pytest.mark.parametrize("backend_type", ["legacy", "partitioned"])
def test_storage_read_write(tmp_path, backend_type):
    # Setup backend based on parameter
    if backend_type == "legacy":
        backend = StorageBackend(
            root_path=tmp_path,
            empty_frame_factory=empty_frame,
            normalizer=normalize,
            column_provider=get_columns
        )
    else:
        path_builder = PartitionPathBuilder(tmp_path)
        backend = PartitionedStorageBackend(
            path_builder=path_builder,
            empty_frame_factory=empty_frame,
            normalizer=normalize,
            column_provider=get_columns
        )
    
    # Test applies to both backends
    df = create_test_data()
    backend.save(df, interval="1d", ticker="AAPL", 
                 market="us", source="yahoo")
    
    result = backend.read(interval="1d", ticker="AAPL",
                          market="us", source="yahoo")
    
    assert len(result) == len(df)
    assert result["close"].iloc[0] == df["close"].iloc[0]
```

### 4. Fixture Reuse

**Create reusable fixtures for common test setups.**

```python
import pytest

@pytest.fixture
def yf_parqed_instance(tmp_path):
    """Provides a fully initialized YFParqed instance in temp directory."""
    yf = YFParqed(working_path=tmp_path)
    yf.config.save_intervals(["1d"])
    yf.config.save_tickers({
        "AAPL": {"ticker": "AAPL", "status": "active", "intervals": {}}
    })
    return yf

def test_using_fixture(yf_parqed_instance):
    # Instance is ready to use
    assert yf_parqed_instance.config.load_intervals() == ["1d"]
```

### 5. Testing Corruption Recovery

**Simulate corrupt files to verify recovery behavior.**

```python
def test_corruption_recovery(tmp_path):
    backend = StorageBackend(root_path=tmp_path, ...)
    
    # Create corrupt file
    corrupt_path = tmp_path / "stocks_1d" / "AAPL.parquet"
    corrupt_path.parent.mkdir(parents=True)
    corrupt_path.write_bytes(b"not a valid parquet file")
    
    # Backend should detect and delete corrupt file
    result = backend.read(interval="1d", ticker="AAPL")
    
    # Returns empty DataFrame, corrupt file is deleted
    assert result.empty
    assert not corrupt_path.exists()
```

### 6. Testing CLI Commands

**Use Typer's CliRunner for CLI integration tests.**

```python
from typer.testing import CliRunner
from yf_parqed.yfinance_cli import app

def test_cli_command():
    runner = CliRunner()
    
    # Invoke command
    result = runner.invoke(app, ["initialize", "--help"])
    
    # Verify exit code and output
    assert result.exit_code == 0
    assert "Download ticker lists" in result.stdout
```

### 7. Testing Async/Daemon Operations

**Mock time and signals for daemon tests.**

```python
from unittest.mock import patch
import signal

def test_daemon_shutdown():
    service = XetraService()
    
    # Mock signal handler
    with patch('signal.signal') as mock_signal:
        service.start_daemon(interval=1)
        
        # Verify SIGTERM handler registered
        mock_signal.assert_called_with(signal.SIGTERM, service._shutdown_handler)
```

---

## Writing New Tests

### Test Naming Conventions

```python
# Good - describes what is tested and expected outcome
def test_ticker_registry_activates_not_found_ticker_when_data_found():
    pass

def test_storage_backend_deletes_corrupt_parquet_file():
    pass

def test_data_fetcher_respects_rate_limit_between_requests():
    pass

# Bad - vague or unclear
def test_ticker_stuff():
    pass

def test_backend():
    pass
```

### Test Structure (AAA Pattern)

```python
def test_example():
    # Arrange - setup test conditions
    config = ConfigService(working_path=tmp_path)
    registry = TickerRegistry(config)
    
    # Act - perform the action being tested
    registry.add_ticker("AAPL")
    
    # Assert - verify expected outcome
    assert "AAPL" in registry.tickers
    assert registry.tickers["AAPL"]["status"] == "active"
```

### Testing Exceptions

```python
import pytest

def test_invalid_interval_raises_error():
    with pytest.raises(ValueError, match="Invalid interval"):
        fetcher.fetch("AAPL", "invalid_interval")
```

### Testing State Transitions

```python
def test_ticker_lifecycle():
    registry = TickerRegistry(config)
    
    # Initial state
    registry.add_ticker("AAPL")
    assert registry.is_active("AAPL")
    
    # Transition to not_found
    registry.mark_not_found("AAPL", "1d")
    assert not registry.is_active_for_interval("AAPL", "1d")
    
    # Reactivation
    registry.reactivate_ticker("AAPL")
    assert registry.is_active("AAPL")
```

---

## Test Coverage

### Checking Coverage

```bash
# Generate HTML coverage report
uv run pytest --cov=yf_parqed --cov-report=html

# Open in browser
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### Coverage Goals

- **Services**: >90% coverage (core business logic)
- **CLI**: >80% coverage (command handlers)
- **Storage**: 100% coverage (data safety critical)
- **Utils**: >70% coverage (helper functions)

### Excluding from Coverage

```python
# In code, use pragma comment for unreachable code
if TYPE_CHECKING:  # pragma: no cover
    from typing import ...
```

---

## Debugging Failing Tests

### Common Failure Patterns

#### 1. Path Issues

```python
# Problem: Relative paths break in tests
path = Path("data/stocks_1d")  # ❌ Uses current directory

# Solution: Use absolute paths from fixtures
path = tmp_path / "data" / "stocks_1d"  # ✓ Isolated
```

#### 2. State Leakage

```python
# Problem: Tests affect each other
global_state = {}  # ❌ Shared across tests

# Solution: Reset state or use fixtures
@pytest.fixture
def fresh_state():
    return {}  # ✓ New instance per test
```

#### 3. Time-Dependent Tests

```python
# Problem: Tests fail at different times
assert datetime.now().hour == 10  # ❌ Only works at 10am

# Solution: Mock time
from unittest.mock import patch
with patch('datetime.datetime') as mock_dt:
    mock_dt.now.return_value = datetime(2025, 1, 1, 10, 0)
    # Test with known time ✓
```

#### 4. Missing Test Data

```python
# Problem: Test expects specific file
df = pd.read_parquet("fixtures/test_data.parquet")  # ❌ File might not exist

# Solution: Create data in test or fixture
@pytest.fixture
def test_data():
    return pd.DataFrame({...})  # ✓ Always available
```

### Debugging Commands

```bash
# Run with print statements visible
uv run pytest -s

# Run with debugger on failure
uv run pytest --pdb

# Run single test with full traceback
uv run pytest tests/test_file.py::test_name -vv

# Show local variables on failure
uv run pytest --showlocals
```

---

## Continuous Integration

### Pre-commit Checks

```bash
# Install pre-commit hooks
uv tool install pre-commit --with pre-commit-uv --force-reinstall
pre-commit install

# Run manually
pre-commit run --all-files
```

### CI Pipeline

When pushing to GitHub, CI runs:
1. `uv sync` - restore dependencies
2. `uv run ruff check .` - linting
3. `uv run ruff format --check .` - formatting
4. `uv run pytest --cov=yf_parqed` - tests with coverage

All must pass before merge.

---

## Performance Testing

### Timing Tests

```python
import time

def test_performance():
    start = time.time()
    
    # Operation to measure
    result = expensive_operation()
    
    elapsed = time.time() - start
    
    # Assert performance requirement
    assert elapsed < 1.0  # Must complete in <1 second
```

### Memory Profiling

```bash
# Install memory profiler
uv add --dev memory-profiler

# Profile test
uv run pytest --memprof tests/test_memory_intensive.py
```

---

## Best Practices

### ✓ DO

- Use `tmp_path` for all file I/O
- Mock external APIs and network calls
- Test both happy path and error cases
- Use descriptive test names
- Keep tests independent (no shared state)
- Test one thing per test function
- Use fixtures for common setup
- Add docstrings to complex tests

### ✗ DON'T

- Write to repo root in tests
- Rely on external services (network, DB)
- Skip cleanup (use fixtures/tmp_path)
- Test multiple unrelated things in one test
- Use hardcoded dates/times
- Leave commented-out test code
- Test implementation details (test behavior)

---

## Additional Resources

- **Development Workflows**: See `.github/DEVELOPMENT_GUIDE.md`
- **Architecture Details**: See `ARCHITECTURE.md`
- **Troubleshooting**: See `.github/TROUBLESHOOTING.md`

---

**Last Updated:** 2025-12-05
