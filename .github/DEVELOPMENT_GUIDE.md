# Development Guide for yf_parqed

This guide provides detailed workflows and patterns for common development tasks.

## Making Code Changes

### Standard Workflow

1. **Before starting**: `uv sync`
2. **Make changes** to source files
3. **Run tests**: `uv run pytest`
4. **Fix linting**: `uv run ruff check . --fix && uv run ruff format .`
5. **Verify**: `uv run pytest` again
6. **Commit** changes

### Quick Iteration Loop

```bash
# One-liner for rapid feedback
uv run pytest tests/test_myfeature.py && uv run ruff check . --fix
```

---

## Adding a New Service

Services follow dependency injection patterns for testability and maintainability.

### Step-by-Step Process

1. **Create service file** in `src/yf_parqed/`
   ```python
   # src/yf_parqed/my_service.py
   class MyService:
       def __init__(self, dependency: DependencyType):
           self.dependency = dependency
       
       def do_something(self):
           # Implementation
           pass
   ```

2. **Use constructor injection** for dependencies
   - Accept dependencies via `__init__` parameters
   - Default to `None` with lazy initialization if needed
   - Document expected types in docstrings

3. **Add unit tests** in `tests/test_<service>.py`
   ```python
   # tests/test_my_service.py
   def test_my_service_initialization(tmp_path):
       service = MyService(dependency=mock_dep)
       assert service is not None
   ```

4. **Wire into `YFParqed` façade** in `primary_class.py`
   ```python
   class YFParqed:
       def __init__(self):
           self.my_service = MyService(dependency=self.some_dep)
   ```

5. **Update `ARCHITECTURE.md`** with service description
   - Add to service list
   - Document responsibilities
   - Show dependency graph

6. **Run full test suite** to ensure integration works
   ```bash
   uv run pytest
   ```

---

## Modifying Ticker Logic

Ticker state is centralized in `tickers.json` with per-interval tracking.

### Key Files

- **State changes**: Edit `ticker_registry.py`
- **Tests**: Update `test_ticker_registry.py` or `test_ticker_operations.py`
- **Execution flow**: Check `interval_scheduler.py` for orchestration
- **Persistence**: Handled automatically by `ConfigService`

### Example: Adding New Ticker Metadata Field

```python
# 1. Update TickerRegistry to handle new field
def update_ticker_with_new_field(self, ticker: str, new_value: str):
    self.tickers[ticker]["new_field"] = new_value
    self.save()

# 2. Add migration logic in ConfigService
def migrate_tickers_schema(self):
    for ticker_data in self.tickers.values():
        if "new_field" not in ticker_data:
            ticker_data["new_field"] = "default_value"

# 3. Add tests
def test_new_field_initialization(tmp_path):
    registry = TickerRegistry(config)
    registry.update_ticker_with_new_field("AAPL", "test_value")
    assert registry.tickers["AAPL"]["new_field"] == "test_value"
```

---

## Storage Backend Changes

**⚠️ CRITICAL**: Before modifying storage, read [`.github/DATA_SAFETY_STRATEGY.md`](.github/DATA_SAFETY_STRATEGY.md)

### File Responsibilities

- **Legacy backend**: `storage_backend.py` - Flat parquet files
- **Partitioned backend**: `partitioned_storage_backend.py` - Hive-style partitions
- **Path resolution**: `partition_path_builder.py` - Path construction logic
- **Migration orchestration**: `partition_migration_service.py` - Legacy→Partitioned
- **Tests**: `test_storage_backend.py`, `test_partitioned_storage_backend.py`

### Adding a New Storage Feature

```python
# 1. Add to interface (if needed)
class StorageInterface:
    def new_operation(self, params):
        raise NotImplementedError

# 2. Implement in both backends
class StorageBackend(StorageInterface):
    def new_operation(self, params):
        # Legacy implementation
        pass

class PartitionedStorageBackend(StorageInterface):
    def new_operation(self, params):
        # Partitioned implementation
        pass

# 3. Add tests for both backends
def test_new_operation_legacy(tmp_path):
    backend = StorageBackend(...)
    result = backend.new_operation(params)
    assert result is not None

def test_new_operation_partitioned(tmp_path):
    backend = PartitionedStorageBackend(...)
    result = backend.new_operation(params)
    assert result is not None

# 4. Update façade to use new operation
class YFParqed:
    def do_something(self):
        self.storage.new_operation(params)
```

---

## Adding a New CLI Command

CLI uses Typer for command-line interface with dependency injection.

### Yahoo Finance CLI (`yfinance_cli.py`)

```python
@app.command()
def my_command(
    ticker: str = typer.Option(..., help="Ticker symbol"),
    interval: str = typer.Option("1d", help="Data interval"),
):
    """
    Brief description of what this command does.
    """
    yf_parqed = YFParqed()
    # Implementation
    typer.echo(f"Processing {ticker} at {interval} interval")
```

### Xetra CLI (`xetra_cli.py`)

```python
@app.command()
def my_xetra_command(
    venue: str = typer.Option(..., help="Trading venue"),
    date: Optional[str] = typer.Option(None, help="Date (YYYY-MM-DD)"),
):
    """
    Brief description of Xetra-specific command.
    """
    service = XetraService()
    # Implementation
    typer.echo(f"Processing {venue} for {date or 'today'}")
```

### Testing CLI Commands

```python
from typer.testing import CliRunner
from yf_parqed.yfinance_cli import app

def test_my_command():
    runner = CliRunner()
    result = runner.invoke(app, ["my-command", "--ticker", "AAPL"])
    assert result.exit_code == 0
    assert "Processing AAPL" in result.stdout
```

---

## Working with Tests

### Test Structure Philosophy

**Bottom-up → Top-down → End-to-end**

1. **Unit tests**: Test individual services in isolation
2. **Integration tests**: Test service interactions
3. **End-to-end tests**: Test full CLI workflows

### Common Test Patterns

#### Using Temporary Directories

```python
def test_with_temp_dir(tmp_path):
    # tmp_path is pytest fixture providing isolated directory
    config = ConfigService(working_path=tmp_path)
    config.save_tickers({"AAPL": {...}})
    
    # Verify
    assert (tmp_path / "tickers.json").exists()
    # Cleanup is automatic
```

#### Mocking External APIs

```python
def test_data_fetcher_with_mock():
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame(...)
    
    fetcher = DataFetcher(
        limiter=lambda: None,  # Disable rate limiting in tests
        ticker_factory=lambda symbol: mock_ticker
    )
    
    result = fetcher.fetch("AAPL", "1d")
    assert not result.empty
```

#### Testing Both Storage Backends

```python
@pytest.mark.parametrize("backend_type", ["legacy", "partitioned"])
def test_storage_operation(tmp_path, backend_type):
    if backend_type == "legacy":
        backend = StorageBackend(...)
    else:
        backend = PartitionedStorageBackend(...)
    
    # Test applies to both backends
    backend.save(df, interval="1d", ticker="AAPL")
    result = backend.read(interval="1d", ticker="AAPL")
    assert len(result) == len(df)
```

---

## Debugging Common Issues

### Tests Failing After Pull

```bash
# Environment out of sync
uv sync

# Stale test cache
rm -rf .pytest_cache
uv run pytest
```

### Lint Errors on Commit

```bash
# Auto-fix most issues
uv run ruff check . --fix
uv run ruff format .

# Check what remains
uv run ruff check .
```

### Import Errors in Tests

```bash
# Ensure package is installed in editable mode
uv sync

# Verify Python path
uv run python -c "import yf_parqed; print(yf_parqed.__file__)"
```

### Data Files Not Found in Tests

- Check you're using `tmp_path` fixture
- Verify paths are absolute, not relative
- Ensure `ConfigService` points to test directory

```python
# Wrong
config = ConfigService()  # Uses current directory

# Right
config = ConfigService(working_path=tmp_path)
```

---

## Performance Considerations

### Rate Limiting

- Always respect Yahoo Finance rate limits (default: 3 req/2s)
- Mock rate limiter in unit tests: `limiter=lambda: None`
- Use batch operations when possible

### Parquet File Size

- Partitioned storage reduces corruption blast radius
- Monthly partitions balance file count vs size
- Monitor parquet file sizes: `du -sh data/us/yahoo/stocks_1d/`

### Test Execution Time

- Full suite: ~11 seconds
- Parallelization: `pytest -n auto` (requires pytest-xdist)
- Focus on changed code: `pytest tests/test_myfeature.py`

---

## Documentation Standards

### Code Comments

```python
# Good: Explains WHY, not WHAT
# Yahoo API returns split-adjusted prices without metadata.
# We store unadjusted + factors separately for reproducibility.
df_unadjusted = remove_adjustments(df)

# Bad: Obvious from code
# Loop through tickers
for ticker in tickers:
    ...
```

### Docstrings

```python
def aggregate_ohlcv(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """
    Resample tick data to OHLCV bars at specified interval.
    
    Args:
        df: Raw tick data with price/volume columns
        interval: Pandas resample interval (e.g., "1h", "1d")
    
    Returns:
        DataFrame with OHLCV columns and datetime index
    
    Raises:
        ValueError: If df is empty or missing required columns
    """
```

### Architecture Updates

When adding new services or major features:
1. Update `ARCHITECTURE.md` with service description
2. Add to dependency graph diagram
3. Document key decisions in ADR (`docs/adr/YYYY-MM-DD-feature.md`)
4. Update release notes if user-facing

---

## Git Workflow

### Branch Naming

- Feature: `feature/add-ohlcv-aggregation`
- Bugfix: `fix/corrupt-parquet-handling`
- Docs: `docs/update-testing-guide`

### Commit Messages

```bash
# Good
git commit -m "Add OHLCVAggregator service with pandas resample

- Implements reusable aggregation from 1m→1h/1d
- Supports corporate action adjustments
- Adds 15 unit tests covering edge cases
- Refs #42"

# Bad
git commit -m "fix stuff"
```

### Pre-commit Checks

Automated via `.pre-commit-config.yaml`:
- `uv lock` validation
- `ruff` linting
- `ruff` formatting

If hooks fail:
```bash
# Fix issues
uv run ruff check . --fix
uv run ruff format .

# Retry commit
git commit
```

---

## Release Process

1. Update version in `pyproject.toml`
2. Update `docs/release-notes.md` with changes
3. Update `docs/roadmap.md` (move completed items)
4. Run full test suite: `uv run pytest`
5. Tag release: `git tag -a v0.4.0 -m "Release 0.4.0: Daemon Mode"`
6. Push: `git push && git push --tags`

---

## Getting Help

- **Architecture questions**: See `ARCHITECTURE.md`
- **Storage changes**: See `.github/DATA_SAFETY_STRATEGY.md`
- **Test patterns**: See `.github/TESTING_GUIDE.md`
- **Troubleshooting**: See `.github/TROUBLESHOOTING.md`
- **Build issues**: Check copilot instructions in `.github/copilot-instructions.md`

---

**Last Updated:** 2025-12-05
