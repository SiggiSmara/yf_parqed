## Phase 1.4: Xetra CLI Implementation

You are implementing **Phase 1.4** of the Xetra delayed-data pipeline in the `yf_parqed` repository.

### Context

**What's already done:**
- ✅ Phase 1.3: `save_xetra_trades()` method exists in `PartitionedStorageBackend` 
- ✅ Tests in `tests/test_xetra_storage.py` pass (3 tests)
- ✅ Trade storage uses venue-first partitioning: `data/{market}/{source}/trades/venue=VENUE/year=YYYY/month=MM/day=DD/trades.parquet`

**What you're building now:**
A separate CLI entry point (`xetra-parqed`) for Xetra data operations, independent from the Yahoo Finance CLI (`yf-parqed`).

### Required Reading (in order)

1. **IMPLEMENTATION_CHECKLIST.md** – Phase 1.4 section (lines 250-358)
   - Tasks, acceptance criteria, code patterns
2. **xetra_implementation_addendum.md** – Sections 2 & 3
   - API endpoints, file discovery, partition layout
3. **2025-10-12-xetra-delayed-data.md** – AD-2 & AD-3
   - Architecture decisions for storage and CLI separation

### Architecture Decision: Why Separate CLIs?

**Problem:** The existing `main.py` creates a global `YFParqed` instance at module load. Xetra has its own services (XetraFetcher, XetraParser) and shouldn't depend on YFParqed.

**Solution:** Two independent CLI entry points:
- `yf-parqed` → Yahoo Finance (uses YFParqed class)
- `xetra-parqed` → Deutsche Börse Xetra (uses Xetra services, lazy-loaded)

This follows the existing pattern: `yf-parqed-migrate` is a separate entry point for migrations.

---

## Tasks (complete in this order)

### Task 1: Rename Existing CLI (clarity)

**Goal:** Make YFParqed coupling explicit by renaming `main.py` to `yfinance_cli.py`.

**Steps:**
1. Rename `src/yf_parqed/main.py` → `src/yf_parqed/yfinance_cli.py`
2. Update `pyproject.toml`:
   ```toml
   [project.scripts]
   yf-parqed = "yf_parqed.yfinance_cli:app"  # Changed from main:app
   ```
3. Update test imports:
   - `tests/test_cli.py` (line ~10)
   - `tests/test_cli_integration.py` (line ~10)
   - Change: `from yf_parqed.main import app` → `from yf_parqed.yfinance_cli import app`
4. Run tests to verify:
   ```bash
   uv run pytest tests/test_cli.py tests/test_cli_integration.py
   ```

**Acceptance:** All existing CLI tests pass with new module name.

---

### Task 2: Create Xetra CLI Entry Point

**Goal:** New `xetra_cli.py` with its own Typer app and entry point.

**File:** `src/yf_parqed/xetra_cli.py`

**Requirements:**
- NO global service instantiation (unlike `yfinance_cli.py` which has global `yf_parqed`)
- Lazy-load services per command
- Match logging setup from `yfinance_cli.py` (loguru configuration)
- Support `--wrk-dir` and `--log-level` global options

**Code Template:**
```python
import typer
from pathlib import Path
from loguru import logger
import sys
from typing_extensions import Annotated

app = typer.Typer()

@app.callback()
def main(
    wrk_dir: Annotated[
        Path, typer.Option(help="Working directory, default is current directory")
    ] = Path.cwd(),
    log_level: Annotated[str, typer.Option(help="Log level")] = "INFO",
):
    """Xetra delayed data CLI - Deutsche Börse parquet storage."""
    logger.remove()
    logger.add(sys.stderr, level=log_level)
    # Store wrk_dir in context for commands to access if needed
    # (Don't instantiate any services here - keep it lazy)

@app.command()
def fetch_trades(
    venue: Annotated[str, typer.Option(help="Xetra venue code (DETR, DFRA, DGAT, DEUR)")],
    date: Annotated[str, typer.Option(help="Trade date (YYYY-MM-DD)")],
    store: Annotated[bool, typer.Option(help="Store trades to parquet")] = False,
):
    """Fetch Xetra trades for a venue/date and optionally store them."""
    # Lazy imports - services only loaded when command runs
    from .xetra_fetcher import XetraFetcher
    from .xetra_parser import XetraParser
    from .partitioned_storage_backend import PartitionedStorageBackend
    
    logger.info(f"Fetching trades for {venue} on {date}")
    
    # TODO: Implement workflow
    # 1. fetcher.list_available_files(venue, date)
    # 2. fetcher.download_file() for each
    # 3. parser.parse() to DataFrame
    # 4. Display stats (row count, ISIN count, etc.)
    # 5. If --store: backend.save_xetra_trades(df, venue, date)

@app.command()
def list_files(
    venue: Annotated[str, typer.Option(help="Xetra venue code")],
    date: Annotated[str, typer.Option(help="Trade date (YYYY-MM-DD)")],
):
    """List available trade files for a venue/date."""
    from .xetra_fetcher import XetraFetcher
    
    fetcher = XetraFetcher()
    files = fetcher.list_available_files(venue, date)
    
    if not files:
        typer.echo(f"No files found for {venue} on {date}")
        return
    
    typer.echo(f"Found {len(files)} files:")
    for filename in files:
        typer.echo(f"  - {filename}")
```

**Entry Point:** Add to `pyproject.toml`:
```toml
[project.scripts]
yf-parqed = "yf_parqed.yfinance_cli:app"
xetra-parqed = "yf_parqed.xetra.xetra_cli:app"  # NEW
yf-parqed-migrate = "yf_parqed.tools.partition_migrate:app"
```

**Note:** For now, stub out `XetraFetcher` and `XetraParser` with placeholder classes if they don't exist yet. Focus on CLI wiring and structure.

---

### Task 3: Create CLI Tests

**File:** `tests/test_xetra_cli.py`

**Requirements:**
- Test command wiring (not full end-to-end yet)
- Mock XetraFetcher and XetraParser
- Verify no YFParqed instantiation
- Test `--help` output
- Test argument parsing

**Test Template:**
```python
import pytest
from typer.testing import CliRunner
from unittest.mock import Mock, patch
from yf_parqed.xetra.xetra_cli import app

runner = CliRunner()

def test_fetch_trades_help():
    """Verify fetch-trades --help works."""
    result = runner.invoke(app, ["fetch-trades", "--help"])
    assert result.exit_code == 0
    assert "venue" in result.output.lower()
    assert "date" in result.output.lower()

@patch("yf_parqed.xetra.xetra_cli.XetraFetcher")
@patch("yf_parqed.xetra.xetra_cli.XetraParser")
def test_fetch_trades_no_store(mock_parser, mock_fetcher):
    """Test fetch-trades without --store flag."""
    # Mock the fetcher
    mock_fetcher_instance = Mock()
    mock_fetcher_instance.list_available_files.return_value = ["file1.json.gz"]
    mock_fetcher.return_value = mock_fetcher_instance
    
    result = runner.invoke(app, [
        "fetch-trades",
        "--venue", "DETR",
        "--date", "2025-11-01"
    ])
    
    assert result.exit_code == 0
    mock_fetcher_instance.list_available_files.assert_called_once_with("DETR", "2025-11-01")

@patch("yf_parqed.xetra.xetra_cli.XetraFetcher")
def test_list_files(mock_fetcher):
    """Test list-files command."""
    mock_fetcher_instance = Mock()
    mock_fetcher_instance.list_available_files.return_value = [
        "2025-11-01_DETR_XTRD.json.gz",
        "2025-11-01_DETR_XOFF.json.gz"
    ]
    mock_fetcher.return_value = mock_fetcher_instance
    
    result = runner.invoke(app, [
        "list-files",
        "--venue", "DETR",
        "--date", "2025-11-01"
    ])
    
    assert result.exit_code == 0
    assert "2025-11-01_DETR_XTRD.json.gz" in result.output
    assert "Found 2 files" in result.output
```

**Run tests:**
```bash
uv run pytest tests/test_xetra_cli.py -v
```

---

### Task 4: Smoke Test Both CLIs

**Goal:** Verify both CLI entry points work after changes.

**Test commands:**
```bash
# Yahoo Finance CLI (unchanged functionality)
uv run yf-parqed --help
uv run yf-parqed update-data --help

# New Xetra CLI
uv run xetra-parqed --help
uv run xetra-parqed fetch-trades --help
uv run xetra-parqed list-files --help
```

**Expected:**
- All `--help` commands work
- No import errors
- Correct command descriptions displayed

---

## Stub Services (if needed)

If `XetraFetcher` or `XetraParser` don't exist yet, create minimal stubs:

**`src/yf_parqed/xetra_fetcher.py`:**
```python
from typing import List

class XetraFetcher:
    def list_available_files(self, venue: str, date: str) -> List[str]:
        """List available trade files for a venue/date."""
        # Stub implementation - return empty for now
        return []
    
    def download_file(self, venue: str, date: str, filename: str) -> bytes:
        """Download a trade file."""
        raise NotImplementedError("XetraFetcher.download_file not implemented yet")
```

**`src/yf_parqed/xetra_parser.py`:**
```python
import pandas as pd

class XetraParser:
    def parse(self, json_data: str) -> pd.DataFrame:
        """Parse Xetra JSON to DataFrame."""
        raise NotImplementedError("XetraParser.parse not implemented yet")
```

---

## Validation Checklist

Before marking Phase 1.4 complete:

- [ ] `src/yf_parqed/main.py` renamed to `yfinance_cli.py`
- [ ] `pyproject.toml` updated with both entry points
- [ ] `src/yf_parqed/xetra_cli.py` created with `fetch-trades` and `list-files` commands
- [ ] `tests/test_xetra_cli.py` created with at least 3 tests
- [ ] All CLI tests pass: `uv run pytest tests/test_cli.py tests/test_cli_integration.py tests/test_xetra_cli.py`
- [ ] `uv run yf-parqed --help` works (Yahoo Finance CLI)
- [ ] `uv run xetra-parqed --help` works (Xetra CLI)
- [ ] No global `YFParqed` instantiation in `xetra_cli.py`
- [ ] Logging configured consistently across both CLIs
- [ ] Update `IMPLEMENTATION_CHECKLIST.md` Phase 1.4 tasks to `[x]`

---

## Running the Full Test Suite

```bash
# Run all tests to ensure nothing broke
uv run pytest

# Run linting
uv run ruff check . --fix
uv run ruff format .
```

**Expected:** All 186+ tests pass (183 existing + 3 from test_xetra_storage.py + new CLI tests).

---

## Success Criteria

You're done when:
1. ✅ Two separate CLI entry points exist (`yf-parqed`, `xetra-parqed`)
2. ✅ Xetra CLI has lazy service loading (no global instantiation)
3. ✅ All tests pass
4. ✅ `--help` works for all commands
5. ✅ Checklist Phase 1.4 marked complete

**Do NOT implement:**
- XetraFetcher full implementation (Phase 1.1)
- XetraParser full implementation (Phase 1.2)
- Actual data fetching/parsing (use stubs/mocks)
- Phase 2 aggregation work

**Focus:** CLI structure and wiring only.
