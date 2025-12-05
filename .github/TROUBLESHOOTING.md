# Troubleshooting Guide for yf_parqed

Common issues and their solutions when working with yf_parqed.

## Environment Issues

### Problem: `uv sync` Fails

**Symptoms:**
- Error: "Could not find compatible versions"
- Error: "Failed to download dependencies"

**Solutions:**

```bash
# 1. Update uv itself
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clear uv cache
uv cache clean

# 3. Remove lock file and retry
rm uv.lock
uv sync

# 4. Check Python version (requires 3.12+)
python --version
uv python list
```

---

### Problem: Import Errors in Tests

**Symptoms:**
- `ModuleNotFoundError: No module named 'yf_parqed'`
- Tests can't find package modules

**Solutions:**

```bash
# 1. Ensure package installed in editable mode
uv sync

# 2. Verify Python path points to venv
uv run python -c "import sys; print(sys.executable)"

# 3. Check package location
uv run python -c "import yf_parqed; print(yf_parqed.__file__)"

# 4. Clean and reinstall
rm -rf .venv
uv sync
```

---

### Problem: Pre-commit Hooks Failing

**Symptoms:**
- Hooks don't run on commit
- Error: "pre-commit not found"

**Solutions:**

```bash
# 1. Install pre-commit correctly
uv tool install pre-commit --with pre-commit-uv --force-reinstall

# 2. Install hooks
pre-commit install

# 3. Test hooks manually
pre-commit run --all-files

# 4. If still failing, check .pre-commit-config.yaml
cat .pre-commit-config.yaml
```

---

## Test Issues

### Problem: Tests Failing After Pull

**Symptoms:**
- Tests passed before `git pull`, now fail
- Dependency mismatch errors

**Solutions:**

```bash
# 1. Sync dependencies (most common fix)
uv sync

# 2. Clear pytest cache
rm -rf .pytest_cache
rm -rf .ruff_cache

# 3. Re-run tests
uv run pytest

# 4. If specific test file fails, run in isolation
uv run pytest tests/test_failing_file.py -v
```

---

### Problem: Tests Pass Locally, Fail in CI

**Symptoms:**
- All tests green on local machine
- CI pipeline shows failures

**Common Causes & Fixes:**

1. **Different Python version**
   ```bash
   # Check local version matches CI (3.12+)
   python --version
   ```

2. **Missing test fixtures or data files**
   ```bash
   # Verify all test files committed
   git status
   git add tests/
   ```

3. **Hardcoded paths**
   ```python
   # Bad: Absolute path only works locally
   path = Path("/home/user/data")
   
   # Good: Use tmp_path fixture
   def test_example(tmp_path):
       path = tmp_path / "data"
   ```

4. **Time-dependent tests**
   ```python
   # Bad: Fails at different times of day
   assert datetime.now().hour == 14
   
   # Good: Mock time
   with patch('datetime.datetime') as mock_dt:
       mock_dt.now.return_value = datetime(2025, 1, 1, 14, 0)
   ```

---

### Problem: Slow Test Execution

**Symptoms:**
- Test suite takes >30 seconds
- Individual tests hang

**Solutions:**

```bash
# 1. Find slow tests
uv run pytest --durations=10

# 2. Run tests in parallel (requires pytest-xdist)
uv add --dev pytest-xdist
uv run pytest -n auto

# 3. Skip slow tests during development
uv run pytest -m "not slow"

# 4. Profile specific test
uv run pytest tests/test_slow.py --profile
```

**Common Slow Test Causes:**
- Not mocking external APIs (network calls)
- Not mocking rate limiter (real delays)
- Creating too many test files (use minimal data)
- Running full integration tests unnecessarily

---

## CLI Issues

### Problem: Command Not Found

**Symptoms:**
- `yf-parqed: command not found`
- `xetra-parqed: command not found`

**Solutions:**

```bash
# 1. Run via uv (always works)
uv run yf-parqed --help
uv run xetra-parqed --help

# 2. Activate virtual environment
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows
yf-parqed --help

# 3. Check installation
uv run which yf-parqed
```

---

### Problem: CLI Hangs or Times Out

**Symptoms:**
- Command starts but never completes
- No output for several minutes

**Common Causes & Fixes:**

1. **Rate limiting delays (expected behavior)**
   - Yahoo Finance: 3 requests per 2 seconds
   - Large ticker lists take time
   - Use `--log-level DEBUG` to see progress

2. **Network issues**
   ```bash
   # Test connectivity
   curl https://query1.finance.yahoo.com/v8/finance/chart/AAPL
   
   # Check proxy settings
   echo $HTTP_PROXY
   echo $HTTPS_PROXY
   ```

3. **Deadlock in daemon mode**
   ```bash
   # Check for stale PID file
   cat /tmp/yf-parqed.pid
   
   # Remove if process not running
   ps aux | grep yf-parqed
   rm /tmp/yf-parqed.pid
   ```

---

## Data & Storage Issues

### Problem: Corrupt Parquet Files

**Symptoms:**
- Error: "Invalid Parquet file"
- Error: "ArrowInvalid"

**Expected Behavior:**
Storage backends automatically delete corrupt files and retry. Check logs for warnings.

**Manual Recovery:**

```bash
# 1. Find corrupt file
find data/ -name "*.parquet" -exec python -c "
import sys
import pyarrow.parquet as pq
try:
    pq.read_table(sys.argv[1])
except Exception as e:
    print(f'{sys.argv[1]}: {e}')
" {} \;

# 2. Delete corrupt file
rm path/to/corrupt.parquet

# 3. Re-fetch data
uv run yf-parqed update-data --ticker AAPL --interval 1d
```

---

### Problem: Missing Data After Migration

**Symptoms:**
- Tickers showing no data after partition migration
- Row count mismatches

**Diagnostic Steps:**

```bash
# 1. Check migration status
uv run yf-parqed-migrate status

# 2. Verify both layouts exist
ls -lh data/legacy/stocks_1d/
ls -lh data/us/yahoo/stocks_1d/

# 3. Check tickers.json for storage metadata
cat tickers.json | grep -A 5 "AAPL"

# 4. Verify migration checksums
uv run yf-parqed-migrate verify us:yahoo 1d
```

**Recovery:**

```bash
# If verification fails, rollback
uv run yf-parqed-migrate rollback --venue us:yahoo --interval 1d

# Re-run migration
uv run yf-parqed-migrate migrate --venue us:yahoo --interval 1d
```

---

### Problem: Disk Space Issues

**Symptoms:**
- Error: "No space left on device"
- Migration fails during copy

**Solutions:**

```bash
# 1. Check available space
df -h

# 2. Estimate required space
du -sh data/legacy/stocks_*

# 3. Clean up old migrations
rm -rf data/.migration-staging

# 4. Clean up test artifacts
rm -rf .pytest_cache
rm -rf htmlcov

# 5. Compress old partitions (if using partitioned storage)
find data/us/yahoo/ -name "*.parquet" -mtime +180 | \
  xargs -I {} sh -c 'gzip {}'
```

**Prevention:**
Migration CLI checks disk space before starting. Requires 2.5x source size available.

---

## Ticker Management Issues

### Problem: Ticker Stuck in "not_found" Status

**Symptoms:**
- Ticker not updating despite being actively traded
- All intervals show `not_found`

**Solutions:**

```bash
# 1. Check ticker status
cat tickers.json | grep -A 20 "TICKER_SYMBOL"

# 2. Reactivate manually
uv run yf-parqed reparse-not-founds

# 3. Or edit tickers.json directly (last resort)
# Change "status": "not_found" → "status": "active"
# Remove "not_found" dates from intervals
```

---

### Problem: Cooldown Preventing Updates

**Symptoms:**
- Ticker skipped during updates
- Log: "Ticker AAPL in cooldown for interval 1h"

**Expected Behavior:**
30-day cooldown after interval-specific failures prevents repeated API calls.

**Override (if needed):**

```python
# Modify cooldown in ticker_registry.py (for testing)
COOLDOWN_DAYS = 0  # Disable cooldown

# Or manually reset in tickers.json
# Remove "last_not_found_date" from interval metadata
```

---

## Rate Limiting Issues

### Problem: 429 Too Many Requests

**Symptoms:**
- Error: "HTTPError: 429 Client Error: Too Many Requests"
- Yahoo Finance blocks requests

**Solutions:**

```bash
# 1. Increase delay between requests (default: 3 req/2s)
uv run yf-parqed --limits 2 3 update-data  # More conservative

# 2. Wait 15-30 minutes for rate limit reset

# 3. Check if IP is temporarily blocked
curl -I https://query1.finance.yahoo.com/v8/finance/chart/AAPL

# 4. Use different network if blocked
```

---

### Problem: Xetra Rate Limit Errors

**Symptoms:**
- Connection timeouts during Xetra bulk downloads
- Deutsche Börse throttling

**Solutions:**

```bash
# 1. Increase inter-request delay (default: 0.6s)
# Edit config_service.py or xetra_fetcher.py
inter_request_delay = 1.0  # More conservative

# 2. Reduce burst size (default: 30)
burst_size = 15

# 3. Enable trading hours filtering (reduces requests by 56%)
# Already enabled by default, but verify:
uv run xetra-parqed fetch-trades DETR --active-hours "08:30-18:00"
```

---

## Daemon Mode Issues

### Problem: Daemon Won't Start

**Symptoms:**
- Error: "PID file already exists"
- Error: "Another instance is running"

**Solutions:**

```bash
# 1. Check if process actually running
cat /tmp/yf-parqed.pid  # Get PID
ps aux | grep <PID>     # Check if alive

# 2. If process not running, remove stale PID
rm /tmp/yf-parqed.pid

# 3. Force kill if hung
kill -9 <PID>
rm /tmp/yf-parqed.pid

# 4. Check logs for errors
tail -f /var/log/yf-parqed/update.log
```

---

### Problem: Daemon Not Respecting Trading Hours

**Symptoms:**
- Daemon runs outside market hours
- Updates during weekends

**Solutions:**

```bash
# 1. Verify trading hours configuration
uv run yf-parqed update-data --daemon --help

# 2. Check system timezone
timedatectl  # Linux
date         # General

# 3. Explicitly set trading hours
uv run yf-parqed update-data --daemon \
  --trading-hours "09:30-16:00" \
  --market-timezone "US/Eastern"

# 4. Check daemon logs for "Outside trading hours" messages
grep "Outside trading hours" /var/log/yf-parqed/update.log
```

---

## Performance Issues

### Problem: High Memory Usage

**Symptoms:**
- Process uses >2GB RAM
- System slows down during updates

**Common Causes & Fixes:**

1. **Too many tickers loading simultaneously**
   ```python
   # Modify interval_scheduler.py to process in smaller batches
   batch_size = 100  # Process 100 tickers at a time
   ```

2. **Large parquet files not releasing memory**
   ```python
   # Force garbage collection in loops
   import gc
   gc.collect()
   ```

3. **Partitioned storage more memory efficient**
   ```bash
   # Migrate to partitioned storage
   uv run yf-parqed-migrate migrate --venue us:yahoo --interval 1d
   ```

---

### Problem: Slow Parquet Reads

**Symptoms:**
- Reading ticker data takes >1 second
- Update loop very slow

**Solutions:**

```bash
# 1. Use partitioned storage (faster for large datasets)
uv run yf-parqed-migrate migrate --all

# 2. Reduce parquet file size via compression
# Edit storage_backend.py
df.to_parquet(path, compression='gzip', compression_level=6)

# 3. Check for disk I/O bottlenecks
iostat -x 1  # Linux

# 4. Use SSD instead of HDD for data/ directory
```

---

## Debugging Tools

### Enable Debug Logging

```bash
# CLI
uv run yf-parqed --log-level DEBUG update-data

# In code
from loguru import logger
logger.add("debug.log", level="DEBUG")
```

### Inspect Data Files

```bash
# View parquet file contents
uv run python -c "
import pandas as pd
df = pd.read_parquet('data/stocks_1d/AAPL.parquet')
print(df.head())
print(df.info())
"

# Check parquet file size
du -sh data/stocks_1d/*.parquet | sort -h

# Validate parquet integrity
uv run python -c "
import pyarrow.parquet as pq
table = pq.read_table('data/stocks_1d/AAPL.parquet')
print(f'Rows: {table.num_rows}, Columns: {table.num_columns}')
"
```

### Monitor During Updates

```bash
# Watch logs in real-time
tail -f ~/.yf_parqed.log

# Monitor network activity
watch -n 1 'lsof -i -P | grep yf-parqed'

# Monitor file changes
watch -n 1 'ls -lht data/stocks_1d/ | head -10'
```

---

## Getting Help

If issue persists after trying solutions above:

1. **Check existing documentation:**
   - `.github/DATA_SAFETY_STRATEGY.md` - Storage-related issues
   - `.github/DEVELOPMENT_GUIDE.md` - Development workflows
   - `.github/TESTING_GUIDE.md` - Test-related issues
   - `ARCHITECTURE.md` - Architecture and design questions

2. **Gather diagnostic information:**
   ```bash
   # System info
   uv --version
   python --version
   uv run pytest --version
   
   # Package info
   cat pyproject.toml | grep version
   
   # Test results
   uv run pytest -v > test_output.txt 2>&1
   
   # Logs
   cat ~/.yf_parqed.log
   ```

3. **Create minimal reproduction:**
   - Isolate failing code
   - Remove unrelated components
   - Provide sample data if needed

4. **File issue with:**
   - Clear description of problem
   - Steps to reproduce
   - Expected vs actual behavior
   - Diagnostic information from step 2
   - Minimal reproduction from step 3

---

**Last Updated:** 2025-12-05
