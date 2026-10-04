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
- Log: `Unable to read parquet file ...: ... File moved aside to data.parquet.damaged-<timestamp>.`
- Log: `Damaged partition file ...` (from the month-close check or `verify-partitions`)
- A line in `damaged_partitions.jsonl` in the working directory
- Error: "Invalid Parquet file", "ArrowInvalid"

**What the code does (Yahoo data):**

Nothing is deleted. Yahoo serves 1-minute bars for seven days only and the data has no backup, so a file that looks broken is kept.

- A file the daemon cannot read while storing new bars (it tries twice) is **renamed** in its directory to `data.parquet.damaged-<UTC timestamp>` (legacy layout: `<TICKER>.parquet.damaged-...`). With partitioned storage that ticker's update fails for the cycle, and the cycle ends there; the next cycle starts a new `data.parquet` for the month from what Yahoo still serves.
- A file found by the month-close check or by `yf-parqed verify-partitions` is **left where it is**. These checks only read.
- Every such file gets one line in `damaged_partitions.jsonl`: when, path, ticker, interval, month, error, what was done (`moved aside` or `left in place`), the new path, and who found it.
- A file with a schema problem or no rows is not renamed; the daemon logs the error and fails that ticker until the file is dealt with.

**Finding damaged files:**

```bash
# What has been recorded
cat damaged_partitions.jsonl

# Files that were moved aside (a name with .tmp- in it is a write that a kill cut short)
find data/ -name '*.damaged-*'

# Read every file of a month (default: the last closed month), or of all months.
# Read-only; can run next to the daemon. Exit code 1 means damage was found.
uv run yf-parqed verify-partitions --month 2026-09
uv run yf-parqed verify-partitions --all        # about an hour of disk on production

# On the production host, as the service user, so that the two files it writes
# stay writable for the daemon:
cd /var/lib/yf_parqed && sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed verify-partitions
```

**What to do with a `.damaged-*` file:**

1. **Inspect it.** The error may have been a passing one (a disk or memory hiccup), in which case the file is fine:

   ```bash
   uv run python3 -c "
   import sys, pyarrow.parquet as pq
   t = pq.ParquetFile(sys.argv[1]).read()
   print(t.num_rows, 'rows,', t.schema.names)
   " 'data/us/yahoo/stocks_1m/ticker=AAPL/year=2026/month=09/data.parquet.damaged-20261004T101500Z'
   ```

2. **If it reads (false alarm) and no new `data.parquet` exists next to it:** rename it back. Do this while the Yahoo daemon is between cycles or stopped.

   ```bash
   mv data.parquet.damaged-20261004T101500Z data.parquet
   ```

3. **If it reads and a new `data.parquet` exists** (capture continued), merge the two. Stop the Yahoo daemon first so that nothing writes the file meanwhile.

   ```bash
   uv run python3 -c "
   import sys, pandas as pd
   old, new = pd.read_parquet(sys.argv[1]), pd.read_parquet(sys.argv[2])
   merged = (pd.concat([old, new]).sort_values(['date', 'sequence'])
             .drop_duplicates('date', keep='last').sort_values('date'))
   assert set(old['date']) | set(new['date']) == set(merged['date'])
   merged.to_parquet(sys.argv[2] + '.merged', index=False, compression='gzip')
   print(len(old), '+', len(new), '->', len(merged), 'rows')
   " data.parquet.damaged-20261004T101500Z data.parquet
   mv data.parquet.merged data.parquet      # only after checking the row counts
   ```

   Keep the `.damaged-*` file until the merged file has been read back.

4. **If it does not read,** try to salvage what is left. A file with an intact footer can be read one row group and one column at a time:

   ```bash
   uv run python3 -c "
   import sys, pyarrow.parquet as pq
   f = pq.ParquetFile(sys.argv[1])          # fails here if the footer is gone
   for g in range(f.num_row_groups):
       for c in f.schema_arrow.names:
           try: f.read_row_group(g, columns=[c])
           except Exception as e: print('row group', g, 'column', c, ':', e)
   " data.parquet.damaged-20261004T101500Z
   ```

   Yahoo partition files usually hold a single row group, so this tells you which columns survive rather than which rows. If the footer is gone, nothing can be read; keep the file anyway and note it. Bars from the last seven days come back by themselves at the next cycle. Older bars cannot be fetched again.

**Do not delete a damaged file** unless you have decided that nothing in it is worth keeping. The line in `damaged_partitions.jsonl` stays either way.

**Xetra files** are not covered by this: daily Parquet files can be rebuilt from the raw cache within its retention (`xetra-parqed reprocess-raw-cache`).

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

### Problem: Daemon Killed on Stop (`stop-sigterm` timed out)

**Symptoms:**
- `journalctl -u 'xetra@DETR'` or `-u yf-parqed` shows `State 'stop-sigterm' timed out. Killing.`
- The service ends with `Failed with result 'timeout'`

**Causes and solutions:**

```bash
# 1. Was the daemon in the middle of a cycle? Look at the last log lines before the stop.
#    Both daemons check for a stop between items (Yahoo: tickers, Xetra: files and dates;
#    ADR 2026-10-03, Step E) and log "Stop requested, ending ...". If that line is missing,
#    the installed code predates Step E: this must print 6:
grep -c 'should_stop' /opt/yf_parqed/src/yf_parqed/xetra/xetra_service.py
#    A stop during ticker maintenance, or inside one long download or day merge, still waits for it.
tail -n 20 /var/log/yf_parqed/xetra-DETR.log

# 2. Was the daemon idle and did it log "Daemon shutting down gracefully" before the kill?
#    Then the installed code predates the fast exit (ADR 2026-10-03, Step F). This must print 3:
grep -c 'exit_daemon_process' /opt/yf_parqed/src/yf_parqed/xetra_cli.py
```

---

### Problem: `Read-only file system: '/run/yf_parqed'` at Start

**Symptoms:**
- A service fails right after start with `OSError: [Errno 30] Read-only file system: '/run/yf_parqed'`
- It comes back by itself after `RestartSec`

**Cause:** all yf_parqed services share the runtime directory `/run/yf_parqed`. Without `RuntimeDirectoryPreserve=yes`, systemd deletes it when any one of them stops, which removes the PID files of the others and breaks a service that is starting at that moment.

**Solution:**

```bash
# Must print "yes" for every service
systemctl show yf-parqed 'xetra@DETR' -p RuntimeDirectoryPreserve

# If not: reinstall the unit templates (answer y to the template question)
sudo ./daemon-manage.sh update
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
- Swap fills up, the whole machine slows down, other projects on the host run short of memory
- High iowait while a daemon cycle is running

**Find out which daemon and when, before changing anything:**

```bash
# 1. History of memory, swap and CPU in 10-minute samples (about a week is kept).
#    DD is the day of the month, e.g. sa02 for the 2nd.
sar -r -f /var/log/sysstat/saDD     # memory; watch kbavail
sar -S -f /var/log/sysstat/saDD     # swap used
sar -u -f /var/log/sysstat/saDD     # CPU; watch %iowait

# 2. Peaks per service as recorded by systemd.
#    The slice keeps its peaks across service restarts.
systemctl show yf-parqed 'xetra@DETR' system-xetra.slice \
  -p MemoryCurrent,MemoryPeak,MemorySwapPeak,CPUUsageNSec

# 3. Real memory of the running processes (HWM = peak resident size).
for p in $(pgrep -u yfparqed); do
  grep -E '^(Name|VmHWM|VmRSS|VmSwap)' /proc/$p/status
done

# 4. What the daemons were doing at the time of a spike.
grep -E 'Consolidat|rolled over' /var/log/yf_parqed/xetra-DETR.log*   # Xetra logs to file
journalctl -u yf-parqed --since "YYYY-MM-DD HH:MM"                     # Yahoo logs to the journal
```

A service's `MemoryPeak` includes the page cache of the files it read and wrote, which the kernel can reclaim. Compare it with `VmHWM` before concluding that a process is large.

**Known causes:**

1. **Xetra monthly consolidation at the start of a month.** The log shows `Month rolled over ... consolidating` on every cycle, and swap follows a sawtooth with the same period. Each run loads the whole previous month into memory. It stops by itself after two or three days. Tracked in [ADR 2026-10-03](../docs/adr/in-progress/2026-10-03-daemon-resource-footprint.md), Step A.

2. **Yahoo daemon rewriting every partition each cycle.** This shows as steady CPU and iowait rather than memory: the process stays around 250 MB. Check whether the installed code has the fix: `grep -c 'def merge' /opt/yf_parqed/src/yf_parqed/common/partitioned_storage_backend.py` prints 1 when it does (ADR Steps B and C: a cycle opens only the months its new bars fall into and rewrites a file only when it changes). The daemon still refetches every ticker each cycle until Step D (saving the ticker registry) is deployed. Tracked in the same ADR.

3. **A one-off migration or backfill using pandas on a large month.** Use Polars or PyArrow and process one file at a time.

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
