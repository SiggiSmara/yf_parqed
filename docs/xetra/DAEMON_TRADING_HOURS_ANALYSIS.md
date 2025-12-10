# Xetra Daemon Trading Hours Issue Analysis

**Date**: 2025-12-10  
**Issue**: Final ~30 minutes of trading data (17:30-18:00 CET) not being downloaded by daemon

## Summary

The Xetra daemon is configured with default trading hours `08:30-18:00` CET/CEST, but the final half hour of data is consistently missing. Analysis reveals **two separate but related issues**:

1. **Data filtering during download**: XetraFetcher filters files based on trading hours
2. **Daemon execution timing**: TradingHoursChecker prevents daemon runs after market close

## Current Configuration

### Daemon Setup (`daemon-manage.sh`)

```bash
ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-file /var/log/yf_parqed/xetra-%i.log \
    --log-level INFO \
    fetch-trades %i \
    --daemon \
    --interval 1 \              # Check every 1 hour
    --pid-file /run/yf_parqed/xetra-%i.pid
```

**Missing**: No `--active-hours` parameter specified, so defaults to `08:30-18:00` CET/CEST

### Trading Hours Definition

**XetraFetcher** (`xetra_fetcher.py:21`):
```python
VENUE_TRADING_HOURS = {
    "DETR": ("08:30", "18:00"),  # Xetra: data 09:00-17:30 CET, +30min safety
    "DFRA": ("08:30", "18:00"),
    "DGAT": ("08:30", "18:00"),
    "DEUR": ("08:30", "18:00"),
}
```

**TradingHoursChecker** (`xetra_cli.py:202`):
```python
# Default to Xetra trading hours (08:30-18:00 CET/CEST)
default_hours = active_hours or "08:30-18:00"
```

## Problem Analysis

### Issue 1: File Filtering Excludes Final Data

**Location**: `xetra_fetcher.py:128-209` (`is_within_trading_hours()`)

**Logic**:
```python
berlin_time = utc_dt.astimezone(ZoneInfo("Europe/Berlin"))
within_hours = start_time <= berlin_time <= end_time  # "08:30" <= time <= "18:00"
```

**Problem**: Files timestamped exactly at `18:00` CET are **included** (due to `<=`), but files after `18:00` are **excluded**. However, Deutsche Börse files are timestamped **at the end of the capture window**, not the beginning.

**Example**:
- File: `DETR-posttrade-2025-12-10T17_00.json.gz` (UTC 16:00 = CET 17:00)
  - Contains trades from approximately **16:30-17:00 CET**
  - **Included** ✓
- File: `DETR-posttrade-2025-12-10T17_30.json.gz` (UTC 16:30 = CET 17:30)
  - Contains trades from approximately **17:00-17:30 CET**
  - **Included** ✓
- File: `DETR-posttrade-2025-12-10T18_00.json.gz` (UTC 17:00 = CET 18:00)
  - Contains trades from approximately **17:30-18:00 CET**
  - **Included** (borderline) ✓
- File: `DETR-posttrade-2025-12-10T18_30.json.gz` (UTC 17:30 = CET 18:30)
  - Contains trades from approximately **18:00-18:30 CET**
  - **EXCLUDED** ✗ (but may contain late trades from 17:55-18:05)

**Reality**: Actual trading on Xetra ends at **17:30 CET** (continuous trading), but:
- Settlement processing continues until ~18:00
- Late trades and corrections may be timestamped up to 18:30
- Deutsche Börse provides data with 30-minute safety margin

### Issue 2: Daemon Stops Running After Market Close

**Location**: `xetra_cli.py:302-323`

**Logic**:
```python
while not shutdown_requested["flag"]:
    # Check if within active hours
    if not hours_checker.is_within_hours():
        wait_seconds = hours_checker.seconds_until_active()
        # Sleep until next market open
        # ...
        continue
```

**Problem**: If daemon wakes up at 18:05 CET (after `--interval 1` hour from 17:05), `is_within_hours()` returns `False`, and daemon **waits until next day's open** (08:30 CET) instead of fetching the final data.

**Scenario**:
1. **17:00 CET**: Daemon runs, fetches data up to ~16:30
2. **18:00 CET**: Daemon wakes up (1 hour later)
3. **18:00 CET**: `is_within_hours()` returns `False` (exactly at boundary)
4. **Result**: Daemon **waits until 08:30 next day** without fetching 17:00-17:30 data

### Issue 3: Sleep Until Close Logic

**Location**: `xetra_cli.py:341-345`

```python
# Calculate next run time, but avoid sleeping past market close
base_sleep_seconds = interval * 3600
close_remaining = hours_checker.seconds_until_close()
if close_remaining > 0:
    base_sleep_seconds = min(base_sleep_seconds, close_remaining)
```

**Intent**: Wake up before market close to ensure final data fetch  
**Problem**: Only works if daemon is running **before** close. If it wakes up **at or after** close, it skips fetching entirely.

## Root Causes

### 1. Conservative Trading Hours Definition

The `08:30-18:00` window is based on:
- Xetra continuous trading: **09:00-17:30 CET**
- +30 minutes safety margin before/after
- **Assumption**: All trade data will be available within this window

**Reality**:
- Deutsche Börse publishes files continuously for ~24 hours
- Files timestamped 18:00-19:00 may contain late/corrected trades
- The API is accessible 24/7, not just during trading hours

### 2. Conflation of Two Concepts

The code conflates **two different time windows**:

1. **Data filtering window** (what trades to keep):
   - Should filter **trade timestamps** to 08:00-18:00 CET
   - Removes pre-market/after-hours noise
   - Correctly implemented in `XetraParser.parse_and_filter()`

2. **Daemon execution window** (when to fetch):
   - Should fetch **whenever new data is available**
   - API is available 24/7 with ~24-hour rolling window
   - Should NOT be limited to market hours

**Current implementation**: Uses same `08:30-18:00` window for both purposes, causing daemon to stop fetching when market closes.

### 3. File Timestamp Semantics

Deutsche Börse file timestamps represent **capture completion time**, not trade time:
- File `DETR-posttrade-2025-12-10T18_30.json.gz` may contain trades from 17:55-18:05
- Filtering by file timestamp (not trade timestamp) is too aggressive
- Should download all files and filter trades by their actual `trade_time`

## Implemented Solution (2025-12-10)

### ✅ Option 1: 24/7 Daemon + Extended File Window

**Selected approach combines:**
1. **Daemon runs 24/7** (`--active-hours "00:00-23:59"`)
2. **File download window: 07:30-18:30** (+1 hour buffer before/after trading)
3. **Trade filtering: 08:00-18:00** (unchanged, in parser)

**Changes made:**

**/home/siggi/github/yf_parqed/daemon-manage.sh:**
```bash
# Line ~222: Added --active-hours parameter to xetra@.service template
# Daemon runs 24/7 to ensure all available data is captured
# File filtering (07:30-18:30) and trade filtering (08:00-18:00) happen in the fetcher/parser
ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-file /var/log/yf_parqed/xetra-%i.log \
    --log-level INFO \
    fetch-trades %i \
    --daemon \
    --interval 1 \
    --active-hours "00:00-23:59" \
    --pid-file /run/yf_parqed/xetra-%i.pid
```

**/home/siggi/github/yf_parqed/src/yf_parqed/xetra/xetra_fetcher.py:**
```python
# Line ~21: Extended VENUE_TRADING_HOURS from (08:30, 18:00) to (07:30, 18:30)
VENUE_TRADING_HOURS = {
    "DETR": ("07:30", "18:30"),  # Xetra: +1h buffer for late/early files
    "DFRA": ("07:30", "18:30"),  # Frankfurt: same as DETR
    "DGAT": ("07:30", "18:30"),  # XETRA GATEWAYS: same as DETR
    "DEUR": ("07:30", "18:30"),  # Eurex: same as DETR
}
```

**Rationale:**
- **24/7 daemon**: Eliminates risk of missing data due to daemon not running
- **Extended file window**: Captures files timestamped 18:00-18:30 that contain 17:30-18:00 trades
- **Trade filtering unchanged**: Parser still filters to 08:00-18:00 based on actual `trade_time` field
- **Minimal impact**: Configuration change only, no complex code refactoring needed

**To apply on production:**
```bash
cd /home/siggi/github/yf_parqed
sudo ./daemon-manage.sh install xetra DETR
# Or if already installed:
sudo systemctl daemon-reload
sudo systemctl restart 'xetra@*'
```

---

## Alternative Solutions (For Reference)

### Option 1 (Original): Separate Download and Filter Windows

**Change daemon to always download, filter only trade data:**

1. **Daemon execution**: Run 24/7 or use extended window `07:00-19:00`
2. **Data filtering**: Keep existing trade-time filtering in parser

**Implementation**:

```bash
# daemon-manage.sh - systemd service
ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-file /var/log/yf_parqed/xetra-%i.log \
    --log-level INFO \
    fetch-trades %i \
    --daemon \
    --interval 1 \
    --active-hours "00:00-23:59" \  # Fetch 24/7, filter trades in parser
    --pid-file /run/yf_parqed/xetra-%i.pid
```

**Pros**:
- Guarantees all available data is downloaded
- Existing trade-time filtering ensures only valid trades are stored
- No code changes needed
- Can capture late/corrected trades published after market close

**Cons**:
- Daemon runs continuously (minimal CPU impact, mostly sleeping)
- May download empty files during off-hours (mitigated by existing `filter_empty_files` logic)

### Option 2: Extend Trading Hours Window

**Use wider window with post-close buffer:**

```bash
--active-hours "08:00-19:00"  # +1 hour buffer after close
```

**Pros**:
- Simple configuration change
- Ensures 18:00-18:30 files are fetched
- Daemon still respects some time boundaries

**Cons**:
- Still arbitrary cutoff (what if late files at 19:15?)
- Doesn't solve fundamental conflation issue

### Option 3: Add Post-Close Fetch Logic

**Modify daemon to always fetch once after market close:**

```python
# After market closes at 18:00, do one final fetch
if now_time == "18:01" and not final_fetch_done:
    run_fetch_once()
    final_fetch_done = True
```

**Pros**:
- Targeted fix for the specific issue
- Minimizes unnecessary API calls

**Cons**:
- Requires code changes
- Adds complexity (state tracking)
- Doesn't handle files published hours after close

### Option 4: Make File Filtering Configurable

**Add separate flag for file timestamp filtering:**

```python
# xetra_cli.py
@app.command()
def fetch_trades(
    # ... existing parameters ...
    filter_files_by_time: Annotated[
        bool, 
        typer.Option("--filter-files/--no-filter-files", 
                     help="Filter files by timestamp (default: False, filter trades only)")
    ] = False,
```

**Default behavior**: Download all files, filter trades by `trade_time`  
**Opt-in**: Enable file filtering for bandwidth-constrained environments

**Pros**:
- Backward compatible (opt-in change)
- Gives users control
- Aligns with principle of "download everything, filter intelligently"

**Cons**:
- API change (minor)
- Requires user awareness of the flag

## Comparison Table

| Option | Code Changes | Config Changes | Completeness | Complexity |
|--------|-------------|----------------|--------------|------------|
| **Option 1** (24/7 fetch) | None | `--active-hours "00:00-23:59"` | ✓✓✓ Best | ⭐ Simplest |
| **Option 2** (extend hours) | None | `--active-hours "08:00-19:00"` | ✓✓ Good | ⭐ Simple |
| **Option 3** (post-close) | Moderate | None | ✓✓ Good | ⭐⭐ Medium |
| **Option 4** (configurable) | Moderate | Add `--no-filter-files` | ✓✓✓ Best | ⭐⭐ Medium |

## Recommended Action Plan

### Immediate Fix (Zero Code Changes)

Update systemd service configuration:

```bash
sudo systemctl edit xetra@.service
```

Add override:
```ini
[Service]
ExecStart=
ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-file /var/log/yf_parqed/xetra-%i.log \
    --log-level INFO \
    fetch-trades %i \
    --daemon \
    --interval 1 \
    --active-hours "00:00-23:59" \
    --pid-file /run/yf_parqed/xetra-%i.pid
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl restart 'xetra@*'
```

**Verification**:
```bash
# Check that final files are now being fetched
sudo journalctl -u xetra@DETR -f | grep -E "17:|18:"
```

### Long-Term Fix (Code Refactor)

1. **Separate concerns** in `xetra_cli.py`:
   - Add `--fetch-hours` (when daemon runs, default: `00:00-23:59`)
   - Keep `--active-hours` for backward compat (alias for `--fetch-hours`)
   - Document that trade filtering happens in parser, not CLI

2. **Update default in `daemon-manage.sh`**:
   ```bash
   --active-hours "00:00-23:59" \
   ```

3. **Add documentation** explaining:
   - Daemon fetches 24/7 by default
   - Trade filtering happens via `trade_time` in parser
   - Use `--active-hours` to limit fetch window if needed (e.g., cost control)

4. **Consider removing file timestamp filtering** entirely:
   - `filter_empty_files` flag → `filter_by_trade_time_only`
   - Download all available files
   - Filter by trade timestamps in parser (already implemented)

## Testing Plan

1. **Monitor current daemon** to confirm missing data:
   ```bash
   duckdb << 'EOF'
   SELECT 
       CAST(trade_time AS DATE) as date,
       MIN(trade_time) as first_trade,
       MAX(trade_time) as last_trade,
       EXTRACT(hour FROM MAX(trade_time)) as last_hour,
       EXTRACT(minute FROM MAX(trade_time)) as last_minute
   FROM read_parquet('/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet', 
                     hive_partitioning=1)
   GROUP BY date
   ORDER BY date DESC
   LIMIT 10;
   EOF
   ```

2. **Apply immediate fix** (`--active-hours "00:00-23:59"`)

3. **Monitor for 3-5 days** to confirm:
   - Final trades (17:30-18:00 CET) are now captured
   - No excessive API calls or empty files
   - Daemon stability

4. **Document findings** and update user guides

## Related Files

- `daemon-manage.sh:222` - Systemd service template
- `src/yf_parqed/xetra_cli.py:202` - Default trading hours
- `src/yf_parqed/xetra_cli.py:302-323` - Daemon main loop
- `src/yf_parqed/xetra/xetra_fetcher.py:21` - File filtering hours
- `src/yf_parqed/xetra/xetra_fetcher.py:128` - `is_within_trading_hours()`
- `src/yf_parqed/xetra/trading_hours_checker.py:86` - `is_within_hours()`
- `src/yf_parqed/xetra/xetra_parser.py` - Trade time filtering (correct)

## References

- Deutsche Börse Xetra hours: https://www.xetra.com/xetra-en/trading/trading-calendar-and-trading-hours
- Xetra continuous trading: 09:00-17:30 CET/CEST
- Closing auction: 17:30-17:35 CET/CEST
- Data availability: ~24 hours rolling window via API
