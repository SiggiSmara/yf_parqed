# Daemon Mode Guide

## Overview

Both `yf-parqed` (Yahoo Finance) and `xetra-parqed` (Xetra trades) support daemon mode for continuous data collection. This is useful for:

- Running as a background service
- Automated daily data collection
- Production deployments
- Scheduled data updates

This guide covers both data sources. Jump to:
- [Yahoo Finance Daemon](#yahoo-finance-daemon-mode)
- [Xetra Daemon](#xetra-daemon-mode)

---

# Yahoo Finance Daemon Mode

## Quick Start

### One-time update (default)
```bash
yf-parqed update-data
```

### Daemon mode (continuous, during NYSE trading hours)
```bash
yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  update-data \
  --daemon \
  --interval 1 \
  --pid-file /tmp/yf-parqed.pid

# For production with proper permissions, use /run/yf-parqed/yf-parqed.pid
# Note: By default, only runs during 09:30-16:00 US/Eastern (NYSE hours)
```

## Daemon Mode Features

### 1. Trading Hours Awareness
- **NYSE Regular Hours**: 09:30-16:00 US/Eastern (default)
- **Extended Hours**: 04:00-20:00 US/Eastern with `--extended-hours`
- **Custom Hours**: Override with `--trading-hours "HH:MM-HH:MM"`
- **Timezone Handling**: Auto-detects system timezone, converts market hours
- **DST Transitions**: Handles EST ↔ EDT automatically

```bash
# Regular trading hours (default)
yf-parqed update-data --daemon --interval 1

# Extended hours (pre-market + regular + after-hours)
yf-parqed update-data --daemon --interval 1 --extended-hours

# Custom hours in market timezone
yf-parqed update-data --daemon --interval 1 --trading-hours "08:00-18:00"

# Override market timezone (e.g., for US/Pacific)
yf-parqed update-data --daemon --interval 1 --market-timezone "US/Pacific" --trading-hours "06:30-13:00"
```

### 2. Ticker Maintenance
Periodically updates ticker lists, confirms not-founds, and reparses failed tickers.

- **weekly** (default): Every 7 days
- **daily**: Every day at first daemon cycle
- **monthly**: Every 30 days
- **never**: Manual maintenance only

```bash
# Weekly maintenance (recommended)
yf-parqed update-data --daemon --ticker-maintenance weekly

# Daily for rapidly changing ticker lists
yf-parqed update-data --daemon --ticker-maintenance daily

# Never - manual control
yf-parqed update-data --daemon --ticker-maintenance never
```

Maintenance runs:
- `update-tickers` - Fetch latest NASDAQ/NYSE ticker lists
- `confirm-not-founds` - Re-check globally not-found tickers
- `reparse-not-founds` - Reactivate tickers with recent interval data

### 3. PID File Management
- **Prevents multiple instances**: Won't start if another instance is running
- **Stale detection**: Removes stale PID files from crashed processes
- **Automatic cleanup**: PID file removed on graceful shutdown

```bash
# Development: /tmp
yf-parqed update-data --daemon --pid-file /tmp/yf-parqed.pid

# Production: /run (created by systemd RuntimeDirectory)
yf-parqed update-data --daemon --pid-file /run/yf-parqed/yf-parqed.pid
```

### 4. Graceful Shutdown
- **Signal handling**: Responds to SIGTERM and SIGINT (Ctrl+C)
- **Clean exit**: Ends the current update cycle after the ticker it is working on, then shuts down. Writes are atomic, so a ticker is either fully saved or not touched
- **Resource cleanup**: Releases locks, removes PID file

> **Known limitation:** the optional ticker maintenance (`--ticker-maintenance`) does not check for a stop request while it runs. A stop that arrives during maintenance waits for it to finish. The signal handler only records the request; the log line `Received signal ..., shutting down gracefully...` is written by the main loop when it notices. See [ADR 2026-10-03](adr/in-progress/2026-10-03-daemon-resource-footprint.md), Step E.

```bash
# Graceful shutdown
kill $(cat /tmp/yf-parqed.pid)

# Or Ctrl+C in foreground mode
```

### 5. Error Resilience
- **Per-ticker errors**: Logs the error, leaves the ticker's saved state as it was, continues with the others; after 20 failures in a row it checks whether Yahoo is answering and ends the cycle if not
- **Network failures**: The failed tickers are asked again in the next cycle of the same night (see section 6)
- **Rate limiting**: Built-in rate limiting (3 requests per 2 seconds default)
- **Damaged files**: An unreadable Parquet file is renamed (`data.parquet.damaged-<timestamp>`), never deleted, and recorded in `damaged_partitions.jsonl`; the next cycle starts a new file
- **Month-close check**: Once per month, after a cycle, every stored file of the month that just closed is read back; the result goes to `partition_checks.json`

### 6. Nightly Collection and the Ticker Registry

In daemon mode the Yahoo collector fetches every ticker **once per night**. The data is for historical analysis, so nothing is gained by fetching during the session; what protects a bar is that it is asked for on every night Yahoo still serves it (7 nights for 1-minute bars).

- **The night starts at 22:00 UTC.** That is after the US close all year (20:00 UTC in summer, 21:00 UTC in winter), so no market calendar or time zone is involved. A ticker is fetched when Yahoo has not answered for it since the most recent 22:00 UTC. Weekend nights run like any other.
- **The daemon still wakes every `--interval` hours.** A later cycle of the same night asks only for the tickers whose request failed; the log line reads `Processing 3 tickers for interval 1m (9275 already fetched tonight)`. The wait between cycles ends at 22:00 UTC, so the night's first cycle starts on time.
- **A failed request is not "no data".** A rate limit, a network error or a storage error is logged (`<TICKER> failed for interval 1m: ...`), nothing is recorded for the ticker, and the next cycle asks again. The cycle goes on with the next ticker. Yahoo's answer "no price data" is a result: the ticker is done for the night and asked again the next. An error response that yfinance words the same way (an invalid crumb, a status code) is recognised and counted as a failure.
- **A ticker that goes quiet stays open for the night.** When a ticker that had bars at its last answer returns nothing, every later cycle of that night asks again (`<n> tickers that had bars at their last fetch returned nothing; they are asked again in the next cycle of this night`). From the next night on its empty answers stand.
- **Twenty in a row: is Yahoo answering?** After 20 tickers in a row that failed or went quiet, the daemon asks Yahoo for a ticker it knows has bars. If bars come back, the cycle goes on (`... but Yahoo answers for a ticker that has bars; going on`); if not, the cycle ends and the next one tries again. A hundred failures in a row end the cycle in any case.
- **No ticker is paused or dropped by a count of empty answers.** Every ticker in the registry is asked every night. Only `remove-ticker` and `tools/prune_registry.py` stop one.
- **The registry is saved during the cycle.** `tickers.json` is written every 500 tickers and at the end of each cycle (`Tickers file saved.`), through a temp file that is synced before the rename. After a kill, the next start fetches only the tickers not yet done that night. `--save-not-founds` applies to single runs only.
- **Writers do not overwrite each other.** The daemon writes only what it changed, onto a fresh read of the file, holding `tickers.json.lock`; a ticker changed by both sides is merged key by key. `add-ticker` and `remove-ticker` can be run while the daemon is in a cycle. An unreadable `tickers.json` is never written over (`Could not save the ticker registry: ...`), and an empty registry never replaces one with content.

A single run (`update-data` without `--daemon`) is not held back by the night: it fetches every time it is called. It does not close the night either, so the daemon still does its own fetch afterwards. It exits with code 1 when a ticker failed or the cycle was ended early.

See [ADR 2026-10-03](adr/in-progress/2026-10-03-daemon-resource-footprint.md), Decision 6.

## Production Deployment

### Using systemd (Linux)

Create `/etc/systemd/system/yf-parqed.service`:

```ini
[Unit]
Description=Yahoo Finance Data Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=yfparqed
Group=yfparqed
WorkingDirectory=/var/lib/yf_parqed

# Run daemon mode
ExecStart=/opt/yf_parqed/.venv/bin/yf-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-level INFO \
    update-data \
    --daemon \
    --interval 1 \
    --ticker-maintenance weekly \
    --pid-file /run/yf-parqed/yf-parqed.pid

# Graceful shutdown
ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=60

# Restart on failure
Restart=on-failure
RestartSec=30

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/yf_parqed

# Create PID directory at startup
RuntimeDirectory=yf-parqed
RuntimeDirectoryMode=0755

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable yf-parqed
sudo systemctl start yf-parqed

# Check status
sudo systemctl status yf-parqed

# View logs
sudo journalctl -u yf-parqed -f

# Restart
sudo systemctl restart yf-parqed
```

### Using Docker

Create `Dockerfile`:
```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project
COPY . .
RUN uv sync

# Create data directory
RUN mkdir -p /app/data

# Run daemon
CMD ["uv", "run", "yf-parqed", \
     "--wrk-dir", "/app/data", \
     "update-data", \
     "--daemon", \
     "--interval", "1", \
     "--ticker-maintenance", "weekly"]
```

Run container:
```bash
docker build -t yf-parqed-daemon .
docker run -d \
  --name yf-parqed \
  -v $(pwd)/data:/app/data \
  --restart unless-stopped \
  yf-parqed-daemon

# View logs
docker logs -f yf-parqed

# Stop gracefully
docker stop yf-parqed
```

## Monitoring

### Check daemon status
```bash
# Via PID file
if [ -f /tmp/yf-parqed.pid ]; then
  pid=$(cat /tmp/yf-parqed.pid)
  if ps -p $pid > /dev/null; then
    echo "Daemon running (PID: $pid)"
  else
    echo "Daemon not running (stale PID file)"
  fi
else
  echo "Daemon not running"
fi

# Via systemd
sudo systemctl status yf-parqed
```

### Check data freshness
```bash
# Find most recently updated ticker (legacy storage)
find /var/lib/yf_parqed/stocks_1d -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -1

# Find most recently updated ticker (partitioned storage)
find /var/lib/yf_parqed/data/us/yahoo/stocks_1d -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -1
```

### Check collected data statistics (DuckDB)

```bash
# Install DuckDB if not already installed
# Ubuntu/Debian: sudo apt install duckdb-cli
# Or: wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip

# Query data statistics (partitioned storage)
duckdb << 'EOF'
-- Overall summary
SELECT 
    COUNT(DISTINCT ticker) as total_tickers,
    MIN(date) as first_date,
    MAX(date) as last_date,
    COUNT(*) as total_records,
    ROUND(SUM("close" * volume) / 1000000000, 2) as total_volume_billions_usd
FROM '/var/lib/yf_parqed/data/us/yahoo/stocks_1d/**/*.parquet';

-- Per-ticker summary (top 10 by volume)
SELECT 
    ticker,
    COUNT(*) as days_collected,
    MIN(date) as first_date,
    MAX(date) as last_date,
    ROUND(SUM("close" * volume) / 1000000, 2) as total_volume_millions_usd
FROM '/var/lib/yf_parqed/data/us/yahoo/stocks_1d/**/*.parquet'
GROUP BY ticker
ORDER BY total_volume_millions_usd DESC
LIMIT 10;

-- Recent activity (last 7 days)
SELECT 
    date,
    COUNT(DISTINCT ticker) as tickers_updated,
    COUNT(*) as total_records
FROM '/var/lib/yf_parqed/data/us/yahoo/stocks_1d/**/*.parquet'
WHERE date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY date
ORDER BY date DESC;
EOF

# Quick shell summary
echo "Yahoo Finance data summary:"
echo "  Tickers (legacy): $(find /var/lib/yf_parqed/stocks_1d -name '*.parquet' -type f | wc -l)"
echo "  Tickers (partitioned): $(find /var/lib/yf_parqed/data/us/yahoo/stocks_1d/ticker=* -maxdepth 0 -type d 2>/dev/null | wc -l)"
echo "  Total size: $(du -sh /var/lib/yf_parqed/data 2>/dev/null | cut -f1 || echo 'N/A')"
```

### Ticker maintenance status
```bash
# Check tickers.json for maintenance timestamps
cat /var/lib/yf_parqed/tickers.json | jq -r '.[] | select(.last_checked) | "\(.ticker): \(.last_checked)"' | head -10

# Count active vs not-found tickers
echo "Active tickers: $(cat /var/lib/yf_parqed/tickers.json | jq '[.[] | select(.status == "active")] | length')"
echo "Not found tickers: $(cat /var/lib/yf_parqed/tickers.json | jq '[.[] | select(.status == "not_found")] | length')"
```

## Troubleshooting

### Daemon won't start
```bash
# Check if another instance is running
cat /tmp/yf-parqed.pid
ps aux | grep yf-parqed

# Remove stale PID file
rm /tmp/yf-parqed.pid

# Check for errors
yf-parqed --log-level DEBUG update-data --daemon
```

### Missing data for some tickers
- Check `tickers.json` for ticker status
- Look for interval-specific not-found status
- Verify ticker is still traded (not delisted)
- Check logs for rate limiting errors

### High memory usage
- Reduce number of tickers (edit `tickers.json`)
- Increase interval between runs
- Use partitioned storage backend (better memory efficiency)

### Trading hours not working correctly
- Verify system timezone: `timedatectl`
- Check daemon logs for "Outside trading hours" messages
- Test with `--trading-hours "00:00-23:59"` to run 24/7

## System-Wide Installation

### Directory Layout
```
/opt/yf_parqed/          # Application code (shared between YF and Xetra)
├── .venv/               # Python virtual environment
├── src/                 # Source code
└── pyproject.toml       # Project configuration

/var/lib/yf_parqed/      # Persistent data (shared root)
├── data/                # Partitioned parquet files
│   ├── us/yahoo/stocks_*/     # YF data (no collision risk)
│   └── de/xetra/trades/       # Xetra data (if running both daemons)
├── stocks_*/            # Legacy parquet files (if applicable)
├── tickers.json         # Ticker state (YF)
├── intervals.json       # Configured intervals (YF)
└── storage_config.json  # Storage backend config (YF)

/run/yf-parqed/          # Runtime state
└── yf-parqed.pid        # PID file
```

### Installation Steps

```bash
# 1. Create dedicated user for YF data
sudo useradd -r -s /bin/false -d /var/lib/yf_parqed yfparqed

# 2. Create shared group for application and data access
sudo groupadd yf_parqed_app 2>/dev/null || true
sudo usermod -aG yf_parqed_app yfparqed

# 3. Create directories
sudo mkdir -p /opt/yf_parqed /var/lib/yf_parqed/data /run/yf-parqed

# 4. Set ownership for shared data directory
# Both YF and Xetra (if used) will write to /var/lib/yf_parqed/data
# Partition structure (us/yahoo vs de/xetra) prevents collisions
sudo chown -R yfparqed:yf_parqed_app /var/lib/yf_parqed
sudo chmod -R 775 /var/lib/yf_parqed/data  # Group write access
sudo chown -R yfparqed:yfparqed /run/yf-parqed

# 5. Install application (shared between YF and Xetra)
# If /opt/yf_parqed already exists (e.g., from Xetra installation), skip to step 6
cd /opt/yf_parqed
git clone https://github.com/SiggiSmara/yf_parqed.git .
uv sync

# Set shared ownership for application code
sudo chgrp -R yf_parqed_app /opt/yf_parqed
sudo chmod -R g+rX /opt/yf_parqed

# 6. Initialize data
cd /var/lib/yf_parqed
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed --wrk-dir /var/lib/yf_parqed initialize

# 7. Test daemon (foreground)
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  update-data --daemon --interval 1 --pid-file /tmp/yf-parqed-test.pid

# 8. Set up systemd service (see above)
```

## Best Practices

1. **Use weekly ticker maintenance** - balances freshness vs API load
2. **Stick to regular trading hours** - data is most reliable during NYSE hours
3. **Use partitioned storage** - better performance for large datasets
4. **Monitor ticker status** - check for high not-found rates
5. **Set up alerting** - notify on persistent errors
6. **Use systemd in production** - automatic restart on failure
7. **Keep interval ≥ 1 hour** - respect Yahoo Finance API rate limits
8. **Use absolute paths** - avoid working directory issues

## Security Considerations

- Run as dedicated non-root user
- Use systemd security hardening
- Restrict file permissions on `tickers.json` and data directories
- Monitor for unauthorized access
- Consider firewall rules for API access
- Regularly review not-found tickers for suspicious patterns

---

# Xetra Daemon Mode

## Quick Start

### One-time fetch (default)
```bash
xetra-parqed fetch-trades DETR
```

### Daemon mode (continuous, during trading hours)
```bash
xetra-parqed \
  --log-file logs/xetra-detr.log \
  fetch-trades DETR \
  --daemon \
  --interval 1 \
  --pid-file /tmp/xetra-detr.pid

# For production with proper permissions, use /run/xetra/detr.pid
# (requires directory creation: sudo mkdir -p /run/xetra && sudo chown xetra:xetra /run/xetra)

# Default behavior: runs 24/7. Use --active-hours to narrow (e.g., "08:30-18:00").
```

## Daemon Mode Features

### 1. File Logging with Rotation
- **Automatic rotation**: Logs rotate when they reach 10 MB
- **Retention**: Logs kept for 30 days
- **Compression**: Rotated logs are gzip compressed
- **Thread-safe**: Safe for concurrent writes

```bash
# Enable file logging
xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon
```

Log format includes timestamp, level, location, and message:
```
2025-12-01 20:39:08.993 | INFO     | yf_parqed.xetra.xetra_cli:run_fetch_once:184 - Checking missing dates for DETR
```

### 2. Scheduling with Trading Hours Awareness
- **Interval-based**: Run every N hours (default: 1 hour)
- **Trading hours**: Default is 24/7; provide `--active-hours` to narrow the window
- **Smart sleeping**: If an active-hours window is set, sleeps outside it and checks for shutdown every 10 seconds
- **Error recovery**: Continues running even if individual fetches fail
- **Timezone aware**: Handles CET/CEST transitions automatically

```bash
# Run every 2 hours (24/7 default)
xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon --interval 2

# Narrow to trading hours
xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon --interval 1 --active-hours "08:30-18:00"

# Custom window (e.g., pre/post buffers)
xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon --interval 1 --active-hours "07:00-19:00"
```

### 3. PID File Management
- **Prevents multiple instances**: Won't start if another instance is running
- **Stale detection**: Removes stale PID files from crashed processes
- **Automatic cleanup**: PID file removed on graceful shutdown

```bash
# Use PID file to prevent duplicates (development)
xetra-parqed --log-file logs/xetra.log fetch-trades DETR \
  --daemon --pid-file /tmp/xetra-detr.pid

# Production: use /run/xetra/ (created by systemd RuntimeDirectory)
xetra-parqed --log-file logs/xetra.log fetch-trades DETR \
  --daemon --pid-file /run/xetra/detr.pid
```

### 4. Graceful Shutdown
- **Signal handling**: Responds to SIGTERM and SIGINT (Ctrl+C)
- **Clean exit**: Ends the current fetch cycle after the file it is downloading (and abandons a monthly consolidation between days); while waiting between cycles it reacts within 10 seconds. The fetcher's 35-second burst cooldown and its 429 retry waits also end early. Files already stored are kept and the next run resumes where it stopped
- **Resource cleanup**: Closes HTTP connections, removes PID file
- **Fast exit**: Once cleanup is done the process ends immediately, without Python's interpreter teardown. On a host short of RAM the idle daemon's memory is in swap, and a normal exit reads it all back from disk before freeing it, which used to take longer than `TimeoutStopSec`. The Yahoo and ISIN mapping daemons exit the same way.

> **Note:** a download or a single-day merge that is already running finishes first, which takes a few seconds. The signal handler only records the request; the main loop logs `Received signal ..., shutting down gracefully...`. See [ADR 2026-10-03](adr/in-progress/2026-10-03-daemon-resource-footprint.md), Step E.

```bash
# Graceful shutdown
kill $(cat /tmp/xetra-detr.pid)

# Or Ctrl+C in foreground mode
```

### 5. Error Resilience
- **Transient errors**: Logs errors but continues running
- **Network failures**: Retries on next scheduled run
- **Rate limiting**: Built-in rate limiting prevents API bans

## Production Deployment

### Using systemd (Linux)

Create `/etc/systemd/system/xetra-detr.service`:

```ini
[Unit]
Description=Xetra DETR Trade Data Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=xetra
Group=xetra
WorkingDirectory=/var/lib/yf_parqed
Environment="PATH=/opt/yf_parqed/.venv/bin:/usr/local/bin:/usr/bin:/bin"

# Run daemon mode with logging
ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-file /var/log/xetra/detr.log \
    fetch-trades DETR \
    --daemon \
    --interval 1 \
    --pid-file /run/xetra/detr.pid

# Graceful shutdown
ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=30

# Restart on failure
Restart=on-failure
RestartSec=30

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/yf_parqed /var/log/xetra /run/xetra

# Create PID directory at startup
RuntimeDirectory=xetra
RuntimeDirectoryMode=0755

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable xetra-detr
sudo systemctl start xetra-detr

# Check status
sudo systemctl status xetra-detr

# View logs
sudo journalctl -u xetra-detr -f
```

### Using Docker

Create `Dockerfile`:
```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project
COPY . .
RUN uv sync

# Create log directory
RUN mkdir -p /app/logs

# Run daemon
CMD ["uv", "run", "xetra-parqed", \
     "--log-file", "/app/logs/xetra.log", \
     "fetch-trades", "DETR", \
     "--daemon", "--interval", "1"]
```

Run container:
```bash
docker build -t xetra-daemon .
docker run -d \
  --name xetra-detr \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  --restart unless-stopped \
  xetra-daemon

# View logs
docker logs -f xetra-detr
```

### Using cron (Alternative)

If you prefer cron over daemon mode:

```bash
# Add to crontab (run every hour)
0 * * * * cd /var/lib/xetra && /opt/yf_parqed/.venv/bin/xetra-parqed --wrk-dir /var/lib/xetra fetch-trades DETR >> /var/log/xetra/detr.log 2>&1
```

**Note**: Daemon mode is preferred over cron because:
- No need for run-lock coordination
- Better error handling and logging
- Graceful shutdown support
- PID file prevents overlapping runs

## Log Levels

Control verbosity with `--log-level`:

```bash
# INFO (default) - high-level progress
xetra-parqed --log-level INFO --log-file logs/xetra.log fetch-trades DETR --daemon

# DEBUG - detailed per-file operations
xetra-parqed --log-level DEBUG --log-file logs/xetra.log fetch-trades DETR --daemon

# WARNING - errors and warnings only
xetra-parqed --log-level WARNING --log-file logs/xetra.log fetch-trades DETR --daemon
```

## Monitoring

### Check daemon status
```bash
# Via PID file
if [ -f /tmp/xetra-detr.pid ]; then
  pid=$(cat /tmp/xetra-detr.pid)
  if ps -p $pid > /dev/null; then
    echo "Daemon running (PID: $pid)"
  else
    echo "Daemon not running (stale PID file)"
  fi
else
  echo "Daemon not running"
fi
```

### Tail logs
```bash
tail -f logs/xetra-detr.log

# Or with grep for errors
tail -f logs/xetra-detr.log | grep -i error
```

### Check data freshness
```bash
# Find most recent data file
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -1
```

### Check collected data statistics
```bash
# Count total days collected for a venue
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name "day=*" -type d | wc -l

# List all collected dates (year/month/day format)
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -type f -name "*.parquet" | \
  sed -E 's|.*year=([0-9]{4})/month=([0-9]{2})/day=([0-9]{2})/.*|\1-\2-\3|' | sort -u

# Count total parquet files
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name "*.parquet" -type f | wc -l

# Check total data size
du -sh /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR

# Detailed summary with DuckDB (requires DuckDB installed)
duckdb << 'EOF'
-- Overall summary
SELECT 
    COUNT(*) as total_trades,
    COUNT(DISTINCT day) as total_days,
    MIN(day) as first_date,
    MAX(day) as last_date,
    ROUND(SUM(price * volume) / 1000000, 2) as total_volume_millions_eur
FROM '/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/**/*.parquet';

-- Per-day breakdown with minutes captured
SELECT 
    day,
    COUNT(*) as trades,
    COUNT(DISTINCT strftime(trade_time, '%H:%M')) as unique_minutes,
    MIN(trade_time)::TIME as first_trade,
    MAX(trade_time)::TIME as last_trade,
    ROUND(SUM(price * volume) / 1000000, 2) as volume_millions_eur,
    COUNT(DISTINCT isin) as unique_isins
FROM '/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/**/*.parquet'
GROUP BY day
ORDER BY day DESC;
EOF

# Quick summary (shell script - no DuckDB required)
echo "Collected data summary for DETR:"
echo "  Days: $(find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name 'day=*' -type d | wc -l)"
echo "  Files: $(find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name '*.parquet' -type f | wc -l)"
echo "  Size: $(du -sh /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR | cut -f1)"
echo "  Dates collected:"
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -type f -name "*.parquet" | \
  sed -E 's|.*year=([0-9]{4})/month=([0-9]{2})/day=([0-9]{2})/.*|    \1-\2-\3|' | sort -u
```

## Multiple Venues

Run separate daemons for each venue:

```bash
# DETR (Xetra)
xetra-parqed --log-file logs/detr.log fetch-trades DETR --daemon --pid-file /tmp/xetra-detr.pid &

# DFRA (Frankfurt Floor)
xetra-parqed --log-file logs/dfra.log fetch-trades DFRA --daemon --pid-file /tmp/xetra-dfra.pid &

# DGAT (Gateways)
xetra-parqed --log-file logs/dgat.log fetch-trades DGAT --daemon --pid-file /tmp/xetra-dgat.pid &

# DEUR (Eurex)
xetra-parqed --log-file logs/deur.log fetch-trades DEUR --daemon --pid-file /tmp/xetra-deur.pid &
```

Or use systemd templates (see systemd documentation).

## Troubleshooting

### Daemon won't start
```bash
# Check if another instance is running
cat /tmp/xetra-detr.pid
ps aux | grep xetra-parqed

# Remove stale PID file
rm /tmp/xetra-detr.pid

# Check logs for errors
tail -50 logs/xetra-detr.log
```

### Permission denied or read-only file system for PID file
If you see `OSError: [Errno 30] Read-only file system` or `PermissionError` for PID file:

```bash
# This happens when PID path doesn't match RuntimeDirectory
# Correct: --pid-file /run/xetra/detr.pid (matches RuntimeDirectory=xetra)
# Wrong: --pid-file /run/xetra-detr.pid (no directory separator)

# Fix: Update service file and reload
sudo systemctl daemon-reload
sudo systemctl restart xetra@DETR

# Verify RuntimeDirectory created the directory
ls -la /run/xetra/
```

### High memory usage
- Check log rotation is working (old logs should be compressed)
- Ensure download log is pruned periodically
- Consider increasing interval between runs

### Missing data
- Check logs for errors: `grep ERROR logs/xetra-detr.log`
- Verify network connectivity to Deutsche Börse API
- Check rate limiting hasn't triggered (look for 429 errors)
- Manually run one-time fetch to see immediate feedback

### Logs growing too large
The daemon already rotates its own log file at 10 MB and keeps 30 days (see `rotation` and `retention` in `xetra_cli.py`). To also rotate on a schedule, use logrotate with `copytruncate`:

```bash
# /etc/logrotate.d/xetra
/var/log/yf_parqed/*.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    missingok
    copytruncate
}
```

`copytruncate` keeps the daemon writing to the same file, so no restart is needed. Do not add a `postrotate` that restarts or reloads the service: the units have no reload action, so that restarts the daemon and interrupts the cycle in progress. (`daemon-manage.sh install` sets this up for you.)

## Trading Hours Behavior

### Default (Recommended)
By default, daemon mode respects Xetra trading hours:
- **Active**: 08:30-18:00 CET/CEST (includes safety margins around 09:00-17:30 core trading)
- **Inactive**: Sleeps outside these hours, wakes up at 08:30
- **Automatic**: Handles DST transitions (CET ↔ CEST)

**Why?** The Deutsche Börse API updates data during trading hours. Running outside these hours wastes resources and finds no new data.

### Custom Hours
Override with `--active-hours` for special cases:
```bash
# Extended hours (pre-market + post-market monitoring)
--active-hours "07:00-20:00"

# 24/7 operation (not recommended - API has no data outside trading hours)
--active-hours "00:00-23:59"

# Overnight processing only (unusual use case)
--active-hours "22:00-06:00"
```

### Log Messages
When daemon is outside active hours:
```
2025-12-01 18:30:15 | INFO | Outside active hours. Waiting until 2025-12-02 08:30:00 CET
2025-12-02 08:30:00 | INFO | Entering active hours, starting fetch cycle
```

### Weekends and Holidays
**Note**: The daemon will still wake up during active hours on weekends and holidays, but will find no new data (API returns empty results). This is expected behavior and causes no harm - the daemon will simply log "All available data already stored" and wait for the next interval.

If you want to avoid unnecessary weekend runs, use systemd calendar-based scheduling or cron instead of daemon mode.

## Best Practices

1. **Use separate log files per venue** - easier to troubleshoot
2. **Monitor log file sizes** - ensure rotation is working
3. **Set up alerting** - notify on persistent errors
4. **Use systemd in production** - automatic restart on failure
5. **Test with --no-store first** - verify configuration before storing data
6. **Keep default trading hours** - API only has data 08:30-18:00 CET/CEST
7. **Keep interval ≥ 1 hour** - API data updates roughly hourly
8. **Use absolute paths** - avoid issues with working directory

## System-Wide Installation

For daemon mode, install `yf_parqed` system-wide following Linux Filesystem Hierarchy Standard:

### Directory Layout
```
/opt/yf_parqed/          # Application code (shared with YF daemon)
├── .venv/               # Python virtual environment
├── src/                 # Source code
└── pyproject.toml       # Project configuration

/var/lib/yf_parqed/      # Persistent data (shared root with YF)
├── data/                # Partitioned parquet files
│   ├── us/yahoo/stocks_*/     # YF data (if running both daemons)
│   └── de/xetra/trades/       # Xetra data (no collision risk)
├── tickers.json         # YF state (if applicable)
└── intervals.json       # YF config (if applicable)

/var/log/xetra/          # Application logs
└── *.log                # Log files with rotation

/run/xetra/              # Runtime state (systemd RuntimeDirectory)
└── *.pid                # PID files
```

### Installation Steps

```bash
# 1. Create dedicated user for Xetra data (system account, no login)
sudo useradd -r -s /bin/false -d /var/lib/yf_parqed xetra

# 2. Add to shared group for data access
sudo groupadd yf_parqed_app 2>/dev/null || true
sudo usermod -aG yf_parqed_app xetra

# 3. Create directory structure
# Note: Using shared /var/lib/yf_parqed for data (partition structure prevents collisions)
sudo mkdir -p /opt/yf_parqed /var/lib/yf_parqed/data /var/log/xetra /run/xetra

# 4. Set ownership for shared data directory
# If /var/lib/yf_parqed already exists from YF installation, just add xetra to group
if [ ! -d "/var/lib/yf_parqed" ]; then
  sudo chown -R xetra:yf_parqed_app /var/lib/yf_parqed
  sudo chmod -R 775 /var/lib/yf_parqed/data
fi
sudo chown -R xetra:xetra /var/log/xetra /run/xetra

# 5. Install application code and dependencies
# If /opt/yf_parqed already exists (e.g., from YF installation), skip to step 6

if [ ! -d "/opt/yf_parqed/.git" ]; then
  sudo mkdir -p /opt/yf_parqed
  git clone https://github.com/SiggiSmara/yf_parqed.git /opt/yf_parqed
  cd /opt/yf_parqed
  uv sync
  
  # Set shared ownership for application code
  sudo chgrp -R yf_parqed_app /opt/yf_parqed
  sudo chmod -R g+rX /opt/yf_parqed
fi

# 6. Verify installation
sudo -u xetra /opt/yf_parqed/.venv/bin/xetra-parqed --help

# 7. Test data collection
cd /var/lib/yf_parqed
sudo -u xetra /opt/yf_parqed/.venv/bin/xetra-parqed --wrk-dir /var/lib/yf_parqed fetch-trades DETR --no-store
```

### Why This Structure?

- **`/opt/yf_parqed`** - Optional software packages (FHS standard for add-on applications)
  - Can be updated/reinstalled without affecting data
  - Managed by version control (git)
  - Shared between YF and Xetra daemons
  
- **`/var/lib/yf_parqed`** - Variable application state/data (FHS standard)
  - Persists across application upgrades
  - Backed up separately from application code
  - **Shared data directory**: Both YF and Xetra write here
  - Partition structure prevents collisions:
    * YF: `data/us/yahoo/stocks_*/`
    * Xetra: `data/de/xetra/trades/`
  - Group permissions (yf_parqed_app) allow both daemons write access
  
- **`/var/log/xetra`** - Application logs (FHS standard)
  - Managed by logrotate
  - Can be monitored by log aggregation tools
  - Separate logs per daemon for clarity
  
- **`/var/run`** - Runtime variable data (FHS standard)
  - PID files for process management
  - Cleaned on reboot

## Example: Production Setup

Complete setup for DETR venue:

```bash
# 1. System-wide installation (see above)
# Follow all steps in "System-Wide Installation" section

# 2. Install application (if not already installed by YF daemon)
if [ ! -d "/opt/yf_parqed/.git" ]; then
  cd /opt/yf_parqed
  git clone https://github.com/SiggiSmara/yf_parqed.git .
  uv sync
  sudo chgrp -R yf_parqed_app /opt/yf_parqed
  sudo chmod -R g+rX /opt/yf_parqed
fi

# 3. Create systemd service (see above)
sudo nano /etc/systemd/system/xetra-detr.service

# 4. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable xetra-detr
sudo systemctl start xetra-detr

# 5. Verify
sudo systemctl status xetra-detr
sudo tail -f /var/log/xetra/detr.log

# 6. Set up log rotation
sudo nano /etc/logrotate.d/xetra

# 7. Monitor
sudo journalctl -u xetra-detr -f
```

## Security Considerations

- Run as dedicated user (not root)
- Use systemd security hardening (ProtectSystem, PrivateTmp, etc.)
- Restrict file permissions on logs and data directories
- Consider firewall rules if running on a server
- Rotate and archive logs regularly
- Monitor for unauthorized access to data files

## Support

For issues or questions:
- GitHub: https://github.com/SiggiSmara/yf_parqed/issues
- Check logs first: `tail -f logs/xetra.log`
- Run with `--log-level DEBUG` for detailed diagnostics
