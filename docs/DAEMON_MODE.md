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
- **Clean exit**: Completes current ticker before shutting down
- **Resource cleanup**: Releases locks, removes PID file

```bash
# Graceful shutdown
kill $(cat /tmp/yf-parqed.pid)

# Or Ctrl+C in foreground mode
```

### 5. Error Resilience
- **Per-ticker errors**: Logs errors for individual tickers, continues with others
- **Network failures**: Retries on next scheduled run
- **Rate limiting**: Built-in rate limiting (3 requests per 2 seconds default)
- **Corruption recovery**: Automatically handles corrupt parquet files

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

# Note: By default, only runs during 08:30-18:00 CET/CEST
# Use --active-hours "00:00-23:59" for 24/7 operation
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
2025-12-01 20:39:08.993 | INFO     | yf_parqed.xetra_cli:run_fetch_once:184 - Checking missing dates for DETR
```

### 2. Scheduling with Trading Hours Awareness
- **Interval-based**: Run every N hours (default: 1 hour)
- **Trading hours**: Only runs during market hours (08:30-18:00 CET/CEST by default)
- **Smart sleeping**: Waits outside trading hours, checks for shutdown every minute
- **Error recovery**: Continues running even if individual fetches fail
- **Timezone aware**: Handles CET/CEST transitions automatically

```bash
# Run every 2 hours during trading hours (default)
xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon --interval 2

# Override to run 24/7
xetra-parqed --log-file logs/xetra.log fetch-trades DETR --daemon --interval 1 --active-hours "00:00-23:59"

# Custom hours (e.g., pre-market + trading + post-market)
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
- **Clean exit**: Completes current operation before shutting down
- **Resource cleanup**: Closes HTTP connections, removes PID file

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
Adjust rotation settings by editing the CLI source or use external log rotation:

```bash
# /etc/logrotate.d/xetra
/var/log/xetra/*.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    create 0640 xetra xetra
    sharedscripts
    postrotate
        systemctl reload xetra-detr
    endscript
}
```

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
