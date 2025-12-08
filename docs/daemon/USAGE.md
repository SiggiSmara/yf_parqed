# Daemon Usage Guide

Day-to-day operations, monitoring, and troubleshooting for yf_parqed daemons.

## Prerequisites

This guide assumes you have already installed the daemons. If not, see **[INSTALLATION.md](INSTALLATION.md)**.

## Managing Daemons

### Yahoo Finance Daemon

```bash
# Start daemon
sudo systemctl start yf-parqed

# Stop daemon
sudo systemctl stop yf-parqed

# Restart daemon
sudo systemctl restart yf-parqed

# Enable auto-start on boot
sudo systemctl enable yf-parqed

# Disable auto-start
sudo systemctl disable yf-parqed

# Check status
sudo systemctl status yf-parqed

# View recent logs
sudo journalctl -u yf-parqed -n 100

# Follow logs in real-time
sudo journalctl -u yf-parqed -f
```

### Xetra Daemons (Per Venue)

Xetra uses systemd templates, so you manage each venue independently:

```bash
# Start DETR (Xetra) daemon
sudo systemctl start xetra@DETR

# Start DFRA (Frankfurt Floor) daemon
sudo systemctl start xetra@DFRA

# Start multiple venues
sudo systemctl start xetra@DETR xetra@DFRA xetra@DGAT

# Enable auto-start for all venues
sudo systemctl enable xetra@DETR xetra@DFRA xetra@DGAT xetra@DEUR

# Check status of all Xetra daemons
sudo systemctl status 'xetra@*'

# Stop all Xetra daemons
sudo systemctl stop 'xetra@*'

# Restart all Xetra daemons
sudo systemctl restart 'xetra@*'
```

### Managing All Daemons

```bash
# Start all
sudo systemctl start yf-parqed 'xetra@*'

# Stop all
sudo systemctl stop yf-parqed 'xetra@*'

# Restart all
sudo systemctl restart yf-parqed 'xetra@*'

# Check status of all
sudo systemctl status yf-parqed 'xetra@*'

# View all daemon logs
sudo journalctl -u yf-parqed -u 'xetra@*' -f
```

## Monitoring

### Quick Status Check

```bash
# Use the monitoring script (installed during setup)
yf-parqed-status

# Sample output:
# === YF Parqed Daemon Status ===
# Services:
# yf-parqed active
# xetra@DETR.service active
#
# Data freshness (last 5 updates):
# 2025-12-04 14:23:45 /var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=AAPL/year=2025/month=12/data.parquet
# ...
```

### Check Data Freshness

```bash
# Yahoo Finance - most recent ticker updates (partitioned storage)
find /var/lib/yf_parqed/data/us/yahoo/stocks_1m -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -5

# Xetra - most recent trade data (partitioned by day)
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -5

# Count tickers updated today (partitioned storage)
find /var/lib/yf_parqed/data/us/yahoo/stocks_1m -name "*.parquet" -type f -mtime 0 | wc -l
```

### Check Storage Usage

```bash
# Total data size
du -sh /var/lib/yf_parqed/data

# Per data source (partitioned storage)
du -sh /var/lib/yf_parqed/data/us/yahoo/stocks_*
du -sh /var/lib/yf_parqed/data/de/xetra/trades
du -sh /var/lib/yf_parqed/data/de/xetra/trades_monthly

# Per venue breakdown
du -sh /var/lib/yf_parqed/data/de/xetra/trades/venue=*

# Detailed breakdown
du -h --max-depth=4 /var/lib/yf_parqed/data | sort -h
```

### Monitor Logs

```bash
# Follow all daemon logs
sudo journalctl -u yf-parqed -u 'xetra@*' -f

# Check for errors in last hour
sudo journalctl -u yf-parqed -u 'xetra@*' --since "1 hour ago" | grep -i error

# Check specific time range
sudo journalctl -u yf-parqed --since "2025-12-04 09:00" --until "2025-12-04 17:00"

# View log files directly
tail -f /var/log/yf_parqed/yf-parqed.log
tail -f /var/log/yf_parqed/xetra-DETR.log

# Search logs for specific ticker
sudo journalctl -u yf-parqed | grep -i "AAPL"
```

### Check Daemon Process

```bash
# Find running daemons
ps aux | grep -E 'yf-parqed|xetra-parqed'

# Check memory and CPU usage
ps aux | grep -E 'yf-parqed|xetra-parqed' | awk '{print $2, $3, $4, $11}'

# View PID files
ls -la /run/yf_parqed/

# Check if PID is still running
cat /run/yf_parqed/yf-parqed.pid
ps -p $(cat /run/yf_parqed/yf-parqed.pid)
```

### Trading Hours Status

```bash
# Check if daemon is currently active (inside trading hours)
sudo journalctl -u yf-parqed -n 20 | grep -i "trading hours"

# Sample output:
# "Within trading hours, starting data collection"
# or
# "Outside trading hours. Waiting until 2025-12-05 09:30:00 EST"
```

## Updating

### Using Setup Script (Recommended)

```bash
# Update to latest version
sudo /opt/yf_parqed/daemon-manage.sh update

# The script will:
# 1. Stop all daemons
# 2. Pull latest code from GitHub
# 3. Update Python dependencies
# 4. Restart daemons

# Check status after update
yf-parqed-status
```

### Manual Update

```bash
# Stop daemons
sudo systemctl stop yf-parqed 'xetra@*'

# Update code
cd /opt/yf_parqed
sudo -u yfparqed git fetch origin
sudo -u yfparqed git pull origin main

# Update dependencies
sudo -u yfparqed /var/lib/yf_parqed/.local/bin/uv sync

# Restart daemons
sudo systemctl start yf-parqed
sudo systemctl start 'xetra@*'

# Verify
sudo systemctl status yf-parqed 'xetra@*'
```

### Check Current Version

```bash
# Check installed version
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed --version
sudo -u yfparqed /opt/yf_parqed/.venv/bin/xetra-parqed --version

# Check git commit
cd /opt/yf_parqed
sudo -u yfparqed git log -1 --oneline
```

## Configuration

### Modifying Yahoo Finance Daemon

Edit the service file:

```bash
sudo nano /etc/systemd/system/yf-parqed.service
```

Common changes:

```ini
# Change update interval (default: 1 hour)
--interval 2

# Change ticker maintenance frequency
--ticker-maintenance daily    # or weekly, monthly, never

# Enable extended trading hours (04:00-20:00 ET)
--extended-hours

# Custom trading hours (in market timezone)
--trading-hours "08:00-18:00"

# Custom market timezone
--market-timezone "US/Pacific"

# Change log level
--log-level DEBUG    # or INFO, WARNING, ERROR
```

After changes:

```bash
sudo systemctl daemon-reload
sudo systemctl restart yf-parqed
sudo systemctl status yf-parqed
```

### Modifying Xetra Daemon

Edit the template service file:

```bash
sudo nano /etc/systemd/system/xetra@.service
```

Common changes:

```ini
# Change update interval
--interval 2

# Custom active hours
--active-hours "07:00-19:00"

# Run 24/7 (not recommended - API has no data outside trading hours)
--active-hours "00:00-23:59"

# Change log level
--log-level DEBUG
```

After changes:

```bash
sudo systemctl daemon-reload
sudo systemctl restart 'xetra@*'
sudo systemctl status 'xetra@*'
```

### Modifying Data Storage Location

```bash
# Create new data directory
sudo mkdir -p /data/yf_parqed
sudo chown yfparqed:yfparqed /data/yf_parqed

# Update service file
sudo nano /etc/systemd/system/yf-parqed.service
# Change: --wrk-dir /data/yf_parqed

# Update Xetra service
sudo nano /etc/systemd/system/xetra@.service
# Change: --wrk-dir /data/yf_parqed

# Migrate existing data (optional)
sudo -u yfparqed rsync -av /var/lib/yf_parqed/data/ /data/yf_parqed/data/

# Reload and restart
sudo systemctl daemon-reload
sudo systemctl restart yf-parqed 'xetra@*'
```

## Troubleshooting

### Daemon Won't Start

```bash
# Check service status for errors
sudo systemctl status yf-parqed

# View full logs
sudo journalctl -u yf-parqed -n 100 --no-pager

# Check if another instance is running
ps aux | grep yf-parqed

# Check PID file
cat /run/yf_parqed/yf-parqed.pid
ps -p $(cat /run/yf_parqed/yf-parqed.pid)

# Remove stale PID file
sudo rm /run/yf_parqed/yf-parqed.pid
sudo systemctl start yf-parqed

# Test manually
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  --log-level DEBUG \
  update-data --daemon --pid-file /tmp/test.pid
```

### Permission Errors

```bash
# Check ownership
ls -la /opt/yf_parqed
ls -la /var/lib/yf_parqed
ls -la /var/log/yf_parqed

# Fix ownership
sudo chown -R yfparqed:yfparqed /opt/yf_parqed
sudo chown -R yfparqed:yfparqed /var/lib/yf_parqed
sudo chown -R yfparqed:yfparqed /var/log/yf_parqed

# Check file permissions
sudo find /opt/yf_parqed -type f -perm /o+w  # Should be empty
sudo find /var/lib/yf_parqed -type f -perm /o+w  # Should be empty

# Fix if needed
sudo chmod -R u+rwX,go+rX,go-w /opt/yf_parqed
sudo chmod -R u+rwX,go+rX,go-w /var/lib/yf_parqed
```

### Data Not Updating

```bash
# Check if daemon is running
sudo systemctl is-active yf-parqed

# Check trading hours status
sudo journalctl -u yf-parqed -n 50 | grep -i "trading hours"

# Test manual data fetch
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  --log-level DEBUG \
  update-data

# Check ticker status
sudo -u yfparqed cat /var/lib/yf_parqed/tickers.json | jq -r '.[] | select(.status == "not_found")' | head -10

# Check if rate limiting is affecting
sudo journalctl -u yf-parqed -n 100 | grep -i "rate"

# Manually trigger ticker maintenance
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  update-tickers
```

### High Memory Usage

```bash
# Check memory usage
ps aux | grep -E 'yf-parqed|xetra-parqed' | awk '{print $2, $3, $4, $6, $11}'

# Check number of tickers
sudo -u yfparqed cat /var/lib/yf_parqed/tickers.json | jq 'length'

# Reduce ticker count (edit tickers.json)
sudo -u yfparqed nano /var/lib/yf_parqed/tickers.json

# Or filter to specific tickers
sudo -u yfparqed cat /var/lib/yf_parqed/tickers.json | \
  jq 'with_entries(select(.key | IN("AAPL", "MSFT", "GOOGL")))' > /tmp/filtered.json
sudo -u yfparqed mv /tmp/filtered.json /var/lib/yf_parqed/tickers.json

# Increase update interval
sudo nano /etc/systemd/system/yf-parqed.service
# Change --interval 1 to --interval 2
sudo systemctl daemon-reload
sudo systemctl restart yf-parqed
```

### High Disk Usage

```bash
# Check data size
du -sh /var/lib/yf_parqed/data/*

# Check old/legacy data
find /var/lib/yf_parqed -name "stocks_*" -type d

# Clean up legacy data (if migrated to partitioned storage)
# WARNING: Only do this if you're sure data is in partitioned format
sudo -u yfparqed rm -rf /var/lib/yf_parqed/stocks_*

# Check log sizes
du -sh /var/log/yf_parqed/*

# Force log rotation
sudo logrotate -f /etc/logrotate.d/yf_parqed
```

### Network/API Errors

```bash
# Check recent errors
sudo journalctl -u yf-parqed --since "1 hour ago" | grep -i error

# Test network connectivity
curl -I https://query1.finance.yahoo.com
curl -I https://api.deutsche-boerse.com

# Check DNS resolution
nslookup query1.finance.yahoo.com

# Test with increased verbosity
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  --log-level DEBUG \
  update-data
```

### Trading Hours Not Working

```bash
# Check system timezone
timedatectl

# Check daemon logs for trading hours messages
sudo journalctl -u yf-parqed -n 50 | grep -i "trading hours"

# Test with 24/7 mode temporarily
sudo nano /etc/systemd/system/yf-parqed.service
# Add: --trading-hours "00:00-23:59"
sudo systemctl daemon-reload
sudo systemctl restart yf-parqed

# Verify market timezone setting
sudo journalctl -u yf-parqed -n 10 | grep -i timezone
```

## Backup and Recovery

### Backup Data

```bash
# Backup data directory
sudo tar -czf /backup/yf_parqed-data-$(date +%Y%m%d).tar.gz \
  -C /var/lib/yf_parqed data

# Backup configuration
sudo tar -czf /backup/yf_parqed-config-$(date +%Y%m%d).tar.gz \
  /var/lib/yf_parqed/tickers.json \
  /var/lib/yf_parqed/intervals.json \
  /var/lib/yf_parqed/storage_config.json

# Backup everything
sudo tar -czf /backup/yf_parqed-full-$(date +%Y%m%d).tar.gz \
  /var/lib/yf_parqed
```

### Restore Data

```bash
# Stop daemons
sudo systemctl stop yf-parqed 'xetra@*'

# Restore data
sudo tar -xzf /backup/yf_parqed-data-20251204.tar.gz \
  -C /var/lib/yf_parqed

# Restore configuration
sudo tar -xzf /backup/yf_parqed-config-20251204.tar.gz -C /

# Fix ownership
sudo chown -R yfparqed:yfparqed /var/lib/yf_parqed

# Restart daemons
sudo systemctl start yf-parqed 'xetra@*'
```

### Automated Backup

Create `/etc/cron.daily/yf_parqed-backup`:

```bash
sudo tee /etc/cron.daily/yf_parqed-backup > /dev/null << 'EOF'
#!/bin/bash
BACKUP_DIR="/backup/yf_parqed"
DATE=$(date +%Y%m%d)
mkdir -p "$BACKUP_DIR"

# Backup data
tar -czf "$BACKUP_DIR/data-$DATE.tar.gz" \
  -C /var/lib/yf_parqed data

# Backup config
tar -czf "$BACKUP_DIR/config-$DATE.tar.gz" \
  /var/lib/yf_parqed/*.json

# Keep last 30 days
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +30 -delete
EOF

sudo chmod +x /etc/cron.daily/yf_parqed-backup
```

## Performance Tuning

### Optimize for Low Resource Systems

```bash
# Reduce update frequency
sudo nano /etc/systemd/system/yf-parqed.service
# Change: --interval 2  (or higher)

# Reduce ticker count
sudo -u yfparqed nano /var/lib/yf_parqed/tickers.json

# Use monthly ticker maintenance
# Change: --ticker-maintenance monthly
```

### Optimize for High Throughput

```bash
# Run multiple interval-specific daemons
# See "Advanced" section in README.md

# Use faster storage (SSD)
# Mount /var/lib/yf_parqed/data on SSD

# Increase system limits (if needed)
sudo nano /etc/security/limits.conf
# Add: yfparqed soft nofile 65536
# Add: yfparqed hard nofile 65536
```

## Advanced Usage

### Query Data with DuckDB

```bash
# Install DuckDB if not already installed
sudo apt install duckdb  # or download from duckdb.org

# Overview: Yahoo Finance data capture statistics (1m interval)
duckdb << 'EOF'
WITH trading_days AS (
    SELECT 
        "date"::DATE as trading_date,
        COUNT(DISTINCT ticker) as tickers_captured
    FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet', hive_partitioning=1)
    GROUP BY trading_date
),
totals AS (
    SELECT COUNT(DISTINCT ticker) as total_tickers
    FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet', hive_partitioning=1)
)
SELECT 
    trading_date,
    tickers_captured,
    (SELECT total_tickers FROM totals) as total_tickers,
    ROUND(100.0 * tickers_captured / (SELECT total_tickers FROM totals), 2) as capture_rate_pct,
    CASE 
        WHEN tickers_captured = (SELECT total_tickers FROM totals) THEN '✓ Complete'
        WHEN tickers_captured >= (SELECT total_tickers FROM totals) * 0.95 THEN '⚠ Partial'
        ELSE '✗ Incomplete'
    END as status
FROM trading_days
ORDER BY trading_date DESC
LIMIT 10;
EOF

# Query Yahoo Finance data (partitioned storage)
duckdb << 'EOF'
-- Ticker count and date range
SELECT 
    COUNT(DISTINCT ticker) as total_tickers,
    MIN("date") as first_date,
    MAX("date") as last_date,
    COUNT(*) as total_records
FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet', hive_partitioning=1);

-- Top 10 tickers by volume
SELECT 
    ticker,
    ROUND(SUM("close" * volume) / 1000000, 2) as total_volume_millions
FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet', hive_partitioning=1)
GROUP BY ticker
ORDER BY total_volume_millions DESC
LIMIT 10;
EOF

# Overview: Xetra data capture statistics
# Shows minutes of data captured per day vs theoretical max (trading hours: 08:00-18:00 CET = 600 minutes)
duckdb << 'EOF'
WITH daily_stats AS (
    SELECT 
        CAST(trade_time AS DATE) as trade_date,
        COUNT(*) as trades_captured,
        COUNT(DISTINCT isin) as unique_isins,
        -- Calculate unique minutes with trade data
        COUNT(DISTINCT strftime('%Y-%m-%d %H:%M', trade_time)) as minutes_captured,
        ROUND(SUM(price * volume) / 1000000, 2) as volume_millions_eur,
        MIN(trade_time) as first_trade,
        MAX(trade_time) as last_trade
    FROM read_parquet('/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet', hive_partitioning=1)
    GROUP BY trade_date
)
SELECT 
    trade_date,
    trades_captured,
    unique_isins,
    minutes_captured,
    600 as theoretical_max_minutes,  -- 10 hours (08:00-18:00) = 600 minutes
    ROUND(100.0 * minutes_captured / 600, 2) as capture_rate_pct,
    volume_millions_eur,
    strftime('%H:%M', first_trade) as first_trade_time,
    strftime('%H:%M', last_trade) as last_trade_time,
    CASE 
        WHEN minutes_captured >= 540 THEN '✓ Complete'  -- 90%+ of trading hours
        WHEN minutes_captured >= 450 THEN '⚠ Partial'   -- 75%+ of trading hours
        ELSE '✗ Incomplete'
    END as status
FROM daily_stats
ORDER BY trade_date DESC
LIMIT 10;
EOF

# Overview: Yahoo Finance minute coverage (1m interval)
# Data is stored as one parquet per ticker per month (monthly partitions). This reports, per trading date, how many unique minutes have at least one trade across any ticker and compares to the theoretical US regular session (09:30-16:00 ET = 390 minutes).
# If you see "No files found" adjust DATA_ROOT to your working directory (e.g., /opt/yf_parqed/data) and confirm files exist:
#   find "$DATA_ROOT/us/yahoo/stocks_1m" -name "*.parquet" | head
duckdb << 'EOF'
WITH minute_counts AS (
  SELECT 
    CAST("date" AS DATE) AS trade_date,
    COUNT(DISTINCT strftime('%Y-%m-%d %H:%M', "date")) AS minutes_with_trades
  FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet', hive_partitioning=1)
  GROUP BY trade_date
)
SELECT 
  trade_date,
  minutes_with_trades,
  390 AS theoretical_minutes_regular,  -- 09:30-16:00 ET
  ROUND(100.0 * minutes_with_trades / 390, 2) AS capture_rate_pct,
  CASE 
    WHEN minutes_with_trades >= 370 THEN '✓ Complete'  -- ~95%+
    WHEN minutes_with_trades >= 300 THEN '⚠ Partial'
    ELSE '✗ Incomplete'
  END AS status
FROM minute_counts
ORDER BY trade_date DESC
LIMIT 10;
EOF

# Query Xetra data (raw trades)
duckdb << 'EOF'
-- Query by partition day
SELECT 
    day,
    COUNT(*) as trades,
    ROUND(SUM(price * volume) / 1000000, 2) as volume_millions_eur
FROM read_parquet('/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet', hive_partitioning=1)
GROUP BY day
ORDER BY day DESC
LIMIT 10;

-- Query by trade_time date
SELECT 
    CAST(trade_time AS DATE) as trade_date,
    COUNT(*) as trades,
    ROUND(SUM(price * volume) / 1000000, 2) as volume_millions_eur
FROM read_parquet('/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet', hive_partitioning=1)
GROUP BY trade_date
ORDER BY trade_date DESC
LIMIT 10;
EOF
```

### Custom Monitoring Dashboard

Create a simple monitoring script:

```bash
sudo tee /usr/local/bin/yf-parqed-monitor > /dev/null << 'EOF'
#!/bin/bash
while true; do
    clear
    echo "=== YF Parqed Live Monitor ==="
    date
    echo
    yf-parqed-status
    echo
    echo "Last 5 log entries:"
    sudo journalctl -u yf-parqed -u 'xetra@*' -n 5 --no-pager
    sleep 30
done
EOF

sudo chmod +x /usr/local/bin/yf-parqed-monitor

# Run monitor
yf-parqed-monitor
```

## See Also

- **[README.md](README.md)** - Overview and quick start
- **[INSTALLATION.md](INSTALLATION.md)** - Initial setup instructions
- **[DAEMON_MODE.md](../DAEMON_MODE.md)** - Complete technical reference
- **GitHub**: https://github.com/SiggiSmara/yf_parqed
