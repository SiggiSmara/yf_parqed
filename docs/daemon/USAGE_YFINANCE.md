# Daemon Usage Guide — Yahoo Finance

Day-to-day operations, monitoring, and troubleshooting for the Yahoo Finance daemon.

## Prerequisites

Installed daemon and service file (`yf-parqed.service`) are in place. If not, see `INSTALLATION.md`.

## Managing Daemon

```bash
# Start daemon
sudo systemctl start yf-parqed

# Stop daemon
sudo systemctl stop yf-parqed

# Restart daemon
sudo systemctl restart yf-parqed

# Enable/disable auto-start
sudo systemctl enable yf-parqed
sudo systemctl disable yf-parqed

# Status + logs
sudo systemctl status yf-parqed
sudo journalctl -u yf-parqed -n 100
sudo journalctl -u yf-parqed -f
```

## Monitoring

```bash
# Quick status (installed during setup)
yf-parqed-status

# Recent parquet writes (partitioned storage)
find /var/lib/yf_parqed/data/us/yahoo/stocks_1m -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -5

# Count tickers updated today
find /var/lib/yf_parqed/data/us/yahoo/stocks_1m -name "*.parquet" -type f -mtime 0 | wc -l

# Storage usage
du -sh /var/lib/yf_parqed/data
```

## Updating

```bash
# Recommended update path
sudo /opt/yf_parqed/daemon-manage.sh update

# Manual update
sudo systemctl stop yf-parqed
cd /opt/yf_parqed
sudo -u yfparqed git fetch origin && sudo -u yfparqed git pull origin main
sudo -u yfparqed /var/lib/yf_parqed/.local/bin/uv sync
sudo systemctl start yf-parqed
sudo systemctl status yf-parqed
```

## Configuration

Edit `/etc/systemd/system/yf-parqed.service`, then reload and restart.

Common flags:
```ini
--interval 2                 # hours between update loops
--ticker-maintenance daily   # ticker maintenance frequency
--extended-hours             # include extended session
--trading-hours "08:00-18:00" # custom hours
--market-timezone "US/Pacific"
--log-level DEBUG
```

After edits:
```bash
sudo systemctl daemon-reload
sudo systemctl restart yf-parqed
sudo systemctl status yf-parqed
```

## Troubleshooting

```bash
# Service errors
sudo systemctl status yf-parqed
sudo journalctl -u yf-parqed -n 100 --no-pager

# Manual run for debugging
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed --log-level DEBUG update-data

# Check PID
cat /run/yf_parqed/yf-parqed.pid
ps -p $(cat /run/yf_parqed/yf-parqed.pid)
```

## Backup and Recovery

```bash
# Backup data and config
sudo tar -czf /backup/yf_parqed-data-$(date +%Y%m%d).tar.gz -C /var/lib/yf_parqed data
sudo tar -czf /backup/yf_parqed-config-$(date +%Y%m%d).tar.gz /var/lib/yf_parqed/*.json

# Restore
sudo systemctl stop yf-parqed
sudo tar -xzf /backup/yf_parqed-data-<DATE>.tar.gz -C /var/lib/yf_parqed
sudo tar -xzf /backup/yf_parqed-config-<DATE>.tar.gz -C /
sudo chown -R yfparqed:yfparqed /var/lib/yf_parqed
sudo systemctl start yf-parqed
```

## Performance Tuning

```bash
--interval 2            # reduce workload
--ticker-maintenance monthly
# Reduce ticker universe: edit /var/lib/yf_parqed/tickers.json
```

## Advanced Usage (DuckDB)

Install DuckDB if needed: `sudo apt install duckdb`.

```bash
# 1m coverage by day (minutes with trades vs 390-minute US session)
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
    WHEN minutes_with_trades >= 370 THEN '✓ Complete'
    WHEN minutes_with_trades >= 300 THEN '⚠ Partial'
    ELSE '✗ Incomplete'
  END AS status
FROM minute_counts
ORDER BY trade_date DESC
LIMIT 10;
EOF

# Ticker count and date range (1m interval)
duckdb << 'EOF'
SELECT 
    COUNT(DISTINCT ticker) as total_tickers,
    MIN("date") as first_date,
    MAX("date") as last_date,
    COUNT(*) as total_records
FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet', hive_partitioning=1);
EOF

# Top 10 tickers by notional volume (1m interval)
duckdb << 'EOF'
SELECT 
    ticker,
    ROUND(SUM("close" * volume) / 1000000, 2) as total_volume_millions
FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet', hive_partitioning=1)
GROUP BY ticker
ORDER BY total_volume_millions DESC
LIMIT 10;
EOF
```
