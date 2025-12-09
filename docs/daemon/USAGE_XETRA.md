# Daemon Usage Guide — Xetra (DETR example)

Day-to-day operations, monitoring, and troubleshooting for Xetra daemons using DETR as the example venue.

## Prerequisites

Systemd template `xetra@.service` installed. If not, see `INSTALLATION.md`.

## Managing Daemons

```bash
# Start/stop/restart DETR
sudo systemctl start xetra@DETR
sudo systemctl stop xetra@DETR
sudo systemctl restart xetra@DETR

# Enable/disable auto-start
sudo systemctl enable xetra@DETR
sudo systemctl disable xetra@DETR

# Status + logs
sudo systemctl status xetra@DETR
sudo journalctl -u xetra@DETR -n 100
sudo journalctl -u xetra@DETR -f

# Start/stop all venues (template)
sudo systemctl start 'xetra@*'
sudo systemctl stop 'xetra@*'
```

## Monitoring

```bash
# Recent parquet writes (DETR)
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -5

# Count collected days (DETR)
find /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR -name 'day=*' -type d | wc -l

# Storage usage
du -sh /var/lib/yf_parqed/data/de/xetra/trades/venue=DETR
```

## Updating

```bash
# Recommended
sudo /opt/yf_parqed/daemon-manage.sh update

# Manual
sudo systemctl stop 'xetra@*'
cd /opt/yf_parqed
sudo -u yfparqed git fetch origin && sudo -u yfparqed git pull origin main
sudo -u yfparqed /var/lib/yf_parqed/.local/bin/uv sync
sudo systemctl start 'xetra@*'
sudo systemctl status 'xetra@*'
```

## Configuration

Edit `/etc/systemd/system/xetra@.service`, then reload and restart.

Common flags:
```ini
--interval 2
--active-hours "07:00-19:00"
--log-level DEBUG
```

After edits:
```bash
sudo systemctl daemon-reload
sudo systemctl restart 'xetra@*'
sudo systemctl status 'xetra@*'
```

## Troubleshooting

```bash
# Service errors
sudo systemctl status xetra@DETR
sudo journalctl -u xetra@DETR -n 100 --no-pager

# Manual run for debugging (DETR)
sudo -u yfparqed /opt/yf_parqed/.venv/bin/xetra-parqed \
  --wrk-dir /var/lib/yf_parqed --log-level DEBUG fetch-trades DETR

# PID check
cat /run/yf_parqed/xetra-DETR.pid
ps -p $(cat /run/yf_parqed/xetra-DETR.pid)
```

## Backup and Recovery

```bash
# Backup data and config
sudo tar -czf /backup/yf_parqed-data-$(date +%Y%m%d).tar.gz -C /var/lib/yf_parqed data
sudo tar -czf /backup/yf_parqed-config-$(date +%Y%m%d).tar.gz /var/lib/yf_parqed/*.json

# Restore (includes Xetra data)
sudo systemctl stop 'xetra@*'
sudo tar -xzf /backup/yf_parqed-data-<DATE>.tar.gz -C /var/lib/yf_parqed
sudo tar -xzf /backup/yf_parqed-config-<DATE>.tar.gz -C /
sudo chown -R yfparqed:yfparqed /var/lib/yf_parqed
sudo systemctl start 'xetra@*'
```

## Performance Tuning

```bash
--interval 2
--active-hours "08:00-18:00"  # tighten to core hours
```

## Advanced Usage (DuckDB — DETR)

Install DuckDB if needed: `sudo apt install duckdb`.

```bash
# Minutes captured per day vs 600-minute session (08:00-18:00 CET)
duckdb << 'EOF'
WITH daily_stats AS (
    SELECT 
        CAST(trade_time AS DATE) as trade_date,
        COUNT(*) as trades_captured,
        COUNT(DISTINCT isin) as unique_isins,
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
    600 as theoretical_max_minutes,
    ROUND(100.0 * minutes_captured / 600, 2) as capture_rate_pct,
    volume_millions_eur,
    strftime('%H:%M', first_trade) as first_trade_time,
    strftime('%H:%M', last_trade) as last_trade_time,
    CASE 
        WHEN minutes_captured >= 540 THEN '✓ Complete'
        WHEN minutes_captured >= 450 THEN '⚠ Partial'
        ELSE '✗ Incomplete'
    END as status
FROM daily_stats
ORDER BY trade_date DESC
LIMIT 10;
EOF

# Trades by partition day (DETR)
duckdb << 'EOF'
SELECT 
    day,
    COUNT(*) as trades,
    ROUND(SUM(price * volume) / 1000000, 2) as volume_millions_eur
FROM read_parquet('/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet', hive_partitioning=1)
GROUP BY day
ORDER BY day DESC
LIMIT 10;
EOF

# Trades by trade_time date (DETR)
duckdb << 'EOF'
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
