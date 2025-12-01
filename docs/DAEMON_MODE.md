# Xetra Daemon Mode Guide

## Overview

The `xetra-parqed fetch-trades` command supports daemon mode for continuous data collection. This is useful for:

- Running as a background service
- Automated daily data collection
- Production deployments
- Scheduled data updates

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
# Use PID file to prevent duplicates
xetra-parqed --log-file logs/xetra.log fetch-trades DETR \
  --daemon --pid-file /var/run/xetra-detr.pid
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
WorkingDirectory=/var/lib/xetra
Environment="PATH=/opt/yf_parqed/.venv/bin:/usr/local/bin:/usr/bin:/bin"

# Run daemon mode with logging
ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/xetra \
    --log-file /var/log/xetra/detr.log \
    fetch-trades DETR \
    --daemon \
    --interval 1 \
    --pid-file /var/run/xetra-detr.pid

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
ReadWritePaths=/var/lib/xetra /var/log/xetra /var/run

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
find /var/lib/xetra/data/de/xetra/trades/venue=DETR -name "*.parquet" -type f -printf '%T@ %p\n' | sort -rn | head -1
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
/opt/yf_parqed/          # Application code (managed by git/package manager)
├── .venv/               # Python virtual environment
├── src/                 # Source code
└── pyproject.toml       # Project configuration

/var/lib/xetra/          # Persistent data (survives upgrades)
├── data/                # Parquet files
│   └── de/xetra/trades/
├── tickers.json         # State files (if applicable)
└── intervals.json

/var/log/xetra/          # Application logs
└── *.log                # Log files with rotation

/var/run/                # Runtime state
└── xetra-*.pid          # PID files
```

### Installation Steps

```bash
# 1. Create dedicated user (system account, no login)
sudo useradd -r -s /bin/false -d /var/lib/xetra xetra

# 2. Create directory structure
sudo mkdir -p /opt/yf_parqed /var/lib/xetra /var/log/xetra /var/run
sudo chown -R xetra:xetra /var/lib/xetra /var/log/xetra

# 3. Install application code
sudo mkdir -p /opt/yf_parqed
sudo chown xetra:xetra /opt/yf_parqed
sudo -u xetra git clone https://github.com/SiggiSmara/yf_parqed.git /opt/yf_parqed
cd /opt/yf_parqed

# 4. Install Python dependencies
sudo -u xetra uv sync

# 5. Verify installation
sudo -u xetra /opt/yf_parqed/.venv/bin/xetra-parqed --help

# 6. Test data collection (stores in current directory by default)
cd /var/lib/xetra
sudo -u xetra /opt/yf_parqed/.venv/bin/xetra-parqed --wrk-dir /var/lib/xetra fetch-trades DETR --no-store
```

### Why This Structure?

- **`/opt/yf_parqed`** - Optional software packages (FHS standard for add-on applications)
  - Can be updated/reinstalled without affecting data
  - Managed by version control (git)
  
- **`/var/lib/xetra`** - Variable application state/data (FHS standard)
  - Persists across application upgrades
  - Backed up separately from application code
  - Used as `--wrk-dir` for data storage
  
- **`/var/log/xetra`** - Application logs (FHS standard)
  - Managed by logrotate
  - Can be monitored by log aggregation tools
  
- **`/var/run`** - Runtime variable data (FHS standard)
  - PID files for process management
  - Cleaned on reboot

## Example: Production Setup

Complete setup for DETR venue:

```bash
# 1. System-wide installation (see above)
# Follow all steps in "System-Wide Installation" section

# 2. Install application
cd /opt/yf_parqed
sudo -u xetra git clone https://github.com/SiggiSmara/yf_parqed.git .
sudo -u xetra uv sync

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
