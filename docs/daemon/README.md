# YF Parqed Daemon Mode

Complete guide for running yf_parqed as a system daemon for continuous data collection.

## Overview

**Single User, Multiple Daemons**: Use one system user (`yfparqed`) to run all daemons (Yahoo Finance, Xetra, etc.). Each daemon operates independently with its own PID file and log file.

## Documentation Structure

- **[Installation Guide](INSTALLATION.md)** - Detailed step-by-step manual installation
- **[Usage Guide](USAGE.md)** - Day-to-day operations, monitoring, and troubleshooting
- **[Main Daemon Reference](../DAEMON_MODE.md)** - Complete technical documentation

## Architecture

```
/opt/yf_parqed/          # Application code (owned by yfparqed)
├── .venv/               # Python virtual environment
├── src/                 # Source code
└── setup-daemon.sh      # Setup/update script

/var/lib/yf_parqed/      # Persistent data (owned by yfparqed)
├── data/                # Partitioned parquet files
│   ├── us/yahoo/stocks_*/     # Yahoo Finance data
│   └── de/xetra/trades/       # Xetra data
├── tickers.json         # YF ticker state
├── intervals.json       # YF intervals config
└── storage_config.json  # Storage backend config

/var/log/yf_parqed/      # Application logs (owned by yfparqed)
├── yf-parqed.log        # Yahoo Finance daemon logs
└── xetra-*.log          # Xetra daemon logs (per venue)

/run/yf_parqed/          # Runtime state (created by systemd)
├── yf-parqed.pid        # Yahoo Finance daemon PID
└── xetra-*.pid          # Xetra daemon PIDs (per venue)
```

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
# Download and run setup script
curl -O https://raw.githubusercontent.com/SiggiSmara/yf_parqed/main/setup-daemon.sh
chmod +x setup-daemon.sh
sudo ./setup-daemon.sh install

# Start Yahoo Finance daemon
sudo systemctl enable --now yf-parqed

# Check status
yf-parqed-status
```

### Option 2: Manual Installation

For custom installations or to understand the process, see **[INSTALLATION.md](INSTALLATION.md)**.

## What Gets Installed

- **System user**: `yfparqed` (runs all daemons)
- **Application**: `/opt/yf_parqed` (Python code, virtual environment)
- **Data**: `/var/lib/yf_parqed/data` (partitioned parquet files)
- **Logs**: `/var/log/yf_parqed/*.log` (with rotation)
- **Services**: `yf-parqed.service`, `xetra@.service` (systemd)
- **Monitoring**: `yf-parqed-status` command
- **Setup script**: `/opt/yf_parqed/setup-daemon.sh` (for updates)

## Common Tasks

For detailed instructions, see **[USAGE.md](USAGE.md)**.

### Start/Stop Daemons

```bash
# Yahoo Finance
sudo systemctl start yf-parqed
sudo systemctl stop yf-parqed

# Xetra (specific venue)
sudo systemctl start xetra@DETR
sudo systemctl stop xetra@DETR
```

### Check Status

```bash
# Quick overview
yf-parqed-status

# Detailed service status
sudo systemctl status yf-parqed
sudo systemctl status xetra@DETR

# View logs
sudo journalctl -u yf-parqed -f
```

### Update to Latest Version

```bash
sudo /opt/yf_parqed/setup-daemon.sh update
sudo systemctl restart yf-parqed 'xetra@*'
```

## Next Steps

1. **Installation**: Follow **[INSTALLATION.md](INSTALLATION.md)** for step-by-step setup
2. **Usage**: Read **[USAGE.md](USAGE.md)** for daily operations and troubleshooting
3. **Configuration**: See **[DAEMON_MODE.md](../DAEMON_MODE.md)** for advanced options

## Support

- **GitHub Issues**: https://github.com/SiggiSmara/yf_parqed/issues
- **Quick diagnostics**: `yf-parqed-status`
- **Debug mode**: `sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed --log-level DEBUG update-data`

---

## Quick Reference (Advanced)

### Daemon Won't Start

```bash
# Check service status for errors
sudo systemctl status yf-parqed

# View full logs
sudo journalctl -u yf-parqed -n 100

# Check if another instance is running
ps aux | grep yf-parqed

# Check PID file
cat /run/yf_parqed/yf-parqed.pid

# Remove stale PID file
sudo rm /run/yf_parqed/yf-parqed.pid
sudo systemctl start yf-parqed
```

### Permission Issues

```bash
# Verify ownership
ls -la /opt/yf_parqed
ls -la /var/lib/yf_parqed
ls -la /var/log/yf_parqed

# Fix ownership if needed
sudo chown -R yfparqed:yfparqed /opt/yf_parqed
sudo chown -R yfparqed:yfparqed /var/lib/yf_parqed
sudo chown -R yfparqed:yfparqed /var/log/yf_parqed
```

### Data Not Updating

```bash
# Check if daemon is running during trading hours
sudo journalctl -u yf-parqed -n 50 | grep -i "trading hours"

# Test manual fetch
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed --wrk-dir /var/lib/yf_parqed update-data

# Check ticker status
sudo -u yfparqed cat /var/lib/yf_parqed/tickers.json | jq -r '.[] | select(.status == "not_found")' | head -10
```

### High Resource Usage

```bash
# Check memory usage
ps aux | grep -E 'yf-parqed|xetra-parqed' | awk '{print $2, $3, $4, $11}'

# Check disk usage
du -sh /var/lib/yf_parqed/data/*

# Reduce ticker count (if needed)
sudo -u yfparqed nano /var/lib/yf_parqed/tickers.json
```

## Configuration

### Yahoo Finance Daemon Settings

Edit `/etc/systemd/system/yf-parqed.service`:

```ini
# Change update interval (default: 1 hour)
--interval 2

# Change ticker maintenance frequency (default: weekly)
--ticker-maintenance daily

# Enable extended trading hours (04:00-20:00 ET)
--extended-hours

# Custom trading hours
--trading-hours "08:00-18:00"
```

After changes:
```bash
sudo systemctl daemon-reload
sudo systemctl restart yf-parqed
```

### Xetra Daemon Settings

Edit `/etc/systemd/system/xetra@.service`:

```ini
# Change update interval (default: 1 hour)
--interval 2

# Custom active hours
--active-hours "07:00-19:00"

# Run 24/7 (not recommended)
--active-hours "00:00-23:59"
```

After changes:
```bash
sudo systemctl daemon-reload
sudo systemctl restart 'xetra@*'
```

## Security

### Service Hardening

Both daemon services include systemd security hardening:
- `ProtectSystem=strict` - Read-only system directories
- `ProtectHome=true` - No access to user home directories
- `PrivateTmp=true` - Isolated /tmp
- `NoNewPrivileges=true` - Cannot escalate privileges

### File Permissions

```bash
# Verify secure permissions
sudo find /opt/yf_parqed -type f -perm /o+w  # Should be empty
sudo find /var/lib/yf_parqed -type f -perm /o+w  # Should be empty
```

### Network Access

```bash
# Check outbound connections (optional firewall rules)
sudo netstat -tuln | grep -E 'yf-parqed|xetra-parqed'

# Restrict to data sources only (ufw example)
sudo ufw allow out to query1.finance.yahoo.com port 443
sudo ufw allow out to api.deutsche-boerse.com port 443
```

## Backup

### Data Backup

```bash
# Backup data directory
sudo tar -czf yf_parqed-data-$(date +%Y%m%d).tar.gz -C /var/lib yf_parqed/data

# Backup configuration
sudo tar -czf yf_parqed-config-$(date +%Y%m%d).tar.gz \
  /var/lib/yf_parqed/tickers.json \
  /var/lib/yf_parqed/intervals.json \
  /var/lib/yf_parqed/storage_config.json
```

### Automated Backup (cron)

```bash
# Add to /etc/cron.daily/yf_parqed-backup
#!/bin/bash
BACKUP_DIR="/backup/yf_parqed"
DATE=$(date +%Y%m%d)
mkdir -p "$BACKUP_DIR"

# Backup data (exclude legacy stocks_* directories)
tar -czf "$BACKUP_DIR/data-$DATE.tar.gz" \
  -C /var/lib/yf_parqed data

# Backup config
tar -czf "$BACKUP_DIR/config-$DATE.tar.gz" \
  /var/lib/yf_parqed/*.json

# Keep last 30 days
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +30 -delete
```

## Advanced

### Custom Data Directory

```bash
# Use different data directory
sudo mkdir -p /data/yf_parqed
sudo chown yfparqed:yfparqed /data/yf_parqed

# Update systemd service
sudo nano /etc/systemd/system/yf-parqed.service
# Change --wrk-dir to /data/yf_parqed

sudo systemctl daemon-reload
sudo systemctl restart yf-parqed
```

### Multiple YF Daemon Instances

```bash
# Create template service
sudo cp /etc/systemd/system/yf-parqed.service /etc/systemd/system/yf-parqed@.service

# Edit to use instance name in paths
--wrk-dir /var/lib/yf_parqed/%i
--pid-file /run/yf_parqed/%i.pid

# Start instances
sudo systemctl start yf-parqed@1m yf-parqed@1h yf-parqed@1d
```

## References

- [Installation Details](./INSTALLATION.md) - Step-by-step manual installation
- [Usage Guide](./USAGE.md) - Daemon features and CLI options
- [Systemd Services](./systemd/) - Service file templates
- [Main Documentation](../DAEMON_MODE.md) - Complete daemon mode reference
