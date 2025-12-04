# Daemon Installation Guide

Complete step-by-step installation for yf_parqed daemons.

## Prerequisites

- Linux system (Ubuntu 20.04+, Debian 11+, or similar)
- Python 3.12 or higher
- systemd
- sudo access
- Internet connection

## Installation Steps

### 1. Create System User

Create a dedicated system user to run all daemons:

```bash
# Create user with home directory at /var/lib/yf_parqed
sudo useradd -r -m -d /var/lib/yf_parqed -s /bin/bash -c "YF Parqed Daemon User" yfparqed

# Verify user creation
id yfparqed
# Expected: uid=xxx(yfparqed) gid=xxx(yfparqed) groups=xxx(yfparqed)
```

**Why this user?**
- `-r` - System account (UID < 1000)
- `-m -d /var/lib/yf_parqed` - Create home/data directory
- `-s /bin/bash` - Allow shell access for maintenance
- Single user runs all daemons (YF, Xetra, etc.)

### 2. Create Directory Structure

```bash
# Application directory
sudo mkdir -p /opt/yf_parqed

# Log directory
sudo mkdir -p /var/log/yf_parqed

# Runtime directory (systemd will create this, but good to have)
sudo mkdir -p /run/yf_parqed

# Data subdirectories
sudo mkdir -p /var/lib/yf_parqed/data

# Set ownership
sudo chown -R yfparqed:yfparqed /opt/yf_parqed
sudo chown -R yfparqed:yfparqed /var/lib/yf_parqed
sudo chown -R yfparqed:yfparqed /var/log/yf_parqed
sudo chown -R yfparqed:yfparqed /run/yf_parqed

# Verify permissions
ls -la /opt/yf_parqed
ls -la /var/lib/yf_parqed
ls -la /var/log/yf_parqed
```

### 3. Install UV (Python Package Manager)

```bash
# Install uv as yfparqed user
sudo -u yfparqed bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh"

# Verify installation
sudo -u yfparqed /var/lib/yf_parqed/.local/bin/uv --version

# Add to PATH for convenience
echo 'export PATH="/var/lib/yf_parqed/.local/bin:$PATH"' | sudo tee -a /var/lib/yf_parqed/.bashrc
```

### 4. Clone and Install Application

```bash
# Clone repository
cd /opt/yf_parqed
sudo -u yfparqed git clone https://github.com/SiggiSmara/yf_parqed.git .

# Install Python dependencies
sudo -u yfparqed /var/lib/yf_parqed/.local/bin/uv sync

# Verify installation
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed --version
sudo -u yfparqed /opt/yf_parqed/.venv/bin/xetra-parqed --version
```

### 5. Initialize Data (Yahoo Finance)

```bash
# Initialize YF data structures
cd /var/lib/yf_parqed
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  initialize

# Verify initialization
ls -la /var/lib/yf_parqed
# Should see: tickers.json, intervals.json, storage_config.json
```

### 6. Test Manual Data Fetch

```bash
# Test Yahoo Finance
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed \
  --wrk-dir /var/lib/yf_parqed \
  --log-level DEBUG \
  update-data

# Check data was created
ls -la /var/lib/yf_parqed/data/us/yahoo/stocks_1d/

# Test Xetra (optional)
sudo -u yfparqed /opt/yf_parqed/.venv/bin/xetra-parqed \
  --wrk-dir /var/lib/yf_parqed \
  --log-level DEBUG \
  fetch-trades DETR --no-store

# Should see successful fetch messages
```

### 7. Install Systemd Services

#### Yahoo Finance Service

Create `/etc/systemd/system/yf-parqed.service`:

```bash
sudo tee /etc/systemd/system/yf-parqed.service > /dev/null << 'EOF'
[Unit]
Description=Yahoo Finance Data Collector
After=network-online.target
Wants=network-online.target
Documentation=https://github.com/SiggiSmara/yf_parqed

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
    --pid-file /run/yf_parqed/yf-parqed.pid

# Graceful shutdown
ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=60
KillMode=mixed

# Restart on failure
Restart=on-failure
RestartSec=30

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/yf_parqed /var/log/yf_parqed

# Create PID directory at startup
RuntimeDirectory=yf_parqed
RuntimeDirectoryMode=0755

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=yf-parqed

[Install]
WantedBy=multi-user.target
EOF
```

#### Xetra Service Template

Create `/etc/systemd/system/xetra@.service`:

```bash
sudo tee /etc/systemd/system/xetra@.service > /dev/null << 'EOF'
[Unit]
Description=Xetra %I Trade Data Collector
After=network-online.target
Wants=network-online.target
Documentation=https://github.com/SiggiSmara/yf_parqed

[Service]
Type=simple
User=yfparqed
Group=yfparqed
WorkingDirectory=/var/lib/yf_parqed

# Run daemon mode with venue from instance name (%i)
ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-file /var/log/yf_parqed/xetra-%i.log \
    --log-level INFO \
    fetch-trades %i \
    --daemon \
    --interval 1 \
    --pid-file /run/yf_parqed/xetra-%i.pid

# Graceful shutdown
ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=30
KillMode=mixed

# Restart on failure
Restart=on-failure
RestartSec=30

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/yf_parqed /var/log/yf_parqed

# Use shared PID directory
RuntimeDirectory=yf_parqed
RuntimeDirectoryMode=0755

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=xetra-%i

[Install]
WantedBy=multi-user.target
EOF
```

### 8. Enable and Start Services

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable YF daemon (start on boot)
sudo systemctl enable yf-parqed

# Start YF daemon
sudo systemctl start yf-parqed

# Check status
sudo systemctl status yf-parqed

# View logs
sudo journalctl -u yf-parqed -f

# Enable and start Xetra daemons (optional)
sudo systemctl enable xetra@DETR
sudo systemctl start xetra@DETR
sudo systemctl status xetra@DETR
```

### 9. Verify Installation

```bash
# Check services are running
sudo systemctl status yf-parqed
sudo systemctl status xetra@DETR  # if installed

# Check PID files exist
ls -la /run/yf_parqed/

# Check logs for errors
sudo journalctl -u yf-parqed -n 50
sudo journalctl -u xetra@DETR -n 50  # if installed

# Check data is being collected
find /var/lib/yf_parqed/data -name "*.parquet" -mtime -1 -ls

# Check disk usage
du -sh /var/lib/yf_parqed/data
```

### 10. Set Up Log Rotation

Create `/etc/logrotate.d/yf_parqed`:

```bash
sudo tee /etc/logrotate.d/yf_parqed > /dev/null << 'EOF'
/var/log/yf_parqed/*.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    missingok
    create 0640 yfparqed yfparqed
    sharedscripts
    postrotate
        systemctl reload-or-restart yf-parqed 'xetra@*' 2>/dev/null || true
    endscript
}
EOF

# Test logrotate configuration
sudo logrotate -d /etc/logrotate.d/yf_parqed
```

## Post-Installation

### Optional: Install DuckDB for Analytics

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install duckdb

# Or download latest binary
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/
sudo chmod +x /usr/local/bin/duckdb

# Verify
duckdb --version
```

### Optional: Set Up Monitoring

Create monitoring script at `/usr/local/bin/yf-parqed-status`:

```bash
sudo tee /usr/local/bin/yf-parqed-status > /dev/null << 'EOF'
#!/bin/bash

echo "=== YF Parqed Daemon Status ==="
echo

echo "Services:"
systemctl is-active yf-parqed xetra@* 2>/dev/null | paste -d ' ' <(echo "yf-parqed"; systemctl list-units 'xetra@*' --no-legend | awk '{print $1}') - || echo "No services found"
echo

echo "Data freshness (last 5 updates):"
find /var/lib/yf_parqed/data -name "*.parquet" -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -5 | awk '{print strftime("%Y-%m-%d %H:%M:%S", $1), $2}'
echo

echo "Storage usage:"
du -sh /var/lib/yf_parqed/data/* 2>/dev/null
echo

echo "Recent errors (last hour):"
journalctl -u yf-parqed -u 'xetra@*' --since "1 hour ago" 2>/dev/null | grep -i error | tail -10 || echo "No errors"
EOF

sudo chmod +x /usr/local/bin/yf-parqed-status

# Run status check
yf-parqed-status
```

### Optional: Set Up Alerting

Create alert script at `/usr/local/bin/yf-parqed-alert`:

```bash
sudo tee /usr/local/bin/yf-parqed-alert > /dev/null << 'EOF'
#!/bin/bash

ALERT_EMAIL="your-email@example.com"  # Change this

# Check if any service is failed
if systemctl is-failed yf-parqed xetra@* 2>/dev/null | grep -q failed; then
    echo "YF Parqed daemon failure detected" | mail -s "YF Parqed Alert" "$ALERT_EMAIL"
fi

# Check if no data in last 2 hours
if [ -z "$(find /var/lib/yf_parqed/data -name '*.parquet' -mmin -120 2>/dev/null)" ]; then
    echo "No data updates in last 2 hours" | mail -s "YF Parqed Data Alert" "$ALERT_EMAIL"
fi
EOF

sudo chmod +x /usr/local/bin/yf-parqed-alert

# Add to cron (every hour)
echo "0 * * * * /usr/local/bin/yf-parqed-alert" | sudo crontab -u yfparqed -
```

## Troubleshooting Installation

### Issue: uv command not found

```bash
# Verify uv installation
sudo -u yfparqed /var/lib/yf_parqed/.local/bin/uv --version

# If missing, reinstall
sudo -u yfparqed bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh"
```

### Issue: Permission denied errors

```bash
# Reset all ownership
sudo chown -R yfparqed:yfparqed /opt/yf_parqed
sudo chown -R yfparqed:yfparqed /var/lib/yf_parqed
sudo chown -R yfparqed:yfparqed /var/log/yf_parqed

# Check permissions
ls -la /opt/yf_parqed
ls -la /var/lib/yf_parqed
```

### Issue: Service won't start

```bash
# Check service file syntax
sudo systemd-analyze verify /etc/systemd/system/yf-parqed.service

# Check detailed status
sudo systemctl status yf-parqed -l

# View full logs
sudo journalctl -u yf-parqed -n 100 --no-pager

# Test manually
sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed --wrk-dir /var/lib/yf_parqed update-data --daemon --pid-file /tmp/test.pid
```

### Issue: Python version mismatch

```bash
# Check Python version
python3 --version

# If < 3.12, install newer Python
sudo apt update
sudo apt install python3.12 python3.12-venv

# Reinstall dependencies with correct Python
cd /opt/yf_parqed
sudo -u yfparqed /var/lib/yf_parqed/.local/bin/uv sync --python python3.12
```

## Next Steps

- [Setup Guide](./SETUP.md) - Managing and monitoring daemons
- [Usage Guide](./USAGE.md) - Daemon features and configuration
- [Main Documentation](../DAEMON_MODE.md) - Complete reference

## Uninstallation

To completely remove yf_parqed daemons:

```bash
# Stop and disable services
sudo systemctl stop yf-parqed 'xetra@*'
sudo systemctl disable yf-parqed 'xetra@*'

# Remove service files
sudo rm /etc/systemd/system/yf-parqed.service
sudo rm /etc/systemd/system/xetra@.service
sudo systemctl daemon-reload

# Remove user and data (WARNING: deletes all data!)
sudo userdel -r yfparqed

# Remove application
sudo rm -rf /opt/yf_parqed

# Remove logs
sudo rm -rf /var/log/yf_parqed

# Remove logrotate config
sudo rm /etc/logrotate.d/yf_parqed

# Remove monitoring scripts (if installed)
sudo rm /usr/local/bin/yf-parqed-status
sudo rm /usr/local/bin/yf-parqed-alert
```
