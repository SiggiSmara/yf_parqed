#!/bin/bash
# YF Parqed Daemon Management Script
# Automates installation, updates, and removal for yf_parqed daemons

set -e  # Exit on error

SCRIPT_VERSION="1.0.0"
INSTALL_DIR="/opt/yf_parqed"
DATA_DIR="/var/lib/yf_parqed"
LOG_DIR="/var/log/yf_parqed"
RUN_DIR="/run/yf_parqed"
DAEMON_USER="yfparqed"
REPO_URL="https://github.com/SiggiSmara/yf_parqed.git"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

check_dependencies() {
    local missing_deps=()
    
    for cmd in git python3 systemctl; do
        if ! command -v $cmd &> /dev/null; then
            missing_deps+=($cmd)
        fi
    done
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        log_error "Missing required dependencies: ${missing_deps[*]}"
        log_info "Install them with: apt install ${missing_deps[*]} (Debian/Ubuntu)"
        exit 1
    fi
    
    # Check Python version
    py_version=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    if [ "$(printf '%s\n' "3.12" "$py_version" | sort -V | head -n1)" != "3.12" ]; then
        log_error "Python 3.12+ required, found $py_version"
        exit 1
    fi
}

create_user() {
    if id "$DAEMON_USER" &>/dev/null; then
        log_info "User $DAEMON_USER already exists"
    else
        log_info "Creating system user $DAEMON_USER..."
        useradd -r -m -d "$DATA_DIR" -s /bin/bash -c "YF Parqed Daemon User" "$DAEMON_USER"
        log_info "Created user $DAEMON_USER"
    fi
    
    # Add current user to yfparqed group for data access
    if [ -n "$SUDO_USER" ] && [ "$SUDO_USER" != "root" ]; then
        log_info "Adding $SUDO_USER to $DAEMON_USER group..."
        usermod -aG "$DAEMON_USER" "$SUDO_USER"
        log_info "User $SUDO_USER added to $DAEMON_USER group (logout/login required)"
    fi
}

create_directories() {
    log_info "Creating directory structure..."
    
    mkdir -p "$INSTALL_DIR"
    mkdir -p "$DATA_DIR/data"
    mkdir -p "$LOG_DIR"
    mkdir -p "$RUN_DIR"
    
    chown -R ${DAEMON_USER}:${DAEMON_USER} "$INSTALL_DIR"
    chown -R ${DAEMON_USER}:${DAEMON_USER} "$DATA_DIR"
    chown -R ${DAEMON_USER}:${DAEMON_USER} "$LOG_DIR"
    chown -R ${DAEMON_USER}:${DAEMON_USER} "$RUN_DIR"
    
    log_info "Directories created and ownership set"
}

install_uv() {
    if sudo -u "$DAEMON_USER" command -v uv &>/dev/null; then
        log_info "UV already installed"
        return
    fi
    
    log_info "Installing UV package manager..."
    sudo -u "$DAEMON_USER" bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh"
    
    # Add to PATH
    echo 'export PATH="$HOME/.local/bin:$PATH"' | sudo -u "$DAEMON_USER" tee -a "$DATA_DIR/.bashrc" >/dev/null
    
    log_info "UV installed successfully"
}

clone_or_update_repo() {
    if [ -d "$INSTALL_DIR/.git" ]; then
        log_info "Updating existing repository..."
        cd "$INSTALL_DIR"
        sudo -u "$DAEMON_USER" git fetch origin
        sudo -u "$DAEMON_USER" git pull origin main
    else
        log_info "Cloning repository..."
        sudo -u "$DAEMON_USER" git clone "$REPO_URL" "$INSTALL_DIR"
    fi
    
    log_info "Repository updated"
}

install_dependencies() {
    log_info "Installing Python dependencies..."
    cd "$INSTALL_DIR"
    
    # Get uv path
    UV_PATH="$DATA_DIR/.local/bin/uv"
    
    sudo -u "$DAEMON_USER" "$UV_PATH" sync
    
    log_info "Dependencies installed"
}

initialize_data() {
    if [ -f "$DATA_DIR/tickers.json" ]; then
        log_info "Data already initialized, skipping..."
        return
    fi
    
    log_info "Initializing Yahoo Finance data..."
    sudo -u "$DAEMON_USER" "$INSTALL_DIR/.venv/bin/yf-parqed" \
        --wrk-dir "$DATA_DIR" \
        initialize
    
    log_info "Data initialized"
}

install_systemd_services() {
    log_info "Installing systemd service files..."
    
    # YF Parqed service
    cat > /etc/systemd/system/yf-parqed.service << 'EOF'
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

ExecStart=/opt/yf_parqed/.venv/bin/yf-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-level INFO \
    update-data \
    --daemon \
    --interval 1 \
    --ticker-maintenance weekly \
    --pid-file /run/yf_parqed/yf-parqed.pid

ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=60
KillMode=mixed

Restart=on-failure
RestartSec=30

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/yf_parqed /var/log/yf_parqed

RuntimeDirectory=yf_parqed
RuntimeDirectoryMode=0755

StandardOutput=journal
StandardError=journal
SyslogIdentifier=yf-parqed

[Install]
WantedBy=multi-user.target
EOF

    # Xetra template service
    cat > /etc/systemd/system/xetra@.service << 'EOF'
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

ExecStart=/opt/yf_parqed/.venv/bin/xetra-parqed \
    --wrk-dir /var/lib/yf_parqed \
    --log-file /var/log/yf_parqed/xetra-%i.log \
    --log-level INFO \
    fetch-trades %i \
    --daemon \
    --interval 1 \
    --pid-file /run/yf_parqed/xetra-%i.pid

ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=30
KillMode=mixed

Restart=on-failure
RestartSec=30

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/yf_parqed /var/log/yf_parqed

RuntimeDirectory=yf_parqed
RuntimeDirectoryMode=0755

StandardOutput=journal
StandardError=journal
SyslogIdentifier=xetra-%i

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    log_info "Systemd services installed"
}

install_logrotate() {
    log_info "Installing logrotate configuration..."
    
    cat > /etc/logrotate.d/yf_parqed << 'EOF'
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

    log_info "Logrotate configured"
}

install_monitoring_script() {
    log_info "Installing monitoring script..."
    
    cat > /usr/local/bin/yf-parqed-status << 'EOF'
#!/bin/bash
echo "=== YF Parqed Daemon Status ==="
echo
echo "Services:"
systemctl is-active yf-parqed xetra@* 2>/dev/null | paste -d ' ' <(echo "yf-parqed"; systemctl list-units 'xetra@*' --no-legend | awk '{print $1}') - || echo "No services running"
echo
echo "Data freshness (last 5 updates):"
find /var/lib/yf_parqed/data -name "*.parquet" -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -5 | awk '{print strftime("%Y-%m-%d %H:%M:%S", $1), $2}' || echo "No data found"
echo
echo "Storage usage:"
du -sh /var/lib/yf_parqed/data/* 2>/dev/null || echo "No data yet"
echo
echo "Recent errors (last hour):"
journalctl -u yf-parqed -u 'xetra@*' --since "1 hour ago" 2>/dev/null | grep -i error | tail -10 || echo "No errors"
EOF

    chmod +x /usr/local/bin/yf-parqed-status
    log_info "Monitoring script installed at /usr/local/bin/yf-parqed-status"
}

do_install() {
    log_info "Starting YF Parqed daemon installation..."
    
    check_root
    check_dependencies
    create_user
    create_directories
    install_uv
    clone_or_update_repo
    install_dependencies
    initialize_data
    install_systemd_services
    install_logrotate
    install_monitoring_script
    
    log_info "Installation complete!"
    echo
    if [ -n "$SUDO_USER" ] && [ "$SUDO_USER" != "root" ]; then
        log_warn "IMPORTANT: User $SUDO_USER was added to yfparqed group."
        log_warn "You must logout and login again for group membership to take effect."
        log_warn "After logging back in, you can access data files in /var/lib/yf_parqed/data"
        echo
    fi
    log_info "Next steps:"
    echo "  1. Enable and start Yahoo Finance daemon:"
    echo "     sudo systemctl enable --now yf-parqed"
    echo
    echo "  2. Check status:"
    echo "     sudo systemctl status yf-parqed"
    echo "     yf-parqed-status"
    echo
    echo "  3. View logs:"
    echo "     sudo journalctl -u yf-parqed -f"
    echo
    echo "  4. Optional: Enable Xetra daemon(s):"
    echo "     sudo systemctl enable --now xetra@DETR"
    echo "     sudo systemctl enable --now xetra@DFRA"
    echo
}

do_update() {
    log_info "Updating YF Parqed daemons..."
    
    check_root
    
    # Stop services
    log_info "Stopping daemons..."
    systemctl stop yf-parqed 'xetra@*' 2>/dev/null || true
    
    # Update code
    clone_or_update_repo
    install_dependencies
    
    # Restart services
    log_info "Restarting daemons..."
    systemctl start yf-parqed 2>/dev/null || log_warn "yf-parqed not enabled"
    systemctl start 'xetra@*' 2>/dev/null || log_warn "No xetra services enabled"
    
    log_info "Update complete!"
    echo
    log_info "Check status with: yf-parqed-status"
}

do_uninstall() {
    log_warn "This will remove all daemons, data, and configurations!"
    read -p "Are you sure? (type 'yes' to confirm): " confirm
    
    if [ "$confirm" != "yes" ]; then
        log_info "Uninstall cancelled"
        exit 0
    fi
    
    check_root
    
    log_info "Uninstalling YF Parqed daemons..."
    
    # Stop and disable services
    systemctl stop yf-parqed 'xetra@*' 2>/dev/null || true
    systemctl disable yf-parqed 'xetra@*' 2>/dev/null || true
    
    # Remove service files
    rm -f /etc/systemd/system/yf-parqed.service
    rm -f /etc/systemd/system/xetra@.service
    systemctl daemon-reload
    
    # Remove user and data
    userdel -r "$DAEMON_USER" 2>/dev/null || true
    
    # Remove application
    rm -rf "$INSTALL_DIR"
    
    # Remove logs
    rm -rf "$LOG_DIR"
    
    # Remove logrotate
    rm -f /etc/logrotate.d/yf_parqed
    
    # Remove monitoring scripts
    rm -f /usr/local/bin/yf-parqed-status
    
    log_info "Uninstall complete!"
}

show_usage() {
    cat << EOF
YF Parqed Daemon Setup Script v${SCRIPT_VERSION}

Usage: $0 [COMMAND]

Commands:
    install     - Install daemons and dependencies
    update      - Update to latest version
    uninstall   - Remove all daemons and data
    status      - Show daemon status
    help        - Show this help message

Examples:
    sudo $0 install
    sudo $0 update
    yf-parqed-status

EOF
}

# Main command dispatcher
case "${1:-help}" in
    install)
        do_install
        ;;
    update)
        do_update
        ;;
    uninstall)
        do_uninstall
        ;;
    status)
        if command -v yf-parqed-status &>/dev/null; then
            yf-parqed-status
        else
            log_error "Monitoring script not installed. Run: sudo $0 install"
            exit 1
        fi
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        log_error "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac
