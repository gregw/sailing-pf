#!/usr/bin/env bash
# install.sh — installs the sailing-pf systemd service on a Debian/Raspberry Pi system.
# Must be run as root (or with sudo).
set -euo pipefail

SERVICE_USER=sailing-pf
INSTALL_DIR=/opt/sailing-pf
DATA_DIR=/var/lib/sailing-pf
SERVICE_FILE=/etc/systemd/system/sailing-pf.service

# ---- Verify prerequisites ----
for cmd in java mvn; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: '$cmd' not found. Install it before running this script."
        exit 1
    fi
done

echo "==> Creating system user '$SERVICE_USER' (if not already present)…"
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi

echo "==> Installing project to $INSTALL_DIR…"
mkdir -p "$INSTALL_DIR"
# Copy the project source so Maven can be run from there
rsync -a --delete --exclude='.git' --exclude='pf-data' --exclude='target' \
    "$(dirname "$0")/../" "$INSTALL_DIR/"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"

echo "==> Creating data directory $DATA_DIR…"
mkdir -p "$DATA_DIR"
chown -R "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR"

echo "==> Pre-building the project…"
sudo -u "$SERVICE_USER" \
    HOME="$DATA_DIR" \
    sh -c "cd '$INSTALL_DIR' && mvn --batch-mode \
        -Dmaven.repo.local='$DATA_DIR/.m2/repository' \
        compile -q"

echo "==> Installing systemd service unit…"
install -m 644 "$(dirname "$0")/sailing-pf.service" "$SERVICE_FILE"

echo "==> Reloading systemd and enabling service…"
systemctl daemon-reload
systemctl enable sailing-pf.service

echo ""
echo "Installation complete."
echo ""
echo "  Start:   sudo systemctl start sailing-pf"
echo "  Stop:    sudo systemctl stop sailing-pf"
echo "  Status:  sudo systemctl status sailing-pf"
echo "  Logs:    sudo journalctl -u sailing-pf -f"
echo ""
echo "Data directory: $DATA_DIR"
echo "To pre-populate with existing data, copy your pf-data/ contents there."
