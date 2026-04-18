#!/usr/bin/env bash
# install.sh — installs the sailing-hpf systemd service on a Debian/Raspberry Pi system.
# Must be run as root (or with sudo).
set -euo pipefail

SERVICE_USER=sailing-hpf
INSTALL_DIR=/opt/sailing-hpf
DATA_DIR=/var/lib/sailing-hpf
SERVICE_FILE=/etc/systemd/system/sailing-hpf.service

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
rsync -a --exclude='.git' --exclude='hpf-data' --exclude='target' \
    "$(dirname "$0")/../" "$INSTALL_DIR/"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"

echo "==> Creating data directory $DATA_DIR…"
mkdir -p "$DATA_DIR"
chown -R "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR"

echo "==> Pre-building the project…"
sudo -u "$SERVICE_USER" \
    HOME="$DATA_DIR" \
    mvn --batch-mode -f "$INSTALL_DIR/pom.xml" \
        -Dmaven.repo.local="$DATA_DIR/.m2/repository" \
        compile -q

echo "==> Installing systemd service unit…"
install -m 644 "$(dirname "$0")/sailing-hpf.service" "$SERVICE_FILE"

echo "==> Reloading systemd and enabling service…"
systemctl daemon-reload
systemctl enable sailing-hpf.service

echo ""
echo "Installation complete."
echo ""
echo "  Start:   sudo systemctl start sailing-hpf"
echo "  Stop:    sudo systemctl stop sailing-hpf"
echo "  Status:  sudo systemctl status sailing-hpf"
echo "  Logs:    sudo journalctl -u sailing-hpf -f"
echo ""
echo "Data directory: $DATA_DIR"
echo "To pre-populate with existing data, copy your hpf-data/ contents there."
