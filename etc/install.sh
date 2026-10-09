#!/usr/bin/env bash
# install.sh — installs the sailing-pf systemd service on a Debian/Raspberry Pi system.
# Must be run as root (or with sudo).
set -euo pipefail

SERVICE_USER=sailing-pf
INSTALL_DIR=/opt/sailing-pf
DATA_DIR=/var/lib/sailing-pf
SERVICE_NAME=sailing-pf.service
SERVICE_FILE=/etc/systemd/system/$SERVICE_NAME

# Set once we have stopped a running service, so the EXIT trap and the final step
# know it is ours to start again.
WAS_ACTIVE=false

# ---- Verify prerequisites ----
for cmd in java mvn; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: '$cmd' not found. Install it before running this script."
        exit 1
    fi
done

# Leave the machine as we found it if we bail out part-way through: an install that
# fails after the stop should not leave the service down. Idempotent, so the normal
# success path (where we have already started it) is a no-op.
restore_service() {
    if [ "$WAS_ACTIVE" = true ] && ! systemctl is-active --quiet "$SERVICE_NAME"; then
        echo "==> Install did not complete — restarting the service that was running…"
        systemctl start "$SERVICE_NAME" || true
    fi
}
trap restore_service EXIT

# ---- Stop a running service before replacing the tree it runs from ----
# rsync --delete rewrites $INSTALL_DIR underneath the running JVM, so an upgrade over
# a live install stops first and starts again at the end. A service that was not
# running (or not yet installed) is left stopped.
if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
    WAS_ACTIVE=true
    echo "==> Service is running — stopping it for the upgrade…"
    systemctl stop "$SERVICE_NAME"
fi

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

# The sign-in example only — auth.yaml itself (which holds the client secret) is never
# written here, so an upgrade cannot replace the deployed OAuth client.
mkdir -p "$DATA_DIR/config"
install -m 644 -o "$SERVICE_USER" -g "$SERVICE_USER" \
    "$(dirname "$0")/../pf-data/config/auth.yaml.example" "$DATA_DIR/config/auth.yaml.example"

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
systemctl enable "$SERVICE_NAME"

if [ "$WAS_ACTIVE" = true ]; then
    echo "==> Restarting the service…"
    systemctl start "$SERVICE_NAME"
    systemctl --no-pager --lines=0 status "$SERVICE_NAME" || true
fi

echo ""
echo "Installation complete."
if [ "$WAS_ACTIVE" = true ]; then
    echo "The service was running and has been restarted on the new build."
else
    echo "The service is installed but not running — start it with the command below."
fi
echo ""
echo "  Start:   sudo systemctl start sailing-pf"
echo "  Stop:    sudo systemctl stop sailing-pf"
echo "  Status:  sudo systemctl status sailing-pf"
echo "  Logs:    sudo journalctl -u sailing-pf -f"
echo ""
echo "Data directory: $DATA_DIR"
echo "To pre-populate with existing data, copy your pf-data/ contents there."
