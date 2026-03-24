#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
SERVICE_FILE="/etc/systemd/system/worb.service"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ "$(id -u)" -ne 0 ]; then
    echo "Run as root or with sudo"
    exit 1
fi

if [ ! -f "$SCRIPT_DIR/worb" ]; then
    echo "Binary not found. Run 'make' first."
    exit 1
fi

if ! id -u worb &>/dev/null; then
    echo "Creating worb user..."
    useradd --system --no-create-home --shell /usr/sbin/nologin worb
fi

mkdir -p /var/lib/worb
chown worb:worb /var/lib/worb

echo "Installing binary to $INSTALL_DIR/worb"
install -m 755 "$SCRIPT_DIR/worb" "$INSTALL_DIR/worb"

echo "Installing systemd unit to $SERVICE_FILE"
install -m 644 "$SCRIPT_DIR/worb.service" "$SERVICE_FILE"

systemctl daemon-reload
systemctl enable --now worb

echo "Done. Check status with: systemctl status worb"
