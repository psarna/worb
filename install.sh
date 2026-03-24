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
    echo "Binary not found. Building..."
    (cd "$SCRIPT_DIR" && go build -o worb .)
fi

echo "Installing binary to $INSTALL_DIR/worb"
install -m 755 "$SCRIPT_DIR/worb" "$INSTALL_DIR/worb"

echo "Installing systemd unit to $SERVICE_FILE"
install -m 644 "$SCRIPT_DIR/worb.service" "$SERVICE_FILE"

systemctl daemon-reload
systemctl enable --now worb

echo "Done. Check status with: systemctl status worb"
