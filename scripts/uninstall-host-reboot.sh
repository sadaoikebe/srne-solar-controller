#!/bin/bash
# Reverse of install-host-reboot.sh: remove the host-side systemd plumbing
# and the compose.override.yaml that bind-mounts the sentinel directory.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Must be run as root (sudo)." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Stopping and disabling srne-reboot.path"
systemctl disable --now srne-reboot.path 2>/dev/null || true

echo "Removing systemd unit files and trigger script"
rm -f /etc/systemd/system/srne-reboot.path
rm -f /etc/systemd/system/srne-reboot.service
rm -f /usr/local/sbin/srne-reboot.sh
systemctl daemon-reload

# Drop any pending request first so we don't trigger a reboot mid-uninstall.
# (last-reboot is also wiped — a fresh install starts with no cooldown history.)
echo "Removing sentinel directory /var/lib/srne-reboot"
rm -f /var/lib/srne-reboot/reboot-requested
rm -f /var/lib/srne-reboot/last-reboot
rm -rf /var/lib/srne-reboot

echo "Removing ${PROJECT_DIR}/compose.override.yaml"
rm -f "${PROJECT_DIR}/compose.override.yaml"

echo
echo "Uninstalled. Now run:  docker compose up -d"
