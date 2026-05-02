#!/usr/bin/env bash
# migrate_temps.sh
#
# Automated runner for v1_to_v2_growatt_temps.py that:
#
#   1. Makes a writable COPY of the v1 backup (the original backup is never
#      touched by this script).
#   2. Starts a temporary InfluxDB 1.8 container against that copy.
#   3. Runs the python migrator (2024-11-01 .. 2026-04-30).
#   4. Stops and removes the container.
#   5. Prints the path of the temp copy and asks the user to delete it
#      manually after they have verified the v2 data.
#
# This script does NOT delete:
#   - the original backup (/var/lib/influxdb_backup_1_8)
#   - the temporary copy (/var/lib/influxdb_temp_copy_<timestamp>)
#   - any v2 data
#
# Re-running is safe: each run creates a new temp copy and a uniquely-named
# container, and the python migrator has its own .migrate_growatt_temps_progress.json
# so it picks up where it left off.
#
# Env vars consumed (defaults shown):
#   SOURCE_DIR   = /var/lib/influxdb_backup_1_8
#   TEMP_BASE    = /var/lib/influxdb_temp_copy
#   V1_HOST_PORT = 8087
#   V1_IMAGE     = influxdb:1.8
#   PY_BIN       = python3
#
# v2 connection details are read from ./.env by the python script (load_dotenv).

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────
SOURCE_DIR="${SOURCE_DIR:-/var/lib/influxdb_backup_1_8}"
TEMP_BASE="${TEMP_BASE:-/var/lib/influxdb_temp_copy}"
V1_HOST_PORT="${V1_HOST_PORT:-8087}"
V1_IMAGE="${V1_IMAGE:-influxdb:1.8}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
TEMP_DIR="${TEMP_BASE}_${TS}"
CONTAINER_NAME="influxdb1_temp_${TS}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MIGRATOR="${SCRIPT_DIR}/v1_to_v2_growatt_temps.py"

# Prefer ${SCRIPT_DIR}/venv/bin/python if it exists. Under `sudo`, system
# python3 is used by default and won't see venv-installed packages.
if [[ -z "${PY_BIN:-}" ]]; then
  if [[ -x "${SCRIPT_DIR}/venv/bin/python" ]]; then
    PY_BIN="${SCRIPT_DIR}/venv/bin/python"
  else
    PY_BIN="python3"
  fi
fi

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# ── Sanity checks ──────────────────────────────────────────────────────────
[[ -d "$SOURCE_DIR" ]] || { log "ERROR: source backup not found: $SOURCE_DIR"; exit 1; }
[[ -f "$MIGRATOR" ]]   || { log "ERROR: migrator script not found: $MIGRATOR"; exit 1; }
command -v docker >/dev/null || { log "ERROR: docker not on PATH"; exit 1; }

# Host port in use? Bail rather than collide with another instance.
if ss -tln 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${V1_HOST_PORT}\$"; then
  log "ERROR: host port ${V1_HOST_PORT} is already in use"; exit 1
fi

log "Source backup : $SOURCE_DIR  (read-only — will not be modified)"
log "Temp copy     : $TEMP_DIR    (NOT auto-deleted)"
log "v1 container  : $CONTAINER_NAME  (image: $V1_IMAGE, port: ${V1_HOST_PORT})"
log "Migrator      : $MIGRATOR"
log "Python        : $PY_BIN"

# ── 1. Copy the backup (preserves perms, never touches source) ─────────────
log "Copying backup -> temp copy ..."
mkdir -p "$(dirname "$TEMP_DIR")"
cp -a "$SOURCE_DIR" "$TEMP_DIR"
log "Copy complete: $(du -sh "$TEMP_DIR" | awk '{print $1}')"

# ── 2. Start temporary v1.8 container ──────────────────────────────────────
cleanup_started=0
cleanup() {
  if [[ "$cleanup_started" -eq 1 ]]; then return; fi
  cleanup_started=1
  log "Stopping v1 container ..."
  docker stop  "$CONTAINER_NAME" >/dev/null 2>&1 || true
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

log "Starting $V1_IMAGE ..."
# --user 0:0  -> sidestep ownership mismatches in the copied data dir
# no --rm     -> keep the container around on failure so logs survive
docker run -d \
  --name "$CONTAINER_NAME" \
  --user 0:0 \
  -p "127.0.0.1:${V1_HOST_PORT}:8086" \
  -v "${TEMP_DIR}:/var/lib/influxdb" \
  "$V1_IMAGE" >/dev/null

# Wait for /ping (v1 returns 204 No Content when ready). Bail early if the
# container exits — the connection-reset we'd otherwise see is meaningless.
log "Waiting for InfluxDB 1.8 to come up on 127.0.0.1:${V1_HOST_PORT} ..."
for i in $(seq 1 60); do
  state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo missing)"
  if [[ "$state" != "running" ]]; then
    log "ERROR: container state is '$state' — InfluxDB 1.8 did not stay up."
    log "---- docker logs (last 80 lines) ----"
    docker logs --tail=80 "$CONTAINER_NAME" 2>&1 || true
    log "-------------------------------------"
    log "Container left in place for inspection: $CONTAINER_NAME"
    log "Remove it manually with:  docker rm -f $CONTAINER_NAME"
    trap - EXIT INT TERM
    exit 1
  fi
  if curl -fsS -o /dev/null "http://127.0.0.1:${V1_HOST_PORT}/ping"; then
    log "InfluxDB 1.8 is up (after ${i}s)"
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    log "ERROR: InfluxDB 1.8 did not respond on /ping within 60s"
    log "---- docker logs (last 80 lines) ----"
    docker logs --tail=80 "$CONTAINER_NAME" 2>&1 || true
    log "-------------------------------------"
    exit 1
  fi
done

# ── 3. Run migration ───────────────────────────────────────────────────────
log "Running migrator ..."
(
  cd "$SCRIPT_DIR"
  INFLUXDB1_HOST=127.0.0.1 \
  INFLUXDB1_PORT="${V1_HOST_PORT}" \
  "$PY_BIN" "$MIGRATOR" "$@"
)
log "Migrator finished."

# ── 4. Stop container (handled by trap) ────────────────────────────────────
cleanup

# ── 5. Final note (no deletion) ────────────────────────────────────────────
cat <<EOF

──────────────────────────────────────────────────────────────────────────────
Migration complete.

Original backup (untouched):  $SOURCE_DIR
Temporary copy (NOT deleted): $TEMP_DIR

After you have verified the v2 data, you can remove the temp copy manually:

    sudo rm -rf "$TEMP_DIR"

──────────────────────────────────────────────────────────────────────────────
EOF
