#!/usr/bin/env bash
# migrate_temps.sh — verifiable end-to-end runner for v1_to_v2_growatt_temps.py
#
# Six explicit phases, each prints the numbers we care about so you can spot
# where it goes wrong:
#
#   1) copy backup        -> writable temp dir, source untouched
#   2) start v1.8         -> InfluxDB 1.8 container against the temp copy
#   3) count v1 source    -> rows with fields 2025/2026/2032/2033 in the
#                            migration window (proves data exists in v1)
#   4) count v2 BEFORE    -> existing temp_*_growatt points in v2 (baseline)
#   5) run migrator       -> Python migration with progress reset, --dry-run
#                            optional via REAL_RUN=0 env
#   6) count v2 AFTER     -> temp_*_growatt counts after migration; compare
#                            against the v1 row count
#   7) stop v1 container  -> always (trap)
#
# Set REAL_RUN=0 to do steps 1..7 with the migrator in --dry-run mode.
# Default is REAL_RUN=1 (writes to v2).
#
# This script never:
#   - touches the source backup
#   - deletes the temp copy
#   - deletes any v2 data
#
# Connection details (v1 host/port set by this script; v2 from .env):
#   SOURCE_DIR    = /var/lib/influxdb_backup_1_8
#   TEMP_BASE     = /var/lib/influxdb_temp_copy
#   V1_HOST_PORT  = 8087
#   V1_IMAGE      = influxdb:1.8
#   V1_DB         = mysolardb            (the v1 database name)
#   TIME_START    = 2024-11-01T00:00:00Z
#   TIME_STOP     = 2026-05-01T00:00:00Z

set -euo pipefail

SOURCE_DIR="${SOURCE_DIR:-/var/lib/influxdb_backup_1_8}"
TEMP_BASE="${TEMP_BASE:-/var/lib/influxdb_temp_copy}"
V1_HOST_PORT="${V1_HOST_PORT:-8087}"
V1_IMAGE="${V1_IMAGE:-influxdb:1.8}"
V1_DB="${V1_DB:-mysolardb}"
TIME_START="${TIME_START:-2024-11-01T00:00:00Z}"
TIME_STOP="${TIME_STOP:-2026-05-01T00:00:00Z}"
REAL_RUN="${REAL_RUN:-1}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
TEMP_DIR="${TEMP_BASE}_${TS}"
CONTAINER_NAME="influxdb1_temp_${TS}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MIGRATOR="${SCRIPT_DIR}/v1_to_v2_growatt_temps.py"
PROGRESS_FILE="${SCRIPT_DIR}/.migrate_growatt_temps_progress.json"

if [[ -z "${PY_BIN:-}" ]]; then
  if [[ -x "${SCRIPT_DIR}/venv/bin/python" ]]; then
    PY_BIN="${SCRIPT_DIR}/venv/bin/python"
  else
    PY_BIN="python3"
  fi
fi

log()    { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
banner() { printf '\n=== %s ===\n' "$*"; }

# ── Sanity ────────────────────────────────────────────────────────────────
[[ -d "$SOURCE_DIR" ]] || { log "ERROR: source backup not found: $SOURCE_DIR"; exit 1; }
[[ -f "$MIGRATOR" ]]   || { log "ERROR: migrator not found: $MIGRATOR"; exit 1; }
command -v docker >/dev/null || { log "ERROR: docker not on PATH"; exit 1; }
command -v curl   >/dev/null || { log "ERROR: curl not on PATH"; exit 1; }
command -v jq     >/dev/null || { log "ERROR: jq not on PATH (apt install jq)"; exit 1; }

if ss -tln 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${V1_HOST_PORT}\$"; then
  log "ERROR: host port ${V1_HOST_PORT} already in use"; exit 1
fi

# Source .env so V2_* vars are available for the v2 count queries.
# (Python migrator also reads .env via load_dotenv.)
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/.env"
  set +a
fi
: "${INFLUXDB2_BUCKET:?INFLUXDB2_BUCKET not set (source .env)}"

log "Source backup : $SOURCE_DIR"
log "Temp copy     : $TEMP_DIR"
log "v1 container  : $CONTAINER_NAME  ($V1_IMAGE on 127.0.0.1:${V1_HOST_PORT})"
log "v1 database   : $V1_DB"
log "Migration win : $TIME_START -> $TIME_STOP"
log "v2 bucket     : $INFLUXDB2_BUCKET"
log "Mode          : $([[ $REAL_RUN -eq 1 ]] && echo "REAL (writes to v2)" || echo "DRY-RUN")"
log "Python        : $PY_BIN"

# ── Phase 1: copy backup ──────────────────────────────────────────────────
banner "1) Copying backup -> temp copy"
mkdir -p "$(dirname "$TEMP_DIR")"
cp -a "$SOURCE_DIR" "$TEMP_DIR"
log "Copy complete: $(du -sh "$TEMP_DIR" | awk '{print $1}')"

# ── Phase 2: start v1.8 ───────────────────────────────────────────────────
cleanup_started=0
cleanup() {
  if [[ "$cleanup_started" -eq 1 ]]; then return; fi
  cleanup_started=1
  banner "7) Stopping v1 container"
  docker stop  "$CONTAINER_NAME" >/dev/null 2>&1 || true
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

banner "2) Starting v1.8 container"
docker run -d \
  --name "$CONTAINER_NAME" \
  --user 0:0 \
  -p "127.0.0.1:${V1_HOST_PORT}:8086" \
  -v "${TEMP_DIR}:/var/lib/influxdb" \
  "$V1_IMAGE" >/dev/null

log "Waiting for /ping ..."
for i in $(seq 1 60); do
  state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo missing)"
  if [[ "$state" != "running" ]]; then
    log "ERROR: container state '$state'"
    docker logs --tail=80 "$CONTAINER_NAME" 2>&1 || true
    exit 1
  fi
  curl -fsS -o /dev/null "http://127.0.0.1:${V1_HOST_PORT}/ping" && { log "v1 is up (${i}s)"; break; }
  sleep 1
  [[ $i -eq 60 ]] && { log "ERROR: /ping timeout"; docker logs --tail=80 "$CONTAINER_NAME" || true; exit 1; }
done

# ── Phase 3: count source rows in v1 ──────────────────────────────────────
v1_query() {
  local q="$1"
  curl -sS -G "http://127.0.0.1:${V1_HOST_PORT}/query" \
    --data-urlencode "db=${V1_DB}" \
    --data-urlencode "q=${q}"
}

# Returns the integer count from a single-series, single-value response.
v1_count() {
  local q="$1"
  v1_query "$q" | jq -r '.results[0].series[0].values[0][1] // 0'
}

banner "3) Count source rows in v1 (fields 2025/2026/2032/2033)"
declare -A V1_COUNT
for fk in 2025 2026 2032 2033; do
  q="SELECT count(\"$fk\") FROM \"registers\" WHERE time >= '${TIME_START}' AND time < '${TIME_STOP}'"
  c="$(v1_count "$q")"
  V1_COUNT["$fk"]="$c"
  printf '  field %s : %s rows\n' "$fk" "$c"
done

# Show a peek at the actual values so we can sanity-check °C × 10.
log "Sample (latest 3 rows):"
q_peek="SELECT \"2025\",\"2026\",\"2032\",\"2033\" FROM \"registers\" WHERE time >= '${TIME_START}' AND time < '${TIME_STOP}' ORDER BY time DESC LIMIT 3"
v1_query "$q_peek" | jq -r '.results[0].series[0] | "  columns: " + ([.columns[]] | join(",")), (.values[]? | "  " + (map(tostring) | join(",")))' || true

# Bail if v1 has no data — migrator would write nothing
total_v1=$(( V1_COUNT[2025] + V1_COUNT[2026] + V1_COUNT[2032] + V1_COUNT[2033] ))
if [[ $total_v1 -eq 0 ]]; then
  log "v1 has 0 rows for the temp fields in the migration window."
  log "Either the window is wrong or v1 didn't record these. Aborting."
  exit 1
fi

# ── Phase 4: count v2 BEFORE migration ────────────────────────────────────
v2_count_name() {
  local name="$1"
  local flux
  flux=$(cat <<FLUX
from(bucket: "${INFLUXDB2_BUCKET}")
  |> range(start: ${TIME_START}, stop: ${TIME_STOP})
  |> filter(fn: (r) => r._measurement == "modbus" and r._field == "value" and r.name == "${name}")
  |> count()
FLUX
)
  # influx CLI inside the v2 container; --raw gives annotated CSV
  docker exec influxdb influx query --raw "$flux" 2>/dev/null \
    | awk -F, 'BEGIN{c=0} /^,_result,/{ c+=$NF+0 } END{print c+0}'
}

banner "4) Count v2 BEFORE migration"
declare -A V2_BEFORE
for nm in temp_inverter_growatt temp_dcdc_growatt temp_buck1_growatt temp_buck2_growatt; do
  c="$(v2_count_name "$nm")"
  V2_BEFORE["$nm"]="$c"
  printf '  %-22s : %s points\n' "$nm" "$c"
done

# ── Phase 5: run migrator ─────────────────────────────────────────────────
banner "5) Running migrator"
log "Resetting progress file (in case a previous partial run is recorded)."
rm -f "$PROGRESS_FILE"

migrator_args=()
[[ "$REAL_RUN" -eq 1 ]] || migrator_args+=("--dry-run")

(
  cd "$SCRIPT_DIR"
  INFLUXDB1_HOST=127.0.0.1 \
  INFLUXDB1_PORT="${V1_HOST_PORT}" \
  INFLUXDB1_DB="${V1_DB}" \
  "$PY_BIN" "$MIGRATOR" "${migrator_args[@]}"
)
log "Migrator finished."

# ── Phase 6: count v2 AFTER migration ─────────────────────────────────────
banner "6) Count v2 AFTER migration"
all_ok=1
printf '  %-22s : %10s -> %10s   delta\n' "metric" "before" "after"
for nm in temp_inverter_growatt temp_dcdc_growatt temp_buck1_growatt temp_buck2_growatt; do
  before="${V2_BEFORE[$nm]}"
  after="$(v2_count_name "$nm")"
  delta=$(( after - before ))
  printf '  %-22s : %10d -> %10d   %+d\n' "$nm" "$before" "$after" "$delta"
  if [[ "$REAL_RUN" -eq 1 && "$delta" -le 0 ]]; then
    all_ok=0
  fi
done

if [[ "$REAL_RUN" -eq 1 && "$all_ok" -eq 1 ]]; then
  log "All four temp metrics gained points. Migration looks successful."
elif [[ "$REAL_RUN" -eq 0 ]]; then
  log "DRY-RUN: no points written; v2 counts unchanged is expected."
else
  log "WARNING: at least one metric did not gain points. Check the output above."
fi

# Phase 7 (cleanup) runs via the EXIT trap.

cat <<EOF

Source backup (untouched):  $SOURCE_DIR
Temporary copy:             $TEMP_DIR
v1 row counts (window):     2025=${V1_COUNT[2025]}  2026=${V1_COUNT[2026]}  2032=${V1_COUNT[2032]}  2033=${V1_COUNT[2033]}

Delete the temp copy when you're done:
    sudo rm -rf "$TEMP_DIR"
EOF
