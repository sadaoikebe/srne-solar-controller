#!/usr/bin/env bash
# migrate_temps.sh — phased runner for v1_to_v2_growatt_temps.py
#
# Each phase is its own invocation. Run them in order, decide go/no-go
# between each:
#
#   start    Copy /var/lib/influxdb_backup_1_8 to a writable temp dir,
#            launch a temporary InfluxDB 1.8 container against it.
#            Records the container name and temp-dir path in a session
#            file so later phases can find them.
#
#   inspect  Read-only counts on both sides:
#              - v1: rows with fields 2025/2026/2032/2033 in the
#                migration window, plus a few sample rows.
#              - v2: existing temp_*_growatt point counts (baseline).
#            Stores both in the session file so `verify` can diff.
#
#   dry-run  Runs the Python migrator with --dry-run. Reads from v1,
#            transforms, prints "rows=N points=M" per day chunk, writes
#            nothing to v2. Confirms the transform pipeline works.
#
#   migrate  Resets the migrator's progress file (so a stale "all done"
#            record can't skip writes) and runs the migrator for real.
#
#   verify   Re-counts temp_*_growatt in v2, compares against the
#            baseline captured by `inspect`, prints deltas.
#
#   stop     Stops + removes the temporary v1 container. Leaves the
#            temp-dir copy and the session file in place for inspection.
#
#   status   Prints the current session: container, temp-dir, v1/v2
#            counts captured so far. Handy between phases.
#
# Constraints:
#   - Source backup at SOURCE_DIR is opened only by `cp -a`. Never
#     modified, never deleted.
#   - Temp copy is never auto-deleted.
#   - The Python migrator is the only thing that writes to v2.
#
# Session file: ${SCRIPT_DIR}/.migrate_temps_session.json
# A single active session at a time. Re-running `start` while a session
# exists is refused unless the previous container has already been
# stopped (or you delete the session file manually).
#
# Env-overridable defaults:
#   SOURCE_DIR    = /var/lib/influxdb_backup_1_8
#   TEMP_BASE     = /var/lib/influxdb_temp_copy
#   V1_HOST_PORT  = 8087
#   V1_IMAGE      = influxdb:1.8
#   V1_DB         = mysolardb
#   TIME_START    = 2024-11-01T00:00:00Z   (must match the migrator's window)
#   TIME_STOP     = 2026-05-01T00:00:00Z
#   PY_BIN        = ./venv/bin/python (auto) or python3

set -euo pipefail

# ── Paths & defaults ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATE_FILE="${SCRIPT_DIR}/.migrate_temps_session.json"
MIGRATOR="${SCRIPT_DIR}/v1_to_v2_growatt_temps.py"
PROGRESS_FILE="${SCRIPT_DIR}/.migrate_growatt_temps_progress.json"

SOURCE_DIR="${SOURCE_DIR:-/var/lib/influxdb_backup_1_8}"
TEMP_BASE="${TEMP_BASE:-/var/lib/influxdb_temp_copy}"
V1_HOST_PORT="${V1_HOST_PORT:-8087}"
V1_IMAGE="${V1_IMAGE:-influxdb:1.8}"
V1_DB="${V1_DB:-mysolardb}"
TIME_START="${TIME_START:-2024-11-01T00:00:00Z}"
TIME_STOP="${TIME_STOP:-2026-05-01T00:00:00Z}"

if [[ -z "${PY_BIN:-}" ]]; then
  if [[ -x "${SCRIPT_DIR}/venv/bin/python" ]]; then
    PY_BIN="${SCRIPT_DIR}/venv/bin/python"
  else
    PY_BIN="python3"
  fi
fi

V1_FIELDS=(2025 2026 2032 2033)
V2_NAMES=(temp_inverter_growatt temp_dcdc_growatt temp_buck1_growatt temp_buck2_growatt)

# ── Logging helpers ───────────────────────────────────────────────────────
log()    { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
banner() { printf '\n=== %s ===\n' "$*"; }
die()    { log "ERROR: $*"; exit 1; }

# ── Common preconditions ──────────────────────────────────────────────────
require_tools() {
  command -v docker >/dev/null || die "docker not on PATH"
  command -v curl   >/dev/null || die "curl not on PATH"
  command -v jq     >/dev/null || die "jq not on PATH (apt install jq)"
}

load_env() {
  if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/.env"
    set +a
  fi
  : "${INFLUXDB2_BUCKET:?INFLUXDB2_BUCKET not set (source .env)}"
}

# ── State file (one active session) ───────────────────────────────────────
state_exists() { [[ -f "$STATE_FILE" ]]; }
state_get()    { jq -r "$1 // empty" "$STATE_FILE" 2>/dev/null; }
state_set() {
  local mutation="$1"
  local tmp; tmp="$(mktemp)"
  jq "$mutation" "$STATE_FILE" > "$tmp" && mv "$tmp" "$STATE_FILE"
}
state_require() {
  state_exists || die "no active session — run '$0 start' first"
}

container_state() {
  local c="$1"
  docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing
}

require_running() {
  local c; c="$(state_get .container)"
  [[ -n "$c" ]] || die "session has no container recorded"
  local st; st="$(container_state "$c")"
  [[ "$st" == "running" ]] || die "container $c is '$st'; run '$0 start' or '$0 stop'"
}

# ── v1 / v2 query helpers ─────────────────────────────────────────────────
v1_query() {
  # Run a single InfluxQL query against the temporary v1 container.
  local q="$1" port; port="$(state_get .v1_host_port)"
  local db;   db="$(state_get .v1_db)"
  curl -sS -G "http://127.0.0.1:${port}/query" \
    --data-urlencode "db=${db}" \
    --data-urlencode "q=${q}"
}

v1_count() {
  v1_query "$1" | jq -r '.results[0].series[0].values[0][1] // 0'
}

v2_count_name() {
  # Count points in v2 for a given `name` tag in the migration window.
  local name="$1" start stop bucket
  start="$(state_get .time_start)"
  stop="$(state_get .time_stop)"
  bucket="${INFLUXDB2_BUCKET}"
  local flux
  flux=$(cat <<FLUX
from(bucket: "${bucket}")
  |> range(start: ${start}, stop: ${stop})
  |> filter(fn: (r) => r._measurement == "modbus" and r._field == "value" and r.name == "${name}")
  |> count()
FLUX
)
  docker exec influxdb influx query --raw "$flux" 2>/dev/null \
    | awk -F, 'BEGIN{c=0} /^,_result,/{ c+=$NF+0 } END{print c+0}'
}

# ── Phase: start ──────────────────────────────────────────────────────────
phase_start() {
  require_tools
  load_env

  if state_exists; then
    local c; c="$(state_get .container)"
    if [[ -n "$c" ]] && [[ "$(container_state "$c")" == "running" ]]; then
      die "session already active (container $c). Run '$0 stop' first, or rm -f $STATE_FILE if you know what you're doing."
    fi
    log "Stale session file present; replacing."
    rm -f "$STATE_FILE"
  fi

  [[ -d "$SOURCE_DIR" ]] || die "source backup not found: $SOURCE_DIR"
  [[ -f "$MIGRATOR"   ]] || die "migrator not found: $MIGRATOR"

  if ss -tln 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${V1_HOST_PORT}\$"; then
    die "host port ${V1_HOST_PORT} already in use"
  fi

  local ts;       ts="$(date -u +%Y%m%dT%H%M%SZ)"
  local temp_dir="${TEMP_BASE}_${ts}"
  local cname="influxdb1_temp_${ts}"

  banner "start: copy backup -> $temp_dir"
  mkdir -p "$(dirname "$temp_dir")"
  cp -a "$SOURCE_DIR" "$temp_dir"
  log "Copy size: $(du -sh "$temp_dir" | awk '{print $1}')"

  banner "start: launch v1 container ($cname)"
  docker run -d \
    --name "$cname" \
    --user 0:0 \
    -p "127.0.0.1:${V1_HOST_PORT}:8086" \
    -v "${temp_dir}:/var/lib/influxdb" \
    "$V1_IMAGE" >/dev/null

  log "Waiting for /ping ..."
  local i st
  for i in $(seq 1 60); do
    st="$(container_state "$cname")"
    if [[ "$st" != "running" ]]; then
      log "Container state: $st"
      docker logs --tail=80 "$cname" 2>&1 || true
      die "v1 container did not stay up"
    fi
    if curl -fsS -o /dev/null "http://127.0.0.1:${V1_HOST_PORT}/ping"; then
      log "v1 is up after ${i}s"
      break
    fi
    sleep 1
    [[ $i -eq 60 ]] && { docker logs --tail=80 "$cname" || true; die "/ping timeout"; }
  done

  cat > "$STATE_FILE" <<JSON
{
  "container": "${cname}",
  "temp_dir": "${temp_dir}",
  "v1_host_port": ${V1_HOST_PORT},
  "v1_db": "${V1_DB}",
  "v1_image": "${V1_IMAGE}",
  "time_start": "${TIME_START}",
  "time_stop": "${TIME_STOP}",
  "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "v1_counts": {},
  "v2_baseline": {}
}
JSON
  log "Session: $STATE_FILE"
  log "Next: $0 inspect"
}

# ── Phase: inspect ────────────────────────────────────────────────────────
phase_inspect() {
  require_tools
  load_env
  state_require
  require_running

  banner "inspect: v1 source row counts (window $(state_get .time_start) -> $(state_get .time_stop))"
  local total_v1=0
  local v1_json="{}"
  for fk in "${V1_FIELDS[@]}"; do
    local q="SELECT count(\"$fk\") FROM \"registers\" WHERE time >= '$(state_get .time_start)' AND time < '$(state_get .time_stop)'"
    local c; c="$(v1_count "$q")"
    printf '  field %s : %s rows\n' "$fk" "$c"
    total_v1=$(( total_v1 + c ))
    v1_json="$(jq -c --arg k "$fk" --argjson v "$c" '. + {($k): $v}' <<<"$v1_json")"
  done

  log "Sample (latest 3 rows):"
  local q_peek="SELECT \"2025\",\"2026\",\"2032\",\"2033\" FROM \"registers\" WHERE time >= '$(state_get .time_start)' AND time < '$(state_get .time_stop)' ORDER BY time DESC LIMIT 3"
  v1_query "$q_peek" \
    | jq -r '.results[0].series[0] | "  cols: " + ([.columns[]] | join(",")), (.values[]? | "  " + (map(tostring) | join(",")))' \
    || true

  banner "inspect: v2 baseline (existing temp_*_growatt points in window)"
  local v2_json="{}"
  for nm in "${V2_NAMES[@]}"; do
    local c; c="$(v2_count_name "$nm")"
    printf '  %-22s : %s points\n' "$nm" "$c"
    v2_json="$(jq -c --arg k "$nm" --argjson v "$c" '. + {($k): $v}' <<<"$v2_json")"
  done

  state_set ".v1_counts = ${v1_json} | .v2_baseline = ${v2_json} | .inspected_at = \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""

  echo
  if [[ $total_v1 -eq 0 ]]; then
    log "v1 has 0 rows for the temp fields in this window."
    log "STOP. Either v1 didn't record these or the window is wrong. Do not proceed."
  else
    log "v1 has $total_v1 total rows across the four temp fields."
    log "Next: $0 dry-run   (transform-only sanity check, no writes)"
  fi
}

# ── Phase: dry-run ────────────────────────────────────────────────────────
phase_dry_run() {
  require_tools
  load_env
  state_require
  require_running

  banner "dry-run: migrator with --dry-run (no v2 writes)"
  log "Resetting progress file (so all chunks are processed)."
  rm -f "$PROGRESS_FILE"

  (
    cd "$SCRIPT_DIR"
    INFLUXDB1_HOST=127.0.0.1 \
    INFLUXDB1_PORT="$(state_get .v1_host_port)" \
    INFLUXDB1_DB="$(state_get .v1_db)" \
    "$PY_BIN" "$MIGRATOR" --dry-run
  )
  log "Dry-run complete. Inspect the per-chunk 'rows=N, points=M' lines."
  log "If points totals look right, next: $0 migrate"
}

# ── Phase: migrate ────────────────────────────────────────────────────────
phase_migrate() {
  require_tools
  load_env
  state_require
  require_running

  banner "migrate: resetting progress and writing to v2"
  log "About to write to v2 bucket: $INFLUXDB2_BUCKET"
  read -r -p "Proceed? [yes/NO]: " ans
  [[ "$ans" == "yes" ]] || { log "Aborted."; return; }

  rm -f "$PROGRESS_FILE"
  (
    cd "$SCRIPT_DIR"
    INFLUXDB1_HOST=127.0.0.1 \
    INFLUXDB1_PORT="$(state_get .v1_host_port)" \
    INFLUXDB1_DB="$(state_get .v1_db)" \
    "$PY_BIN" "$MIGRATOR"
  )
  state_set ".migrated_at = \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""
  log "Migrator finished. Next: $0 verify"
}

# ── Phase: verify ─────────────────────────────────────────────────────────
phase_verify() {
  require_tools
  load_env
  state_require

  banner "verify: v2 counts vs baseline captured by 'inspect'"
  printf '  %-22s : %10s -> %10s   delta\n' "metric" "before" "after"
  local all_ok=1
  for nm in "${V2_NAMES[@]}"; do
    local before; before="$(state_get ".v2_baseline.\"$nm\"")"
    [[ -z "$before" ]] && before=0
    local after; after="$(v2_count_name "$nm")"
    local delta=$(( after - before ))
    printf '  %-22s : %10d -> %10d   %+d\n' "$nm" "$before" "$after" "$delta"
    [[ "$delta" -gt 0 ]] || all_ok=0
  done

  echo
  if [[ "$all_ok" -eq 1 ]]; then
    log "All four metrics gained points. Migration looks successful."
    log "Spot-check Grafana, then run: $0 stop"
  else
    log "WARNING: at least one metric did not gain points."
    log "Compare to v1 source counts: $(jq -c '.v1_counts' "$STATE_FILE")"
  fi
}

# ── Phase: stop ───────────────────────────────────────────────────────────
phase_stop() {
  require_tools
  state_require

  local cname temp_dir
  cname="$(state_get .container)"
  temp_dir="$(state_get .temp_dir)"

  banner "stop: removing v1 container ($cname)"
  docker stop  "$cname" >/dev/null 2>&1 || true
  docker rm -f "$cname" >/dev/null 2>&1 || true

  echo
  cat <<EOF
Source backup (untouched):  $SOURCE_DIR
Temporary copy (left in place): $temp_dir
Session file (left in place):  $STATE_FILE

Delete the temp copy when you're done:
    sudo rm -rf "$temp_dir"
    rm -f "$STATE_FILE"
EOF
}

# ── Phase: status ─────────────────────────────────────────────────────────
phase_status() {
  if ! state_exists; then
    echo "No active session. Start one with: $0 start"
    return
  fi
  local cname; cname="$(state_get .container)"
  local st;    st="$(container_state "$cname")"
  echo "Session file : $STATE_FILE"
  echo "Container    : $cname  (state: $st)"
  echo "Temp dir     : $(state_get .temp_dir)"
  echo "v1 db / port : $(state_get .v1_db) / $(state_get .v1_host_port)"
  echo "Window       : $(state_get .time_start) -> $(state_get .time_stop)"
  echo "Started      : $(state_get .started_at)"
  local insp; insp="$(state_get .inspected_at)"
  [[ -n "$insp" ]] && echo "Inspected    : $insp"
  local migd; migd="$(state_get .migrated_at)"
  [[ -n "$migd" ]] && echo "Migrated     : $migd"
  echo
  echo "v1 counts captured:"
  jq -r '.v1_counts | to_entries | map("  field \(.key) = \(.value) rows") | .[]' "$STATE_FILE" 2>/dev/null || true
  echo "v2 baseline captured:"
  jq -r '.v2_baseline | to_entries | map("  \(.key) = \(.value) points") | .[]' "$STATE_FILE" 2>/dev/null || true
}

# ── Dispatcher ────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
usage: $0 {start|inspect|dry-run|migrate|verify|stop|status}

Phases (run in order, decide go/no-go between each):
  start     copy backup, launch v1 container
  inspect   count v1 source rows + v2 baseline; saved in session file
  dry-run   transform-only (--dry-run), no v2 writes
  migrate   reset progress, write to v2 (interactive confirmation)
  verify    diff v2 vs baseline; warn on zero delta
  stop      remove v1 container; temp dir + session file kept
  status    show current session state
EOF
}

case "${1:-status}" in
  start)            phase_start ;;
  inspect)          phase_inspect ;;
  dry-run|dryrun)   phase_dry_run ;;
  migrate)          phase_migrate ;;
  verify)           phase_verify ;;
  stop)             phase_stop ;;
  status)           phase_status ;;
  -h|--help|help)   usage ;;
  *)                usage; exit 2 ;;
esac
