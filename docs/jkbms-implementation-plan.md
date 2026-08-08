# JK-BMS integration — implementation plan

**Branch:** `feature/jkbms-api`  
**Worktree:** `/home/shobon/srne-solar-controller-jkbms` (isolated from production `~/srne-solar-controller`)  
**Status:** Step 1 (BLE client) done — host CLI + unit tests verified

## Locked decisions

| Item | Choice |
|------|--------|
| Service name | `jkbms_api` |
| Influx measurement | `jkbms` |
| Poll ownership | Background cache inside `jkbms_api` |
| Phase 1 fields | SoC, SoH, V, I, power, cells (16×2), temps, cycles, FETs, balance, runtime, min/max/Δ |
| Safety | Query-only BLE (`0x96` / `0x97`); no control endpoints |
| Banks | `a` = 40904495693 / `98:DA:20:09:98:80`; `b` = 41101490573 / `98:DA:20:06:85:65` |
| Parallel validation | Standalone collector + Influx writer; **do not** touch production `db_writer` until proven |

## Banks (fixed config)

| bank | serial | MAC |
|------|--------|-----|
| `a` | `40904495693` | `98:DA:20:09:98:80` |
| `b` | `41101490573` | `98:DA:20:06:85:65` |

## Influx point model (phase 1)

**Measurement:** `jkbms`

**Pack metrics** (one point per name per bank):

- Tags: `bank`, `serial`, `name`, `unit`
- Field: `value` (float, SI units)

Names: `soc`, `soh`, `voltage`, `current`, `power`, `remain_ah`, `nominal_ah`, `cycles`, `mos_temp`, `temp1`, `temp2`, `balance_current`, `balancing`, `charge_mosfet`, `discharge_mosfet`, `runtime_s`, `cell_count`, `cell_min`, `cell_max`, `cell_delta`

**Cell voltages:**

- Tags: `bank`, `serial`, `name=cell_voltage`, `cell=01..16`, `unit=V`
- Field: `value`

---

## Phase order (recommended)

### Phase 0 — Isolation (done)

- [x] Clone repo to separate directory
- [x] Create branch `feature/jkbms-api`
- [x] Production tree left untouched

### Phase 1 — BLE library module (host Python first) ✅

**Goal:** Reuse proven logic from `~/jkbms-ble/03_read_basic.py` as importable code.

**Deliverables:**

- [x] `jkbms_client.py` — allowlist queries, frame collector, parsers, `read_bank` / `read_banks`
- [x] `jkbms.yaml` — bank A/B MAC + serial
- [x] `requirements-jkbms.txt` — bleak (+ pyyaml)
- [x] `tests/test_jkbms_client.py` — pure unit tests (no BLE)
- [x] Host one-shot CLI verified: 2/2 banks OK

**Exit criteria:** CLI one-shot reads A and B successfully on this Pi. **Met.**

### Phase 2 — `jkbms_api` service (cache + HTTP only)

**Goal:** Background poller + FastAPI; no Influx yet.

**Deliverables:**

- `jkbms_api.py` (FastAPI + asyncio background task)
- Config: `jkbms.yaml` (or env) with banks, poll interval (default 30 s)
- Endpoints:
  - `GET /health` — process up; optionally “at least one bank ever ok”
  - `GET /bms` — full snapshot both banks
  - `GET /bms/{bank}` — single bank
- Response includes `ok`, `age_s`, `error`, all phase-1 fields + `cells[]`
- Partial failure: A ok / B fail still returns 200 with per-bank status
- Logging via existing `log_config.py` pattern
- **No write/control routes**

**Exit criteria:** `curl localhost:…/bms` shows live data; values match app within noise.

### Phase 3 — Docker packaging for parallel run

**Goal:** Run `jkbms_api` without touching production compose services.

**Deliverables:**

- `Dockerfile.jkbms` **or** extend Dockerfile carefully (prefer separate image/target first)
- `compose.jkbms.yaml` (standalone overlay / project name) that:
  - Builds/runs only `jkbms_api`
  - Does **not** redefine `modbus_api`, `db_writer`, `battery_controller`
  - Uses host Bluetooth access (likely `network_mode: host` or D-Bus mounts + caps — verify on this Pi)
  - Publishes HTTP port (e.g. `5005`)
- `requirements-jkbms.txt` or extra deps: `bleak`, reuse fastapi/uvicorn
- Document: phone app must not hold BLE during Pi polls

**Exit criteria:** Container serves `/bms`; production stack still healthy on its own compose project.

### Phase 4 — Standalone Influx writer (parallel validation)

**Goal:** Persist to **running** Influx without modifying production `db_writer.py`.

**Deliverables:**

- `jkbms_db_writer.py` (or flag mode later):
  - 30 s wall-aligned loop (same style as `db_writer`)
  - `GET http://…/bms`
  - Map JSON → Influx points (measurement `jkbms`)
  - Write to existing `INFLUX_URL` / `INFLUX_ORG` / `INFLUX_BUCKET` / token from env
- Compose service `jkbms_db_writer` in `compose.jkbms.yaml` only
- Failures: log + skip tick; never crash-loop the host stack

**Exit criteria:** Flux query shows `jkbms` points every ~30 s for both banks.

Example Flux:

```flux
from(bucket: "mysolardb")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "jkbms")
  |> filter(fn: (r) => r._field == "value")
```

### Phase 5 — Simple Grafana dashboard (validation UI)

**Goal:** Confirm collection visually; do not rewrite existing solar dashboards.

**Deliverables:**

- `grafana/provisioning/dashboards/5_jkbms.json` (or a **manual** import first if production Grafana should not auto-provision from the worktree)
- **Preferred for parallel validation:** import JSON once via Grafana UI into the **running** Grafana, so production provisioning path stays clean until merge
- Panels (minimal set):
  1. SoC A vs B (%)
  2. Pack voltage A vs B (V)
  3. Current A vs B (A)
  4. Temps (MOS / T1 / T2) per bank
  5. Cell voltages bank A (16 series)
  6. Cell voltages bank B (16 series)
  7. Cell Δ (mV) A vs B
  8. Optional: SoH, cycles, MOSFET on/off as stat panels
- All queries: `_measurement == "jkbms"`

**Exit criteria:** Dashboard matches phone app for SoC/V/cells within normal drift.

### Phase 6 — Soak test (before any merge into main path)

**Goal:** Confidence under real life.

**Checks (24–48 h):**

- Point continuity (no multi-hour gaps unless explained)
- BLE contention (phone app usage)
- One bank disconnect recovery
- Container restart recovery
- CPU/memory of jkbms services
- Production Modbus path unaffected (existing Grafana unchanged)

### Phase 7 — Integrate into main compose / `db_writer` (later)

**Only after soak test.** Optional merge strategies:

**A (recommended):** Keep `jkbms_api` + fold `jkbms_db_writer` logic into production `db_writer` as a second fetch path  
**B:** Keep separate writer service forever (simpler isolation, two clocks)

Also:

- Add services to main `compose.yaml`
- Provision dashboard `5_jkbms.json` under `grafana/provisioning`
- README section
- **Still** no BMS → battery_controller coupling unless explicitly requested

### Phase 8 — Future (explicitly out of scope now)

- Feed BMS SoC into `battery_controller`
- Error bitmask alerts
- Settings frame dumps
- BLE control (charge FET, balance) — do not enable without separate safety review

---

## Suggested file layout (when coding starts)

```
srne-solar-controller-jkbms/
  jkbms_client.py          # BLE read-only client
  jkbms_api.py             # FastAPI + background cache
  jkbms_db_writer.py       # Parallel Influx writer (validation)
  jkbms.yaml               # bank MACs / poll interval
  compose.jkbms.yaml       # parallel stack only
  Dockerfile.jkbms         # optional separate image
  docs/jkbms-implementation-plan.md
  grafana/.../5_jkbms.json # after validation
```

Production `~/srne-solar-controller` remains the running compose project until you deliberately cut over.

---

## Parallelism with production (how not to collide)

| Resource | Production | JK-BMS parallel stack |
|----------|------------|------------------------|
| Compose project | existing (e.g. dir name) | `docker compose -f compose.jkbms.yaml -p jkbms …` |
| `modbus_api` / USB | production only | not started |
| `db_writer` | production only | not modified; not started from worktree |
| Bluetooth | free when app disconnected | `jkbms_api` only |
| InfluxDB | shared | shared bucket, **new measurement** `jkbms` |
| Grafana | shared | new dashboard (import or later provision) |
| Port 5004 | production modbus | use **5005** (or host network) for jkbms |

---

## Order cheat-sheet

1. ~~Clone + branch~~  
2. **Host BLE client library** (prove parse again in-repo)  
3. **`jkbms_api` + background cache + `/bms`**  
4. **Docker parallel compose** (BT access)  
5. **`jkbms_db_writer` → running Influx**  
6. **Grafana validation dashboard**  
7. **Soak test**  
8. **Later:** wire into main compose / optionally fold writer into `db_writer`  

Do **not** start with Grafana or with `db_writer.py` edits — those come after the API returns correct JSON.
