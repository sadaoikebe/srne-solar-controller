# Charge and output-priority control

`battery_controller` (5 s). Pack SoC from `GET /soc`, not `0x0100`. Cells and
MOSFETs from `GET /bms`. Writes SBU/UTI and **grid** charge current via
`modbus_api`. See [`status.md`](status.md) for where we are.

`daily_charge_current` is the PowMr **grid** charge cap only. **0 = do not
buy night kWh.** Growatt / PowMr PV can still charge. Full-charge CC ignores
that cap (uses 120 A, grid-limited).

## Output priority (SBU / UTI)

| State | PowMr `0xE204` | Grid charge | House load |
|---|---|---|---|
| `SBU` | SBU | 0 A | Battery |
| `UTI_STOPPED` | UTI | 0 A | Grid |
| `UTI_CHARGING` | UTI | `I_cmd` > 0 | Grid |

UTI_CHARGING and UTI_STOPPED both look like **UTI** on the inverter. The
real state is `charge_control.controller_state` (0 / 1 / 2).

Boot default: `UTI_STOPPED` until the first good `/soc`.

### Cheap (23:01–06:58)

Idle pad (observed DC leak ~2 % / 8 h):

```
sbu_exit = target_soc + 0.25 %/h × hours_left_in_cheap
```

Leave **SBU → UTI_STOPPED** when `SoC < sbu_exit + 0.4`.

- 20 % at 23:12 (~8 h left) → **UTI_STOPPED** (pad ~2 %).
- 20 % at 06:51 (~7 min left) → **stay SBU**.
- `daily_charge_current = 0`: **never** `UTI_CHARGING`.
- `daily_charge_current > 0` and SoC < target − 0.4: `UTI_CHARGING`.
- `full_charge`: force `UTI_CHARGING` (CC/SOAK/CALIBRATE).

### Day (`sbu_fixed`, 06:59–23:00)

Voltage 49.4 / 51.6 V, `CUTOFF_SOC = 9 %`, `cell_min ≤ 3.05 V` blocks SBU.
30 min cooldown after SBU→UTI.

## Full charge (`full_charge: true`)

Not clocked SYNC. `max(cell)` over both banks.

```
CC   I ≤ 120 A (slew 10 A / 5 s, grid headroom)
     until any cell ≥ 3.45 V

SOAK hold max(cell) ~ 3.50–3.52 V; still send current to lagging cells
     balancer starts at 3.50 V / 20 mV / 2 A
     ≥ 45 min or cheap about to end → CALIBRATE

CALIBRATE I ≤ 10 A, creep to 3.59 V (estimator full_anchor)
     abort ceiling 3.62 V (not 3.55)

DONE  I = 0, clear flag, last_full_charge = today
```

SOAK current vs `max(cell)`: 3.50 V → 20 A, 3.51 → 12 A, 3.52 → 4 A,
≥ 3.53 → 0 A. `dV/dt` that would hit 3.55 within ~10 s → 0 A.

Planner auto-sets the flag on poor PV, ≥ 14 days since last full, midday GHI
weak. Manual: form **Full charge** + **Skip next 22:59**.

## BMS abort (every night)

`I = 0` if either charge MOSFET is off or `cell_max ≥ 3.55 V` (3.62 V in
CALIBRATE). Trip latches if BLE then drops. No trip + BLE blip: pack-V table.
After MOSFET trip: resume 8 A only when `cell_max < 3.48 V` and 90 s elapsed.

Query-only BLE. Do not write the BMS.

## Influx `charge_control` (5 s)

| `name` | Meaning |
|---|---|
| `charge_mode` | 0 NORMAL, 1 CC, 2 SOAK, 3 CALIBRATE |
| `controller_state` | 0 UTI_STOPPED, 1 UTI_CHARGING, 2 SBU |
| `i_cmd` / `i_pack` | Commanded grid A / measured pack A |
| `cell_max` / `cell_min` / `cell_delta` | From live BLE |
| `bms_abort` / `charge_mosfet` | Abort flag / MOSFETs on |
| `hot_cell` / `hot_bank` | Highest cell (1-based, a=0 b=1) |

Grafana: JK-BMS and Graphs — **Full-charge night** (cells, currents, Charge mode,
MOSFET) and **SoC estimator** (source A/B, Output priority). State timelines, not
box-and-arrow diagrams.

## Fail closed

| `/soc` missing or `age_s > 30 s` | Hold last Est. SoC; 5 min SBU → UTI_STOPPED |
| `/bms` stale, no prior abort | Pack-V current table |
| `/bms` stale after abort | Keep `I = 0` |
| Manual override | Pins SBU/UTI 60 min; abort and grid limit still apply |
