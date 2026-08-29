# Charge and output-priority control

`battery_controller` (5 s). Pack SoC from `GET /soc`, not `0x0100`. Cells and
MOSFETs from `GET /bms`. Writes SBU/UTI and **grid** charge current via
`modbus_api`. See [`status.md`](status.md) for where we are.

`daily_charge_current` is the PowMr **grid** fill current (A). Growatt / PowMr
PV can still charge. Full-charge CC ignores that cap (uses 120 A, grid-limited).

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

One setpoint (idle ~0.25 %/h). It slides down toward `target_soc` as morning
approaches:

```
hold = target_soc + 0.25 %/h × hours_left_in_cheap
```

Regulate SoC to `hold`. Fill current is `daily_charge_current`.

- SoC well above `hold` → **SBU** until `hold`, then park.
- SoC near `hold` → **UTI_STOPPED**.
- SoC well below `hold` → **UTI_CHARGING** (amps = `daily_charge_current`).
- `full_charge`: force `UTI_CHARGING` (CC/SOAK/CALIBRATE).

Stop charge / leave SBU at `hold + 0.4`. Start charge or SBU only at `hold ± 2`
(about one night of idle). A 1 % dip after parking does not start another fill.

- 20 % at 23:12, target 18 % → `hold ≈ 20` → park.
- 20 % at 06:51, target 18 % → `hold ≈ 18` → stay SBU.

### Day (`sbu_fixed`, 06:59–23:00)

Voltage 49.4 / 51.6 V, `CUTOFF_SOC = 9 %`, `cell_min ≤ 3.05 V` blocks SBU.
30 min cooldown after SBU→UTI.

## Full charge (`full_charge: true`)

Not clocked SYNC. Every 5 s, no slew, no extra hysteresis.

Compared to the old BULK + clocked SYNC 30 A, not to the failed millivolt servo.

- **CC:** 120 A, pack-V table still applies, **no SoC taper**. Old BULK cut
  from 60 % SoC while cells were still on the plateau. Do **not** end CC on
  loaded `cell_max ≥ 3.45 V` — that is IR on the high-R cells (B05 / B08 /
  B12 / B14 rotate as hottest; A07 / A08 the same class), not the knee.
  Leave CC when pack ≥ 55.2 V or **IR-free** `max(V − I·R)` over all 32
  cells ≥ 3.45 V. Loaded abort remains 3.55 V (CC / NORMAL only).
- **SOAK:** pack-V table min **loaded** hottest-cell table (same amp
  steps). Last bin **3 A**. Do not zero at 3.59 V. Abort **3.62 V** /
  pack **57.9 V**. Stay until ~06:40.
- **CALIBRATE:** flat **10 A** to 3.59 V. Same 3.62 V / 57.9 V abort. No
  IR-free 7 A cap.

```
CC   120 A until pack-V 55.2 or IR-free cell_max ≥ 3.45 V
     loaded 3.45 V does nothing; loaded 3.55 V → 0 A

SOAK table on loaded hottest cell (tail 3 A); stay until ~06:40
     abort 3.62 V / pack 57.9 V → CALIBRATE

CALIBRATE 10 A to 3.59 V; abort 3.62 V / pack 57.9 V
     complete only on cell 3.59 V or remain_est at full

DONE  I = 0, last_full_charge = today
      cheap-end without snap: drop flag, do not stamp last_full_charge
      rest of cheap after snap: UTI_STOPPED, not SBU
```

Planner auto-sets the flag on poor PV, ≥ 14 days since last full, midday GHI
weak. Manual: form **Full charge** + **Skip next 22:59**.

## BMS abort (every night)

`I = 0` if either charge MOSFET is off or `cell_max ≥ 3.55 V` (3.62 V in
SOAK / CALIBRATE). Pack abort 56.8 V in CC / NORMAL, 57.9 V in SOAK /
CALIBRATE. Trip latches if BLE then drops. No trip + BLE blip: pack-V table.
After MOSFET trip: resume 8 A only when `cell_max < 3.48 V` and 90 s elapsed.

Query-only BLE. Do not write the BMS.

## Influx `charge_control` (5 s)

| `name` | Meaning |
|---|---|
| `charge_mode` | 0 NORMAL, 1 CC, 2 SOAK, 3 CALIBRATE |
| `controller_state` | 0 UTI_STOPPED, 1 UTI_CHARGING, 2 SBU |
| `i_cmd` / `i_pack` | Commanded grid A / measured pack A |
| `cell_max` / `cell_min` / `cell_delta` | Loaded, from live BLE |
| `cell_max_ir_free` | `max(V − I·R)` over all 32 cells |
| `bms_abort` / `charge_mosfet` | Abort flag / MOSFETs on |
| `hot_cell` / `hot_bank` | Highest **loaded** cell (1-based, a=0 b=1) |

Grafana: **Full-charge night** (cells, currents, Charge mode, MOSFET) on JK-BMS
and Graphs. **Output priority** sits in Load & Grid. **SoC estimator** (source
A/B) on Graphs only. State timelines, not box-and-arrow diagrams.

## Fail closed

| `/soc` missing or `age_s > 30 s` | Hold last Est. SoC; 5 min SBU → UTI_STOPPED |
| `/bms` stale, no prior abort | Pack-V current table |
| `/bms` stale after abort | Keep `I = 0` |
| Manual override | Pins SBU/UTI 60 min; abort and grid limit still apply |
