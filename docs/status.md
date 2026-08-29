# Status — 2026-08-26

Snapshot after the SoC-estimator + full-charge-control work. Code on `main`
(latest: Grafana state timelines, cheap idle pad, `controller_state`).
**Not pushed** unless you push it.

## Hardware (unchanged)

| Piece | Role |
|---|---|
| PowMr SunSmart-10KP | Hybrid inverter; SBU/UTI and **grid** charge current |
| Growatt SPF6000ES Plus | Mostly PV charger; DC idle stays on the pack in UTI |
| JK-PB2A16S20P A | FW 15.32, serial 40904495693, MAC `98:DA:20:09:98:80` |
| JK-PB2A16S20P B | FW 15.34, serial 41101490573, MAC `98:DA:20:06:85:65` |
| Cheap window | 23:01–06:58 JST |
| Day (`sbu_fixed`) | 06:59–23:00 |

JK app (both banks): OVP **3.65 V**, OVPR **3.55 V**, SOC-100% **3.59 V**,
start balance **3.50 V** / 20 mV / 2 A, UVP **2.58 V**, SOC-0% **2.60 V**.
Bank A live `nominal_ah` ~196 Ah (SoH 71 % of 275). Real usable ~250–260 Ah.
Estimator tape: **A 260 + B 280 = 540 Ah**.

## Cables

| Link | Unplug? |
|---|---|
| JK RS485 → PowMr (BMS SoC / `0x0100`) | **Later**, when ready. Still plugged. |
| Pi USB → PowMr Modbus (V/I/load/**writes**) | **Never** |
| Pi BLE → JK A/B (query-only) | Needed for Track / Coast JK / cell-CV |

## What is implemented

- Pack SoC from `GET /soc` (260/280 tape). **Never** steer from `0x0100`.
- Estimator modes: `track` / `coast_jk` / `held` / `coast_inverters` /
  `full_anchor` (3.59 V) / `empty_anchor` (3.05 V). See [`soc-control.md`](soc-control.md).
- BMS abort: `I = 0` if charge MOSFET off or `cell_max ≥ 3.55 V` (3.62 V in
  SOAK / CALIBRATE). Pack abort 56.8 V / 57.9 V. Latch if BLE then drops.
- Full-charge nights: **CC → SOAK → CALIBRATE**. IR-free cell table on CC/SOAK;
  SOAK floor 20 A until loaded 3.59 V then 0 A; CALIBRATE 10 A. Stamp
  `last_full_charge` only on 3.59 V / remain_est, not cheap-end. SOAK until
  ~06:40; no SBU after complete the same cheap window.
- Cheap night: regulate to `hold = target + 0.25 %/h × hours left`. 23:12 at
  20 % parks; 20 % at 06:51 keeps SBU. Fill current is `daily_charge_current`.
  See [`charge-control.md`](charge-control.md).
- Influx `charge_control` every 5 s (`charge_mode`, `controller_state`, `i_cmd`, …).
- Grafana state timelines: Charge mode, Output priority, estimator source A/B, MOSFET/abort.
  Full-charge night row (two-up) on JK-BMS and Graphs.

## What is not implemented (on purpose)

- `powmr_compat` backup `/soc` (designed in [`soc-control.md`](soc-control.md)).
- Unplug RS485 / PowMr voltage deadman.
- Capacity-learn discharge to ~2.6 V; rewriting `usable_ah`.
- `daily_target` still uses old Wh/% (~476 Ah tape). `target_soc` % is of 540 Ah.
- BMS writes over BLE. Cell-health job (untracked draft).

## Live operator state (this afternoon)

`targets.json` (do not commit): `target_soc` 18, `daily_charge_current` 0,
`full_charge` **true**, `last_full_charge` 2026-08-13,
`skip_next_auto` 2026-08-26.

If that flag is still set at 23:01, tonight is a **full-charge** night (CC/SOAK),
not a target-18 hold. 22:59 cron is skipped once.

## Next (ops, not code)

1. Grafana: Full-charge night + SoC estimator + Output priority bands look right.
2. Cheap night **or** tonight’s full charge: watch Charge mode / cells / Output priority.
   Until RS485 is out, PowMr may still grid-switch on JK ~9–10 %.
3. When Est. SoC is trusted overnight: unplug **only** JK RS485; set PowMr to
   voltage/user UV/OV deadman; keep Pi USB.
4. Then optionally lower nightly cutoff (small code change) and **one** gated
   2.60–2.65 V capacity-learn cycle. Pi on grid.

## Do not

- Unplug Pi USB Modbus.
- Write JK settings/MOSFET/balance over BLE.
- Nightly 2.6 V cycles.
- Auto-switch to `0x0100` on BLE loss.
- Grid trickle “to hold SoC”.
