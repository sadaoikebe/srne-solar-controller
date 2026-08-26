# Per-cell LFP notes (banks A / B)

Field notes for the dual 16S LiFePO4 pack behind two JK-BMS units. All 32 cells were bought together; chemistry is LFP only.

| | |
| --- | --- |
| **Date** | 2026-08-22 … 2026-08-23 |
| **Data** | Influx measurement `jkbms`, ~30 s, 2026-08-08 22:32 JST → 2026-08-23 |
| **Banks** | A serial `40904495693` (16S), B serial `41101490573` (16S), paralleled at the inverter bus |
| **Cells** | Prismatic ~174 × 72 × 207 mm (280 Ah class). BMS A reports `nominal_ah` ≈ 196 Ah / pack SoH 71 % / 139 cycles; BMS B reports 280 Ah / 100 % / 307 cycles. Treat those pack integers as BMS programming, not per-cell truth. |
| **Status** | Analysis + read-only report script. **Busbar re-torque not done yet.** |

Related: design draft [`cell-health-diagnostics.md`](cell-health-diagnostics.md). Grafana already plots raw voltages on **JK-BMS Battery Banks** (`/d/solar-jkbms/jk-bms-battery-banks`). Cell IDs are `A01`–`A16` / `B01`–`B16` (zero-padded; “A6” = **A06**).

---

## What we built

Read-only CLI. Queries existing `jkbms` history. **Writes nothing** to Influx, Grafana, or the BMS.

```bash
cd srne-solar-controller
./venv/bin/python jkbms_cell_health_report.py
./venv/bin/python jkbms_cell_health_report.py --start "2026-08-08" --stop "2026-08-23" -o /tmp/cell-health-report.md
```

| File | Role |
| --- | --- |
| `jkbms_cell_health_report.py` | Influx query + markdown/JSON report |
| `jkbms_cell_health_metrics.py` | Residuals vs **bank median**, regimes, classification |
| `jkbms_cell_health.yaml` | LFP thresholds for *this* pack |
| `tests/test_jkbms_cell_health_metrics.py` | Unit tests (no Influx) |

Canonical SoH is remaining capacity \(SOH_Q = Q/Q_\text{nom}\). Mid-plateau voltage is not SoH. LFP OCV is ~3.20–3.35 V from roughly 10–90 % SoC. Diagnose by *when* a cell is off: rest, charge load, discharge load, charge-end, or empty.

There is **no** per-cell current (series 16S shares pack `current`). Temps are pack-level. Pack BMS `soh` is a crude pack heuristic.

---

## How to read a residual

Residual = cell voltage − **that bank’s median** (mV). Never mix A and B in one median. Charge current is **positive**, discharge **negative**.

| When the offset appears | Likely cause |
| --- | --- |
| Rest only, few mV | SoC imbalance / OCV / ADC. Usually not fade. |
| Grows with \|I\| (high on charge, low on discharge) | Extra DCIR or **connection** |
| Persistently highest at charge-end (~3.50 V) | Lower remaining capacity (fills first) |
| Lowest at empty (~3.00 V) | Lower remaining capacity (empties first) |
| Same offset at rest, load, **and** charge-end | Wiring / sense-lead / ADC |

House charge-end window uses pack V ≥ 55.2 V and SoC ≥ 98 % (first `VOLT_LIMITS` taper), I ≤ 20 A. SYNC 30 A is **not** charge-end.

---

## Full-history report (2026-08-08 → 2026-08-22)

~40 k aligned samples per bank. Rest cell Δ p95: **A 26 mV**, **B 11 mV**. No cell hit a true 3.00 V **rest** knee in that window (BMS A SoC did go 0–100 %; B only 10–100 %).

### Bank A (standouts)

| Cell | Class | Notes |
| --- | --- | --- |
| **A06** | `nominal` at mid-SoC | Lowest at rest **74 %** of rest samples, but only **−3 mV**. Everyday “A6 looks different” is plateau ranking, not 40 mV of fade. |
| **A07, A08** | `high_dcir_or_connection` | Rest ≈ 0. Charge **+22 / +24 mV**, discharge **−16 mV**. 30 s ΔV/ΔI ~3.0 mΩ (~2× bank). Adjacent pair → inspect the **A07–A08 joint** before blaming two cells. |
| A03, A10, A15 | same IR family, milder | ~2.1 mΩ |
| A02 | `mixed_or_unclear` | Fills *last* at CE; low IR. Second-lowest at empty only because A06 had already dragged the string. |

### Bank B (standouts)

| Cell | Class | Notes |
| --- | --- | --- |
| **B10** | `capacity_mismatch` | Highest at charge-end **63 %** of CE samples. Rest ~0, DCIR *below* bank median. Qualitative until B is actually emptied. |
| **B05, B08, B12, B14** | `high_dcir_or_connection` | ~1.65–1.75 mΩ, 2.6–2.8× their bank. |
| B01 | `nominal` | Sticky min at rest by *rank*, but only ~−2 mV at low SoC. |

Pack BMS SoH 71 % (A) vs 100 % (B) is **not** a fair cell comparison (`nominal_ah` 196 vs 280, cycle counters 139 vs 307).

---

## 17 Aug 2026 — bank A actually emptied

The one deep event in this dataset.

- Bank A hit **SoC 0** around **22:59 JST**. A06 was the lowest cell in **all** of the deepest 30 samples, down to **3.055 V** at only **−5.4 A**, neighbours still ~3.16 V (spread **0.11 V**).
- At SoC ≤ 15 % and \|I\| < 5 A, A06 is the minimum **100 %** of the time (mean residual **−32 mV**). DCIR near the bank median, so this is **empty-first**, not ohmic sag.
- Bank B that evening never went below ~13 % SoC / **3.147 V**. Its “lowest” ticks were the IR cells at **−18 A** (ohmic).

So: **A06 is the capacity limiter of string A.** It does not show up as a villain in the middle of the plateau or at a 75 A house peak (see below).

---

## 22–23 Aug 2026 — house peak load (~170 A combined)

Two bursts (23:01–23:14 JST and 23:59–00:09 JST). Combined discharge peaked at **~175 A** (A ≈ **−75 A**, B ≈ **−100 A**). That is only ~0.27 C / ~0.36 C on 280 Ah, but it is this house’s practical maximum, and on the LFP plateau it is plenty to rank IR.

### Bank A at −75 A, SoC 42 % (47 hard ticks, I ≤ −50 A)

Median residual vs bank median:

| Cell | r_med | Approx extra R |
| --- | --- | --- |
| **A08** | **−94 mV** (worst tick **−127 mV**, **2.999 V**) | ~1.6 mΩ |
| **A07** | **−77 mV** | ~1.3 mΩ |
| A10 | −50 mV | ~0.9 mΩ |
| A03 | −47 mV | ~0.8 mΩ |
| A15 | −42 mV | ~0.7 mΩ |
| **A06** | **−10 mV** | ~0.2 mΩ — boring at this SoC |
| A01 / A02 / A12 / A13 / A16 | **+50…+65 mV** | low IR |

Pack spread on the worst A tick: **216 mV**. **A08**, not A06, is the cell that will drag the BMS into low-voltage protection on a house peak while the string is still ~40 % SoC.

### Bank B at −100 A, SoC ~30 % (47 hard ticks, I ≤ −80 A)

| Cell | r_med | Approx extra R |
| --- | --- | --- |
| **B14 / B08 / B05 / B12** | **~−112…−117 mV** | ~1.2 mΩ each |
| B04 | −62 mV | milder |
| **B10** | **+18 mV** | *low* IR (matches the charge-end-only story) |

Pack spread on the worst B tick: **189 mV**. B took more of the 170 A than A: A sagged harder (A08), so the parallel bus pulled more from B.

**100 A / 170 A can rank connections. It cannot measure remaining amp-hours.** A06 stays quiet until the pack is actually empty.

---

## Replacement (dropped)

Buying four new cells from the same supplier is a **poor cost-to-benefit** bet with the data we have. A06 is the only proven empty-first cell; B10 is a mild fill-first; A07/A08 (and B05/B08/B12/B14) look like **resistance**, quite possibly busbars. Four new cells plus matching and teardown does not pay.

If one *had* to name four: **A06, A07, A08, B10**, and only after proving A07–A08 are not a joint. We are **not** doing that.

Regrouping all 32 into “16 best / 16 worst” is logically cleaner (A06 currently caps the other 15 in string A) but is still a full teardown of both packs. Not doing that either. For the record, the intended split was:

- **Strong:** A01, A02, A05, A12, A13, A16, B01, B02, B03, B06, B07, B09, B11, B13, B15, B16
- **Weak:** A03, A04, A06, A07, A08, A09, A10, A11, A14, A15, B04, B05, B08, B10, B12, B14

B10 goes in the weak set despite excellent IR (fill-first). A02 goes in the strong set (second-lowest at empty only because of A06).

---

## Busbar torque (pending)

Bank A is accessible. Plan: re-torque **tomorrow in daylight**. **Not done yet.**

Manufacturer gave no torque spec. These are 280 Ah-class prismatic cells; the pole is almost always an **aluminium M6** insert. EVE LF280K (173.7 × 71.7 × 207.2 mm) lists **max torque on the terminal = 6 N·m**. Same 6 N·m shows up on several 280 Ah sheets.

| | |
| --- | --- |
| Working torque | **6 N·m** (53 lbf·in), dry threads |
| Hard stop | **6 N·m** if the bolt goes *into* aluminium |
| Order | Isolate pack. **A07–A08 first**, then the rest of bank A while the cover is off. |
| Watch | Too-long bolt bottoming in the insert; oil/grease on threads; spun balance lug; a bolt that suddenly goes easy (stripped insert — stop). |

If A08’s **−94 mV** under the next ~75 A discharge collapses after this, it was the joint. If it does not, the milliohms are more likely in the cell (or a cracked pole).

Bank B IR cluster (B05/B08/B12/B14) is the same job if/when that pack is opened; not scheduled.

---

## Next

1. Torque bank A at 6 N·m (A07–A08 first). Record date/time here when done.
2. Re-run `jkbms_cell_health_report.py` and/or catch the next ~170 A house peak; compare A07/A08 residuals to the 22–23 Aug table.
3. Do **not** schedule a 3.00 V deep discharge unless a cell looks genuinely bad after the joints are tight. \(SOH_Q\) stays unavailable until then.
4. No Influx writer, no new Grafana dashboard, no BMS writes, no `battery_controller.py` edits for this work.
