"""Unit tests for LFP cell-health metrics (no Influx)."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jkbms_cell_health_metrics import (
    HealthConfig,
    PackSample,
    analyze_bank,
    cell_id,
    classify_cell,
    consecutive_kendall_tau,
    detect_steps,
    label_regimes,
    load_config,
    ranks,
    residuals_vs_median,
    samples_from_influx_rows,
)

REPO = Path(__file__).resolve().parent.parent
YAML = REPO / "jkbms_cell_health.yaml"

CFG = HealthConfig()
NS = 1_000_000_000
T0 = 1_700_000_000 * NS


def _cells(outlier_index: int | None = None, outlier_v: float = 3.26, base: float = 3.30) -> tuple[float, ...]:
    vals = [base] * 16
    if outlier_index is not None:
        vals[outlier_index] = outlier_v
    return tuple(vals)


def _sample(
    i: int,
    *,
    current: float,
    soc: float = 50.0,
    voltage: float | None = None,
    cells: tuple[float, ...] | None = None,
    bank: str = "a",
    balancing: float = 0.0,
    balance_current: float = 0.0,
    interval_s: int = 30,
) -> PackSample:
    cells = cells if cells is not None else _cells()
    if voltage is None:
        voltage = sum(cells)
    return PackSample(
        ts_ns=T0 + i * interval_s * NS,
        bank=bank,
        serial="serial-a",
        soc=soc,
        voltage=voltage,
        current=current,
        balance_current=balance_current,
        balancing=balancing,
        cells=cells,
        temp1=30.0,
        cell_delta=max(cells) - min(cells),
        nominal_ah=196.0,
        soh=71.0,
        cycles=139.0,
    )


class TestBasics(unittest.TestCase):
    def test_cell_id_zero_padded(self):
        self.assertEqual(cell_id("a", 6), "A06")
        self.assertEqual(cell_id("b", 16), "B16")

    def test_yaml_round_trip(self):
        cfg = load_config(YAML)
        self.assertEqual(cfg.n_cells, 16)
        self.assertAlmostEqual(cfg.v_pack_abs_start, 55.2)
        self.assertAlmostEqual(cfg.v_cell_charge_end, 3.50)
        self.assertAlmostEqual(cfg.i_rest, 0.5)

    def test_median_not_mean(self):
        cells = _cells(5, 3.10, 3.30)
        med, res, _mad = residuals_vs_median(cells)
        self.assertAlmostEqual(med, 3.30)
        self.assertAlmostEqual(res[5], -0.20)
        self.assertAlmostEqual(sum(res) / 16, -0.20 / 16)  # mean is pulled; we don't use it

    def test_ranks_ties_share_min(self):
        cells = (3.2, 3.1, 3.1, 3.3) + (3.25,) * 12
        r = ranks(cells)
        self.assertEqual(r[1], 1)
        self.assertEqual(r[2], 1)


class TestReconstruction(unittest.TestCase):
    def test_inner_join_complete_tick(self):
        t = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)
        pack = {
            "_time": t,
            "bank": "a",
            "serial": "s",
            "soc": 50,
            "voltage": 52.8,
            "current": -1.0,
            "balance_current": 0.0,
            "balancing": 0.0,
        }
        cell = {"_time": t, "bank": "a"}
        for i in range(1, 17):
            cell[f"{i:02d}"] = 3.3
        samples = samples_from_influx_rows([pack], [cell])
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].bank, "a")
        self.assertEqual(len(samples[0].cells), 16)

    def test_missing_cell_06_drops_tick(self):
        t = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)
        pack = {
            "_time": t,
            "bank": "a",
            "serial": "s",
            "soc": 50,
            "voltage": 52.8,
            "current": -1.0,
            "balance_current": 0.0,
            "balancing": 0.0,
        }
        cell = {"_time": t, "bank": "a"}
        for i in range(1, 17):
            if i == 6:
                continue
            cell[f"{i:02d}"] = 3.3
        self.assertEqual(samples_from_influx_rows([pack], [cell]), [])

    def test_missing_pack_current_drops_tick(self):
        t = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)
        pack = {
            "_time": t,
            "bank": "a",
            "serial": "s",
            "soc": 50,
            "voltage": 52.8,
            "balance_current": 0.0,
            "balancing": 0.0,
        }
        cell = {"_time": t, "bank": "a"}
        for i in range(1, 17):
            cell[f"{i:02d}"] = 3.3
        self.assertEqual(samples_from_influx_rows([pack], [cell]), [])


class TestRegimes(unittest.TestCase):
    def test_rest_needs_20_samples(self):
        samples = [_sample(i, current=0.1) for i in range(19)]
        regimes = label_regimes(samples, CFG)
        self.assertTrue(all(r != "rest" for r in regimes))
        samples.append(_sample(19, current=0.1))
        regimes = label_regimes(samples, CFG)
        self.assertEqual(regimes[-1], "rest")
        self.assertNotEqual(regimes[18], "rest")

    def test_gap_resets_rest_timer(self):
        samples = [_sample(i, current=0.1) for i in range(15)]
        samples.append(_sample(15, current=0.1, interval_s=30))
        # 15th sample 120 s after 14th? default interval is 30. Inject a gap via ts.
        gapped = list(samples)
        gapped.append(
            PackSample(
                ts_ns=samples[-1].ts_ns + 120 * NS,
                bank="a",
                serial="serial-a",
                soc=50,
                voltage=52.8,
                current=0.1,
                balance_current=0.0,
                balancing=0.0,
                cells=_cells(),
            )
        )
        for i in range(19):
            gapped.append(
                PackSample(
                    ts_ns=gapped[-1].ts_ns + 30 * NS,
                    bank="a",
                    serial="serial-a",
                    soc=50,
                    voltage=52.8,
                    current=0.1,
                    balance_current=0.0,
                    balancing=0.0,
                    cells=_cells(),
                )
            )
        regimes = label_regimes(gapped, CFG)
        # After the 120 s gap the rest counter restarted; 19 samples after gap is not rest.
        self.assertNotEqual(regimes[16], "rest")

    def test_sync_30a_is_charge_not_charge_end(self):
        rest = [_sample(i, current=0.1) for i in range(20)]
        ce = _sample(
            20,
            current=8.0,
            soc=99.0,
            voltage=55.5,
            cells=_cells(None, base=3.47),
        )
        sync = _sample(
            21,
            current=30.0,
            soc=99.0,
            voltage=56.5,
            cells=_cells(None, base=3.52),
        )
        regimes = label_regimes(rest + [ce, sync], CFG)
        self.assertEqual(regimes[20], "charge_end")
        self.assertEqual(regimes[21], "charge")

    def test_hysteresis_soc_97_stays_charge_end(self):
        rest = [_sample(i, current=0.1) for i in range(20)]
        enter = _sample(20, current=8.0, soc=99.0, voltage=55.5, cells=_cells(None, base=3.50))
        stay = _sample(21, current=8.0, soc=97.0, voltage=55.5, cells=_cells(None, base=3.50))
        exit_s = _sample(22, current=8.0, soc=96.0, voltage=55.5, cells=_cells(None, base=3.49))
        regimes = label_regimes(rest + [enter, stay, exit_s], CFG)
        self.assertEqual(regimes[20], "charge_end")
        self.assertEqual(regimes[21], "charge_end")
        self.assertEqual(regimes[22], "charge")

    def test_ce_to_discharge_uses_i_dsg(self):
        rest = [_sample(i, current=0.1) for i in range(20)]
        enter = _sample(20, current=8.0, soc=99.0, voltage=55.5, cells=_cells(None, base=3.50))
        dsg = _sample(21, current=-2.0, soc=98.0, voltage=55.0, cells=_cells(None, base=3.40))
        regimes = label_regimes(rest + [enter, dsg], CFG)
        self.assertEqual(regimes[-1], "discharge")

    def test_knee_false_until_3v(self):
        samples = [_sample(i, current=-10.0, cells=_cells(None, base=3.20)) for i in range(5)]
        samples.append(_sample(5, current=-10.0, cells=_cells(0, 3.00, 3.20)))
        regimes = label_regimes(samples, CFG)
        self.assertEqual(regimes[0], "discharge")
        self.assertEqual(regimes[-1], "discharge_knee")


class TestClassification(unittest.TestCase):
    def _cls(self, **kwargs):
        defaults = dict(
            r_rest=0.0,
            r_lc=None,
            r_ld=None,
            r_ce=None,
            mad_rest=0.002,
            has_rest=True,
            has_load_charge=False,
            has_load_discharge=False,
            has_charge_end=False,
            has_step_dcir=False,
            persist_ready=True,
            f_max_ce=0.0,
            f_min_ce=0.0,
            cfg=CFG,
        )
        defaults.update(kwargs)
        return classify_cell(**defaults)

    def test_rest_only_not_adc(self):
        name, conf, _flags, also = self._cls(
            r_rest=-0.040,
            r_lc=-0.040,
            r_ld=-0.040,
            has_load_charge=True,
            has_load_discharge=True,
            has_charge_end=False,
        )
        self.assertEqual(name, "soc_imbalance_or_ocv")
        self.assertFalse(also)

    def test_load_only_ir(self):
        name, _c, _f, also = self._cls(
            r_rest=0.0,
            r_lc=0.040,
            r_ld=-0.040,
            has_load_charge=True,
            has_load_discharge=True,
        )
        self.assertEqual(name, "high_dcir_or_connection")
        self.assertTrue(also)

    def test_charge_end_lead(self):
        name, _c, _f, also = self._cls(
            r_rest=0.0,
            has_charge_end=True,
            r_ce=0.070,
            f_max_ce=0.6,
        )
        self.assertEqual(name, "capacity_mismatch")
        self.assertFalse(also)

    def test_constant_adc_requires_ce(self):
        name, _c, _f, _a = self._cls(
            r_rest=-0.040,
            r_lc=-0.040,
            r_ld=-0.040,
            r_ce=-0.040,
            has_load_charge=True,
            has_load_discharge=True,
            has_charge_end=True,
        )
        self.assertEqual(name, "adc_or_wiring")

    def test_fade_plus_ir(self):
        name, _c, _f, also = self._cls(
            r_rest=0.0,
            r_lc=0.040,
            r_ld=-0.040,
            has_load_charge=True,
            has_load_discharge=True,
            has_charge_end=True,
            f_max_ce=0.7,
            r_ce=0.05,
        )
        self.assertEqual(name, "capacity_mismatch")
        self.assertTrue(also)

    def test_no_rest_insufficient(self):
        name, conf, _f, _a = self._cls(has_rest=False, r_rest=None)
        self.assertEqual(name, "insufficient_data")
        self.assertEqual(conf, "none")


class TestAnalyzeAndSteps(unittest.TestCase):
    def test_rest_only_bank_classifies_a06(self):
        samples = []
        for i in range(40):
            samples.append(_sample(i, current=0.1, cells=_cells(5, 3.26, 3.30)))
        for i in range(40, 70):
            samples.append(_sample(i, current=10.0, cells=_cells(5, 3.26, 3.30)))
        for i in range(70, 100):
            samples.append(_sample(i, current=-10.0, cells=_cells(5, 3.26, 3.30)))
        report = analyze_bank(samples, CFG, auto_i_rest=False)
        a06 = report.cells[5]
        self.assertEqual(a06.cell_id, "A06")
        self.assertEqual(a06.class_name, "soc_imbalance_or_ocv")
        self.assertTrue(a06.r_rest_v is not None and a06.r_rest_v < -0.03)
        self.assertEqual(report.cells[0].class_name, "nominal")

    def test_mixed_load_does_not_cancel(self):
        samples = []
        for i in range(40):
            samples.append(_sample(i, current=0.1, cells=_cells()))
        for i in range(40, 70):
            samples.append(_sample(i, current=10.0, cells=_cells(5, 3.34, 3.30)))
        for i in range(70, 100):
            samples.append(_sample(i, current=-10.0, cells=_cells(5, 3.26, 3.30)))
        report = analyze_bank(samples, CFG, auto_i_rest=False)
        self.assertEqual(report.cells[5].class_name, "high_dcir_or_connection")

    def test_dcir_20a_10mV(self):
        samples = []
        for i in range(5):
            samples.append(_sample(i, current=10.0, cells=_cells()))
        # step +20 A, +10 mV on cell 0 → 0.5 mΩ
        high = list(_cells())
        high[0] = 3.31
        samples.append(_sample(5, current=30.0, cells=tuple(high)))
        events = detect_steps(samples, CFG)
        self.assertTrue(events)
        r0 = events[0].r_mohm[0]
        self.assertIsNotNone(r0)
        self.assertAlmostEqual(r0, 0.5, places=3)

    def test_dcir_rejects_small_di_and_balancing(self):
        samples = [
            _sample(0, current=10.0, cells=_cells()),
            _sample(1, current=11.0, cells=_cells(0, 3.31, 3.30)),
        ]
        self.assertEqual(detect_steps(samples, CFG), [])
        samples_b = [
            _sample(0, current=10.0, cells=_cells(), balancing=1.0),
            _sample(1, current=30.0, cells=_cells(0, 3.31, 3.30), balancing=1.0),
        ]
        self.assertEqual(detect_steps(samples_b, CFG), [])

    def test_kendall_identical_and_reversed(self):
        graded = tuple(3.20 + i * 0.01 for i in range(16))
        r = ranks(graded)
        self.assertEqual(r, tuple(range(1, 17)))
        self.assertAlmostEqual(consecutive_kendall_tau([r, r]), 1.0)
        rev = tuple(17 - x for x in r)
        self.assertAlmostEqual(consecutive_kendall_tau([r, rev]), -1.0)

    def test_auto_i_rest_when_idle_above_yaml(self):
        # Overnight-like idle at 0.8 A never satisfies yaml i_rest=0.5.
        jst = timezone(timedelta(hours=9))
        base = datetime(2026, 8, 21, 1, 0, tzinfo=jst)
        idle = []
        for i in range(40):
            s = _sample(i, current=-0.8)
            ts = int((base.timestamp() + i * 30) * 1e9)
            idle.append(
                PackSample(
                    ts_ns=ts,
                    bank=s.bank,
                    serial=s.serial,
                    soc=s.soc,
                    voltage=s.voltage,
                    current=s.current,
                    balance_current=0.0,
                    balancing=0.0,
                    cells=s.cells,
                    temp1=30.0,
                    cell_delta=s.cell_delta,
                )
            )
        report = analyze_bank(idle, CFG, auto_i_rest=True)
        self.assertTrue(report.i_rest_auto_adjusted)
        self.assertGreater(report.i_rest_used, CFG.i_rest)
        self.assertGreater(report.regime_counts["rest"], 0)


if __name__ == "__main__":
    unittest.main()
