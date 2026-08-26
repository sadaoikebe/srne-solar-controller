"""Unit tests for the shadow SoC estimator (no BLE / Influx / serial)."""

from __future__ import annotations

import unittest

from datetime import datetime, timezone

from soc_estimator import (
    BankSample,
    BankState,
    EstimatorConfig,
    InverterSnapshot,
    ble_fresh,
    build_soc_payload,
    cold_start_remain,
    inverter_fresh,
    remain_moving,
    step,
)


def _cfg() -> EstimatorConfig:
    return EstimatorConfig(
        interval_s=10,
        usable_ah={"a": 260.0, "b": 280.0},
        full_cell_v=3.59,
        empty_cell_v=3.05,
        ble_stale_s=25.0,
        powmr_stale_s=15.0,
        growatt_stale_s=45.0,
        move_fraction=0.25,
        min_expected_ah=0.01,
    )


def _live(
    remain: float,
    current: float,
    *,
    nominal: float = 195.777,
    cell_min: float = 3.30,
    cell_max: float = 3.33,
    soc: float = 20.0,
    age_s: float = 1.0,
) -> BankSample:
    return BankSample(
        ok=True,
        age_s=age_s,
        remain_ah=remain,
        nominal_ah=nominal,
        current=current,
        cell_min=cell_min,
        cell_max=cell_max,
        soc=soc,
    )


def _dead() -> BankSample:
    return BankSample(ok=False, age_s=40.0)


class TestRemainMoving(unittest.TestCase):
    def test_rest_is_not_stuck(self):
        self.assertTrue(remain_moving(0.0, 0.4, 10.0, move_fraction=0.25, min_expected_ah=0.01))

    def test_charge_freeze_is_stuck(self):
        # 25 A × 10 s = 0.069 Ah expected; remain did not move.
        self.assertFalse(remain_moving(0.0, 25.0, 10.0, move_fraction=0.25, min_expected_ah=0.01))

    def test_charge_delta_tracks(self):
        self.assertTrue(remain_moving(0.07, 25.0, 10.0, move_fraction=0.25, min_expected_ah=0.01))

    def test_discharge_floor_is_stuck(self):
        self.assertFalse(remain_moving(0.0, -20.0, 10.0, move_fraction=0.25, min_expected_ah=0.01))


class TestFreshness(unittest.TestCase):
    def test_ble_stale(self):
        s = _live(100.0, 5.0, age_s=40.0)
        self.assertFalse(ble_fresh(s, 25.0))
        self.assertTrue(ble_fresh(_live(100.0, 5.0, age_s=1.0), 25.0))

    def test_inverter_requires_growatt(self):
        cfg = _cfg()
        self.assertFalse(
            inverter_fresh(
                InverterSnapshot(pack_current_a=10.0, powmr_age_s=1.0, growatt_age_s=None),
                cfg,
            )
        )
        self.assertTrue(
            inverter_fresh(
                InverterSnapshot(pack_current_a=10.0, powmr_age_s=1.0, growatt_age_s=12.0),
                cfg,
            )
        )


class TestColdStart(unittest.TestCase):
    def test_adds_short_tape_offset(self):
        # 42.8 on a 195.8 tape → +64.2 onto 260.
        got = cold_start_remain(42.814, 195.777, 260.0)
        self.assertAlmostEqual(got, 42.814 + (260.0 - 195.777), places=3)


class TestStep(unittest.TestCase):
    def test_track_applies_remain_delta(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=100.0, last_remain_jk=40.0, initialized=True, mode="track"),
            "b": BankState(remain_est=140.0, last_remain_jk=140.0, initialized=True, mode="track"),
        }
        samples = {
            "a": _live(40.07, 25.0, nominal=195.777),
            "b": _live(140.1, 40.0, nominal=280.0),
        }
        r = step(s0, samples, None, cfg=cfg, dt_s=10.0)
        self.assertTrue(r.write)
        self.assertEqual(r.modes["a"], "track")
        self.assertAlmostEqual(r.states["a"].remain_est, 100.07, places=3)

    def test_stuck_remain_coasts_on_jk_current(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=259.0, last_remain_jk=193.8, initialized=True, mode="track"),
            "b": BankState(remain_est=280.0, last_remain_jk=280.0, initialized=True, mode="track"),
        }
        samples = {
            "a": _live(193.8, 25.0, nominal=195.777, cell_max=3.55),
            "b": _live(280.0, 0.4, nominal=280.0, cell_max=3.45),
        }
        r = step(s0, samples, None, cfg=cfg, dt_s=10.0)
        self.assertEqual(r.modes["a"], "coast_jk")
        # 25 A × 10 s / 3600 = 0.0694 Ah
        self.assertAlmostEqual(r.states["a"].remain_est, 259.0 + 25.0 * 10.0 / 3600.0, places=3)
        self.assertEqual(r.modes["b"], "track")  # rest-sized I, remain still

    def test_full_anchor(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=250.0, last_remain_jk=193.8, initialized=True, mode="coast_jk"),
            "b": BankState(remain_est=280.0, last_remain_jk=280.0, initialized=True, mode="track"),
        }
        samples = {
            "a": _live(195.777, 4.0, cell_max=3.60, cell_min=3.50),
            "b": _live(280.0, 1.0, nominal=280.0, cell_max=3.40),
        }
        r = step(s0, samples, None, cfg=cfg, dt_s=10.0)
        self.assertEqual(r.modes["a"], "full_anchor")
        self.assertAlmostEqual(r.states["a"].remain_est, 260.0)
        self.assertAlmostEqual(r.states["a"].last_remain_jk, 195.777)

    def test_empty_anchor(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=5.0, last_remain_jk=0.0, initialized=True, mode="coast_jk"),
            "b": BankState(remain_est=20.0, last_remain_jk=20.0, initialized=True, mode="track"),
        }
        samples = {
            "a": _live(0.0, -10.0, cell_min=3.00, cell_max=3.10),
            "b": _live(20.0, -5.0, nominal=280.0, cell_min=3.20, cell_max=3.25),
        }
        r = step(s0, samples, None, cfg=cfg, dt_s=10.0)
        self.assertEqual(r.modes["a"], "empty_anchor")
        self.assertEqual(r.states["a"].remain_est, 0.0)

    def test_one_bank_ble_down_holds(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=100.0, last_remain_jk=40.0, initialized=True, mode="track"),
            "b": BankState(remain_est=140.0, last_remain_jk=140.0, initialized=True, mode="track"),
        }
        samples = {"a": _dead(), "b": _live(140.1, 10.0, nominal=280.0)}
        inv = InverterSnapshot(pack_current_a=-30.0, powmr_age_s=1.0, growatt_age_s=5.0)
        r = step(s0, samples, inv, cfg=cfg, dt_s=10.0)
        self.assertEqual(r.modes["a"], "held")
        self.assertAlmostEqual(r.states["a"].remain_est, 100.0)
        self.assertTrue(r.write)  # B still measured

    def test_both_ble_down_coasts_inverters(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=100.0, last_remain_jk=40.0, initialized=True, mode="track"),
            "b": BankState(remain_est=140.0, last_remain_jk=140.0, initialized=True, mode="track"),
        }
        samples = {"a": _dead(), "b": _dead()}
        inv = InverterSnapshot(pack_current_a=-36.0, powmr_age_s=1.0, growatt_age_s=8.0)
        shares = {"a": 0.25, "b": 0.75}
        r = step(s0, samples, inv, cfg=cfg, dt_s=10.0, shares=shares)
        self.assertTrue(r.write)
        self.assertEqual(r.modes["a"], "coast_inverters")
        self.assertAlmostEqual(
            r.states["a"].remain_est,
            100.0 + 0.25 * (-36.0) * 10.0 / 3600.0,
            places=4,
        )
        self.assertAlmostEqual(
            r.states["b"].remain_est,
            140.0 + 0.75 * (-36.0) * 10.0 / 3600.0,
            places=4,
        )

    def test_everything_down_skips_write(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=100.0, initialized=True, mode="held"),
            "b": BankState(remain_est=140.0, initialized=True, mode="held"),
        }
        r = step(s0, {"a": _dead(), "b": _dead()}, None, cfg=cfg, dt_s=10.0)
        self.assertFalse(r.write)
        self.assertEqual(r.modes["a"], "held")
        self.assertAlmostEqual(r.states["a"].remain_est, 100.0)

    def test_returning_from_held_does_not_apply_gap_delta(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(
                remain_est=100.0, last_remain_jk=40.0, initialized=True, mode="held",
            ),
            "b": BankState(remain_est=140.0, last_remain_jk=140.0, initialized=True, mode="track"),
        }
        # JK remain jumped 40 → 50 while we were held; must not add +10 Ah.
        samples = {
            "a": _live(50.0, 5.0),
            "b": _live(140.0, 0.2, nominal=280.0),
        }
        r = step(s0, samples, None, cfg=cfg, dt_s=10.0)
        self.assertEqual(r.modes["a"], "track")
        self.assertAlmostEqual(r.states["a"].remain_est, 100.0)
        self.assertAlmostEqual(r.states["a"].last_remain_jk, 50.0)

    def test_soc_pack_uses_usable_ah(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=130.0, last_remain_jk=66.0, initialized=True, mode="track"),
            "b": BankState(remain_est=140.0, last_remain_jk=140.0, initialized=True, mode="track"),
        }
        samples = {
            "a": _live(66.0, 0.2),
            "b": _live(140.0, 0.2, nominal=280.0),
        }
        r = step(s0, samples, None, cfg=cfg, dt_s=10.0)
        self.assertAlmostEqual(r.soc_pack, 100.0 * (130.0 + 140.0) / (260.0 + 280.0), places=4)


class TestSocPayload(unittest.TestCase):
    def test_pack_cell_min_and_source(self):
        cfg = _cfg()
        s0 = {
            "a": BankState(remain_est=130.0, last_remain_jk=66.0, initialized=True, mode="track"),
            "b": BankState(remain_est=140.0, last_remain_jk=140.0, initialized=True, mode="track"),
        }
        samples = {
            "a": _live(66.0, 0.2, cell_min=3.21, cell_max=3.33),
            "b": _live(140.0, 0.2, nominal=280.0, cell_min=3.30, cell_max=3.35),
        }
        r = step(s0, samples, None, cfg=cfg, dt_s=10.0)
        body = build_soc_payload(
            r, samples, cfg,
            sampled_at=datetime(2026, 8, 26, tzinfo=timezone.utc),
            poll_count=3,
        )
        self.assertTrue(body["ok"])
        self.assertAlmostEqual(body["cell_min"], 3.21)
        self.assertAlmostEqual(body["cell_max"], 3.35)
        self.assertIn("soc_pack", body)
        self.assertEqual(body["banks"]["a"]["mode"], "track")
        self.assertEqual(body["poll_count"], 3)


if __name__ == "__main__":
    unittest.main()
