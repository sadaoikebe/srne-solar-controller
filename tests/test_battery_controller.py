"""Unit tests for controller SoC feed helpers (no inverter)."""

from __future__ import annotations

import unittest
from datetime import datetime

from battery_controller import (
    CELL_MAX_ABORT_V,
    CELL_MIN_FLOOR_V,
    ChargeMode,
    State,
    adjust_battery_charge,
    determine_next_state,
    interpret_bms_charge_abort,
    interpret_soc,
)


def _bank(
    *,
    ok: bool = True,
    age_s: float = 1.0,
    cell_max: float = 3.33,
    charge_mosfet: object = True,
) -> dict:
    return {
        "ok": ok,
        "age_s": age_s,
        "cell_max": cell_max,
        "charge_mosfet": charge_mosfet,
    }


class TestInterpretSoc(unittest.TestCase):
    def test_ok(self):
        soc, cmin, status = interpret_soc(
            {"soc_pack": 45.9, "cell_min": 3.30, "age_s": 1.2},
            max_age_s=30,
        )
        self.assertEqual(status, "ok")
        self.assertAlmostEqual(soc, 45.9)
        self.assertAlmostEqual(cmin, 3.30)

    def test_missing(self):
        soc, cmin, status = interpret_soc(None)
        self.assertEqual(status, "missing")
        self.assertIsNone(soc)
        self.assertIsNone(cmin)

    def test_stale(self):
        soc, _, status = interpret_soc(
            {"soc_pack": 40.0, "age_s": 90},
            max_age_s=30,
        )
        self.assertEqual(status, "stale")
        self.assertIsNone(soc)

    def test_invalid_without_soc_pack(self):
        _, _, status = interpret_soc({"age_s": 1.0})
        self.assertEqual(status, "invalid")


class TestCellMinFloor(unittest.TestCase):
    def test_blocks_sbu(self):
        state, _, _ = determine_next_state(
            State.SBU,
            estimated_soc=50.0,
            target_soc=18.0,
            battery_voltage=52.0,
            time_period="sbu_fixed",
            daily_charge_current=0.0,
            last_sbu_to_uti_time=None,
            cell_min_v=CELL_MIN_FLOOR_V,
        )
        self.assertEqual(state, State.UTI_STOPPED)

    def test_allows_sbu_above_floor(self):
        state, _, _ = determine_next_state(
            State.SBU,
            estimated_soc=50.0,
            target_soc=18.0,
            battery_voltage=52.0,
            time_period="sbu_fixed",
            daily_charge_current=0.0,
            last_sbu_to_uti_time=datetime.now(),
            cell_min_v=3.20,
        )
        self.assertEqual(state, State.SBU)


class TestInterpretBmsChargeAbort(unittest.TestCase):
    def test_both_on_below_abort(self):
        fresh, abort, reason, cmax = interpret_bms_charge_abort(
            {"banks": {"a": _bank(cell_max=3.40), "b": _bank(cell_max=3.33)}},
        )
        self.assertTrue(fresh)
        self.assertFalse(abort)
        self.assertEqual(reason, "ok")
        self.assertAlmostEqual(cmax, 3.40)

    def test_mosfet_off_aborts(self):
        fresh, abort, reason, cmax = interpret_bms_charge_abort(
            {
                "banks": {
                    "a": _bank(charge_mosfet=False, cell_max=3.40),
                    "b": _bank(cell_max=3.33),
                }
            },
        )
        self.assertTrue(fresh)
        self.assertTrue(abort)
        self.assertEqual(reason, "mosfet_off")
        self.assertAlmostEqual(cmax, 3.40)

    def test_mosfet_zero_aborts(self):
        _, abort, reason, _ = interpret_bms_charge_abort(
            {"banks": {"a": _bank(charge_mosfet=0, cell_max=3.30)}},
        )
        self.assertTrue(abort)
        self.assertEqual(reason, "mosfet_off")

    def test_cell_max_at_abort_voltage(self):
        fresh, abort, reason, cmax = interpret_bms_charge_abort(
            {"banks": {"a": _bank(cell_max=CELL_MAX_ABORT_V), "b": _bank(cell_max=3.40)}},
        )
        self.assertTrue(fresh)
        self.assertTrue(abort)
        self.assertEqual(reason, "cell_max")
        self.assertAlmostEqual(cmax, CELL_MAX_ABORT_V)

    def test_cell_max_just_below_abort(self):
        _, abort, reason, _ = interpret_bms_charge_abort(
            {"banks": {"a": _bank(cell_max=CELL_MAX_ABORT_V - 0.001)}},
        )
        self.assertFalse(abort)
        self.assertEqual(reason, "ok")

    def test_stale_bank_ignored(self):
        fresh, abort, reason, cmax = interpret_bms_charge_abort(
            {
                "banks": {
                    "a": _bank(ok=True, age_s=40.0, cell_max=3.60, charge_mosfet=False),
                    "b": _bank(cell_max=3.33),
                }
            },
        )
        self.assertTrue(fresh)
        self.assertFalse(abort)
        self.assertEqual(reason, "ok")
        self.assertAlmostEqual(cmax, 3.33)

    def test_all_stale_is_not_abort(self):
        fresh, abort, reason, cmax = interpret_bms_charge_abort(
            {"banks": {"a": _bank(age_s=40.0, cell_max=3.60, charge_mosfet=False)}},
        )
        self.assertFalse(fresh)
        self.assertFalse(abort)
        self.assertEqual(reason, "stale")
        self.assertIsNone(cmax)

    def test_failed_bank_ignored(self):
        fresh, abort, reason, _ = interpret_bms_charge_abort(
            {
                "banks": {
                    "a": _bank(ok=False, charge_mosfet=False, cell_max=3.60),
                    "b": _bank(cell_max=3.33),
                }
            },
        )
        self.assertTrue(fresh)
        self.assertFalse(abort)
        self.assertEqual(reason, "ok")

    def test_missing_payload(self):
        fresh, abort, reason, _ = interpret_bms_charge_abort(None)
        self.assertFalse(fresh)
        self.assertFalse(abort)
        self.assertEqual(reason, "missing")

    def test_invalid_payload(self):
        fresh, abort, reason, _ = interpret_bms_charge_abort({"poll_count": 1})
        self.assertFalse(fresh)
        self.assertFalse(abort)
        self.assertEqual(reason, "invalid")

    def test_mosfet_off_beats_cell_max_reason(self):
        _, abort, reason, _ = interpret_bms_charge_abort(
            {"banks": {"a": _bank(charge_mosfet=False, cell_max=3.60)}},
        )
        self.assertTrue(abort)
        self.assertEqual(reason, "mosfet_off")


class TestAdjustBatteryChargeBmsAbort(unittest.TestCase):
    def _charging(self, **kwargs) -> float:
        args = dict(
            battery_soc=50.0,
            load_power=200.0,
            battery_voltage=52.0,
            daily_charge_current=80.0,
            state=State.UTI_CHARGING,
            charge_mode=ChargeMode.NORMAL,
            bms_abort=False,
        )
        args.update(kwargs)
        return adjust_battery_charge(**args)

    def test_abort_zeros_normal_charge(self):
        self.assertGreater(self._charging(bms_abort=False), 0.0)
        self.assertEqual(self._charging(bms_abort=True), 0.0)

    def test_abort_zeros_sync(self):
        self.assertGreater(
            self._charging(charge_mode=ChargeMode.SYNC, bms_abort=False), 0.0
        )
        self.assertEqual(
            self._charging(charge_mode=ChargeMode.SYNC, bms_abort=True), 0.0
        )


if __name__ == "__main__":
    unittest.main()
