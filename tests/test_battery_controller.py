"""Unit tests for controller SoC feed helpers (no inverter)."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from battery_controller import (
    CALIBRATE_RESERVE_S,
    CELL_CALIBRATE_ABORT_V,
    CELL_CALIBRATE_V,
    CELL_KNEE_V,
    CELL_MAX_ABORT_V,
    CELL_MIN_FLOOR_V,
    CELL_SOAK_V,
    CC_MAX_CURRENT,
    ChargeMode,
    FullChargeState,
    SOAK_MIN_DURATION_S,
    State,
    adjust_battery_charge,
    advance_full_charge,
    banks_at_full,
    cell_cv_current,
    determine_next_state,
    interpret_bms_charge_abort,
    interpret_soc,
    parse_bms_view,
    seconds_until_cheap_end,
)


def _bank(
    *,
    ok: bool = True,
    age_s: float = 1.0,
    cell_max: float = 3.33,
    cell_min: float = 3.30,
    cell_delta: float | None = None,
    charge_mosfet: object = True,
) -> dict:
    delta = cell_delta if cell_delta is not None else cell_max - cell_min
    return {
        "ok": ok,
        "age_s": age_s,
        "cell_max": cell_max,
        "cell_min": cell_min,
        "cell_delta": delta,
        "charge_mosfet": charge_mosfet,
    }


def _bms(*banks: tuple[str, dict]) -> dict:
    return {"banks": {name: sample for name, sample in banks}}


def _view(**kwargs):
    payload = _bms(
        ("a", _bank(
            cell_max=kwargs.pop("a_max", 3.33),
            cell_min=kwargs.pop("a_min", 3.30),
            charge_mosfet=kwargs.pop("a_mosfet", True),
        )),
        ("b", _bank(
            cell_max=kwargs.pop("b_max", 3.32),
            cell_min=kwargs.pop("b_min", 3.30),
            charge_mosfet=kwargs.pop("b_mosfet", True),
        )),
    )
    return parse_bms_view(payload, **kwargs)


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

    def test_abort_zeros_cc(self):
        self.assertGreater(
            self._charging(charge_mode=ChargeMode.CC, bms_abort=False), 0.0
        )
        self.assertEqual(
            self._charging(charge_mode=ChargeMode.CC, bms_abort=True), 0.0
        )

    def test_normal_ignores_soc_taper_below_knee(self):
        # Old SOC_LIMITS would cap 90% SoC at 70 A. CC uses daily_charge_current.
        i = self._charging(
            battery_soc=90.0,
            daily_charge_current=80.0,
            cell_max=3.33,
            i_prev=80.0,
        )
        self.assertEqual(i, 80.0)

    def test_normal_tapers_at_cell_knee(self):
        i = self._charging(
            battery_soc=90.0,
            daily_charge_current=80.0,
            cell_max=CELL_SOAK_V,
            i_prev=80.0,
        )
        self.assertLess(i, 80.0)
        self.assertGreaterEqual(i, 0.0)

    def test_cc_uses_hardware_cap_not_daily_zero(self):
        i = self._charging(
            charge_mode=ChargeMode.CC,
            daily_charge_current=0.0,
            cell_max=3.33,
            i_prev=120.0,
            battery_soc=50.0,
        )
        self.assertEqual(i, 120.0)

    def test_calibrate_caps_at_10a(self):
        i = self._charging(
            charge_mode=ChargeMode.CALIBRATE,
            daily_charge_current=120.0,
            cell_max=3.50,
            i_prev=10.0,
        )
        self.assertLessEqual(i, 10.0)
        self.assertGreater(i, 0.0)


class TestCellCvCurrent(unittest.TestCase):
    def test_below_knee_goes_to_cc(self):
        i = cell_cv_current(
            cell_max=3.33, v_set=CELL_SOAK_V, i_prev=80.0, i_cc=80.0,
        )
        self.assertEqual(i, 80.0)

    def test_slew_up_from_zero(self):
        i = cell_cv_current(
            cell_max=3.33, v_set=CELL_SOAK_V, i_prev=0.0, i_cc=80.0,
        )
        self.assertEqual(i, 10.0)

    def test_at_soak_setpoint_holds_low(self):
        i = cell_cv_current(
            cell_max=CELL_SOAK_V, v_set=CELL_SOAK_V, i_prev=20.0, i_cc=120.0,
        )
        self.assertLessEqual(i, 20.0)

    def test_abort_voltage_zeros(self):
        i = cell_cv_current(
            cell_max=CELL_MAX_ABORT_V, v_set=CELL_SOAK_V, i_prev=40.0, i_cc=120.0,
        )
        self.assertEqual(i, 0.0)

    def test_dvdt_projects_abort(self):
        i = cell_cv_current(
            cell_max=3.50,
            v_set=CELL_SOAK_V,
            i_prev=40.0,
            i_cc=120.0,
            dv_dt=0.01,  # 10 mV/s → 3.55 in 5 s
        )
        self.assertEqual(i, 0.0)

    def test_missing_cell_max_is_none(self):
        self.assertIsNone(
            cell_cv_current(cell_max=None, v_set=CELL_SOAK_V, i_prev=40.0, i_cc=80.0)
        )

    def test_calibrate_abort_allows_359(self):
        i = cell_cv_current(
            cell_max=3.59,
            v_set=CELL_CALIBRATE_V,
            i_prev=10.0,
            i_cc=10.0,
            cell_abort_v=CELL_CALIBRATE_ABORT_V,
        )
        self.assertGreater(i, 0.0)


class TestAdvanceFullCharge(unittest.TestCase):
    def _now(self) -> datetime:
        return datetime(2026, 8, 26, 2, 0, 0)

    def test_normal_becomes_cc(self):
        nxt = advance_full_charge(
            FullChargeState(),
            now=self._now(),
            bms=_view(a_max=3.33, b_max=3.32),
            pack_v=52.0,
            pack_charge_a=40.0,
            soc_payload=None,
            seconds_left=4 * 3600,
        )
        self.assertEqual(nxt.mode, ChargeMode.CC)
        self.assertFalse(nxt.complete)

    def test_cc_enters_soak_at_knee(self):
        nxt = advance_full_charge(
            FullChargeState(mode=ChargeMode.CC),
            now=self._now(),
            bms=_view(a_max=CELL_KNEE_V, b_max=3.40, a_min=3.40, b_min=3.38),
            pack_v=55.0,
            pack_charge_a=80.0,
            soc_payload=None,
            seconds_left=4 * 3600,
        )
        self.assertEqual(nxt.mode, ChargeMode.SOAK)
        self.assertIsNotNone(nxt.soak_started_at)

    def test_soak_waits_for_min_duration(self):
        started = self._now()
        nxt = advance_full_charge(
            FullChargeState(mode=ChargeMode.SOAK, soak_started_at=started),
            now=started + timedelta(minutes=10),
            bms=_view(a_max=3.50, b_max=3.49, a_min=3.46, b_min=3.45),
            pack_v=56.0,
            pack_charge_a=10.0,
            soc_payload=None,
            seconds_left=4 * 3600,
        )
        self.assertEqual(nxt.mode, ChargeMode.SOAK)

    def test_soak_advances_after_min_duration_and_quality(self):
        started = self._now()
        now = started + timedelta(seconds=SOAK_MIN_DURATION_S + 5)
        nxt = advance_full_charge(
            FullChargeState(
                mode=ChargeMode.SOAK,
                soak_started_at=started,
                tail_ok_since=started,
            ),
            now=now,
            bms=_view(a_max=3.50, b_max=3.50, a_min=3.46, b_min=3.46),
            pack_v=56.0,
            pack_charge_a=8.0,
            soc_payload=None,
            seconds_left=3 * 3600,
        )
        self.assertEqual(nxt.mode, ChargeMode.CALIBRATE)

    def test_soak_advances_when_cheap_window_running_out(self):
        started = self._now()
        nxt = advance_full_charge(
            FullChargeState(mode=ChargeMode.SOAK, soak_started_at=started),
            now=started + timedelta(minutes=5),
            bms=_view(a_max=3.50, b_max=3.49, a_min=3.40, b_min=3.39),
            pack_v=56.0,
            pack_charge_a=40.0,
            soc_payload=None,
            seconds_left=CALIBRATE_RESERVE_S - 10,
        )
        self.assertEqual(nxt.mode, ChargeMode.CALIBRATE)

    def test_calibrate_completes_on_both_cell_max(self):
        nxt = advance_full_charge(
            FullChargeState(mode=ChargeMode.CALIBRATE),
            now=self._now(),
            bms=_view(a_max=3.59, b_max=3.595, a_min=3.50, b_min=3.50),
            pack_v=57.4,
            pack_charge_a=5.0,
            soc_payload=None,
            seconds_left=600,
        )
        self.assertTrue(nxt.complete)
        self.assertEqual(nxt.mode, ChargeMode.NORMAL)

    def test_calibrate_completes_on_remain_est(self):
        bms = _view(a_max=3.50, b_max=3.50, a_min=3.48, b_min=3.48)
        payload = {
            "banks": {
                "a": {"remain_est": 259.0, "usable_ah": 260.0},
                "b": {"remain_est": 279.0, "usable_ah": 280.0},
            }
        }
        nxt = advance_full_charge(
            FullChargeState(mode=ChargeMode.CALIBRATE),
            now=self._now(),
            bms=bms,
            pack_v=56.0,
            pack_charge_a=5.0,
            soc_payload=payload,
            seconds_left=600,
        )
        self.assertTrue(nxt.complete)

    def test_banks_at_full_needs_both(self):
        bms = _view(a_max=3.59, b_max=3.50)
        self.assertFalse(banks_at_full(bms, None))


class TestSecondsUntilCheapEnd(unittest.TestCase):
    def test_overnight(self):
        now = datetime(2026, 8, 26, 0, 30, 0)
        self.assertGreater(seconds_until_cheap_end(now), 6 * 3600)
        self.assertLess(seconds_until_cheap_end(now), 7 * 3600)

    def test_before_midnight(self):
        now = datetime(2026, 8, 26, 23, 30, 0)
        self.assertGreater(seconds_until_cheap_end(now), 7 * 3600)

    def test_daytime_is_zero(self):
        now = datetime(2026, 8, 26, 12, 0, 0)
        self.assertEqual(seconds_until_cheap_end(now), 0.0)


if __name__ == "__main__":
    unittest.main()
