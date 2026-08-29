"""Unit tests for controller SoC feed helpers (no inverter)."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from battery_controller import (
    CALIBRATE_RESERVE_S,
    IDLE_SOC_PCT_PER_H,
    CELL_CALIBRATE_ABORT_V,
    CELL_CALIBRATE_V,
    CELL_KNEE_V,
    CELL_MAX_ABORT_V,
    CELL_MIN_FLOOR_V,
    CELL_R_MOHM,
    CELL_SOAK_V,
    PACK_ABORT_V,
    PACK_CALIBRATE_ABORT_V,
    CC_MAX_CURRENT,
    CHARGE_MODE_CODE,
    CONTROLLER_STATE_CODE,
    ChargeMode,
    FullChargeState,
    PACK_KNEE_V,
    SOAK_MIN_DURATION_S,
    State,
    adjust_battery_charge,
    advance_full_charge,
    banks_at_full,
    build_charge_control_records,
    cell_cv_current,
    cheap_end_full_charge_action,
    cheap_hold_soc,
    determine_next_state,
    format_charge_tick,
    hot_cell_label,
    interpret_bms_charge_abort,
    interpret_soc,
    ir_free_cell_max,
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
    cells: list[float] | None = None,
    balance_current: float = 0.0,
    current: float | None = None,
) -> dict:
    delta = cell_delta if cell_delta is not None else cell_max - cell_min
    sample = {
        "ok": ok,
        "age_s": age_s,
        "cell_max": cell_max,
        "cell_min": cell_min,
        "cell_delta": delta,
        "charge_mosfet": charge_mosfet,
        "cells": cells,
        "balance_current": balance_current,
    }
    if current is not None:
        sample["current"] = current
    return sample


def _bms(*banks: tuple[str, dict]) -> dict:
    return {"banks": {name: sample for name, sample in banks}}


def _cells(hot_idx0: int, hot_v: float, rest: float = 3.33, n: int = 16) -> list[float]:
    out = [rest] * n
    out[hot_idx0] = hot_v
    return out


def _view(**kwargs):
    payload = _bms(
        ("a", _bank(
            cell_max=kwargs.pop("a_max", 3.33),
            cell_min=kwargs.pop("a_min", 3.30),
            charge_mosfet=kwargs.pop("a_mosfet", True),
            cells=kwargs.pop("a_cells", None),
            current=kwargs.pop("a_current", None),
        )),
        ("b", _bank(
            cell_max=kwargs.pop("b_max", 3.32),
            cell_min=kwargs.pop("b_min", 3.30),
            charge_mosfet=kwargs.pop("b_mosfet", True),
            cells=kwargs.pop("b_cells", None),
            current=kwargs.pop("b_current", None),
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


class TestCheapHold(unittest.TestCase):
    def _next(
        self, state, soc, *, seconds_left, daily=0.0, target=18.0,
        full_charge_done_today=False,
    ):
        nxt, daily_out, _ = determine_next_state(
            state,
            estimated_soc=soc,
            target_soc=target,
            battery_voltage=52.0,
            time_period="cheap",
            daily_charge_current=daily,
            last_sbu_to_uti_time=None,
            seconds_left_cheap=seconds_left,
            full_charge_done_today=full_charge_done_today,
        )
        return nxt, daily_out

    def test_hold_at_2312_is_about_20(self):
        # 23:12 → 06:58 = 7 h 46 min
        left = 7 * 3600 + 46 * 60
        hold = cheap_hold_soc(18.0, left)
        self.assertAlmostEqual(hold, 18.0 + IDLE_SOC_PCT_PER_H * left / 3600.0)
        self.assertAlmostEqual(hold, 19.94, places=2)

    def test_sbu_at_20pct_2312_stops(self):
        left = 7 * 3600 + 46 * 60
        self.assertEqual(
            self._next(State.SBU, 20.0, seconds_left=left)[0],
            State.UTI_STOPPED,
        )

    def test_sbu_at_20pct_0651_stays_sbu(self):
        left = 7 * 60
        self.assertEqual(
            self._next(State.SBU, 20.0, seconds_left=left)[0],
            State.SBU,
        )

    def test_sbu_at_18pct_0651_stops(self):
        left = 7 * 60
        self.assertEqual(
            self._next(State.SBU, 18.2, seconds_left=left)[0],
            State.UTI_STOPPED,
        )

    def test_below_hold_goes_to_charging(self):
        left = 8 * 3600
        for daily in (0.0, 40.0):
            self.assertEqual(
                self._next(State.SBU, 16.0, seconds_left=left, daily=daily)[0],
                State.UTI_CHARGING,
            )
            self.assertEqual(
                self._next(State.UTI_STOPPED, 16.0, seconds_left=left, daily=daily)[0],
                State.UTI_CHARGING,
            )
            self.assertEqual(
                self._next(State.UTI_CHARGING, 16.0, seconds_left=left, daily=daily)[0],
                State.UTI_CHARGING,
            )

    def test_charging_stops_at_hold_not_raw_target(self):
        # Last night: target 54 %, ~5 h left, SoC 54.4 % must keep filling.
        left = 5 * 3600
        self.assertEqual(
            self._next(
                State.UTI_CHARGING, 54.4, seconds_left=left, daily=10.0, target=54.0,
            )[0],
            State.UTI_CHARGING,
        )

    def test_charging_parks_at_hold_does_not_sbu(self):
        left = 8 * 3600
        hold = cheap_hold_soc(18.0, left)
        self.assertEqual(
            self._next(State.UTI_CHARGING, hold + 0.5, seconds_left=left)[0],
            State.UTI_STOPPED,
        )

    def test_full_charge_done_today_does_not_sbu(self):
        left = 2 * 3600
        self.assertEqual(
            self._next(
                State.UTI_STOPPED, 99.0, seconds_left=left,
                target=54.0, full_charge_done_today=True,
            )[0],
            State.UTI_STOPPED,
        )
        self.assertEqual(
            self._next(
                State.SBU, 99.0, seconds_left=left,
                target=54.0, full_charge_done_today=True,
            )[0],
            State.UTI_STOPPED,
        )

    def test_idle_matched_to_hold_does_not_restart(self):
        # Charged to hold+0.4 with 5 h left, then 3.2 h of 0.25 %/h idle.
        soc = cheap_hold_soc(54.0, 5 * 3600) + 0.4 - IDLE_SOC_PCT_PER_H * 3.2
        left = (5 - 3.2) * 3600
        self.assertEqual(
            self._next(
                State.UTI_STOPPED, soc, seconds_left=left, daily=10.0, target=54.0,
            )[0],
            State.UTI_STOPPED,
        )

    def test_small_dip_after_park_does_not_restart(self):
        # Last night 05:07: ~1 % below hold must not start another fill.
        left = 1 * 3600 + 51 * 60
        hold = cheap_hold_soc(54.0, left)
        self.assertGreater(53.6, hold - 2.0)
        self.assertEqual(
            self._next(
                State.UTI_STOPPED, 53.6, seconds_left=left, daily=70.0, target=54.0,
            )[0],
            State.UTI_STOPPED,
        )

    def test_does_not_mutate_daily_current(self):
        left = 8 * 3600
        hold = cheap_hold_soc(18.0, left)
        _, daily = self._next(
            State.UTI_CHARGING, hold + 0.5, seconds_left=left, daily=40.0,
        )
        self.assertEqual(daily, 40.0)


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

    def test_uti_charging_at_zero_daily_is_zero_amps(self):
        self.assertEqual(self._charging(daily_charge_current=0.0), 0.0)

    def test_normal_applies_soc_table(self):
        i = self._charging(
            battery_soc=90.0,
            daily_charge_current=80.0,
            cell_max=3.33,
            i_prev=80.0,
        )
        self.assertEqual(i, 60.0)

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

    def test_cc_ignores_soc_taper_until_knee(self):
        i = self._charging(
            charge_mode=ChargeMode.CC,
            daily_charge_current=0.0,
            cell_max=3.33,
            i_prev=120.0,
            battery_soc=90.0,
            battery_voltage=52.0,
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

    def test_pack_v_table_caps_when_cell_not_abort(self):
        i = self._charging(
            battery_soc=50.0,
            daily_charge_current=120.0,
            battery_voltage=55.5,
            cell_max=3.40,
            i_prev=120.0,
            charge_mode=ChargeMode.CC,
        )
        self.assertEqual(i, 80.0)

    def test_hot_cell_aborts_despite_low_pack_v(self):
        i = self._charging(
            charge_mode=ChargeMode.CC,
            daily_charge_current=120.0,
            battery_voltage=54.0,
            cell_max=CELL_MAX_ABORT_V,
            i_prev=120.0,
            battery_soc=50.0,
        )
        self.assertEqual(i, 0.0)

    def test_soak_uses_loaded_hottest_cell_table(self):
        # Loaded 3.46 V → 80 A even if pack is still 54 V.
        i_80 = self._charging(
            charge_mode=ChargeMode.SOAK,
            daily_charge_current=0.0,
            battery_voltage=54.0,
            cell_max=3.46,
            cell_min=3.30,
            battery_soc=40.0,
        )
        i_30 = self._charging(
            charge_mode=ChargeMode.SOAK,
            daily_charge_current=0.0,
            battery_voltage=54.0,
            cell_max=3.50,
            cell_min=3.40,
            battery_soc=95.0,
        )
        i_24 = self._charging(
            charge_mode=ChargeMode.SOAK,
            daily_charge_current=0.0,
            battery_voltage=54.0,
            cell_max=3.52,
            cell_min=3.48,
            battery_soc=99.0,
        )
        self.assertEqual(i_80, 80.0)
        self.assertEqual(i_30, 30.0)
        self.assertEqual(i_24, 24.0)

    def test_soak_follows_table_at_loaded_355(self):
        i = self._charging(
            charge_mode=ChargeMode.SOAK,
            daily_charge_current=0.0,
            battery_voltage=56.8,
            cell_max=3.55,
            battery_soc=99.0,
        )
        self.assertEqual(i, 7.0)

    def test_soak_3a_at_loaded_359(self):
        i = self._charging(
            charge_mode=ChargeMode.SOAK,
            daily_charge_current=0.0,
            battery_voltage=57.0,
            cell_max=CELL_CALIBRATE_V,
            battery_soc=99.0,
        )
        self.assertEqual(i, 3.0)

    def test_soak_aborts_at_362(self):
        i = self._charging(
            charge_mode=ChargeMode.SOAK,
            daily_charge_current=0.0,
            battery_voltage=57.0,
            cell_max=CELL_CALIBRATE_ABORT_V,
            cell_max_eff=3.55,
            battery_soc=99.0,
        )
        self.assertEqual(i, 0.0)

    def test_calibrate_10a_at_355_despite_pack_568(self):
        i = self._charging(
            charge_mode=ChargeMode.CALIBRATE,
            daily_charge_current=120.0,
            battery_voltage=PACK_ABORT_V,
            cell_max=3.55,
            cell_max_eff=3.55,
            battery_soc=99.0,
        )
        self.assertEqual(i, 10.0)

    def test_calibrate_pack_abort_at_579(self):
        i = self._charging(
            charge_mode=ChargeMode.CALIBRATE,
            daily_charge_current=120.0,
            battery_voltage=PACK_CALIBRATE_ABORT_V,
            cell_max=3.58,
            battery_soc=99.0,
        )
        self.assertEqual(i, 0.0)

    def test_cc_still_pack_aborts_at_568(self):
        i = self._charging(
            charge_mode=ChargeMode.CC,
            daily_charge_current=120.0,
            battery_voltage=PACK_ABORT_V,
            cell_max=3.40,
            battery_soc=50.0,
        )
        self.assertEqual(i, 0.0)

    def test_normal_120a_while_high_r_cells_loaded_345(self):
        # B05/B08/B12/B14 at 3.46 V loaded, ~61 A, pack 53.9 V, SoC 39 %.
        i_b = 61.4
        loaded = 3.46
        for idx in (4, 7, 11, 13):
            r = CELL_R_MOHM["b"][idx] * 1e-3
            eff = loaded - i_b * r
            self.assertLess(eff, CELL_KNEE_V, msg=f"B{idx+1:02d}")
            i = self._charging(
                battery_soc=39.0,
                daily_charge_current=120.0,
                battery_voltage=53.9,
                cell_max=loaded,
                cell_max_eff=eff,
                charge_mode=ChargeMode.NORMAL,
            )
            self.assertEqual(i, 120.0, msg=f"B{idx+1:02d}")


class TestCellCvCurrent(unittest.TestCase):
    def test_below_knee_goes_to_cc(self):
        i = cell_cv_current(
            cell_max=3.33, v_set=CELL_SOAK_V, i_prev=80.0, i_cc=80.0,
        )
        self.assertEqual(i, 80.0)

    def test_from_zero_writes_table_value(self):
        i = cell_cv_current(
            cell_max=3.33, v_set=CELL_SOAK_V, i_prev=0.0, i_cc=80.0,
        )
        self.assertEqual(i, 80.0)

    def test_below_abort_does_not_cap(self):
        i = cell_cv_current(
            cell_max=CELL_SOAK_V, v_set=CELL_SOAK_V, i_prev=20.0, i_cc=120.0,
        )
        self.assertEqual(i, 120.0)

    def test_abort_voltage_zeros(self):
        i = cell_cv_current(
            cell_max=CELL_MAX_ABORT_V, v_set=CELL_SOAK_V, i_prev=40.0, i_cc=120.0,
        )
        self.assertEqual(i, 0.0)

    def test_dvdt_does_not_slam_to_zero(self):
        i = cell_cv_current(
            cell_max=3.50,
            v_set=CELL_SOAK_V,
            i_prev=20.0,
            i_cc=120.0,
            dv_dt=0.01,
        )
        self.assertEqual(i, 120.0)

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

    def test_cc_enters_soak_at_pack_knee(self):
        nxt = advance_full_charge(
            FullChargeState(mode=ChargeMode.CC),
            now=self._now(),
            bms=_view(a_max=3.33, b_max=3.33, a_min=3.30, b_min=3.30),
            pack_v=PACK_KNEE_V,
            pack_charge_a=80.0,
            soc_payload=None,
            seconds_left=4 * 3600,
        )
        self.assertEqual(nxt.mode, ChargeMode.SOAK)
        self.assertIsNotNone(nxt.soak_started_at)

    def test_cc_does_not_end_on_loaded_high_r_ir(self):
        # Whichever of B05/B08/B12/B14 is loaded-hottest at 3.46 V / 61 A,
        # pack 53.9 V — still CC. Same IR class, rotating identity.
        for idx in (4, 7, 11, 13):
            b_cells = _cells(idx, 3.46)
            view = _view(
                a_max=3.40, b_max=3.46, a_min=3.30, b_min=3.33,
                b_cells=b_cells, b_current=61.4,
                a_cells=_cells(7, 3.40), a_current=35.0,
            )
            nxt = advance_full_charge(
                FullChargeState(mode=ChargeMode.CC),
                now=self._now(),
                bms=view,
                pack_v=53.9,
                pack_charge_a=100.0,
                soc_payload=None,
                seconds_left=4 * 3600,
            )
            self.assertEqual(nxt.mode, ChargeMode.CC, msg=f"B{idx+1:02d}")
            self.assertIsNotNone(view.cell_max_ir_free, msg=f"B{idx+1:02d}")
            self.assertLess(view.cell_max_ir_free, CELL_KNEE_V, msg=f"B{idx+1:02d}")

    def test_ir_free_max_is_all_cells_not_loaded_hottest(self):
        cells = [3.33] * 16
        cells[4] = 3.43   # B05
        cells[7] = 3.46   # B08 loaded hottest
        cells[11] = 3.45  # B12
        cells[13] = 3.45  # B14
        cells[0] = 3.34   # B01 low R
        ve = ir_free_cell_max({"b": cells}, {"b": 61.4})
        self.assertIsNotNone(ve)
        self.assertLess(ve, CELL_KNEE_V)
        # Low-R B01 IR-free can exceed a high-R cell's IR-free.
        ve_b01 = 3.34 - 61.4 * CELL_R_MOHM["b"][0] * 1e-3
        ve_b08 = 3.46 - 61.4 * CELL_R_MOHM["b"][7] * 1e-3
        self.assertAlmostEqual(ve, max(ve_b01, ve_b08, 3.43 - 61.4 * CELL_R_MOHM["b"][4] * 1e-3,
                                      3.45 - 61.4 * CELL_R_MOHM["b"][11] * 1e-3,
                                      3.45 - 61.4 * CELL_R_MOHM["b"][13] * 1e-3),
                               places=4)

    def test_cc_enters_soak_on_ir_free_knee(self):
        # Resting / low-I 3.45 V is a real knee.
        nxt = advance_full_charge(
            FullChargeState(mode=ChargeMode.CC),
            now=self._now(),
            bms=_view(
                a_max=3.45, b_max=3.40, a_min=3.40, b_min=3.38,
                a_cells=_cells(0, 3.45, rest=3.40), a_current=0.0,
                b_cells=_cells(0, 3.40), b_current=0.0,
            ),
            pack_v=54.0,
            pack_charge_a=0.0,
            soc_payload=None,
            seconds_left=4 * 3600,
        )
        self.assertEqual(nxt.mode, ChargeMode.SOAK)

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

    def test_soak_holds_until_cheap_reserve(self):
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
        self.assertEqual(nxt.mode, ChargeMode.SOAK)

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

    def test_cheap_end_completes_only_when_full(self):
        full = _view(a_max=3.59, b_max=3.595, a_min=3.50, b_min=3.50)
        not_full = _view(a_max=3.55, b_max=3.55, a_min=3.50, b_min=3.50)
        self.assertEqual(
            cheap_end_full_charge_action(ChargeMode.CALIBRATE, full, None),
            "complete",
        )
        self.assertEqual(
            cheap_end_full_charge_action(ChargeMode.SOAK, not_full, None),
            "abandon",
        )
        self.assertEqual(
            cheap_end_full_charge_action(ChargeMode.CC, full, None),
            "abort",
        )


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


class TestChargeObservability(unittest.TestCase):
    def test_hot_cell_is_one_based(self):
        cells = [3.30] * 16
        cells[5] = 3.51  # A06
        view = parse_bms_view(
            _bms(
                ("a", _bank(cell_max=3.51, cell_min=3.30, cells=cells, balance_current=2.0)),
                ("b", _bank(cell_max=3.40, cell_min=3.38, cells=[3.39] * 16)),
            )
        )
        self.assertEqual(view.hot_bank, "a")
        self.assertEqual(view.hot_cell, 6)
        self.assertEqual(hot_cell_label(view), "a06")
        self.assertAlmostEqual(view.balance_current, 2.0)

    def test_log_line_includes_phase_and_hot_cell(self):
        cells = [3.30] * 16
        cells[5] = 3.50
        view = parse_bms_view(
            _bms(("a", _bank(cell_max=3.50, cell_min=3.45, cells=cells, balance_current=1.8)))
        )
        line = format_charge_tick(
            mode=ChargeMode.SOAK, i_cmd=24.0, i_pack=22.5, bms=view, abort=False,
        )
        self.assertIn("mode=SOAK", line)
        self.assertIn("I_cmd=24 A", line)
        self.assertIn("a06", line)
        self.assertIn("MOSFET=on", line)
        self.assertIn("abort=no", line)

    def test_influx_records_include_mode_and_i_cmd(self):
        view = parse_bms_view(
            _bms(
                ("a", _bank(cell_max=3.50, cell_min=3.45, balance_current=1.5)),
                ("b", _bank(cell_max=3.48, cell_min=3.46)),
            )
        )
        recs = build_charge_control_records(
            mode=ChargeMode.SOAK,
            state=State.UTI_CHARGING,
            i_cmd=20.0,
            i_pack=18.0,
            bms=view,
            abort=False,
        )
        by = {(r.bank, r.name): r for r in recs}
        self.assertEqual(by[("pack", "charge_mode")].value, CHARGE_MODE_CODE[ChargeMode.SOAK])
        self.assertEqual(
            by[("pack", "controller_state")].value,
            CONTROLLER_STATE_CODE[State.UTI_CHARGING],
        )
        self.assertEqual(by[("pack", "i_cmd")].value, 20.0)
        self.assertEqual(by[("pack", "i_pack")].value, 18.0)
        self.assertEqual(by[("pack", "bms_abort")].value, 0.0)
        self.assertEqual(by[("pack", "charge_mosfet")].value, 1.0)
        self.assertIn(("a", "balance_current"), by)
        self.assertIn(("pack", "cell_max"), by)


if __name__ == "__main__":
    unittest.main()
