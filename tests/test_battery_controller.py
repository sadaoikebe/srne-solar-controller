"""Unit tests for controller SoC feed helpers (no inverter)."""

from __future__ import annotations

import unittest
from datetime import datetime

from battery_controller import (
    CELL_MIN_FLOOR_V,
    State,
    determine_next_state,
    interpret_soc,
)


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


if __name__ == "__main__":
    unittest.main()
