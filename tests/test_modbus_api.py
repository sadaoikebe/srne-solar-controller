"""Unit tests for battery-current latch helpers (no RS485)."""

from __future__ import annotations

import time
import unittest
from datetime import datetime, timezone

from modbus_api import (
    BatteryCurrentLatch,
    decode_growatt_battery,
    decode_powmr_battery,
)


class TestDecodePowmrBattery(unittest.TestCase):
    def test_discharge_matches_controller_sign(self):
        # Raw +13 → Influx +1.3 A; controller / latch = -1.3 A (discharging).
        got = decode_powmr_battery({"0x0101": 533, "0x0102": 13})
        self.assertIsNotNone(got)
        assert got is not None
        self.assertAlmostEqual(got["voltage_v"], 53.3)
        self.assertAlmostEqual(got["current_a"], -1.3)

    def test_charge_uses_signed_register(self):
        # uint16 0xFFF6 = -10 → register -1.0 A → latch +1.0 A (charging).
        got = decode_powmr_battery({"0x0101": 520, "0x0102": 0xFFF6})
        self.assertIsNotNone(got)
        assert got is not None
        self.assertAlmostEqual(got["current_a"], 1.0)

    def test_missing_keys(self):
        self.assertIsNone(decode_powmr_battery({"0x0100": 24}))


class TestDecodeGrowattBattery(unittest.TestCase):
    def test_charge_and_draw(self):
        got = decode_growatt_battery({"17": 5354, "83": 363, "84": 0})
        self.assertIsNotNone(got)
        assert got is not None
        self.assertAlmostEqual(got["voltage_v"], 53.54)
        self.assertAlmostEqual(got["charge_current_a"], 36.3)
        self.assertAlmostEqual(got["draw_current_a"], 0.0)

    def test_voltage_optional(self):
        got = decode_growatt_battery({"83": 0, "84": 4})
        self.assertIsNotNone(got)
        assert got is not None
        self.assertNotIn("voltage_v", got)
        self.assertAlmostEqual(got["draw_current_a"], 0.4)


class TestBatteryCurrentLatch(unittest.TestCase):
    def test_empty_snapshot(self):
        latch = BatteryCurrentLatch()
        snap = latch.snapshot()
        self.assertIsNone(snap["powmr"])
        self.assertIsNone(snap["growatt"])
        self.assertIsNone(snap["pack_current_a"])

    def test_pack_current_and_ages(self):
        latch = BatteryCurrentLatch()
        t0 = datetime(2026, 8, 26, tzinfo=timezone.utc)
        latch.update_powmr({"voltage_v": 52.0, "current_a": -1.3}, when=t0)
        time.sleep(0.02)
        latch.update_growatt(
            {"voltage_v": 52.1, "charge_current_a": 0.0, "draw_current_a": 0.4},
            when=t0,
        )
        snap = latch.snapshot()
        self.assertAlmostEqual(snap["pack_current_a"], -1.7)
        self.assertGreater(snap["powmr"]["age_s"], snap["growatt"]["age_s"])
        self.assertIn("T", snap["powmr"]["sampled_at"])


if __name__ == "__main__":
    unittest.main()
