"""Unit tests for jkbms_db_writer transforms (no Influx / no network)."""

from __future__ import annotations

import os
import unittest

# Minimal env so importing the module does not require a live Influx server.
os.environ.setdefault("INFLUX_URL", "http://127.0.0.1:8086")
os.environ.setdefault("INFLUX_TOKEN", "test-token")
os.environ.setdefault("INFLUX_ORG", "test-org")
os.environ.setdefault("INFLUX_BUCKET", "test-bucket")

from jkbms_db_writer import (  # noqa: E402
    PACK_FIELDS,
    transform_bank_to_points,
    transform_snapshot_to_points,
)


def _ok_bank(bank: str = "a", n_cells: int = 16) -> dict:
    return {
        "ok": True,
        "bank": bank,
        "serial": f"serial-{bank}",
        "soc": 50,
        "soh": 99,
        "voltage": 51.2,
        "current": -1.5,
        "power": -76.8,
        "remain_ah": 100.0,
        "nominal_ah": 280.0,
        "cycles": 10,
        "mos_temp": 30.0,
        "temp1": 29.0,
        "temp2": 28.0,
        "balance_current": 0.0,
        "balancing": 0,
        "charge_mosfet": True,
        "discharge_mosfet": True,
        "runtime_s": 3600,
        "cell_count": n_cells,
        "cell_min": 3.2,
        "cell_max": 3.21,
        "cell_delta": 0.01,
        "cells": [3.2 + i * 0.001 for i in range(n_cells)],
        "error": None,
    }


class TestTransform(unittest.TestCase):
    def test_ok_bank_point_count(self):
        pts = transform_bank_to_points(0, "a", _ok_bank("a", 16))
        # All pack fields present + 16 cells
        self.assertEqual(len(pts), len(PACK_FIELDS) + 16)

    def test_failed_bank_skipped(self):
        pts = transform_bank_to_points(
            0, "a", {"ok": False, "bank": "a", "error": "timeout"},
        )
        self.assertEqual(pts, [])

    def test_snapshot_both_banks(self):
        snap = {
            "poll_count": 1,
            "banks": {
                "a": _ok_bank("a", 16),
                "b": _ok_bank("b", 16),
            },
        }
        pts = transform_snapshot_to_points(0, snap)
        self.assertEqual(len(pts), 2 * (len(PACK_FIELDS) + 16))

        # Spot-check tags on a cell point
        cell_pts = [
            p for p in pts
            if p._tags.get("name") == "cell_voltage"  # noqa: SLF001
        ]
        self.assertEqual(len(cell_pts), 32)
        banks = {p._tags.get("bank") for p in cell_pts}  # noqa: SLF001
        self.assertEqual(banks, {"a", "b"})

    def test_bool_as_float(self):
        pts = transform_bank_to_points(0, "a", _ok_bank("a", 0))
        by_name = {p._tags.get("name"): p for p in pts}  # noqa: SLF001
        self.assertIn("charge_mosfet", by_name)
        # field value is on the point's _fields
        self.assertEqual(by_name["charge_mosfet"]._fields.get("value"), 1.0)  # noqa: SLF001


if __name__ == "__main__":
    unittest.main()
