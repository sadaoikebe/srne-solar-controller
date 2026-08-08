"""Unit tests for jkbms_api cache helpers (no BLE / no live server)."""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone

from jkbms_api import BmsCache


def _run(coro):
    return asyncio.run(coro)


class TestBmsCache(unittest.TestCase):
    def test_update_and_snapshot_ages(self):
        cache = BmsCache()
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        finished = datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc)
        samples = {
            "a": {
                "ok": True,
                "bank": "a",
                "soc": 10,
                "cells": [3.2] * 16,
                "error": None,
            },
            "b": {
                "ok": False,
                "bank": "b",
                "error": "timeout",
            },
        }
        _run(
            cache.update_poll(
                samples,
                started=started,
                finished=finished,
                duration_s=5.0,
            )
        )
        snap = _run(cache.snapshot())
        self.assertEqual(snap["poll_count"], 1)
        self.assertEqual(snap["ok_count"], 1)
        self.assertEqual(snap["fail_count"], 1)
        self.assertTrue(snap["banks"]["a"]["ok"])
        self.assertFalse(snap["banks"]["b"]["ok"])
        self.assertIsInstance(snap["banks"]["a"]["age_s"], float)
        self.assertEqual(snap["banks"]["a"]["soc"], 10)
        self.assertEqual(len(snap["banks"]["a"]["cells"]), 16)

    def test_health_status_transitions(self):
        cache = BmsCache()
        body = _run(cache.health_body())
        self.assertEqual(body["status"], "starting")

        finished = datetime.now(timezone.utc)
        _run(
            cache.update_poll(
                {
                    "a": {"ok": True, "bank": "a"},
                    "b": {"ok": False, "bank": "b", "error": "x"},
                },
                started=finished,
                finished=finished,
                duration_s=1.0,
            )
        )
        body = _run(cache.health_body())
        self.assertEqual(body["status"], "degraded")
        self.assertEqual(body["ok_banks"], ["a"])
        self.assertEqual(body["fail_banks"], ["b"])

        _run(
            cache.update_poll(
                {
                    "a": {"ok": True, "bank": "a"},
                    "b": {"ok": True, "bank": "b"},
                },
                started=finished,
                finished=finished,
                duration_s=1.0,
            )
        )
        body = _run(cache.health_body())
        self.assertEqual(body["status"], "ok")


if __name__ == "__main__":
    unittest.main()
