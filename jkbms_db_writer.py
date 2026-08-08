#!/usr/bin/env python3
"""Write JK-BMS cache snapshots to InfluxDB v2 (parallel validation writer).

Fetches GET /bms from jkbms_api every SAMPLE_INTERVAL_SECONDS (30 s, wall-
aligned like production db_writer) and writes measurement ``jkbms``.

Does NOT talk to BLE directly and does NOT modify production db_writer.

Point model (see docs/jkbms-implementation-plan.md)
--------------------------------------------------
measurement: jkbms
  pack metrics:
    tags:  bank, serial, name, unit
    field: value (float)
  cell voltages:
    tags:  bank, serial, name=cell_voltage, cell=01..16, unit=V
    field: value (float)

Environment
-----------
  JKBMS_API_URL     default http://127.0.0.1:5005/bms  (host-network stack)
  INFLUX_URL        e.g. http://127.0.0.1:8086
  INFLUX_TOKEN      (required)
  INFLUX_ORG        (required)
  INFLUX_BUCKET     (required)
  LOG_LEVEL         default INFO
"""

from __future__ import annotations

import atexit
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from log_config import get_logger

log = get_logger("jkbms_db_writer")

# ── Configuration ─────────────────────────────────────────────────────────────

JKBMS_API_URL: str = os.getenv("JKBMS_API_URL", "http://127.0.0.1:5005/bms")

INFLUX_URL = os.environ["INFLUX_URL"]
INFLUX_TOKEN = os.environ["INFLUX_TOKEN"]
INFLUX_ORG = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]

SAMPLE_INTERVAL_SECONDS: int = int(os.getenv("JKBMS_WRITE_INTERVAL_S", "30"))
FETCH_TIMEOUT_S: float = float(os.getenv("JKBMS_FETCH_TIMEOUT_S", "8"))

MEASUREMENT = "jkbms"

# Pack-level numeric fields → unit tag. Order is stable for log summaries.
PACK_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("soc", "%"),
    ("soh", "%"),
    ("voltage", "V"),
    ("current", "A"),
    ("power", "W"),
    ("remain_ah", "Ah"),
    ("nominal_ah", "Ah"),
    ("cycles", "count"),
    ("mos_temp", "C"),
    ("temp1", "C"),
    ("temp2", "C"),
    ("balance_current", "A"),
    ("balancing", "code"),
    ("charge_mosfet", "bool"),
    ("discharge_mosfet", "bool"),
    ("runtime_s", "s"),
    ("cell_count", "count"),
    ("cell_min", "V"),
    ("cell_max", "V"),
    ("cell_delta", "V"),
)

# ── Influx client ─────────────────────────────────────────────────────────────

_influx_client = influxdb_client.InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)
atexit.register(lambda: _influx_client.close())


# ── Fetch ─────────────────────────────────────────────────────────────────────


def fetch_bms() -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(JKBMS_API_URL, timeout=FETCH_TIMEOUT_S)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict) or "banks" not in data:
            log.warning("Unexpected /bms payload type: %s", type(data).__name__)
            return None
        return data
    except requests.RequestException as e:
        log.warning("BMS fetch failed: %s", e)
        return None


# ── Point construction ────────────────────────────────────────────────────────


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_pack_point(
    ts_ns: int,
    *,
    bank: str,
    serial: str,
    name: str,
    unit: str,
    value: float,
) -> Point:
    return (
        Point(MEASUREMENT)
        .time(ts_ns)
        .tag("bank", bank)
        .tag("serial", serial)
        .tag("name", name)
        .tag("unit", unit)
        .field("value", float(value))
    )


def build_cell_point(
    ts_ns: int,
    *,
    bank: str,
    serial: str,
    cell_index: int,
    voltage: float,
) -> Point:
    return (
        Point(MEASUREMENT)
        .time(ts_ns)
        .tag("bank", bank)
        .tag("serial", serial)
        .tag("name", "cell_voltage")
        .tag("cell", f"{cell_index:02d}")
        .tag("unit", "V")
        .field("value", float(voltage))
    )


def transform_bank_to_points(
    ts_ns: int,
    bank_id: str,
    sample: Mapping[str, Any],
) -> List[Point]:
    """Convert one bank snapshot to Influx points. Empty if not ok."""
    if not sample.get("ok"):
        log.warning(
            "bank=%s not ok — skipping write (%s)",
            bank_id,
            sample.get("error") or "unknown error",
        )
        return []

    serial = str(sample.get("serial") or bank_id)
    bank = str(sample.get("bank") or bank_id)
    out: List[Point] = []

    for name, unit in PACK_FIELDS:
        if name not in sample:
            continue
        val = _as_float(sample.get(name))
        if val is None:
            continue
        out.append(
            build_pack_point(
                ts_ns, bank=bank, serial=serial, name=name, unit=unit, value=val,
            )
        )

    cells = sample.get("cells") or []
    if isinstance(cells, Sequence):
        for i, raw in enumerate(cells, start=1):
            v = _as_float(raw)
            if v is None:
                continue
            out.append(
                build_cell_point(
                    ts_ns, bank=bank, serial=serial, cell_index=i, voltage=v,
                )
            )

    return out


def transform_snapshot_to_points(
    ts_ns: int,
    snapshot: Mapping[str, Any],
) -> List[Point]:
    banks = snapshot.get("banks") or {}
    if not isinstance(banks, dict):
        return []
    points: List[Point] = []
    for bank_id, sample in banks.items():
        if not isinstance(sample, dict):
            continue
        points.extend(transform_bank_to_points(ts_ns, str(bank_id), sample))
    return points


# ── Write ─────────────────────────────────────────────────────────────────────


def write_points(points: List[Point]) -> None:
    if not points:
        log.warning("write_points called with empty list — nothing to write")
        return
    t0 = time.monotonic()
    try:
        with _influx_client.write_api(write_options=SYNCHRONOUS) as w:
            w.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        elapsed = time.monotonic() - t0
        log.info(
            "Wrote %d jkbms points to InfluxDB in %.3f s  (bucket=%s)",
            len(points), elapsed, INFLUX_BUCKET,
        )
    except Exception as e:
        elapsed = time.monotonic() - t0
        log.error(
            "InfluxDB write failed after %.3f s: %s  (%d points lost)",
            elapsed, e, len(points),
        )
        raise


# ── Timing ────────────────────────────────────────────────────────────────────


def wait_until_next_tick() -> datetime:
    """Sleep until the next wall-clock boundary aligned to SAMPLE_INTERVAL_SECONDS."""
    now = datetime.now()
    seconds = now.second + now.microsecond / 1_000_000
    next_off = (int(seconds // SAMPLE_INTERVAL_SECONDS) + 1) * SAMPLE_INTERVAL_SECONDS
    nxt = now.replace(second=0, microsecond=0) + timedelta(seconds=next_off)
    delay = (nxt - now).total_seconds()
    if delay > 0:
        log.debug("Sleeping %.1f s until next %ds boundary", delay, SAMPLE_INTERVAL_SECONDS)
        time.sleep(delay)
    return nxt


# ── Main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    log.info("=" * 60)
    log.info("JK-BMS Influx writer starting")
    log.info("  BMS API      : %s", JKBMS_API_URL)
    log.info("  InfluxDB     : %s  org=%s  bucket=%s", INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET)
    log.info("  Measurement  : %s", MEASUREMENT)
    log.info("  Interval     : %d s", SAMPLE_INTERVAL_SECONDS)
    log.info("=" * 60)

    tick_time = wait_until_next_tick()

    while True:
        log.debug("Tick at %s", tick_time.strftime("%H:%M:%S"))
        ts_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)

        snapshot = fetch_bms()
        if snapshot is None:
            log.warning(
                "No BMS snapshot at %s — skipping this tick (data gap)",
                tick_time.strftime("%H:%M:%S"),
            )
        else:
            try:
                points = transform_snapshot_to_points(ts_ns, snapshot)
                if points:
                    write_points(points)
                else:
                    log.warning(
                        "transform produced 0 points (poll_count=%s ok_count=%s)",
                        snapshot.get("poll_count"),
                        snapshot.get("ok_count"),
                    )
            except Exception as e:
                log.error("Failed to process or write BMS data: %s", e)

        tick_time = wait_until_next_tick()


if __name__ == "__main__":
    main()
