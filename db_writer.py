"""InfluxDB writer.

Wakes at each wall-clock minute boundary, fetches all inverter registers
from modbus_api, maps them through regmap.yaml, and writes the resulting
points to InfluxDB v2.

Log levels
----------
  DEBUG  — raw register dict, per-point transforms, schema misses
  INFO   — startup configuration, per-minute write summary (N points, elapsed time)
  WARNING — fetch failure (one data-gap per minute), empty result sets
  ERROR  — InfluxDB write failure
"""
from __future__ import annotations

import os
import time
import atexit
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
import yaml

import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from log_config import get_logger

log = get_logger("db_writer")

# ── Configuration ─────────────────────────────────────────────────────────────

_API_PORT: int = int(os.getenv("MODBUS_API_PORT", "5004"))
API_URL: str   = f"http://modbus_api:{_API_PORT}/registers"

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "regmap.yaml")

INFLUX_URL    = os.environ["INFLUX_URL"]
INFLUX_TOKEN  = os.environ["INFLUX_TOKEN"]
INFLUX_ORG    = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]

# ── InfluxDB client ───────────────────────────────────────────────────────────

_influx_client = influxdb_client.InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)
atexit.register(lambda: _influx_client.close())

# ── Fetch ─────────────────────────────────────────────────────────────────────


def fetch_registers() -> Optional[Dict[str, int]]:
    try:
        r = requests.get(API_URL, timeout=8)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict):
            log.warning("Unexpected response type from modbus_api: %s", type(data).__name__)
            return None
        log.debug("Fetched %d registers from modbus_api", len(data))
        return data
    except requests.RequestException as e:
        log.warning("Register fetch failed: %s", e)
        return None


# ── Schema ────────────────────────────────────────────────────────────────────


def load_schema(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        schema = yaml.safe_load(f) or {}
    log.info("Loaded schema from %s: %d register definitions", path, len(schema))
    return schema


# ── Register helpers ──────────────────────────────────────────────────────────


def to_signed_16_relaxed(x: int) -> int:
    """Reinterpret a uint16 as a signed int16; pass-through if already negative."""
    if x < 0:
        return x
    return x - 0x10000 if x >= 0x8000 else x


def combine_uint32(hi: int, lo: int) -> int:
    return ((hi & 0xFFFF) << 16) | (lo & 0xFFFF)


def combine_auto(reg_key: str, left_val: int, right_val: int) -> int:
    """Combine two 16-bit register values into a 32-bit integer.

    Key naming convention determines byte order:
      PowMr  hex notation ("0x..."): stored as [lo, hi] — right=hi, left=lo.
      Growatt decimal notation ("N-M"): stored as [hi, lo] — left=hi, right=lo.
    """
    if reg_key.startswith("0x"):
        return combine_uint32(right_val, left_val)
    return combine_uint32(left_val, right_val)


# ── Point construction ────────────────────────────────────────────────────────


def build_point(
    ts_ns: int,
    reg_key: str,
    meta: Dict[str, Any],
    value: float,
    raw_int: int,
) -> Point:
    p = Point("modbus").time(ts_ns).tag("reg", reg_key)
    if "name" in meta:
        p = p.tag("name", str(meta["name"]))
    if "unit" in meta:
        p = p.tag("unit", str(meta["unit"]))
    return p.field("value", float(value)).field("raw", int(raw_int))


def transform_to_points(
    ts_ns: int,
    data: Dict[str, int],
    schema: Dict[str, Any],
) -> List[Point]:
    """Convert raw register data to InfluxDB Points using the regmap schema.

    *ts_ns* is captured before the fetch call so the timestamp reflects when
    the measurement was initiated, not when it was processed.
    """
    out: List[Point] = []
    skipped = 0

    for key, meta in (schema or {}).items():
        if not isinstance(meta, dict) or "name" not in meta:
            continue
        scale = float(meta.get("scale", 1.0))
        name  = meta["name"]

        if "-" in key:
            left, right = key.split("-", 1)
            if left not in data or right not in data:
                log.debug("Schema key %r skipped: register(s) not in fetch data", key)
                skipped += 1
                continue
            raw32 = combine_auto(key, int(data[left]), int(data[right]))
            val   = float(raw32) * scale
            log.debug("Point: %-30s = %.3f %s  (raw32=%d)", name, val, meta.get("unit", ""), raw32)
            out.append(build_point(ts_ns, key, meta, val, raw32))
            continue

        if key not in data:
            log.debug("Schema key %r skipped: not in fetch data", key)
            skipped += 1
            continue
        raw = int(data[key])
        val = to_signed_16_relaxed(raw) if meta.get("signed") else raw
        scaled_val = float(val) * scale
        log.debug(
            "Point: %-30s = %.3f %s  (raw=%d)", name, scaled_val, meta.get("unit", ""), raw
        )
        out.append(build_point(ts_ns, key, meta, scaled_val, raw))

    if skipped:
        log.debug("%d schema key(s) skipped (registers absent in this fetch)", skipped)

    return out


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
            "Wrote %d points to InfluxDB in %.3f s  (bucket: %s)",
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


SAMPLE_INTERVAL_SECONDS: int = 30


def wait_until_next_tick() -> datetime:
    """Sleep until the next wall-clock tick aligned to SAMPLE_INTERVAL_SECONDS."""
    now      = datetime.now()
    seconds  = now.second + now.microsecond / 1_000_000
    next_off = (int(seconds // SAMPLE_INTERVAL_SECONDS) + 1) * SAMPLE_INTERVAL_SECONDS
    nxt      = now.replace(second=0, microsecond=0) + timedelta(seconds=next_off)
    delay    = (nxt - now).total_seconds()
    if delay > 0:
        log.debug("Sleeping %.1f s until next %ds boundary", delay, SAMPLE_INTERVAL_SECONDS)
        time.sleep(delay)
    return nxt


# ── Main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    log.info("=" * 60)
    log.info("DB writer starting")
    log.info("  API URL       : %s", API_URL)
    log.info("  InfluxDB      : %s  org=%s  bucket=%s", INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET)
    log.info("  Schema        : %s", SCHEMA_PATH)
    log.info("=" * 60)

    schema = load_schema(SCHEMA_PATH)
    wait_until_next_tick()

    while True:
        log.debug("Tick at %s", datetime.now().strftime("%H:%M:%S"))

        # Capture timestamp *before* the fetch so InfluxDB points reflect
        # when the measurement was initiated, not when it was processed.
        ts_ns         = int(datetime.now(timezone.utc).timestamp() * 1e9)
        register_data = fetch_registers()

        if register_data is not None:
            try:
                points = transform_to_points(ts_ns, register_data, schema)
                if points:
                    write_points(points)
                else:
                    log.warning(
                        "transform_to_points produced 0 points from %d registers — "
                        "check regmap.yaml vs fetch keys",
                        len(register_data),
                    )
            except Exception as e:
                log.error("Failed to process or write data: %s", e)
        else:
            log.warning(
                "No register data available at %s — skipping this tick (data gap)",
                datetime.now().strftime("%H:%M:%S"),
            )

        wait_until_next_tick()


if __name__ == "__main__":
    main()
