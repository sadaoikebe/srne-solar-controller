"""InfluxDB writer.

Wakes at each wall-clock minute boundary, fetches all inverter registers
from modbus_api, maps them through regmap.yaml, and writes the resulting
points to InfluxDB v2.
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

# ── Configuration ─────────────────────────────────────────────────────────────

_API_PORT: int = int(os.getenv("MODBUS_API_PORT", "5004"))
API_URL: str   = f"http://modbus_api:{_API_PORT}/registers"

# regmap.yaml lives beside this script in /app
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "regmap.yaml")

# InfluxDB v2 connection — read from environment, fail fast at startup if missing
INFLUX_URL    = os.environ["INFLUX_URL"]
INFLUX_TOKEN  = os.environ["INFLUX_TOKEN"]
INFLUX_ORG    = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]

# ── InfluxDB client ───────────────────────────────────────────────────────────

# Singleton — created once and reused every minute to avoid repeated TCP overhead.
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
        return data if isinstance(data, dict) else None
    except Exception as e:
        print(f"Error fetching registers: {e}")
        return None


# ── Schema ────────────────────────────────────────────────────────────────────


def load_schema(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


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
      PowMr  uses hex notation ("0x...") and stores values as [lo, hi],
              so we pass (right=hi, left=lo) to combine_uint32.
      Growatt uses decimal notation ("N-M") and stores values as [hi, lo],
              so we pass (left=hi, right=lo) to combine_uint32.
    """
    if reg_key.startswith("0x"):
        return combine_uint32(right_val, left_val)   # PowMr: left=lo, right=hi
    return combine_uint32(left_val, right_val)        # Growatt: left=hi, right=lo


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

    *ts_ns* should be captured just before the HTTP register fetch so that
    the timestamp reflects the actual measurement time, not the processing time.
    """
    out: List[Point] = []

    for key, meta in (schema or {}).items():
        if not isinstance(meta, dict) or "name" not in meta:
            continue
        scale = float(meta.get("scale", 1.0))

        if "-" in key:
            left, right = key.split("-", 1)
            if left not in data or right not in data:
                continue
            raw32 = combine_auto(key, int(data[left]), int(data[right]))
            out.append(build_point(ts_ns, key, meta, float(raw32) * scale, raw32))
            continue

        if key not in data:
            continue
        raw = int(data[key])
        val = to_signed_16_relaxed(raw) if meta.get("signed") else raw
        out.append(build_point(ts_ns, key, meta, float(val) * scale, raw))

    return out


# ── Write ─────────────────────────────────────────────────────────────────────


def write_points(points: List[Point]) -> None:
    if not points:
        return
    with _influx_client.write_api(write_options=SYNCHRONOUS) as w:
        w.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)


# ── Timing ────────────────────────────────────────────────────────────────────


def wait_until_next_minute() -> datetime:
    """Sleep until the start of the next wall-clock minute."""
    now         = datetime.now()
    next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    delay       = (next_minute - now).total_seconds()
    if delay > 0:
        time.sleep(delay)
    return next_minute


# ── Main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    schema = load_schema(SCHEMA_PATH)
    print("Starting DB writer script...")
    wait_until_next_minute()

    while True:
        print(f"Processing at {datetime.now()}")

        # Capture the timestamp *before* the network call so that the InfluxDB
        # point reflects when the measurement was initiated, not when it was parsed.
        ts_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
        register_data = fetch_registers()

        if register_data is not None:
            try:
                write_points(transform_to_points(ts_ns, register_data, schema))
            except Exception as e:
                print(f"Error writing to InfluxDB: {e}")
        else:
            print("No data to write (fetch failed)")

        wait_until_next_minute()


if __name__ == "__main__":
    main()
