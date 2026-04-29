#!/usr/bin/env python3
# v1_to_v2_growatt_extras.py
#
# Backfills the Growatt registers that were added to regmap.yaml AFTER the
# original v1 -> v2 migration completed:
#
#   - pv4_daily                       (input regs 52-53)
#   - pv4_cumulative                  (input regs 54-55)
#   - batt_charge_daily_growatt       (input regs 70-71)
#   - batt_charge_cumulative_growatt  (input regs 72-73)
#   - batt_discharge_daily_growatt    (input regs 74-75)
#   - batt_discharge_cumulative_growatt (input regs 76-77)
#
# v1 stored these as fields "2052"-"2055" and "2070"-"2077" on the wide
# `registers` measurement (Growatt input register N -> v1 field str(N+2000)).
#
# This script is a narrow variant of the original v1_to_v2.py: it reuses the
# same per-day chunking, the same idempotent (measurement, tags, ts) write
# semantics, and the same regmap-derived 32-bit (hi-lo) decode, but filters
# the schema down to the names listed above so nothing else is touched.
#
# Progress is recorded in a separate file (.migrate_growatt_progress.json)
# so it does NOT collide with any prior migration progress.

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import influxdb_client
import yaml
from dotenv import load_dotenv
from influxdb import InfluxDBClient as Influx1
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

# ── InfluxDB 1.x (source) ─────────────────────────────────────────────────
V1_HOST         = "localhost"
V1_PORT         = 8086
V1_DB           = "mysolardb"
MEASUREMENT_SRC = "registers"

# ── InfluxDB 2.x (destination) ────────────────────────────────────────────
V2_URL          = "http://192.168.1.216:8086"
V2_ORG          = os.getenv("INFLUXDB2_ORG")
V2_BUCKET       = os.getenv("INFLUXDB2_BUCKET")
V2_TOKEN        = os.getenv("INFLUXDB2_TOKEN")
MEASUREMENT_DST = "modbus"

# ── Migration window ──────────────────────────────────────────────────────
TIME_START = "2025-01-01T00:00:00Z"
TIME_STOP  = "2026-01-01T00:00:00Z"
CHUNK_DAYS = 1

# ── Schema and progress ───────────────────────────────────────────────────
SCHEMA_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "regmap.yaml")
PROGRESS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    ".migrate_growatt_progress.json",
)

# Whitelist: only registers added after the original cutover.
ONLY_NAMES: Set[str] = {
    "pv4_daily",
    "pv4_cumulative",
    "batt_charge_daily_growatt",
    "batt_charge_cumulative_growatt",
    "batt_discharge_daily_growatt",
    "batt_discharge_cumulative_growatt",
}


# ── Logging ───────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Progress tracking ─────────────────────────────────────────────────────

def load_progress() -> Set[str]:
    try:
        with open(PROGRESS_FILE) as f:
            return set(json.load(f).get("completed", []))
    except FileNotFoundError:
        return set()
    except Exception as e:
        log(f"[warn] could not read progress file: {e} — starting fresh")
        return set()


def save_progress(completed: Set[str]) -> None:
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({"completed": sorted(completed)}, f, indent=2)
    except Exception as e:
        log(f"[warn] could not save progress: {e}")


def reset_progress() -> None:
    try:
        os.remove(PROGRESS_FILE)
        log("Progress file deleted.")
    except FileNotFoundError:
        log("No progress file to delete.")


# ── Schema utils ──────────────────────────────────────────────────────────

def load_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        full = yaml.safe_load(f) or {}
    return {
        k: v for k, v in full.items()
        if isinstance(v, dict) and v.get("name") in ONLY_NAMES
    }


def parse_ts_ns(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1e9)


def combine_uint32_be(hi: int, lo: int) -> int:
    return ((hi & 0xFFFF) << 16) | (lo & 0xFFFF)


# ── v1 field index <-> v2 register key mapping ────────────────────────────
# All registers in this script are decimal-keyed Growatt input registers.
# v1 stored Growatt input reg N at field index str(N + 2000), for N in 0..239.

def reg_to_old_field_single(k: str) -> Optional[str]:
    if k.startswith("0x"):
        return None
    a = int(k)
    if 0 <= a <= 239:
        return str(2000 + a)
    return None


def old_field_to_reg_key(n: int) -> Optional[str]:
    if 2000 <= n <= 2239:
        return str(n - 2000)
    return None


def compute_needed_fields(schema: Dict[str, Any]) -> List[str]:
    need: Set[str] = set()
    for key in schema:
        parts = key.split("-") if "-" in key else [key]
        for part in parts:
            f = reg_to_old_field_single(part)
            if f is not None:
                need.add(f)
    return sorted(need, key=lambda s: int(s))


# ── Point construction ────────────────────────────────────────────────────

def build_point(ts_ns: int, reg_key: str, meta: Dict[str, Any],
                value: float, raw_int: int) -> Point:
    p = Point(MEASUREMENT_DST).time(ts_ns).tag("reg", reg_key)
    if "name" in meta: p = p.tag("name", str(meta["name"]))
    if "unit" in meta: p = p.tag("unit", str(meta["unit"]))
    return p.field("value", float(value)).field("raw", int(raw_int))


def points_from_row(row: Dict[str, Any], schema: Dict[str, Any]) -> List[Point]:
    """Convert one v1 result row into the v2 Points it represents.

    All keys in `schema` are 32-bit decimal-keyed Growatt input registers
    (big-endian: hi-lo). 16-bit decoding is intentionally not handled here.
    """
    ts_ns = parse_ts_ns(row["time"])

    reg_values: Dict[str, int] = {}
    for fk, fv in row.items():
        if fk == "time" or fv is None:
            continue
        reg = old_field_to_reg_key(int(fk))
        if reg is not None:
            reg_values[reg] = int(fv)

    out: List[Point] = []
    for key, meta in schema.items():
        if "-" not in key:
            continue
        scale = float(meta.get("scale", 1.0))
        hi_k, lo_k = key.split("-", 1)
        if hi_k not in reg_values or lo_k not in reg_values:
            continue
        raw32 = combine_uint32_be(reg_values[hi_k], reg_values[lo_k])
        out.append(build_point(ts_ns, key, meta, float(raw32) * scale, raw32))
    return out


# ── Main migration ────────────────────────────────────────────────────────

def migrate(dry_run: bool = False) -> None:
    schema = load_schema(SCHEMA_PATH)
    if not schema:
        log("ERROR: no matching registers in regmap.yaml — expected names: "
            + ", ".join(sorted(ONLY_NAMES)))
        sys.exit(1)

    found_names = sorted(m["name"] for m in schema.values())
    log(f"Migrating {len(schema)} register(s): {found_names}")
    missing = ONLY_NAMES - set(found_names)
    if missing:
        log(f"[warn] regmap.yaml is missing expected names: {sorted(missing)}")

    need_fields = compute_needed_fields(schema)
    log(f"v1 fields needed: {need_fields}")

    completed = load_progress()

    t0 = datetime.fromisoformat(TIME_START.replace("Z", "+00:00"))
    t1 = datetime.fromisoformat(TIME_STOP.replace("Z", "+00:00"))

    chunks = []
    cur = t0
    while cur < t1:
        nxt = min(cur + timedelta(days=CHUNK_DAYS), t1)
        chunks.append((cur, nxt))
        cur = nxt

    total   = len(chunks)
    skipped = sum(1 for (c, _) in chunks if c.isoformat() in completed)
    log(f"Migration: {TIME_START} -> {TIME_STOP}")
    log(f"Chunks: {total} total, {skipped} already done, {total - skipped} to process")
    if dry_run:
        log("DRY RUN — no data will be written")

    src = Influx1(host=V1_HOST, port=V1_PORT, database=V1_DB)

    dst       = None
    write_api = None
    if not dry_run:
        dst       = influxdb_client.InfluxDBClient(url=V2_URL, token=V2_TOKEN, org=V2_ORG)
        write_api = dst.write_api(write_options=SYNCHRONOUS)

    try:
        for idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
            chunk_key = chunk_start.isoformat()

            if chunk_key in completed:
                log(f"[{idx:3d}/{total}] SKIP  {chunk_start.date()} (already done)")
                continue

            log(f"[{idx:3d}/{total}] START {chunk_start.date()} .. {chunk_end.date()}")

            sel = ",".join(f'"{f}"' for f in need_fields)
            q = (
                f'SELECT {sel} FROM "{MEASUREMENT_SRC}" '
                f"WHERE time >= '{chunk_start.replace(tzinfo=timezone.utc).isoformat()}' "
                f"AND time < '{chunk_end.replace(tzinfo=timezone.utc).isoformat()}'"
            )

            rs = src.query(q)

            # Collect ALL points for this chunk, then write in a single call.
            chunk_points: List[Point] = []
            if rs:
                for (_series, rows) in rs.items():
                    for row in rows:
                        chunk_points.extend(points_from_row(row, schema))

            rows_read = sum(len(list(rows)) for _, rows in rs.items()) if rs else 0
            log(f"         rows={rows_read}, points={len(chunk_points)}")

            if dry_run:
                log(f"         [dry-run] would write {len(chunk_points)} points")
            elif chunk_points:
                write_api.write(bucket=V2_BUCKET, org=V2_ORG, record=chunk_points)

            # Mark chunk as completed only AFTER a successful write
            completed.add(chunk_key)
            save_progress(completed)
            log(f"         OK")

    except KeyboardInterrupt:
        log("Interrupted by user. Progress saved — re-run to continue.")
    except Exception as e:
        log(f"ERROR: {e}")
        raise
    finally:
        if write_api is not None:
            write_api.close()
        if dst is not None:
            dst.close()
        src.close()

    done = sum(1 for (c, _) in chunks if c.isoformat() in completed)
    log(f"Done. {done}/{total} chunks completed.")
    if done < total:
        log(f"Re-run the script to process the remaining {total - done} chunks.")


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill the newly-added Growatt registers (pv4 + battery "
            "charge/discharge daily/cumulative) from InfluxDB 1.x to 2.x."
        )
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read from v1 and transform, but do not write to v2",
    )
    parser.add_argument(
        "--reset-progress", action="store_true",
        help="Delete the progress file and exit (next run will start from scratch)",
    )
    args = parser.parse_args()

    if args.reset_progress:
        reset_progress()
        return

    migrate(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
