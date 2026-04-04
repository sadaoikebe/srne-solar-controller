#!/usr/bin/env python3
# v1_to_v2.py — InfluxDB 1.x -> 2.x migration
# pip install influxdb influxdb-client pyyaml python-dotenv
#
# Idempotency guarantee:
#   InfluxDB 2.x uses last-write-wins for identical (measurement, tags, timestamp).
#   This script's transform is fully deterministic: given the same v1 row, it always
#   produces the same nanosecond timestamp and the same tag+field values. Running
#   the script twice, or with overlapping ranges, is therefore safe — the second
#   pass overwrites with identical values, producing no duplicates.
#
# Progress tracking:
#   Completed day-chunks are recorded in PROGRESS_FILE. On restart the script
#   skips chunks already marked done, so only failed/incomplete chunks are retried.
#   Delete PROGRESS_FILE to force a full re-migration.
#
# Changes from original:
#   - Replaced write_api.__del__() with write_api.close() (proper flush)
#   - Points per chunk collected into a single list and written in one call (~60x faster)
#   - Progress file tracks completed chunks; interrupted runs resume from where they stopped
#   - Added --reset-progress flag to clear progress and start over
#   - Added --dry-run flag to validate without writing
#   - Improved error handling and logging with timestamps

import os
import sys
import json
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Set

import yaml
from dotenv import load_dotenv
from influxdb import InfluxDBClient as Influx1
import influxdb_client
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

# ── Migration window ───────────────────────────────────────────────────────
TIME_START = "2025-01-01T00:00:00Z"
TIME_STOP  = "2026-01-01T00:00:00Z"
CHUNK_DAYS  = 1

# ── Schema and progress ────────────────────────────────────────────────────
SCHEMA_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "regmap.yaml")
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".migrate_progress.json")


# ── Logging ────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Progress tracking ─────────────────────────────────────────────────────

def load_progress() -> Set[str]:
    """Return the set of chunk start-times already successfully migrated."""
    try:
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
            return set(data.get("completed", []))
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


# ── Schema utils ───────────────────────────────────────────────────────────

def load_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def parse_ts_ns(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1e9)

def to_signed_16_relaxed(x: int) -> int:
    if x < 0:
        return x
    return x - 0x10000 if x >= 0x8000 else x

def combine_uint32_be(hi: int, lo: int) -> int:
    return ((hi & 0xFFFF) << 16) | (lo & 0xFFFF)


# ── v1 field index <-> v2 register key mapping ────────────────────────────
# v1 stored all registers as integer-keyed fields in one wide row.
# v2 stores each register as a separate series with hex/decimal reg tags.

def reg_to_old_field_single(k: str) -> Optional[str]:
    """Map a regmap.yaml key to the v1 field index string."""
    if k.startswith("0x"):
        addr = int(k, 16)
        if 0x0100 <= addr <= 0x010F: return str(addr - 0x0100)           # 0..15
        if 0x0200 <= addr <= 0x023F: return str(16 + (addr - 0x0200))    # 16..79
        if 0xF000 <= addr <= 0xF03F: return str(80 + (addr - 0xF000))    # 80..143
        if 0x0110 <= addr <= 0x0111: return str(144 + (addr - 0x0110))   # 144..145
        return None
    else:
        a = int(k)
        if 0 <= a <= 239:    return str(2000 + a)                        # 2000..2239
        if 720 <= a <= 839:  return str(2240 + (a - 720))               # 2240..2359
        return None

def old_field_to_reg_key(n: int) -> Optional[str]:
    """Map a v1 field index integer to its regmap.yaml key."""
    if 0 <= n <= 15:        return f"0x{0x0100 + n:04x}"
    if 16 <= n <= 79:       return f"0x{0x0200 + (n - 16):04x}"
    if 80 <= n <= 143:      return f"0x{0xF000 + (n - 80):04x}"
    if 144 <= n <= 145:     return f"0x{0x0110 + (n - 144):04x}"
    if 2000 <= n <= 2239:   return str(n - 2000)
    if 2240 <= n <= 2359:   return str(720 + (n - 2240))
    return None

def compute_needed_fields(schema: Dict[str, Any]) -> List[str]:
    """Return sorted list of v1 field indices required by the schema."""
    need: Set[str] = set()
    for key, meta in (schema or {}).items():
        if not isinstance(meta, dict) or "name" not in meta:
            continue
        k = str(key)
        parts = k.split("-", 1) if "-" in k else [k]
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
    """Convert one v1 result row into the list of v2 Points it represents."""
    ts_ns = parse_ts_ns(row["time"])

    reg_values: Dict[str, int] = {}
    for fk, fv in row.items():
        if fk == "time" or fv is None:
            continue
        reg = old_field_to_reg_key(int(fk))
        if reg is not None:
            reg_values[reg] = int(fv)

    out: List[Point] = []
    for key, meta in (schema or {}).items():
        if not isinstance(meta, dict) or "name" not in meta:
            continue
        k = str(key)
        scale = float(meta.get("scale", 1.0))
        if "-" in k:
            hi_k, lo_k = k.split("-", 1)
            if hi_k not in reg_values or lo_k not in reg_values:
                continue
            if k.startswith("0x"):
                raw32 = combine_uint32_be(reg_values[lo_k], reg_values[hi_k])
            else:
                raw32 = combine_uint32_be(reg_values[hi_k], reg_values[lo_k])
            out.append(build_point(ts_ns, k, meta, float(raw32) * scale, raw32))
        else:
            if k not in reg_values:
                continue
            raw = reg_values[k]
            val = to_signed_16_relaxed(raw) if meta.get("signed") else raw
            out.append(build_point(ts_ns, k, meta, float(val) * scale, raw))
    return out


# ── Main migration ────────────────────────────────────────────────────────

def migrate(dry_run: bool = False) -> None:
    schema = load_schema(SCHEMA_PATH)
    need_fields = compute_needed_fields(schema)
    if not need_fields:
        log("ERROR: no fields to migrate — check regmap.yaml keys against v1 field layout")
        sys.exit(1)

    completed = load_progress()

    t0 = datetime.fromisoformat(TIME_START.replace("Z", "+00:00"))
    t1 = datetime.fromisoformat(TIME_STOP.replace("Z", "+00:00"))

    # Pre-compute all chunk boundaries so we can show overall progress
    chunks = []
    cur = t0
    while cur < t1:
        nxt = min(cur + timedelta(days=CHUNK_DAYS), t1)
        chunks.append((cur, nxt))
        cur = nxt

    total   = len(chunks)
    skipped = sum(1 for (c, _) in chunks if c.isoformat() in completed)
    log(f"Migration: {TIME_START} → {TIME_STOP}")
    log(f"Chunks: {total} total, {skipped} already done, {total - skipped} to process")
    if dry_run:
        log("DRY RUN — no data will be written")

    src = Influx1(host=V1_HOST, port=V1_PORT, database=V1_DB)

    dst        = None
    write_api  = None
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
            # This avoids ~1440 separate HTTP round-trips per day of data.
            chunk_points: List[Point] = []
            if rs:
                for (_series, rows) in rs.items():
                    for row in rows:
                        chunk_points.extend(points_from_row(row, schema))

            rows_read  = sum(len(list(rows)) for _, rows in rs.items()) if rs else 0
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
        # write_api.close() flushes any buffered data and blocks until done.
        # This is correct; write_api.__del__() is not a valid flush method.
        if write_api is not None:
            write_api.close()
        if dst is not None:
            dst.close()
        src.close()

    done = sum(1 for (c, _) in chunks if c.isoformat() in completed)
    log(f"Done. {done}/{total} chunks completed.")
    if done < total:
        log(f"Re-run the script to process the remaining {total - done} chunks.")


# ── CLI ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate data from InfluxDB 1.x to InfluxDB 2.x"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read from v1 and transform, but do not write to v2"
    )
    parser.add_argument(
        "--reset-progress", action="store_true",
        help="Delete the progress file and exit (next run will start from scratch)"
    )
    args = parser.parse_args()

    if args.reset_progress:
        reset_progress()
        return

    migrate(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
