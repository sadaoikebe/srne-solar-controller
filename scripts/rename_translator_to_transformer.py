#!/usr/bin/env python3
"""Rename temp_translator_powmr → temp_transformer_powmr in InfluxDB.

Why: register 0x0222 was originally added to regmap.yaml as
"temp_translator_powmr". "Translator" is a mistranslation of the Chinese
变压器 (biànyāqì) — the correct English term is "transformer". The regmap
has been corrected, but any data written before the redeploy still carries
the old `name` tag and won't match dashboard filters on the new name.

InfluxDB v2 has no UPDATE — this script implements the rename as:

  1. Query every (measurement="modbus", name="temp_translator_powmr",
     reg="0x0222") point in the given JST window, recovering its tags
     (reg, unit) and fields (value, raw).
  2. Write each one back with name="temp_transformer_powmr" — same
     timestamp, same reg, same unit, same fields. Same timestamp + tagset
     means InfluxDB upserts, so re-running after a partial failure is safe.
  3. Delete the originals via a single predicate-based delete call.

Default is dry-run. Pass --commit AND type the confirmation phrase to
actually rewrite. Always run dry-run first to confirm scope.

Usage:
  python scripts/rename_translator_to_transformer.py \
      --start "2026-05-04 00:00" --stop "2026-05-05 12:00"

  python scripts/rename_translator_to_transformer.py \
      --start "2026-05-04 00:00" --stop "2026-05-05 12:00" --commit
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, NamedTuple

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# ── Constants ────────────────────────────────────────────────────────────────

JST = timezone(timedelta(hours=9))
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOTENV_PATH  = PROJECT_ROOT / ".env"

OLD_NAME       = "temp_translator_powmr"
NEW_NAME       = "temp_transformer_powmr"
TARGET_REG     = "0x0222"  # only register that ever carried the old name
CONFIRM_PHRASE = "rename translator to transformer"


# ── Config ───────────────────────────────────────────────────────────────────

def load_dotenv(path: Path) -> None:
    """Tiny .env loader — pre-existing env vars win."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


# ── Time parsing ─────────────────────────────────────────────────────────────

def parse_jst(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=JST)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Invalid JST datetime: {s!r}")


# ── Query ────────────────────────────────────────────────────────────────────

class Row(NamedTuple):
    ts:    datetime
    reg:   str
    unit:  str
    value: float
    raw:   int


def find_old_name_rows(qa, org, bucket, start, stop) -> List[Row]:
    """Pivot value+raw into one row per timestamp; preserve reg/unit tags."""
    flux = f'''
from(bucket: "{bucket}")
  |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
  |> filter(fn: (r) =>
        r._measurement == "modbus"
        and r.name == "{OLD_NAME}"
        and r.reg == "{TARGET_REG}"
     )
  |> pivot(rowKey: ["_time", "reg", "name", "unit"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
'''
    rows: List[Row] = []
    for table in qa.query(flux, org=org):
        for record in table.records:
            value = record.values.get("value")
            raw   = record.values.get("raw")
            if value is None or raw is None:
                # Should never happen — db_writer always writes both fields.
                continue
            rows.append(Row(
                ts    = record.get_time(),
                reg   = record.values.get("reg", ""),
                unit  = record.values.get("unit", ""),
                value = float(value),
                raw   = int(raw),
            ))
    return rows


# ── Reporting ────────────────────────────────────────────────────────────────

def print_sample(rows: List[Row], n: int = 5) -> None:
    if not rows:
        return
    print("  First few:")
    for r in rows[:n]:
        print(f"    {r.ts.astimezone(JST).isoformat()}  reg={r.reg}  unit={r.unit}  "
              f"value={r.value}  raw={r.raw}")
    if len(rows) > 2 * n:
        print(f"    ... ({len(rows) - 2*n} more)")
    if len(rows) > n:
        print("  Last few:")
        for r in rows[-n:]:
            print(f"    {r.ts.astimezone(JST).isoformat()}  reg={r.reg}  unit={r.unit}  "
                  f"value={r.value}  raw={r.raw}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    load_dotenv(DOTENV_PATH)

    parser = argparse.ArgumentParser(
        description=f"Rename {OLD_NAME} → {NEW_NAME} in InfluxDB (dry-run by default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start", type=parse_jst, required=True,
                        help='JST datetime, e.g. "2026-05-04 00:00"')
    parser.add_argument("--stop",  type=parse_jst, required=True,
                        help='JST datetime, e.g. "2026-05-05 12:00"')
    parser.add_argument("--commit", action="store_true",
                        help="Actually rewrite + delete (default is dry-run).")
    parser.add_argument("--url",    default=os.environ.get("INFLUX_URL", "http://localhost:8086"))
    parser.add_argument("--token",  default=os.environ.get("INFLUX_TOKEN"))
    parser.add_argument("--org",    default=os.environ.get("INFLUX_ORG"))
    parser.add_argument("--bucket", default=os.environ.get("INFLUX_BUCKET"))
    args = parser.parse_args()

    if args.start >= args.stop:
        sys.exit("--start must be before --stop")
    if not (args.token and args.org and args.bucket):
        sys.exit("Missing INFLUX_TOKEN / INFLUX_ORG / INFLUX_BUCKET (env or CLI).")

    print("=" * 70)
    print(f"Rename {OLD_NAME}  →  {NEW_NAME}")
    print(f"  Range (JST)  : {args.start.isoformat()}  →  {args.stop.isoformat()}")
    print(f"  Bucket       : {args.bucket}")
    print(f"  Target reg   : {TARGET_REG}")
    print(f"  Mode         : {'COMMIT (will rewrite + delete)' if args.commit else 'DRY-RUN'}")
    print("=" * 70)

    with InfluxDBClient(url=args.url, token=args.token, org=args.org, timeout=600_000) as client:
        qa = client.query_api()

        print(f'\nFinding rows with name="{OLD_NAME}" AND reg="{TARGET_REG}"...')
        rows = find_old_name_rows(qa, args.org, args.bucket, args.start, args.stop)
        print(f"  {len(rows)} row(s) match")
        print_sample(rows)

        if not rows:
            print("\nNothing to do.")
            return 0

        if not args.commit:
            print("\nDRY-RUN — no data was changed. Re-run with --commit to rewrite + delete.")
            return 0

        # ── Confirmation gate ────────────────────────────────────────────
        print("\nThis will:")
        print(f"  1. Write {len(rows)} replacement point(s) tagged name=\"{NEW_NAME}\"")
        print(f"     (same timestamp, reg, unit, value, raw → InfluxDB upserts)")
        print(f"  2. Delete every point matching:")
        print(f"     _measurement=\"modbus\" AND name=\"{OLD_NAME}\" AND reg=\"{TARGET_REG}\"")
        print(f"     in the window [{args.start.isoformat()}, {args.stop.isoformat()}].")
        print(f'\nType exactly:  {CONFIRM_PHRASE}')
        try:
            typed = input("> ").strip()
        except EOFError:
            typed = ""
        if typed != CONFIRM_PHRASE:
            print("Confirmation phrase did not match — aborting.")
            return 1

        # ── Step 1: write replacements ───────────────────────────────────
        # Write FIRST so a delete failure leaves both copies intact (recoverable),
        # never zero copies (data loss). Same (measurement, tagset, _time)
        # makes the write an upsert, so re-running is safe.
        print(f"\nWriting {len(rows)} replacement point(s) under name=\"{NEW_NAME}\"...")
        with client.write_api(write_options=SYNCHRONOUS) as wa:
            points = [
                Point("modbus")
                    .time(r.ts)
                    .tag("reg",  r.reg)
                    .tag("name", NEW_NAME)
                    .tag("unit", r.unit)
                    .field("value", r.value)
                    .field("raw",   r.raw)
                for r in rows
            ]
            wa.write(bucket=args.bucket, org=args.org, record=points)
        print("  Write complete.")

        # ── Step 2: delete originals ─────────────────────────────────────
        # InfluxDB v2 delete predicate supports AND on tag equality (no OR).
        # One predicate covers every point in the window matching name+reg.
        print(f'\nDeleting originals (name="{OLD_NAME}" AND reg="{TARGET_REG}")...')
        try:
            client.delete_api().delete(
                start=args.start,
                stop=args.stop,
                predicate=f'_measurement="modbus" AND name="{OLD_NAME}" AND reg="{TARGET_REG}"',
                bucket=args.bucket,
                org=args.org,
            )
        except Exception as e:
            print(f"  DELETE FAILED: {e}")
            print(f"  Replacements were written successfully — both copies now exist.")
            print(f"  Re-run this script to retry the delete (the write step is idempotent).")
            return 2
        print("  Delete complete.")

        # ── Verify ───────────────────────────────────────────────────────
        print("\nVerifying...")
        remaining = find_old_name_rows(qa, args.org, args.bucket, args.start, args.stop)
        if remaining:
            print(f"  WARNING: {len(remaining)} row(s) with old name still present.")
            return 2
        print("  OK — 0 rows under old name remain.")
        print(f"\nDone. {len(rows)} point(s) renamed: {OLD_NAME} → {NEW_NAME}.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
