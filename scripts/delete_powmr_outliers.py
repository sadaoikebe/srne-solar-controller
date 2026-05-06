#!/usr/bin/env python3
"""Delete ALL PowMr records in a time range from InfluxDB.

This is the "scorched-earth" cleanup for damage caused by the modbus race
condition. There is NO per-row criterion: every PowMr point in the
specified [start, stop) window is deleted, across every PowMr register
listed in regmap.yaml (any key starting with "0x").

Why no criterion: earlier versions filtered by abnormal grid-frequency
reads (reg 0x0215 outside a normal band). That under-deleted because the
race can corrupt other registers (pv2_power, battery_soc, temps, …) at
samples where freq itself read a plausible value. Those rows survived and
left visibly bad data behind.

Growatt data (decimal-keyed regs) is NEVER touched.

Default is dry-run. Pass --commit AND type the confirmation phrase to
actually delete. Always run dry-run first to confirm the count.

Usage:
  python scripts/delete_powmr_outliers.py --start "2026-05-01 12:00" --stop "2026-05-01 18:00"
  python scripts/delete_powmr_outliers.py --start "2026-05-01 12:00" --stop "2026-05-01 18:00" --commit
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import yaml
from influxdb_client import InfluxDBClient

# ── Constants ────────────────────────────────────────────────────────────────

JST = timezone(timedelta(hours=9))
PROJECT_ROOT = Path(__file__).resolve().parent.parent
REGMAP_PATH  = PROJECT_ROOT / "regmap.yaml"
DOTENV_PATH  = PROJECT_ROOT / ".env"

CONFIRM_PHRASE = "delete corrupted powmr"


# ── Config ───────────────────────────────────────────────────────────────────

def load_dotenv(path: Path) -> None:
    """Tiny .env loader — no dependency on python-dotenv.

    Pre-existing env vars win, so a shell-exported value isn't shadowed.
    """
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


# ── Schema ───────────────────────────────────────────────────────────────────

def load_powmr_regs() -> List[str]:
    """All hex-keyed reg tags in regmap.yaml, including combined keys like '0xf034-0xf035'."""
    with open(REGMAP_PATH, encoding="utf-8") as f:
        schema = yaml.safe_load(f) or {}
    regs = sorted(k for k in schema if k.startswith("0x"))
    if not regs:
        sys.exit("regmap.yaml contains no PowMr (hex-keyed) entries — refusing to run")
    return regs


# ── Flux helpers ─────────────────────────────────────────────────────────────

def count_powmr_points(qa, org, bucket, start, stop) -> int:
    """Total number of PowMr points (any hex-keyed reg) in the range.

    Filters by `r.reg` starting with "0x" so Growatt's decimal-keyed regs
    are excluded. Counts all fields, not just `value`, so the figure
    reflects what the delete will actually remove.
    """
    flux = f'''
import "strings"

from(bucket: "{bucket}")
  |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
  |> filter(fn: (r) => r._measurement == "modbus")
  |> filter(fn: (r) => strings.hasPrefix(v: r.reg, prefix: "0x"))
  |> count()
  |> group()
  |> sum()
'''
    total = 0
    for table in qa.query(flux, org=org):
        for record in table.records:
            v = record.get_value()
            if v:
                total += int(v)
    return total


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    load_dotenv(DOTENV_PATH)

    parser = argparse.ArgumentParser(
        description="Delete corrupted PowMr records from InfluxDB (dry-run by default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start", type=parse_jst, required=True,
                        help='JST datetime, e.g. "2026-05-01 12:00"')
    parser.add_argument("--stop", type=parse_jst, required=True,
                        help='JST datetime, e.g. "2026-05-01 18:00"')
    parser.add_argument("--commit", action="store_true",
                        help="Actually delete (default is dry-run).")
    parser.add_argument("--url",    default=os.environ.get("INFLUX_URL", "http://localhost:8086"))
    parser.add_argument("--token",  default=os.environ.get("INFLUX_TOKEN"))
    parser.add_argument("--org",    default=os.environ.get("INFLUX_ORG"))
    parser.add_argument("--bucket", default=os.environ.get("INFLUX_BUCKET"))
    args = parser.parse_args()

    if args.start >= args.stop:
        sys.exit("--start must be before --stop")
    if not (args.token and args.org and args.bucket):
        sys.exit("Missing INFLUX_TOKEN / INFLUX_ORG / INFLUX_BUCKET (env or CLI).")

    powmr_regs = load_powmr_regs()

    print("=" * 70)
    print("Delete ALL PowMr records in range")
    print(f"  Range (JST)  : {args.start.isoformat()}  →  {args.stop.isoformat()}")
    print(f"  Bucket       : {args.bucket}")
    print(f"  PowMr regs   : {len(powmr_regs)} from regmap.yaml")
    print(f"  Criterion    : (none — every PowMr point in range will be deleted)")
    print(f"  Mode         : {'COMMIT (will delete)' if args.commit else 'DRY-RUN'}")
    print("=" * 70)

    with InfluxDBClient(url=args.url, token=args.token, org=args.org, timeout=600_000) as client:
        qa = client.query_api()

        # ── Preview ──────────────────────────────────────────────────────
        print("\nCounting PowMr points in range...")
        n_points = count_powmr_points(qa, args.org, args.bucket, args.start, args.stop)
        print(f"  {n_points} PowMr point(s) currently in [{args.start.isoformat()}, "
              f"{args.stop.isoformat()})")

        if n_points == 0:
            print("\nNothing to do.")
            return 0

        if not args.commit:
            print("\nDRY-RUN — no data was changed. Re-run with --commit to delete.")
            return 0

        # ── Confirmation gate ────────────────────────────────────────────
        print(f"\nThis will permanently delete every PowMr point in the range "
              f"({n_points} total).")
        print(f'Type exactly:  {CONFIRM_PHRASE}')
        try:
            typed = input("> ").strip()
        except EOFError:
            typed = ""
        if typed != CONFIRM_PHRASE:
            print("Confirmation phrase did not match — aborting.")
            return 1

        # ── Per-reg bulk deletes ─────────────────────────────────────────
        # InfluxDB v2's delete predicate language doesn't support OR, so we
        # issue one delete per PowMr reg, each covering the full window.
        # Growatt's decimal-keyed regs are not in `powmr_regs`, so they're
        # untouched.
        delete_api = client.delete_api()
        print(f"\nDeleting all PowMr data ({len(powmr_regs)} regs × full range)...")

        failures = 0
        for i, reg in enumerate(powmr_regs, 1):
            try:
                delete_api.delete(
                    start=args.start,
                    stop=args.stop,
                    predicate=f'_measurement="modbus" AND reg="{reg}"',
                    bucket=args.bucket,
                    org=args.org,
                )
                print(f"  [{i}/{len(powmr_regs)}] reg={reg}  OK")
            except Exception as e:
                failures += 1
                print(f"  [{i}/{len(powmr_regs)}] reg={reg}  FAILED: {e}")
                if failures >= 5:
                    sys.exit("Too many failures — aborting.")

        print(f"\nDone. {len(powmr_regs) - failures}/{len(powmr_regs)} deletes succeeded, "
              f"{failures} failure(s).")
        return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
