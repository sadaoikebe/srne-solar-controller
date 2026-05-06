#!/usr/bin/env python3
"""Delete corrupted PowMr records from InfluxDB.

A "bad" timestamp is one where the grid frequency register (0x0215,
_field=value) reads OUTSIDE the normal band [FREQ_MIN_HZ, FREQ_MAX_HZ].
Real grid frequency is always ~50 Hz (East Japan) or ~60 Hz (West Japan)
with a small margin; anything outside the band is a corrupted read
caused by the modbus race condition.

For each such timestamp, every PowMr point at that time (any reg starting
with "0x", as derived from regmap.yaml) is deleted. Growatt data
(decimal-keyed regs) is never touched.

This rule is intentionally aggressive — it deletes the entire PowMr row at
any timestamp where the freq read is bogus, on the assumption that other
PowMr fields at the same instant are likely also corrupted. Some legitimate
data may be removed (e.g. true grid-lost readings reading 0 Hz), which is
considered acceptable for cleaning up race-condition damage.

Default is dry-run. Pass --commit AND type the confirmation phrase to
actually delete.

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

GRID_FREQ_REG = "0x0215"

FREQ_MIN_HZ = 50.0
FREQ_MAX_HZ = 70.0

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

def _common_filter(bucket: str, start: datetime, stop: datetime) -> str:
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: {start.isoformat()}, stop: {stop.isoformat()})\n'
        f'  |> filter(fn: (r) => r._measurement == "modbus" and r._field == "value")'
    )


def find_bad_timestamps(qa, org, bucket, start, stop) -> List[datetime]:
    """Distinct timestamps where 0x0215 (grid frequency) reads outside the normal band."""
    flux = f'''
{_common_filter(bucket, start, stop)}
  |> filter(fn: (r) => r.reg == "{GRID_FREQ_REG}")
  |> filter(fn: (r) => r._value < {FREQ_MIN_HZ} or r._value > {FREQ_MAX_HZ})
  |> keep(columns: ["_time"])
  |> group()
  |> distinct(column: "_time")
  |> sort(columns: ["_value"])
'''
    seen, out = set(), []
    for table in qa.query(flux, org=org):
        for record in table.records:
            ts = record.get_value()
            if ts not in seen:
                seen.add(ts)
                out.append(ts)
    return out


# ── Reporting ────────────────────────────────────────────────────────────────

def print_sample(timestamps: List[datetime], n: int = 5) -> None:
    if not timestamps:
        return
    print("  First few:")
    for ts in timestamps[:n]:
        print(f"    {ts.astimezone(JST).isoformat()}")
    if len(timestamps) > 2 * n:
        print(f"    ... ({len(timestamps) - 2*n} more)")
    if len(timestamps) > n:
        print("  Last few:")
        for ts in timestamps[-n:]:
            print(f"    {ts.astimezone(JST).isoformat()}")


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
    print("Delete corrupted PowMr records")
    print(f"  Range (JST)  : {args.start.isoformat()}  →  {args.stop.isoformat()}")
    print(f"  Bucket       : {args.bucket}")
    print(f"  PowMr regs   : {len(powmr_regs)} from regmap.yaml")
    print(f"  Criterion    : grid freq (0x0215) outside [{FREQ_MIN_HZ:.1f}, {FREQ_MAX_HZ:.1f}] Hz")
    print(f"  Mode         : {'COMMIT (will delete)' if args.commit else 'DRY-RUN'}")
    print("=" * 70)

    with InfluxDBClient(url=args.url, token=args.token, org=args.org, timeout=600_000) as client:
        qa = client.query_api()

        # ── Distinct bad timestamps ──────────────────────────────────────
        print(f"\nFinding timestamps where grid freq is outside "
              f"[{FREQ_MIN_HZ:.1f}, {FREQ_MAX_HZ:.1f}] Hz...")
        bad_ts = find_bad_timestamps(qa, args.org, args.bucket, args.start, args.stop)
        print(f"  {len(bad_ts)} unique timestamp(s) to clean")
        print_sample(bad_ts)

        if not bad_ts:
            print("\nNothing to do.")
            return 0

        est_points = len(bad_ts) * len(powmr_regs)
        print(f"\nEstimated PowMr points to delete: ~{est_points} "
              f"({len(bad_ts)} timestamps × {len(powmr_regs)} regs)")

        if not args.commit:
            print("\nDRY-RUN — no data was changed. Re-run with --commit to delete.")
            return 0

        # ── Confirmation gate ────────────────────────────────────────────
        print("\nThis will permanently delete the points listed above.")
        print(f'Type exactly:  {CONFIRM_PHRASE}')
        try:
            typed = input("> ").strip()
        except EOFError:
            typed = ""
        if typed != CONFIRM_PHRASE:
            print("Confirmation phrase did not match — aborting.")
            return 1

        # ── Per-(timestamp, reg) deletes ─────────────────────────────────
        # InfluxDB v2's delete predicate language doesn't support OR, so we
        # can't list all PowMr regs in a single predicate. One delete per
        # (timestamp, reg) pair is the only correct workaround — narrow time
        # window keeps each call cheap.
        delete_api  = client.delete_api()
        total_calls = len(bad_ts) * len(powmr_regs)
        print(f"\nDeleting {total_calls} (timestamp × reg) pairs "
              f"({len(bad_ts)} timestamps × {len(powmr_regs)} regs)...")

        failures = 0
        done     = 0
        for i, ts in enumerate(bad_ts, 1):
            start_ns = ts - timedelta(microseconds=1)
            stop_ns  = ts + timedelta(microseconds=1)
            for reg in powmr_regs:
                done += 1
                try:
                    delete_api.delete(
                        start=start_ns,
                        stop=stop_ns,
                        predicate=f'_measurement="modbus" AND reg="{reg}"',
                        bucket=args.bucket,
                        org=args.org,
                    )
                except Exception as e:
                    failures += 1
                    print(f"  [{done}/{total_calls}] FAILED ts={ts.astimezone(JST).isoformat()} "
                          f"reg={reg}: {e}")
                    if failures >= 5:
                        sys.exit("Too many failures — aborting.")
            if i % 10 == 0 or i == len(bad_ts):
                print(f"  [{i}/{len(bad_ts)} ts] {ts.astimezone(JST).isoformat()}")

        print(f"\nDone. {done - failures}/{total_calls} delete calls succeeded, "
              f"{failures} failure(s).")
        return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
