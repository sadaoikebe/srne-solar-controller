#!/usr/bin/env python3
"""Delete corrupted PowMr records from InfluxDB.

A "bad" timestamp is one where ANY of the following holds in the modbus
measurement, _field=value:

  - reg in (0x0108, 0x0110) AND value >= 25.0   (PV current; max possible ~13 A)
  - reg == 0x0100          AND value >= 110.0   (battery SoC > 100%)
  - reg == 0x0215          AND value >= 100.0   (grid frequency)

For each such timestamp, every PowMr point at that time (any reg starting
with "0x", as derived from regmap.yaml) is deleted. Growatt data
(decimal-keyed regs) is never touched.

Default is dry-run. Pass --commit AND type the confirmation phrase to actually
delete. The script prints per-criterion counts, a sample of bad timestamps,
and a final summary so you can sanity-check before committing.

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

PV_CURRENT_REGS  = ("0x0108", "0x0110")
SOC_REG          = "0x0100"
GRID_FREQ_REG    = "0x0215"

PV_CURRENT_THRESHOLD = 25.0    # A — physically impossible per string
SOC_THRESHOLD        = 110.0   # % — over 100, impossible
FREQ_THRESHOLD       = 100.0   # Hz — never a real grid value

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


def count_criterion(qa, org, bucket, start, stop, regs, threshold) -> int:
    reg_filter = " or ".join(f'r.reg == "{r}"' for r in regs)
    flux = f'''
{_common_filter(bucket, start, stop)}
  |> filter(fn: (r) => {reg_filter})
  |> filter(fn: (r) => r._value >= {threshold})
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


def find_bad_timestamps(qa, org, bucket, start, stop) -> List[datetime]:
    pv_filter = " or ".join(f'r.reg == "{r}"' for r in PV_CURRENT_REGS)
    flux = f'''
bad_pv =
{_common_filter(bucket, start, stop)}
  |> filter(fn: (r) => {pv_filter})
  |> filter(fn: (r) => r._value >= {PV_CURRENT_THRESHOLD})
  |> keep(columns: ["_time"])

bad_soc =
{_common_filter(bucket, start, stop)}
  |> filter(fn: (r) => r.reg == "{SOC_REG}")
  |> filter(fn: (r) => r._value >= {SOC_THRESHOLD})
  |> keep(columns: ["_time"])

bad_freq =
{_common_filter(bucket, start, stop)}
  |> filter(fn: (r) => r.reg == "{GRID_FREQ_REG}")
  |> filter(fn: (r) => r._value >= {FREQ_THRESHOLD})
  |> keep(columns: ["_time"])

union(tables: [bad_pv, bad_soc, bad_freq])
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
    print(f"  Mode         : {'COMMIT (will delete)' if args.commit else 'DRY-RUN'}")
    print("=" * 70)

    with InfluxDBClient(url=args.url, token=args.token, org=args.org, timeout=600_000) as client:
        qa = client.query_api()

        # ── Per-criterion counts (sanity check) ──────────────────────────
        print("\nPer-criterion match counts:")
        n_pv = count_criterion(qa, args.org, args.bucket, args.start, args.stop,
                               PV_CURRENT_REGS, PV_CURRENT_THRESHOLD)
        n_soc = count_criterion(qa, args.org, args.bucket, args.start, args.stop,
                                (SOC_REG,), SOC_THRESHOLD)
        n_freq = count_criterion(qa, args.org, args.bucket, args.start, args.stop,
                                 (GRID_FREQ_REG,), FREQ_THRESHOLD)
        print(f"  PV current  >= {PV_CURRENT_THRESHOLD:>5} A : {n_pv}")
        print(f"  Battery SoC >= {SOC_THRESHOLD:>5} %  : {n_soc}")
        print(f"  Grid freq   >= {FREQ_THRESHOLD:>5} Hz : {n_freq}")

        # ── Distinct bad timestamps ──────────────────────────────────────
        print("\nResolving distinct bad timestamps...")
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
