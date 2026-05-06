#!/usr/bin/env python3
"""Delete temp_translator_powmr records from InfluxDB.

Background: register 0x0222 was originally added to regmap.yaml as
"temp_translator_powmr". "Translator" is a mistranslation of the Chinese
变压器 — the correct English term is "transformer". The regmap has been
corrected, so all new writes carry name="temp_transformer_powmr". The
transformer copy already exists for the affected window, so this script
just removes the residual translator-tagged rows. No migration / no copy.

Why this is a delete (not a rename) and how the reserved-word bug is fixed:
the previous rename script tried to delete with predicate
`name="temp_translator_powmr"`, but `name` is a reserved word in
InfluxDB v2's delete-predicate parser. We sidestep that by deleting
per-timestamp using ONLY `_measurement` and `reg` — Flux QUERIES still
accept `r.name`, so the translator timestamps are located that way first.

Side effect: at any timestamp where transformer also exists (e.g. samples
the earlier failed rename script wrote a copy at), the per-(ts, reg)
delete will remove that transformer point too, since the predicate cannot
distinguish names. This is acceptable — re-migration is out of scope and
the dashboard runs on transformer data at later timestamps anyway.

Default is dry-run. Pass --commit AND type the confirmation phrase to
actually delete. Always run dry-run first to confirm scope.

Usage:
  python scripts/rename_translator_to_transformer.py \\
      --start "2026-05-04 00:00" --stop "2026-05-05 12:00"

  python scripts/rename_translator_to_transformer.py \\
      --start "2026-05-04 00:00" --stop "2026-05-05 12:00" --commit
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from influxdb_client import InfluxDBClient

# ── Constants ────────────────────────────────────────────────────────────────

JST = timezone(timedelta(hours=9))
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOTENV_PATH  = PROJECT_ROOT / ".env"

OLD_NAME       = "temp_translator_powmr"
TARGET_REG     = "0x0222"  # only register that ever carried the old name
CONFIRM_PHRASE = "delete translator"


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

def find_translator_timestamps(qa, org, bucket, start, stop) -> List[datetime]:
    """Distinct timestamps where the translator-named point exists.

    Flux QUERIES allow filtering on `r.name`, so we use it here to locate
    the rows. The reserved-word restriction only applies to the delete
    predicate parser, not to Flux.
    """
    flux = f'''
from(bucket: "{bucket}")
  |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
  |> filter(fn: (r) =>
        r._measurement == "modbus"
        and r.name == "{OLD_NAME}"
        and r.reg == "{TARGET_REG}"
     )
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
        description=f"Delete {OLD_NAME} records in InfluxDB (dry-run by default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start", type=parse_jst, required=True,
                        help='JST datetime, e.g. "2026-05-04 00:00"')
    parser.add_argument("--stop",  type=parse_jst, required=True,
                        help='JST datetime, e.g. "2026-05-05 12:00"')
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

    print("=" * 70)
    print(f"Delete {OLD_NAME}")
    print(f"  Range (JST)  : {args.start.isoformat()}  →  {args.stop.isoformat()}")
    print(f"  Bucket       : {args.bucket}")
    print(f"  Target reg   : {TARGET_REG}")
    print(f"  Mode         : {'COMMIT (will delete)' if args.commit else 'DRY-RUN'}")
    print("=" * 70)

    with InfluxDBClient(url=args.url, token=args.token, org=args.org, timeout=600_000) as client:
        qa = client.query_api()

        print(f'\nFinding timestamps with name="{OLD_NAME}" AND reg="{TARGET_REG}"...')
        timestamps = find_translator_timestamps(qa, args.org, args.bucket, args.start, args.stop)
        print(f"  {len(timestamps)} unique timestamp(s) carry the old name")
        print_sample(timestamps)

        if not timestamps:
            print("\nNothing to do.")
            return 0

        if not args.commit:
            print("\nDRY-RUN — no data was changed. Re-run with --commit to delete.")
            return 0

        # ── Confirmation gate ────────────────────────────────────────────
        print(f'\nThis will permanently delete every point at the listed timestamps')
        print(f'matching _measurement="modbus" AND reg="{TARGET_REG}"')
        print(f"(also removes any transformer-tagged copy at those exact timestamps).")
        print(f'\nType exactly:  {CONFIRM_PHRASE}')
        try:
            typed = input("> ").strip()
        except EOFError:
            typed = ""
        if typed != CONFIRM_PHRASE:
            print("Confirmation phrase did not match — aborting.")
            return 1

        # ── Per-timestamp deletes ────────────────────────────────────────
        # `name` is a reserved word in InfluxDB v2's delete predicate parser,
        # so we predicate only on `_measurement` and `reg`. To avoid touching
        # other timestamps at this reg, we delete in a 1µs window around
        # each translator timestamp.
        delete_api = client.delete_api()
        print(f"\nDeleting {len(timestamps)} timestamp(s)...")

        failures = 0
        for i, ts in enumerate(timestamps, 1):
            start_ns = ts - timedelta(microseconds=1)
            stop_ns  = ts + timedelta(microseconds=1)
            try:
                delete_api.delete(
                    start=start_ns,
                    stop=stop_ns,
                    predicate=f'_measurement="modbus" AND reg="{TARGET_REG}"',
                    bucket=args.bucket,
                    org=args.org,
                )
            except Exception as e:
                failures += 1
                print(f"  [{i}/{len(timestamps)}] FAILED ts={ts.astimezone(JST).isoformat()}: {e}")
                if failures >= 5:
                    sys.exit("Too many failures — aborting.")
            if i % 10 == 0 or i == len(timestamps):
                print(f"  [{i}/{len(timestamps)}] {ts.astimezone(JST).isoformat()}")

        # ── Verify ───────────────────────────────────────────────────────
        print("\nVerifying...")
        remaining = find_translator_timestamps(qa, args.org, args.bucket, args.start, args.stop)
        if remaining:
            print(f"  WARNING: {len(remaining)} timestamp(s) with old name still present.")
            return 2
        print("  OK — 0 timestamps under old name remain.")
        print(f"\nDone. {len(timestamps) - failures}/{len(timestamps)} delete calls succeeded, "
              f"{failures} failure(s).")
        return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
