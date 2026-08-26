#!/usr/bin/env python3
"""Read-only per-cell LFP statistics from existing Influx `jkbms` history.

Queries InfluxDB. Writes nothing — no new measurement, no Grafana, no BMS I/O.

Usage:
  python jkbms_cell_health_report.py
  python jkbms_cell_health_report.py --start "2026-08-08" --stop "2026-08-22 12:00"
  python jkbms_cell_health_report.py --bank a --format json
  python jkbms_cell_health_report.py --i-rest 1.2 --no-auto-i-rest
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from influxdb_client import InfluxDBClient

from jkbms_cell_health_metrics import (
    HealthConfig,
    PackSample,
    analyze_bank,
    bank_report_to_dict,
    format_markdown,
    load_config,
    samples_from_influx_rows,
)

JST = timezone(timedelta(hours=9))
PROJECT_ROOT = Path(__file__).resolve().parent
DOTENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_CONFIG = PROJECT_ROOT / "jkbms_cell_health.yaml"
CHUNK = timedelta(days=1)
QUERY_TIMEOUT_MS = 300_000

def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def parse_when(s: str) -> datetime:
    """JST wall time, or RFC3339 UTC with Z / offset."""
    raw = s.strip()
    if raw.endswith("Z"):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as e:
            raise argparse.ArgumentTypeError(f"invalid RFC3339: {s!r}") from e
    if "+" in raw[1:] or raw.count("-") >= 3:
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is not None:
                return dt
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=JST)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"invalid datetime {s!r} (use JST 'YYYY-MM-DD[ HH:MM]' or RFC3339 with Z)"
    )


def _rfc3339(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _flux_pack(bucket: str, bank: str, start: str, stop: str) -> str:
    return f'''
from(bucket: "{bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "jkbms" and r._field == "value" and r.bank == "{bank}")
  |> filter(fn: (r) => r.name != "cell_voltage")
  |> keep(columns: ["_time", "bank", "serial", "name", "_value"])
'''


def _flux_cells(bucket: str, bank: str, start: str, stop: str) -> str:
    return f'''
from(bucket: "{bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "jkbms" and r._field == "value"
      and r.bank == "{bank}" and r.name == "cell_voltage")
  |> keep(columns: ["_time", "bank", "cell", "_value"])
'''


def _records(qa, flux: str, org: str) -> List[Any]:
    out: List[Any] = []
    for table in qa.query(flux, org=org):
        out.extend(table.records)
    return out


def _fold_pack(records: Sequence[Any]) -> List[Dict[str, Any]]:
    by: Dict[Tuple[Any, str], Dict[str, Any]] = {}
    for rec in records:
        t = rec.get_time()
        bank = str(rec.values.get("bank") or "")
        name = rec.values.get("name")
        if not bank or not name:
            continue
        row = by.setdefault(
            (t, bank),
            {"_time": t, "bank": bank, "serial": rec.values.get("serial") or bank},
        )
        if rec.values.get("serial"):
            row["serial"] = rec.values.get("serial")
        row[str(name)] = rec.get_value()
    return list(by.values())


def _fold_cells(records: Sequence[Any]) -> List[Dict[str, Any]]:
    by: Dict[Tuple[Any, str], Dict[str, Any]] = {}
    for rec in records:
        t = rec.get_time()
        bank = str(rec.values.get("bank") or "")
        cell = rec.values.get("cell")
        if not bank or cell is None:
            continue
        key = str(cell).zfill(2)
        row = by.setdefault((t, bank), {"_time": t, "bank": bank})
        row[key] = rec.get_value()
    return list(by.values())


def fetch_bank_samples(
    qa,
    *,
    org: str,
    bucket: str,
    bank: str,
    start: datetime,
    stop: datetime,
) -> Tuple[List[PackSample], int]:
    pack_rows: List[Dict[str, Any]] = []
    cell_rows: List[Dict[str, Any]] = []
    cursor = start
    while cursor < stop:
        chunk_end = min(cursor + CHUNK, stop)
        s, e = _rfc3339(cursor), _rfc3339(chunk_end)
        pack_rows.extend(_fold_pack(_records(qa, _flux_pack(bucket, bank, s, e), org)))
        cell_rows.extend(_fold_cells(_records(qa, _flux_cells(bucket, bank, s, e), org)))
        cursor = chunk_end

    samples = samples_from_influx_rows(pack_rows, cell_rows)
    n_pack_ticks = len({(r["_time"], r["bank"]) for r in pack_rows})
    n_cell_ticks = len({(r["_time"], r["bank"]) for r in cell_rows})
    dropped = max(n_pack_ticks, n_cell_ticks) - len(samples)
    return samples, max(0, dropped)


def discovery_range(qa, org: str, bucket: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    flux = f'''
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "jkbms" and r._field == "value" and r.name == "cell_voltage")
  |> group()
  |> first()
  |> keep(columns: ["_time"])
'''
    flux_last = f'''
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "jkbms" and r._field == "value" and r.name == "cell_voltage")
  |> group()
  |> last()
  |> keep(columns: ["_time"])
'''
    first = last = None
    for table in qa.query(flux, org=org):
        for rec in table.records:
            first = rec.get_time()
    for table in qa.query(flux_last, org=org):
        for rec in table.records:
            last = rec.get_time()
    return first, last


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv(DOTENV_PATH)

    parser = argparse.ArgumentParser(
        description="Read-only per-cell LFP statistics from Influx measurement jkbms.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start", type=parse_when, default=None, help="JST start (default: first jkbms point)")
    parser.add_argument("--stop", type=parse_when, default=None, help="JST stop (default: now)")
    parser.add_argument("--bank", choices=("a", "b"), default=None, help="Single bank (default: both)")
    parser.add_argument("--format", choices=("md", "json"), default="md", dest="fmt")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="YAML thresholds")
    parser.add_argument("--i-rest", type=float, default=None, help="Override rest |I| threshold (A)")
    parser.add_argument(
        "--no-auto-i-rest",
        action="store_true",
        help="Do not raise i_rest from the overnight current histogram",
    )
    parser.add_argument("--url", default=os.environ.get("INFLUX_URL", "http://127.0.0.1:8086"))
    parser.add_argument("--token", default=os.environ.get("INFLUX_TOKEN"))
    parser.add_argument("--org", default=os.environ.get("INFLUX_ORG"))
    parser.add_argument("--bucket", default=os.environ.get("INFLUX_BUCKET"))
    parser.add_argument("-o", "--output", default=None, help="Write report to a file instead of stdout")
    args = parser.parse_args(argv)

    if not (args.token and args.org and args.bucket):
        print("Missing INFLUX_TOKEN / INFLUX_ORG / INFLUX_BUCKET (env, .env, or flags).", file=sys.stderr)
        return 2

    cfg: HealthConfig = load_config(args.config)
    if args.i_rest is not None:
        cfg = cfg.with_i_rest(args.i_rest)

    banks = [args.bank] if args.bank else ["a", "b"]

    print(
        f"Querying Influx {args.url}  org={args.org}  bucket={args.bucket}  (read-only)",
        file=sys.stderr,
    )

    with InfluxDBClient(
        url=args.url, token=args.token, org=args.org, timeout=QUERY_TIMEOUT_MS
    ) as client:
        qa = client.query_api()
        first, last = discovery_range(qa, args.org, args.bucket)
        if first is None or last is None:
            print("No jkbms cell_voltage points in this bucket.", file=sys.stderr)
            return 1
        start = args.start if args.start is not None else first
        stop = args.stop if args.stop is not None else datetime.now(timezone.utc)
        if start.tzinfo is None:
            start = start.replace(tzinfo=JST)
        if stop.tzinfo is None:
            stop = stop.replace(tzinfo=JST)
        if start >= stop:
            print("--start must be before --stop", file=sys.stderr)
            return 2

        reports = []
        for bank in banks:
            print(f"  bank {bank}: {_rfc3339(start)} → {_rfc3339(stop)} …", file=sys.stderr)
            samples, dropped = fetch_bank_samples(
                qa,
                org=args.org,
                bucket=args.bucket,
                bank=bank,
                start=start,
                stop=stop,
            )
            print(f"    {len(samples)} aligned samples, {dropped} incomplete ticks dropped", file=sys.stderr)
            if not samples:
                print(f"    no complete samples for bank {bank}", file=sys.stderr)
                continue
            reports.append(
                analyze_bank(
                    samples,
                    cfg,
                    n_incomplete_dropped=dropped,
                    auto_i_rest=not args.no_auto_i_rest,
                )
            )

    if not reports:
        print("Nothing to report.", file=sys.stderr)
        return 1

    if args.fmt == "json":
        text = json.dumps([bank_report_to_dict(r) for r in reports], indent=2, default=str)
    else:
        text = format_markdown(reports, cfg=cfg)

    if args.output:
        Path(args.output).write_text(text + ("" if text.endswith("\n") else "\n"), encoding="utf-8")
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text if text.endswith("\n") else text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
