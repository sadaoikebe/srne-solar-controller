#!/usr/bin/env python3
# relabel_reg_case.py
#
# Fixes a tag-case inconsistency in the InfluxDB v2 `modbus` measurement:
# many PowMr hex-keyed registers exist as BOTH `reg=0xF03C` (uppercase) AND
# `reg=0xf03c` (lowercase) variants. Different series, dashboards collapse
# them silently, daily totals come out wrong.
#
# Direction: lowercase is canonical (it's the dominant historical form, and
# db_writer now emits lowercase via `reg_key.lower()`). This script
# converts the small uppercase batch back to lowercase, then deletes the
# uppercase originals.
#
# Phases (run them explicitly, in order):
#
#   discover   List uppercase (name, reg, samples). Read-only.
#   rewrite    For every uppercase reg, write a copy with reg.lower().
#              Single Flux query: filter -> map -> to(). Idempotent.
#   verify     Confirm lowercase counts cover the original uppercase counts.
#              Read-only.
#   delete     Delete the uppercase-reg points. Destructive — only run
#              after `verify` looks right.
#
# Default phase is `discover`. There is no `--phase all`.
# Connection details come from .env (same as the v1->v2 migrators).

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from influxdb_client.client.delete_api import DeleteApi

load_dotenv()

V2_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
V2_ORG = os.getenv("INFLUX_ORG")
V2_BUCKET = os.getenv("INFLUX_BUCKET")
V2_TOKEN = os.getenv("INFLUX_TOKEN")
MEASUREMENT = "modbus"

# Wide enough to cover all data; Flux requires an explicit range start.
TIME_RANGE_START = "2020-01-01T00:00:00Z"
TIME_RANGE_STOP = "2099-01-01T00:00:00Z"

# ── Flux queries ──────────────────────────────────────────────────────────


def _flux_discover_uppercase() -> str:
    # Regex matches any reg containing an uppercase hex digit (A..F).
    # The 'x' in '0x...' is lowercase by convention so it doesn't match.
    # Decimal regs ("10", "83") and already-canonical lowercase regs
    # ("0xf03c") are NOT flagged.
    return f'''
from(bucket: "{V2_BUCKET}")
  |> range(start: {TIME_RANGE_START})
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r._field == "value")
  |> filter(fn: (r) => r.reg =~ /[A-F]/)
  |> group(columns: ["name", "reg"])
  |> count()
  |> keep(columns: ["name", "reg", "_value"])
  |> rename(columns: {{_value: "samples"}})
  |> group()
  |> sort(columns: ["name"])
'''


def _flux_count_by_reg(name: str, reg: str) -> str:
    return f'''
from(bucket: "{V2_BUCKET}")
  |> range(start: {TIME_RANGE_START})
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r._field == "value")
  |> filter(fn: (r) => r.name == "{name}" and r.reg == "{reg}")
  |> count()
'''


def _flux_rewrite_upper_to_lower() -> str:
    # Filter every reg containing an uppercase hex digit, lowercase the
    # whole tag value, and write back. `to()` keys on the row's current
    # column values, so the rewritten point has a new (lowercase) tag set
    # and is stored as a new series — the uppercase originals stay in place
    # until the `delete` phase.
    return f'''
import "strings"

from(bucket: "{V2_BUCKET}")
  |> range(start: {TIME_RANGE_START})
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r.reg =~ /[A-F]/)
  |> map(fn: (r) => ({{r with reg: strings.toLower(v: r.reg)}}))
  |> to(bucket: "{V2_BUCKET}", org: "{V2_ORG}")
'''


# ── Helpers ───────────────────────────────────────────────────────────────


def to_canonical_reg(reg: str) -> str:
    """Canonical form: all-lowercase. Mirrors the Flux rewrite step.
    e.g. '0xF03C' -> '0xf03c', '0xF034-0xF035' -> '0xf034-0xf035'.
    Decimal regs ('10', '83') unchanged.
    """
    return reg.lower()


def log(msg: str) -> None:
    print(msg, flush=True)


def _client() -> InfluxDBClient:
    if not (V2_URL and V2_ORG and V2_BUCKET and V2_TOKEN):
        log("ERROR: missing env vars (INFLUX_URL/ORG/BUCKET/TOKEN). "
            "Source .env first.")
        sys.exit(1)
    # The rewrite query touches every modbus point in the bucket (~3M rows on
    # this deployment). The default 10 s read timeout is way too short.
    return InfluxDBClient(
        url=V2_URL, token=V2_TOKEN, org=V2_ORG,
        timeout=600_000,  # 10 minutes, in ms
    )


def _confirm(prompt: str) -> bool:
    ans = input(f"{prompt} [yes/NO]: ").strip().lower()
    return ans == "yes"


# ── Phases ────────────────────────────────────────────────────────────────


def phase_discover(client: InfluxDBClient) -> List[Tuple[str, str, int]]:
    """Return a list of (name, uppercase_reg, sample_count)."""
    log(f"Bucket: {V2_BUCKET}")
    log("Searching for uppercase-hex reg tags (case-collision candidates)...\n")
    tables = client.query_api().query(_flux_discover_uppercase(), org=V2_ORG)
    rows: List[Tuple[str, str, int]] = []
    for t in tables:
        for r in t.records:
            rows.append((r.values.get("name"), r.values.get("reg"),
                         int(r.values.get("samples"))))
    rows.sort()

    if not rows:
        log("No uppercase-hex reg tags found. Nothing to do.")
        return rows

    log(f"{'name':30s}  {'reg':24s}  samples")
    log("-" * 70)
    for name, reg, n in rows:
        log(f"{name:30s}  {reg:24s}  {n:>10d}")
    log(f"\nTotal: {len(rows)} affected (name, reg) pairs, "
        f"{sum(n for _, _, n in rows):,} samples to relabel.")
    return rows


def phase_rewrite(client: InfluxDBClient) -> None:
    log("Rewriting uppercase-reg points with lowercase reg via Flux to() ...")
    # query_api with a query that ends in `to()` runs the write as a side
    # effect; the query yields no records.
    client.query_api().query(_flux_rewrite_upper_to_lower(), org=V2_ORG)
    log("Rewrite query submitted. Run `--phase verify` to confirm.")


def phase_verify(client: InfluxDBClient,
                 uppercase_rows: List[Tuple[str, str, int]]) -> bool:
    """For each (name, uppercase_reg, n_up), check that the lowercase
    counterpart now has at least n_up samples (it should have far more,
    since lowercase is the dominant historical form). Returns True iff
    all pass.
    """
    log("Verifying lowercase reg counts >= original uppercase counts ...\n")
    qa = client.query_api()
    all_ok = True
    log(f"{'name':30s}  {'reg':24s}  {'upper→lower':>15s}  status")
    log("-" * 90)
    for name, up_reg, n_up in uppercase_rows:
        low_reg = to_canonical_reg(up_reg)
        tables = qa.query(_flux_count_by_reg(name, low_reg), org=V2_ORG)
        n_low = 0
        for t in tables:
            for r in t.records:
                v = r.get_value()
                if v is not None:
                    n_low += int(v)
        status = "OK" if n_low >= n_up else "MISSING"
        if n_low < n_up:
            all_ok = False
        log(f"{name:30s}  {up_reg:24s}  {n_up:>6d} → {n_low:<6d}  {status}")
    log("")
    if all_ok:
        log("All lowercase counts cover the original uppercase counts. Safe to delete.")
    else:
        log("Some lowercase counts are SHORT. Do NOT run --phase delete.")
        log("Re-run --phase rewrite, then --phase verify again.")
    return all_ok


def phase_delete(client: InfluxDBClient,
                 uppercase_rows: List[Tuple[str, str, int]],
                 yes: bool) -> None:
    if not uppercase_rows:
        log("Nothing to delete.")
        return

    log("About to DELETE the following uppercase-reg series:\n")
    for name, reg, n in uppercase_rows:
        log(f"  {name:30s}  reg={reg:24s}  samples={n}")
    log(f"\nBucket: {V2_BUCKET}")
    log(f"Time range: {TIME_RANGE_START} -> {TIME_RANGE_STOP}")
    log("This is destructive. The lowercase counterparts must already cover "
        "this data (you ran --phase verify, right?).\n")

    if not yes and not _confirm("Proceed with delete?"):
        log("Aborted.")
        return

    delete_api: DeleteApi = client.delete_api()
    for name, reg, _ in uppercase_rows:
        predicate = f'_measurement="{MEASUREMENT}" AND reg="{reg}"'
        log(f"  deleting reg={reg} ...")
        delete_api.delete(
            start=TIME_RANGE_START,
            stop=TIME_RANGE_STOP,
            predicate=predicate,
            bucket=V2_BUCKET,
            org=V2_ORG,
        )
    log("\nDelete complete.")


# ── CLI ───────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix lowercase/uppercase reg tag inconsistency in the "
                    "InfluxDB v2 `modbus` measurement."
    )
    parser.add_argument(
        "--phase",
        choices=["discover", "rewrite", "verify", "delete"],
        default="discover",
        help="Which phase to run (default: discover)",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Skip the interactive confirmation in --phase delete",
    )
    args = parser.parse_args()

    client = _client()
    try:
        rows = phase_discover(client)

        if args.phase == "discover":
            return
        if args.phase == "rewrite":
            phase_rewrite(client)
            return
        if args.phase == "verify":
            phase_verify(client, rows)
            return
        if args.phase == "delete":
            ok = phase_verify(client, rows)
            if not ok:
                log("\nVerify failed. Refusing to delete.")
                sys.exit(2)
            phase_delete(client, rows, yes=args.yes)
            return
    finally:
        client.close()


if __name__ == "__main__":
    main()
