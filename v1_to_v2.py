#!/usr/bin/env python3
# v1_to_v2.py — InfluxDB 1.x -> 2.x migration
# pip install influxdb influxdb-client pyyaml

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Set
import yaml

# ---- InfluxDB 1.x (source) ----
from influxdb import InfluxDBClient as Influx1
V1_HOST = "localhost"
V1_PORT = 8086
V1_DB   = "mysolardb"
MEASUREMENT_SRC = "registers"

# ---- InfluxDB 2.x (dest) ----
import influxdb_client
from influxdb_client import Point

import os
from dotenv import load_dotenv

load_dotenv()

# .env から値を取得
V2_ORG = os.getenv("INFLUXDB2_ORG")
V2_BUCKET = os.getenv("INFLUXDB2_BUCKET")
V2_TOKEN = os.getenv("INFLUXDB2_TOKEN")  # ← これを使う

from influxdb_client.client.write_api import SYNCHRONOUS

V2_URL    = "http://192.168.1.216:8086"
MEASUREMENT_DST = "modbus"

# ---- Window (UTC) ----
TIME_START = "2025-09-15T03:00:00Z"
TIME_STOP  = "2025-10-31T23:59:00Z"
CHUNK_DAYS = 1

# ---- Schema ----
SCHEMA_PATH = "regmap.yaml"


# ========== utils ==========
def parse_ts_ns(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1e9)

def to_signed_16_relaxed(x: int) -> int:
    if x < 0:
        return x
    return x - 0x10000 if x >= 0x8000 else x

def combine_uint32_be(hi: int, lo: int) -> int:
    return ((hi & 0xFFFF) << 16) | (lo & 0xFFFF)

def load_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ========== new<->old field mapping ==========
def reg_to_old_field_single(k: str) -> Optional[str]:
    # 小文字0x前提（PowMr）/ それ以外はGrowattの10進
    if k.startswith("0x"):
        addr = int(k, 16)
        if 0x0100 <= addr <= 0x010F: return str(addr - 0x0100)                 # 0..15
        if 0x0200 <= addr <= 0x023F: return str(16 + (addr - 0x0200))           # 16..79
        if 0xF000 <= addr <= 0xF03F: return str(80 + (addr - 0xF000))           # 80..143
        if 0x0110 <= addr <= 0x0111: return str(144 + (addr - 0x0110))          # 144..145
        return None
    else:
        a = int(k)
        if 0 <= a <= 239:  return str(2000 + a)                                 # 2000..2239
        if 720 <= a <= 839: return str(2240 + (a - 720))                        # 2240..2359
        return None

def old_field_to_reg_key(n: int) -> Optional[str]:
    if 0 <= n <= 15:        return f"0x{0x0100 + n:04x}"
    if 16 <= n <= 79:       return f"0x{0x0200 + (n - 16):04x}"
    if 80 <= n <= 143:      return f"0x{0xF000 + (n - 80):04x}"
    if 144 <= n <= 145:     return f"0x{0x0110 + (n - 144):04x}"
    if 2000 <= n <= 2239:   return str(n - 2000)
    if 2240 <= n <= 2359:   return str(720 + (n - 2240))
    return None

def compute_needed_fields(schema: Dict[str, Any]) -> List[str]:
    need: Set[str] = set()
    for key, meta in (schema or {}).items():
        if not isinstance(meta, dict) or "name" not in meta:
            continue
        k = str(key)
        if "-" in k:
            hi, lo = k.split("-", 1)
            for part in (hi, lo):
                f = reg_to_old_field_single(part)
                if f is not None:
                    need.add(f)
        else:
            f = reg_to_old_field_single(k)
            if f is not None:
                need.add(f)
    return sorted(need, key=lambda s: int(s))

def build_point(ts_ns: int, reg_key: str, meta: Dict[str, Any], value: float, raw_int: int) -> Point:
    p = Point(MEASUREMENT_DST).time(ts_ns).tag("reg", reg_key)
    if "name" in meta: p = p.tag("name", str(meta["name"]))
    if "unit" in meta: p = p.tag("unit", str(meta["unit"]))
    return p.field("value", float(value)).field("raw", int(raw_int))

def points_from_row(row: Dict[str, Any], schema: Dict[str, Any]) -> List[Point]:
    ts_ns = parse_ts_ns(row["time"])
    # 旧 "registers.N" の N → 新 reg キーへ
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
            hi, lo = k.split("-", 1)
            if hi not in reg_values or lo not in reg_values:
                continue
            if k.startswith("0x"):
                raw32 = combine_uint32_be(reg_values[lo], reg_values[hi])
            else:
                raw32 = combine_uint32_be(reg_values[hi], reg_values[lo])
            val = float(raw32) * scale
            out.append(build_point(ts_ns, k, meta, val, raw32))
        else:
            if k not in reg_values:
                continue
            raw = reg_values[k]
            val = to_signed_16_relaxed(raw) if meta.get("signed") else raw
            val = float(val) * scale
            out.append(build_point(ts_ns, k, meta, val, raw))
    return out

def migrate():
    schema = load_schema(SCHEMA_PATH)
    need_fields = compute_needed_fields(schema)
    if not need_fields:
        print("[migrate] no fields to migrate (check your flat YAML keys against v1 layout)")
        return

    src = Influx1(host=V1_HOST, port=V1_PORT, database=V1_DB)
    dst = influxdb_client.InfluxDBClient(url=V2_URL, token=V2_TOKEN, org=V2_ORG)
    write_api = dst.write_api(write_options=SYNCHRONOUS)

    t0 = datetime.fromisoformat(TIME_START.replace("Z", "+00:00"))
    t1 = datetime.fromisoformat(TIME_STOP.replace("Z", "+00:00"))

    try:
        cur = t0
        while cur < t1:
            nxt = min(cur + timedelta(days=CHUNK_DAYS), t1)
            sel = ",".join(f'"{f}"' for f in need_fields)
            q = (
                f'SELECT {sel} FROM "{MEASUREMENT_SRC}" '
                f"WHERE time >= '{cur.replace(tzinfo=timezone.utc).isoformat()}' "
                f"AND time < '{nxt.replace(tzinfo=timezone.utc).isoformat()}'"
            )
            print(f"[migrate] {cur.isoformat()} .. {nxt.isoformat()}  fields={len(need_fields)}")
            rs = src.query(q)

            wrote = 0
            if rs:
                for (_series, rows) in rs.items():
                    for row in rows:
                        pts = points_from_row(row, schema)
                        if pts:
                            # --- debug: what we will write (after combine/scaling/sign fix) ---
                            # for p in pts:
                            #     lp = p.to_line_protocol()  # e.g. modbus,reg=0x0109,name=pv1_power value=123,raw=123 172...ns
                            #     try:
                            #         head, rest = lp.split(" ", 1)
                            #         tags = head.split(",")[1:]                      # ["reg=0x0109","name=pv1_power",...]
                            #         reg  = next((t.split("=",1)[1] for t in tags if t.startswith("reg=")), None)
                            #         name = next((t.split("=",1)[1] for t in tags if t.startswith("name=")), None)
                            #         fields_str, ts_ns = rest.rsplit(" ", 1)         # "value=...,raw=..." , "172..."
                            #         fields = dict(s.split("=",1) for s in fields_str.split(","))
                            #         val = fields.get("value")
                            #         raw = fields.get("raw")
                            #         print(f"[migrate:write] ts={row['time']} reg={reg} name={name} value={val} raw={raw}")
                            #     except Exception:
                            #         # フォールバック：そのままLPを出す
                            #         print(f"[migrate:write] lp={lp}")

                            write_api.write(bucket=V2_BUCKET, org=V2_ORG, record=pts)
                            wrote += len(pts)
            print(f"[migrate] wrote points: {wrote}")
            cur = nxt
    finally:
        try:
            write_api.__del__()  # flush
        except Exception:
            pass
        dst.close()
        src.close()

    print("[migrate] done.")

if __name__ == "__main__":
    migrate()
