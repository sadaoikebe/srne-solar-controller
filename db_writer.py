import requests, yaml
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

API_URL = "http://influxdb:5004/registers"
SCHEMA_PATH = "regmap.yaml"

INFLUX_URL    = os.environ["INFLUX_URL"]
INFLUX_TOKEN  = os.environ["INFLUX_TOKEN"]
INFLUX_ORG    = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]

def fetch_registers() -> Optional[Dict[str, int]]:
    try:
        r = requests.get(API_URL, timeout=8)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def load_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def now_utc_ns() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1e9)

def to_signed_16_relaxed(x: int) -> int:
    if x < 0:
        return x
    return x - 0x10000 if x >= 0x8000 else x

def combine_uint32(hi: int, lo: int) -> int:
    return ((hi & 0xFFFF) << 16) | (lo & 0xFFFF)

def combine_auto(reg_key: str, left_val: int, right_val: int) -> int:
    """
    キーが 'A-B' のとき:
      - PowMr(16進 '0x...' で始まる) は [lo, hi] の順に BE 結合
      - Growatt(10進) は [hi, lo] の順に BE 結合
    """
    if reg_key.startswith("0x"):
        return combine_uint32(right_val, left_val)
    else:
        return combine_uint32(left_val, right_val)

def build_point(ts_ns: int, reg_key: str, meta: Dict[str, Any], value: float, raw_int: int) -> Point:
    p = Point("modbus").time(ts_ns).tag("reg", reg_key)
    if "name" in meta: p = p.tag("name", str(meta["name"]))
    if "unit" in meta: p = p.tag("unit", str(meta["unit"]))
    return p.field("value", float(value)).field("raw", int(raw_int))

def transform_to_points(data: Dict[str, int], schema: Dict[str, Any]) -> List[Point]:
    ts_ns = now_utc_ns()
    out: List[Point] = []

    for key, meta in (schema or {}).items():
        if not isinstance(meta, dict) or "name" not in meta:
            continue
        scale = float(meta.get("scale", 1.0))

        if "-" in key:
            left, right = key.split("-", 1)
            if left not in data or right not in data:
                continue
            raw32 = combine_auto(key, int(data[left]), int(data[right]))
            out.append(build_point(ts_ns, key, meta, float(raw32) * scale, raw32))
            continue

        if key not in data:
            continue
        raw = int(data[key])
        val = to_signed_16_relaxed(raw) if meta.get("signed") else raw
        out.append(build_point(ts_ns, key, meta, float(val) * scale, raw))

    return out

def write_points(points: List[Point]) -> None:
    if not points:
        return
    client = influxdb_client.InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    try:
        with client.write_api(write_options=SYNCHRONOUS) as w:
            w.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    finally:
        client.close()

def wait_until_next_minute():
    """次の1分開始点まで待機"""
    now = datetime.now()
    next_minute_start = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    delay = (next_minute_start - now).total_seconds()
    if delay > 0:
        time.sleep(delay)
    return next_minute_start

# メインループ
mapcfg = load_schema(SCHEMA_PATH)
print("Starting DB writer script...")
next_minute_start = wait_until_next_minute()
while True:
    print(f"Processing at {datetime.now()}")
    register_data = fetch_registers()
    write_points(transform_to_points(register_data, mapcfg))

    # 次の1分まで待機
    next_minute_start = wait_until_next_minute()
