"""Battery charge controller.

Polls the inverter every POLL_INTERVAL_S seconds via modbus_api (V / I / load
and writes). Pack SoC comes from soc_estimator GET /soc — not PowMr 0x0100.
Charge abort (I = 0) comes from jkbms_api GET /bms: either charge MOSFET off
or cell_max >= CELL_MAX_ABORT_V (CELL_CALIBRATE_ABORT_V during CALIBRATE).

Full-charge nights run CC → SOAK → CALIBRATE on max(cell), not clocked SYNC.
Daily (NORMAL) charging is CC to target_soc until the cell knee, then cell-CV.

Log levels
----------
  DEBUG  — raw register values, SoC estimator steps, grid-limit arithmetic,
           cell-CV arithmetic, per-tick loop heartbeat
  INFO   — state transitions, charge-current changes, priority changes,
           config reloads, startup/shutdown, BMS abort clear, full-charge phases
  WARNING — fetch failures, config-file errors (non-fatal), BMS charge abort
  ERROR  — currently unused (caller should watch WARNING closely)
"""
from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum, IntEnum


import requests

from log_config import get_logger

log = get_logger("battery_controller")

# ── Hardware / system constants ───────────────────────────────────────────────

POLL_INTERVAL_S: int = 5

GRID_MAX_POWER_W: float    = 9000.0    # Maximum grid power budget (W)
HYSTERESIS_SOC:   float    = 2.0       # SoC hysteresis band (%)
IDLE_SOC_PCT_PER_H: float  = 0.25      # Cheap SBU pad: overnight DC idle (% / h)
NEAR_TARGET_SOC:  float    = 0.4       # Near-target band (%)
CUTOFF_SOC:       float    = 9.0       # Emergency SoC floor (%)
SBU_RETURN_MIN_SOC: float  = 13.0      # Minimum SoC (%) to (re)enter SBU during sbu_fixed.
                                       # Set above CUTOFF_SOC so a rainy-day rebound to
                                       # V>51.6 V on a barely-recovered battery doesn't
                                       # flap back into SBU only to trip out again.
SBU_TO_UTI_COOLDOWN_S: int = 30 * 60   # Minimum seconds between SBU→UTI switches
FAIL_SAFE_TICKS:       int = 60        # After this many consecutive fetch failures
                                       # (60 × 5 s = 5 min), force SBU → UTI_STOPPED
                                       # to stop discharging the battery without monitoring.
SOC_STALE_S:          float = 30.0     # /soc age_s above this → treat as missing
CELL_MIN_FLOOR_V:     float = 3.05     # weakest-cell floor (matches estimator empty_cell_v)
BMS_STALE_S:          float = 25.0     # /bms bank age_s above this → bank not used
CELL_MAX_ABORT_V:     float = 3.55     # JK OVPR; I = 0 at or above this (not CALIBRATE)

# ── Full-charge (CC → SOAK → CALIBRATE) ──────────────────────────────────────
# Triggered by daily_target.py setting "full_charge: true" in targets.json.
# CC fills at up to CC_MAX_CURRENT until any cell hits CELL_KNEE_V.
# SOAK holds max(cell) at CELL_SOAK_V so the 2 A balancer can run.
# CALIBRATE creeps to CELL_CALIBRATE_V (JK / estimator full snap). Abort
# during CALIBRATE is CELL_CALIBRATE_ABORT_V so 3.59 is reachable.

CC_MAX_CURRENT:            float = 120.0
CELL_KNEE_V:               float = 3.45
CELL_SOAK_V:               float = 3.50
CELL_CALIBRATE_V:          float = 3.59
CELL_CALIBRATE_ABORT_V:    float = 3.62  # below OVP 3.65; allows 3.59 creep
PACK_KNEE_V:               float = 55.2  # backstop to enter SOAK if BLE is down
PACK_ABORT_V:              float = 56.8  # ~3.55 × 16; hard I = 0
I_SLEW_A:                  float = 10.0  # A per poll tick
SOAK_HOLD_CURRENT_A:       float = 20.0  # still feeding lagging cells at 3.50 V
CALIBRATE_MAX_CURRENT:     float = 10.0
SOAK_MIN_CELL_V:           float = 3.45
SOAK_TAIL_CURRENT_A:       float = 15.0
SOAK_TAIL_HOLD_S:          int   = 180
SOAK_DELTA_V:              float = 0.020
SOAK_MIN_DURATION_S:       int   = 45 * 60
CALIBRATE_RESERVE_S:       int   = 8 * 60
MOSFET_RESUME_CELL_V:      float = 3.48
MOSFET_RESUME_COOLDOWN_S:  int   = 90
MOSFET_RESUME_CURRENT_A:   float = 8.0
FULL_REMAIN_FRAC:          float = 0.995
BLE_DVDT_HORIZON_S:        float = 10.0

PACK_VOLT_LIMITS: list[tuple[float, float]] = [
    (55.2, 120), (55.6, 80), (55.8, 60), (56.0, 40),
    (56.3, 30), (56.5, 24), (56.6, 18), (56.7, 14),
    (56.8, 10), (56.9, 7),
]

# ── Runtime configuration ─────────────────────────────────────────────────────

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/targets.json")

_API_PORT: int = int(os.getenv("MODBUS_API_PORT", "5004"))
_API_BASE: str = f"http://modbus_api:{_API_PORT}"

LIMITED_REGISTERS_URL:   str = f"{_API_BASE}/limited_registers"
SET_CHARGE_CURRENT_URL:  str = f"{_API_BASE}/set_charge_current"
SET_OUTPUT_PRIORITY_URL: str = f"{_API_BASE}/set_output_priority"
SOC_API_URL: str = os.getenv("SOC_API_URL", "http://host.docker.internal:5006/soc")
BMS_API_URL: str = os.getenv("BMS_API_URL", "http://host.docker.internal:5005/bms")

_AUTH_USER = os.getenv("BASIC_AUTH_USER")
_AUTH_PASS = os.getenv("BASIC_AUTH_PASS")
_API_AUTH: tuple[str, str] | None = (
    (_AUTH_USER, _AUTH_PASS) if _AUTH_USER and _AUTH_PASS else None
)

# ── Enums ─────────────────────────────────────────────────────────────────────


class OutputPriority(IntEnum):
    SOL = 0
    UTI = 1
    SBU = 2


class State(Enum):
    UTI_CHARGING = "UTI_CHARGING"
    UTI_STOPPED  = "UTI_STOPPED"
    SBU          = "SBU"


class ChargeMode(Enum):
    NORMAL    = "NORMAL"     # Daily SoC-target fill; cell-CV if max(cell) hits the knee
    CC        = "CC"         # Full charge: CC until any cell ≥ CELL_KNEE_V
    SOAK      = "SOAK"       # Full charge: hold max(cell) at CELL_SOAK_V (balancer window)
    CALIBRATE = "CALIBRATE"  # Full charge: creep max(cell) to CELL_CALIBRATE_V


CHARGE_MODE_CODE: dict[ChargeMode, int] = {
    ChargeMode.NORMAL: 0,
    ChargeMode.CC: 1,
    ChargeMode.SOAK: 2,
    ChargeMode.CALIBRATE: 3,
}
CONTROLLER_STATE_CODE: dict[State, int] = {
    State.UTI_STOPPED: 0,
    State.UTI_CHARGING: 1,
    State.SBU: 2,
}
CHARGE_CONTROL_MEASUREMENT = "charge_control"


# ── Time-period helpers ───────────────────────────────────────────────────────

# Edit these entries to match your electricity tariff schedule.
TIME_PERIODS: list[dict] = [
    {"name": "cheap",     "start": "23:01", "end": "6:58"},
    {"name": "sbu_fixed", "start": "6:59",  "end": "23:00"},
]


def _str_to_time(s: str) -> datetime.time:
    try:
        return datetime.strptime(s, "%H:%M").time()
    except ValueError:
        raise ValueError(f"Invalid time format '{s}'. Expected H:MM or HH:MM.")


def _time_in_period(
    current: datetime.time,
    start: datetime.time,
    end: datetime.time,
) -> bool:
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def get_time_period() -> str:
    now = datetime.now().time()
    for period in TIME_PERIODS:
        if _time_in_period(now, _str_to_time(period["start"]), _str_to_time(period["end"])):
            return period["name"]
    return "unknown"


# ── Register helpers ──────────────────────────────────────────────────────────


def _to_signed_16(raw: int) -> int:
    """Reinterpret a uint16 as a signed int16; pass-through if already negative."""
    if raw < 0:
        return raw
    return raw - 0x10000 if raw >= 0x8000 else raw


# ── I/O helpers ───────────────────────────────────────────────────────────────


def fetch_registers() -> dict | None:
    """Fetch the limited register set from modbus_api. Returns None on failure."""
    try:
        r = requests.get(LIMITED_REGISTERS_URL, timeout=3)
        r.raise_for_status()
        data = r.json()
        log.debug(
            "Registers fetched: SoC=%s%%  raw_V=%s  raw_I=%s  load_L1=%s W  load_L2=%s W",
            data.get("0x0100"), data.get("0x0101"),
            data.get("0x0102"), data.get("0x021c"), data.get("0x0234"),
        )
        return data
    except requests.RequestException as e:
        log.warning("Register fetch failed: %s", e)
        return None


def fetch_soc() -> dict | None:
    """Fetch pack SoC from soc_estimator. Returns None on failure."""
    try:
        r = requests.get(SOC_API_URL, timeout=2)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else None
    except requests.RequestException as e:
        log.warning("SoC fetch failed: %s", e)
        return None


def interpret_soc(
    payload: dict | None,
    *,
    max_age_s: float = SOC_STALE_S,
) -> tuple[float | None, float | None, str]:
    """Return (soc_pack, cell_min, status). status is ok | missing | stale | invalid."""
    if not payload:
        return None, None, "missing"
    try:
        age = payload.get("age_s")
        soc = payload.get("soc_pack")
        cell_min = payload.get("cell_min")
        if soc is None:
            return None, None, "invalid"
        if age is not None and float(age) > max_age_s:
            return None, None, "stale"
        cmin = float(cell_min) if cell_min is not None else None
        return float(soc), cmin, "ok"
    except (TypeError, ValueError):
        return None, None, "invalid"


def fetch_bms() -> dict | None:
    """Fetch JK snapshot from jkbms_api. Returns None on failure."""
    try:
        r = requests.get(BMS_API_URL, timeout=2)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else None
    except requests.RequestException as e:
        log.warning("BMS fetch failed: %s", e)
        return None


def _charge_mosfet_off(value: object) -> bool:
    """True only for an explicit off reading (bool False or 0). Missing is not off."""
    if value is False:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0:
        return True
    return False


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class BmsView:
    fresh: bool
    abort: bool
    reason: str
    cell_max: float | None = None
    cell_min: float | None = None
    cell_delta: float | None = None
    mosfet_off: bool = False
    bank_cell_max: dict[str, float] = field(default_factory=dict)
    hot_bank: str | None = None
    hot_cell: int | None = None  # 1-based, matches JK app (A06)
    balance_current: float | None = None
    bank_balance_current: dict[str, float] = field(default_factory=dict)


@dataclass
class FullChargeState:
    mode: ChargeMode = ChargeMode.NORMAL
    soak_started_at: datetime | None = None
    tail_ok_since: datetime | None = None
    complete: bool = False
    complete_reason: str | None = None


def parse_bms_view(
    payload: dict | None,
    *,
    max_age_s: float = BMS_STALE_S,
    cell_max_abort_v: float = CELL_MAX_ABORT_V,
) -> BmsView:
    """Fresh ok banks only. abort uses cell_max_abort_v (3.55, or 3.62 in CALIBRATE)."""
    if not payload:
        return BmsView(fresh=False, abort=False, reason="missing")
    banks = payload.get("banks")
    if not isinstance(banks, dict) or not banks:
        return BmsView(fresh=False, abort=False, reason="invalid")

    cell_maxes: list[float] = []
    cell_mins: list[float] = []
    deltas: list[float] = []
    bank_cell_max: dict[str, float] = {}
    bank_balance: dict[str, float] = {}
    mosfet_off_banks: list[str] = []
    hot_bank: str | None = None
    hot_cell: int | None = None
    hot_v: float | None = None
    saw_fresh = False
    for name, sample in banks.items():
        if not isinstance(sample, dict) or not sample.get("ok"):
            continue
        age = _as_float(sample.get("age_s"))
        if age is not None and age > max_age_s:
            continue
        saw_fresh = True
        bank_name = str(name)
        if _charge_mosfet_off(sample.get("charge_mosfet")):
            mosfet_off_banks.append(bank_name)
        cmax = _as_float(sample.get("cell_max"))
        cmin = _as_float(sample.get("cell_min"))
        if cmax is not None:
            cell_maxes.append(cmax)
            bank_cell_max[bank_name] = cmax
        if cmin is not None:
            cell_mins.append(cmin)
        delta = _as_float(sample.get("cell_delta"))
        if delta is None and cmax is not None and cmin is not None:
            delta = cmax - cmin
        if delta is not None:
            deltas.append(delta)
        bal = _as_float(sample.get("balance_current"))
        if bal is not None:
            bank_balance[bank_name] = bal
        cells = sample.get("cells")
        cell_idx: int | None = None
        cell_v: float | None = cmax
        if isinstance(cells, list) and cells:
            nums: list[float] = []
            for item in cells:
                fv = _as_float(item)
                if fv is None:
                    nums.append(float("-inf"))
                else:
                    nums.append(fv)
            if any(v != float("-inf") for v in nums):
                idx0 = max(range(len(nums)), key=lambda i: nums[i])
                cell_idx = idx0 + 1
                cell_v = nums[idx0]
        if cell_v is not None and (hot_v is None or cell_v > hot_v):
            hot_v = cell_v
            hot_bank = bank_name
            hot_cell = cell_idx

    if not saw_fresh:
        return BmsView(fresh=False, abort=False, reason="stale")

    cell_max = max(cell_maxes) if cell_maxes else None
    cell_min = min(cell_mins) if cell_mins else None
    cell_delta = max(deltas) if deltas else None
    mosfet_off = bool(mosfet_off_banks)
    balance = None
    if bank_balance:
        balance = max(bank_balance.values(), key=lambda x: abs(x))
    if mosfet_off:
        reason = "mosfet_off"
        abort = True
    elif cell_max is not None and cell_max >= cell_max_abort_v:
        reason = "cell_max"
        abort = True
    else:
        reason = "ok"
        abort = False
    return BmsView(
        fresh=True,
        abort=abort,
        reason=reason,
        cell_max=cell_max,
        cell_min=cell_min,
        cell_delta=cell_delta,
        mosfet_off=mosfet_off,
        bank_cell_max=bank_cell_max,
        hot_bank=hot_bank,
        hot_cell=hot_cell,
        balance_current=balance,
        bank_balance_current=bank_balance,
    )


def interpret_bms_charge_abort(
    payload: dict | None,
    *,
    max_age_s: float = BMS_STALE_S,
    cell_max_abort_v: float = CELL_MAX_ABORT_V,
) -> tuple[bool, bool, str, float | None]:
    """Return (fresh, abort, reason, cell_max). Wrapper around parse_bms_view."""
    view = parse_bms_view(
        payload, max_age_s=max_age_s, cell_max_abort_v=cell_max_abort_v,
    )
    return view.fresh, view.abort, view.reason, view.cell_max


@dataclass(frozen=True)
class ChargeControlRecord:
    bank: str
    name: str
    unit: str
    value: float


def hot_cell_label(bms: BmsView) -> str:
    if bms.hot_bank and bms.hot_cell is not None:
        return f"{bms.hot_bank}{bms.hot_cell:02d}"
    return "n/a"


def format_charge_tick(
    *,
    mode: ChargeMode,
    i_cmd: float,
    i_pack: float | None,
    bms: BmsView,
    abort: bool,
) -> str:
    """One-line snapshot for logs (V_max, which cell, I_cmd, phase, MOSFET, delta)."""
    def _v(v: float | None) -> str:
        return f"{v:.3f} V" if v is not None else "n/a"

    def _a(v: float | None) -> str:
        return f"{v:.2f} A" if v is not None else "n/a"

    delta = (
        f"{bms.cell_delta * 1000:.0f} mV" if bms.cell_delta is not None else "n/a"
    )
    mos = "off" if bms.mosfet_off else ("on" if bms.fresh else "n/a")
    return (
        f"mode={mode.value} I_cmd={i_cmd:.0f} A I_pack={_a(i_pack)} "
        f"cell_max={_v(bms.cell_max)} ({hot_cell_label(bms)}) "
        f"cell_min={_v(bms.cell_min)} delta={delta} "
        f"bal={_a(bms.balance_current)} MOSFET={mos} abort={'yes' if abort else 'no'}"
    )


def build_charge_control_records(
    *,
    mode: ChargeMode,
    state: State,
    i_cmd: float,
    i_pack: float | None,
    bms: BmsView,
    abort: bool,
) -> list[ChargeControlRecord]:
    """Influx records for measurement charge_control (field=value, like soc_estimate)."""
    recs: list[ChargeControlRecord] = [
        ChargeControlRecord("pack", "charge_mode", "code", float(CHARGE_MODE_CODE[mode])),
        ChargeControlRecord(
            "pack", "controller_state", "code", float(CONTROLLER_STATE_CODE[state]),
        ),
        ChargeControlRecord("pack", "i_cmd", "A", float(i_cmd)),
        ChargeControlRecord("pack", "bms_abort", "bool", 1.0 if abort else 0.0),
        ChargeControlRecord("pack", "charge_mosfet", "bool", 0.0 if bms.mosfet_off else 1.0),
    ]
    if i_pack is not None:
        recs.append(ChargeControlRecord("pack", "i_pack", "A", float(i_pack)))
    if bms.cell_max is not None:
        recs.append(ChargeControlRecord("pack", "cell_max", "V", bms.cell_max))
    if bms.cell_min is not None:
        recs.append(ChargeControlRecord("pack", "cell_min", "V", bms.cell_min))
    if bms.cell_delta is not None:
        recs.append(ChargeControlRecord("pack", "cell_delta", "V", bms.cell_delta))
    if bms.balance_current is not None:
        recs.append(ChargeControlRecord("pack", "balance_current", "A", bms.balance_current))
    if bms.hot_cell is not None:
        recs.append(ChargeControlRecord("pack", "hot_cell", "count", float(bms.hot_cell)))
    if bms.hot_bank:
        recs.append(ChargeControlRecord(
            "pack", "hot_bank", "code", 0.0 if bms.hot_bank == "a" else 1.0,
        ))
    for bank, val in bms.bank_balance_current.items():
        recs.append(ChargeControlRecord(bank, "balance_current", "A", val))
    for bank, val in bms.bank_cell_max.items():
        recs.append(ChargeControlRecord(bank, "cell_max", "V", val))
    return recs


_influx_client = None


def write_charge_control(records: list[ChargeControlRecord]) -> None:
    """Best-effort Influx write. Missing env or errors must not stop control."""
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")
    bucket = os.getenv("INFLUX_BUCKET")
    if not token or not org or not bucket or not records:
        return
    global _influx_client
    try:
        import influxdb_client
        from influxdb_client import Point
        from influxdb_client.client.write_api import SYNCHRONOUS
        if _influx_client is None:
            url = os.getenv("INFLUX_URL", "http://influxdb:8086")
            _influx_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
            log.info("charge_control Influx: %s  org=%s  bucket=%s", url, org, bucket)
        ts_ns = time.time_ns()
        points = [
            Point(CHARGE_CONTROL_MEASUREMENT)
            .time(ts_ns)
            .tag("bank", rec.bank)
            .tag("name", rec.name)
            .tag("unit", rec.unit)
            .field("value", float(rec.value))
            for rec in records
        ]
        with _influx_client.write_api(write_options=SYNCHRONOUS) as w:
            w.write(bucket=bucket, org=org, record=points)
    except Exception as e:
        log.warning("charge_control Influx write failed: %s", e)


def seconds_until_cheap_end(now: datetime | None = None) -> float:
    """Seconds remaining in the cheap window, or 0 if not in it."""
    now = now or datetime.now()
    cheap = next(p for p in TIME_PERIODS if p["name"] == "cheap")
    start = _str_to_time(cheap["start"])
    end = _str_to_time(cheap["end"])
    t = now.time()
    if not _time_in_period(t, start, end):
        return 0.0
    end_dt = datetime.combine(now.date(), end)
    if end < start and t >= start:
        end_dt += timedelta(days=1)
    return max(0.0, (end_dt - now).total_seconds())


def cheap_sbu_exit_soc(
    target_soc: float,
    seconds_left: float,
    *,
    idle_pct_per_h: float = IDLE_SOC_PCT_PER_H,
) -> float:
    """SoC at which cheap-period SBU should stop so idle leak lands on target at 06:58."""
    hours = max(0.0, seconds_left) / 3600.0
    return target_soc + idle_pct_per_h * hours


def pack_volt_current_cap(battery_voltage: float) -> float:
    """Dumb pack-V current cap. Used when cell voltages are missing."""
    for volt_threshold, limit in PACK_VOLT_LIMITS:
        if battery_voltage < volt_threshold:
            return float(limit)
    return 2.0


def slew_current(i_prev: float, i_want: float, i_slew: float = I_SLEW_A) -> float:
    if i_want > i_prev + i_slew:
        return i_prev + i_slew
    if i_want < i_prev - i_slew:
        return i_prev - i_slew
    return i_want


def cell_cv_current(
    *,
    cell_max: float | None,
    v_set: float,
    i_prev: float,
    i_cc: float,
    dv_dt: float | None = None,
    i_slew: float = I_SLEW_A,
    cell_abort_v: float = CELL_MAX_ABORT_V,
    cell_knee_v: float = CELL_KNEE_V,
    i_hold: float = SOAK_HOLD_CURRENT_A,
    ble_horizon_s: float = BLE_DVDT_HORIZON_S,
) -> float | None:
    """CC below the knee; taper toward i_hold by v_set; 0 at abort.

    Returns None when cell_max is missing so the caller can use pack-V.
    """
    i_cc = max(0.0, i_cc)
    if cell_max is None:
        return None
    if cell_max >= cell_abort_v:
        return 0.0
    if dv_dt is not None and dv_dt > 0:
        headroom = cell_abort_v - cell_max
        if headroom / dv_dt <= ble_horizon_s:
            return 0.0
    if cell_max < cell_knee_v:
        i_want = i_cc
    elif cell_max < v_set:
        span = max(v_set - cell_knee_v, 1e-6)
        frac = (cell_max - cell_knee_v) / span
        floor = min(i_hold, i_cc)
        i_want = i_cc + frac * (floor - i_cc)
    else:
        overshoot = cell_max - v_set
        if overshoot >= 0.030:
            i_want = 0.0
        else:
            hold = min(i_hold, i_cc)
            i_want = max(0.0, hold - 800.0 * overshoot)
    i_want = max(0.0, min(i_cc, i_want))
    return slew_current(i_prev, i_want, i_slew)


def banks_at_full(
    bms: BmsView,
    soc_payload: dict | None,
    *,
    full_cell_v: float = CELL_CALIBRATE_V,
    remain_frac: float = FULL_REMAIN_FRAC,
    min_banks: int = 2,
) -> bool:
    """True when both banks have snapped full (cell_max or remain_est)."""
    if bms.fresh and len(bms.bank_cell_max) >= min_banks:
        if all(v >= full_cell_v for v in bms.bank_cell_max.values()):
            return True
    banks = soc_payload.get("banks") if isinstance(soc_payload, dict) else None
    if not isinstance(banks, dict):
        return False
    n = 0
    for entry in banks.values():
        if not isinstance(entry, dict):
            continue
        usable = _as_float(entry.get("usable_ah"))
        remain = _as_float(entry.get("remain_est"))
        if usable is not None and usable > 1.0 and remain is not None:
            if remain >= usable * remain_frac:
                n += 1
    return n >= min_banks


def advance_full_charge(
    state: FullChargeState,
    *,
    now: datetime,
    bms: BmsView,
    pack_v: float,
    pack_charge_a: float,
    soc_payload: dict | None,
    seconds_left: float,
) -> FullChargeState:
    """CC → SOAK → CALIBRATE → done. Does not clear targets.json."""
    mode = ChargeMode.CC if state.mode == ChargeMode.NORMAL else state.mode
    soak_started = state.soak_started_at
    tail_ok_since = state.tail_ok_since

    if mode == ChargeMode.CC:
        enter = False
        if bms.fresh and bms.cell_max is not None and bms.cell_max >= CELL_KNEE_V:
            enter = True
        elif not bms.fresh and pack_v >= PACK_KNEE_V:
            enter = True
        if enter:
            return FullChargeState(mode=ChargeMode.SOAK, soak_started_at=now)
        return FullChargeState(mode=ChargeMode.CC)

    if mode == ChargeMode.SOAK:
        if soak_started is None:
            soak_started = now
        if pack_charge_a <= SOAK_TAIL_CURRENT_A:
            tail_ok_since = tail_ok_since or now
        else:
            tail_ok_since = None
        elapsed = (now - soak_started).total_seconds()
        cells_in = (
            bms.fresh
            and bms.cell_min is not None
            and bms.cell_min >= SOAK_MIN_CELL_V
        )
        delta_ok = bms.cell_delta is not None and bms.cell_delta <= SOAK_DELTA_V
        tail_held = (
            tail_ok_since is not None
            and (now - tail_ok_since).total_seconds() >= SOAK_TAIL_HOLD_S
        )
        quality = cells_in and (delta_ok or tail_held)
        go_cal = False
        if quality and elapsed >= SOAK_MIN_DURATION_S:
            go_cal = True
        if (
            seconds_left <= CALIBRATE_RESERVE_S
            and bms.fresh
            and bms.cell_max is not None
            and bms.cell_max >= CELL_SOAK_V - 0.02
        ):
            go_cal = True
        if go_cal:
            return FullChargeState(
                mode=ChargeMode.CALIBRATE, soak_started_at=soak_started,
            )
        return FullChargeState(
            mode=ChargeMode.SOAK,
            soak_started_at=soak_started,
            tail_ok_since=tail_ok_since,
        )

    if mode == ChargeMode.CALIBRATE:
        if banks_at_full(bms, soc_payload):
            return FullChargeState(
                mode=ChargeMode.NORMAL,
                complete=True,
                complete_reason="calibrated (cell_max or remain_est at full)",
            )
        return FullChargeState(
            mode=ChargeMode.CALIBRATE, soak_started_at=state.soak_started_at,
        )

    return FullChargeState(mode=mode, soak_started_at=soak_started, tail_ok_since=tail_ok_since)


def set_charge_current(current: float) -> bool:
    try:
        r = requests.post(
            SET_CHARGE_CURRENT_URL,
            json={"value": current},
            timeout=3,
            auth=_API_AUTH,
        )
        r.raise_for_status()
        result = r.json()
        if result.get("success"):
            log.info("Charge current set to %.0f A", current)
            return True
        log.warning("set_charge_current API error: %s", result.get("message"))
        return False
    except requests.RequestException as e:
        log.warning("set_charge_current request failed: %s", e)
        return False


def set_output_priority(priority: int) -> bool:
    priority_name = OutputPriority(priority).name if priority in [e.value for e in OutputPriority] else str(priority)
    try:
        r = requests.post(
            SET_OUTPUT_PRIORITY_URL,
            json={"value": priority},
            timeout=3,
            auth=_API_AUTH,
        )
        r.raise_for_status()
        result = r.json()
        if result.get("success"):
            log.info("Output priority set to %s", result.get("value", priority_name))
            return True
        log.warning("set_output_priority API error: %s", result.get("message"))
        return False
    except requests.RequestException as e:
        log.warning("set_output_priority request failed: %s", e)
        return False


def _read_targets_file() -> dict:
    """Read targets.json; return {} on any error."""
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def update_targets_json(daily_charge_current: float, target_soc: float) -> None:
    """Write daily_charge_current and target_soc to targets.json, preserving other keys
    (full_charge, last_full_charge) so the full-charge bookkeeping isn't clobbered."""
    targets = _read_targets_file()
    targets["target_soc"] = target_soc
    targets["daily_charge_current"] = daily_charge_current
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        log.info(
            "targets.json updated: target_soc=%.0f%%  daily_charge_current=%.0f A",
            target_soc, daily_charge_current,
        )
    except Exception as e:
        log.warning("Failed to write targets.json: %s", e)


def _complete_full_charge() -> None:
    """Mark full-charge as completed: clear the flag and record today's date."""
    targets = _read_targets_file()
    targets["full_charge"] = False
    targets["last_full_charge"] = date.today().isoformat()
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        log.info(
            "Full charge completed: cleared full_charge flag, last_full_charge=%s",
            targets["last_full_charge"],
        )
    except Exception as e:
        log.warning("Failed to write targets.json on full-charge completion: %s", e)


def load_manual_override() -> tuple[State, datetime] | None:
    """Read manual_override from targets.json; return (state, expires_at) or None.

    A malformed or expired override is treated as absent.  An expired override
    is also pruned from the file so it doesn't keep getting parsed each tick.
    """
    targets = _read_targets_file()
    raw = targets.get("manual_override")
    if not raw:
        return None
    try:
        state   = State(raw["state"])
        expires = datetime.fromisoformat(raw["expires_at"])
    except Exception as e:
        log.warning("Invalid manual_override (%s) — ignoring", e)
        return None

    now = datetime.now(expires.tzinfo) if expires.tzinfo else datetime.now()
    if now >= expires:
        targets.pop("manual_override", None)
        try:
            with open(CONFIG_PATH, "w") as f:
                json.dump(targets, f)
            log.info("Manual override expired (was %s) — cleared from targets.json", state.value)
        except Exception as e:
            log.warning("Failed to clear expired override: %s", e)
        return None
    return state, expires


def load_targets_from_file(
    current_daily_charge_current: float,
    current_target_soc: float,
) -> tuple[float, float, bool]:
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
        daily = float(targets.get("daily_charge_current", current_daily_charge_current))
        soc   = float(targets.get("target_soc", current_target_soc))
        full_charge = bool(targets.get("full_charge", False))
        log.debug(
            "Targets loaded from file: target_soc=%.0f%%  daily_charge_current=%.0f A  full_charge=%s",
            soc, daily, full_charge,
        )
        return daily, soc, full_charge
    except Exception as e:
        log.warning(
            "Failed to load targets.json: %s — keeping target_soc=%.0f%%  daily_charge_current=%.0f A",
            e, current_target_soc, current_daily_charge_current,
        )
        return current_daily_charge_current, current_target_soc, False


# ── Control logic ─────────────────────────────────────────────────────────────


def calculate_grid_limit_current(load_power: float, battery_voltage: float) -> float:
    """Maximum charge current (A) without exceeding the grid power budget."""
    grid_headroom = GRID_MAX_POWER_W - load_power
    if 30.0 < battery_voltage < 70.0:
        limit = math.floor((grid_headroom / battery_voltage) / 5.0) * 5.0
        log.debug(
            "Grid limit: headroom=%.0f W  voltage=%.1f V  → %.0f A",
            grid_headroom, battery_voltage, limit,
        )
        return limit
    log.warning(
        "Battery voltage %.1f V is outside safe range (30–70 V) — grid limit set to 0", battery_voltage
    )
    return 0.0


def determine_next_state(
    current_state: State,
    estimated_soc: float | None,
    target_soc: float,
    battery_voltage: float,
    time_period: str,
    daily_charge_current: float,
    last_sbu_to_uti_time: datetime | None,
    full_charge_active: bool = False,
    cell_min_v: float | None = None,
    seconds_left_cheap: float | None = None,
) -> tuple[State, float, datetime | None]:
    """Compute the next control state and any side-effects on targets.

    Returns (next_state, new_daily_charge_current, new_last_sbu_to_uti_time).
    """
    if estimated_soc is None:
        log.debug("estimated_soc not yet available — holding state %s", current_state.value)
        return current_state, daily_charge_current, last_sbu_to_uti_time

    next_state = current_state
    lower_charge_current = False
    new_daily_charge_current = daily_charge_current
    new_last_sbu_to_uti_time = last_sbu_to_uti_time

    now = datetime.now()
    cooldown_elapsed = (
        last_sbu_to_uti_time is None
        or (now - last_sbu_to_uti_time).total_seconds() >= SBU_TO_UTI_COOLDOWN_S
    )

    if time_period == "sbu_fixed":
        if current_state == State.UTI_CHARGING:
            if battery_voltage > 51.6 and estimated_soc > SBU_RETURN_MIN_SOC:
                if cooldown_elapsed:
                    next_state = State.SBU
                else:
                    remaining = SBU_TO_UTI_COOLDOWN_S - (now - last_sbu_to_uti_time).total_seconds()
                    log.debug(
                        "UTI→SBU suppressed: cooldown active (%.0f s remaining)", remaining
                    )
            elif battery_voltage > 50.6:
                next_state = State.UTI_STOPPED

        elif current_state == State.UTI_STOPPED:
            if battery_voltage > 51.6 and estimated_soc > SBU_RETURN_MIN_SOC:
                if cooldown_elapsed:
                    next_state = State.SBU
                else:
                    remaining = SBU_TO_UTI_COOLDOWN_S - (now - last_sbu_to_uti_time).total_seconds()
                    log.debug(
                        "UTI→SBU suppressed: cooldown active (%.0f s remaining)", remaining
                    )
            elif battery_voltage < 49.4:
                next_state = State.UTI_CHARGING

        else:  # State.SBU
            if battery_voltage < 49.4:
                next_state = State.UTI_CHARGING
                new_last_sbu_to_uti_time = now
                log.info(
                    "SBU→UTI_CHARGING triggered: low voltage %.1f V (threshold 49.4 V)"
                    " — cooldown started",
                    battery_voltage,
                )
            elif battery_voltage < 49.6 or estimated_soc <= CUTOFF_SOC:
                next_state = State.UTI_STOPPED
                new_last_sbu_to_uti_time = now
                log.info(
                    "SBU→UTI_STOPPED triggered: voltage=%.1f V  est_SoC=%.1f%%"
                    " (cutoff=%.0f%%) — cooldown started",
                    battery_voltage, estimated_soc, CUTOFF_SOC,
                )

    elif time_period == "cheap":
        # Full charge in progress: stay in UTI_CHARGING regardless of SoC vs target.
        # Phase progression and current calculation are handled by the main loop.
        if full_charge_active:
            if current_state != State.UTI_CHARGING:
                log.info(
                    "Full charge active: forcing %s → UTI_CHARGING in cheap period",
                    current_state.value,
                )
                next_state = State.UTI_CHARGING
            return next_state, daily_charge_current, last_sbu_to_uti_time

        if seconds_left_cheap is None:
            seconds_left_cheap = seconds_until_cheap_end(now)
        sbu_exit = cheap_sbu_exit_soc(target_soc, seconds_left_cheap)
        can_buy = daily_charge_current > 0

        if current_state == State.UTI_CHARGING:
            if not can_buy:
                next_state = State.UTI_STOPPED
                lower_charge_current = True
                log.debug("Cheap: daily charge 0 A — UTI_CHARGING → UTI_STOPPED")
            elif estimated_soc > sbu_exit + NEAR_TARGET_SOC:
                next_state = State.SBU
                lower_charge_current = True
                log.debug(
                    "Cheap: SoC %.1f%% > SBU-exit+band %.1f%% → SBU + lower current",
                    estimated_soc, sbu_exit + NEAR_TARGET_SOC,
                )
            elif estimated_soc > target_soc + NEAR_TARGET_SOC:
                next_state = State.UTI_STOPPED
                lower_charge_current = True
                log.debug(
                    "Cheap: SoC %.1f%% > target+band %.1f%% → UTI_STOPPED + lower current",
                    estimated_soc, target_soc + NEAR_TARGET_SOC,
                )

        elif current_state == State.UTI_STOPPED:
            if can_buy and estimated_soc < target_soc - NEAR_TARGET_SOC:
                next_state = State.UTI_CHARGING
                log.debug(
                    "Cheap: SoC %.1f%% < target-band %.1f%% → UTI_CHARGING",
                    estimated_soc, target_soc - NEAR_TARGET_SOC,
                )
            elif estimated_soc > sbu_exit + HYSTERESIS_SOC:
                next_state = State.SBU
                log.debug(
                    "Cheap: SoC %.1f%% > SBU-exit+hysteresis %.1f%% → SBU",
                    estimated_soc, sbu_exit + HYSTERESIS_SOC,
                )

        else:  # State.SBU
            if can_buy and estimated_soc < target_soc - NEAR_TARGET_SOC:
                next_state = State.UTI_CHARGING
                log.debug(
                    "Cheap: SoC %.1f%% < target-band %.1f%% → UTI_CHARGING",
                    estimated_soc, target_soc - NEAR_TARGET_SOC,
                )
            elif estimated_soc < sbu_exit + NEAR_TARGET_SOC:
                next_state = State.UTI_STOPPED
                log.debug(
                    "Cheap: SoC %.1f%% < SBU-exit+band %.1f%% (pad to target %.0f%%) → UTI_STOPPED",
                    estimated_soc, sbu_exit + NEAR_TARGET_SOC, target_soc,
                )

    else:
        # "unknown" time period — no transitions, hold current state
        log.debug("Time period 'unknown' — holding state %s", current_state.value)

    if (
        cell_min_v is not None
        and cell_min_v <= CELL_MIN_FLOOR_V
        and next_state == State.SBU
    ):
        log.info(
            "Blocking SBU: cell_min %.3f V <= floor %.2f V → UTI_STOPPED",
            cell_min_v, CELL_MIN_FLOOR_V,
        )
        next_state = State.UTI_STOPPED
        if current_state == State.SBU:
            new_last_sbu_to_uti_time = now

    if lower_charge_current:
        new_daily_charge_current = min(10.0, daily_charge_current)
        if new_daily_charge_current != daily_charge_current:
            update_targets_json(new_daily_charge_current, target_soc)
            log.info(
                "Daily charge current lowered: %.0f A → %.0f A",
                daily_charge_current, new_daily_charge_current,
            )

    return next_state, new_daily_charge_current, new_last_sbu_to_uti_time


def adjust_battery_charge(
    battery_soc: float,
    load_power: float,
    battery_voltage: float,
    daily_charge_current: float,
    state: State,
    charge_mode: ChargeMode = ChargeMode.NORMAL,
    bms_abort: bool = False,
    cell_max: float | None = None,
    dv_dt: float | None = None,
    i_prev: float = 0.0,
    mosfet_resume_hold: bool = False,
) -> float:
    """Return the target charge current (A) for the given state and conditions."""
    if state in (State.SBU, State.UTI_STOPPED):
        log.debug("Charge current = 0 A (state=%s)", state.value)
        return 0.0
    if bms_abort:
        log.debug("Charge current = 0 A (BMS abort)")
        return 0.0
    if battery_voltage >= PACK_ABORT_V:
        log.warning(
            "Pack abort: voltage %.2f V >= %.2f V — charge current 0 A",
            battery_voltage, PACK_ABORT_V,
        )
        return 0.0

    grid_limit = calculate_grid_limit_current(load_power, battery_voltage)
    if mosfet_resume_hold:
        if cell_max is not None and cell_max >= MOSFET_RESUME_CELL_V:
            log.debug("MOSFET resume hold: cell_max %.3f V still high — 0 A", cell_max)
            return 0.0
        target = min(MOSFET_RESUME_CURRENT_A, grid_limit)
        final = slew_current(i_prev, target)
        log.debug(
            "MOSFET resume: cap=%.0f A  grid=%.0f A  i_prev=%.0f A → %.0f A",
            MOSFET_RESUME_CURRENT_A, grid_limit, i_prev, final,
        )
        return float(round(final))

    if charge_mode == ChargeMode.CALIBRATE:
        i_cc = CALIBRATE_MAX_CURRENT
        v_set = CELL_CALIBRATE_V
        abort_v = CELL_CALIBRATE_ABORT_V
    elif charge_mode in (ChargeMode.CC, ChargeMode.SOAK):
        i_cc = CC_MAX_CURRENT
        v_set = CELL_SOAK_V
        abort_v = CELL_MAX_ABORT_V
    else:
        i_cc = daily_charge_current
        v_set = CELL_SOAK_V
        abort_v = CELL_MAX_ABORT_V

    cell_i = cell_cv_current(
        cell_max=cell_max,
        v_set=v_set,
        i_prev=i_prev,
        i_cc=i_cc,
        dv_dt=dv_dt,
        cell_abort_v=abort_v,
    )
    if cell_i is None:
        # No cell voltages: pack-V table. During SOAK/CALIBRATE, stay conservative.
        pack_cap = pack_volt_current_cap(battery_voltage)
        if charge_mode in (ChargeMode.SOAK, ChargeMode.CALIBRATE) and battery_voltage >= PACK_KNEE_V:
            target = min(SOAK_HOLD_CURRENT_A, i_cc, pack_cap, grid_limit)
        else:
            target = min(i_cc, pack_cap, grid_limit)
        final = slew_current(i_prev, target)
        log.debug(
            "%s charge (pack-V): cap=%.0f A  pack_cap=%.0f A (V=%.2f V)  "
            "grid=%.0f A  SoC=%.0f%%  → %.0f A",
            charge_mode.value, i_cc, pack_cap, battery_voltage, grid_limit,
            battery_soc, final,
        )
        return float(round(max(0.0, final)))

    final = min(cell_i, grid_limit)
    log.debug(
        "%s charge (cell-CV): cap=%.0f A  cell_max=%s  v_set=%.2f V  "
        "grid=%.0f A  i_prev=%.0f A  → %.0f A",
        charge_mode.value, i_cc,
        f"{cell_max:.3f}" if cell_max is not None else "n/a",
        v_set, grid_limit, i_prev, final,
    )
    return float(round(max(0.0, final)))


def determine_output_priority(state: State) -> OutputPriority:
    return OutputPriority.SBU if state == State.SBU else OutputPriority.UTI


# ── Main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    log.info("=" * 60)
    log.info("Battery charge controller starting")
    log.info("  Poll interval : %d s", POLL_INTERVAL_S)
    log.info("  SoC API       : %s", SOC_API_URL)
    log.info("  BMS API       : %s", BMS_API_URL)
    log.info("  BMS abort     : MOSFET off or cell_max >= %.2f V", CELL_MAX_ABORT_V)
    log.info("  Grid budget   : %.0f W", GRID_MAX_POWER_W)
    log.info("  SBU→UTI cooldown: %d s", SBU_TO_UTI_COOLDOWN_S)
    log.info("  API base      : %s", _API_BASE)
    log.info("  Auth          : %s", "enabled" if _API_AUTH else "disabled (no credentials)")
    log.info("  Config file   : %s", CONFIG_PATH)
    log.info("  Time periods  : %s",
             "  ".join(f"{p['name']} ({p['start']}–{p['end']})" for p in TIME_PERIODS))
    log.info(
        "  Full charge   : CC ≤%.0fA → SOAK cell %.2f V → CALIBRATE %.2f V "
        "(abort %.2f / %.2f V)",
        CC_MAX_CURRENT, CELL_SOAK_V, CELL_CALIBRATE_V,
        CELL_MAX_ABORT_V, CELL_CALIBRATE_ABORT_V,
    )
    log.info("=" * 60)

    last_charge_current:  float                  = 0.0
    daily_charge_current: float                  = 0.0
    target_soc:           float                  = 90.0
    last_output_priority: OutputPriority | None  = None
    estimated_soc:        float | None           = None
    cell_min_v:           float | None           = None
    bms_abort:            bool                   = False
    bms_abort_reason:     str                    = "ok"
    bms_abort_ticks:      int                    = 0
    bms_cell_max_held:    float | None           = None
    current_state:        State                  = State.UTI_STOPPED  # safe default until first data
    battery_voltage:      float                  = 52.0
    last_sbu_to_uti_time: datetime | None        = None
    consecutive_failures: int                    = 0
    soc_fail_ticks:       int                    = 0
    charge_mode:          ChargeMode             = ChargeMode.NORMAL
    fc_state:             FullChargeState        = FullChargeState()
    last_cell_max:        float | None           = None
    last_cell_max_at:     datetime | None        = None
    mosfet_resume_until:  datetime | None        = None
    soc_payload:          dict | None            = None
    bms:                  BmsView                = BmsView(fresh=False, abort=False, reason="missing")
    pack_charge_a:        float                  = 0.0
    i_pack:               float | None           = None
    load_power:           float                  = 0.0
    fc_log_ticks:         int                    = 0

    while True:
        daily_charge_current, target_soc, full_charge = load_targets_from_file(
            daily_charge_current, target_soc
        )

        limited_data = fetch_registers()

        # Validate all required keys are present before parsing.
        if limited_data is not None:
            _REQUIRED_KEYS = ("0x0101", "0x0102", "0x021c", "0x0234")
            missing = [k for k in _REQUIRED_KEYS if k not in limited_data]
            if missing:
                log.warning(
                    "Register response missing keys %s — treating as fetch failure", missing
                )
                limited_data = None

        if limited_data:
            if consecutive_failures > 0:
                log.info(
                    "Register fetch recovered after %d consecutive failure(s)", consecutive_failures
                )
                consecutive_failures = 0

            #   0x0101 = battery voltage (×0.1 V)
            #   0x0102 = battery current (×0.1 A, signed; positive = charging)
            #   0x021c = load apparent power L1 (W)
            #   0x0234 = load apparent power L2 (W)
            battery_voltage = int(limited_data["0x0101"]) / 10.0
            battery_current = -_to_signed_16(int(limited_data["0x0102"])) / 10.0
            load_power      = int(limited_data["0x021c"]) + int(limited_data["0x0234"])
            pack_charge_a   = max(0.0, battery_current)
            i_pack          = battery_current

            log.debug(
                "Readings: V=%.1f V  I=%+.1f A  load=%.0f W",
                battery_voltage, battery_current, load_power,
            )

        else:
            consecutive_failures += 1
            if consecutive_failures == 1:
                log.warning(
                    "Register fetch failed — holding previous state "
                    "(V=%.1f V  last_SoC=%s%%)",
                    battery_voltage,
                    f"{estimated_soc:.1f}" if estimated_soc is not None else "N/A",
                )
            elif consecutive_failures % 12 == 0:
                log.warning(
                    "Register fetch still failing: %d consecutive attempts (%d s elapsed)",
                    consecutive_failures, consecutive_failures * POLL_INTERVAL_S,
                )

        soc_payload = fetch_soc()
        soc_pack, cell_min_fresh, soc_status = interpret_soc(soc_payload)
        if soc_status == "ok" and soc_pack is not None:
            if soc_fail_ticks:
                log.info("SoC feed recovered after %d tick(s)", soc_fail_ticks)
            estimated_soc = soc_pack
            cell_min_v = cell_min_fresh
            soc_fail_ticks = 0
        else:
            soc_fail_ticks += 1
            if soc_fail_ticks == 1:
                log.warning(
                    "SoC feed %s — holding last est_SoC=%s%%",
                    soc_status,
                    f"{estimated_soc:.1f}" if estimated_soc is not None else "N/A",
                )
            elif soc_fail_ticks % 12 == 0:
                log.warning(
                    "SoC feed still %s: %d ticks (%d s)",
                    soc_status, soc_fail_ticks, soc_fail_ticks * POLL_INTERVAL_S,
                )

        bms_payload = fetch_bms()
        abort_v = (
            CELL_CALIBRATE_ABORT_V
            if charge_mode == ChargeMode.CALIBRATE
            else CELL_MAX_ABORT_V
        )
        bms = parse_bms_view(bms_payload, cell_max_abort_v=abort_v)
        bms_fresh, bms_trip, bms_reason, bms_cell_max = (
            bms.fresh, bms.abort, bms.reason, bms.cell_max,
        )
        if bms_fresh:
            if bms_trip and not bms_abort:
                log.warning(
                    "BMS charge abort: %s  cell_max=%s",
                    bms_reason,
                    f"{bms_cell_max:.3f} V" if bms_cell_max is not None else "n/a",
                )
            elif not bms_trip and bms_abort:
                log.info(
                    "BMS charge abort cleared: %s  cell_max=%s",
                    bms_reason,
                    f"{bms_cell_max:.3f} V" if bms_cell_max is not None else "n/a",
                )
                if bms_abort_reason == "mosfet_off":
                    mosfet_resume_until = datetime.now() + timedelta(
                        seconds=MOSFET_RESUME_COOLDOWN_S,
                    )
                    last_charge_current = min(
                        last_charge_current, MOSFET_RESUME_CURRENT_A,
                    )
            bms_abort = bms_trip
            bms_abort_reason = bms_reason
            bms_cell_max_held = bms_cell_max
        elif bms_abort:
            log.debug(
                "BMS feed %s while abort latched (%s) — keeping I = 0",
                bms_reason, bms_abort_reason,
            )
        else:
            log.debug("BMS feed %s — no abort latched, using pack-V table", bms_reason)

        if bms_abort:
            bms_abort_ticks += 1
            mosfet_resume_until = None
            if bms_abort_ticks % 12 == 0:
                log.warning(
                    "BMS charge abort still active: %s  ticks=%d  cell_max=%s%s",
                    bms_abort_reason, bms_abort_ticks,
                    f"{bms_cell_max_held:.3f} V" if bms_cell_max_held is not None else "n/a",
                    "" if bms_fresh else f"  feed={bms_reason}",
                )
        else:
            bms_abort_ticks = 0

        now_tick = datetime.now()
        dv_dt: float | None = None
        if (
            bms.fresh
            and bms.cell_max is not None
            and last_cell_max is not None
            and last_cell_max_at is not None
        ):
            dt = (now_tick - last_cell_max_at).total_seconds()
            if dt >= 1.0:
                dv_dt = (bms.cell_max - last_cell_max) / dt
        if bms.fresh and bms.cell_max is not None:
            last_cell_max = bms.cell_max
            last_cell_max_at = now_tick

        mosfet_resume_hold = False
        if mosfet_resume_until is not None:
            still_hot = (
                bms_cell_max_held is not None
                and bms_cell_max_held >= MOSFET_RESUME_CELL_V
            )
            if now_tick < mosfet_resume_until or still_hot:
                mosfet_resume_hold = True
            else:
                mosfet_resume_until = None

        # ── Manual override (UI-driven, time-limited) ────────────────
        override = load_manual_override()

        # ── State transitions ─────────────────────────────────────────
        # Only evaluate the state machine with fresh data.  On fetch failure
        # we hold the current state to avoid acting on stale values.  After a
        # sustained outage (FAIL_SAFE_TICKS), force a safe fallback.
        prev_state = current_state

        if limited_data:
            time_period = get_time_period()

            if override is not None:
                current_state = override[0]
            elif soc_fail_ticks >= FAIL_SAFE_TICKS and current_state == State.SBU:
                log.warning(
                    "Forcing SBU → UTI_STOPPED: no SoC feed for %d s — "
                    "refusing to discharge without a gauge",
                    soc_fail_ticks * POLL_INTERVAL_S,
                )
                current_state = State.UTI_STOPPED
                last_sbu_to_uti_time = datetime.now()
            else:
                current_state, daily_charge_current, last_sbu_to_uti_time = determine_next_state(
                    current_state,
                    estimated_soc,
                    target_soc,
                    battery_voltage,
                    time_period,
                    daily_charge_current,
                    last_sbu_to_uti_time,
                    full_charge_active=full_charge,
                    cell_min_v=cell_min_v if soc_fail_ticks == 0 else None,
                )

            if current_state != prev_state:
                if override is not None:
                    log.info(
                        "State (override): %s → %s  (expires %s, V=%.1f V)",
                        prev_state.value, current_state.value,
                        override[1].astimezone().strftime("%H:%M"),
                        battery_voltage,
                    )
                else:
                    log.info(
                        "State: %s → %s  (est_SoC=%.1f%%  V=%.1f V  period=%s)",
                        prev_state.value, current_state.value,
                        estimated_soc if estimated_soc is not None else 0.0,
                        battery_voltage, time_period,
                    )
            else:
                log.debug(
                    "State: %s  est_SoC=%.1f%%  V=%.1f V  target_SoC=%.0f%%  period=%s%s",
                    current_state.value,
                    estimated_soc if estimated_soc is not None else 0.0,
                    battery_voltage, target_soc, time_period,
                    "  override=ACTIVE" if override is not None else "",
                )
        elif consecutive_failures >= FAIL_SAFE_TICKS and current_state == State.SBU:
            log.warning(
                "Forcing SBU → UTI_STOPPED: no register data for %d s — "
                "refusing to discharge battery without monitoring",
                consecutive_failures * POLL_INTERVAL_S,
            )
            current_state = State.UTI_STOPPED
        # else: hold current state — don't transition on stale data

        # ── Full-charge phase progression ─────────────────────────────
        # NORMAL → CC → SOAK → CALIBRATE → done. Skip while override is active.
        if limited_data and full_charge and time_period == "cheap" and override is None:
            prev_mode = charge_mode
            fc_state = FullChargeState(
                mode=charge_mode,
                soak_started_at=fc_state.soak_started_at,
                tail_ok_since=fc_state.tail_ok_since,
            )
            fc_state = advance_full_charge(
                fc_state,
                now=now_tick,
                bms=bms,
                pack_v=battery_voltage,
                pack_charge_a=pack_charge_a,
                soc_payload=soc_payload,
                seconds_left=seconds_until_cheap_end(now_tick),
            )
            charge_mode = fc_state.mode
            if charge_mode != prev_mode:
                log.info(
                    "Full charge: %s → %s  (cell_max=%s  cell_min=%s  delta=%s  V=%.2f V)",
                    prev_mode.value, charge_mode.value,
                    f"{bms.cell_max:.3f} V" if bms.cell_max is not None else "n/a",
                    f"{bms.cell_min:.3f} V" if bms.cell_min is not None else "n/a",
                    f"{bms.cell_delta * 1000:.0f} mV" if bms.cell_delta is not None else "n/a",
                    battery_voltage,
                )
            if charge_mode == ChargeMode.CALIBRATE and abort_v != CELL_CALIBRATE_ABORT_V:
                bms = parse_bms_view(
                    bms_payload, cell_max_abort_v=CELL_CALIBRATE_ABORT_V,
                )
                if bms.fresh:
                    bms_abort = bms.abort
                    bms_abort_reason = bms.reason
                    bms_cell_max_held = bms.cell_max
            if fc_state.complete:
                log.info("Full charge complete — %s", fc_state.complete_reason)
                _complete_full_charge()
                charge_mode = ChargeMode.NORMAL
                fc_state = FullChargeState()
                full_charge = False

        elif limited_data and not full_charge and charge_mode != ChargeMode.NORMAL:
            log.info(
                "Full charge flag cleared from targets — resetting charge_mode %s → NORMAL",
                charge_mode.value,
            )
            charge_mode = ChargeMode.NORMAL
            fc_state = FullChargeState()

        elif limited_data and full_charge and time_period != "cheap" and charge_mode != ChargeMode.NORMAL:
            if charge_mode in (ChargeMode.SOAK, ChargeMode.CALIBRATE):
                log.info(
                    "Full charge: cheap period ended in %s — counting as complete",
                    charge_mode.value,
                )
                _complete_full_charge()
                full_charge = False
            else:
                log.warning(
                    "Full charge: cheap period ended (now=%s, mode=%s) — "
                    "aborting without completing",
                    datetime.now().strftime("%H:%M"), charge_mode.value,
                )
            charge_mode = ChargeMode.NORMAL
            fc_state = FullChargeState()

        # ── Output priority ───────────────────────────────────────────
        desired_priority = determine_output_priority(current_state)
        if last_output_priority != desired_priority:
            log.info(
                "Output priority: %s → %s",
                last_output_priority.name if last_output_priority is not None else "None",
                desired_priority.name,
            )
            if set_output_priority(desired_priority):
                last_output_priority = desired_priority

        # ── Charge current (only with fresh data) ─────────────────────
        i_cmd_now = last_charge_current
        if limited_data and estimated_soc is not None:
            target_charge_current = adjust_battery_charge(
                estimated_soc, load_power, battery_voltage, daily_charge_current, current_state,
                charge_mode=charge_mode,
                bms_abort=bms_abort,
                cell_max=bms.cell_max if bms.fresh else None,
                dv_dt=dv_dt,
                i_prev=last_charge_current,
                mosfet_resume_hold=mosfet_resume_hold,
            )
            i_cmd_now = target_charge_current
            if last_charge_current != target_charge_current:
                log.info(
                    "Charge current: %.0f A → %.0f A",
                    last_charge_current, target_charge_current,
                )
                if set_charge_current(target_charge_current):
                    last_charge_current = target_charge_current

        tick_line = format_charge_tick(
            mode=charge_mode,
            i_cmd=i_cmd_now,
            i_pack=i_pack,
            bms=bms,
            abort=bms_abort,
        )
        if charge_mode != ChargeMode.NORMAL or bms_abort:
            fc_log_ticks += 1
            if fc_log_ticks == 1 or fc_log_ticks % 12 == 0:
                log.info("Charge: %s", tick_line)
            else:
                log.debug("Charge: %s", tick_line)
        else:
            fc_log_ticks = 0
            log.debug("Charge: %s", tick_line)

        write_charge_control(
            build_charge_control_records(
                mode=charge_mode,
                state=current_state,
                i_cmd=i_cmd_now,
                i_pack=i_pack,
                bms=bms,
                abort=bms_abort,
            )
        )

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
