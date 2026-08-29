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
IDLE_SOC_PCT_PER_H: float  = 0.25      # Overnight DC idle (% / h); cheap hold slides at this rate
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
# CC fills at up to CC_MAX_CURRENT until pack 55.2 V or IR-free cell_max
# hits CELL_KNEE_V. Loaded hottest-cell 3.45 V is IR on the high-R cells
# (B05/B08/B12/B14 rotate; A07/A08 the same class), not the knee.
# SOAK is the pack-V table min loaded-hottest-cell table until ~06:40.
# Last bin 3 A. Do not zero at 3.59 V — 3 A holds the balancer window.
# Abort 3.62 V / pack 57.9 V.
# CALIBRATE is a flat 10 A to CELL_CALIBRATE_V. Same 3.62 V abort.

CC_MAX_CURRENT:            float = 120.0
CELL_KNEE_V:               float = 3.45  # IR-free (OCV-like), not loaded cell_max
CELL_SOAK_V:               float = 3.50
CELL_CALIBRATE_V:          float = 3.59
CELL_CALIBRATE_ABORT_V:    float = 3.62  # below OVP 3.65; allows 3.59 creep
PACK_KNEE_V:               float = 55.2  # backstop to enter SOAK if BLE is down
PACK_ABORT_V:              float = 56.8  # ~3.55 × 16; CC / NORMAL
PACK_CALIBRATE_ABORT_V:    float = 57.9  # ~3.62 × 16; SOAK / CALIBRATE
I_SLEW_A:                  float = 10.0  # unused (no slew on I_cmd)
SOAK_HOLD_CURRENT_A:       float = 20.0  # BLE-down cap when cells missing at the knee
CALIBRATE_MAX_CURRENT:     float = 10.0
SOAK_MIN_CELL_V:           float = 3.45
SOAK_TAIL_CURRENT_A:       float = 15.0
SOAK_TAIL_HOLD_S:          int   = 180
SOAK_DELTA_V:              float = 0.020
SOAK_MIN_DURATION_S:       int   = 45 * 60
CALIBRATE_RESERVE_S:       int   = 18 * 60  # leave SOAK ~06:40 if cheap ends 06:58
MOSFET_RESUME_CELL_V:      float = 3.48
MOSFET_RESUME_COOLDOWN_S:  int   = 90
MOSFET_RESUME_CURRENT_A:   float = 8.0
FULL_REMAIN_FRAC:          float = 0.995
BLE_DVDT_HORIZON_S:        float = 10.0  # unused by I_cmd; kept for log/compat

PACK_VOLT_LIMITS: list[tuple[float, float]] = [
    (55.2, 120), (55.6, 80), (55.8, 60), (56.0, 40),
    (56.3, 30), (56.5, 24), (56.6, 18), (56.7, 14),
    (56.8, 10), (56.9, 7),
]
# Same I steps as PACK_VOLT_LIMITS, bounds in V/cell (pack / 16).
# CC uses IR-free cell_max. SOAK uses loaded hottest cell.
CELL_CURRENT_LIMITS: list[tuple[float, float]] = [
    (v / 16.0, lim) for v, lim in PACK_VOLT_LIMITS
]
# 30 s ΔV/ΔI medians from cell-health (mΩ). Open-loop IR subtract, not a
# control gain — do not retune so one night's I(t) looks pretty.
CELL_R_MOHM: dict[str, tuple[float, ...]] = {
    "a": (
        0.41, 0.52, 2.12, 1.30, 0.96, 1.54, 2.60, 2.89,
        1.33, 2.05, 1.52, 0.59, 0.50, 1.54, 2.13, 0.60,
    ),
    "b": (
        0.42, 0.66, 0.72, 1.12, 1.63, 0.51, 0.52, 1.74,
        0.50, 0.43, 0.56, 1.68, 0.67, 1.73, 0.50, 0.49,
    ),
}
SOC_LIMITS: list[tuple[float, float]] = [
    (60, 120), (70, 105), (80, 90), (85, 80),
    (90, 70), (93, 60), (96, 50), (98, 40),
    (99, 30), (100, 20),
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
    bank_current: dict[str, float] = field(default_factory=dict)
    bank_cells: dict[str, list[float | None]] = field(default_factory=dict)
    hot_bank: str | None = None
    hot_cell: int | None = None  # 1-based, matches JK app (A06)
    balance_current: float | None = None
    bank_balance_current: dict[str, float] = field(default_factory=dict)
    cell_max_ir_free: float | None = None


@dataclass
class FullChargeState:
    mode: ChargeMode = ChargeMode.NORMAL
    soak_started_at: datetime | None = None
    tail_ok_since: datetime | None = None
    complete: bool = False
    complete_reason: str | None = None


def ir_free_cell_max(
    bank_cells: dict[str, list[float | None]],
    bank_current: dict[str, float],
    r_mohm: dict[str, tuple[float, ...]] | None = None,
) -> float | None:
    """max(V - I_charge·R) per cell. None if a bank has cells but no current.

    Max over every cell in both banks. Loaded hottest identity rotates
    among high-R cells; this is not a named-cell special case.
    Charge current only (I>0); missing I → None so we do not treat
    loaded V as IR-free.
    """
    r_map = r_mohm if r_mohm is not None else CELL_R_MOHM
    best: float | None = None
    for bank, cells in bank_cells.items():
        if bank not in bank_current:
            continue
        i_chg = max(0.0, float(bank_current[bank]))
        r_bank = r_map.get(bank)
        if r_bank is None:
            continue
        for idx, v in enumerate(cells):
            if v is None:
                continue
            r_ohm = (r_bank[idx] if idx < len(r_bank) else 0.0) * 1e-3
            ve = float(v) - i_chg * r_ohm
            if best is None or ve > best:
                best = ve
    return best


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
    bank_current: dict[str, float] = {}
    bank_cells: dict[str, list[float | None]] = {}
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
        cur = _as_float(sample.get("current"))
        if cur is not None:
            bank_current[bank_name] = cur
        cells = sample.get("cells")
        cell_idx: int | None = None
        cell_v: float | None = cmax
        if isinstance(cells, list) and cells:
            nums: list[float | None] = [_as_float(item) for item in cells]
            finite = [(i, v) for i, v in enumerate(nums) if v is not None]
            if finite:
                bank_cells[bank_name] = nums
                idx0, cell_v = max(finite, key=lambda p: p[1])
                cell_idx = idx0 + 1
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
        bank_current=bank_current,
        bank_cells=bank_cells,
        hot_bank=hot_bank,
        hot_cell=hot_cell,
        balance_current=balance,
        bank_balance_current=bank_balance,
        cell_max_ir_free=ir_free_cell_max(bank_cells, bank_current),
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
        f"ir_free={_v(bms.cell_max_ir_free)} "
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
    if bms.cell_max_ir_free is not None:
        recs.append(ChargeControlRecord("pack", "cell_max_ir_free", "V", bms.cell_max_ir_free))
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


def cheap_hold_soc(
    target_soc: float,
    seconds_left: float,
    *,
    idle_pct_per_h: float = IDLE_SOC_PCT_PER_H,
) -> float:
    """SoC to sit at now so idle leak lands on target_soc at the end of cheap."""
    hours = max(0.0, seconds_left) / 3600.0
    return target_soc + idle_pct_per_h * hours


def pack_volt_current_cap(battery_voltage: float) -> float:
    """Dumb pack-V current cap. Used when cell voltages are missing."""
    for volt_threshold, limit in PACK_VOLT_LIMITS:
        if battery_voltage < volt_threshold:
            return float(limit)
    return 3.0


def cell_current_cap(
    cell_v: float,
    i_cc: float,
    cell_abort_v: float = CELL_MAX_ABORT_V,
) -> float:
    """PACK_VOLT_LIMITS amp steps on IR-free cell voltage (bounds ÷ 16)."""
    i_cc = max(0.0, i_cc)
    if cell_v >= cell_abort_v:
        return 0.0
    for bound, cap in CELL_CURRENT_LIMITS:
        if cell_v < bound:
            return min(float(cap), i_cc)
    return min(3.0, i_cc)


def soc_current_cap(soc_pct: float, i_cc: float) -> float:
    """Old SoC table. Lookup every 5 s, no hysteresis."""
    i_cc = max(0.0, i_cc)
    for thr, lim in SOC_LIMITS:
        if soc_pct < thr:
            return min(float(lim), i_cc)
    return min(20.0, i_cc)


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
    """Cell overlay: 0 at abort, otherwise i_cc (pack-V + SoC tables do the rest).

    Unused kwargs kept for call-compat.
    """
    del v_set, i_prev, dv_dt, i_slew, cell_knee_v, i_hold, ble_horizon_s
    if cell_max is None:
        return None
    if cell_max >= cell_abort_v:
        return 0.0
    return max(0.0, i_cc)


def table_charge_current(
    *,
    cell_max: float | None,
    pack_v: float,
    soc_pct: float,
    i_cc: float,
    cell_abort_v: float = CELL_MAX_ABORT_V,
    use_soc_table: bool = True,
    cell_max_eff: float | None = None,
) -> float:
    """Every 5 s: pack-V table + optional SoC table + IR-free cell table.

    Loaded cell_max only aborts (3.55 V). Cell amp steps use cell_max_eff
    (IR-free). CC sets use_soc_table=False so SoC 60 % does not taper on
    the plateau.
    """
    i_cc = max(0.0, i_cc)
    if cell_max is not None and cell_max >= cell_abort_v:
        return 0.0
    caps = [i_cc, pack_volt_current_cap(pack_v)]
    if use_soc_table:
        caps.append(soc_current_cap(soc_pct, i_cc))
    if cell_max_eff is not None:
        caps.append(cell_current_cap(cell_max_eff, i_cc, cell_abort_v))
    return min(caps)


def soak_cell_current(
    cell_max: float | None,
    cell_min: float | None,
    pack_v: float,
    cell_abort_v: float = CELL_CALIBRATE_ABORT_V,
) -> float:
    """SOAK: pack-V table min loaded hottest-cell table.

    Same 120/80/60/40/30/24/… steps. cell_min unused (16S series shares I).
    """
    del cell_min
    i_want = pack_volt_current_cap(pack_v)
    if cell_max is not None:
        i_want = min(i_want, cell_current_cap(cell_max, 120.0, cell_abort_v))
    return i_want


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
        enter = pack_v >= PACK_KNEE_V
        if (
            not enter
            and bms.fresh
            and bms.cell_max_ir_free is not None
            and bms.cell_max_ir_free >= CELL_KNEE_V
        ):
            enter = True
        if enter:
            return FullChargeState(mode=ChargeMode.SOAK, soak_started_at=now)
        return FullChargeState(mode=ChargeMode.CC)

    if mode == ChargeMode.SOAK:
        if soak_started is None:
            soak_started = now
        # Stay in SOAK (balancer window) until cheap is almost over.
        # Completing on remain_est/quality at 04:35 cut balancing short and SBU'd.
        go_cal = (
            seconds_left <= CALIBRATE_RESERVE_S
            and bms.fresh
            and bms.cell_max is not None
            and bms.cell_max >= CELL_SOAK_V - 0.02
        )
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


def _abandon_full_charge(*, reason: str) -> None:
    """Clear full_charge for the day. Do not stamp last_full_charge."""
    targets = _read_targets_file()
    targets["full_charge"] = False
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        log.info(
            "Full charge abandoned: %s — flag cleared, last_full_charge unchanged",
            reason,
        )
    except Exception as e:
        log.warning("Failed to write targets.json on full-charge abandon: %s", e)


def cheap_end_full_charge_action(
    mode: ChargeMode,
    bms: BmsView,
    soc_payload: dict | None,
) -> str:
    """What to do when cheap ends mid full-charge: complete | abandon | abort."""
    if mode not in (ChargeMode.SOAK, ChargeMode.CALIBRATE):
        return "abort"
    if banks_at_full(bms, soc_payload):
        return "complete"
    return "abandon"


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
) -> tuple[float, float, bool, bool]:
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
        daily = float(targets.get("daily_charge_current", current_daily_charge_current))
        soc   = float(targets.get("target_soc", current_target_soc))
        full_charge = bool(targets.get("full_charge", False))
        last = str(targets.get("last_full_charge") or "")
        done_today = last == date.today().isoformat()
        log.debug(
            "Targets loaded from file: target_soc=%.0f%%  daily_charge_current=%.0f A  full_charge=%s",
            soc, daily, full_charge,
        )
        return daily, soc, full_charge, done_today
    except Exception as e:
        log.warning(
            "Failed to load targets.json: %s — keeping target_soc=%.0f%%  daily_charge_current=%.0f A",
            e, current_target_soc, current_daily_charge_current,
        )
        return current_daily_charge_current, current_target_soc, False, False


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
    full_charge_done_today: bool = False,
) -> tuple[State, float, datetime | None]:
    """Compute the next control state.

    Returns (next_state, daily_charge_current, new_last_sbu_to_uti_time).
    daily_charge_current is passed through unchanged.
    """
    if estimated_soc is None:
        log.debug("estimated_soc not yet available — holding state %s", current_state.value)
        return current_state, daily_charge_current, last_sbu_to_uti_time

    next_state = current_state
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
        # Full charge in progress: stay in UTI_CHARGING regardless of SoC vs hold.
        # Phase progression and current calculation are handled by the main loop.
        if full_charge_active:
            if current_state != State.UTI_CHARGING:
                log.info(
                    "Full charge active: forcing %s → UTI_CHARGING in cheap period",
                    current_state.value,
                )
                next_state = State.UTI_CHARGING
            return next_state, daily_charge_current, last_sbu_to_uti_time

        # Just finished a full charge this cheap window: do not SBU a 100 % pack.
        if full_charge_done_today and current_state == State.SBU:
            next_state = State.UTI_STOPPED
            new_last_sbu_to_uti_time = now
            return next_state, daily_charge_current, new_last_sbu_to_uti_time

        if seconds_left_cheap is None:
            seconds_left_cheap = seconds_until_cheap_end(now)
        hold = cheap_hold_soc(target_soc, seconds_left_cheap)

        if current_state == State.UTI_CHARGING:
            if estimated_soc > hold + NEAR_TARGET_SOC:
                next_state = State.UTI_STOPPED
                log.debug(
                    "Cheap: SoC %.1f%% > hold+band %.1f%% → UTI_STOPPED",
                    estimated_soc, hold + NEAR_TARGET_SOC,
                )

        elif current_state == State.UTI_STOPPED:
            if estimated_soc < hold - HYSTERESIS_SOC:
                next_state = State.UTI_CHARGING
                log.debug(
                    "Cheap: SoC %.1f%% < hold-hysteresis %.1f%% → UTI_CHARGING",
                    estimated_soc, hold - HYSTERESIS_SOC,
                )
            elif estimated_soc > hold + HYSTERESIS_SOC:
                if full_charge_done_today:
                    log.debug(
                        "Cheap: full charge done today — not SBU at SoC %.1f%%",
                        estimated_soc,
                    )
                else:
                    next_state = State.SBU
                    log.debug(
                        "Cheap: SoC %.1f%% > hold+hysteresis %.1f%% → SBU",
                        estimated_soc, hold + HYSTERESIS_SOC,
                    )

        else:  # State.SBU
            if estimated_soc < hold - HYSTERESIS_SOC:
                next_state = State.UTI_CHARGING
                log.debug(
                    "Cheap: SoC %.1f%% < hold-hysteresis %.1f%% → UTI_CHARGING",
                    estimated_soc, hold - HYSTERESIS_SOC,
                )
            elif estimated_soc < hold + NEAR_TARGET_SOC:
                next_state = State.UTI_STOPPED
                log.debug(
                    "Cheap: SoC %.1f%% < hold+band %.1f%% → UTI_STOPPED",
                    estimated_soc, hold + NEAR_TARGET_SOC,
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

    return next_state, daily_charge_current, new_last_sbu_to_uti_time


def adjust_battery_charge(
    battery_soc: float,
    load_power: float,
    battery_voltage: float,
    daily_charge_current: float,
    state: State,
    charge_mode: ChargeMode = ChargeMode.NORMAL,
    bms_abort: bool = False,
    cell_max: float | None = None,
    cell_min: float | None = None,
    cell_max_eff: float | None = None,
    dv_dt: float | None = None,
    i_prev: float = 0.0,
    mosfet_resume_hold: bool = False,
) -> float:
    """Return the target charge current (A)."""
    if state in (State.SBU, State.UTI_STOPPED):
        log.debug("Charge current = 0 A (state=%s)", state.value)
        return 0.0
    if bms_abort:
        log.debug("Charge current = 0 A (BMS abort)")
        return 0.0

    soak_or_cal = charge_mode in (ChargeMode.SOAK, ChargeMode.CALIBRATE)
    pack_abort_v = PACK_CALIBRATE_ABORT_V if soak_or_cal else PACK_ABORT_V
    if battery_voltage >= pack_abort_v:
        log.warning(
            "Pack abort: voltage %.2f V >= %.2f V — charge current 0 A",
            battery_voltage, pack_abort_v,
        )
        return 0.0

    grid_limit = calculate_grid_limit_current(load_power, battery_voltage)
    if mosfet_resume_hold:
        if cell_max is not None and cell_max >= MOSFET_RESUME_CELL_V:
            log.debug("MOSFET resume hold: cell_max %.3f V still high — 0 A", cell_max)
            return 0.0
        target = min(MOSFET_RESUME_CURRENT_A, grid_limit)
        log.debug(
            "MOSFET resume: cap=%.0f A  grid=%.0f A → %.0f A",
            MOSFET_RESUME_CURRENT_A, grid_limit, target,
        )
        return float(round(max(0.0, target)))

    if charge_mode == ChargeMode.CALIBRATE:
        i_cc = CALIBRATE_MAX_CURRENT
        abort_v = CELL_CALIBRATE_ABORT_V
    elif charge_mode == ChargeMode.SOAK:
        i_cc = CC_MAX_CURRENT
        abort_v = CELL_CALIBRATE_ABORT_V
    elif charge_mode == ChargeMode.CC:
        i_cc = CC_MAX_CURRENT
        abort_v = CELL_MAX_ABORT_V
    else:
        i_cc = daily_charge_current
        abort_v = CELL_MAX_ABORT_V

    del dv_dt, i_prev
    if cell_max is not None and cell_max >= abort_v:
        return 0.0

    if charge_mode == ChargeMode.CALIBRATE:
        # Flat 10 A to 3.59 V. IR-free table would cap 7 A at 3.55 V.
        target = CALIBRATE_MAX_CURRENT
    elif charge_mode == ChargeMode.SOAK:
        target = soak_cell_current(
            cell_max, cell_min, battery_voltage, cell_abort_v=abort_v,
        )
    else:
        target = table_charge_current(
            cell_max=cell_max,
            pack_v=battery_voltage,
            soc_pct=battery_soc,
            i_cc=i_cc,
            cell_abort_v=abort_v,
            use_soc_table=(charge_mode != ChargeMode.CC),
            cell_max_eff=cell_max_eff,
        )
    if (
        cell_max is None
        and charge_mode in (ChargeMode.SOAK, ChargeMode.CALIBRATE)
        and battery_voltage >= PACK_KNEE_V
    ):
        target = min(target, SOAK_HOLD_CURRENT_A)
    final = min(target, grid_limit)
    log.debug(
        "%s charge (table): cap=%.0f A  cell_max=%s  SoC=%.0f%%  "
        "grid=%.0f A → %.0f A",
        charge_mode.value, i_cc,
        f"{cell_max:.3f}" if cell_max is not None else "n/a",
        battery_soc, grid_limit, final,
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
        "  Full charge   : CC ≤%.0fA → SOAK table / abort %.2f V "
        "→ CALIBRATE %.0fA to %.2f V (abort %.2f / pack %.1f V)",
        CC_MAX_CURRENT, CELL_CALIBRATE_ABORT_V,
        CALIBRATE_MAX_CURRENT, CELL_CALIBRATE_V,
        CELL_CALIBRATE_ABORT_V, PACK_CALIBRATE_ABORT_V,
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
        daily_charge_current, target_soc, full_charge, full_charge_done_today = (
            load_targets_from_file(daily_charge_current, target_soc)
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
            if charge_mode in (ChargeMode.CALIBRATE, ChargeMode.SOAK)
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
                    full_charge_done_today=full_charge_done_today,
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
            if (
                charge_mode in (ChargeMode.SOAK, ChargeMode.CALIBRATE)
                and abort_v != CELL_CALIBRATE_ABORT_V
            ):
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
            action = cheap_end_full_charge_action(charge_mode, bms, soc_payload)
            if action == "complete":
                log.info(
                    "Full charge: cheap period ended in %s — cells at full, completing",
                    charge_mode.value,
                )
                _complete_full_charge()
                full_charge = False
            elif action == "abandon":
                log.warning(
                    "Full charge: cheap period ended in %s without 3.59 V — "
                    "not stamping last_full_charge",
                    charge_mode.value,
                )
                _abandon_full_charge(
                    reason=f"cheap ended in {charge_mode.value} without snap",
                )
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
                cell_min=bms.cell_min if bms.fresh else None,
                cell_max_eff=bms.cell_max_ir_free if bms.fresh else None,
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
