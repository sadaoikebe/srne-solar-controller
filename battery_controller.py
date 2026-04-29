"""Battery charge controller.

Polls the inverter every POLL_INTERVAL_S seconds via modbus_api, computes
the desired output priority and charge current, and pushes changes back.

Log levels
----------
  DEBUG  — raw register values, SoC estimator steps, grid-limit arithmetic,
           charge-taper table lookups, per-tick loop heartbeat
  INFO   — state transitions, charge-current changes, priority changes,
           config reloads, startup/shutdown
  WARNING — fetch failures, config-file errors (non-fatal)
  ERROR  — currently unused (caller should watch WARNING closely)
"""
from __future__ import annotations

import json
import math
import os
import time
from datetime import date, datetime
from enum import Enum, IntEnum

import requests

from log_config import get_logger

log = get_logger("battery_controller")

# ── Hardware / system constants ───────────────────────────────────────────────

POLL_INTERVAL_S: int = 5

# Battery bank parameters — update these if the pack is replaced.
BATTERY_CAPACITY_AH: float = 520.0

# Wh stored per 1% SoC.  Charging and discharging use different values because
# the average bus voltage differs: ~52 V during charging vs ~49 V during discharge.
BATTERY_WH_PER_SOC_CHARGING:    float = 270.0   # 520 Ah × 52 V / 100
BATTERY_WH_PER_SOC_DISCHARGING: float = 255.0   # 520 Ah × 49 V / 100

# Pre-computed: SoC (%) change per amp per polling tick.
# Derivation: 100 % / (capacity_Ah × 3600 s/h) × POLL_INTERVAL_S
_SOC_DELTA_PER_A_PER_TICK: float = (
    100.0 / (BATTERY_CAPACITY_AH * 3600.0) * POLL_INTERVAL_S
)

GRID_MAX_POWER_W: float    = 9000.0    # Maximum grid power budget (W)
HYSTERESIS_SOC:   float    = 2.0       # SoC hysteresis band (%)
CUTOFF_SOC:       float    = 9.0       # Emergency SoC floor (%)
SBU_TO_UTI_COOLDOWN_S: int = 30 * 60   # Minimum seconds between SBU→UTI switches
FAIL_SAFE_TICKS:       int = 60        # After this many consecutive fetch failures
                                       # (60 × 5 s = 5 min), force SBU → UTI_STOPPED
                                       # to stop discharging the battery without monitoring.

# ── Full-charge (LFP balancing / SoC sync) constants ─────────────────────────
# Triggered by daily_target.py setting "full_charge: true" in targets.json
# the night before a tier-5 weather day, no more than once per
# FULL_CHARGE_MIN_INTERVAL_DAYS.  Phases: BULK → BALANCE → SYNC → done.

BALANCE_ENTRY_VOLTAGE: float = 55.6   # V — leave BULK once voltage reaches this
BALANCE_ENTRY_CURRENT: float = 15.0   # A — and current has tapered down to this
SYNC_START_TIME:       str   = "06:43"  # Begin SYNC nudge at/after this time
SYNC_DEADLINE:         str   = "06:58"  # Hard stop (cheap period ends 06:58, sbu_fixed at 06:59)
SYNC_MAX_CURRENT:      float = 30.0   # A — current cap during SYNC nudge
SYNC_VOLTAGE_CEILING:  float = 57.2   # V — abort SYNC if voltage exceeds this
SYNC_TIMEOUT_MINUTES:  int   = 15     # Maximum SYNC duration

# ── Runtime configuration ─────────────────────────────────────────────────────

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/targets.json")

_API_PORT: int = int(os.getenv("MODBUS_API_PORT", "5004"))
_API_BASE: str = f"http://modbus_api:{_API_PORT}"

LIMITED_REGISTERS_URL:   str = f"{_API_BASE}/limited_registers"
SET_CHARGE_CURRENT_URL:  str = f"{_API_BASE}/set_charge_current"
SET_OUTPUT_PRIORITY_URL: str = f"{_API_BASE}/set_output_priority"

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
    NORMAL  = "NORMAL"   # Default — daily SoC-target-driven charging
    BULK    = "BULK"     # Full charge phase 1: charge until voltage rises and current tapers
    BALANCE = "BALANCE"  # Full charge phase 2: hold at high voltage to let cells equalize
    SYNC    = "SYNC"     # Full charge phase 3: brief nudge to force BMS SoC calibration to 100%


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
            data.get("0x0102"), data.get("0x021C"), data.get("0x0234"),
        )
        return data
    except requests.RequestException as e:
        log.warning("Register fetch failed: %s", e)
        return None


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
            if battery_voltage > 51.6 and estimated_soc > CUTOFF_SOC:
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
            if battery_voltage > 51.6 and estimated_soc > CUTOFF_SOC:
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

        if current_state == State.UTI_CHARGING:
            if estimated_soc > target_soc + HYSTERESIS_SOC:
                next_state = State.SBU
                lower_charge_current = True
                log.debug(
                    "Cheap: SoC %.1f%% > target+hysteresis %.0f%% → SBU + lower current",
                    estimated_soc, target_soc + HYSTERESIS_SOC,
                )
            elif estimated_soc > target_soc + 0.4:
                next_state = State.UTI_STOPPED
                lower_charge_current = True
                log.debug(
                    "Cheap: SoC %.1f%% > target+0.4 %.1f%% → UTI_STOPPED + lower current",
                    estimated_soc, target_soc + 0.4,
                )

        elif current_state == State.UTI_STOPPED:
            if estimated_soc > target_soc + HYSTERESIS_SOC:
                next_state = State.SBU
                log.debug(
                    "Cheap: SoC %.1f%% > target+hysteresis %.0f%% → SBU",
                    estimated_soc, target_soc + HYSTERESIS_SOC,
                )
            elif estimated_soc < target_soc - 0.4:
                next_state = State.UTI_CHARGING
                log.debug(
                    "Cheap: SoC %.1f%% < target-0.4 %.1f%% → UTI_CHARGING",
                    estimated_soc, target_soc - 0.4,
                )

        else:  # State.SBU
            if estimated_soc < target_soc - 0.4:
                next_state = State.UTI_CHARGING
                log.debug(
                    "Cheap: SoC %.1f%% < target-0.4 %.1f%% → UTI_CHARGING",
                    estimated_soc, target_soc - 0.4,
                )
            elif estimated_soc < target_soc + 0.4:
                next_state = State.UTI_STOPPED
                log.debug(
                    "Cheap: SoC %.1f%% near target %.0f%% → UTI_STOPPED",
                    estimated_soc, target_soc,
                )

    else:
        # "unknown" time period — no transitions, hold current state
        log.debug("Time period 'unknown' — holding state %s", current_state.value)

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
) -> float:
    """Return the target charge current (A) for the given state and conditions."""
    if state in (State.SBU, State.UTI_STOPPED):
        log.debug("Charge current = 0 A (state=%s)", state.value)
        return 0.0

    grid_limit = calculate_grid_limit_current(load_power, battery_voltage)

    # SYNC: bypass voltage taper to nudge BMS coulomb counter to 100%.
    # Hard-abort if voltage approaches BMS over-voltage cutoff.
    if charge_mode == ChargeMode.SYNC:
        if battery_voltage >= SYNC_VOLTAGE_CEILING:
            log.warning(
                "SYNC abort: voltage %.2f V >= ceiling %.2f V — charge current 0 A",
                battery_voltage, SYNC_VOLTAGE_CEILING,
            )
            return 0.0
        target = min(SYNC_MAX_CURRENT, grid_limit)
        log.debug(
            "SYNC charge: cap=%.0f A  grid_limit=%.0f A  V=%.2f V  → %.0f A",
            SYNC_MAX_CURRENT, grid_limit, battery_voltage, target,
        )
        return target

    # State.UTI_CHARGING — apply SoC-taper and voltage-taper limits
    target = daily_charge_current

    SOC_LIMITS = [
        (60, 120), (70, 105), (80,  90), (85, 80),
        (90,  70), (93,  60), (96,  50), (98, 40),
        (99,  30), (100, 20),
    ]
    soc_limit_applied: float | None = None
    for soc_threshold, limit in SOC_LIMITS:
        if battery_soc < soc_threshold:
            target = min(float(limit), target)
            soc_limit_applied = float(limit)
            break
    else:
        target = min(10.0, target)
        soc_limit_applied = 10.0

    VOLT_LIMITS = [
        (55.2, 120), (55.6,  80), (55.8, 60), (56.0, 40),
        (56.3,  30), (56.5,  24), (56.6, 18), (56.7, 14),
        (56.8,  10), (56.9,   7),
    ]
    volt_limit_applied: float | None = None
    for volt_threshold, limit in VOLT_LIMITS:
        if battery_voltage < volt_threshold:
            target = min(float(limit), target)
            volt_limit_applied = float(limit)
            break
    else:
        target = min(2.0, target)
        volt_limit_applied = 2.0

    final = min(grid_limit, target)
    log.debug(
        "Charge calc: daily=%.0f A  soc_limit=%.0f A (SoC=%.0f%%)  "
        "volt_limit=%.0f A (V=%.2f V)  grid_limit=%.0f A  → %.0f A",
        daily_charge_current, soc_limit_applied, battery_soc,
        volt_limit_applied, battery_voltage, grid_limit, final,
    )
    return final


def determine_output_priority(state: State) -> OutputPriority:
    return OutputPriority.SBU if state == State.SBU else OutputPriority.UTI


# ── Main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    log.info("=" * 60)
    log.info("Battery charge controller starting")
    log.info("  Poll interval : %d s", POLL_INTERVAL_S)
    log.info("  Battery       : %.0f Ah", BATTERY_CAPACITY_AH)
    log.info("  Grid budget   : %.0f W", GRID_MAX_POWER_W)
    log.info("  SBU→UTI cooldown: %d s", SBU_TO_UTI_COOLDOWN_S)
    log.info("  API base      : %s", _API_BASE)
    log.info("  Auth          : %s", "enabled" if _API_AUTH else "disabled (no credentials)")
    log.info("  Config file   : %s", CONFIG_PATH)
    log.info("  Time periods  : %s",
             "  ".join(f"{p['name']} ({p['start']}–{p['end']})" for p in TIME_PERIODS))
    log.info(
        "  Full charge   : balance≥%.1fV/≤%.0fA, sync %s–%s, max %.0fA, ceiling %.1fV, timeout %d min",
        BALANCE_ENTRY_VOLTAGE, BALANCE_ENTRY_CURRENT,
        SYNC_START_TIME, SYNC_DEADLINE, SYNC_MAX_CURRENT,
        SYNC_VOLTAGE_CEILING, SYNC_TIMEOUT_MINUTES,
    )
    log.info("=" * 60)

    last_charge_current:  float                  = 0.0
    daily_charge_current: float                  = 0.0
    target_soc:           float                  = 90.0
    last_output_priority: OutputPriority | None  = None
    battery_soc:          float | None           = None
    estimated_soc:        float | None           = None
    current_state:        State                  = State.UTI_STOPPED  # safe default until first data
    battery_voltage:      float                  = 52.0
    last_sbu_to_uti_time: datetime | None        = None
    consecutive_failures: int                    = 0
    charge_mode:          ChargeMode             = ChargeMode.NORMAL
    sync_start_time:      datetime | None        = None

    while True:
        daily_charge_current, target_soc, full_charge = load_targets_from_file(
            daily_charge_current, target_soc
        )

        limited_data = fetch_registers()

        # Validate all required keys are present before parsing.
        if limited_data is not None:
            _REQUIRED_KEYS = ("0x0100", "0x0101", "0x0102", "0x021C", "0x0234")
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

            last_battery_soc = battery_soc

            # Register keys use modbus_api v2 hex-address schema:
            #   0x0100 = battery SoC (%)
            #   0x0101 = battery voltage (×0.1 V)
            #   0x0102 = battery current (×0.1 A, signed; positive = charging)
            #   0x021C = load apparent power L1 (W)
            #   0x0234 = load apparent power L2 (W)
            battery_soc     = float(int(limited_data["0x0100"]))
            battery_voltage = int(limited_data["0x0101"]) / 10.0
            battery_current = -_to_signed_16(int(limited_data["0x0102"])) / 10.0
            load_power      = int(limited_data["0x021C"]) + int(limited_data["0x0234"])

            log.debug(
                "Readings: SoC=%d%%  V=%.1f V  I=%+.1f A  load=%.0f W",
                int(battery_soc), battery_voltage, battery_current, load_power,
            )

            # ── Sub-integer SoC estimator ──────────────────────────────────
            if estimated_soc is None or (
                last_battery_soc is not None and abs(battery_soc - last_battery_soc) >= 2
            ):
                log.debug(
                    "SoC estimator snapped to hardware value: %.0f%%", battery_soc
                )
                estimated_soc = battery_soc
            else:
                prev_est = estimated_soc
                if last_battery_soc is not None:
                    if battery_soc == last_battery_soc - 1:
                        estimated_soc = battery_soc + 0.49
                    elif battery_soc == last_battery_soc + 1:
                        estimated_soc = battery_soc - 0.49

                if (
                    last_battery_soc is not None
                    and last_battery_soc == battery_soc
                    and battery_current != 0
                ):
                    delta = battery_current * _SOC_DELTA_PER_A_PER_TICK
                    estimated_soc += delta
                    estimated_soc = max(battery_soc - 0.5, min(battery_soc + 0.5, estimated_soc))

                log.debug(
                    "SoC estimator: hw=%d%%  est %.3f%% → %.3f%%  I=%+.1f A",
                    int(battery_soc), prev_est, estimated_soc, battery_current,
                )

        else:
            consecutive_failures += 1
            if consecutive_failures == 1:
                log.warning(
                    "Register fetch failed — holding previous state "
                    "(V=%.1f V  last_SoC=%s%%)",
                    battery_voltage,
                    f"{battery_soc:.0f}" if battery_soc is not None else "N/A",
                )
            elif consecutive_failures % 12 == 0:
                log.warning(
                    "Register fetch still failing: %d consecutive attempts (%d s elapsed)",
                    consecutive_failures, consecutive_failures * POLL_INTERVAL_S,
                )
            last_battery_soc = battery_soc

        # ── State transitions ─────────────────────────────────────────
        # Only evaluate the state machine with fresh data.  On fetch failure
        # we hold the current state to avoid acting on stale values.  After a
        # sustained outage (FAIL_SAFE_TICKS), force a safe fallback.
        prev_state = current_state

        if limited_data:
            time_period = get_time_period()

            current_state, daily_charge_current, last_sbu_to_uti_time = determine_next_state(
                current_state,
                estimated_soc,
                target_soc,
                battery_voltage,
                time_period,
                daily_charge_current,
                last_sbu_to_uti_time,
                full_charge_active=full_charge,
            )

            if current_state != prev_state:
                log.info(
                    "State: %s → %s  (est_SoC=%.1f%%  V=%.1f V  period=%s)",
                    prev_state.value, current_state.value,
                    estimated_soc if estimated_soc is not None else 0.0,
                    battery_voltage, time_period,
                )
            else:
                log.debug(
                    "State: %s  est_SoC=%.1f%%  V=%.1f V  target_SoC=%.0f%%  period=%s",
                    current_state.value,
                    estimated_soc if estimated_soc is not None else 0.0,
                    battery_voltage, target_soc, time_period,
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
        # Only progresses with fresh data, in cheap period, while flag is set.
        # NORMAL → BULK → BALANCE → SYNC → done (clears flag, returns to NORMAL).
        if limited_data and full_charge and time_period == "cheap":
            now = datetime.now()
            sync_start = _str_to_time(SYNC_START_TIME)
            sync_deadline = _str_to_time(SYNC_DEADLINE)

            if charge_mode == ChargeMode.NORMAL:
                charge_mode = ChargeMode.BULK
                log.info(
                    "Full charge: NORMAL → BULK (V=%.2f V, target_SoC=%.0f%%)",
                    battery_voltage, target_soc,
                )

            if charge_mode == ChargeMode.BULK:
                if (battery_voltage >= BALANCE_ENTRY_VOLTAGE
                        and last_charge_current <= BALANCE_ENTRY_CURRENT):
                    charge_mode = ChargeMode.BALANCE
                    log.info(
                        "Full charge: BULK → BALANCE (V=%.2f V ≥ %.2f, I=%.0f A ≤ %.0f)",
                        battery_voltage, BALANCE_ENTRY_VOLTAGE,
                        last_charge_current, BALANCE_ENTRY_CURRENT,
                    )

            if charge_mode in (ChargeMode.BULK, ChargeMode.BALANCE):
                # Time-based BALANCE → SYNC transition: starts at SYNC_START_TIME.
                # Also catches BULK that didn't reach BALANCE thresholds — still try SYNC nudge.
                if now.time() >= sync_start and now.time() <= sync_deadline:
                    prev_mode = charge_mode
                    charge_mode = ChargeMode.SYNC
                    sync_start_time = now
                    log.info(
                        "Full charge: %s → SYNC (time=%s, V=%.2f V)",
                        prev_mode.value, now.strftime("%H:%M"), battery_voltage,
                    )

            if charge_mode == ChargeMode.SYNC:
                completion_reason: str | None = None
                if battery_soc is not None and battery_soc >= 100:
                    completion_reason = f"BMS SoC reached 100% (V={battery_voltage:.2f} V)"
                elif battery_voltage >= SYNC_VOLTAGE_CEILING:
                    completion_reason = (
                        f"voltage {battery_voltage:.2f} V hit ceiling {SYNC_VOLTAGE_CEILING:.2f} V"
                    )
                elif now.time() > sync_deadline:
                    completion_reason = f"deadline {SYNC_DEADLINE} reached"
                elif sync_start_time is not None:
                    elapsed_min = (now - sync_start_time).total_seconds() / 60.0
                    if elapsed_min >= SYNC_TIMEOUT_MINUTES:
                        completion_reason = f"timeout after {elapsed_min:.1f} min"

                if completion_reason:
                    log.info("Full charge: SYNC complete — %s", completion_reason)
                    _complete_full_charge()
                    charge_mode = ChargeMode.NORMAL
                    sync_start_time = None
                    full_charge = False  # reflect cleared flag for the rest of this tick

        elif limited_data and not full_charge and charge_mode != ChargeMode.NORMAL:
            # Flag cleared externally (e.g., by daily_target rewrite) — reset local mode.
            log.info(
                "Full charge flag cleared from targets — resetting charge_mode %s → NORMAL",
                charge_mode.value,
            )
            charge_mode = ChargeMode.NORMAL
            sync_start_time = None

        elif limited_data and full_charge and time_period != "cheap" and charge_mode != ChargeMode.NORMAL:
            # Cheap period ended mid-progression — abort cleanly without clearing the flag
            # (last_full_charge stays unchanged, so the trigger logic can retry next eligible night).
            log.warning(
                "Full charge: cheap period ended (now=%s, mode=%s) — aborting and resetting to NORMAL",
                datetime.now().strftime("%H:%M"), charge_mode.value,
            )
            charge_mode = ChargeMode.NORMAL
            sync_start_time = None

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
        if limited_data:
            target_charge_current = adjust_battery_charge(
                battery_soc, load_power, battery_voltage, daily_charge_current, current_state,
                charge_mode=charge_mode,
            )
            if last_charge_current != target_charge_current:
                log.info(
                    "Charge current: %.0f A → %.0f A",
                    last_charge_current, target_charge_current,
                )
                if set_charge_current(target_charge_current):
                    last_charge_current = target_charge_current

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
