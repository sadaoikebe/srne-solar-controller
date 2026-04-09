"""Battery charge controller.

Polls the inverter every POLL_INTERVAL_S seconds via modbus_api, computes the
desired output priority and charge current, and pushes changes back.
"""
from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime
from enum import Enum, IntEnum
from typing import Optional

import requests

# ── Hardware / system constants ───────────────────────────────────────────────

POLL_INTERVAL_S: int = 5

# Battery bank parameters — update these if the pack changes.
BATTERY_CAPACITY_AH: float = 520.0

# Wh stored per 1% SoC change.
# Charging and discharging use slightly different values because the average
# bus voltage differs: ~52 V during charging vs ~49 V during discharge.
BATTERY_WH_PER_SOC_CHARGING:    float = 270.0   # 520 Ah × 52 V / 100
BATTERY_WH_PER_SOC_DISCHARGING: float = 255.0   # 520 Ah × 49 V / 100

# Pre-computed: SoC (%) change per amp-second, scaled to one polling tick.
# Derivation: 100 % / (capacity_Ah × 3600 s/h) × POLL_INTERVAL_S
_SOC_DELTA_PER_A_PER_TICK: float = (
    100.0 / (BATTERY_CAPACITY_AH * 3600.0) * POLL_INTERVAL_S
)  # ≈ 1/3744  (at 520 Ah, 5 s)

GRID_MAX_POWER_W: float    = 9000.0   # Maximum grid power budget (W)
HYSTERESIS_SOC:   float    = 2.0      # SoC hysteresis band (%)
CUTOFF_SOC:       float    = 9.0      # Emergency SoC floor (%)
SBU_TO_UTI_COOLDOWN_S: int = 30 * 60  # Minimum seconds between SBU→UTI switches

# ── Runtime configuration ─────────────────────────────────────────────────────

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/targets.json")

_API_PORT: int = int(os.getenv("MODBUS_API_PORT", "5004"))
_API_BASE: str = f"http://modbus_api:{_API_PORT}"

LIMITED_REGISTERS_URL:   str = f"{_API_BASE}/limited_registers"
SET_CHARGE_CURRENT_URL:  str = f"{_API_BASE}/set_charge_current"
SET_OUTPUT_PRIORITY_URL: str = f"{_API_BASE}/set_output_priority"

# Optional HTTP Basic Auth for write endpoints — reads from the same env vars
# that modbus_api uses, so no extra secrets are needed.
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


# ── Time-period helpers ───────────────────────────────────────────────────────

# Edit these entries to match your electricity tariff schedule.
# Each period must have a "name", "start", and "end" in "H:MM" or "HH:MM".
# Periods may wrap midnight (e.g. "23:01"→"6:58").
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
    """True if *current* is within [start, end], crossing midnight when start > end."""
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def get_time_period() -> str:
    """Return the name of the current time period, or 'unknown' if none matches."""
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
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching registers: {e}")
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
            return True
        print(f"Error setting charge current: {result.get('message')}")
        return False
    except requests.RequestException as e:
        print(f"Error setting charge current: {e}")
        return False


def set_output_priority(priority: int) -> bool:
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
            print(f"Set Output Priority: {result['value']}")
            return True
        print(f"Error setting Output Priority: {result.get('message')}")
        return False
    except requests.RequestException as e:
        print(f"Error setting Output Priority: {e}")
        return False


def update_targets_json(daily_charge_current: float, target_soc: float) -> None:
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(
                {"target_soc": target_soc, "daily_charge_current": daily_charge_current}, f
            )
        print(
            f"Wrote targets: target_soc={target_soc}, "
            f"daily_charge_current={daily_charge_current} A"
        )
    except Exception as e:
        print(f"Failed to write targets.json: {e}")


def load_targets_from_file(
    current_daily_charge_current: float,
    current_target_soc: float,
) -> tuple[float, float]:
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
        return (
            float(targets.get("daily_charge_current", current_daily_charge_current)),
            float(targets.get("target_soc", current_target_soc)),
        )
    except Exception as e:
        print(
            f"Failed to load targets.json: {e} — "
            f"using target_soc={current_target_soc}, "
            f"daily_charge_current={current_daily_charge_current}"
        )
        return current_daily_charge_current, current_target_soc


# ── Control logic ─────────────────────────────────────────────────────────────


def calculate_grid_limit_current(load_power: float, battery_voltage: float) -> float:
    """Maximum charge current (A) without exceeding the grid power budget."""
    grid_headroom = GRID_MAX_POWER_W - load_power
    if 30.0 < battery_voltage < 70.0:
        return math.floor((grid_headroom / battery_voltage) / 5.0) * 5.0
    return 0.0


def determine_next_state(
    current_state: State,
    estimated_soc: float | None,
    target_soc: float,
    battery_voltage: float,
    time_period: str,
    daily_charge_current: float,
    last_sbu_to_uti_time: datetime | None,
) -> tuple[State, float, datetime | None]:
    """Compute the next control state and any side-effects on targets.

    Returns (next_state, new_daily_charge_current, new_last_sbu_to_uti_time).
    If estimated_soc is None (no data yet), the current state is held.
    """
    if estimated_soc is None:
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
                    print(f"UTI→SBU suppressed: cooldown active ({remaining:.0f} s remaining)")
            elif battery_voltage > 50.6:
                next_state = State.UTI_STOPPED

        elif current_state == State.UTI_STOPPED:
            if battery_voltage > 51.6 and estimated_soc > CUTOFF_SOC:
                if cooldown_elapsed:
                    next_state = State.SBU
                else:
                    remaining = SBU_TO_UTI_COOLDOWN_S - (now - last_sbu_to_uti_time).total_seconds()
                    print(f"UTI→SBU suppressed: cooldown active ({remaining:.0f} s remaining)")
            elif battery_voltage < 49.4:
                next_state = State.UTI_CHARGING

        else:  # State.SBU
            if battery_voltage < 49.4:
                next_state = State.UTI_CHARGING
                new_last_sbu_to_uti_time = now
                print(
                    f"SBU→UTI_CHARGING: low voltage ({battery_voltage:.1f} V), cooldown started"
                )
            elif battery_voltage < 49.6 or estimated_soc <= CUTOFF_SOC:
                next_state = State.UTI_STOPPED
                new_last_sbu_to_uti_time = now
                print(
                    f"SBU→UTI_STOPPED: voltage ({battery_voltage:.1f} V) "
                    f"or low SoC ({estimated_soc:.1f}%), cooldown started"
                )

    elif time_period == "cheap":
        if current_state == State.UTI_CHARGING:
            if estimated_soc > target_soc + HYSTERESIS_SOC:
                next_state = State.SBU
                lower_charge_current = True
            elif estimated_soc > target_soc + 0.4:
                next_state = State.UTI_STOPPED
                lower_charge_current = True

        elif current_state == State.UTI_STOPPED:
            if estimated_soc > target_soc + HYSTERESIS_SOC:
                next_state = State.SBU
            elif estimated_soc < target_soc - 0.4:
                next_state = State.UTI_CHARGING

        else:  # State.SBU
            if estimated_soc < target_soc - 0.4:
                next_state = State.UTI_CHARGING
            elif estimated_soc < target_soc + 0.4:
                next_state = State.UTI_STOPPED

    # "unknown" time period — hold current state without any transitions

    if lower_charge_current:
        new_daily_charge_current = min(10.0, daily_charge_current)
        if new_daily_charge_current != daily_charge_current:
            update_targets_json(new_daily_charge_current, target_soc)
            print(f"Lowered daily_charge_current to {new_daily_charge_current} A")

    return next_state, new_daily_charge_current, new_last_sbu_to_uti_time


def adjust_battery_charge(
    battery_soc: float,
    load_power: float,
    battery_voltage: float,
    daily_charge_current: float,
    state: State,
) -> float:
    """Return the target charge current (A) for the given state and conditions."""
    if state in (State.SBU, State.UTI_STOPPED):
        return 0.0

    # State.UTI_CHARGING — apply SoC-taper and voltage-taper limits
    grid_limit = calculate_grid_limit_current(load_power, battery_voltage)
    target = daily_charge_current

    SOC_LIMITS = [
        (60, 120), (70, 105), (80,  90), (85, 80),
        (90,  70), (93,  60), (96,  50), (98, 40),
        (99,  30), (100, 20),
    ]
    for soc_threshold, limit in SOC_LIMITS:
        if battery_soc < soc_threshold:
            target = min(float(limit), target)
            break
    else:
        target = min(10.0, target)

    VOLT_LIMITS = [
        (55.2, 120), (55.6,  80), (55.8, 60), (56.0, 40),
        (56.3,  30), (56.5,  24), (56.6, 18), (56.7, 14),
        (56.8,  10), (56.9,   7),
    ]
    for volt_threshold, limit in VOLT_LIMITS:
        if battery_voltage < volt_threshold:
            target = min(float(limit), target)
            break
    else:
        target = min(2.0, target)

    return min(grid_limit, target)


def determine_output_priority(state: State) -> OutputPriority:
    return OutputPriority.SBU if state == State.SBU else OutputPriority.UTI


# ── Main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    last_charge_current:  float            = 0.0
    daily_charge_current: float            = 0.0
    target_soc:           float            = 90.0
    last_output_priority: OutputPriority | None = None
    battery_soc:          float | None     = None
    estimated_soc:        float | None     = None
    current_state:        State            = State.SBU
    battery_voltage:      float            = 52.0
    last_sbu_to_uti_time: datetime | None  = None

    print("Starting charge controller...")
    while True:
        daily_charge_current, target_soc = load_targets_from_file(
            daily_charge_current, target_soc
        )

        limited_data = fetch_registers()

        if limited_data:
            last_battery_soc = battery_soc

            # Register-key → value (modbus_api v2 hex-address schema):
            #   0x0100 = battery SoC (%)
            #   0x0101 = battery voltage (raw × 0.1 V)
            #   0x0102 = battery current (raw × 0.1 A, signed 16-bit;
            #            inverter sign: negative register value = charging)
            #   0x021C = load apparent power L1 (W)
            #   0x0234 = load apparent power L2 (W)
            battery_soc     = float(int(limited_data["0x0100"]))
            battery_voltage = int(limited_data["0x0101"]) / 10.0
            # After sign-flip: positive = charging, negative = discharging
            battery_current = -_to_signed_16(int(limited_data["0x0102"])) / 10.0
            load_power      = int(limited_data["0x021C"]) + int(limited_data["0x0234"])

            # ── Sub-integer SoC estimator ──────────────────────────────────
            if estimated_soc is None or (
                last_battery_soc is not None and abs(battery_soc - last_battery_soc) >= 2
            ):
                # First reading or large jump — snap directly to hardware value
                estimated_soc = battery_soc
            else:
                if last_battery_soc is not None:
                    if battery_soc == last_battery_soc - 1:
                        # Display just ticked down; real SoC is near the top of the new integer
                        estimated_soc = battery_soc + 0.49
                    elif battery_soc == last_battery_soc + 1:
                        estimated_soc = battery_soc - 0.49

                # Integrate measured current when the displayed integer hasn't changed
                if (
                    last_battery_soc is not None
                    and last_battery_soc == battery_soc
                    and battery_current != 0
                ):
                    estimated_soc += battery_current * _SOC_DELTA_PER_A_PER_TICK
                    # Clamp to ±0.5% around the hardware-integer reading
                    estimated_soc = max(battery_soc - 0.5, min(battery_soc + 0.5, estimated_soc))

        else:
            last_battery_soc = battery_soc
            load_power       = 0.0
            battery_voltage  = 53.0

        time_period = get_time_period()

        current_state, daily_charge_current, last_sbu_to_uti_time = determine_next_state(
            current_state,
            estimated_soc,
            target_soc,
            battery_voltage,
            time_period,
            daily_charge_current,
            last_sbu_to_uti_time,
        )

        desired_priority = determine_output_priority(current_state)
        if last_output_priority != desired_priority:
            set_output_priority(desired_priority)
            last_output_priority = desired_priority

        if limited_data:
            target_charge_current = adjust_battery_charge(
                battery_soc, load_power, battery_voltage, daily_charge_current, current_state
            )
            if last_charge_current != target_charge_current:
                set_charge_current(target_charge_current)
                last_charge_current = target_charge_current

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
