"""Daily charging target calculator.

Runs once per day (via cron at 22:59) to determine the optimal overnight
charge target and current based on the current battery SoC and tomorrow's
weather forecast.  Results are written to targets.json, which
battery_controller.py reads every polling cycle.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date, datetime, timedelta

import requests

# ── API / config ──────────────────────────────────────────────────────────────

_API_PORT: int = int(os.getenv("MODBUS_API_PORT", "5004"))
_API_BASE: str = f"http://modbus_api:{_API_PORT}"
LIMITED_REGISTERS_URL: str = f"{_API_BASE}/limited_registers"

# Japan Meteorological Agency public forecast endpoint.
# 280000 = Osaka prefecture.  Edit for your region if needed.
WEATHER_API_URL = "https://www.jma.go.jp/bosai/forecast/data/forecast/280000.json"

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/targets.json")

# ── Battery constants ─────────────────────────────────────────────────────────

BATTERY_CAPACITY_AH: float = 520.0

# Wh stored per 1% SoC change.
# These differ because average bus voltage is higher during charging (~52 V)
# than during discharge (~49 V).
BATTERY_WH_PER_SOC_CHARGING:    float = 270.0   # 520 Ah × 52 V / 100  — used when planning charge
BATTERY_WH_PER_SOC_DISCHARGING: float = 255.0   # 520 Ah × 49 V / 100  — used when estimating drain

BATTERY_NOMINAL_VOLTAGE_V: float = 53.0  # voltage assumed for required-current calculation

AVERAGE_LOAD_W: float = 1000.0  # assumed average load for SoC-drain estimation

# ── Register fetch ────────────────────────────────────────────────────────────


def fetch_registers() -> dict | None:
    """Fetch the limited register set from modbus_api. Returns None on failure."""
    try:
        r = requests.get(LIMITED_REGISTERS_URL, timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching registers: {e}")
        return None


# ── Weather ───────────────────────────────────────────────────────────────────


def fetch_tomorrow_weather_code() -> int:
    """Return tomorrow's JMA weather code, defaulting to 200 (cloudy) on any failure."""
    try:
        r = requests.get(WEATHER_API_URL, timeout=10)
        r.raise_for_status()
        weather_data = r.json()

        southern_data = [
            item for item in weather_data
            if any(
                area["area"]["name"] == "南部"
                for area in item["timeSeries"][0]["areas"]
            )
        ]
        # timeSeries[0].areas[0].weatherCodes[0] = today, [1] = tomorrow
        code = int(southern_data[0]["timeSeries"][0]["areas"][0]["weatherCodes"][1])
        print(f"Tomorrow's weather code: {code}")
        return code
    except Exception as e:
        # Catches network errors, JSON parse failures, IndexError, KeyError, etc.
        print(f"Weather fetch/parse failed: {e} — defaulting to code 200 (cloudy)")
        return 200


WEATHER_TIER_RULES: list[tuple] = [
    (lambda wc: wc == 100,                   1),  # Clear
    (lambda wc: 101 <= wc < 200,             2),  # Mostly sunny
    (lambda wc: wc in {200, 201, 210, 211},  3),  # Partly cloudy
    (lambda wc: 200 <= wc < 300,             4),  # Cloudy
    (lambda wc: wc in {300, 301, 311, 313},  5),  # Rain / severe
]


def determine_weather_tier(weather_code: int) -> int:
    for condition, tier in WEATHER_TIER_RULES:
        try:
            if condition(weather_code):
                return tier
        except Exception:
            continue
    return 3  # default: partly cloudy


# ── SOC target lookup table ───────────────────────────────────────────────────
# Indexed by month (1–12), five tiers (tier 1 = sunniest → tier 5 = worst).
# Values are the target SOC (%) at the start of the cheap-rate charging window.

MONTHLY_TARGET_SOC_TABLE: dict[int, list[int]] = {
    1:  [55, 60, 70, 85, 101],
    2:  [50, 55, 65, 80, 101],
    3:  [40, 45, 60, 75, 101],
    4:  [30, 35, 45, 60,  70],
    5:  [25, 30, 35, 40,  60],
    6:  [30, 35, 40, 55,  70],
    7:  [45, 50, 55, 65, 101],
    8:  [45, 50, 55, 65, 101],
    9:  [35, 40, 45, 60,  80],
    10: [30, 35, 45, 60,  70],
    11: [30, 35, 50, 70,  80],
    12: [45, 50, 65, 80, 101],
}


def determine_target_soc(weather_code: int, month: int) -> int:
    """Return the target SoC (%) for tomorrow based on weather tier and month."""
    soc_list = MONTHLY_TARGET_SOC_TABLE.get(month, MONTHLY_TARGET_SOC_TABLE[5])
    tier = determine_weather_tier(weather_code)        # 1..5
    tier_index = tier - 1                              # 0..4

    if not (0 <= tier_index < len(soc_list) == 5):
        print(f"Unexpected tier_index={tier_index} — using conservative fallback 25%")
        return 25

    target = soc_list[tier_index]
    print(f"Month={month}, weather code={weather_code} (Tier {tier}), target SoC={target}%")
    return target


# ── Charging calculations ─────────────────────────────────────────────────────


def calculate_required_current(
    battery_soc: float,
    target_soc: float,
    charging_hours: float,
) -> float:
    """Return the charge current (A) needed to reach *target_soc* within *charging_hours*."""
    soc_diff = target_soc - battery_soc
    if soc_diff <= 0:
        print("SoC difference ≤ 0 — no charging needed.")
        return 0.0

    required_wh     = soc_diff * BATTERY_WH_PER_SOC_CHARGING
    required_power  = required_wh / charging_hours
    required_amps   = required_power / BATTERY_NOMINAL_VOLTAGE_V
    rounded         = max(math.ceil(required_amps), 10)   # minimum 10 A
    print(f"Required current: {required_amps:.2f} A → rounded up to {rounded} A")
    return float(rounded)


def estimate_soc_at_2259(current_soc: float) -> float:
    """Estimate battery SoC at 22:59 assuming AVERAGE_LOAD_W average consumption."""
    now = datetime.now()
    target_time = now.replace(hour=22, minute=59, second=0, microsecond=0)
    if target_time <= now:
        target_time += timedelta(days=1)

    hours_until_2259 = (target_time - now).total_seconds() / 3600.0
    energy_consumed  = AVERAGE_LOAD_W * hours_until_2259          # Wh
    soc_decrease     = energy_consumed / BATTERY_WH_PER_SOC_DISCHARGING
    estimated        = max(0.0, current_soc - soc_decrease)
    print(
        f"Estimated SoC at 22:59: {estimated:.2f}% "
        f"(current: {current_soc}%, hours to go: {hours_until_2259:.2f})"
    )
    return estimated


def calculate_charging_hours(until_time_str: str | None = None) -> float:
    """Return hours from now until *until_time_str* (default: next 05:30).

    If *until_time_str* is given, it must be in HH:MM format.
    Returns 0 if the target time is already in the past.
    """
    now = datetime.now()

    if until_time_str:
        try:
            hour, minute = map(int, until_time_str.split(":"))
        except ValueError:
            raise ValueError(f"Invalid time format '{until_time_str}'. Use HH:MM.")
        until = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if until <= now:
            until += timedelta(days=1)
    else:
        # Default target: 05:30 the following morning
        until = (now + timedelta(days=1)).replace(hour=5, minute=30, second=0, microsecond=0)
        # If it's currently before 05:30, the target is today (not tomorrow +1)
        if now.hour < 5 or (now.hour == 5 and now.minute < 30):
            until = now.replace(hour=5, minute=30, second=0, microsecond=0)

    return max((until - now).total_seconds() / 3600.0, 0.0)


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate and write the overnight charging target."
    )
    parser.add_argument(
        "--estimate-start-soc",
        action="store_true",
        help="Estimate SoC at 22:59 from current SoC using average load.",
    )
    parser.add_argument("--start-soc", type=int, help="Use this SoC instead of fetching.")
    parser.add_argument("--target-soc", type=int, help="Use this target SoC instead of weather-based.")
    parser.add_argument("--charging-hours", type=float, help="Override calculated charging hours.")
    parser.add_argument("--weather-code", type=int, help="Override JMA weather code.")
    parser.add_argument(
        "--until-time",
        help="Charge until this time (HH:MM). Default: 05:30 next morning.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results without writing to targets.json.",
    )
    return parser.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    args = parse_args()

    # ── Determine starting SoC ─────────────────────────────────────────────
    if args.start_soc is not None:
        battery_soc = float(args.start_soc)
        print(f"Using specified start SoC: {battery_soc}%")
    else:
        data = fetch_registers()
        if not data:
            print("Failed to fetch registers — exiting.", file=sys.stderr)
            sys.exit(1)
        # 0x0100 = battery SoC (%) — modbus_api v2 hex-address schema
        battery_soc = float(int(data["0x0100"]))
        print(f"Fetched current SoC: {battery_soc}%")

    if args.estimate_start_soc and args.start_soc is None:
        battery_soc = estimate_soc_at_2259(battery_soc)

    # ── Determine target SoC ───────────────────────────────────────────────
    month = date.today().month

    if args.target_soc is not None:
        target_soc = args.target_soc
        print(f"Using specified target SoC: {target_soc}%")
    else:
        weather_code = args.weather_code if args.weather_code is not None else fetch_tomorrow_weather_code()
        target_soc = determine_target_soc(weather_code, month)

    # ── Determine charging window ──────────────────────────────────────────
    if args.charging_hours is not None and args.until_time is not None:
        print("Error: --charging-hours and --until-time are mutually exclusive.", file=sys.stderr)
        sys.exit(1)

    if args.charging_hours is not None:
        charging_hours = args.charging_hours
    else:
        charging_hours = calculate_charging_hours(args.until_time)
        print(f"Charging window: {charging_hours:.2f} hours")

    # ── Calculate required current ─────────────────────────────────────────
    daily_charge_current = calculate_required_current(battery_soc, target_soc, charging_hours)
    print(
        f"Result — target_soc={target_soc}%, "
        f"daily_charge_current={daily_charge_current} A "
        f"(charging hours: {charging_hours:.2f})"
    )

    # ── Write targets.json ─────────────────────────────────────────────────
    if args.dry_run:
        print("Dry-run: targets.json not modified.")
        return

    targets = {"target_soc": target_soc, "daily_charge_current": daily_charge_current}
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        print(f"Wrote targets to {CONFIG_PATH}: {targets}")
    except Exception as e:
        print(f"Failed to write targets.json: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    print(f"Starting daily target calculation at {datetime.now()}")
    main()
