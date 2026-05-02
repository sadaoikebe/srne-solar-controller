"""Daily charging target calculator.

Runs once per day (via cron at 22:59) to determine the optimal overnight
charge target and current based on the current battery SoC and tomorrow's
weather forecast.  Results are written to targets.json.

Log levels
----------
  DEBUG  — detailed calculation steps, intermediate SoC/energy values
  INFO   — weather code, tier, target SoC, charging hours, required current,
           result written to file
  WARNING — weather API failure (default used), SoC already at target
  ERROR  — register fetch failure, file write failure
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date, datetime, timedelta

import requests

from log_config import get_logger

log = get_logger("daily_target")

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

# Wh stored per 1% SoC change.  Different for charge vs discharge because the
# average bus voltage differs (~52 V charge vs ~49 V discharge).
BATTERY_WH_PER_SOC_CHARGING:    float = 270.0   # 520 Ah × 52 V / 100
BATTERY_WH_PER_SOC_DISCHARGING: float = 255.0   # 520 Ah × 49 V / 100

BATTERY_NOMINAL_VOLTAGE_V: float = 53.0  # assumed voltage for required-current calculation
AVERAGE_LOAD_W:            float = 1000.0  # assumed average load for SoC-drain estimation

# ── Register fetch ────────────────────────────────────────────────────────────


def fetch_registers() -> dict | None:
    try:
        r = requests.get(LIMITED_REGISTERS_URL, timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        log.error("Register fetch failed: %s", e)
        return None


# ── Weather ───────────────────────────────────────────────────────────────────


def fetch_tomorrow_weather_code() -> int:
    """Return tomorrow's JMA weather code, defaulting to 200 (cloudy) on any failure."""
    try:
        log.debug("Fetching weather from %s", WEATHER_API_URL)
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
        log.info("JMA weather code for tomorrow: %d", code)
        return code
    except Exception as e:
        log.warning(
            "Weather fetch/parse failed: %s — defaulting to code 200 (cloudy)", e
        )
        return 200


WEATHER_TIER_RULES: list[tuple] = [
    (lambda wc: wc == 100,                   1),  # Clear
    (lambda wc: 101 <= wc < 200,             2),  # Mostly sunny
    (lambda wc: wc in {200, 201, 210, 211},  3),  # Partly cloudy
    (lambda wc: 200 <= wc < 300,             4),  # Cloudy
    (lambda wc: wc in {300, 301, 311, 313},  5),  # Rain / severe
]

_TIER_NAMES = {1: "clear", 2: "mostly sunny", 3: "partly cloudy", 4: "cloudy", 5: "rain/severe"}


def determine_weather_tier(weather_code: int) -> int:
    for condition, tier in WEATHER_TIER_RULES:
        try:
            if condition(weather_code):
                return tier
        except Exception:
            continue
    log.debug("No tier matched for weather code %d — defaulting to tier 3", weather_code)
    return 3


# ── SoC target lookup table ───────────────────────────────────────────────────
# Indexed by month (1–12), five tiers (tier 1 = sunniest → tier 5 = worst).

MONTHLY_TARGET_SOC_TABLE: dict[int, list[int]] = {
    1:  [55, 60, 70, 85, 95],
    2:  [50, 55, 65, 80, 95],
    3:  [40, 45, 60, 75, 95],
    4:  [30, 35, 45, 60, 70],
    5:  [25, 30, 35, 40, 60],
    6:  [30, 35, 40, 55, 70],
    7:  [45, 50, 55, 65, 95],
    8:  [45, 50, 55, 65, 95],
    9:  [35, 40, 45, 60, 80],
    10: [30, 35, 45, 60, 70],
    11: [30, 35, 50, 70, 80],
    12: [45, 50, 65, 80, 95],
}

# Full charge (LFP balancing / SoC sync) is triggered the night before a
# tier-5 (worst forecast) day, but no more often than this many days.
# Sunny-day cost of wasted PV >> cost of slightly stale SoC, so the trigger
# is gated on weather *and* this minimum interval.
FULL_CHARGE_MIN_INTERVAL_DAYS: int = 30
FULL_CHARGE_MIN_TARGET_SOC:    int = 90  # Ensure BULK has headroom to reach absorption


def _load_last_full_charge() -> date | None:
    """Read the last successful full-charge date from targets.json, if recorded."""
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
        s = targets.get("last_full_charge")
        if not s:
            return None
        return date.fromisoformat(s)
    except Exception as e:
        log.debug("Could not load last_full_charge from targets.json: %s", e)
        return None


def should_trigger_full_charge(weather_code: int, today: date) -> bool:
    """Return True iff tomorrow is tier 5 AND it's been at least
    FULL_CHARGE_MIN_INTERVAL_DAYS since the last completed full charge."""
    tier = determine_weather_tier(weather_code)
    if tier != 5:
        log.debug("Full-charge trigger: tier=%d (need 5) — skip", tier)
        return False

    last = _load_last_full_charge()
    if last is None:
        log.info("Full-charge trigger: tier 5 and no prior full charge recorded — TRIGGER")
        return True

    days_since = (today - last).days
    if days_since >= FULL_CHARGE_MIN_INTERVAL_DAYS:
        log.info(
            "Full-charge trigger: tier 5 and %d days since last (%s, ≥ %d) — TRIGGER",
            days_since, last.isoformat(), FULL_CHARGE_MIN_INTERVAL_DAYS,
        )
        return True

    log.info(
        "Full-charge trigger: tier 5 but only %d days since last (%s, < %d) — skip",
        days_since, last.isoformat(), FULL_CHARGE_MIN_INTERVAL_DAYS,
    )
    return False


def determine_target_soc(weather_code: int, month: int) -> int:
    """Return the target SoC (%) for tomorrow based on weather tier and month."""
    soc_list = MONTHLY_TARGET_SOC_TABLE.get(month, MONTHLY_TARGET_SOC_TABLE[5])
    tier      = determine_weather_tier(weather_code)
    tier_name = _TIER_NAMES.get(tier, "unknown")
    tier_idx  = tier - 1

    if not (0 <= tier_idx < len(soc_list) == 5):
        log.error(
            "Unexpected tier_index=%d for weather_code=%d — using conservative fallback 25%%",
            tier_idx, weather_code,
        )
        return 25

    target = soc_list[tier_idx]
    log.info(
        "Target SoC: month=%d  code=%d  tier=%d (%s) → %d%%",
        month, weather_code, tier, tier_name, target,
    )
    return target


# ── Charging calculations ─────────────────────────────────────────────────────


def calculate_required_current(
    battery_soc: float,
    target_soc: float,
    charging_hours: float,
) -> float:
    """Return the charge current (A) needed to reach *target_soc* in *charging_hours*."""
    soc_diff = target_soc - battery_soc
    if soc_diff <= 0:
        log.warning(
            "SoC %.1f%% already meets or exceeds target %.0f%% — no charging required",
            battery_soc, target_soc,
        )
        return 0.0

    required_wh    = soc_diff * BATTERY_WH_PER_SOC_CHARGING
    required_power = required_wh / charging_hours
    required_amps  = required_power / BATTERY_NOMINAL_VOLTAGE_V
    rounded        = max(math.ceil(required_amps), 10)

    log.info(
        "Required charge: %.1f%% × %.0f Wh/%% = %.0f Wh  "
        "÷ %.2f h  ÷ %.0f V  = %.2f A  → %d A (min 10 A)",
        soc_diff, BATTERY_WH_PER_SOC_CHARGING, required_wh,
        charging_hours, BATTERY_NOMINAL_VOLTAGE_V, required_amps, rounded,
    )
    return float(rounded)


def estimate_soc_at_2259(current_soc: float) -> float:
    """Estimate battery SoC at 22:59 assuming AVERAGE_LOAD_W average consumption."""
    now         = datetime.now()
    target_time = now.replace(hour=22, minute=59, second=0, microsecond=0)
    if target_time <= now:
        target_time += timedelta(days=1)

    hours_until_2259 = (target_time - now).total_seconds() / 3600.0
    energy_consumed  = AVERAGE_LOAD_W * hours_until_2259
    soc_decrease     = energy_consumed / BATTERY_WH_PER_SOC_DISCHARGING
    estimated        = max(0.0, current_soc - soc_decrease)

    log.info(
        "SoC estimate at 22:59: current=%.1f%%  hours=%.2f h  "
        "load=%.0f W  drain=%.1f%% (%.0f Wh ÷ %.0f Wh/%%)  → %.1f%%",
        current_soc, hours_until_2259, AVERAGE_LOAD_W,
        soc_decrease, energy_consumed, BATTERY_WH_PER_SOC_DISCHARGING, estimated,
    )
    return estimated


def calculate_charging_hours(until_time_str: str | None = None) -> float:
    """Return hours from now until *until_time_str* (default: next 05:30)."""
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
        until = (now + timedelta(days=1)).replace(hour=5, minute=30, second=0, microsecond=0)
        if now.hour < 5 or (now.hour == 5 and now.minute < 30):
            until = now.replace(hour=5, minute=30, second=0, microsecond=0)

    hours = max((until - now).total_seconds() / 3600.0, 0.0)
    log.info(
        "Charging window: now=%s  until=%s  → %.2f h",
        now.strftime("%H:%M"), until.strftime("%Y-%m-%d %H:%M"), hours,
    )
    return hours


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
    parser.add_argument("--start-soc",      type=int,   help="Use this SoC instead of fetching.")
    parser.add_argument("--target-soc",     type=int,   help="Override weather-based target SoC.")
    parser.add_argument("--charging-hours", type=float, help="Override calculated charging window.")
    parser.add_argument("--weather-code",   type=int,   help="Override JMA weather code.")
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
    args  = parse_args()
    month = date.today().month

    log.info("=" * 60)
    log.info("Daily target calculation started")
    log.info("  Date          : %s (month=%d)", date.today().isoformat(), month)
    log.info("  Config file   : %s", CONFIG_PATH)
    log.info("  API base      : %s", _API_BASE)
    log.info("  Dry run       : %s", args.dry_run)
    log.info("=" * 60)

    # ── Starting SoC ──────────────────────────────────────────────────────
    if args.start_soc is not None:
        battery_soc = float(args.start_soc)
        log.info("Using CLI start SoC: %.0f%%", battery_soc)
    else:
        data = fetch_registers()
        if not data:
            log.error("Failed to fetch registers — cannot calculate target, exiting")
            sys.exit(1)
        battery_soc = float(int(data["0x0100"]))
        log.info("Current SoC from inverter: %.0f%%", battery_soc)

    if args.estimate_start_soc and args.start_soc is None:
        battery_soc = estimate_soc_at_2259(battery_soc)

    log.debug("Effective start SoC for calculation: %.2f%%", battery_soc)

    # ── Target SoC ────────────────────────────────────────────────────────
    weather_code: int | None = None
    if args.target_soc is not None:
        target_soc = args.target_soc
        log.info("Using CLI target SoC: %d%%", target_soc)
    else:
        weather_code = (
            args.weather_code if args.weather_code is not None
            else fetch_tomorrow_weather_code()
        )
        target_soc = determine_target_soc(weather_code, month)

    # ── Full-charge trigger ───────────────────────────────────────────────
    # Only trigger on tier-5 nights and not more often than the min interval —
    # full charge wastes the next day's PV, so cost is high on sunny days.
    full_charge = False
    if weather_code is not None and args.target_soc is None:
        full_charge = should_trigger_full_charge(weather_code, date.today())
        if full_charge and target_soc < FULL_CHARGE_MIN_TARGET_SOC:
            log.info(
                "Full charge: lifting target_soc %d%% → %d%% to give BULK enough headroom",
                target_soc, FULL_CHARGE_MIN_TARGET_SOC,
            )
            target_soc = FULL_CHARGE_MIN_TARGET_SOC

    # ── Charging window ────────────────────────────────────────────────────
    if args.charging_hours is not None and args.until_time is not None:
        log.error("--charging-hours and --until-time are mutually exclusive")
        sys.exit(1)

    if args.charging_hours is not None:
        charging_hours = args.charging_hours
        log.info("Using CLI charging hours: %.2f h", charging_hours)
    else:
        charging_hours = calculate_charging_hours(args.until_time)

    # ── Required current ───────────────────────────────────────────────────
    daily_charge_current = calculate_required_current(battery_soc, target_soc, charging_hours)

    log.info(
        "Result: target_soc=%d%%  daily_charge_current=%.0f A  (over %.2f h)",
        target_soc, daily_charge_current, charging_hours,
    )

    # ── Write targets.json ─────────────────────────────────────────────────
    if args.dry_run:
        log.info(
            "Dry run — targets.json not modified (would set full_charge=%s)", full_charge
        )
        return

    # Preserve last_full_charge (set by battery_controller on completion); only the
    # full_charge flag and the daily values are owned by this script.
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
    except Exception:
        targets = {}
    targets["target_soc"] = target_soc
    targets["daily_charge_current"] = daily_charge_current
    targets["full_charge"] = full_charge
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        log.info(
            "Wrote targets to %s: target_soc=%d%%  daily_charge_current=%.0f A  full_charge=%s",
            CONFIG_PATH, target_soc, daily_charge_current, full_charge,
        )
    except Exception as e:
        log.error("Failed to write targets.json: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
