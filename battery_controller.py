import time
import math
import requests
import json
from enum import IntEnum

TIME_MARGIN_MINUTES = 1
HYSTERESIS_SOC = 2

LIMITED_REGISTERS_URL = "http://localhost:5004/limited_registers"
SET_CHARGE_CURRENT_URL = "http://localhost:5004/set_charge_current"

class OutputPriority(IntEnum):
    SOL = 0
    UTI = 1
    SBU = 2

def fetch_registers():
    try:
        response = requests.get(LIMITED_REGISTERS_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching registers: {e}")
        return None

def set_charge_current(current):
    try:
        response = requests.post(SET_CHARGE_CURRENT_URL, json={"value": current})
        response.raise_for_status()
        result = response.json()
        if result.get('success'):
            print(f"Set charge current successfully: value={result['value']}")
            return True
        else:
            print(f"Error setting charge current: {result.get('message')}")
            return False
    except requests.RequestException as e:
        print(f"Error setting charge current: {e}")
        return False

def set_output_priority(priority):
    try:
        response = requests.post("http://localhost:5004/set_output_priority", json={"value": priority})
        response.raise_for_status()
        result = response.json()
        if result.get('success'):
            print(f"Set Output Priority successfully: {result['value']}")
            return True
        else:
            print(f"Error setting Output Priority: {result.get('message')}")
            return False
    except requests.RequestException as e:
        print(f"Error setting Output Priority: {e}")
        return False

def calculate_grid_limit_current(load_power, battery_voltage):
    GRID_MAX_POWER = 9200
    grid_max_draw = GRID_MAX_POWER - load_power
    if 30 < battery_voltage < 70:
        return math.floor((grid_max_draw / battery_voltage) / 5) * 5
    return 0

def adjust_battery_charge(battery_soc, load_power, battery_voltage, daily_charge_current, target_soc):
    if battery_soc > target_soc or battery_soc >= 100:
        return 0
    if battery_soc == target_soc:
        return 1

    grid_limit_current = calculate_grid_limit_current(load_power, battery_voltage)
    target_charge_current = daily_charge_current

    soc_charge_limits = [
        (60, 120),  # SOC < 60: 120A
        (70, 105),  # 60 <= SOC < 70: 105A
        (80, 90),   # 70 <= SOC < 80: 90A
        (90, 75),   # 80 <= SOC < 90: 75A
        (96, 55),   # 90 <= SOC < 96: 55A
        (99, 40),   # 96 <= SOC < 99: 40A
        (100, 25),  # 99 <= SOC < 100: 25A
    ]

    for soc_threshold, limit in soc_charge_limits:
        if battery_soc < soc_threshold:
            target_charge_current = min(limit, target_charge_current)
            break
    else:
        target_charge_current = min(10, target_charge_current)  # SOC >= 100: 10A

    # 時間帯ごとの追加制限
    current_hour = time.localtime().tm_hour
    if 7 <= current_hour <= 22:
        target_charge_current = min(5, target_charge_current)
    if 1 <= current_hour <= 6:
        target_charge_current = min(100, target_charge_current)

    target_charge_current = min(grid_limit_current, target_charge_current)
    return target_charge_current

def load_targets_from_file(current_daily_charge_current, current_target_soc):
    try:
        with open("/opt/modbus_api/targets.json", "r") as f:
            targets = json.load(f)
            daily_charge_current = targets.get("daily_charge_current", current_daily_charge_current)
            target_soc = targets.get("target_soc", current_target_soc)
            return daily_charge_current, target_soc
    except Exception as e:
        print(f"Failed to load targets.json: {e}, using previous target_soc={current_target_soc}, daily_charge_current={current_daily_charge_current}")
        return current_daily_charge_current, current_target_soc

def determine_output_priority(current_hour, current_minute, battery_soc, target_soc, last_output_priority):
    # デフォルトは SBU
    desired_priority = OutputPriority.SBU

    # 23:01 〜 6:59 の時間帯での処理
    is_cheap_time = (
        (current_hour == 23 and current_minute >= TIME_MARGIN_MINUTES) or
        (current_hour < 6) or
        (current_hour == 6 and current_minute < (60 - TIME_MARGIN_MINUTES))
    )

    # 6:59 〜 23:01 の時間帯では常に SBU
    is_sbu_fixed_time = (
        (current_hour == 6 and current_minute >= (60 - TIME_MARGIN_MINUTES)) or
        (6 < current_hour < 23) or
        (current_hour == 23 and current_minute < TIME_MARGIN_MINUTES)
    )

    if is_cheap_time:
        if last_output_priority == OutputPriority.UTI:
            if battery_soc > target_soc + HYSTERESIS_SOC:
                desired_priority = OutputPriority.SBU
                print(f"Switching to SBU: battery_soc ({battery_soc}) > target_soc ({target_soc}) + hysteresis ({HYSTERESIS_SOC})")
            else:
                desired_priority = OutputPriority.UTI
        else:
            if battery_soc <= target_soc:
                desired_priority = OutputPriority.UTI
                print(f"Switching to UTI: battery_soc ({battery_soc}) <= target_soc ({target_soc})")
            else:
                desired_priority = OutputPriority.SBU
    elif is_sbu_fixed_time:
        desired_priority = OutputPriority.SBU

    return desired_priority

def main():
    last_charge_current = 0
    daily_charge_current = 0
    target_soc = 90
    last_output_priority = None
    last_battery_soc = 50

    print("Starting charge controller...")
    while True:
        daily_charge_current, target_soc = load_targets_from_file(daily_charge_current, target_soc)

        limited_data = fetch_registers()
        if limited_data:
            battery_soc = int(limited_data["0"])
            last_battery_soc = battery_soc
            load_power = int(limited_data["44"]) + int(limited_data["68"])
            battery_voltage = int(limited_data["1"]) / 10.0
        else:
            battery_soc = last_battery_soc
            load_power = 0
            battery_voltage = 53

        current_hour = time.localtime().tm_hour
        current_minute = time.localtime().tm_min
        desired_priority = determine_output_priority(
            current_hour, current_minute, battery_soc, target_soc, last_output_priority
        )

        if last_output_priority != desired_priority:
            set_output_priority(desired_priority)
            last_output_priority = desired_priority

        if limited_data:
            target_charge_current = adjust_battery_charge(
                battery_soc, load_power, battery_voltage, daily_charge_current, target_soc
            )
            if last_charge_current != target_charge_current:
                set_charge_current(target_charge_current)
                last_charge_current = target_charge_current

        time.sleep(5)

if __name__ == "__main__":
    main()
