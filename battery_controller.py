import time
import math
import requests
import json
from enum import IntEnum
from enum import Enum
from datetime import datetime
import os

TIME_MARGIN_MINUTES = 1
HYSTERESIS_SOC = 2
CUTOFF_SOC = 9
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/targets.json")

LIMITED_REGISTERS_URL = "http://modbus_api:5004/limited_registers"
SET_CHARGE_CURRENT_URL = "http://modbus_api:5004/set_charge_current"

class OutputPriority(IntEnum):
    SOL = 0
    UTI = 1
    SBU = 2

class State(Enum):
    UTI_CHARGING = "UTI_CHARGING"
    UTI_STOPPED  = "UTI_STOPPED"
    SBU          = "SBU"

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
            return True
        else:
            print(f"Error setting charge current: {result.get('message')}")
            return False
    except requests.RequestException as e:
        print(f"Error setting charge current: {e}")
        return False

def set_output_priority(priority):
    try:
        response = requests.post("http://modbus_api:5004/set_output_priority", json={"value": priority})
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
    GRID_MAX_POWER = 9000
    grid_max_draw = GRID_MAX_POWER - load_power
    if 30.0 < battery_voltage < 70.0:
        return math.floor((grid_max_draw / battery_voltage) / 5.0) * 5.0
    return 0

TIME_PERIODS = [
    {
        "name": "cheap",
        "start": "23:01",
        "end": "6:58"
    },
    {
        "name": "sbu_fixed",
        "start": "6:59",
        "end": "23:00"
    }
]

def str_to_time(time_str):
    """文字列をdatetime.timeに変換"""
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except ValueError:
        raise ValueError("Invalid time format. Use HH:MM (e.g., '6:59')")

def is_time_in_period(current_time, start_time, end_time):
    """現在時刻が指定時間帯にあるか判定"""
    if start_time <= end_time:
        return start_time <= current_time <= end_time
    else:  # 深夜を跨ぐ場合（例: 23:01〜6:59）
        return current_time >= start_time or current_time <= end_time

def get_time_period():
    """
    時間帯を判定する。
    Returns:
        str: 時間帯の名前（例: "sbu_fixed", "cheap"）または "unknown"
    """
    current_time = datetime.now().time()
    for period in TIME_PERIODS:
        start = str_to_time(period["start"])
        end   = str_to_time(period["end"])
        if is_time_in_period(current_time, start, end):
            return period["name"]
    return "unknown"

def update_targets_json(daily_charge_current, target_soc):
    """targets.json を更新"""
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump({"target_soc": target_soc, "daily_charge_current": daily_charge_current}, f)
        print(f"Wrote targets to {CONFIG_PATH}: target_soc={target_soc}, daily_charge_current={daily_charge_current}")
    except Exception as e:
        print(f"Failed to write targets.json: {e}")

def determine_next_state(current_state, estimated_soc, target_soc, battery_voltage, time_period, daily_charge_current):
    """
    次の状態を決定する。
    Args:
        current_state: 現在の状態 (State 型)
        estimated_soc: 推測された SoC (小数点以下含む)、None の場合は状態を維持
        target_soc: 目標 SoC
        battery_voltage: バッテリー電圧
        time_period: 現在の時間帯 ("sbu_fixed" または "cheap")
        daily_charge_current: 1 日の充電電流
    Returns:
        (次の状態, new_daily_charge_current)
    """
    # estimated_soc が取得できていない場合は現在の状態を維持する
    if estimated_soc is None:
        return current_state, daily_charge_current

    next_state = current_state
    lower_charge_current = False
    new_daily_charge_current = daily_charge_current

    if time_period == "sbu_fixed":
        if current_state == State.UTI_CHARGING:
            if battery_voltage > 51.6 and estimated_soc > CUTOFF_SOC:
                next_state = State.SBU
            elif battery_voltage > 50.6:
                next_state = State.UTI_STOPPED
        elif current_state == State.UTI_STOPPED:
            if battery_voltage > 51.6 and estimated_soc > CUTOFF_SOC:
                next_state = State.SBU
            elif battery_voltage < 49.4:
                next_state = State.UTI_CHARGING
        else:  # SBU
            if battery_voltage < 49.4:
                next_state = State.UTI_CHARGING
            elif battery_voltage < 49.6 or estimated_soc <= CUTOFF_SOC:
                next_state = State.UTI_STOPPED

    elif time_period == "uti_fixed":
        # UTI 固定時間帯では常に UTI
        next_state = State.UTI_STOPPED

    else:
        # Cheap 時間帯での状態遷移（estimated_soc を使用）
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
        elif current_state == State.SBU:
            if estimated_soc < target_soc - 0.4:
                next_state = State.UTI_CHARGING
            elif estimated_soc < target_soc + 0.4:
                next_state = State.UTI_STOPPED

    if lower_charge_current:
        new_daily_charge_current = min(10, daily_charge_current)
        if new_daily_charge_current != daily_charge_current:
            update_targets_json(new_daily_charge_current, target_soc)
            print(f"Updated daily_charge_current to {new_daily_charge_current}A")

    return next_state, new_daily_charge_current

def adjust_battery_charge(battery_soc, load_power, battery_voltage, daily_charge_current, state):
    """
    状態に応じて充電電流を調整する。
    Args:
        battery_soc: 現在のバッテリー SoC
        load_power: 負荷電力
        battery_voltage: バッテリー電圧
        daily_charge_current: 1 日の充電電流（targets.json から）
        state: 現在の状態 (State 型)
    Returns:
        充電電流 (A)
    """
    if state == State.SBU:
        return 0  # SBU では充電しない
    if state == State.UTI_STOPPED:
        return 0  # UTI(充電停止) では充電しない

    # UTI(充電中) の場合、通常の充電ロジック
    grid_limit_current = calculate_grid_limit_current(load_power, battery_voltage)
    target_charge_current = daily_charge_current

    soc_charge_limits = [
        (60,  120),  # SOC < 60: 120A
        (70,  105),  # 60 <= SOC < 70: 105A
        (80,   90),  # 70 <= SOC < 80: 90A
        (85,   80),  # 80 <= SOC < 85: 80A
        (90,   70),
        (93,   60),
        (96,   50),
        (98,   40),
        (99,   30),
        (100,  20),
    ]

    for soc_threshold, limit in soc_charge_limits:
        if battery_soc < soc_threshold:
            target_charge_current = min(limit, target_charge_current)
            break
    else:
        target_charge_current = min(10, target_charge_current)  # SOC >= 100: 10A

    voltage_charge_limits = [
        (55.2, 120),
        (55.6,  80),
        (55.8,  60),
        (56.0,  40),
        (56.3,  30),
        (56.5,  24),
        (56.6,  18),
        (56.7,  14),
        (56.8,  10),
        (56.9,   7),
    ]

    for volt_threshold, limit in voltage_charge_limits:
        if battery_voltage < volt_threshold:
            target_charge_current = min(limit, target_charge_current)
            break
    else:
        target_charge_current = min(2, target_charge_current)

    target_charge_current = min(grid_limit_current, target_charge_current)
    return target_charge_current

def determine_output_priority(state):
    """
    状態に応じて出力優先度を決定する。
    Args:
        state: 現在の状態 (State 型)
    Returns:
        出力優先度 (OutputPriority)
    """
    if state == State.SBU:
        return OutputPriority.SBU
    return OutputPriority.UTI

def load_targets_from_file(current_daily_charge_current, current_target_soc):
    try:
        with open(CONFIG_PATH, "r") as f:
            targets = json.load(f)
            daily_charge_current = targets.get("daily_charge_current", current_daily_charge_current)
            target_soc = targets.get("target_soc", current_target_soc)
            return daily_charge_current, target_soc
    except Exception as e:
        print(f"Failed to load targets.json: {e}, "
              f"using previous target_soc={current_target_soc}, "
              f"daily_charge_current={current_daily_charge_current}")
        return current_daily_charge_current, current_target_soc

def main():
    last_charge_current  = 0
    daily_charge_current = 0
    target_soc           = 90
    last_output_priority = None
    battery_soc          = None
    estimated_soc        = None
    current_state        = State.SBU  # 初期状態
    battery_voltage      = 52.0

    print("Starting charge controller...")
    while True:
        daily_charge_current, target_soc = load_targets_from_file(daily_charge_current, target_soc)

        limited_data = fetch_registers()

        if limited_data:
            last_battery_soc = battery_soc

            # ── Register key names use hex addresses matching modbus_api v2 schema ──
            # 0x0100 = battery SOC (%)
            # 0x0101 = battery voltage (raw ×0.1 V)
            # 0x0102 = battery current (raw ×0.1 A, 16-bit two's complement)
            # 0x021C = load apparent power L1 (W)
            # 0x0234 = load apparent power L2 (W)
            battery_soc     = int(limited_data["0x0100"])
            raw_current     = int(limited_data["0x0102"])
            battery_current = (65536 - raw_current) / 10 if raw_current > 32767 else -raw_current / 10
            load_power      = int(limited_data["0x021C"]) + int(limited_data["0x0234"])
            battery_voltage = int(limited_data["0x0101"]) / 10.0

            if estimated_soc is None or (last_battery_soc is not None and abs(battery_soc - last_battery_soc) >= 2):
                estimated_soc = float(battery_soc)
            else:
                # SoC が変化した場合
                if last_battery_soc is not None:
                    if battery_soc == last_battery_soc - 1:
                        estimated_soc = battery_soc + 0.49
                    elif battery_soc == last_battery_soc + 1:
                        estimated_soc = battery_soc - 0.49

                # SoC が変化しない場合、充電電流から推測
                if last_battery_soc is not None and last_battery_soc == battery_soc and battery_current != 0:
                    delta_soc = battery_current / 3744  # 520Ah * 3600s / 5s / 100%
                    estimated_soc += delta_soc

                    # 上限下限の制限 (battery_soc ± 0.49)
                    estimated_soc = max(battery_soc - 0.5, min(battery_soc + 0.5, estimated_soc))

        else:
            last_battery_soc = battery_soc
            load_power       = 0
            battery_voltage  = 53.0

        # 時間帯を取得
        time_period = get_time_period()

        # 状態遷移
        current_state, daily_charge_current = determine_next_state(
            current_state, estimated_soc, target_soc, battery_voltage, time_period, daily_charge_current
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

        time.sleep(5)

if __name__ == "__main__":
    main()
