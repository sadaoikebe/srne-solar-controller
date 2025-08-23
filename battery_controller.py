import time
import math
import requests
import json
from enum import IntEnum
from enum import Enum
from datetime import datetime

TIME_MARGIN_MINUTES = 1
HYSTERESIS_SOC = 2
CUTOFF_SOC = 9

LIMITED_REGISTERS_URL = "http://localhost:5004/limited_registers"
SET_CHARGE_CURRENT_URL = "http://localhost:5004/set_charge_current"

class OutputPriority(IntEnum):
    SOL = 0
    UTI = 1
    SBU = 2

class State(Enum):
    UTI_CHARGING = "UTI_CHARGING"
    UTI_STOPPED = "UTI_STOPPED"
    SBU = "SBU"

# # InfluxDBクライアントの設定
# client = InfluxDBClient(host='localhost', port=8086)
# client.switch_database('mysolardb')

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
            # print(f"Set charge current successfully: value={result['value']}")
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
        end = str_to_time(period["end"])

        if is_time_in_period(current_time, start, end):
            return period["name"]

    return "unknown"

def update_targets_json(daily_charge_current, target_soc):
    """targets.json を更新"""
    try:
        with open("/opt/modbus_api/targets.json", "w") as f:
            json.dump({"target_soc": target_soc, "daily_charge_current": daily_charge_current}, f)
        print(f"Wrote targets to /opt/modbus_api/targets.json: target_soc={target_soc}, daily_charge_current={daily_charge_current}")
    except Exception as e:
        print(f"Failed to write targets.json: {e}")

def determine_next_state(current_state, estimated_soc, target_soc, battery_voltage, time_period, daily_charge_current):
    """
    次の状態を決定する。
    Args:
        current_state: 現在の状態 (State 型)
        estimated_soc: 推測された SoC (小数点以下含む)
        target_soc: 目標 SoC
        time_period: 現在の時間帯 ("sbu_fixed" または "cheap")
    Returns:
        次の状態 (State 型)
    """

    next_state = current_state
    lower_charge_current = False
    new_daily_charge_current = daily_charge_current

    if time_period == "sbu_fixed":
        if current_state == State.UTI_CHARGING:
            if battery_voltage > 51.4 and estimated_soc > CUTOFF_SOC:
                next_state = State.SBU
            elif battery_voltage > 50.6:
                next_state = State.UTI_STOPPED
        elif current_state == State.UTI_STOPPED:
            if battery_voltage > 51.4 and estimated_soc > CUTOFF_SOC:
                next_state = State.SBU
            elif battery_voltage < 49.6:
                next_state = State.UTI_CHARGING
        else:
            if battery_voltage < 49.6:
                next_state = State.UTI_CHARGING
            elif battery_voltage < 49.9 or estimated_soc <= CUTOFF_SOC:
                next_state = State.UTI_STOPPED

    # UTI 固定時間帯では常に UTI
    elif time_period == "uti_fixed":
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
    # 状態に応じた充電電流
    if state == State.SBU:
        return 0  # SBU では充電しない
    if state == State.UTI_STOPPED:
        return 0  # UTI(充電停止) では充電しない

    # UTI(充電中) の場合、通常の充電ロジック
    grid_limit_current = calculate_grid_limit_current(load_power, battery_voltage)
    target_charge_current = daily_charge_current

    # print(f"daily_charge_current = {daily_charge_current}A")

    # SOC ごとの充電電流制限をテーブル形式で定義
    # soc_charge_limits = [
    #     (60, 120),  # SOC < 60: 120A
    #     (70, 110),  # 60 <= SOC < 70: 110A
    #     (80, 90),  # 70 <= SOC < 80: 100A
    #     (85, 70),   # 80 <= SOC < 85: 90A
    #     (90, 60),   # 85 <= SOC < 90: 80A
    #     (93, 50),   # 90 <= SOC < 93: 70A
    #     (96, 40),   # 93 <= SOC < 96: 60A
    #     (98, 30),   # 96 <= SOC < 98: 50A
    #     (99, 25),   # 98 <= SOC < 99: 40A
    #     (100, 20),  # 99 <= SOC < 100: 25A
    # ]
    soc_charge_limits = [
        (60, 120),  # SOC < 60: 120A
        (70, 110),  # 60 <= SOC < 70: 110A
        (80, 100),  # 70 <= SOC < 80: 100A
        (85, 90),   # 80 <= SOC < 85: 90A
        (90, 80),
        (93, 60),
        (96, 50),
        (98, 40),
        (99, 30),
        (100, 20),
    ]

    # SOC に応じた充電電流制限を適用
    for soc_threshold, limit in soc_charge_limits:
        if battery_soc < soc_threshold:
            target_charge_current = min(limit, target_charge_current)
            break
    else:
        target_charge_current = min(10, target_charge_current)  # SOC >= 100: 10A

    voltage_charge_limits = [
        (54.8, 120),
        (55.1, 80),
        (55.2, 60),
        (55.3, 40),
        (55.4, 30),
        (55.5, 25),
        (55.6, 20),
        (55.7, 12),
        (55.8, 9),
        (55.9, 7),
        (56.0, 5),
        (56.1, 4),
        (56.2, 3),
    ]

    for volt_threshold, limit in voltage_charge_limits:
        if battery_voltage < volt_threshold:
            target_charge_current = min(limit, target_charge_current)
            break
    else:
        target_charge_current = min(2, target_charge_current)

    # print(f"target_charge_current = {target_charge_current}A after battery_soc {battery_soc}%")

    target_charge_current = min(grid_limit_current, target_charge_current)

    # print(f"target_charge_current = {target_charge_current}A after grid_limit_current {grid_limit_current}A")

    return target_charge_current

def determine_output_priority(state):
    """
    状態に応じて出力優先度を決定する。
    Args:
        state: 現在の状態 (State 型)
    Returns:
        出力優先度 ("SBU" または "UTI")
    """
    if state == State.SBU:
        return OutputPriority.SBU
    return OutputPriority.UTI

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

def main():
    last_charge_current = 0
    daily_charge_current = 0
    target_soc = 90
    last_output_priority = None
    battery_soc = None
    estimated_soc = None
    current_state = State.SBU  # 初期状態
    battery_voltage = 52.0

    print("Starting charge controller...")
    while True:
        daily_charge_current, target_soc = load_targets_from_file(daily_charge_current, target_soc)

        limited_data = fetch_registers()

        if limited_data:
            last_battery_soc = battery_soc
            battery_soc = int(limited_data["0"])
            if int(limited_data["2"]) > 32767:
                battery_current = (65536 - int(limited_data["2"])) / 10
            else:
                battery_current = (-int(limited_data["2"])) / 10
            load_power = int(limited_data["44"]) + int(limited_data["68"])
            battery_voltage = int(limited_data["1"]) / 10.0

            if estimated_soc is None or (last_battery_soc is not None and abs(battery_soc - last_battery_soc) >= 2):
                estimated_soc = float(battery_soc)
                # print(f"Reset estimated_soc to {estimated_soc} (initial or change >= 2)")
            else:
                # SoC が変化した場合
                if last_battery_soc is not None:
                    if battery_soc == last_battery_soc - 1:
                        estimated_soc = battery_soc + 0.49
                        # print(f"SoC decreased from {last_battery_soc} to {battery_soc}, estimated_soc = {estimated_soc}")
                    elif battery_soc == last_battery_soc + 1:
                        estimated_soc = battery_soc - 0.49
                        # print(f"SoC increased from {last_battery_soc} to {battery_soc}, estimated_soc = {estimated_soc}")

                # SoC が変化しない場合、充電電流から推測
                if last_battery_soc is not None and last_battery_soc == battery_soc and battery_current != 0:
                    delta_soc = battery_current / 3744  # 520Ah * 3600s / 5s / 100%
                    estimated_soc += delta_soc

                    # 上限下限の制限 (battery_soc ± 0.49)
                    min_estimated = battery_soc - 0.5
                    max_estimated = battery_soc + 0.5
                    estimated_soc = max(min_estimated, min(max_estimated, estimated_soc))

                    # print(f"No SoC change, battery_current={battery_current}A, estimated_soc={estimated_soc}")

        else:
            last_battery_soc = battery_soc
            load_power = 0
            battery_voltage = 53.0

        # 時間帯を取得
        time_period = get_time_period()

        # 状態遷移
        current_state, daily_charge_current = determine_next_state(current_state, estimated_soc, target_soc, battery_voltage, time_period, daily_charge_current)
        
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
