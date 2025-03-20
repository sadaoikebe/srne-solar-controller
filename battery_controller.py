import time
import math
import requests
import json

LIMITED_REGISTERS_URL = "http://localhost:5004/limited_registers"
SET_CHARGE_CURRENT_URL = "http://localhost:5004/set_charge_current"

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

def calculate_grid_limit_current(load_power, battery_voltage):
    GRID_MAX_POWER = 9200
    grid_max_draw = GRID_MAX_POWER - load_power
    if 30 < battery_voltage < 70:
        return math.floor((grid_max_draw / battery_voltage) / 5) * 5
    return 0

def adjust_battery_charge(data, daily_charge_current, target_soc):
    battery_soc = int(data["0"])
    load_power = int(data["44"]) + int(data["68"])
    battery_voltage = int(data["1"]) / 10.0
    current_hour = time.localtime().tm_hour

    if battery_soc >= target_soc:
        return 0

    grid_limit_current = calculate_grid_limit_current(load_power, battery_voltage)
    target_charge_current = daily_charge_current

    if battery_soc < 60:
        target_charge_current = min(120, target_charge_current)
    elif 60 <= battery_soc < 70:
        target_charge_current = min(105, target_charge_current)
    elif 70 <= battery_soc < 80:
        target_charge_current = min(90, target_charge_current)
    elif 80 <= battery_soc < 90:
        target_charge_current = min(75, target_charge_current)
    elif 90 <= battery_soc < 96:
        target_charge_current = min(55, target_charge_current)
    elif 96 <= battery_soc < 99:
        target_charge_current = min(40, target_charge_current)
    elif 99 <= battery_soc < 100:
        target_charge_current = min(25, target_charge_current)
    else:
        target_charge_current = min(10, target_charge_current)

    if target_soc - 1 == battery_soc:
        target_charge_current = min(20, target_charge_current)

    if 7 <= current_hour <= 22:
        target_charge_current = min(5, target_charge_current)

    target_charge_current = min(grid_limit_current, target_charge_current)
    return target_charge_current

def main():
    last_charge_current = 0
    daily_charge_current = 0  # 初期値
    target_soc = 90  # 初期値

    print("Starting charge controller...")
    while True:
        # targets.json から読み込み
        try:
            with open("/opt/modbus_api/targets.json", "r") as f:
                targets = json.load(f)
                daily_charge_current = targets.get("daily_charge_current", daily_charge_current)
                target_soc = targets.get("target_soc", target_soc)  # target_soc を読み込み
                print(f"Loaded targets: target_soc={target_soc}, daily_charge_current={daily_charge_current}")
        except Exception as e:
            print(f"Failed to load targets.json: {e}, using previous target_soc={target_soc}, daily_charge_current={daily_charge_current}")

        limited_data = fetch_registers()
        if limited_data:
            target_charge_current = adjust_battery_charge(limited_data, daily_charge_current, target_soc)  # target_soc を渡す
            if last_charge_current != target_charge_current:
                set_charge_current(target_charge_current)
                last_charge_current = target_charge_current

        time.sleep(5)  # 単純な5秒ウェイトループ

if __name__ == "__main__":
    main()