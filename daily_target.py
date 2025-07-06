import requests
import json
import argparse
from datetime import datetime, timedelta
import math

WEATHER_API_URL = "https://www.jma.go.jp/bosai/forecast/data/forecast/280000.json"
LIMITED_REGISTERS_URL = "http://localhost:5004/limited_registers"

def fetch_registers():
    try:
        response = requests.get(LIMITED_REGISTERS_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching registers: {e}")
        return None

def fetch_tomorrow_weather_code():
    """JMAから明日の天気コードを取得する"""
    try:
        response = requests.get(WEATHER_API_URL)
        response.raise_for_status()
        weather_data = response.json()

        southern_data = [item for item in weather_data if any(area['area']['name'] == '南部' for area in item['timeSeries'][0]['areas'])]
        weather_code = int(southern_data[0]['timeSeries'][0]['areas'][0]['weatherCodes'][1])
        print(f"Tomorrow's weather code: {weather_code}")
        return weather_code
    except requests.RequestException as e:
        print(f"Weather API request failed: {e}, defaulting to weather code 200")
        return 200

# TARGET_SOC_TABLE_SPRING = [
#     (lambda wc: wc == 100, 50),  # 快晴
#     (lambda wc: 101 <= wc < 200, 55),  # 晴れ
#     (lambda wc: wc in {200, 201, 210, 211}, 60),  # 曇時々晴
#     (lambda wc: 200 <= wc < 300, 70),  # 曇り
#     (lambda wc: wc in {300, 301, 311, 313}, 90),  # 雨
# ]

TARGET_SOC_TABLE = [
    (lambda wc: wc == 100, 55),  # 快晴
    (lambda wc: 101 <= wc < 200, 60),  # 晴れ
    (lambda wc: wc in {200, 201, 210, 211}, 65),  # 曇時々晴
    (lambda wc: 200 <= wc < 300, 70),  # 曇り
    (lambda wc: wc in {300, 301, 311, 313}, 90),  # 雨
]

def determine_target_soc_from_weather(weather_code):
    target_soc = 101 # 悪天候
    for condition, soc in TARGET_SOC_TABLE:
        if condition(weather_code):
            target_soc = soc
            break

    print(f"Weather code={weather_code}, Target SOC={target_soc}")
    return target_soc

def calculate_required_current(battery_soc, target_soc, charging_hours):
    """必要な充電電流を計算"""
    soc_diff = target_soc - battery_soc
    if soc_diff <= 0:
        print("SOC difference <= 0, no charging needed")
        return 0
    
    required_energy_wh = soc_diff * 270  # SOC 1あたり
    required_power_w = required_energy_wh / charging_hours
    battery_voltage = 53  # 予測用に53V固定
    required_current = required_power_w / battery_voltage
    # rounded_current = math.ceil(required_current / 5) * 5  # 切り上げて5の倍数に
    rounded_current = math.ceil(required_current)
    rounded_current = max(rounded_current, 10)
    print(f"Required current: {required_current:.2f} A, rounded up to: {rounded_current} A")
    return rounded_current

def estimate_soc_at_2259(current_soc):
    """22:59までの平均1kW消費で推定SOCを計算"""
    now = datetime.now()
    target_time = now.replace(hour=22, minute=59, second=0, microsecond=0)
    if target_time <= now:
        target_time += timedelta(days=1)
    
    hours_until_2259 = (target_time - now).total_seconds() / 3600
    energy_consumed_wh = 1000 * hours_until_2259  # 1kW = 1000W
    soc_decrease = energy_consumed_wh / 255  # SOC 1あたり255Wh
    estimated_soc = max(0, current_soc - soc_decrease)
    print(f"Estimated SOC at 22:59: {estimated_soc:.2f} (current SOC: {current_soc}, hours until 22:59: {hours_until_2259:.2f})")
    return estimated_soc


def calculate_charging_hours(until_time_str=None):
    """
    指定された時間（またはデフォルトで翌朝 5:30）までの充電時間を計算する。
    Args:
        until_time_str: "HH:MM" 形式の時間（例："05:30"）。未指定なら翌朝 5:30。
    Returns:
        充電時間 (時間、float)
    """
    now = datetime.now()
    if until_time_str:
        try:
            hour, minute = map(int, until_time_str.split(":"))
            until_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if until_time <= now:
                until_time += timedelta(days=1)
        except ValueError:
            raise ValueError("Invalid time format. Use HH:MM (e.g., 05:30).")
    else:
        until_time = (now + timedelta(days=1)).replace(hour=5, minute=30, second=0, microsecond=0)
        if now.hour < 5 or (now.hour == 5 and now.minute < 30):
            until_time = now.replace(hour=5, minute=30, second=0, microsecond=0)

    time_diff = (until_time - now).total_seconds() / 3600
    return max(time_diff, 0)

def parse_args():
    parser = argparse.ArgumentParser(description="Daily Target Calculation Script")
    parser.add_argument("--estimate-start-soc", action="store_true", help="Estimate SOC at 22:59 based on 1kW average consumption")
    parser.add_argument("--start-soc", type=int, help="Use this SOC value instead of fetching from registers")
    parser.add_argument("--target-soc", type=int, help="Use this target SOC instead of weather-based calculation")
    parser.add_argument("--charging-hours", type=float, help="Charging hours to use instead of calculating until a specific time")
    parser.add_argument("--weather-code", type=int, help="Weather code to use instead of fetching from JMA")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode without writing to targets.json")
    parser.add_argument("--until-time", help="Time to charge until in HH:MM format (e.g., 05:30). Default is 05:30 next morning.")
    return parser.parse_args()

def main():
    args = parse_args()

    # battery_soc の取得
    if args.start_soc is not None:
        battery_soc = args.start_soc
        print(f"Using specified start SOC: {battery_soc}")
    else:
        limited_data = fetch_registers()
        if not limited_data:
            print("Failed to fetch registers, exiting.")
            return
        battery_soc = int(limited_data["0"])
        print(f"Fetched current SOC: {battery_soc}")

    # --estimate-start-soc が指定されている場合
    if args.estimate_start_soc and args.start_soc is None:
        battery_soc = estimate_soc_at_2259(battery_soc)

    # target_soc の決定
    if args.target_soc is not None:
        target_soc = args.target_soc
        print(f"Using specified target SOC: {target_soc}")
    else:
        weather_code = args.weather_code
        if weather_code is None:
            weather_code = fetch_tomorrow_weather_code()
        target_soc = determine_target_soc_from_weather(weather_code)

    charging_hours = args.charging_hours
    if charging_hours is not None and args.until_time is not None:
        print("Error: Cannot specify both --charging-hours and --until-time.")
        return

    # 充電時間の決定
    if charging_hours is None:
        charging_hours = calculate_charging_hours(args.until_time)
        print(f"Calculated charging hours: {charging_hours:.2f} hours")

    # daily_charge_current の計算
    daily_charge_current = calculate_required_current(battery_soc, target_soc, charging_hours)
    print(f"Calculated daily_charge_current: {daily_charge_current} (charging hours: {charging_hours})")

    # targets.json に書き込み
    if not args.dry_run: 
        try:
            with open("/opt/modbus_api/targets.json", "w") as f:
                json.dump({"target_soc": target_soc, "daily_charge_current": daily_charge_current}, f)
            print(f"Wrote targets to /opt/modbus_api/targets.json: target_soc={target_soc}, daily_charge_current={daily_charge_current}")
        except Exception as e:
            print(f"Failed to write targets.json: {e}")

if __name__ == "__main__":
    print(f"Starting daily target calculation at {datetime.now()}")
    main()