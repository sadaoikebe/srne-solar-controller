import time
import math
import requests
from datetime import datetime, timedelta
import argparse

# デバッグフラグをコマンドライン引数で制御
parser = argparse.ArgumentParser(description="Battery Controller Script")
parser.add_argument("--debug", action="store_true", help="Enable debug output")
args = parser.parse_args()
DEBUG = args.debug

def debug_print(*args, **kwargs):
    """デバッグ出力（DEBUGがTrueのときのみ）"""
    if DEBUG:
        print(*args, **kwargs)

# APIエンドポイント
LIMITED_REGISTERS_URL = "http://localhost:5004/limited_registers"
SET_CHARGE_CURRENT_URL = "http://localhost:5004/set_charge_current"
WEATHER_API_URL = "https://www.jma.go.jp/bosai/forecast/data/forecast/280000.json"

def fetch_registers():
    """APIから限定レジスタデータを取得"""
    debug_print("Fetching registers from API...")
    try:
        response = requests.get(LIMITED_REGISTERS_URL)
        response.raise_for_status()
        data = response.json()
        debug_print(f"Fetched registers: {data}")
        return data
    except requests.RequestException as e:
        print(f"Error fetching registers: {e}")
        debug_print(f"Fetch failed with error: {e}")
        return None

def set_charge_current(current):
    """API経由で充電電流を書き込む"""
    debug_print(f"Setting charge current to {current}...")
    try:
        response = requests.post(SET_CHARGE_CURRENT_URL, json={"current": current})
        response.raise_for_status()
        debug_print(f"Set charge current: {response.json()['message']}")
        return True
    except requests.RequestException as e:
        print(f"Error setting charge current: {e}")
        debug_print(f"Set charge current failed with error: {e}")
        return False

def calculate_required_current(battery_soc, target_soc):
    """必要な平均充電電流を計算（調整済みパラメータ）"""
    debug_print(f"Calculating required current: battery_soc={battery_soc}, target_soc={target_soc}")
    
    soc_diff = target_soc - battery_soc
    if soc_diff <= 0:
        debug_print("SOC difference <= 0, no charging needed")
        return 0
    
    required_energy_wh = soc_diff * 255  # SOC 1あたり255Wh
    charging_hours = 4.5  # 4.5時間
    required_power_w = required_energy_wh / charging_hours
    battery_voltage = 53  # 予測用に53V固定
    required_current = required_power_w / battery_voltage
    rounded_current = math.ceil(required_current / 5) * 5
    debug_print(f"Required energy: {required_energy_wh} Wh, power: {required_power_w:.2f} W, current: {required_current:.2f} A, rounded: {rounded_current} A")
    return rounded_current

def calculate_grid_limit_current(load_power, battery_voltage):
    """グリッド最大電力制約に基づく最大充電電流を計算"""
    debug_print(f"Calculating grid limit current: load_power={load_power}, battery_voltage={battery_voltage}")
    GRID_MAX_POWER = 9200
    grid_max_draw = GRID_MAX_POWER - load_power
    debug_print(f"grid_max_draw: {grid_max_draw} W")
    
    if 30 < battery_voltage < 70:
        grid_limit_current = math.floor((grid_max_draw / battery_voltage) / 5) * 5
    else:
        grid_limit_current = 0
    debug_print(f"grid_limit_current: {grid_limit_current} A")
    return grid_limit_current

def adjust_battery_charge(data, daily_charge_current, last_charge_current):
    """バッテリー充電電流を調整"""
    debug_print("Adjusting battery charge...")
    battery_soc = int(data["0"])
    load_power = int(data["44"]) + int(data["68"])
    battery_voltage = int(data["1"]) / 10.0
    debug_print(f"battery_soc: {battery_soc}, load_power: {load_power}, voltage: {battery_voltage}")

    grid_limit_current = calculate_grid_limit_current(load_power, battery_voltage)
    current_hour = datetime.now().hour
    debug_print(f"current_hour: {current_hour}")

    target_charge_current = daily_charge_current
    debug_print(f"Initial target_charge_current: {target_charge_current}")

    # SOCに基づくバッテリー保護制約
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
    debug_print(f"After SOC protection: {target_charge_current}")

    # 時間帯による制限
    if 7 <= current_hour <= 22:
        target_charge_current = min(5, target_charge_current)
    if 1 <= current_hour <= 6:
        target_charge_current = min(100, target_charge_current)
    debug_print(f"After hour check: {target_charge_current}")

    # グリッド最大電力制約
    target_charge_current = min(grid_limit_current, target_charge_current)
    debug_print(f"Final target_charge_current: {target_charge_current}")

    if last_charge_current != target_charge_current:
        debug_print(f"Charge current changed from {last_charge_current} to {target_charge_current}")
        set_charge_current(target_charge_current)
    else:
        debug_print("No change in charge current")
    
    return target_charge_current

def daily_process(battery_soc):
    """1日ごとの処理：天気からtarget_socと充電電流を決定"""
    debug_print("Running daily process...")
    try:
        response = requests.get(WEATHER_API_URL)
        response.raise_for_status()
        weather_data = response.json()

        southern_data = [item for item in weather_data if any(area['area']['name'] == '南部' for area in item['timeSeries'][0]['areas'])]
        weather_code = int(southern_data[0]['timeSeries'][0]['areas'][0]['weatherCodes'][1])
        debug_print(f"Tomorrow's weather code: {weather_code}")

        if 100 <= weather_code <= 199:
            target_soc = 80  # 晴れ
            debug_print("Sunny weather, target_soc=80")
        elif 200 <= weather_code <= 299:
            target_soc = 90  # 曇り
            debug_print("Cloudy weather, target_soc=90")
        else:
            target_soc = 101  # 悪天候
            debug_print("Bad weather, target_soc=101")
    except requests.RequestException as e:
        print(f"Weather API request failed: {e}, defaulting to target_soc 90")
        debug_print(f"Weather fetch failed, using default target_soc=90")
        target_soc = 90  # エラー時

    daily_charge_current = calculate_required_current(battery_soc, target_soc)
    debug_print(f"Daily process complete: target_soc={target_soc}, daily_charge_current={daily_charge_current}")
    return target_soc, daily_charge_current

def wait_until(target_time):
    """指定時刻まで待機"""
    now = datetime.now()
    delay = (target_time - now).total_seconds()
    debug_print(f"Waiting until {target_time}, delay: {delay:.2f} seconds")
    if delay > 0:
        time.sleep(delay)
    debug_print(f"Wait complete, now: {datetime.now()}")

# 初期化
target_soc = 90  # 初期値
daily_charge_current = 0
last_charge_current = 0

# タイミング調整
now = datetime.now()
first_five_sec_start = now.replace(microsecond=0) + timedelta(seconds=(5 - (now.second % 5)) % 5)
next_daily_process = now.replace(hour=22, minute=59, second=0, microsecond=0)
if next_daily_process <= now:
    next_daily_process += timedelta(days=1)

debug_print(f"Initial setup: first_five_sec_start={first_five_sec_start}, next_daily_process={next_daily_process}")
print("Starting battery controller...")
while True:
    debug_print(f"Starting new 1-minute cycle at {datetime.now()}")
    for i in range(12):  # 1分間（5秒×12）
        five_sec_start = first_five_sec_start + timedelta(seconds=i * 5)
        debug_print(f"Processing 5-sec interval {i+1}/12, scheduled at {five_sec_start}")
        wait_until(five_sec_start)
        
        # 5秒ごとの処理
        debug_print("Starting 5-second process...")
        limited_data = fetch_registers()
        if limited_data:
            debug_print("Registers fetched successfully, adjusting charge...")
            last_charge_current = adjust_battery_charge(limited_data, daily_charge_current, last_charge_current)
        else:
            debug_print("No data fetched, skipping charge adjustment")
        
        # 1日ごとの処理（22:59）
        now = datetime.now()
        debug_print(f"Checking daily process: now={now}, next_daily_process={next_daily_process}")
        if now >= next_daily_process and limited_data:
            debug_print("Executing daily process...")
            target_soc, daily_charge_current = daily_process(int(limited_data["0"]))
            next_daily_process += timedelta(days=1)
            debug_print(f"Next daily process scheduled for {next_daily_process}")
        else:
            debug_print("Daily process not due yet")
    
    first_five_sec_start += timedelta(minutes=1)
    debug_print(f"Completed 1-minute cycle, next cycle starts at {first_five_sec_start}")
