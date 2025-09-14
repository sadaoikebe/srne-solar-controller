import time
import requests
import json
import os
from datetime import datetime, timedelta, timezone
from influxdb import InfluxDBClient

# APIエンドポイント
API_URL = "http://modbus_api:5004/registers"

client = InfluxDBClient(host='influxdb', port=8086)
client.switch_database('mysolardb')

def fetch_registers():
    """APIからレジスタデータを取得"""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # HTTPエラー時に例外を投げる
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching registers: {e}")
        return None

def write_to_db(data):
    """指定されたレジスタをDBに書き込む"""
    if data is None:
        print("No data to write to DB")
        return
    
    # data[2]を2の補数変換
    if int(data["2"]) > 32767:
        data["2"] = int(data["2"]) - 65536
    
    utc_now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    json_body = [
        {
            "measurement": "registers",
            "tags": {"host": "host1"},
            "time": utc_now,
            "fields": {key: int(value) for key, value in data.items()}
        }
    ]
    try:
        client.write_points(json_body)
        print(f"Successfully wrote data to DB at {utc_now}")
    except Exception as e:
        print(f"Error writing to DB: {e}")

def wait_until_next_minute():
    """次の1分開始点まで待機"""
    now = datetime.now()
    next_minute_start = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    delay = (next_minute_start - now).total_seconds()
    if delay > 0:
        time.sleep(delay)
    return next_minute_start

# メインループ
print("Starting DB writer script...")
next_minute_start = wait_until_next_minute()
while True:
    print(f"Processing at {datetime.now()}")
    
    # APIからデータ取得
    register_data = fetch_registers()
    
    # DBに書き込み
    write_to_db(register_data)
    
    # 次の1分まで待機
    next_minute_start = wait_until_next_minute()