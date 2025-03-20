# SRNE Hybrid Inverter Modbus monitor and charge controller

This repository contains scripts to manage a Modbus-based inverter and battery system using a FastAPI server and a controller script.

Instruments(example):

- PowMr SunSmart-10KP in split-phase mode
- Raspberry Pi with Ubuntu
- 7.92kW-peak Solar Panels
- 2 x JK-BMS based battery box (XR-07) with 32 x 300Ah LFP batteries

## Setup Instructions

### 1. Clone the Repository
```bash
git clone git@github.com:sadaoikebe/srne-solar-controller.git
cd modbus-control
```

### 2. Set Up Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn pymodbus requests influxdb
deactivate
```

### 3. Install Scripts
```bash
sudo mkdir -p /opt/modbus_api
sudo cp *.py /opt/modbus_api/
sudo cp -r venv /opt/modbus_api/
sudo chown -R your_username:your_username /opt/modbus_api
```

### 4. Configure systemd Services

* Modbus API Service:
```bash
sudo nano /etc/systemd/system/modbus-api.service
```

* DB Writer Service:
```bash
sudo nano /etc/systemd/system/db-writer.service
```

* Battery Controller Service:
```bash
sudo nano /etc/systemd/system/battery-controller.service
```

* Daily Target Calculation:
```bash
sudo nano /etc/systemd/system/daily-target.timer
```

* Enable and Start Services
```bash
sudo systemctl daemon-reload
sudo systemctl enable modbus-api.service
sudo systemctl start modbus-api.service
sudo systemctl enable db-writer.service
sudo systemctl start db-writer.service
sudo systemctl enable battery-controller.service
sudo systemctl start battery-controller.service
sudo systemctl enable daily-target.timer
sudo systemctl start daily-target.timer
sudo systemctl status daily-target.timer
```

## Script Descriptions

- **`modbus_api.py`**: Runs a FastAPI server to read Modbus registers, and write charge current to register 0xe205.
  - Endpoint: `/registers`, `/limited_registers`, `/set_charge_current`.

- **`db_writer.py`**: Writes Modbus registers to an InfluxDB database every minute.

- **`daily_target.py`**: Calculates `target_soc` and `daily_charge_current` daily at 22:59 using JMA weather data, writes to `targets.json`. Supports options like `--start-soc`, `--target-soc`, `--charging-hours`.

- **`battery_controller.py`**: Adjusts battery charge current every 5 seconds based on `targets.json`. Stops charging when SOC reaches `target_soc`.

