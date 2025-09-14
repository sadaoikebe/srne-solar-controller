# SRNE Hybrid Inverter Modbus monitor and charge controller

This repository contains scripts to manage a Modbus-based inverter and battery system using a FastAPI server and a controller script.

Instruments(example):

- PowMr SunSmart-10KP in split-phase mode
- Growatt SPF6000ES Plus
- Raspberry Pi with Ubuntu
- 12.47kW-peak Solar Panels
- 2 x JK-BMS based battery box (XR-07) with 32 x 300Ah LFP batteries

## Setup Instructions

```bash
git clone git@github.com:sadaoikebe/srne-solar-controller.git
cd srne-solar-controller
docker compose up -d
```

## Script Descriptions

- **`modbus_api.py`**: Runs a FastAPI server to read Modbus registers, and write charge current to register 0xe205.
  - Endpoint: `/registers`, `/limited_registers`, `/set_charge_current`.

- **`db_writer.py`**: Writes Modbus registers to an InfluxDB database every minute.

- **`daily_target.py`**: Calculates `target_soc` and `daily_charge_current` daily at 22:59 using JMA weather data, writes to `targets.json`. Supports options like `--start-soc`, `--target-soc`, `--charging-hours`.

- **`battery_controller.py`**: Adjusts battery charge current every 5 seconds based on `targets.json`. Stops charging when SOC reaches `target_soc`.

