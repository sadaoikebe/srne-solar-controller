# Modbus Control

This repository contains scripts to manage a Modbus-based battery system using a FastAPI server and a controller script.

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

```ini
[Unit]
Description=Modbus Register API Service
After=network.target

[Service]
ExecStart=/opt/modbus_api/venv/bin/python3 /opt/modbus_api/modbus_api.py
WorkingDirectory=/opt/modbus_api
Restart=always
User=your_username
Environment="PYTHONPATH=/opt/modbus_api"

[Install]
WantedBy=multi-user.target
```

* DB Writer Service:

```bash
sudo nano /etc/systemd/system/db-writer.service
```

```ini
[Unit]
Description=DB Writer Script
After=network.target modbus-api.service

[Service]
ExecStart=/opt/modbus_api/venv/bin/python3 /opt/modbus_api/db_writer.py
WorkingDirectory=/opt/modbus_api
Restart=always
User=your_username
Environment="PYTHONPATH=/opt/modbus_api"

[Install]
WantedBy=multi-user.target
```

* Battery Controller Service:

```bash
sudo nano /etc/systemd/system/battery-controller.service
```

```ini
[Unit]
Description=Battery Controller Service
After=network.target modbus-api.service

[Service]
ExecStart=/opt/modbus_api/venv/bin/python3 /opt/modbus_api/battery_controller.py
WorkingDirectory=/opt/modbus_api
Restart=always
User=your_username
Environment="PYTHONPATH=/opt/modbus_api"

[Install]
WantedBy=multi-user.target
```

* Enable and Start Services

```bash
sudo systemctl daemon-reload
sudo systemctl enable modbus-api.service
sudo systemctl enable battery-controller.service
sudo systemctl start modbus-api.service
sudo systemctl start battery-controller.service
```

## Script Descriptions

* modbus_api.py:

  Runs a FastAPI server to read Modbus registers (0, 1, 2, 7, 8, 9, 14, 15, 34-44, 58, 60, 62, 66, 68, 77, 78, 125-128, 132-139, 140-141, 144, 145) and limited registers (0, 1, 44, 68), and write charge current to register 0xe205.

  - Endpoint: /registers, /limited_registers, /set_charge_current.

* db_writer.py:

  - Writes Modbus registers to an InfluxDB database every minute.

* battery_controller.py:

  - Adjusts battery charge current every 5 seconds and updates target_soc and daily_charge_current daily at 22:59.

  - Uses weather data to set SOC targets (Sunny: 80, Cloudy: 90, Bad: 101, Default: 90).

