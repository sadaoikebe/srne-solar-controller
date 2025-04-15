from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import pymodbus.client as modbusClient
from typing import Dict, List
import uvicorn
from enum import IntEnum
import json

class OutputPriority(IntEnum):
    SOL = 0
    UTI = 1
    SBU = 2

class ChargingPriority(IntEnum):
    CSO = 0
    CUB = 1
    SNU = 2
    OSO = 3

with open("config.json") as f:
    config = json.load(f)
VALID_USERNAME = config["valid_username"]
VALID_PASSWORD = config["valid_password"]

app = FastAPI(title="Modbus Register API", description="API to read/write Modbus registers")
templates = Jinja2Templates(directory="/opt/modbus_api/templates")
security = HTTPBasic()

# Modbusクライアントの初期化
modbus = modbusClient.ModbusSerialClient(port='/dev/ttyUSB0', baudrate=9600, timeout=1)

def connect_modbus():
    """Modbusに接続"""
    if not modbus.connect():
        raise HTTPException(status_code=500, detail="Failed to connect to Modbus device")
    return modbus

def read_modbus_registers(modbus_client, addresses_and_counts: List[tuple]) -> List[int]:
    """指定されたアドレスとカウントでレジスタを読み込む"""
    data = []
    for address, count in addresses_and_counts:
        response = modbus_client.read_holding_registers(address=address, count=count)
        if not response.isError():
            data.extend(response.registers)
        else:
            raise HTTPException(status_code=500, detail=f"Error reading registers at address {hex(address)}")
    return data

@app.get("/registers", response_model=Dict[str, int])
async def get_all_registers():
    """必要な全レジスタを読み込む"""
    modbus_client = connect_modbus()
    try:
        addresses_and_counts = [(0x100, 16), (0x200, 32), (0x220, 32), (0xf000, 32), (0xf020, 32), (0x110, 2)]
        all_data = read_modbus_registers(modbus_client, addresses_and_counts)
        required_indices = [
            0, 1, 2, 7, 8, 9, 14, 15,
            *range(34, 45),
            58, 60, 62, 66, 68, 77, 78,
            *range(125, 129),
            *range(132, 140),
            140, 141,
            144, 145
        ]
        filtered_data = {str(i): all_data[i] for i in required_indices}
        return filtered_data
    finally:
        modbus_client.close()

@app.get("/limited_registers", response_model=Dict[str, int])
async def get_limited_registers():
    """5秒ごと用の限定レジスタ（0, 1, 44, 68）を読み込む"""
    modbus_client = connect_modbus()
    try:
        data = [0] * 69
        response = modbus_client.read_holding_registers(address=0x100, count=2)
        if not response.isError():
            data[0:2] = response.registers[0:2]
        else:
            raise HTTPException(status_code=500, detail="Error reading registers 0-1")
        response = modbus_client.read_holding_registers(address=0x200 + 28, count=1)
        if not response.isError():
            data[44] = response.registers[0]
        else:
            raise HTTPException(status_code=500, detail="Error reading register 44")
        response = modbus_client.read_holding_registers(address=0x220 + 20, count=1)
        if not response.isError():
            data[68] = response.registers[0]
        else:
            raise HTTPException(status_code=500, detail="Error reading register 68")
        limited_indices = [0, 1, 44, 68]
        return {str(i): data[i] for i in limited_indices}
    finally:
        modbus_client.close()


@app.post("/set_charge_current")
async def set_charge_current(request: Request):
    modbus_client = connect_modbus()
    try:
        request_data = await request.json()
        value = request_data.get('value')
        if value is None or not isinstance(value, (int, float)):
            raise HTTPException(status_code=400, detail="Invalid or missing 'value' in request body")
        
        regval = int(value * 10)  # 電流値を10倍して整数化
        response = modbus_client.write_register(0xe205, regval)
        if not response.isError():  # response.isError() が False なら成功
            return {
                'success': True,
                'value': value
            }
        else:
            return {
                'success': False,
                'message': 'Error occurred when setting the value.'
            }
    except Exception as e:
        return {
            'success': False,
            'message': f"Error: {str(e)}"
        }
    finally:
        modbus_client.close()

@app.post("/set_output_priority")
async def set_output_priority(request: Request):
    modbus_client = connect_modbus()
    try:
        request_data = await request.json()
        value = request_data.get('value')
        if value is None or value not in [e.value for e in OutputPriority]:
            raise HTTPException(status_code=400, detail=f"Invalid value for Output Priority. Must be one of {[e.name for e in OutputPriority]}")
        
        response = modbus_client.write_register(0xe204, int(value))
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to set Output Priority")
        return {
            'success': True,
            'value': OutputPriority(int(value)).name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        modbus_client.close()

@app.get("/get_output_priority")
async def get_output_priority():
    modbus_client = connect_modbus()
    try:
        response = modbus_client.read_holding_registers(address=0xe204, count=1)
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to read Output Priority")
        value = response.registers[0]
        if value not in [e.value for e in OutputPriority]:
            raise HTTPException(status_code=500, detail=f"Invalid Output Priority value: {value}")
        return {
            'value': OutputPriority(value).name,
            'raw_value': value
        }
    finally:
        modbus_client.close()

@app.post("/set_charging_priority")
async def set_charging_priority(request: Request):
    modbus_client = connect_modbus()
    try:
        request_data = await request.json()
        value = request_data.get('value')
        if value is None or value not in [e.value for e in ChargingPriority]:
            raise HTTPException(status_code=400, detail=f"Invalid value for Charging Priority. Must be one of {[e.name for e in ChargingPriority]}")
        
        response = modbus_client.write_register(0xe20f, int(value))
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to set Charging Priority")
        return {
            'success': True,
            'value': ChargingPriority(int(value)).name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        modbus_client.close()

@app.get("/get_charging_priority")
async def get_charging_priority():
    modbus_client = connect_modbus()
    try:
        response = modbus_client.read_holding_registers(address=0xe20f, count=1)
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to read Charging Priority")
        value = response.registers[0]
        if value not in [e.value for e in ChargingPriority]:
            raise HTTPException(status_code=500, detail=f"Invalid Charging Priority value: {value}")
        return {
            'value': ChargingPriority(value).name,
            'raw_value': value
        }
    finally:
        modbus_client.close()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != VALID_USERNAME or credentials.password != VALID_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials

@app.get("/set_targets_form", response_class=HTMLResponse)
async def set_targets_form(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    # 現在の targets.json の値を読み込み
    try:
        with open("/opt/modbus_api/targets.json", "r") as f:
            targets = json.load(f)
            target_soc = targets.get("target_soc", 90)  # デフォルト値
            daily_charge_current = targets.get("daily_charge_current", 0)  # デフォルト値
    except Exception as e:
        print(f"Error reading targets.json: {e}")
        target_soc = 90
        daily_charge_current = 0

    return templates.TemplateResponse("set_targets.html", {
        "request": request,
        "target_soc": target_soc,
        "daily_charge_current": daily_charge_current
    })

@app.post("/set_targets", response_class=HTMLResponse)
async def set_targets(
    request: Request,
    target_soc: int = Form(...),
    daily_charge_current: int = Form(...),
    credentials: HTTPBasicCredentials = Depends(verify_credentials)
):
    targets = {"target_soc": target_soc, "daily_charge_current": daily_charge_current}
    try:
        with open("/opt/modbus_api/targets.json", "w") as f:
            json.dump(targets, f)
        return templates.TemplateResponse("set_targets.html", {
            "request": request,
            "message": f"Targets updated: target_soc={target_soc}, daily_charge_current={daily_charge_current}",
            "target_soc": target_soc,
            "daily_charge_current": daily_charge_current
        })
    except Exception as e:
        return templates.TemplateResponse("set_targets.html", {
            "request": request,
            "message": f"Error: {str(e)}",
            "target_soc": target_soc,
            "daily_charge_current": daily_charge_current
        })

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5004, log_level="info")

