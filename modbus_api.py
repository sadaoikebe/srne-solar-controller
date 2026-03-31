from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import pymodbus.client as modbusClient
from typing import Dict, List
import uvicorn
from enum import IntEnum
import json
import serial.tools.list_ports
import os, hmac

class OutputPriority(IntEnum):
    SOL = 0
    UTI = 1
    SBU = 2

class ChargingPriority(IntEnum):
    CSO = 0
    CUB = 1
    SNU = 2
    OSO = 3

VALID_USERNAME = os.getenv("BASIC_AUTH_USER")
VALID_PASSWORD = os.getenv("BASIC_AUTH_PASS")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/targets.json")

app = FastAPI(title="Modbus Register API", description="API to read/write Modbus registers")
templates = Jinja2Templates(directory="/app")
security = HTTPBasic()

POWMR_HOLDING_BLOCKS: Tuple[Tuple[int, int], ...] = (
    (0x0100, 32),  # 0x0100-0x011F
    (0x0200, 32),  # 0x0200-0x021F
    (0x0220, 32),  # 0x0220-0x023F
    (0xF000, 32),  # 0xF000-0xF01F
    (0xF020, 32),  # 0xF020-0xF03F
)

GROWATT_INPUT_BLOCKS: Tuple[Tuple[int, int], ...] = (
    (0, 96),       # 0..95
)

POWMR_REQUIRED: Tuple[int, ...] = (
    # battery
    0x0100, 0x0101, 0x0102,
    # PV1/2
    0x0107, 0x0108, 0x0109,
    0x010F, 0x0110, 0x0111,
    # AC V/F & load/grid
    0x0213, 0x022A, 0x0216, 0x022C, 0x0215, 0x0218,
    0x021B, 0x0232, 0x023D, 0x023E,
    # today kWh
    0xF02D, 0xF02E, 0xF02F, 0xF030, 0xF03C, 0xF03D,
    # cumulative (32bitの片側) — 結合は db-writer
    0xF034, 0xF035, 0xF036, 0xF037, 0xF038, 0xF039, 0xF03A, 0xF03B,
)

POWMR_FAST_ADDRS: Tuple[int, ...] = (0x0100, 0x0101, 0x0102, 0x021C, 0x0234)

GROWATT_REQUIRED: Tuple[int, ...] = (
    # PV3/PV4
    1, 2,            # pv3/pv4 voltage
    3, 4, 5, 6,      # pv3/pv4 power (32bit hi/lo)
    7, 8,            # pv3/pv4 current
    # Battery/Load
    10,              # load_growatt
    17,              # battery_voltage_growatt
    83, 84,          # batt currents (+charge / +draw)
    # PV3 daily/cumulative (32bit hi/lo)
    48, 49, 50, 51,
)

def _read_holding_blocks(client, blocks: Iterable[Tuple[int, int]]) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for start, count in blocks:
        rr = client.read_holding_registers(address=start, count=count)
        if hasattr(rr, "isError") and rr.isError():
            raise RuntimeError(f"Holding read failed @0x{start:04X}/n={count}: {rr}")
        regs = getattr(rr, "registers", None)
        if regs is None:
            raise RuntimeError(f"Holding read missing 'registers' @0x{start:04X}/n={count}: {rr}")
        for i, v in enumerate(regs):
            out[start + i] = int(v) & 0xFFFF
    return out

def _read_input_blocks(client, blocks: Iterable[Tuple[int, int]]) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for start, count in blocks:
        rr = client.read_input_registers(address=start, count=count)
        if hasattr(rr, "isError") and rr.isError():
            raise RuntimeError(f"Input read failed @{start}/n={count}: {rr}")
        regs = getattr(rr, "registers", None)
        if regs is None:
            raise RuntimeError(f"Input read missing 'registers' @{start}/n={count}: {rr}")
        for i, v in enumerate(regs):
            out[start + i] = int(v) & 0xFFFF
    return out

def _as_hex_dict(raw: Dict[int, int], whitelist: Iterable[int]) -> Dict[str, int]:
    w = set(whitelist)
    return {f"0x{a:04X}": raw[a] for a in sorted(raw) if a in w}

def _as_dec_dict(raw: Dict[int, int], whitelist: Iterable[int]) -> Dict[str, int]:
    w = set(whitelist)
    return {str(a): raw[a] for a in sorted(raw) if a in w}

def get_modbus_client(vid: int, pid: int) -> modbusClient.ModbusSerialClient | None:
    port = next((p.device for p in serial.tools.list_ports.comports() if p.vid == vid and p.pid == pid), None)
    if not port:
        print(f"No device found with VID={vid}, PID={pid}")
        return None
    print(f"Connecting to {port}")
    return modbusClient.ModbusSerialClient(port=port, baudrate=9600, timeout=1)

# Modbusクライアントの初期化
modbus = get_modbus_client(vid=6790, pid=29987)
modbus2 = get_modbus_client(vid=1250, pid=5137)

def connect_modbus():
    """Modbusに接続"""
    if not modbus.connect():
        raise HTTPException(status_code=500, detail="Failed to connect to Modbus device")
    return modbus

def connect_modbus2():
    if not modbus2.connect():
        raise HTTPException(status_code=500, detail="Failed to connect to Modbus2 device")
    return modbus2

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

def read_modbus_input_registers(modbus_client, addresses_and_counts: List[tuple]) -> List[int]:
    """指定されたアドレスとカウントでレジスタを読み込む"""
    data = []
    for address, count in addresses_and_counts:
        response = modbus_client.read_input_registers(address=address, count=count)
        if not response.isError():
            data.extend(response.registers)
        else:
            raise HTTPException(status_code=500, detail=f"Error reading registers at address {hex(address)}")
    return data

@app.get("/registers", response_model=Dict[str, int])
async def get_all_registers() -> Dict[str, int]:
    """必要な全レジスタを読み込む"""
    powmr_client = connect_modbus()
    growatt_client = connect_modbus2()
    try:
        powmr_raw = _read_holding_blocks(powmr_client, POWMR_HOLDING_BLOCKS)
        growatt_raw = _read_input_blocks(growatt_client, GROWATT_INPUT_BLOCKS)

        powmr_part = _as_hex_dict(powmr_raw, POWMR_REQUIRED)
        growatt_part = _as_dec_dict(growatt_raw, GROWATT_REQUIRED)

        combined: Dict[str, int] = {}
        combined.update(powmr_part)
        combined.update(growatt_part)

        if not combined:
            raise HTTPException(status_code=502, detail="No registers returned")

        return combined

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Combined read error: {e}")

    finally:
        try:
            powmr_client.close()
        except Exception:
            pass
        try:
            growatt_client.close()
        except Exception:
            pass

@app.get("/limited_registers", response_model=Dict[str, int])
async def get_limited_registers():
    """5秒ごと用の限定レジスタ（0, 1, 2, 44, 68）を読み込む"""
    client = connect_modbus()
    try:
        partial_blocks = ((0x0100, 3), (0x021C, 1), (0x0234, 1))
        raw = _read_holding_blocks(client, partial_blocks, unit_id=unit_id)
        subset = _as_hex_dict(raw, POWMR_FAST_ADDRS)

        # 念のため不足チェック
        if len(subset) != len(POWMR_FAST_ADDRS):
            need = {f"0x{a:04X}" for a in POWMR_FAST_ADDRS}
            missing = sorted(need - set(subset.keys()))
            raise HTTPException(status_code=502, detail=f"Missing fast addrs: {missing}")

        return subset
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PowMr limited read error: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass

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
    if not VALID_USERNAME or not VALID_PASSWORD: return
    if not (hmac.compare_digest(credentials.username, VALID_USERNAME) and hmac.compare_digest(credentials.password, VALID_PASSWORD)):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})
    return credentials

@app.get("/set_targets_form", response_class=HTMLResponse)
async def set_targets_form(request: Request, credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    # 現在の targets.json の値を読み込み
    try:
        with open(CONFIG_PATH, "r") as f:
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
        with open(CONFIG_PATH, "w") as f:
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
    PORT = int(os.getenv("MODBUS_API_PORT") or "5004")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="error")
