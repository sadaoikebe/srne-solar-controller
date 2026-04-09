"""Modbus API server.

Exposes inverter registers over HTTP so that db_writer, battery_controller,
and daily_target can share a single serial connection without contention.
"""
from __future__ import annotations

import json
import os
import sys
import hmac
from enum import IntEnum
from typing import Dict, Iterable, List, Tuple

import pymodbus.client as modbusClient
import serial.tools.list_ports
import uvicorn
from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

# ── Auth configuration ────────────────────────────────────────────────────────

VALID_USERNAME = os.getenv("BASIC_AUTH_USER")
VALID_PASSWORD = os.getenv("BASIC_AUTH_PASS")
CONFIG_PATH    = os.getenv("CONFIG_PATH", "/app/targets.json")

if not VALID_USERNAME or not VALID_PASSWORD:
    print(
        "WARNING: BASIC_AUTH_USER or BASIC_AUTH_PASS is not set. "
        "All endpoints that modify inverter settings are UNPROTECTED. "
        "Set both env vars to enable authentication.",
        file=sys.stderr,
    )

# ── Enums ─────────────────────────────────────────────────────────────────────


class OutputPriority(IntEnum):
    SOL = 0
    UTI = 1
    SBU = 2


class ChargingPriority(IntEnum):
    CSO = 0
    CUB = 1
    SNU = 2
    OSO = 3


# ── Register maps ─────────────────────────────────────────────────────────────

POWMR_HOLDING_BLOCKS: Tuple[Tuple[int, int], ...] = (
    (0x0100, 32),   # 0x0100–0x011F
    (0x0200, 32),   # 0x0200–0x021F
    (0x0220, 32),   # 0x0220–0x023F
    (0xF000, 32),   # 0xF000–0xF01F
    (0xF020, 32),   # 0xF020–0xF03F
)

GROWATT_INPUT_BLOCKS: Tuple[Tuple[int, int], ...] = (
    (0, 96),        # 0..95
)

POWMR_REQUIRED: Tuple[int, ...] = (
    # Battery
    0x0100, 0x0101, 0x0102,
    # PV1 / PV2
    0x0107, 0x0108, 0x0109,
    0x010F, 0x0110, 0x0111,
    # AC voltage/frequency & load/grid
    0x0213, 0x022A, 0x0216, 0x022C, 0x0215, 0x0218,
    0x021B, 0x0232, 0x021C, 0x0234, 0x023D, 0x023E,
    # Daily kWh
    0xF02D, 0xF02E, 0xF02F, 0xF030, 0xF03C, 0xF03D,
    # Cumulative 32-bit pairs (combined in db_writer)
    0xF034, 0xF035, 0xF036, 0xF037, 0xF038, 0xF039, 0xF03A, 0xF03B,
)

POWMR_FAST_ADDRS: Tuple[int, ...] = (0x0100, 0x0101, 0x0102, 0x021C, 0x0234)

GROWATT_REQUIRED: Tuple[int, ...] = (
    # PV3 / PV4
    1, 2,               # PV3/PV4 voltage
    3, 4, 5, 6,         # PV3/PV4 power (32-bit hi/lo)
    7, 8,               # PV3/PV4 current
    # Battery / load
    10,                 # load_growatt
    17,                 # battery_voltage_growatt
    83, 84,             # battery current (+charge / +draw)
    # PV3 daily / cumulative (32-bit hi/lo)
    48, 49, 50, 51,
)

# ── FastAPI setup ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Modbus Register API",
    description="Reads/writes inverter Modbus registers on behalf of other services.",
)
templates = Jinja2Templates(directory="/app")
security  = HTTPBasic()

# ── Modbus helpers ────────────────────────────────────────────────────────────


def _read_holding_blocks(
    client, blocks: Iterable[Tuple[int, int]]
) -> Dict[int, int]:
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


def _read_input_blocks(
    client, blocks: Iterable[Tuple[int, int]]
) -> Dict[int, int]:
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


def _as_hex_dict(
    raw: Dict[int, int], whitelist: Iterable[int]
) -> Dict[str, int]:
    w = set(whitelist)
    return {f"0x{a:04X}": raw[a] for a in sorted(raw) if a in w}


def _as_dec_dict(
    raw: Dict[int, int], whitelist: Iterable[int]
) -> Dict[str, int]:
    w = set(whitelist)
    return {str(a): raw[a] for a in sorted(raw) if a in w}


# ── Modbus client initialisation ──────────────────────────────────────────────


def get_modbus_client(vid: int, pid: int) -> modbusClient.ModbusSerialClient | None:
    port = next(
        (p.device for p in serial.tools.list_ports.comports() if p.vid == vid and p.pid == pid),
        None,
    )
    if not port:
        print(f"No device found with VID={vid}, PID={pid}", file=sys.stderr)
        return None
    print(f"Connecting to {port} (VID={vid:#06x}, PID={pid:#06x})")
    return modbusClient.ModbusSerialClient(port=port, baudrate=9600, timeout=1)


# Module-level clients — port is looked up once at startup.
# connect() / close() are called per request to keep the shared bus clean.
modbus  = get_modbus_client(vid=6790,  pid=29987)   # PowMr
modbus2 = get_modbus_client(vid=1250,  pid=5137)     # Growatt


def connect_modbus() -> modbusClient.ModbusSerialClient:
    if modbus is None:
        raise HTTPException(status_code=500, detail="PowMr Modbus device not found")
    if not modbus.connect():
        raise HTTPException(status_code=500, detail="Failed to connect to PowMr Modbus device")
    return modbus


def connect_modbus2() -> modbusClient.ModbusSerialClient:
    if modbus2 is None:
        raise HTTPException(status_code=500, detail="Growatt Modbus device not found")
    if not modbus2.connect():
        raise HTTPException(status_code=500, detail="Failed to connect to Growatt Modbus device")
    return modbus2


# ── Authentication ────────────────────────────────────────────────────────────


def verify_credentials(
    credentials: HTTPBasicCredentials = Depends(security),
) -> HTTPBasicCredentials | None:
    """Validate HTTP Basic Auth credentials.

    If BASIC_AUTH_USER / BASIC_AUTH_PASS env vars are not set, auth is bypassed
    (with a startup warning already printed).  Use constant-time comparison to
    prevent timing attacks.
    """
    if not VALID_USERNAME or not VALID_PASSWORD:
        return None
    if not (
        hmac.compare_digest(credentials.username, VALID_USERNAME)
        and hmac.compare_digest(credentials.password, VALID_PASSWORD)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials


# ── Read endpoints ────────────────────────────────────────────────────────────


@app.get("/registers", response_model=Dict[str, int])
async def get_all_registers() -> Dict[str, int]:
    """Read all required registers from both inverters."""
    powmr_client   = connect_modbus()
    growatt_client = connect_modbus2()
    try:
        powmr_raw   = _read_holding_blocks(powmr_client,   POWMR_HOLDING_BLOCKS)
        growatt_raw = _read_input_blocks(growatt_client,   GROWATT_INPUT_BLOCKS)

        combined = {
            **_as_hex_dict(powmr_raw,   POWMR_REQUIRED),
            **_as_dec_dict(growatt_raw, GROWATT_REQUIRED),
        }

        if not combined:
            raise HTTPException(status_code=502, detail="No registers returned")

        return combined

    except HTTPException:
        raise
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
async def get_limited_registers() -> Dict[str, int]:
    """Read the five fast-poll registers used by battery_controller every 5 s.

    Returns a hex-keyed dict, e.g. {"0x0100": 87, "0x0101": 534, ...}:
      0x0100 = battery SoC (%)
      0x0101 = battery voltage (×0.1 V)
      0x0102 = battery current (×0.1 A, 16-bit two's complement)
      0x021C = load apparent power L1 (W)
      0x0234 = load apparent power L2 (W)
    """
    client = connect_modbus()
    try:
        partial_blocks: Tuple[Tuple[int, int], ...] = ((0x0100, 3), (0x021C, 1), (0x0234, 1))
        raw    = _read_holding_blocks(client, partial_blocks)
        subset = _as_hex_dict(raw, POWMR_FAST_ADDRS)

        if len(subset) != len(POWMR_FAST_ADDRS):
            need    = {f"0x{a:04X}" for a in POWMR_FAST_ADDRS}
            missing = sorted(need - set(subset.keys()))
            raise HTTPException(status_code=502, detail=f"Missing fast addrs: {missing}")

        return subset
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PowMr limited read error: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass


# ── Write endpoints ───────────────────────────────────────────────────────────
# All write endpoints require HTTP Basic Auth (same credentials as the form).


@app.post("/set_charge_current")
async def set_charge_current(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    """Set the grid charge current (A).  Body: {"value": <float>}."""
    modbus_client = connect_modbus()
    try:
        body  = await request.json()
        value = body.get("value")
        if value is None or not isinstance(value, (int, float)):
            raise HTTPException(
                status_code=400, detail="Invalid or missing 'value' in request body"
            )

        regval   = int(value * 10)   # register spec: current × 10
        response = modbus_client.write_register(0xE205, regval)
        if response.isError():
            raise HTTPException(status_code=500, detail="Error writing charge-current register")
        return {"success": True, "value": value}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    finally:
        modbus_client.close()


@app.post("/set_output_priority")
async def set_output_priority(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    """Set the output priority.  Body: {"value": 0|1|2}."""
    modbus_client = connect_modbus()
    try:
        body  = await request.json()
        value = body.get("value")
        valid = [e.value for e in OutputPriority]
        if value is None or value not in valid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid output priority. Must be one of {[e.name for e in OutputPriority]}",
            )

        response = modbus_client.write_register(0xE204, int(value))
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to set Output Priority")
        return {"success": True, "value": OutputPriority(int(value)).name}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    finally:
        modbus_client.close()


@app.get("/get_output_priority")
async def get_output_priority():
    """Read the current output priority."""
    modbus_client = connect_modbus()
    try:
        response = modbus_client.read_holding_registers(address=0xE204, count=1)
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to read Output Priority")
        value = response.registers[0]
        if value not in [e.value for e in OutputPriority]:
            raise HTTPException(status_code=500, detail=f"Unexpected Output Priority value: {value}")
        return {"value": OutputPriority(value).name, "raw_value": value}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    finally:
        modbus_client.close()


@app.post("/set_charging_priority")
async def set_charging_priority(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    """Set the charging priority.  Body: {"value": 0|1|2|3}."""
    modbus_client = connect_modbus()
    try:
        body  = await request.json()
        value = body.get("value")
        valid = [e.value for e in ChargingPriority]
        if value is None or value not in valid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid charging priority. Must be one of {[e.name for e in ChargingPriority]}",
            )

        response = modbus_client.write_register(0xE20F, int(value))
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to set Charging Priority")
        return {"success": True, "value": ChargingPriority(int(value)).name}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    finally:
        modbus_client.close()


@app.get("/get_charging_priority")
async def get_charging_priority():
    """Read the current charging priority."""
    modbus_client = connect_modbus()
    try:
        response = modbus_client.read_holding_registers(address=0xE20F, count=1)
        if response.isError():
            raise HTTPException(status_code=500, detail="Failed to read Charging Priority")
        value = response.registers[0]
        if value not in [e.value for e in ChargingPriority]:
            raise HTTPException(
                status_code=500, detail=f"Unexpected Charging Priority value: {value}"
            )
        return {"value": ChargingPriority(value).name, "raw_value": value}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    finally:
        modbus_client.close()


# ── Targets form ──────────────────────────────────────────────────────────────


@app.get("/set_targets_form", response_class=HTMLResponse)
async def set_targets_form(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
        target_soc            = targets.get("target_soc", 90)
        daily_charge_current  = targets.get("daily_charge_current", 0)
    except Exception as e:
        print(f"Error reading targets.json: {e}", file=sys.stderr)
        target_soc           = 90
        daily_charge_current = 0

    return templates.TemplateResponse(
        "set_targets.html",
        {
            "request":              request,
            "target_soc":           target_soc,
            "daily_charge_current": daily_charge_current,
        },
    )


@app.post("/set_targets", response_class=HTMLResponse)
async def set_targets(
    request: Request,
    target_soc: int            = Form(...),
    daily_charge_current: int  = Form(...),
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    # Validate ranges before writing
    errors: List[str] = []
    if not (0 <= target_soc <= 100):
        errors.append(f"target_soc must be 0–100 (got {target_soc})")
    if not (0 <= daily_charge_current <= 150):
        errors.append(f"daily_charge_current must be 0–150 A (got {daily_charge_current})")

    if errors:
        return templates.TemplateResponse(
            "set_targets.html",
            {
                "request":              request,
                "message":              "Validation error: " + "; ".join(errors),
                "target_soc":           target_soc,
                "daily_charge_current": daily_charge_current,
            },
            status_code=400,
        )

    targets = {"target_soc": target_soc, "daily_charge_current": daily_charge_current}
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        return templates.TemplateResponse(
            "set_targets.html",
            {
                "request": request,
                "message": (
                    f"Targets updated: target_soc={target_soc}%, "
                    f"daily_charge_current={daily_charge_current} A"
                ),
                "target_soc":           target_soc,
                "daily_charge_current": daily_charge_current,
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "set_targets.html",
            {
                "request":              request,
                "message":              f"Error saving targets: {e}",
                "target_soc":           target_soc,
                "daily_charge_current": daily_charge_current,
            },
            status_code=500,
        )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    PORT = int(os.getenv("MODBUS_API_PORT") or "5004")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
