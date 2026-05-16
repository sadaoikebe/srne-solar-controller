"""Modbus API server.

Exposes inverter registers over HTTP so that db_writer, battery_controller,
and daily_target can share a single serial connection without contention.

Log levels
----------
  DEBUG  — per-block register read details, raw register key/value dumps,
           write register values before transmission
  INFO   — device discovery at startup, per-request summaries for /registers
           and write endpoints, targets.json saves
  WARNING — device not found, auth bypass active, unexpected register values
  ERROR  — Modbus connection failure, register read/write failure
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import hmac
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pymodbus.client as modbusClient
import serial.tools.list_ports
import uvicorn
from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates

from log_config import get_logger

log = get_logger("modbus_api")

# ── Auth configuration ────────────────────────────────────────────────────────

VALID_USERNAME = os.getenv("BASIC_AUTH_USER")
VALID_PASSWORD = os.getenv("BASIC_AUTH_PASS")
CONFIG_PATH    = os.getenv("CONFIG_PATH", "/app/targets.json")

if not VALID_USERNAME or not VALID_PASSWORD:
    log.warning(
        "BASIC_AUTH_USER or BASIC_AUTH_PASS is not set — "
        "all endpoints that modify inverter settings are UNPROTECTED"
    )

# ── Manual override / host-reboot configuration ──────────────────────────────

OVERRIDE_TTL_MINUTES: int = 60
VALID_OVERRIDE_STATES: Tuple[str, ...] = ("UTI_CHARGING", "UTI_STOPPED", "SBU")

# Bind-mounted by scripts/install-host-reboot.sh; absent in the default setup.
REBOOT_SENTINEL_DIR = Path("/var/run/srne-reboot")
HOST_REBOOT_ENABLED: bool = REBOOT_SENTINEL_DIR.is_dir()
if HOST_REBOOT_ENABLED:
    log.info("Host reboot enabled — sentinel dir %s present", REBOOT_SENTINEL_DIR)

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
    # Read only the ranges that contain registers in POWMR_REQUIRED.
    # Bulk reads (count=32) fail with IllegalAddress when the block
    # spans non-existent registers on some PowMr models.
    (0x0100, 3),    # 0x0100–0x0102  battery SoC, voltage, current
    (0x0107, 3),    # 0x0107–0x0109  PV1 voltage, current, power
    (0x010F, 3),    # 0x010F–0x0111  PV2 voltage, current, power
    (0x0213, 10),   # 0x0213–0x021C  grid/inverter V & freq, load L1
    (0x0220, 3),    # 0x0220–0x0222  DC-DC, inverter, transformer temps
    (0x022A, 3),    # 0x022A–0x022C  grid V L2, inverter V L2
    (0x0232, 3),    # 0x0232–0x0234  load active L2, load apparent L2
    (0x023D, 2),    # 0x023D–0x023E  grid power L1, L2
    (0xF02D, 4),    # 0xF02D–0xF030  daily counters
    (0xF034, 10),   # 0xF034–0xF03D  cumulative + daily grid/batt
)

GROWATT_INPUT_BLOCKS: Tuple[Tuple[int, int], ...] = (
    (0, 96),        # 0..95
)

POWMR_REQUIRED: Tuple[int, ...] = (
    0x0100, 0x0101, 0x0102,
    0x0107, 0x0108, 0x0109,
    0x010F, 0x0110, 0x0111,
    0x0213, 0x022A, 0x0216, 0x022C, 0x0215, 0x0218,
    0x021B, 0x0232, 0x021C, 0x0234, 0x023D, 0x023E,
    0x0220, 0x0221, 0x0222,
    0xF02D, 0xF02E, 0xF02F, 0xF030, 0xF03C, 0xF03D,
    0xF034, 0xF035, 0xF036, 0xF037, 0xF038, 0xF039, 0xF03A, 0xF03B,
)

POWMR_FAST_ADDRS: Tuple[int, ...] = (0x0100, 0x0101, 0x0102, 0x021C, 0x0234)

# Full Growatt input register range exposed by /registers. The same range is
# already read on the wire (see GROWATT_INPUT_BLOCKS); we just stopped filtering
# it down. Registers without a regmap.yaml entry land in the raw tier
# (modbus_raw measurement) so unknowns stay recoverable for later analysis.
GROWATT_RAW_RANGE: Tuple[int, ...] = tuple(range(0, 96))

# ── FastAPI setup ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Modbus Register API",
    description="Reads/writes inverter Modbus registers on behalf of other services.",
)
templates = Jinja2Templates(directory="/app")
security  = HTTPBasic()

# ── Modbus helpers ────────────────────────────────────────────────────────────


def _read_holding_blocks(
    client, blocks: Iterable[Tuple[int, int]], label: str = "device"
) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for start, count in blocks:
        log.debug("%s: reading holding block 0x%04X / count=%d", label, start, count)
        rr = client.read_holding_registers(address=start, count=count)
        if hasattr(rr, "isError") and rr.isError():
            log.error("%s: holding block 0x%04X/%d failed: %s", label, start, count, rr)
            raise RuntimeError(f"Holding read failed @0x{start:04X}/n={count}: {rr}")
        regs = getattr(rr, "registers", None)
        if regs is None:
            log.error("%s: holding block 0x%04X/%d returned no registers: %s", label, start, count, rr)
            raise RuntimeError(f"Holding read missing 'registers' @0x{start:04X}/n={count}: {rr}")
        for i, v in enumerate(regs):
            out[start + i] = int(v) & 0xFFFF
        log.debug("%s: holding block 0x%04X OK (%d regs)", label, start, len(regs))
    return out


def _read_input_blocks(
    client, blocks: Iterable[Tuple[int, int]], label: str = "device"
) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for start, count in blocks:
        log.debug("%s: reading input block %d / count=%d", label, start, count)
        rr = client.read_input_registers(address=start, count=count)
        if hasattr(rr, "isError") and rr.isError():
            log.error("%s: input block %d/%d failed: %s", label, start, count, rr)
            raise RuntimeError(f"Input read failed @{start}/n={count}: {rr}")
        regs = getattr(rr, "registers", None)
        if regs is None:
            log.error("%s: input block %d/%d returned no registers: %s", label, start, count, rr)
            raise RuntimeError(f"Input read missing 'registers' @{start}/n={count}: {rr}")
        for i, v in enumerate(regs):
            out[start + i] = int(v) & 0xFFFF
        log.debug("%s: input block %d OK (%d regs)", label, start, len(regs))
    return out


def _as_hex_dict(
    raw: Dict[int, int], whitelist: Iterable[int]
) -> Dict[str, int]:
    w = set(whitelist)
    return {f"0x{a:04x}": raw[a] for a in sorted(raw) if a in w}


def _as_dec_dict(
    raw: Dict[int, int], whitelist: Iterable[int]
) -> Dict[str, int]:
    w = set(whitelist)
    return {str(a): raw[a] for a in sorted(raw) if a in w}


# ── Modbus client initialisation ──────────────────────────────────────────────


def get_modbus_client(
    vid: int, pid: int, label: str
) -> modbusClient.ModbusSerialClient | None:
    port = next(
        (p.device for p in serial.tools.list_ports.comports() if p.vid == vid and p.pid == pid),
        None,
    )
    if not port:
        log.warning(
            "%s device not found (VID=0x%04X  PID=0x%04X) — "
            "related endpoints will return HTTP 500",
            label, vid, pid,
        )
        return None
    log.info("%s device found: %s  (VID=0x%04X  PID=0x%04X)", label, port, vid, pid)
    return modbusClient.ModbusSerialClient(port=port, baudrate=9600, timeout=3)


# Module-level clients — found once at startup.
# connect() / close() are called per-request to keep the shared bus clean.
#
# Concurrent access to the PowMr serial bus is serialised by `_powmr_lock`
# below: every endpoint that touches `modbus` acquires it for the full
# connect → transact → close sequence. This prevents two coroutines from
# racing inside the shared ModbusSerialClient when polls overlap (e.g.
# db_writer's 30 s /registers vs battery_controller's 5 s /limited_registers).
#
# Growatt (`modbus2`) is intentionally NOT locked — its access pattern has
# never produced the race in practice, so we keep it untouched.
#
# Note: the lock is per-process. If the deployment ever moves to multiple
# uvicorn workers, this protection no longer holds — keep --workers 1.
modbus  = get_modbus_client(vid=6790,  pid=29987, label="PowMr")    # PowMr inverter
modbus2 = get_modbus_client(vid=1250,  pid=5137,  label="Growatt")  # Growatt inverter

_powmr_lock = asyncio.Lock()

# Latching readiness flag for `/health`. Once a single PowMr register read
# succeeds, the bus is considered healthy and `/health` stops touching it.
# Docker's healthcheck can then poll forever without loading the bus.
_ready: bool = False


def connect_modbus() -> modbusClient.ModbusSerialClient:
    if modbus is None:
        raise HTTPException(status_code=500, detail="PowMr Modbus device not found at startup")
    if not modbus.connect():
        log.error("Failed to open serial connection to PowMr")
        raise HTTPException(status_code=500, detail="Failed to connect to PowMr Modbus device")
    return modbus


def connect_modbus2() -> modbusClient.ModbusSerialClient:
    if modbus2 is None:
        raise HTTPException(status_code=500, detail="Growatt Modbus device not found at startup")
    if not modbus2.connect():
        log.error("Failed to open serial connection to Growatt")
        raise HTTPException(status_code=500, detail="Failed to connect to Growatt Modbus device")
    return modbus2


# ── Authentication ────────────────────────────────────────────────────────────


def verify_credentials(
    credentials: HTTPBasicCredentials = Depends(security),
) -> HTTPBasicCredentials | None:
    if not VALID_USERNAME or not VALID_PASSWORD:
        return None  # auth disabled — startup warning already emitted
    if not (
        hmac.compare_digest(credentials.username, VALID_USERNAME)
        and hmac.compare_digest(credentials.password, VALID_PASSWORD)
    ):
        log.warning("Authentication failed for user %r", credentials.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials


# ── Read endpoints ────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """Latching readiness probe used by Docker's healthcheck and depends_on.

    While `_ready` is False, perform a single PowMr register read to confirm
    the bus is alive. On first success, latch `_ready = True` — every
    subsequent call returns immediately without touching the bus, so the
    healthcheck can poll forever at no Modbus cost.
    """
    global _ready
    if _ready:
        return {"status": "ready"}

    async with _powmr_lock:
        client = connect_modbus()
        try:
            rr = client.read_holding_registers(address=0x0100, count=1)
            if hasattr(rr, "isError") and rr.isError():
                raise HTTPException(status_code=503, detail=f"Not ready: {rr}")
            if not getattr(rr, "registers", None):
                raise HTTPException(status_code=503, detail="Not ready: no registers")
            _ready = True
            log.info("/health: readiness latched — PowMr responsive")
            return {"status": "ready"}
        except HTTPException:
            raise
        except Exception as e:
            log.warning("/health: probe failed (will retry): %s", e)
            raise HTTPException(status_code=503, detail=f"Not ready: {e}")
        finally:
            try:
                client.close()
            except Exception:
                pass


@app.get("/registers", response_model=Dict[str, int])
async def get_all_registers() -> Dict[str, int]:
    """Read all required registers from both inverters and return a combined dict."""
    log.debug("Reading all registers: PowMr (%d blocks) + Growatt (%d blocks)",
              len(POWMR_HOLDING_BLOCKS), len(GROWATT_INPUT_BLOCKS))
    try:
        # PowMr is shared with /limited_registers and the write endpoints —
        # serialise the connect → read → close sequence under _powmr_lock.
        async with _powmr_lock:
            powmr_client = connect_modbus()
            try:
                powmr_raw = _read_holding_blocks(powmr_client, POWMR_HOLDING_BLOCKS, "PowMr")
            finally:
                try:
                    powmr_client.close()
                except Exception:
                    pass

        # Growatt: unchanged, no lock.
        growatt_client = connect_modbus2()
        try:
            growatt_raw = _read_input_blocks(growatt_client, GROWATT_INPUT_BLOCKS, "Growatt")
        finally:
            try:
                growatt_client.close()
            except Exception:
                pass

        powmr_part   = _as_hex_dict(powmr_raw,   POWMR_REQUIRED)
        growatt_part = _as_dec_dict(growatt_raw, GROWATT_RAW_RANGE)
        combined     = {**powmr_part, **growatt_part}

        if not combined:
            log.error("Combined register read returned 0 values")
            raise HTTPException(status_code=502, detail="No registers returned")

        log.info(
            "/registers: %d total  (PowMr: %d  Growatt: %d)",
            len(combined), len(powmr_part), len(growatt_part),
        )
        return combined

    except HTTPException:
        raise
    except Exception as e:
        log.error("/registers: unexpected error: %s", e)
        raise HTTPException(status_code=500, detail=f"Combined read error: {e}")


@app.get("/limited_registers", response_model=Dict[str, int])
async def get_limited_registers() -> Dict[str, int]:
    """Read the five fast-poll registers used by battery_controller every 5 s.

    Returns a hex-keyed dict:
      0x0100 = battery SoC (%)
      0x0101 = battery voltage (×0.1 V)
      0x0102 = battery current (×0.1 A, 16-bit two's complement)
      0x021C = load apparent power L1 (W)
      0x0234 = load apparent power L2 (W)
    """
    async with _powmr_lock:
        client = connect_modbus()
        try:
            partial_blocks: Tuple[Tuple[int, int], ...] = ((0x0100, 3), (0x021C, 1), (0x0234, 1))
            raw    = _read_holding_blocks(client, partial_blocks, "PowMr")
            subset = _as_hex_dict(raw, POWMR_FAST_ADDRS)

            if len(subset) != len(POWMR_FAST_ADDRS):
                need    = {f"0x{a:04x}" for a in POWMR_FAST_ADDRS}
                missing = sorted(need - set(subset.keys()))
                log.error("/limited_registers: missing addresses %s", missing)
                raise HTTPException(status_code=502, detail=f"Missing fast addrs: {missing}")

            log.debug(
                "/limited_registers: SoC=%s%%  raw_V=%s  raw_I=%s  L1=%s W  L2=%s W",
                subset.get("0x0100"), subset.get("0x0101"),
                subset.get("0x0102"), subset.get("0x021c"), subset.get("0x0234"),
            )
            return subset
        except HTTPException:
            raise
        except Exception as e:
            log.error("/limited_registers: unexpected error: %s", e)
            raise HTTPException(status_code=500, detail=f"PowMr limited read error: {e}")
        finally:
            try:
                client.close()
            except Exception:
                pass


@app.get("/raw_read")
async def raw_read(addr: str, count: int = 1, device: str = "powmr"):
    """Read raw uint16 register values with no schema decoding.

    For ad-hoc debugging — registers not yet in regmap.yaml, sanity checks, etc.

      addr   : decimal ("259") or hex ("0x0103")
      count  : 1..64 consecutive registers
      device : "powmr" (holding regs) or "growatt" (input regs)
    """
    try:
        address = int(addr, 0)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid addr: {addr!r}")
    if not (1 <= count <= 64):
        raise HTTPException(status_code=400, detail="count must be 1..64")

    if device not in ("powmr", "growatt"):
        raise HTTPException(status_code=400, detail="device must be powmr or growatt")

    # Acquire the PowMr lock only when actually touching PowMr; for Growatt
    # this is a no-op so that path stays unchanged.
    guard = _powmr_lock if device == "powmr" else nullcontext()
    async with guard:
        if device == "powmr":
            client = connect_modbus()
            reader = client.read_holding_registers
            label  = "PowMr"
        else:  # growatt — validated above
            client = connect_modbus2()
            reader = client.read_input_registers
            label  = "Growatt"

        try:
            rr = reader(address=address, count=count)
            if hasattr(rr, "isError") and rr.isError():
                log.error("/raw_read: %s read failed at 0x%04X/%d: %s", label, address, count, rr)
                raise HTTPException(status_code=502, detail=f"{label} read failed: {rr}")
            regs = getattr(rr, "registers", None) or []
            out: Dict[str, Dict[str, int | str]] = {}
            for i, v in enumerate(regs):
                a   = address + i
                v16 = int(v) & 0xFFFF
                out[f"0x{a:04x}"] = {
                    "raw": v16,
                    "hex": f"0x{v16:04x}",
                }
            log.info("/raw_read: %s addr=0x%04X count=%d -> %d regs", label, address, count, len(regs))
            return out
        except HTTPException:
            raise
        except Exception as e:
            log.error("/raw_read: unexpected error: %s", e)
            raise HTTPException(status_code=500, detail=f"Raw read error: {e}")
        finally:
            try:
                client.close()
            except Exception:
                pass


# ── Write endpoints ───────────────────────────────────────────────────────────


@app.post("/set_charge_current")
async def set_charge_current(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    """Set the grid charge current (A).  Body: {"value": <float>}."""
    async with _powmr_lock:
        modbus_client = connect_modbus()
        try:
            body  = await request.json()
            value = body.get("value")
            if value is None or not isinstance(value, (int, float)):
                raise HTTPException(
                    status_code=400, detail="Invalid or missing 'value' in request body"
                )

            regval = int(value * 10)
            log.debug("/set_charge_current: writing 0xE205 = %d (%.1f A)", regval, value)
            response = modbus_client.write_register(0xE205, regval)
            if response.isError():
                log.error("/set_charge_current: register write failed: %s", response)
                raise HTTPException(status_code=500, detail="Error writing charge-current register")

            log.info("/set_charge_current: %.1f A written (reg 0xE205=%d)", value, regval)
            return {"success": True, "value": value}
        except HTTPException:
            raise
        except Exception as e:
            log.error("/set_charge_current: unexpected error: %s", e)
            raise HTTPException(status_code=500, detail=f"Error: {e}")
        finally:
            modbus_client.close()


@app.post("/set_output_priority")
async def set_output_priority(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    """Set the output priority.  Body: {"value": 0|1|2}."""
    async with _powmr_lock:
        modbus_client = connect_modbus()
        try:
            body  = await request.json()
            value = body.get("value")
            valid = [e.value for e in OutputPriority]
            if value is None or value not in valid:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid output priority — must be one of {[e.name for e in OutputPriority]}",
                )

            log.debug("/set_output_priority: writing 0xE204 = %d (%s)", value, OutputPriority(value).name)
            response = modbus_client.write_register(0xE204, int(value))
            if response.isError():
                log.error("/set_output_priority: register write failed: %s", response)
                raise HTTPException(status_code=500, detail="Failed to set Output Priority")

            name = OutputPriority(int(value)).name
            log.info("/set_output_priority: set to %s (reg 0xE204=%d)", name, value)
            return {"success": True, "value": name}
        except HTTPException:
            raise
        except Exception as e:
            log.error("/set_output_priority: unexpected error: %s", e)
            raise HTTPException(status_code=500, detail=f"Error: {e}")
        finally:
            modbus_client.close()


@app.get("/get_output_priority")
async def get_output_priority():
    """Read the current output priority."""
    async with _powmr_lock:
        modbus_client = connect_modbus()
        try:
            response = modbus_client.read_holding_registers(address=0xE204, count=1)
            if response.isError():
                log.error("/get_output_priority: register read failed: %s", response)
                raise HTTPException(status_code=500, detail="Failed to read Output Priority")
            value = response.registers[0]
            if value not in [e.value for e in OutputPriority]:
                log.warning("/get_output_priority: unexpected value %d in register 0xE204", value)
                raise HTTPException(status_code=500, detail=f"Unexpected Output Priority value: {value}")
            log.debug("/get_output_priority: %s (%d)", OutputPriority(value).name, value)
            return {"value": OutputPriority(value).name, "raw_value": value}
        except HTTPException:
            raise
        except Exception as e:
            log.error("/get_output_priority: unexpected error: %s", e)
            raise HTTPException(status_code=500, detail=f"Error: {e}")
        finally:
            modbus_client.close()


@app.post("/set_charging_priority")
async def set_charging_priority(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    """Set the charging priority.  Body: {"value": 0|1|2|3}."""
    async with _powmr_lock:
        modbus_client = connect_modbus()
        try:
            body  = await request.json()
            value = body.get("value")
            valid = [e.value for e in ChargingPriority]
            if value is None or value not in valid:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid charging priority — must be one of {[e.name for e in ChargingPriority]}",
                )

            log.debug("/set_charging_priority: writing 0xE20F = %d (%s)", value, ChargingPriority(value).name)
            response = modbus_client.write_register(0xE20F, int(value))
            if response.isError():
                log.error("/set_charging_priority: register write failed: %s", response)
                raise HTTPException(status_code=500, detail="Failed to set Charging Priority")

            name = ChargingPriority(int(value)).name
            log.info("/set_charging_priority: set to %s (reg 0xE20F=%d)", name, value)
            return {"success": True, "value": name}
        except HTTPException:
            raise
        except Exception as e:
            log.error("/set_charging_priority: unexpected error: %s", e)
            raise HTTPException(status_code=500, detail=f"Error: {e}")
        finally:
            modbus_client.close()


@app.get("/get_charging_priority")
async def get_charging_priority():
    """Read the current charging priority."""
    async with _powmr_lock:
        modbus_client = connect_modbus()
        try:
            response = modbus_client.read_holding_registers(address=0xE20F, count=1)
            if response.isError():
                log.error("/get_charging_priority: register read failed: %s", response)
                raise HTTPException(status_code=500, detail="Failed to read Charging Priority")
            value = response.registers[0]
            if value not in [e.value for e in ChargingPriority]:
                log.warning("/get_charging_priority: unexpected value %d in register 0xE20F", value)
                raise HTTPException(
                    status_code=500, detail=f"Unexpected Charging Priority value: {value}"
                )
            log.debug("/get_charging_priority: %s (%d)", ChargingPriority(value).name, value)
            return {"value": ChargingPriority(value).name, "raw_value": value}
        except HTTPException:
            raise
        except Exception as e:
            log.error("/get_charging_priority: unexpected error: %s", e)
            raise HTTPException(status_code=500, detail=f"Error: {e}")
        finally:
            modbus_client.close()


# ── Targets form ──────────────────────────────────────────────────────────────


def _next_2259_date_iso() -> str:
    """Return the ISO date of the next upcoming 22:59 in local time.

    If the form is submitted before 22:59 today, the daily_target cron run
    we want to skip is *today's*. Submitted at/after 22:59 — today's run
    has already happened (or is about to), so the next run is tomorrow.
    """
    now = datetime.now()
    nxt = now.replace(hour=22, minute=59, second=0, microsecond=0)
    if now >= nxt:
        nxt += timedelta(days=1)
    return nxt.date().isoformat()


def _skip_auto_view(targets: dict) -> tuple[bool, str]:
    """Return (skip_auto_active, skip_auto_date) for template rendering."""
    raw = targets.get("skip_next_auto")
    if not raw:
        return False, ""
    today_iso = datetime.now().date().isoformat()
    if raw < today_iso:
        return False, ""  # stale flag — daily_target ignores it; treat as inactive
    return True, raw


def _override_view(targets: dict) -> tuple[str, str]:
    """Return (override_state, remaining_text) for template rendering.

    State is "auto" when no override is active or when the stored override is
    expired/malformed; the caller doesn't need to know the difference.
    """
    raw = targets.get("manual_override") or {}
    state = raw.get("state")
    expires = raw.get("expires_at")
    if state not in VALID_OVERRIDE_STATES or not expires:
        return "auto", ""
    try:
        exp_dt = datetime.fromisoformat(expires)
    except Exception:
        return "auto", ""
    delta = exp_dt - datetime.now(exp_dt.tzinfo)
    if delta.total_seconds() <= 0:
        return "auto", ""
    return state, f"{int(delta.total_seconds() // 60)} min remaining"


@app.get("/set_targets_form", response_class=HTMLResponse)
async def set_targets_form(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
        # Coerce to int so the form (and confirmation message) never show "80.0".
        target_soc           = int(targets.get("target_soc", 90))
        daily_charge_current = int(targets.get("daily_charge_current", 0))
        full_charge          = bool(targets.get("full_charge", False))
        last_full_charge     = targets.get("last_full_charge") or "never"
        override_state, override_remaining = _override_view(targets)
        skip_auto, skip_auto_date          = _skip_auto_view(targets)
        log.debug(
            "/set_targets_form: loaded target_soc=%s  daily_charge_current=%s  "
            "full_charge=%s  last_full_charge=%s  override=%s  skip_auto=%s",
            target_soc, daily_charge_current, full_charge, last_full_charge,
            override_state, skip_auto,
        )
    except Exception as e:
        log.warning("/set_targets_form: could not read targets.json: %s — using defaults", e)
        target_soc           = 90
        daily_charge_current = 0
        full_charge          = False
        last_full_charge     = "never"
        override_state       = "auto"
        override_remaining   = ""
        skip_auto            = False
        skip_auto_date       = ""

    return templates.TemplateResponse(
        "set_targets.html",
        {
            "request":              request,
            "target_soc":           target_soc,
            "daily_charge_current": daily_charge_current,
            "full_charge":          full_charge,
            "last_full_charge":     last_full_charge,
            "override_state":       override_state,
            "override_remaining":   override_remaining,
            "skip_auto":            skip_auto,
            "skip_auto_date":       skip_auto_date,
            "host_reboot_enabled":  HOST_REBOOT_ENABLED,
        },
    )


@app.post("/set_targets", response_class=HTMLResponse)
async def set_targets(
    request: Request,
    target_soc: int           = Form(...),
    daily_charge_current: int = Form(...),
    full_charge: bool         = Form(False),
    override_state: str       = Form("auto"),
    skip_auto: bool           = Form(False),
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    errors: List[str] = []
    if not (0 <= target_soc <= 100):
        errors.append(f"target_soc must be 0–100 (got {target_soc})")
    if not (0 <= daily_charge_current <= 150):
        errors.append(f"daily_charge_current must be 0–150 A (got {daily_charge_current})")
    if override_state != "auto" and override_state not in VALID_OVERRIDE_STATES:
        errors.append(f"override_state must be auto/UTI_CHARGING/UTI_STOPPED/SBU (got {override_state!r})")

    # Read existing targets so last_full_charge (owned by battery_controller) is preserved
    # — we only ever write the user-facing keys plus whatever was already there.
    try:
        with open(CONFIG_PATH) as f:
            existing = json.load(f)
    except Exception:
        existing = {}
    last_full_charge = existing.get("last_full_charge") or "never"

    def _render(message: str, status_code: int = 200, extra: dict | None = None) -> HTMLResponse:
        ctx = {
            "request":              request,
            "message":              message,
            "target_soc":           target_soc,
            "daily_charge_current": daily_charge_current,
            "full_charge":          full_charge,
            "last_full_charge":     last_full_charge,
            "override_state":       override_state if override_state in {"auto", *VALID_OVERRIDE_STATES} else "auto",
            "override_remaining":   "",
            "skip_auto":            skip_auto,
            "skip_auto_date":       "",
            "host_reboot_enabled":  HOST_REBOOT_ENABLED,
        }
        if extra:
            ctx.update(extra)
        return templates.TemplateResponse("set_targets.html", ctx, status_code=status_code)

    if errors:
        log.warning("/set_targets: validation error: %s", "; ".join(errors))
        return _render("Validation error: " + "; ".join(errors), status_code=400)

    targets = dict(existing)
    targets["target_soc"]           = target_soc
    targets["daily_charge_current"] = daily_charge_current
    targets["full_charge"]          = full_charge

    if override_state == "auto":
        targets.pop("manual_override", None)
        override_summary = "auto (no override)"
    else:
        expires = datetime.now(timezone.utc) + timedelta(minutes=OVERRIDE_TTL_MINUTES)
        targets["manual_override"] = {
            "state":      override_state,
            "expires_at": expires.isoformat(),
        }
        override_summary = f"{override_state} for {OVERRIDE_TTL_MINUTES} min"

    if skip_auto:
        targets["skip_next_auto"] = _next_2259_date_iso()
        skip_summary = f"skip 22:59 on {targets['skip_next_auto']}"
    else:
        targets.pop("skip_next_auto", None)
        skip_summary = "no"

    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        log.info(
            "/set_targets: saved target_soc=%d%%  daily_charge_current=%d A  "
            "full_charge=%s  override=%s  skip_auto=%s",
            target_soc, daily_charge_current, full_charge, override_summary, skip_summary,
        )
        _, override_remaining = _override_view(targets)
        _, skip_auto_date     = _skip_auto_view(targets)
        return _render(
            f"Targets updated: target_soc={target_soc}%, "
            f"daily_charge_current={daily_charge_current} A, "
            f"full_charge={full_charge}, override={override_summary}, "
            f"skip_auto={skip_summary}",
            extra={"override_remaining": override_remaining, "skip_auto_date": skip_auto_date},
        )
    except Exception as e:
        log.error("/set_targets: failed to write targets.json: %s", e)
        return _render(f"Error saving targets: {e}", status_code=500)


# ── Host reboot ───────────────────────────────────────────────────────────────


@app.post("/restart_host", response_class=HTMLResponse)
async def restart_host(
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    """Request a host OS reboot via the systemd path-watch sentinel.

    Returns 404 unless the opt-in setup has been performed (see
    scripts/install-host-reboot.sh).  The container itself never has reboot
    privileges — it merely creates a marker file that root-owned systemd
    watches.
    """
    if not HOST_REBOOT_ENABLED:
        raise HTTPException(
            status_code=404,
            detail=(
                "Host reboot not enabled. "
                "Run scripts/install-host-reboot.sh on the host, "
                "then re-run docker compose up -d."
            ),
        )
    try:
        (REBOOT_SENTINEL_DIR / "reboot-requested").touch()
    except Exception as e:
        log.error("/restart_host: failed to write sentinel: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to request reboot: {e}")
    log.warning("/restart_host: reboot requested")
    return HTMLResponse(
        "<!DOCTYPE html><html><body style='font-family:sans-serif;margin:20px'>"
        "<h2>Host reboot requested</h2>"
        "<p>This page will be unreachable shortly.</p>"
        "<p><a href='/set_targets_form'>Back</a></p>"
        "</body></html>"
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    PORT = int(os.getenv("MODBUS_API_PORT") or "5004")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
