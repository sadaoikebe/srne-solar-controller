#!/usr/bin/env python3
"""JK-BMS BLE client — query-only (no settings / MOSFET / balance control).

Safety policy
-------------
The only GATT writes permitted are the two standard *query* frames:

  0x97 — request device info   (response frame type 0x03)
  0x96 — request settings/cell stream (response types 0x01 / 0x02)

These solicit notification data; they do not change BMS configuration.
Any other command byte raises ``ValueError`` before the radio is touched.

Public API
----------
  make_query_command(cmd, counter=0) -> bytes
  parse_device_info(frame) -> dict
  parse_cell_info(frame) -> dict
  BankSession          keep-alive query-only BLE session (one bank)
  async read_bank(...) one-shot connect / query / disconnect
  async read_banks(...) sequential one-shots (CLI)
  load_config(path) -> dict

CLI (host one-shot)::

  python jkbms_client.py              # both banks from jkbms.yaml
  python jkbms_client.py a            # bank a only
  python jkbms_client.py --json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import struct
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from log_config import get_logger

log = get_logger("jkbms_client")

# ── BLE / protocol constants ──────────────────────────────────────────────────

CHAR_UUID = "0000ffe1-0000-1000-8000-00805f9b34fb"
FRAME_HEADER = bytes([0x55, 0xAA, 0xEB, 0x90])
CMD_HEADER = bytes([0xAA, 0x55, 0x90, 0xEB])

# ONLY these command bytes may ever be written to the BMS.
ALLOWED_QUERY_CMDS = frozenset({0x96, 0x97})
CMD_DEVICE_INFO = 0x97
CMD_CELL_STREAM = 0x96

FRAME_TYPE_SETTINGS = 0x01
FRAME_TYPE_CELL = 0x02
FRAME_TYPE_DEVICE = 0x03

MIN_FRAME = 300
MAX_FRAME = 320

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "jkbms.yaml"

# ── Binary helpers ────────────────────────────────────────────────────────────


def crc8_sum(data: bytes | bytearray) -> int:
    return sum(data) & 0xFF


def u16(data: bytes, i: int) -> int:
    return struct.unpack_from("<H", data, i)[0]


def i16(data: bytes, i: int) -> int:
    return struct.unpack_from("<h", data, i)[0]


def u32(data: bytes, i: int) -> int:
    return struct.unpack_from("<I", data, i)[0]


def i32(data: bytes, i: int) -> int:
    return struct.unpack_from("<i", data, i)[0]


def cstr(data: bytes, start: int, length: int) -> str:
    raw = data[start : start + length]
    return raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace").strip()


def make_query_command(cmd: int, counter: int = 0) -> bytes:
    """Build a 20-byte read-only query frame.

    Raises ``ValueError`` if *cmd* is not in ``ALLOWED_QUERY_CMDS``.
    """
    if cmd not in ALLOWED_QUERY_CMDS:
        raise ValueError(
            f"Refusing non-query command 0x{cmd:02X}. "
            f"Only {[hex(c) for c in sorted(ALLOWED_QUERY_CMDS)]} are allowed."
        )
    frame = bytearray(20)
    frame[0:4] = CMD_HEADER
    frame[4] = cmd & 0xFF
    frame[5] = 0x00  # no payload
    frame[16] = counter & 0xFF
    frame[17] = 0x00
    frame[18] = 0x00
    frame[19] = crc8_sum(frame[0:19])
    return bytes(frame)


# ── Frame assembly ────────────────────────────────────────────────────────────


@dataclass
class FrameCollector:
    """Assemble fragmented BLE notifications into 300-byte JK frames."""

    buffer: bytearray = field(default_factory=bytearray)
    frames: Dict[int, bytes] = field(default_factory=dict)
    chunks: int = 0

    def feed(self, data: bytearray | bytes) -> List[int]:
        """Append a notify chunk; return frame types completed this call."""
        self.chunks += 1
        if not data:
            return []

        if len(data) >= 4 and bytes(data[:4]) == FRAME_HEADER:
            self.buffer.clear()
        self.buffer.extend(data)

        completed: List[int] = []
        while len(self.buffer) >= MIN_FRAME:
            if bytes(self.buffer[:4]) != FRAME_HEADER:
                idx = self.buffer.find(FRAME_HEADER)
                if idx < 0:
                    self.buffer.clear()
                    break
                del self.buffer[:idx]
                if len(self.buffer) < MIN_FRAME:
                    break

            frame_len = MIN_FRAME
            candidate = bytes(self.buffer[:frame_len])
            if crc8_sum(candidate[:-1]) != candidate[-1]:
                if len(self.buffer) >= 320:
                    c320 = bytes(self.buffer[:320])
                    if crc8_sum(c320[:-1]) == c320[-1]:
                        frame_len = 320
                        candidate = c320
                    else:
                        del self.buffer[0]
                        continue
                else:
                    if len(self.buffer) > MAX_FRAME + 50:
                        del self.buffer[0]
                    break

            ftype = candidate[4]
            self.frames[ftype] = candidate
            completed.append(ftype)
            del self.buffer[:frame_len]
        return completed


# ── Parsers (pure; unit-testable) ─────────────────────────────────────────────


def parse_device_info(data: bytes) -> Dict[str, Any]:
    """Frame type 0x03 — identity / uptime. Passwords intentionally omitted."""
    if len(data) < MIN_FRAME:
        raise ValueError(f"device-info frame too short: {len(data)}")
    if data[4] != FRAME_TYPE_DEVICE:
        raise ValueError(f"expected frame type 0x03, got 0x{data[4]:02X}")
    return {
        "vendor": cstr(data, 6, 16),
        "hardware": cstr(data, 22, 8),
        "software": cstr(data, 30, 8),
        "uptime_s": u32(data, 38),
        "power_on_count": u32(data, 42),
        "device_name": cstr(data, 46, 16),
        "mfg_date": cstr(data, 78, 8),
        "serial": cstr(data, 86, 16),
    }


def parse_cell_info_jk02_32s(data: bytes) -> Dict[str, Any]:
    """Frame type 0x02 — live pack data (JK02 32S layout; 16S packs use first 16)."""
    if len(data) < MIN_FRAME:
        raise ValueError(f"cell-info frame too short: {len(data)}")
    if data[4] != FRAME_TYPE_CELL:
        raise ValueError(f"expected frame type 0x02, got 0x{data[4]:02X}")

    cells: List[float] = []
    for i in range(32):
        mv = u16(data, 6 + i * 2)
        if mv == 0:
            continue
        cells.append(round(mv / 1000.0, 3))

    enabled_mask = u32(data, 70)
    if enabled_mask:
        n_enabled = bin(enabled_mask).count("1")
        if n_enabled and n_enabled < len(cells):
            cells = cells[:n_enabled]

    pack_v = u32(data, 150) / 1000.0
    current_a = i32(data, 158) / 1000.0
    power_w = pack_v * current_a

    cell_min = min(cells) if cells else None
    cell_max = max(cells) if cells else None
    cell_delta = (cell_max - cell_min) if cells else None

    return {
        "layout": "JK02_32S",
        "cells": cells,
        "cell_count": len(cells),
        "cell_min": cell_min,
        "cell_max": cell_max,
        "cell_delta": cell_delta,
        "voltage": round(pack_v, 3),
        "current": round(current_a, 3),
        "power": round(power_w, 1),
        "mos_temp": round(i16(data, 144) / 10.0, 1),
        "temp1": round(i16(data, 162) / 10.0, 1),
        "temp2": round(i16(data, 164) / 10.0, 1),
        "balance_current": round(i16(data, 170) / 1000.0, 3),
        "balancing": int(data[172]),
        "soc": int(data[173]),
        "remain_ah": round(u32(data, 174) / 1000.0, 3),
        "nominal_ah": round(u32(data, 178) / 1000.0, 3),
        "cycles": int(u32(data, 182)),
        "soh": int(data[190]),
        "runtime_s": int(u32(data, 194)),
        "charge_mosfet": bool(data[198]),
        "discharge_mosfet": bool(data[199]),
    }


def looks_sane_cell_info(info: Mapping[str, Any]) -> bool:
    cells = info.get("cells") or []
    if not cells:
        return False
    if not all(2.0 < float(v) < 4.5 for v in cells):
        return False
    pack = float(info.get("voltage") or 0)
    if pack < 5 or pack > 120:
        return False
    s = sum(float(v) for v in cells)
    if abs(s - pack) > max(1.0, 0.15 * pack):
        return False
    soc = info.get("soc")
    if soc is None or int(soc) > 100:
        return False
    return True


def parse_cell_info(data: bytes) -> Dict[str, Any]:
    """Parse cell-info frame; currently JK02_32S (validated against our packs)."""
    info = parse_cell_info_jk02_32s(data)
    if not looks_sane_cell_info(info):
        log.warning(
            "Cell-info heuristic uncertain (layout=%s cells=%s V=%.3f SoC=%s)",
            info.get("layout"),
            info.get("cell_count"),
            float(info.get("voltage") or 0),
            info.get("soc"),
        )
    return info


def merge_bank_sample(
    *,
    bank: Optional[str],
    mac: str,
    serial_hint: Optional[str],
    device: Optional[Mapping[str, Any]],
    cell: Mapping[str, Any],
) -> Dict[str, Any]:
    """Flatten device + cell parses into the phase-1 snapshot shape."""
    serial = None
    if device and device.get("serial"):
        serial = device["serial"]
    elif serial_hint:
        serial = serial_hint

    out: Dict[str, Any] = {
        "ok": True,
        "bank": bank,
        "mac": mac.upper(),
        "serial": serial,
        "model": (device or {}).get("vendor"),
        "hardware": (device or {}).get("hardware"),
        "software": (device or {}).get("software"),
        "device_name": (device or {}).get("device_name"),
        "mfg_date": (device or {}).get("mfg_date"),
        "power_on_count": (device or {}).get("power_on_count"),
        "uptime_s": (device or {}).get("uptime_s"),
        # Live pack metrics
        "soc": cell["soc"],
        "soh": cell["soh"],
        "voltage": cell["voltage"],
        "current": cell["current"],
        "power": cell["power"],
        "remain_ah": cell["remain_ah"],
        "nominal_ah": cell["nominal_ah"],
        "cycles": cell["cycles"],
        "mos_temp": cell["mos_temp"],
        "temp1": cell["temp1"],
        "temp2": cell["temp2"],
        "balance_current": cell["balance_current"],
        "balancing": cell["balancing"],
        "charge_mosfet": cell["charge_mosfet"],
        "discharge_mosfet": cell["discharge_mosfet"],
        "runtime_s": cell["runtime_s"],
        "cell_count": cell["cell_count"],
        "cell_min": cell["cell_min"],
        "cell_max": cell["cell_max"],
        "cell_delta": cell["cell_delta"],
        "cells": list(cell["cells"]),
        "error": None,
    }
    return out


def bank_error(
    *,
    bank: Optional[str],
    mac: str,
    serial_hint: Optional[str],
    error: str,
) -> Dict[str, Any]:
    return {
        "ok": False,
        "bank": bank,
        "mac": mac.upper(),
        "serial": serial_hint,
        "error": error,
    }


# ── Config ────────────────────────────────────────────────────────────────────


def load_config(path: Path | str | None = None) -> Dict[str, Any]:
    """Load jkbms.yaml; returns defaults if file missing."""
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    defaults: Dict[str, Any] = {
        "poll_interval_s": 10,
        "read_timeout_s": 8,
        "scan_timeout_s": 10,
        "connect_timeout_s": 25,
        "reconnect_backoff_max_s": 60,
        "banks": {
            "a": {"serial": "40904495693", "mac": "98:DA:20:09:98:80"},
            "b": {"serial": "41101490573", "mac": "98:DA:20:06:85:65"},
        },
    }
    if not cfg_path.is_file():
        log.warning("Config %s not found — using built-in bank defaults", cfg_path)
        return defaults

    try:
        import yaml  # already a project dependency
    except ImportError as e:
        raise RuntimeError("PyYAML required to load jkbms.yaml") from e

    with open(cfg_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid config root in {cfg_path}")

    banks = raw.get("banks") or defaults["banks"]
    if not isinstance(banks, dict) or not banks:
        raise ValueError("jkbms.yaml: 'banks' must be a non-empty mapping")

    return {
        "poll_interval_s": int(raw.get("poll_interval_s", defaults["poll_interval_s"])),
        "read_timeout_s": float(raw.get("read_timeout_s", defaults["read_timeout_s"])),
        "scan_timeout_s": float(raw.get("scan_timeout_s", defaults["scan_timeout_s"])),
        "connect_timeout_s": float(
            raw.get("connect_timeout_s", defaults["connect_timeout_s"])
        ),
        "reconnect_backoff_max_s": float(
            raw.get("reconnect_backoff_max_s", defaults["reconnect_backoff_max_s"])
        ),
        "banks": banks,
        "config_path": str(cfg_path),
    }


# ── BLE session (keep-alive) ──────────────────────────────────────────────────

# Serialize scan+connect across banks (one radio). Frame waits are not locked.
_CONNECT_LOCK = asyncio.Lock()


async def _safe_write_query(client: Any, cmd: int, counter: int) -> None:
    payload = make_query_command(cmd, counter)
    # Defensive: never send anything outside the allowlist.
    assert payload[4] in ALLOWED_QUERY_CMDS
    assert len(payload) == 20
    try:
        await client.write_gatt_char(CHAR_UUID, payload, response=True)
    except Exception:
        await client.write_gatt_char(CHAR_UUID, payload, response=False)


class BankSession:
    """Query-only keep-alive GATT session for one JK-BMS bank.

    ``connect`` / scan is serialized on ``connect_lock`` so two banks do not
    stomp each other on a single adapter. Waiting for a cell-info notify is
    *not* serialized: a timeout on bank A cannot stall bank B's open link.
    """

    def __init__(
        self,
        mac: str,
        *,
        bank: Optional[str] = None,
        serial: Optional[str] = None,
        timeout_s: float = 8.0,
        scan_timeout_s: float = 10.0,
        connect_timeout_s: float = 25.0,
        connect_lock: Optional[asyncio.Lock] = None,
    ) -> None:
        self.mac = mac.upper()
        self.bank = bank
        self.serial = serial
        self.timeout_s = float(timeout_s)
        self.scan_timeout_s = float(scan_timeout_s)
        self.connect_timeout_s = float(connect_timeout_s)
        self._connect_lock = connect_lock if connect_lock is not None else _CONNECT_LOCK
        self._client: Any = None
        self._link_up = False
        self._ever_connected = False
        self._collector = FrameCollector()
        self._cell_gen = 0
        self._cell_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._counter = 0
        self._device_info: Optional[Dict[str, Any]] = None

    def _event(self) -> asyncio.Event:
        if self._cell_event is None:
            self._cell_event = asyncio.Event()
        return self._cell_event

    def _is_connected(self) -> bool:
        client = self._client
        return bool(
            self._link_up and client is not None and getattr(client, "is_connected", False)
        )

    def _on_disconnect(self, _client: Any) -> None:
        was = self._link_up
        self._link_up = False
        if was:
            log.warning("bank=%s BLE disconnected", self.bank)

    def _on_notify(self, _handle: int, data: bytearray) -> None:
        try:
            completed = self._collector.feed(data)
        except Exception:
            log.exception("bank=%s notify parse failed", self.bank)
            return
        got_cell = False
        for ftype in completed:
            name = {
                FRAME_TYPE_SETTINGS: "settings",
                FRAME_TYPE_CELL: "cell_info",
                FRAME_TYPE_DEVICE: "device_info",
            }.get(ftype, f"0x{ftype:02X}")
            log.debug(
                "bank=%s frame %s (%d bytes, chunks=%d)",
                self.bank, name, len(self._collector.frames[ftype]), self._collector.chunks,
            )
            if ftype == FRAME_TYPE_CELL:
                self._cell_gen += 1
                got_cell = True
            if ftype == FRAME_TYPE_DEVICE:
                try:
                    self._device_info = parse_device_info(self._collector.frames[ftype])
                except Exception as e:
                    log.warning("bank=%s device-info parse failed: %s", self.bank, e)
        if got_cell:
            ev = self._cell_event
            loop = self._loop
            if ev is not None and loop is not None:
                loop.call_soon_threadsafe(ev.set)

    async def _connect(self) -> None:
        async with self._connect_lock:
            if self._is_connected():
                return
            await self._disconnect()
            try:
                from bleak import BleakClient, BleakScanner
            except ImportError as e:
                raise RuntimeError(f"bleak not installed: {e}") from e

            log.info("Connecting bank=%s mac=%s serial=%s", self.bank, self.mac, self.serial)
            target: Any = self.mac
            scanned = None
            # First connection: scan so BlueZ has the advertisement cache.
            # Later reconnects try the address first (skips a 10 s scan).
            if not self._ever_connected:
                scanned = await BleakScanner.find_device_by_address(
                    self.mac, timeout=self.scan_timeout_s,
                )
                if scanned is not None:
                    target = scanned
                else:
                    log.info("bank=%s not found in scan — trying address anyway", self.bank)

            client: Any = None
            try:
                client = BleakClient(
                    target,
                    timeout=self.connect_timeout_s,
                    disconnected_callback=self._on_disconnect,
                )
                await client.connect()
            except Exception as first_err:
                if scanned is None:
                    scanned = await BleakScanner.find_device_by_address(
                        self.mac, timeout=self.scan_timeout_s,
                    )
                if scanned is None:
                    raise RuntimeError(
                        "device not found in BLE scan (in range? phone app connected?)"
                    ) from first_err
                client = BleakClient(
                    scanned,
                    timeout=self.connect_timeout_s,
                    disconnected_callback=self._on_disconnect,
                )
                await client.connect()

            if client is None or not client.is_connected:
                raise RuntimeError("connect failed")

            self._collector = FrameCollector()
            self._cell_gen = 0
            self._loop = asyncio.get_running_loop()
            self._cell_event = asyncio.Event()
            self._device_info = None
            self._client = client
            self._link_up = True
            await client.start_notify(CHAR_UUID, self._on_notify)
            await asyncio.sleep(0.2)
            await _safe_write_query(client, CMD_DEVICE_INFO, self._counter)
            self._counter = (self._counter + 1) & 0xFF
            await asyncio.sleep(0.2)
            self._ever_connected = True
            log.info("bank=%s connected (keep-alive)", self.bank)

    async def _disconnect(self) -> None:
        client = self._client
        self._client = None
        self._link_up = False
        if client is None:
            return
        try:
            try:
                await client.stop_notify(CHAR_UUID)
            except Exception:
                pass
            if getattr(client, "is_connected", False):
                await client.disconnect()
        except Exception as e:
            log.debug("bank=%s disconnect: %s", self.bank, e)

    async def _query_cell(self) -> Dict[str, Any]:
        client = self._client
        if client is None or not self._is_connected():
            raise RuntimeError("not connected")
        ev = self._event()
        gen0 = self._cell_gen
        ev.clear()
        await _safe_write_query(client, CMD_CELL_STREAM, self._counter)
        self._counter = (self._counter + 1) & 0xFF
        deadline = time.monotonic() + self.timeout_s
        while self._cell_gen <= gen0:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"no cell-info frame (got {[hex(t) for t in sorted(self._collector.frames)]}; "
                    f"chunks={self._collector.chunks})"
                )
            try:
                await asyncio.wait_for(ev.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"no cell-info frame (got {[hex(t) for t in sorted(self._collector.frames)]}; "
                    f"chunks={self._collector.chunks})"
                ) from None
            ev.clear()

        if FRAME_TYPE_CELL not in self._collector.frames:
            raise RuntimeError("cell-info frame missing after notify")

        cell_info = parse_cell_info(self._collector.frames[FRAME_TYPE_CELL])
        device_info = self._device_info
        if device_info is None and FRAME_TYPE_DEVICE in self._collector.frames:
            try:
                device_info = parse_device_info(self._collector.frames[FRAME_TYPE_DEVICE])
                self._device_info = device_info
            except Exception as e:
                log.warning("bank=%s device-info parse failed: %s", self.bank, e)
        if device_info is None:
            log.warning(
                "bank=%s: no device-info frame; continuing with cell data only", self.bank,
            )

        return merge_bank_sample(
            bank=self.bank,
            mac=self.mac,
            serial_hint=self.serial,
            device=device_info,
            cell=cell_info,
        )

    async def poll(self) -> Dict[str, Any]:
        """Return one snapshot. Reconnects on demand; never raises."""
        try:
            if not self._is_connected():
                await self._connect()
            return await self._query_cell()
        except Exception as e:
            log.error("bank=%s poll failed: %s: %s", self.bank, type(e).__name__, e)
            await self._disconnect()
            return bank_error(
                bank=self.bank,
                mac=self.mac,
                serial_hint=self.serial,
                error=f"{type(e).__name__}: {e}",
            )

    async def close(self) -> None:
        await self._disconnect()


async def read_bank(
    mac: str,
    *,
    bank: Optional[str] = None,
    serial: Optional[str] = None,
    timeout_s: float = 20.0,
    scan_timeout_s: float = 10.0,
    connect_timeout_s: float = 25.0,
) -> Dict[str, Any]:
    """Connect to one BMS, query device+cell info, disconnect.

    Returns a phase-1 snapshot dict with ``ok=True`` or ``ok=False`` + ``error``.
    Never raises for device/protocol failures — callers always get a dict.
    """
    session = BankSession(
        mac,
        bank=bank,
        serial=serial,
        timeout_s=timeout_s,
        scan_timeout_s=scan_timeout_s,
        connect_timeout_s=connect_timeout_s,
    )
    try:
        return await session.poll()
    finally:
        await session.close()


async def read_banks(
    banks: Mapping[str, Mapping[str, Any]],
    *,
    timeout_s: float = 20.0,
    gap_s: float = 1.0,
) -> Dict[str, Dict[str, Any]]:
    """Read multiple banks sequentially (one BLE radio)."""
    results: Dict[str, Dict[str, Any]] = {}
    items = list(banks.items())
    for i, (name, meta) in enumerate(items):
        mac = str(meta["mac"])
        serial = str(meta.get("serial") or "") or None
        results[name] = await read_bank(
            mac, bank=name, serial=serial, timeout_s=timeout_s,
        )
        if i < len(items) - 1 and gap_s > 0:
            await asyncio.sleep(gap_s)
    return results


# ── CLI formatting ────────────────────────────────────────────────────────────


def _fmt_uptime(seconds: Optional[int]) -> str:
    if seconds is None:
        return "?"
    d, rem = divmod(max(0, int(seconds)), 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"


def print_bank(sample: Mapping[str, Any]) -> None:
    bank = sample.get("bank") or "?"
    print("=" * 60)
    print(f"Bank {bank}  mac={sample.get('mac')}  serial={sample.get('serial')}")
    if not sample.get("ok"):
        print(f"  FAILED: {sample.get('error')}")
        return

    print(f"  Model:       {sample.get('model')}  HW={sample.get('hardware')}  SW={sample.get('software')}")
    print(f"  Mfg date:    {sample.get('mfg_date')}")
    print(f"  Pack V:      {sample['voltage']:.3f} V")
    print(f"  Current:     {sample['current']:+.3f} A")
    print(f"  Power:       {sample['power']:+.1f} W")
    print(f"  SoC / SoH:   {sample['soc']} % / {sample['soh']} %")
    print(f"  Capacity:    {sample['remain_ah']:.3f} / {sample['nominal_ah']:.3f} Ah")
    print(f"  Cycles:      {sample['cycles']}")
    print(f"  Temps:       MOS={sample['mos_temp']:.1f}  T1={sample['temp1']:.1f}  T2={sample['temp2']:.1f} °C")
    print(f"  Balance:     I={sample['balance_current']:+.3f} A  action={sample['balancing']}")
    print(
        f"  MOSFETs:     charge={'ON' if sample['charge_mosfet'] else 'OFF'}  "
        f"discharge={'ON' if sample['discharge_mosfet'] else 'OFF'}"
    )
    print(f"  Runtime:     {_fmt_uptime(sample.get('runtime_s'))} ({sample.get('runtime_s')} s)")
    cells = sample.get("cells") or []
    print(f"  Cells ({sample.get('cell_count')}):")
    for i, v in enumerate(cells, 1):
        print(f"    {i:02d}: {v:.3f} V")
    if sample.get("cell_min") is not None:
        print(
            f"  Min/Max/Δ:   {sample['cell_min']:.3f} / {sample['cell_max']:.3f} / "
            f"{sample['cell_delta'] * 1000:.0f} mV"
        )


# ── main ──────────────────────────────────────────────────────────────────────


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="One-shot JK-BMS BLE read (query-only). Close the phone app first.",
    )
    p.add_argument(
        "banks",
        nargs="*",
        help="Bank id(s) from jkbms.yaml (default: all). Example: a b",
    )
    p.add_argument(
        "-c", "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to jkbms.yaml (default: {DEFAULT_CONFIG_PATH})",
    )
    p.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    p.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Per-bank wait for cell-info frame (seconds)",
    )
    return p.parse_args(argv)


async def _amain(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    all_banks: Dict[str, Any] = dict(cfg["banks"])
    timeout = float(args.timeout if args.timeout is not None else cfg["read_timeout_s"])

    if args.banks:
        missing = [b for b in args.banks if b not in all_banks]
        if missing:
            log.error("Unknown bank id(s): %s  (known: %s)", missing, list(all_banks))
            return 2
        selected = {b: all_banks[b] for b in args.banks}
    else:
        selected = all_banks

    log.info(
        "JK-BMS one-shot read  banks=%s  timeout=%.1fs  config=%s",
        list(selected), timeout, cfg.get("config_path", args.config),
    )
    log.info("Query-only mode: commands %s only", sorted(hex(c) for c in ALLOWED_QUERY_CMDS))

    results = await read_banks(selected, timeout_s=timeout)

    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
    else:
        for name in selected:
            print_bank(results[name])
        ok_n = sum(1 for r in results.values() if r.get("ok"))
        print("=" * 60)
        print(f"Done: {ok_n}/{len(results)} bank(s) OK")

    return 0 if all(r.get("ok") for r in results.values()) else 1


def main(argv: Optional[Sequence[str]] = None) -> None:
    raise SystemExit(asyncio.run(_amain(argv)))


if __name__ == "__main__":
    main()
