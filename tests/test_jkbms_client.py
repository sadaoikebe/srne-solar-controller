"""Unit tests for jkbms_client pure helpers (no BLE required)."""

from __future__ import annotations

import struct
import unittest

from jkbms_client import (
    ALLOWED_QUERY_CMDS,
    CMD_CELL_STREAM,
    CMD_DEVICE_INFO,
    FRAME_HEADER,
    FRAME_TYPE_CELL,
    FRAME_TYPE_DEVICE,
    FrameCollector,
    crc8_sum,
    make_query_command,
    merge_bank_sample,
    parse_cell_info_jk02_32s,
    parse_device_info,
)


class TestQueryCommand(unittest.TestCase):
    def test_allowed_cmds(self):
        for cmd in ALLOWED_QUERY_CMDS:
            frame = make_query_command(cmd, counter=3)
            self.assertEqual(len(frame), 20)
            self.assertEqual(frame[0:4], bytes([0xAA, 0x55, 0x90, 0xEB]))
            self.assertEqual(frame[4], cmd)
            self.assertEqual(frame[16], 3)
            self.assertEqual(frame[19], crc8_sum(frame[0:19]))

    def test_rejects_control_cmds(self):
        # Arbitrary non-query bytes that must never be sent.
        for bad in (0x00, 0x01, 0x1E, 0x90, 0xFF):
            with self.assertRaises(ValueError):
                make_query_command(bad)


class TestFrameCollector(unittest.TestCase):
    def test_assembles_split_frame(self):
        body = bytearray(300)
        body[0:4] = FRAME_HEADER
        body[4] = FRAME_TYPE_CELL
        body[299] = crc8_sum(body[:299])
        frame = bytes(body)

        col = FrameCollector()
        self.assertEqual(col.feed(frame[:80]), [])
        self.assertEqual(col.feed(frame[80:200]), [])
        done = col.feed(frame[200:])
        self.assertEqual(done, [FRAME_TYPE_CELL])
        self.assertEqual(col.frames[FRAME_TYPE_CELL], frame)


def _put_u16(buf: bytearray, off: int, val: int) -> None:
    struct.pack_into("<H", buf, off, val & 0xFFFF)


def _put_u32(buf: bytearray, off: int, val: int) -> None:
    struct.pack_into("<I", buf, off, val & 0xFFFFFFFF)


def _put_i32(buf: bytearray, off: int, val: int) -> None:
    struct.pack_into("<i", buf, off, val)


def _put_i16(buf: bytearray, off: int, val: int) -> None:
    struct.pack_into("<h", buf, off, val)


def _synthetic_cell_frame(
    cells_mv: list[int],
    *,
    current_ma: int = 0,
    soc: int = 50,
    soh: int = 100,
) -> bytes:
    buf = bytearray(300)
    buf[0:4] = FRAME_HEADER
    buf[4] = FRAME_TYPE_CELL
    for i, mv in enumerate(cells_mv):
        _put_u16(buf, 6 + i * 2, mv)
    # enabled mask: low n bits set
    mask = (1 << len(cells_mv)) - 1
    _put_u32(buf, 70, mask)
    pack_mv = sum(cells_mv)
    _put_u32(buf, 150, pack_mv)
    _put_i32(buf, 158, current_ma)
    _put_i16(buf, 144, 367)   # 36.7 °C
    _put_i16(buf, 162, 359)
    _put_i16(buf, 164, 351)
    buf[173] = soc
    buf[190] = soh
    _put_u32(buf, 174, 12345)   # remain 12.345 Ah
    _put_u32(buf, 178, 280000)  # nominal 280 Ah
    _put_u32(buf, 182, 42)      # cycles
    _put_u32(buf, 194, 3600)    # runtime 1 h
    buf[198] = 1
    buf[199] = 1
    buf[299] = crc8_sum(buf[:299])
    return bytes(buf)


def _synthetic_device_frame(serial: str = "40904495693") -> bytes:
    buf = bytearray(300)
    buf[0:4] = FRAME_HEADER
    buf[4] = FRAME_TYPE_DEVICE

    def put_str(off: int, n: int, s: str) -> None:
        raw = s.encode("utf-8")[: n - 1]
        buf[off : off + len(raw)] = raw

    put_str(6, 16, "JK_PB2A16S20P")
    put_str(22, 8, "15A")
    put_str(30, 8, "15.32")
    _put_u32(buf, 38, 1000)
    _put_u32(buf, 42, 5)
    put_str(46, 16, serial)
    put_str(78, 8, "241129")
    put_str(86, 16, serial)
    buf[299] = crc8_sum(buf[:299])
    return bytes(buf)


class TestParsers(unittest.TestCase):
    def test_device_info(self):
        info = parse_device_info(_synthetic_device_frame())
        self.assertEqual(info["vendor"], "JK_PB2A16S20P")
        self.assertEqual(info["hardware"], "15A")
        self.assertEqual(info["software"], "15.32")
        self.assertEqual(info["serial"], "40904495693")
        self.assertEqual(info["uptime_s"], 1000)

    def test_cell_info_16s(self):
        cells_mv = [3230 + (i % 3) for i in range(16)]
        info = parse_cell_info_jk02_32s(
            _synthetic_cell_frame(cells_mv, current_ma=-2150, soc=13, soh=99)
        )
        self.assertEqual(info["cell_count"], 16)
        self.assertEqual(len(info["cells"]), 16)
        self.assertAlmostEqual(info["voltage"], sum(cells_mv) / 1000.0, places=3)
        self.assertAlmostEqual(info["current"], -2.15, places=3)
        self.assertEqual(info["soc"], 13)
        self.assertEqual(info["soh"], 99)
        self.assertEqual(info["cycles"], 42)
        self.assertTrue(info["charge_mosfet"])
        self.assertTrue(info["discharge_mosfet"])
        self.assertIsNotNone(info["cell_delta"])

    def test_merge_sample(self):
        device = parse_device_info(_synthetic_device_frame("41101490573"))
        cell = parse_cell_info_jk02_32s(
            _synthetic_cell_frame([3220] * 16, soc=20, soh=100)
        )
        sample = merge_bank_sample(
            bank="b",
            mac="98:da:20:06:85:65",
            serial_hint="41101490573",
            device=device,
            cell=cell,
        )
        self.assertTrue(sample["ok"])
        self.assertEqual(sample["bank"], "b")
        self.assertEqual(sample["serial"], "41101490573")
        self.assertEqual(sample["soc"], 20)
        self.assertEqual(sample["cell_count"], 16)
        self.assertEqual(len(sample["cells"]), 16)
        self.assertIsNone(sample["error"])


if __name__ == "__main__":
    unittest.main()
