#!/usr/bin/env python3
"""JK-BMS HTTP API with background BLE cache (read-only).

Each configured bank has its own keep-alive poll task (query-only 0x96/0x97).
A timeout or missing bank does not stall the other. Scan+connect is serialized
on the one radio; cell-info waits are not. Serves the latest per-bank snapshot
over HTTP so consumers never block on a BLE connect.

Endpoints (all GET, no body, no auth — local monitoring only)
--------------------------------------------------------------
  GET /health       process liveness + poll summary
  GET /bms          full snapshot for every configured bank
  GET /bms/{bank}   single bank (e.g. a, b)

There are intentionally no write/control routes.

Run (host)::

  uvicorn jkbms_api:app --host 0.0.0.0 --port 5005

Environment
-----------
  JKBMS_CONFIG       path to jkbms.yaml (default: ./jkbms.yaml)
  JKBMS_API_PORT     informational only when launched via uvicorn CLI
  LOG_LEVEL          DEBUG | INFO | WARNING | ERROR
"""

from __future__ import annotations

import asyncio
import copy
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from fastapi import FastAPI, HTTPException

from jkbms_client import BankSession, bank_error, load_config
from log_config import get_logger

log = get_logger("jkbms_api")

# ── Configuration (loaded once at import / lifespan start) ────────────────────

CONFIG_PATH = Path(os.getenv("JKBMS_CONFIG", str(Path(__file__).resolve().parent / "jkbms.yaml")))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.isoformat()


# ── In-memory cache ───────────────────────────────────────────────────────────


class BmsCache:
    """Latest per-bank samples plus poll metadata. Guarded by an asyncio.Lock."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        # bank_id -> sample dict (includes ok / error / metrics)
        self.banks: Dict[str, Dict[str, Any]] = {}
        # bank_id -> monotonic time when that sample was stored
        self._mono_at: Dict[str, float] = {}
        # bank_id -> wall-clock UTC when that sample was stored
        self._wall_at: Dict[str, datetime] = {}
        self.poll_count: int = 0
        self.last_poll_started_at: Optional[datetime] = None
        self.last_poll_finished_at: Optional[datetime] = None
        self.last_poll_duration_s: Optional[float] = None
        self.last_poll_error: Optional[str] = None  # last bank error (if any)
        self.started_at: datetime = _utc_now()
        self.configured: List[str] = []

    async def update_bank(
        self,
        name: str,
        sample: Dict[str, Any],
        *,
        started: datetime,
        finished: datetime,
        duration_s: float,
        loop_error: Optional[str] = None,
    ) -> None:
        """Publish one bank. Does not touch other banks' ages."""
        now_mono = time.monotonic()
        async with self.lock:
            self.poll_count += 1
            self.last_poll_started_at = started
            self.last_poll_finished_at = finished
            self.last_poll_duration_s = duration_s
            if loop_error:
                self.last_poll_error = loop_error
            elif not sample.get("ok"):
                self.last_poll_error = str(sample.get("error") or "poll failed")
            # Always store (including ok=False) so callers see fresh errors.
            self.banks[name] = sample
            self._mono_at[name] = now_mono
            self._wall_at[name] = finished

    async def update_poll(
        self,
        samples: Dict[str, Dict[str, Any]],
        *,
        started: datetime,
        finished: datetime,
        duration_s: float,
        loop_error: Optional[str] = None,
    ) -> None:
        """Batch helper (tests / legacy). Increments poll_count once."""
        now_mono = time.monotonic()
        async with self.lock:
            self.poll_count += 1
            self.last_poll_started_at = started
            self.last_poll_finished_at = finished
            self.last_poll_duration_s = duration_s
            self.last_poll_error = loop_error
            for name, sample in samples.items():
                self.banks[name] = sample
                self._mono_at[name] = now_mono
                self._wall_at[name] = finished

    async def snapshot(self) -> Dict[str, Any]:
        """Return a deep-copied response body with age_s filled in."""
        now_mono = time.monotonic()
        async with self.lock:
            banks_out: Dict[str, Dict[str, Any]] = {}
            for name, sample in self.banks.items():
                item = copy.deepcopy(sample)
                mono = self._mono_at.get(name)
                wall = self._wall_at.get(name)
                item["age_s"] = round(now_mono - mono, 1) if mono is not None else None
                item["sampled_at"] = _iso(wall)
                banks_out[name] = item

            ok_banks = [n for n, s in banks_out.items() if s.get("ok")]
            return {
                "updated_at": _iso(self.last_poll_finished_at),
                "poll_count": self.poll_count,
                "poll_duration_s": self.last_poll_duration_s,
                "last_poll_error": self.last_poll_error,
                "banks": banks_out,
                "ok_count": len(ok_banks),
                "fail_count": len(banks_out) - len(ok_banks),
            }

    async def bank(self, name: str) -> Optional[Dict[str, Any]]:
        snap = await self.snapshot()
        return snap["banks"].get(name)

    async def health_body(self) -> Dict[str, Any]:
        async with self.lock:
            ok_now = [n for n, s in self.banks.items() if s.get("ok")]
            fail_now = [n for n, s in self.banks.items() if not s.get("ok")]
            pending = [n for n in self.configured if n not in self.banks]
            ever_polled = self.poll_count > 0
            if not ever_polled:
                status = "starting"
            elif pending:
                status = "degraded" if ok_now else "starting"
            elif ok_now:
                status = "ok" if not fail_now else "degraded"
            else:
                status = "unavailable"

            return {
                "status": status,
                "started_at": _iso(self.started_at),
                "poll_count": self.poll_count,
                "last_poll_finished_at": _iso(self.last_poll_finished_at),
                "last_poll_duration_s": self.last_poll_duration_s,
                "last_poll_error": self.last_poll_error,
                "ok_banks": ok_now,
                "fail_banks": fail_now,
                "pending_banks": pending,
            }


cache = BmsCache()
_config: Dict[str, Any] = {}
_poll_tasks: List[asyncio.Task] = []


# ── Background poller ─────────────────────────────────────────────────────────


def backoff_sleep_s(
    interval_s: float, fail_streak: int, *, cap_s: float = 60.0
) -> float:
    """Sleep after a failed poll. First failure uses ``interval_s``; then 2×, 4×, … capped."""
    interval_s = float(interval_s)
    if fail_streak <= 1:
        return interval_s
    return min(float(cap_s), interval_s * (2 ** (fail_streak - 1)))


async def _poll_loop_bank(name: str, meta: Mapping[str, Any], cfg: Mapping[str, Any]) -> None:
    interval = max(5, int(cfg.get("poll_interval_s", 10)))
    timeout_s = float(cfg.get("read_timeout_s", 8))
    backoff_cap = float(cfg.get("reconnect_backoff_max_s", 60))
    mac = str(meta.get("mac", ""))
    serial = str(meta.get("serial") or "") or None
    session = BankSession(
        mac,
        bank=name,
        serial=serial,
        timeout_s=timeout_s,
        scan_timeout_s=float(cfg.get("scan_timeout_s", 10)),
        connect_timeout_s=float(cfg.get("connect_timeout_s", 25)),
    )
    fail_streak = 0
    ok_streak = 0
    log.info(
        "Bank poller started  bank=%s  interval=%ds  frame_timeout=%.1fs",
        name, interval, timeout_s,
    )
    try:
        while True:
            t0 = time.monotonic()
            started = _utc_now()
            loop_error: Optional[str] = None
            try:
                sample = await session.poll()
            except Exception as e:
                loop_error = f"{type(e).__name__}: {e}"
                log.error("bank=%s unhandled poll error: %s", name, loop_error)
                sample = bank_error(
                    bank=name, mac=mac, serial_hint=serial, error=loop_error,
                )
                await session.close()

            duration = time.monotonic() - t0
            sample = dict(sample)
            sample["poll_duration_s"] = round(duration, 2)
            await cache.update_bank(
                name,
                sample,
                started=started,
                finished=_utc_now(),
                duration_s=round(duration, 2),
                loop_error=loop_error,
            )

            if sample.get("ok"):
                fail_streak = 0
                ok_streak += 1
                if ok_streak == 1 or ok_streak % 6 == 0:
                    log.info(
                        "bank=%s poll ok  V=%s  I=%s A  SoC=%s%%  dt=%.2fs",
                        name,
                        sample.get("voltage"),
                        sample.get("current"),
                        sample.get("soc"),
                        duration,
                    )
                sleep_s = max(0.0, interval - duration)
            else:
                ok_streak = 0
                fail_streak += 1
                sleep_s = backoff_sleep_s(interval, fail_streak, cap_s=backoff_cap)
                log.warning(
                    "bank=%s poll failed (%s) — retry in %.1fs",
                    name, sample.get("error") or loop_error, sleep_s,
                )

            if sleep_s > 0:
                await asyncio.sleep(sleep_s)
    finally:
        await session.close()
        log.info("Bank poller stopped  bank=%s", name)


# ── FastAPI app ───────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _config, _poll_tasks
    _config = load_config(CONFIG_PATH)
    banks = _config.get("banks") or {}
    cache.configured = list(banks)
    log.info("=" * 60)
    log.info("jkbms_api starting")
    log.info("  config     : %s", _config.get("config_path", CONFIG_PATH))
    log.info("  banks      : %s", list(banks))
    log.info("  interval   : %ss (per bank, keep-alive)", _config.get("poll_interval_s"))
    log.info("  frame tout : %ss", _config.get("read_timeout_s"))
    log.info("  mode       : QUERY-ONLY (0x96/0x97)  keep-alive")
    log.info("=" * 60)

    _poll_tasks = [
        asyncio.create_task(
            _poll_loop_bank(name, meta, _config),
            name=f"jkbms-poll-{name}",
        )
        for name, meta in banks.items()
    ]
    try:
        yield
    finally:
        log.info("jkbms_api shutting down — cancelling %d poller(s)", len(_poll_tasks))
        for task in _poll_tasks:
            task.cancel()
        if _poll_tasks:
            await asyncio.gather(*_poll_tasks, return_exceptions=True)
        _poll_tasks = []
        log.info("jkbms_api stopped")


app = FastAPI(
    title="JK-BMS API",
    description=(
        "Read-only JK-BMS telemetry over BLE. Background cache; no control endpoints."
    ),
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> Dict[str, Any]:
    """Liveness / readiness summary.

    Always returns HTTP 200 while the process is up so simple probes work.
    Inspect ``status`` for readiness: starting | ok | degraded | unavailable.
    """
    body = await cache.health_body()
    body["config_banks"] = list((_config or {}).get("banks", {}))
    return body


@app.get("/bms")
async def get_bms() -> Dict[str, Any]:
    """Full snapshot for all banks (partial failure still HTTP 200)."""
    snap = await cache.snapshot()
    if snap["poll_count"] == 0:
        # Poller has not finished a cycle yet.
        raise HTTPException(
            status_code=503,
            detail="No poll completed yet — try again shortly",
        )
    return snap


@app.get("/bms/{bank_id}")
async def get_bms_bank(bank_id: str) -> Dict[str, Any]:
    """Single-bank snapshot. Unknown bank → 404; not yet polled → 503."""
    cfg_banks = (_config or {}).get("banks") or {}
    if bank_id not in cfg_banks:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown bank {bank_id!r}; known: {sorted(cfg_banks)}",
        )

    snap = await cache.snapshot()
    if snap["poll_count"] == 0:
        raise HTTPException(
            status_code=503,
            detail="No poll completed yet — try again shortly",
        )

    sample = snap["banks"].get(bank_id)
    if sample is None:
        # Other bank(s) may already have reported; this one has not yet.
        raise HTTPException(
            status_code=503,
            detail=f"Bank {bank_id!r} not present in latest poll",
        )
    return sample


# ── Dev entrypoint ────────────────────────────────────────────────────────────


def main() -> None:
    import uvicorn

    port = int(os.getenv("JKBMS_API_PORT", "5005"))
    host = os.getenv("JKBMS_API_HOST", "0.0.0.0")
    log.info("Starting uvicorn on %s:%d", host, port)
    uvicorn.run(
        "jkbms_api:app",
        host=host,
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()
