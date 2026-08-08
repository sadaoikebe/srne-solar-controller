#!/usr/bin/env python3
"""JK-BMS HTTP API with background BLE cache (read-only).

Polls both battery banks on a fixed interval via jkbms_client (query-only
0x96/0x97 frames), keeps the latest snapshot in memory, and serves it over
HTTP so consumers never block on a BLE connect.

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
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException

from jkbms_client import load_config, read_banks
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
        self.last_poll_error: Optional[str] = None  # loop-level exception
        self.started_at: datetime = _utc_now()

    async def update_poll(
        self,
        samples: Dict[str, Dict[str, Any]],
        *,
        started: datetime,
        finished: datetime,
        duration_s: float,
        loop_error: Optional[str] = None,
    ) -> None:
        now_mono = time.monotonic()
        async with self.lock:
            self.poll_count += 1
            self.last_poll_started_at = started
            self.last_poll_finished_at = finished
            self.last_poll_duration_s = duration_s
            self.last_poll_error = loop_error
            for name, sample in samples.items():
                # Always store (including ok=False) so callers see fresh errors.
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
            ever_polled = self.poll_count > 0
            if not ever_polled:
                status = "starting"
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
            }


cache = BmsCache()
_config: Dict[str, Any] = {}
_poll_task: Optional[asyncio.Task] = None


# ── Background poller ─────────────────────────────────────────────────────────


async def _poll_once(cfg: Dict[str, Any]) -> None:
    banks_cfg = cfg["banks"]
    timeout_s = float(cfg.get("read_timeout_s", 20))
    started = _utc_now()
    t0 = time.monotonic()
    loop_error: Optional[str] = None
    samples: Dict[str, Dict[str, Any]] = {}

    try:
        samples = await read_banks(banks_cfg, timeout_s=timeout_s, gap_s=1.0)
    except Exception as e:
        loop_error = f"{type(e).__name__}: {e}"
        log.error("Poll loop error: %s", loop_error)
        # Synthesize per-bank failures so the cache still advances.
        for name, meta in banks_cfg.items():
            samples[name] = {
                "ok": False,
                "bank": name,
                "mac": str(meta.get("mac", "")).upper(),
                "serial": meta.get("serial"),
                "error": loop_error,
            }

    finished = _utc_now()
    duration = time.monotonic() - t0
    await cache.update_poll(
        samples,
        started=started,
        finished=finished,
        duration_s=round(duration, 2),
        loop_error=loop_error,
    )

    ok_n = sum(1 for s in samples.values() if s.get("ok"))
    log.info(
        "Poll #%d finished in %.1fs — %d/%d bank(s) OK",
        cache.poll_count, duration, ok_n, len(samples),
    )


async def _poll_loop(cfg: Dict[str, Any]) -> None:
    interval = max(5, int(cfg.get("poll_interval_s", 30)))
    log.info(
        "Background poller started  interval=%ds  banks=%s  config=%s",
        interval,
        list(cfg.get("banks", {})),
        cfg.get("config_path"),
    )
    # First poll immediately so /bms is useful soon after startup.
    while True:
        t0 = time.monotonic()
        try:
            await _poll_once(cfg)
        except Exception as e:
            # Should be rare — _poll_once already guards read_banks.
            log.error("Unhandled poll error: %s: %s", type(e).__name__, e)
        elapsed = time.monotonic() - t0
        sleep_s = max(0.0, interval - elapsed)
        if sleep_s > 0:
            log.debug("Sleeping %.1fs until next poll", sleep_s)
            await asyncio.sleep(sleep_s)


# ── FastAPI app ───────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _config, _poll_task
    _config = load_config(CONFIG_PATH)
    log.info("=" * 60)
    log.info("jkbms_api starting")
    log.info("  config     : %s", _config.get("config_path", CONFIG_PATH))
    log.info("  banks      : %s", list(_config.get("banks", {})))
    log.info("  interval   : %ss", _config.get("poll_interval_s"))
    log.info("  read tout  : %ss", _config.get("read_timeout_s"))
    log.info("  mode       : QUERY-ONLY (0x96/0x97)")
    log.info("=" * 60)

    _poll_task = asyncio.create_task(_poll_loop(_config), name="jkbms-poll")
    try:
        yield
    finally:
        log.info("jkbms_api shutting down — cancelling poller")
        if _poll_task is not None:
            _poll_task.cancel()
            try:
                await _poll_task
            except asyncio.CancelledError:
                pass
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
        # Configured but missing from last poll payload (should not happen).
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
