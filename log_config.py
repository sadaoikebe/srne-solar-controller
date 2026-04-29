"""Centralised logging configuration.

Every script calls ``get_logger(name)`` to obtain a consistently formatted
logger whose verbosity is governed by the ``LOG_LEVEL`` environment variable.

Levels
------
  DEBUG    — Detailed internal steps: raw register values, SoC estimator
             ticks, Modbus block reads, point-by-point transforms, timings.
             Use during initial commissioning or troubleshooting.
  INFO     — Normal production output: control-state transitions, InfluxDB
             write summaries, daily target decisions, startup milestones.
             (default)
  WARNING  — Unexpected-but-recoverable events: fetch failures, fallback
             values used, auth bypass active.
  ERROR    — Failures that affect functionality or safety.

Each named logger gets its own ``StreamHandler`` so that formatting is
consistent whether a script runs standalone or inside uvicorn.  Messages
do NOT propagate to the root logger, which prevents double-printing when
uvicorn is also attached to the root handler.

Usage
-----
    from log_config import get_logger
    log = get_logger("battery_controller")
    log.info("Starting up")
"""
from __future__ import annotations

import logging
import os
import sys

_FMT     = "%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"

# Track which top-level logger families have already been configured
_configured: set[str] = set()


def _resolve_level() -> int:
    """Parse the LOG_LEVEL env var and return the corresponding int constant."""
    raw = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, raw, None)
    if not isinstance(level, int):
        # Bad value — emit a one-shot warning then fall back to INFO
        logging.warning("Invalid LOG_LEVEL=%r — falling back to INFO", raw)
        return logging.INFO
    return level


def get_logger(name: str) -> logging.Logger:
    """Return a named, consistently-formatted logger.

    *name* is typically the module name::

        log = get_logger("battery_controller")

    The first call for a given top-level name attaches a ``StreamHandler``
    to that logger family (idempotent on subsequent calls).  The log level
    is re-read from the environment on every call so that a process-level
    reload (e.g. ``os.environ["LOG_LEVEL"] = "DEBUG"``) takes effect without
    restarting.
    """
    root_name = name.split(".")[0]   # e.g. "modbus_api" from "modbus_api.helpers"
    pkg = logging.getLogger(root_name)

    if root_name not in _configured:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATEFMT))
        pkg.addHandler(handler)
        pkg.propagate = False   # isolate from uvicorn's root handler
        _configured.add(root_name)

    level = _resolve_level()
    pkg.setLevel(level)
    child = logging.getLogger(name)
    child.setLevel(level)
    return child
