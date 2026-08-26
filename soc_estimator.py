#!/usr/bin/env python3
"""Shadow pack SoC estimator (observer only — does not steer the inverter).

Modes per bank
--------------
  track            BLE up, JK remain_ah moving → remain_est += Δremain_jk
  coast_jk         BLE up, remain stuck → remain_est += I_jk × Δt
  coast_inverters  both banks BLE down, latch has current → split I_pack
  held             this bank BLE down, the other is live → freeze remain_est
  full_anchor      cell_max ≥ full_cell_v → remain_est = usable_ah
  empty_anchor     cell_min ≤ empty_cell_v → remain_est = 0

No current sensor at all → no Influx write (data gap).

Does not import battery_controller. Query-only HTTP to jkbms_api + modbus_api.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import requests
import yaml

from log_config import get_logger

log = get_logger("soc_estimator")

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "soc_estimator.yaml"
MEASUREMENT = "soc_estimate"

# ── Config / state ────────────────────────────────────────────────────────────


@dataclass
class EstimatorConfig:
    interval_s: int = 10
    usable_ah: Dict[str, float] = field(default_factory=lambda: {"a": 260.0, "b": 280.0})
    full_cell_v: float = 3.59
    empty_cell_v: float = 3.05
    ble_stale_s: float = 25.0
    powmr_stale_s: float = 15.0
    growatt_stale_s: float = 45.0
    move_fraction: float = 0.25
    min_expected_ah: float = 0.01
    persist_path: str = "/app/soc_estimator_state.json"

    @property
    def banks(self) -> List[str]:
        return list(self.usable_ah)


@dataclass
class BankSample:
    ok: bool
    age_s: Optional[float] = None
    remain_ah: Optional[float] = None
    nominal_ah: Optional[float] = None
    current: Optional[float] = None
    cell_min: Optional[float] = None
    cell_max: Optional[float] = None
    soc: Optional[float] = None
    serial: Optional[str] = None


@dataclass
class InverterSnapshot:
    pack_current_a: Optional[float]
    powmr_age_s: Optional[float]
    growatt_age_s: Optional[float]


@dataclass
class BankState:
    remain_est: float
    last_remain_jk: Optional[float] = None
    last_current_jk: float = 0.0
    mode: str = "init"
    initialized: bool = False


@dataclass
class TickResult:
    states: Dict[str, BankState]
    modes: Dict[str, str]
    shares: Dict[str, float]
    write: bool
    soc_pack: Optional[float]
    pack_current_a: Optional[float]


# ── Pure helpers ──────────────────────────────────────────────────────────────


def load_config(path: Path | str | None = None) -> EstimatorConfig:
    cfg_path = Path(path) if path else Path(os.getenv("SOC_ESTIMATOR_CONFIG", str(DEFAULT_CONFIG_PATH)))
    kwargs: Dict[str, Any] = {}
    if cfg_path.is_file():
        with open(cfg_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"Invalid config root in {cfg_path}")
        kwargs = {k: raw[k] for k in EstimatorConfig.__dataclass_fields__ if k in raw}
        if "usable_ah" in kwargs:
            kwargs["usable_ah"] = {str(k): float(v) for k, v in kwargs["usable_ah"].items()}
    cfg = EstimatorConfig(**kwargs)
    log.info("Loaded estimator config %s  banks=%s", cfg_path, cfg.banks)
    return cfg


def remain_moving(
    delta_remain: float,
    current_a: float,
    dt_s: float,
    *,
    move_fraction: float,
    min_expected_ah: float,
) -> bool:
    """True if remain_ah changed enough vs I×Δt, or the tick is rest-sized."""
    expected = current_a * dt_s / 3600.0
    if abs(expected) < min_expected_ah:
        return True
    if expected > 0:
        return delta_remain >= move_fraction * expected
    return delta_remain <= move_fraction * expected


def ble_fresh(sample: BankSample, stale_s: float) -> bool:
    if not sample.ok or sample.remain_ah is None or sample.current is None:
        return False
    if sample.age_s is None:
        return True
    return sample.age_s <= stale_s


def inverter_fresh(inv: Optional[InverterSnapshot], cfg: EstimatorConfig) -> bool:
    if inv is None or inv.pack_current_a is None:
        return False
    if inv.powmr_age_s is not None and inv.powmr_age_s > cfg.powmr_stale_s:
        return False
    if inv.growatt_age_s is not None and inv.growatt_age_s > cfg.growatt_stale_s:
        return False
    # Growatt side is required so we do not invent idle.
    if inv.growatt_age_s is None:
        return False
    return True


def cold_start_remain(remain_jk: float, nominal_jk: Optional[float], usable: float) -> float:
    nom = float(nominal_jk) if nominal_jk and nominal_jk > 1.0 else usable
    remain = remain_jk + max(0.0, usable - nom)
    return clamp(remain, 0.0, usable)


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def parse_bank_sample(raw: Mapping[str, Any] | None) -> BankSample:
    if not isinstance(raw, Mapping):
        return BankSample(ok=False)
    def _f(key: str) -> Optional[float]:
        v = raw.get(key)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    return BankSample(
        ok=bool(raw.get("ok")),
        age_s=_f("age_s"),
        remain_ah=_f("remain_ah"),
        nominal_ah=_f("nominal_ah"),
        current=_f("current"),
        cell_min=_f("cell_min"),
        cell_max=_f("cell_max"),
        soc=_f("soc"),
        serial=str(raw["serial"]) if raw.get("serial") else None,
    )


def parse_inverter(raw: Mapping[str, Any] | None) -> Optional[InverterSnapshot]:
    if not isinstance(raw, Mapping):
        return None
    pack = raw.get("pack_current_a")
    try:
        pack_f = float(pack) if pack is not None else None
    except (TypeError, ValueError):
        pack_f = None
    powmr = raw.get("powmr") if isinstance(raw.get("powmr"), Mapping) else None
    growatt = raw.get("growatt") if isinstance(raw.get("growatt"), Mapping) else None

    def _age(side: Optional[Mapping[str, Any]]) -> Optional[float]:
        if side is None:
            return None
        v = side.get("age_s")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    return InverterSnapshot(
        pack_current_a=pack_f,
        powmr_age_s=_age(powmr),
        growatt_age_s=_age(growatt),
    )


def _relock(state: BankState, remain_jk: float) -> BankState:
    return replace(state, last_remain_jk=remain_jk)


def step_live_bank(
    state: BankState,
    sample: BankSample,
    *,
    usable: float,
    cfg: EstimatorConfig,
    dt_s: float,
    returning: bool,
) -> BankState:
    remain_jk = float(sample.remain_ah)
    current = float(sample.current)
    st = replace(state, last_current_jk=current)
    skip_delta = False

    if not st.initialized:
        st = replace(
            st,
            remain_est=cold_start_remain(remain_jk, sample.nominal_ah, usable),
            last_remain_jk=remain_jk,
            initialized=True,
            mode="track",
        )
        skip_delta = True
    elif returning or st.last_remain_jk is None:
        st = _relock(st, remain_jk)
        skip_delta = True

    cell_max = sample.cell_max
    cell_min = sample.cell_min
    if cell_max is not None and cell_max >= cfg.full_cell_v:
        return replace(st, remain_est=usable, last_remain_jk=remain_jk, mode="full_anchor")
    if cell_min is not None and cell_min <= cfg.empty_cell_v:
        return replace(st, remain_est=0.0, last_remain_jk=remain_jk, mode="empty_anchor")

    if skip_delta or st.last_remain_jk is None or dt_s <= 0:
        return replace(st, last_remain_jk=remain_jk, mode="track")

    delta = remain_jk - st.last_remain_jk
    if remain_moving(
        delta,
        current,
        dt_s,
        move_fraction=cfg.move_fraction,
        min_expected_ah=cfg.min_expected_ah,
    ):
        remain = clamp(st.remain_est + delta, 0.0, usable)
        return replace(st, remain_est=remain, last_remain_jk=remain_jk, mode="track")

    # Remain stuck — same shunt current still works.
    d_ah = current * dt_s / 3600.0
    remain = clamp(st.remain_est + d_ah, 0.0, usable)
    return replace(st, remain_est=remain, last_remain_jk=remain_jk, mode="coast_jk")


def update_shares(
    shares: Dict[str, float],
    live: Mapping[str, BankSample],
    banks: List[str],
) -> Dict[str, float]:
    currents = {b: float(live[b].current) for b in live if live[b].current is not None}
    if len(currents) < 2:
        return dict(shares)
    total = sum(currents.values())
    mag = sum(abs(v) for v in currents.values())
    if mag < 0.5:
        return dict(shares)
    # Split by |I| so a charging bank and a tiny trickle still share by magnitude.
    out = dict(shares)
    for b in banks:
        if b in currents:
            out[b] = abs(currents[b]) / mag
    # If total current is reverse of some banks, magnitude share is still the
    # right way to split a pack-level inverter current later.
    return out


def step(
    states: Dict[str, BankState],
    samples: Mapping[str, BankSample],
    inverter: Optional[InverterSnapshot],
    *,
    cfg: EstimatorConfig,
    dt_s: float,
    shares: Optional[Dict[str, float]] = None,
) -> TickResult:
    banks = cfg.banks
    shares = dict(shares) if shares else {b: 1.0 / max(1, len(banks)) for b in banks}
    live = {b: samples[b] for b in banks if b in samples and ble_fresh(samples[b], cfg.ble_stale_s)}
    dead = [b for b in banks if b not in live]
    shares = update_shares(shares, live, banks)

    new_states: Dict[str, BankState] = {}
    modes: Dict[str, str] = {}
    measured = False
    inv_ok = inverter_fresh(inverter, cfg)
    all_dead = len(dead) == len(banks)

    for b in banks:
        usable = float(cfg.usable_ah[b])
        prev = states.get(b) or BankState(remain_est=0.0)
        if b in live:
            returning = prev.mode in ("held", "coast_inverters")
            st = step_live_bank(
                prev,
                live[b],
                usable=usable,
                cfg=cfg,
                dt_s=dt_s,
                returning=returning,
            )
            new_states[b] = st
            modes[b] = st.mode
            measured = True
        elif all_dead and inv_ok and dt_s > 0:
            i_pack = float(inverter.pack_current_a)  # type: ignore[union-attr]
            d_ah = shares.get(b, 1.0 / len(banks)) * i_pack * dt_s / 3600.0
            remain = clamp(prev.remain_est + d_ah, 0.0, usable)
            new_states[b] = replace(prev, remain_est=remain, mode="coast_inverters", initialized=True)
            modes[b] = "coast_inverters"
            measured = True
        else:
            new_states[b] = replace(prev, mode="held")
            modes[b] = "held"

    remain_sum = sum(new_states[b].remain_est for b in banks)
    usable_sum = sum(float(cfg.usable_ah[b]) for b in banks)
    soc_pack = 100.0 * remain_sum / usable_sum if usable_sum > 0 else None
    pack_i = inverter.pack_current_a if inv_ok else None
    return TickResult(
        states=new_states,
        modes=modes,
        shares=shares,
        write=measured,
        soc_pack=soc_pack,
        pack_current_a=pack_i,
    )


# ── Persist ───────────────────────────────────────────────────────────────────


def load_state(path: Path, cfg: EstimatorConfig) -> Tuple[Dict[str, BankState], Dict[str, float]]:
    shares = {b: 1.0 / max(1, len(cfg.banks)) for b in cfg.banks}
    states = {b: BankState(remain_est=0.0) for b in cfg.banks}
    if not path.is_file():
        return states, shares
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("Could not read persist file %s: %s", path, e)
        return states, shares
    for b, blob in (raw.get("banks") or {}).items():
        if b not in cfg.usable_ah or not isinstance(blob, dict):
            continue
        usable = float(cfg.usable_ah[b])
        states[b] = BankState(
            remain_est=clamp(float(blob.get("remain_est", 0.0)), 0.0, usable),
            last_remain_jk=(
                float(blob["last_remain_jk"]) if blob.get("last_remain_jk") is not None else None
            ),
            last_current_jk=float(blob.get("last_current_jk") or 0.0),
            mode=str(blob.get("mode") or "init"),
            initialized=bool(blob.get("initialized", True)),
        )
    if isinstance(raw.get("shares"), dict):
        for b, v in raw["shares"].items():
            if b in shares:
                try:
                    shares[b] = float(v)
                except (TypeError, ValueError):
                    pass
    log.info("Restored estimator state from %s", path)
    return states, shares


def save_state(
    path: Path,
    states: Mapping[str, BankState],
    shares: Mapping[str, float],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "banks": {b: asdict(st) for b, st in states.items()},
        "shares": dict(shares),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


# ── HTTP / Influx (I/O) ───────────────────────────────────────────────────────


def fetch_json(url: str, timeout_s: float) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else None
    except (requests.RequestException, ValueError) as e:
        log.warning("GET %s failed: %s", url, e)
        return None


def samples_from_bms(snapshot: Optional[Mapping[str, Any]], banks: List[str]) -> Dict[str, BankSample]:
    raw_banks = (snapshot or {}).get("banks") if isinstance(snapshot, Mapping) else None
    if not isinstance(raw_banks, Mapping):
        raw_banks = {}
    return {b: parse_bank_sample(raw_banks.get(b)) for b in banks}


def build_points(
    ts_ns: int,
    result: TickResult,
    samples: Mapping[str, BankSample],
    cfg: EstimatorConfig,
) -> List[Any]:
    from influxdb_client import Point

    points: List[Any] = []
    mixed = len(set(result.modes.values())) > 1
    pack_source = "mixed" if mixed else next(iter(result.modes.values()), "unknown")

    def add(bank: str, name: str, unit: str, value: float, source: str) -> None:
        points.append(
            Point(MEASUREMENT)
            .time(ts_ns)
            .tag("bank", bank)
            .tag("name", name)
            .tag("unit", unit)
            .tag("source", source)
            .field("value", float(value))
        )

    for b in cfg.banks:
        st = result.states[b]
        src = result.modes[b]
        sample = samples.get(b) or BankSample(ok=False)
        usable = float(cfg.usable_ah[b])
        add(b, "remain_est", "Ah", st.remain_est, src)
        add(b, "usable_ah", "Ah", usable, src)
        add(b, "soc_est", "%", 100.0 * st.remain_est / usable if usable else 0.0, src)
        if sample.remain_ah is not None:
            add(b, "remain_jk", "Ah", sample.remain_ah, src)
            add(b, "offset_ah", "Ah", st.remain_est - sample.remain_ah, src)
        if sample.current is not None:
            add(b, "current", "A", sample.current, src)
        if sample.soc is not None:
            add(b, "soc_jk", "%", sample.soc, src)
        if sample.cell_min is not None:
            add(b, "cell_min", "V", sample.cell_min, src)
        if sample.cell_max is not None:
            add(b, "cell_max", "V", sample.cell_max, src)

    if result.soc_pack is not None:
        add("pack", "soc", "%", result.soc_pack, pack_source)
        add(
            "pack",
            "remain_est",
            "Ah",
            sum(result.states[b].remain_est for b in cfg.banks),
            pack_source,
        )
        add("pack", "usable_ah", "Ah", sum(cfg.usable_ah.values()), pack_source)
    if result.pack_current_a is not None:
        add("pack", "pack_current", "A", result.pack_current_a, pack_source)
    return points


def wait_until_next_tick(interval_s: int) -> datetime:
    now = datetime.now()
    seconds = now.second + now.microsecond / 1_000_000
    next_off = (int(seconds // interval_s) + 1) * interval_s
    nxt = now.replace(second=0, microsecond=0) + timedelta(seconds=next_off)
    delay = (nxt - now).total_seconds()
    if delay > 0:
        time.sleep(delay)
    return nxt


def main() -> None:
    cfg = load_config()
    persist = Path(os.getenv("SOC_ESTIMATOR_STATE", cfg.persist_path))
    bms_url = os.getenv("JKBMS_API_URL", "http://127.0.0.1:5005/bms")
    currents_url = os.getenv(
        "BATTERY_CURRENTS_URL", "http://127.0.0.1:5004/battery_currents"
    )
    fetch_timeout = float(os.getenv("SOC_ESTIMATOR_FETCH_TIMEOUT_S", "8"))
    interval = max(5, int(cfg.interval_s))

    influx_url = os.environ["INFLUX_URL"]
    influx_token = os.environ["INFLUX_TOKEN"]
    influx_org = os.environ["INFLUX_ORG"]
    influx_bucket = os.environ["INFLUX_BUCKET"]

    import atexit
    import influxdb_client
    from influxdb_client.client.write_api import SYNCHRONOUS

    client = influxdb_client.InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    atexit.register(client.close)

    states, shares = load_state(persist, cfg)
    last_tick: Optional[datetime] = None

    log.info("=" * 60)
    log.info("SoC estimator starting (observer only)")
    log.info("  BMS API      : %s", bms_url)
    log.info("  Currents     : %s", currents_url)
    log.info("  Influx       : %s  bucket=%s  meas=%s", influx_url, influx_bucket, MEASUREMENT)
    log.info("  Interval     : %d s", interval)
    log.info("  Usable Ah    : %s", cfg.usable_ah)
    log.info("  Persist      : %s", persist)
    log.info("=" * 60)

    tick_time = wait_until_next_tick(interval)
    while True:
        now = datetime.now(timezone.utc)
        ts_ns = int(now.timestamp() * 1e9)
        dt_s = (now - last_tick).total_seconds() if last_tick is not None else 0.0
        # Cap dt so a long sleep/pause cannot dump a minute of coast into one step
        # after we have been writing; first tick is init (dt 0).
        if dt_s > interval * 2:
            dt_s = float(interval)

        snapshot = fetch_json(bms_url, fetch_timeout)
        inv_raw = fetch_json(currents_url, fetch_timeout)
        samples = samples_from_bms(snapshot, cfg.banks)
        inverter = parse_inverter(inv_raw)

        result = step(states, samples, inverter, cfg=cfg, dt_s=dt_s, shares=shares)
        shares = result.shares
        states = result.states
        last_tick = now

        mode_s = " ".join(f"{b}={result.modes[b]}" for b in cfg.banks)
        if result.write:
            log.info(
                "tick soc_pack=%s%%  %s  remain=%s",
                f"{result.soc_pack:.1f}" if result.soc_pack is not None else "n/a",
                mode_s,
                {b: round(states[b].remain_est, 2) for b in cfg.banks},
            )
            try:
                save_state(persist, states, shares)
            except OSError as e:
                log.warning("persist failed: %s", e)
            points = build_points(ts_ns, result, samples, cfg)
            try:
                with client.write_api(write_options=SYNCHRONOUS) as w:
                    w.write(bucket=influx_bucket, org=influx_org, record=points)
            except Exception as e:
                log.error("Influx write failed: %s", e)
        else:
            log.warning("no current source (%s) — skipping Influx write", mode_s)

        tick_time = wait_until_next_tick(interval)


if __name__ == "__main__":
    main()
