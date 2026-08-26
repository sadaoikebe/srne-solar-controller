#!/usr/bin/env python3
"""Pure LFP per-cell health statistics (no I/O, no Influx writes).

Reconstructs aligned pack+cell samples, labels rest/charge/discharge/charge-end
regimes, and classifies cells from voltage residuals vs the *bank median*.

This is not a state-of-health percentage. LFP OCV is flat on the plateau;
mid-SoC voltage offset is not capacity fade.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import yaml

JST = timezone(timedelta(hours=9))

REGIMES: Tuple[str, ...] = (
    "unknown",
    "rest",
    "charge",
    "discharge",
    "charge_end",
    "discharge_knee",
)
CLASSES: Tuple[str, ...] = (
    "insufficient_data",
    "adc_or_wiring",
    "capacity_mismatch",
    "high_dcir_or_connection",
    "soc_imbalance_or_ocv",
    "mixed_or_unclear",
    "nominal",
)
CONFIDENCES: Tuple[str, ...] = ("none", "low", "medium", "high")

REQUIRED_PACK_FIELDS: Tuple[str, ...] = (
    "soc",
    "voltage",
    "current",
    "balance_current",
    "balancing",
)
OPTIONAL_PACK_FIELDS: Tuple[str, ...] = (
    "temp1",
    "temp2",
    "mos_temp",
    "charge_mosfet",
    "discharge_mosfet",
    "cell_delta",
    "nominal_ah",
    "remain_ah",
    "soh",
    "cycles",
)

BIT_HAS_REST = 1 << 0
BIT_HAS_LOAD_CHARGE = 1 << 1
BIT_HAS_LOAD_DISCHARGE = 1 << 2
BIT_HAS_CHARGE_END = 1 << 3
BIT_HAS_STEP_DCIR = 1 << 4
BIT_HAS_DISCHARGE_KNEE = 1 << 5
BIT_PERSIST_READY = 1 << 6

_MAD_SCALE = 1.4826
_CELL_KEYS: Tuple[str, ...] = tuple(f"{i:02d}" for i in range(1, 17))


@dataclass(frozen=True)
class HealthConfig:
    sample_interval_s: float = 30.0
    i_rest: float = 0.5
    t_rest_s: float = 600.0
    i_chg: float = 2.0
    i_dsg: float = 2.0
    i_load: float = 5.0
    i_step: float = 8.0
    i_balance_skip: float = 0.03
    soc_charge_end_enter: float = 98.0
    soc_charge_end_exit: float = 96.0
    v_pack_abs_start: float = 55.2
    v_pack_abs_exit: float = 54.8
    v_cell_charge_end: float = 3.50
    i_cv_max: float = 20.0
    v_cell_knee: float = 3.00
    persist_window_s: float = 86400.0
    n_load_min: int = 20
    n_charge_end_min: int = 10
    n_dcir_min: int = 3
    f_persist: float = 0.5
    r_abs_v: float = 0.015
    r_mad_k: float = 3.0
    r_ir_v: float = 0.010
    dv_min_v: float = 0.001
    step_dt_min_s: float = 30.0
    step_dt_max_s: float = 90.0
    dtemp1_max_c: float = 5.0
    dcir_median_days: float = 7.0
    n_cells: int = 16
    gap_reset_s: float = 45.0

    def with_i_rest(self, i_rest: float) -> "HealthConfig":
        return replace(self, i_rest=float(i_rest))


@dataclass(frozen=True)
class PackSample:
    ts_ns: int
    bank: str
    serial: str
    soc: float
    voltage: float
    current: float
    balance_current: float
    balancing: float
    cells: Tuple[float, ...]
    temp1: Optional[float] = None
    temp2: Optional[float] = None
    mos_temp: Optional[float] = None
    charge_mosfet: Optional[float] = None
    discharge_mosfet: Optional[float] = None
    cell_delta: Optional[float] = None
    nominal_ah: Optional[float] = None
    remain_ah: Optional[float] = None
    soh: Optional[float] = None
    cycles: Optional[float] = None


@dataclass(frozen=True)
class StepEvent:
    t0_ns: int
    t1_ns: int
    d_i: float
    temp1: Optional[float]
    r_mohm: Tuple[Optional[float], ...]


@dataclass
class CellReport:
    cell_id: str
    cell: str
    n: int
    v_min: float
    v_max: float
    v_median: float
    r_median_v: float
    r_p05_v: float
    r_p95_v: float
    r_rest_v: Optional[float]
    r_load_charge_v: Optional[float]
    r_load_discharge_v: Optional[float]
    r_charge_end_v: Optional[float]
    f_min_rest: Optional[float]
    f_max_rest: Optional[float]
    f_min_charge_end: Optional[float]
    f_max_charge_end: Optional[float]
    f_min_load_charge: Optional[float]
    f_max_load_charge: Optional[float]
    f_min_load_discharge: Optional[float]
    f_max_load_discharge: Optional[float]
    dcir_mohm: Optional[float]
    dcir_n: int
    dcir_vs_median: Optional[float]
    class_name: str
    confidence: str
    also_high_dcir: bool
    data_flags: int


@dataclass
class BankReport:
    bank: str
    serial: str
    n_samples: int
    n_incomplete_dropped: int
    t_first_ns: int
    t_last_ns: int
    soc_min: Optional[float]
    soc_max: Optional[float]
    bms_soh: Optional[float]
    cycles: Optional[float]
    nominal_ah: Optional[float]
    remain_ah: Optional[float]
    regime_counts: Dict[str, int]
    cell_delta_rest_p95: Optional[float]
    rank_stability: Optional[float]
    sticky_outlier_count: int
    has_discharge_knee: bool
    i_rest_yaml: float
    i_rest_used: float
    i_rest_auto_adjusted: bool
    night_current_abs_p50: Optional[float]
    night_current_abs_p95: Optional[float]
    current_abs_p50: Optional[float]
    current_abs_p95: Optional[float]
    cells: List[CellReport]


def cell_id(bank: str, cell_index: int) -> str:
    return f"{bank.upper()}{int(cell_index):02d}"


def load_config(path: str | Path) -> HealthConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"config {path} is not a mapping")
    allowed = {f.name for f in fields(HealthConfig)}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"unknown config keys in {path}: {sorted(unknown)}")
    return HealthConfig(**{k: raw[k] for k in raw if k in allowed})


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_ts_ns(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    raise TypeError(f"unsupported time type: {type(value)!r}")


def _tick_s(value: Any) -> int:
    """Join key: whole UTC seconds. 30 s samples are unique at this grain."""
    return _as_ts_ns(value) // 1_000_000_000


def quantile(values: Sequence[float], q: float) -> float:
    if not values:
        raise ValueError("quantile of empty sequence")
    if not 0.0 <= q <= 1.0:
        raise ValueError("q must be in [0, 1]")
    ys = sorted(float(v) for v in values)
    if len(ys) == 1:
        return ys[0]
    idx = q * (len(ys) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ys[lo]
    w = idx - lo
    return ys[lo] * (1.0 - w) + ys[hi] * w


def median(values: Sequence[float]) -> float:
    return quantile(values, 0.5)


def mad(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    m = median(values)
    return _MAD_SCALE * median([abs(v - m) for v in values])


def residuals_vs_median(cells: Sequence[float]) -> Tuple[float, Tuple[float, ...], float]:
    """Return (median_V, residuals, MAD of residuals). Never uses the mean."""
    med = median(cells)
    res = tuple(float(v) - med for v in cells)
    return med, res, mad(res)


def ranks(cells: Sequence[float]) -> Tuple[int, ...]:
    """1 = lowest voltage. Ties share the minimum rank of the group."""
    n = len(cells)
    order = sorted(range(n), key=lambda i: (cells[i], i))
    out = [0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and cells[order[j + 1]] == cells[order[i]]:
            j += 1
        r = i + 1
        for k in range(i, j + 1):
            out[order[k]] = r
        i = j + 1
    return tuple(out)


def consecutive_kendall_tau(rank_series: Sequence[Tuple[int, ...]]) -> float:
    """Mean consecutive-pair Kendall-τ. 16 cells → 120 pairs; ties count in the denominator."""
    if len(rank_series) < 2:
        return 0.0
    taus: List[float] = []
    for a, b in zip(rank_series, rank_series[1:]):
        n = len(a)
        n_pairs = n * (n - 1) // 2
        if n_pairs == 0 or len(b) != n:
            continue
        c = d = 0
        for i in range(n):
            for j in range(i + 1, n):
                s1 = a[i] - a[j]
                s2 = b[i] - b[j]
                prod = s1 * s2
                if prod > 0:
                    c += 1
                elif prod < 0:
                    d += 1
        taus.append((c - d) / n_pairs)
    return sum(taus) / len(taus) if taus else 0.0


def samples_from_influx_rows(
    pack_rows: Sequence[Mapping[str, Any]],
    cell_rows: Sequence[Mapping[str, Any]],
    *,
    n_cells: int = 16,
) -> List[PackSample]:
    """Inner-join pack and cells on (UTC second, bank). Drop incomplete ticks."""
    keys = tuple(f"{i:02d}" for i in range(1, n_cells + 1))

    packs: Dict[Tuple[int, str], Mapping[str, Any]] = {}
    for row in pack_rows:
        bank = str(row.get("bank") or "")
        if not bank:
            continue
        try:
            tick = _tick_s(row["_time"] if "_time" in row else row.get("ts_ns"))
        except (KeyError, TypeError):
            continue
        packs[(tick, bank)] = row

    cells_by: Dict[Tuple[int, str], Mapping[str, Any]] = {}
    for row in cell_rows:
        bank = str(row.get("bank") or "")
        if not bank:
            continue
        try:
            tick = _tick_s(row["_time"] if "_time" in row else row.get("ts_ns"))
        except (KeyError, TypeError):
            continue
        cells_by[(tick, bank)] = row

    out: List[PackSample] = []
    for key in sorted(set(packs) & set(cells_by)):
        prow = packs[key]
        crow = cells_by[key]
        required: Dict[str, float] = {}
        ok = True
        for name in REQUIRED_PACK_FIELDS:
            val = _as_float(prow.get(name))
            if val is None:
                ok = False
                break
            required[name] = val
        if not ok:
            continue
        voltages: List[float] = []
        for k in keys:
            val = _as_float(crow.get(k))
            if val is None:
                voltages = []
                break
            voltages.append(val)
        if len(voltages) != n_cells:
            continue
        tick, bank = key
        serial = str(prow.get("serial") or bank)
        ts_ns = _as_ts_ns(prow["_time"] if "_time" in prow else prow.get("ts_ns", tick * 1_000_000_000))
        opt = {name: _as_float(prow.get(name)) for name in OPTIONAL_PACK_FIELDS}
        out.append(
            PackSample(
                ts_ns=ts_ns,
                bank=bank,
                serial=serial,
                soc=required["soc"],
                voltage=required["voltage"],
                current=required["current"],
                balance_current=required["balance_current"],
                balancing=required["balancing"],
                cells=tuple(voltages),
                temp1=opt["temp1"],
                temp2=opt["temp2"],
                mos_temp=opt["mos_temp"],
                charge_mosfet=opt["charge_mosfet"],
                discharge_mosfet=opt["discharge_mosfet"],
                cell_delta=opt["cell_delta"],
                nominal_ah=opt["nominal_ah"],
                remain_ah=opt["remain_ah"],
                soh=opt["soh"],
                cycles=opt["cycles"],
            )
        )
    return out


def _enter_charge_end(sample: PackSample, cfg: HealthConfig) -> bool:
    if not (0.0 <= sample.current <= cfg.i_cv_max):
        return False
    if sample.voltage < cfg.v_pack_abs_start:
        return False
    return sample.soc >= cfg.soc_charge_end_enter or max(sample.cells) >= cfg.v_cell_charge_end


def _exit_charge_end(sample: PackSample, cfg: HealthConfig) -> bool:
    return (
        sample.current > cfg.i_cv_max
        or sample.current <= -cfg.i_dsg
        or sample.soc <= cfg.soc_charge_end_exit
        or sample.voltage < cfg.v_pack_abs_exit
    )


def _n_rest_samples(cfg: HealthConfig) -> int:
    return max(1, int(round(cfg.t_rest_s / cfg.sample_interval_s)))


def label_regimes(samples: Sequence[PackSample], cfg: HealthConfig) -> List[str]:
    n_rest = _n_rest_samples(cfg)
    gap_ns = int(cfg.gap_reset_s * 1_000_000_000)
    out: List[str] = []
    prev = "unknown"
    rest_run = 0
    prev_ts: Optional[int] = None

    for sample in samples:
        if prev_ts is not None and sample.ts_ns - prev_ts > gap_ns:
            rest_run = 0
        if abs(sample.current) < cfg.i_rest:
            rest_run += 1
        else:
            rest_run = 0
        rest_ready = rest_run >= n_rest
        prev = _next_regime(prev, sample, rest_ready, cfg)
        out.append(prev)
        prev_ts = sample.ts_ns
    return out


def _next_regime(
    prev: str,
    sample: PackSample,
    rest_ready: bool,
    cfg: HealthConfig,
) -> str:
    current = sample.current

    if prev == "charge_end" and not _exit_charge_end(sample, cfg) and 0.0 <= current <= cfg.i_cv_max:
        return "charge_end"

    if current > cfg.i_cv_max and current >= cfg.i_chg:
        return "charge"

    if current >= cfg.i_chg:
        if _enter_charge_end(sample, cfg):
            return "charge_end"
        return "charge"

    if current <= -cfg.i_dsg:
        if min(sample.cells) <= cfg.v_cell_knee:
            return "discharge_knee"
        return "discharge"

    if rest_ready:
        return "rest"

    if prev in ("charge", "discharge", "charge_end", "discharge_knee"):
        return prev
    return "unknown"


def _balancing(sample: PackSample, cfg: HealthConfig) -> bool:
    return sample.balancing != 0.0 or abs(sample.balance_current) > cfg.i_balance_skip


def detect_steps(
    samples: Sequence[PackSample],
    cfg: HealthConfig,
    regimes: Optional[Sequence[str]] = None,
) -> List[StepEvent]:
    if not samples:
        return []
    if regimes is None:
        regimes = label_regimes(samples, cfg)
    if len(regimes) != len(samples):
        raise ValueError("regimes length must match samples")

    events: List[StepEvent] = []
    allowed = {"charge", "discharge"}
    n = len(samples)
    for i in range(n - 1):
        a, b = samples[i], samples[i + 1]
        dt_s = (b.ts_ns - a.ts_ns) / 1e9
        if dt_s < cfg.step_dt_min_s or dt_s > cfg.step_dt_max_s:
            continue
        if regimes[i] not in allowed or regimes[i + 1] not in allowed:
            continue
        if _balancing(a, cfg) or _balancing(b, cfg):
            continue
        if abs(a.current) < cfg.i_load or abs(b.current) < cfg.i_load:
            continue
        if a.current * b.current <= 0:
            continue
        d_i = b.current - a.current
        if abs(d_i) < cfg.i_step:
            continue
        r_cells: List[Optional[float]] = []
        for va, vb in zip(a.cells, b.cells):
            d_v = vb - va
            if abs(d_v) < cfg.dv_min_v:
                r_cells.append(None)
            else:
                r_cells.append((d_v / d_i) * 1000.0)
        if not any(r is not None for r in r_cells):
            continue
        t1 = a.temp1 if a.temp1 is not None else b.temp1
        events.append(
            StepEvent(
                t0_ns=a.ts_ns,
                t1_ns=b.ts_ns,
                d_i=d_i,
                temp1=t1,
                r_mohm=tuple(r_cells),
            )
        )
    return events


def persistence(
    ranks_by_t: Sequence[Tuple[int, ...]],
    regime_mask: Sequence[bool],
) -> Tuple[Tuple[float, ...], Tuple[float, ...]]:
    if not ranks_by_t:
        return tuple(), tuple()
    n = len(ranks_by_t[0])
    n_win = 0
    n_min = [0] * n
    n_max = [0] * n
    for ranks_t, use in zip(ranks_by_t, regime_mask):
        if not use:
            continue
        n_win += 1
        lo = min(ranks_t)
        hi = max(ranks_t)
        for i, r in enumerate(ranks_t):
            if r == lo:
                n_min[i] += 1
            if r == hi:
                n_max[i] += 1
    if n_win == 0:
        z = tuple(0.0 for _ in range(n))
        return z, z
    return (
        tuple(c / n_win for c in n_min),
        tuple(c / n_win for c in n_max),
    )


def _large(x: Optional[float], mad_rest: float, cfg: HealthConfig) -> bool:
    if x is None:
        return False
    thresh = cfg.r_abs_v if mad_rest < 0.001 else max(cfg.r_abs_v, cfg.r_mad_k * mad_rest)
    return abs(x) > thresh


def classify_cell(
    *,
    r_rest: Optional[float],
    r_lc: Optional[float],
    r_ld: Optional[float],
    r_ce: Optional[float],
    mad_rest: float,
    has_rest: bool,
    has_load_charge: bool,
    has_load_discharge: bool,
    has_charge_end: bool,
    has_step_dcir: bool,
    persist_ready: bool,
    f_max_ce: float,
    f_min_ce: float,
    has_discharge_knee: bool = False,
    cfg: HealthConfig,
) -> Tuple[str, str, int, bool]:
    flags = 0
    if has_rest:
        flags |= BIT_HAS_REST
    if has_load_charge:
        flags |= BIT_HAS_LOAD_CHARGE
    if has_load_discharge:
        flags |= BIT_HAS_LOAD_DISCHARGE
    if has_charge_end:
        flags |= BIT_HAS_CHARGE_END
    if has_step_dcir:
        flags |= BIT_HAS_STEP_DCIR
    if has_discharge_knee:
        flags |= BIT_HAS_DISCHARGE_KNEE
    if persist_ready:
        flags |= BIT_PERSIST_READY

    if not has_rest:
        return "insufficient_data", "none", flags, False

    ir_chg = has_load_charge and r_lc is not None and r_rest is not None and (
        (r_lc - r_rest) > max(cfg.r_ir_v, 2.0 * mad_rest)
    )
    ir_dsg = has_load_discharge and r_ld is not None and r_rest is not None and (
        (r_rest - r_ld) > max(cfg.r_ir_v, 2.0 * mad_rest)
    )
    i_dep = bool(ir_chg or ir_dsg)
    also_high_dcir = i_dep

    if has_load_charge and has_load_discharge:
        load_large = _large(r_lc, mad_rest, cfg) and _large(r_ld, mad_rest, cfg)
    elif has_load_charge:
        load_large = _large(r_lc, mad_rest, cfg)
    elif has_load_discharge:
        load_large = _large(r_ld, mad_rest, cfg)
    else:
        load_large = True

    always_on = (
        _large(r_rest, mad_rest, cfg)
        and load_large
        and has_charge_end
        and _large(r_ce, mad_rest, cfg)
        and not i_dep
    )
    ce_sticky_max = has_charge_end and f_max_ce >= cfg.f_persist
    ce_sticky_min = has_charge_end and f_min_ce >= cfg.f_persist

    if always_on:
        class_name = "adc_or_wiring"
    elif ce_sticky_max:
        class_name = "capacity_mismatch"
    elif i_dep:
        class_name = "high_dcir_or_connection"
    elif _large(r_rest, mad_rest, cfg) and not i_dep:
        class_name = "soc_imbalance_or_ocv"
    elif (
        _large(r_rest, mad_rest, cfg)
        or _large(r_lc, mad_rest, cfg)
        or _large(r_ld, mad_rest, cfg)
        or ce_sticky_min
    ):
        class_name = "mixed_or_unclear"
    else:
        class_name = "nominal"

    if not persist_ready:
        confidence = "none"
    elif not (has_load_charge or has_load_discharge or has_charge_end):
        confidence = "low"
    elif has_load_charge and has_load_discharge and has_charge_end and has_step_dcir:
        confidence = "high"
    else:
        confidence = "medium"

    return class_name, confidence, flags, also_high_dcir


def _median_or_none(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return median(values)


def _last_finite(values: Sequence[Optional[float]]) -> Optional[float]:
    for v in reversed(values):
        if v is not None:
            return float(v)
    return None


def night_abs_currents(
    samples: Sequence[PackSample],
    *,
    tz: timezone = JST,
    hour_start: int = 0,
    hour_end: int = 5,
) -> List[float]:
    out: List[float] = []
    for sample in samples:
        dt = datetime.fromtimestamp(sample.ts_ns / 1e9, tz=timezone.utc).astimezone(tz)
        if hour_start <= dt.hour < hour_end:
            out.append(abs(sample.current))
    return out


def suggest_i_rest(samples: Sequence[PackSample], cfg: HealthConfig) -> Optional[float]:
    """Raise yaml i_rest toward overnight |I| p95 + 0.2 A, capped below i_chg."""
    night = night_abs_currents(samples)
    src = night or [abs(s.current) for s in samples]
    if not src:
        return None
    p95 = quantile(src, 0.95)
    suggested = math.ceil((p95 + 0.2) * 10.0) / 10.0
    cap = cfg.i_chg - 0.1
    if cap <= cfg.i_rest:
        return cfg.i_rest
    return min(max(suggested, cfg.i_rest), cap)


def analyze_bank(
    samples: Sequence[PackSample],
    cfg: HealthConfig,
    *,
    n_incomplete_dropped: int = 0,
    auto_i_rest: bool = True,
) -> BankReport:
    if not samples:
        raise ValueError("analyze_bank: no samples")
    bank = samples[0].bank
    serial = samples[0].serial
    i_rest_yaml = cfg.i_rest
    cfg_used = cfg
    adjusted = False

    regimes = label_regimes(samples, cfg_used)
    rest_n = sum(1 for r in regimes if r == "rest")
    if auto_i_rest and rest_n < _n_rest_samples(cfg):
        suggested = suggest_i_rest(samples, cfg)
        if suggested is not None and suggested > cfg.i_rest + 1e-9:
            cfg_used = cfg.with_i_rest(suggested)
            regimes = label_regimes(samples, cfg_used)
            adjusted = True

    n_cells = len(samples[0].cells)
    ranks_by_t: List[Tuple[int, ...]] = []
    residuals_by_t: List[Tuple[float, ...]] = []
    voltages_by_cell: List[List[float]] = [[] for _ in range(n_cells)]
    r_all: List[List[float]] = [[] for _ in range(n_cells)]
    r_rest: List[List[float]] = [[] for _ in range(n_cells)]
    r_lc: List[List[float]] = [[] for _ in range(n_cells)]
    r_ld: List[List[float]] = [[] for _ in range(n_cells)]
    r_ce: List[List[float]] = [[] for _ in range(n_cells)]
    rest_ranks: List[Tuple[int, ...]] = []
    cell_delta_rest: List[float] = []

    for sample, regime in zip(samples, regimes):
        _med, res, _mad = residuals_vs_median(sample.cells)
        rk = ranks(sample.cells)
        ranks_by_t.append(rk)
        residuals_by_t.append(res)
        for i, (v, r) in enumerate(zip(sample.cells, res)):
            voltages_by_cell[i].append(v)
            r_all[i].append(r)
            if regime == "rest":
                r_rest[i].append(r)
            if sample.current >= cfg_used.i_load and not _balancing(sample, cfg_used):
                r_lc[i].append(r)
            if sample.current <= -cfg_used.i_load and not _balancing(sample, cfg_used):
                r_ld[i].append(r)
            if regime == "charge_end":
                r_ce[i].append(r)
        if regime == "rest":
            rest_ranks.append(rk)
            if sample.cell_delta is not None:
                cell_delta_rest.append(sample.cell_delta)
            else:
                cell_delta_rest.append(max(sample.cells) - min(sample.cells))

    regime_counts = {name: 0 for name in REGIMES}
    for r in regimes:
        regime_counts[r] = regime_counts.get(r, 0) + 1

    f_min_rest, f_max_rest = persistence(ranks_by_t, [r == "rest" for r in regimes])
    f_min_ce, f_max_ce = persistence(ranks_by_t, [r == "charge_end" for r in regimes])
    f_min_lc, f_max_lc = persistence(
        ranks_by_t,
        [
            s.current >= cfg_used.i_load and not _balancing(s, cfg_used)
            for s in samples
        ],
    )
    f_min_ld, f_max_ld = persistence(
        ranks_by_t,
        [
            s.current <= -cfg_used.i_load and not _balancing(s, cfg_used)
            for s in samples
        ],
    )

    steps = detect_steps(samples, cfg_used, regimes)
    dcir_lists: List[List[float]] = [[] for _ in range(n_cells)]
    for ev in steps:
        for i, r in enumerate(ev.r_mohm):
            if r is not None:
                dcir_lists[i].append(r)
    dcir_med = [_median_or_none(xs) for xs in dcir_lists]
    present = [x for x in dcir_med if x is not None]
    bank_dcir_med = median(present) if present else None

    span_s = (samples[-1].ts_ns - samples[0].ts_ns) / 1e9
    persist_ready = span_s >= cfg_used.persist_window_s
    n_ce = regime_counts.get("charge_end", 0)
    has_ce = n_ce >= cfg_used.n_charge_end_min
    has_rest = regime_counts.get("rest", 0) >= _n_rest_samples(cfg_used)
    n_lc = max((len(xs) for xs in r_lc), default=0)
    n_ld = max((len(xs) for xs in r_ld), default=0)
    has_lc = n_lc >= cfg_used.n_load_min
    has_ld = n_ld >= cfg_used.n_load_min
    has_knee = regime_counts.get("discharge_knee", 0) > 0

    rest_res_all = [x for xs in r_rest for x in xs]
    mad_rest_bank = mad(rest_res_all) if rest_res_all else 0.0

    cells: List[CellReport] = []
    for i in range(n_cells):
        r_rest_v = _median_or_none(r_rest[i])
        r_lc_v = _median_or_none(r_lc[i]) if has_lc else _median_or_none(r_lc[i])
        r_ld_v = _median_or_none(r_ld[i]) if has_ld else _median_or_none(r_ld[i])
        r_ce_v = _median_or_none(r_ce[i]) if r_ce[i] else None
        cell_mad = mad(r_rest[i]) if len(r_rest[i]) >= 5 else mad_rest_bank
        dcir_n = len(dcir_lists[i])
        dcir_v = dcir_med[i] if dcir_n >= cfg_used.n_dcir_min else None
        dcir_vs = (
            dcir_v / bank_dcir_med
            if dcir_v is not None and bank_dcir_med not in (None, 0.0)
            else None
        )
        class_name, confidence, flags, also = classify_cell(
            r_rest=r_rest_v,
            r_lc=r_lc_v if has_lc else None,
            r_ld=r_ld_v if has_ld else None,
            r_ce=r_ce_v if has_ce else None,
            mad_rest=cell_mad,
            has_rest=has_rest,
            has_load_charge=has_lc,
            has_load_discharge=has_ld,
            has_charge_end=has_ce,
            has_step_dcir=dcir_n >= cfg_used.n_dcir_min,
            persist_ready=persist_ready,
            f_max_ce=f_max_ce[i] if f_max_ce else 0.0,
            f_min_ce=f_min_ce[i] if f_min_ce else 0.0,
            has_discharge_knee=has_knee,
            cfg=cfg_used,
        )
        cells.append(
            CellReport(
                cell_id=cell_id(bank, i + 1),
                cell=f"{i + 1:02d}",
                n=len(voltages_by_cell[i]),
                v_min=min(voltages_by_cell[i]),
                v_max=max(voltages_by_cell[i]),
                v_median=median(voltages_by_cell[i]),
                r_median_v=median(r_all[i]),
                r_p05_v=quantile(r_all[i], 0.05),
                r_p95_v=quantile(r_all[i], 0.95),
                r_rest_v=r_rest_v,
                r_load_charge_v=r_lc_v,
                r_load_discharge_v=r_ld_v,
                r_charge_end_v=r_ce_v,
                f_min_rest=f_min_rest[i] if f_min_rest else None,
                f_max_rest=f_max_rest[i] if f_max_rest else None,
                f_min_charge_end=f_min_ce[i] if f_min_ce else None,
                f_max_charge_end=f_max_ce[i] if f_max_ce else None,
                f_min_load_charge=f_min_lc[i] if f_min_lc else None,
                f_max_load_charge=f_max_lc[i] if f_max_lc else None,
                f_min_load_discharge=f_min_ld[i] if f_min_ld else None,
                f_max_load_discharge=f_max_ld[i] if f_max_ld else None,
                dcir_mohm=dcir_v,
                dcir_n=dcir_n,
                dcir_vs_median=dcir_vs,
                class_name=class_name,
                confidence=confidence,
                also_high_dcir=also,
                data_flags=flags,
            )
        )

    sticky = 0
    for c in cells:
        if (c.f_min_rest or 0.0) >= cfg_used.f_persist or (c.f_max_rest or 0.0) >= cfg_used.f_persist:
            sticky += 1
        elif has_ce and (
            (c.f_min_charge_end or 0.0) >= cfg_used.f_persist
            or (c.f_max_charge_end or 0.0) >= cfg_used.f_persist
        ):
            sticky += 1

    abs_i = [abs(s.current) for s in samples]
    night = night_abs_currents(samples)
    return BankReport(
        bank=bank,
        serial=serial,
        n_samples=len(samples),
        n_incomplete_dropped=n_incomplete_dropped,
        t_first_ns=samples[0].ts_ns,
        t_last_ns=samples[-1].ts_ns,
        soc_min=min(s.soc for s in samples),
        soc_max=max(s.soc for s in samples),
        bms_soh=_last_finite([s.soh for s in samples]),
        cycles=_last_finite([s.cycles for s in samples]),
        nominal_ah=_last_finite([s.nominal_ah for s in samples]),
        remain_ah=_last_finite([s.remain_ah for s in samples]),
        regime_counts=regime_counts,
        cell_delta_rest_p95=quantile(cell_delta_rest, 0.95) if cell_delta_rest else None,
        rank_stability=consecutive_kendall_tau(rest_ranks) if len(rest_ranks) >= 2 else None,
        sticky_outlier_count=sticky,
        has_discharge_knee=has_knee,
        i_rest_yaml=i_rest_yaml,
        i_rest_used=cfg_used.i_rest,
        i_rest_auto_adjusted=adjusted,
        night_current_abs_p50=quantile(night, 0.50) if night else None,
        night_current_abs_p95=quantile(night, 0.95) if night else None,
        current_abs_p50=quantile(abs_i, 0.50) if abs_i else None,
        current_abs_p95=quantile(abs_i, 0.95) if abs_i else None,
        cells=cells,
    )


def bank_report_to_dict(report: BankReport) -> Dict[str, Any]:
    d = asdict(report)
    return d


def fmt_jst(ts_ns: int) -> str:
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).astimezone(JST)
    return dt.strftime("%Y-%m-%d %H:%M:%S JST")


def _mv(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v * 1000.0:+.1f}"


def _pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{100.0 * v:.0f}%"


def _f(v: Optional[float], nd: int = 3) -> str:
    if v is None:
        return "—"
    return f"{v:.{nd}f}"


def format_markdown(reports: Sequence[BankReport], *, cfg: HealthConfig) -> str:
    lines: List[str] = []
    lines.append("# JK-BMS per-cell statistics (LFP)")
    lines.append("")
    lines.append(
        "Read-only report from measurement `jkbms`. "
        "**Not a state-of-health %.** LFP voltage is flat from ~10–90% SoC; "
        "a mid-plateau offset is usually imbalance/ADC/IR, not missing amp-hours. "
        "Canonical SOH_Q = Q/Q_nom needs a discharge knee (~3.00 V/cell), "
        "which this house has not run."
    )
    lines.append("")
    lines.append(
        "Residuals are vs the **bank median** (mV). "
        "Charge current is positive, discharge negative. "
        f"Charge-end uses pack V ≥ {cfg.v_pack_abs_start:.1f} V and SoC ≥ {cfg.soc_charge_end_enter:.0f}% "
        f"(or a cell ≥ {cfg.v_cell_charge_end:.2f} V), with I ≤ {cfg.i_cv_max:.0f} A."
    )
    lines.append("")

    for report in reports:
        bid = report.bank.upper()
        lines.append(f"## Bank {bid}  (serial {report.serial})")
        lines.append("")
        lines.append(f"- Samples: **{report.n_samples}**  ({fmt_jst(report.t_first_ns)} → {fmt_jst(report.t_last_ns)})")
        if report.n_incomplete_dropped:
            lines.append(f"- Incomplete ticks dropped (missing cell or pack field): {report.n_incomplete_dropped}")
        lines.append(
            f"- Pack SoC range: {_f(report.soc_min, 0)}–{_f(report.soc_max, 0)} %   "
            f"BMS pack SoH (heuristic): {_f(report.bms_soh, 0)} %   "
            f"cycles: {_f(report.cycles, 0)}   "
            f"nominal_ah: {_f(report.nominal_ah, 1)}"
        )
        lines.append(
            f"- Rest threshold: yaml `{report.i_rest_yaml:.2f}` A, used **{report.i_rest_used:.2f} A**"
            + (" (auto-raised from overnight |I| histogram)" if report.i_rest_auto_adjusted else "")
        )
        lines.append(
            f"- |I| overall p50/p95: {_f(report.current_abs_p50, 2)} / {_f(report.current_abs_p95, 2)} A; "
            f"overnight 00–05 JST p50/p95: {_f(report.night_current_abs_p50, 2)} / {_f(report.night_current_abs_p95, 2)} A"
        )
        rc = report.regime_counts
        n = max(report.n_samples, 1)
        lines.append(
            "- Regimes: "
            + ", ".join(f"{name} {100.0 * rc.get(name, 0) / n:.1f}%" for name in REGIMES if rc.get(name, 0))
        )
        lines.append(
            f"- Rest cell Δ p95: {_f(report.cell_delta_rest_p95, 3)} V   "
            f"rest rank-stability τ: {_f(report.rank_stability, 3)}   "
            f"sticky outliers: {report.sticky_outlier_count}   "
            f"discharge knee seen: {'yes' if report.has_discharge_knee else 'no'}"
        )
        lines.append("")
        lines.append(
            "| Cell | Vmed | r_all mV | r_rest | r_chg | r_dsg | r_CE | "
            "min@rest | max@CE | class | conf |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |")
        for c in report.cells:
            note = c.class_name
            if c.also_high_dcir and c.class_name != "high_dcir_or_connection":
                note += "+IR"
            lines.append(
                f"| {c.cell_id} | {c.v_median:.3f} | {_mv(c.r_median_v)} | "
                f"{_mv(c.r_rest_v)} | {_mv(c.r_load_charge_v)} | {_mv(c.r_load_discharge_v)} | "
                f"{_mv(c.r_charge_end_v)} | {_pct(c.f_min_rest)} | {_pct(c.f_max_charge_end)} | "
                f"{note} | {c.confidence} |"
            )
        lines.append("")
        lines.append("Voltage range and 30 s-step ΔV/ΔI proxy (interconnect + polarization, **not SoH%**):")
        lines.append("")
        lines.append("| Cell | Vmin | Vmax | r p05–p95 mV | DCIR mΩ | vs median | n_steps |")
        lines.append("| --- | ---: | ---: | --- | ---: | ---: | ---: |")
        for c in report.cells:
            band = f"{_mv(c.r_p05_v)}…{_mv(c.r_p95_v)}"
            dcir = "—" if c.dcir_mohm is None else f"{c.dcir_mohm:.2f}"
            vs = "—" if c.dcir_vs_median is None else f"{c.dcir_vs_median:.2f}"
            lines.append(
                f"| {c.cell_id} | {c.v_min:.3f} | {c.v_max:.3f} | {band} | {dcir} | {vs} | {c.dcir_n} |"
            )
        lines.append("")
        standouts = [c for c in report.cells if c.class_name not in ("nominal", "insufficient_data")]
        if standouts:
            lines.append("Standouts:")
            for c in standouts:
                bits = [
                    f"rest residual {_mv(c.r_rest_v)} mV",
                    f"all-sample median residual {_mv(c.r_median_v)} mV",
                ]
                if c.f_min_rest and c.f_min_rest >= 0.3:
                    bits.append(f"lowest at rest {_pct(c.f_min_rest)} of rest samples")
                if c.f_max_rest and c.f_max_rest >= 0.3:
                    bits.append(f"highest at rest {_pct(c.f_max_rest)} of rest samples")
                if c.f_max_charge_end and c.f_max_charge_end >= 0.3:
                    bits.append(f"highest at charge-end {_pct(c.f_max_charge_end)}")
                lines.append(f"- **{c.cell_id}** `{c.class_name}` ({c.confidence}): " + "; ".join(bits) + ".")
            lines.append("")
        else:
            lines.append("No cell left the `nominal` / `insufficient_data` classes in this window.")
            lines.append("")

    lines.append("## How to read the classes")
    lines.append("")
    lines.append("| Class | Meaning |")
    lines.append("| --- | --- |")
    lines.append("| `nominal` | Residuals small vs the 15 mV / 3×MAD gate. |")
    lines.append("| `soc_imbalance_or_ocv` | Offset at rest, little current dependence. Usually balance, not fade. |")
    lines.append("| `high_dcir_or_connection` | Extra residual under load (higher on charge, lower on discharge). |")
    lines.append("| `capacity_mismatch` | Persistently highest at charge-end (fills first). Qualitative until a 3.00 V knee. |")
    lines.append("| `adc_or_wiring` | Same offset at rest, load, **and** charge-end. Check sense leads before blaming the cell. |")
    lines.append("| `insufficient_data` | Not enough settled rest in this window. |")
    lines.append("")
    lines.append(
        "Bank comparison is relative only: lower rest Δ, fewer sticky min/max identities, "
        "mixed charge-end ranking. Pack BMS `soh` is a crude pack integer, not per-cell truth."
    )
    lines.append("")
    return "\n".join(lines)


def count_candidate_ticks(
    pack_rows: Sequence[Mapping[str, Any]],
    cell_rows: Sequence[Mapping[str, Any]],
) -> Tuple[int, int]:
    """Return (n_complete_samples, n_incomplete_keys) for logging."""
    samples = samples_from_influx_rows(pack_rows, cell_rows)
    pack_keys = set()
    for row in pack_rows:
        try:
            pack_keys.add((_tick_s(row["_time"] if "_time" in row else row.get("ts_ns")), str(row.get("bank"))))
        except (KeyError, TypeError):
            continue
    cell_keys = set()
    for row in cell_rows:
        try:
            cell_keys.add((_tick_s(row["_time"] if "_time" in row else row.get("ts_ns")), str(row.get("bank"))))
        except (KeyError, TypeError):
            continue
    union = pack_keys | cell_keys
    return len(samples), max(0, len(union) - len(samples))
