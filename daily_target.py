"""Nightly charging target.

Cron (22:59): seasonal GHI planner → target_soc / daily_charge_current /
full_charge in targets.json.

PV from hourly JMA MSM GHI (Open-Meteo), not the daily telop.
Apr–Oct: 14-day calibrated GHI → clip-safe target.
Nov–Mar: scale capped at 1.0, HDD/CDD house-load model.
Auto full-charge only when predicted PV is low *and* midday GHI is weak
and ≥ FULL_CHARGE_MIN_DAYS since last SYNC. No maximum-interval force.

On planner failure this process exits 1 *without* writing, so last night's
targets.json stays in effect.

Log levels
----------
  DEBUG  — hourly GHI/PV/load, Influx row counts
  INFO   — season, predicted PV/load, target SoC, full-charge decision, write
  WARNING — SoC already at/above target
  ERROR  — Influx/weather/register/write failure
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

from log_config import get_logger

log = get_logger("daily_target")

JST = ZoneInfo("Asia/Tokyo")

# Kobe grid (JMA 神戸). Override with PLANNER_LAT / PLANNER_LON.
LAT = 34.6903
LON = 135.1955
HIST_START = "2025-07-01"

WH_PER_SOC_CHG = 270.0
WH_PER_SOC_DIS = 255.0
CLIP_CEILING = 95.0
EVENING_FLOOR = 12.0
MIN_TARGET = 10.0
MAX_TARGET = 95.0
MORNING_FLOOR_SUMMER = 15.0
HDD_BASE = 16.0
CDD_BASE = 24.0
EV_EXTRA_W = 2000.0
EV_ABS_W = 3500.0
POOR_PV_KWH = 10.0
MIDDAY_GHI_MEAN_MAX = 180.0
FULL_CHARGE_MIN_DAYS = 14
WINTER_MONTHS = {11, 12, 1, 2, 3}

OM_FORECAST = "https://api.open-meteo.com/v1/forecast"
OM_PREV = "https://previous-runs-api.open-meteo.com/v1/forecast"
OM_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
JMA_FORECAST = "https://www.jma.go.jp/bosai/forecast/data/forecast/280000.json"

_API_PORT: int = int(os.getenv("MODBUS_API_PORT", "5004"))
_API_BASE: str = os.getenv("MODBUS_API_BASE", f"http://modbus_api:{_API_PORT}")
LIMITED_REGISTERS_URL: str = f"{_API_BASE}/limited_registers"
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/targets.json")

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "")

PLANNER_LAT = float(os.getenv("PLANNER_LAT", str(LAT)))
PLANNER_LON = float(os.getenv("PLANNER_LON", str(LON)))

BATTERY_NOMINAL_VOLTAGE_V: float = 53.0
AVERAGE_LOAD_W: float = 1000.0

_FLUX = """
from(bucket: "{bucket}")
  |> range(start: {start}T00:00:00Z, stop: now())
  |> filter(fn: (r) => r._measurement == "modbus" and r._field == "value")
  |> filter(fn: (r) =>
      r.name == "pv1_power" or r.name == "pv2_power" or r.name == "pv3_power" or r.name == "pv4_power" or
      r.name == "load_active_l1" or r.name == "load_active_l2" or
      r.name == "battery_soc"
  )
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false, timeSrc: "_start")
  |> keep(columns: ["_time", "_value", "name"])
  |> group()
  |> pivot(rowKey: ["_time"], columnKey: ["name"], valueColumn: "_value")
"""


@dataclass
class Plan:
    plan_day: date
    season: str
    target_soc: int
    full_charge: bool
    full_charge_reason: str
    clip_safe: float
    evening_cover: float
    morning_floor: float
    pv_hat_kwh: float
    pv_cal_kwh: float
    load_hat_kwh: float
    scale: float
    midday_ghi_mean: float
    days_since_full: int | None
    last_full_charge: str
    jma_code: int | None
    jma_text: str
    jma_issued: str
    hourly: pd.DataFrame = field(repr=False)
    notes: list[str] = field(default_factory=list)


def net_to_dsoc(net_wh: float) -> float:
    return net_wh / WH_PER_SOC_CHG if net_wh >= 0 else net_wh / WH_PER_SOC_DIS


def unconstrained_path(pv_w: np.ndarray, load_w: np.ndarray) -> tuple[float, float, float]:
    running = mx = mn = 0.0
    for h in range(7, 23):
        running += net_to_dsoc(float(pv_w[h] - load_w[h]))
        mx = max(mx, running)
        mn = min(mn, running)
    return mx, mn, running


def clip_safe_target(
    pv_w: np.ndarray,
    load_w: np.ndarray,
    morning_floor: float,
    pv_scale_clip: float = 1.0,
    pv_scale_eve: float = 1.0,
    load_scale_eve: float = 1.0,
) -> tuple[int, float, float, float]:
    mx, _, _ = unconstrained_path(pv_w * pv_scale_clip, load_w)
    _, mn, final = unconstrained_path(pv_w * pv_scale_eve, load_w * load_scale_eve)
    clip_safe = CLIP_CEILING - mx
    evening_cover = max(EVENING_FLOOR - mn, EVENING_FLOOR - final)
    if clip_safe < morning_floor:
        target = clip_safe
    else:
        target = min(clip_safe, max(evening_cover, morning_floor))
    target_i = int(round(float(np.clip(target, MIN_TARGET, MAX_TARGET))))
    return target_i, clip_safe, evening_cover, morning_floor


def morning_floor_from_drain(pv: np.ndarray, load: np.ndarray) -> float:
    drain_wh = sum(max(0.0, float(load[h] - pv[h])) for h in (7, 8))
    return float(np.clip(13.0 + drain_wh / WH_PER_SOC_DIS + 3.0, 15.0, 45.0))


def detect_ev(load_w: pd.Series) -> pd.Series:
    out = pd.Series(False, index=load_w.index)
    for h in range(24):
        s = load_w[load_w.index.hour == h]
        med_past = s.shift(1).rolling(21, min_periods=7).median()
        extra = s > (med_past + EV_EXTRA_W)
        abs_hi = s > EV_ABS_W
        out.loc[s.index] = (extra | abs_hi).fillna(False)
    return out.astype(bool)


def hod_median_asof(series: pd.Series, ts: pd.Timestamp, lookback_days: int = 28) -> np.ndarray:
    window = series.loc[(series.index >= ts - pd.Timedelta(days=lookback_days)) & (series.index < ts)]
    out = np.zeros(24)
    if window.empty:
        return out
    grp = window.groupby(window.index.hour).median()
    fallback = float(window.median())
    for h in range(24):
        out[h] = float(grp.get(h, fallback))
    return out


def fit_k_hod(ghi: pd.Series, pv: pd.Series, soc: pd.Series, asof: pd.Timestamp) -> np.ndarray:
    past = (ghi.index < asof) & (soc.reindex(ghi.index) < 90) & (ghi > 30) & (pv > 0)
    hours = ghi.index.hour
    k = np.full(24, np.nan)
    for h in range(24):
        m = past & (hours == h)
        x, y = ghi[m].to_numpy(), pv[m].to_numpy()
        if x.size >= 8 and float(np.dot(x, x)) >= 1:
            k[h] = float(np.dot(x, y) / np.dot(x, x))
    x, y = ghi[past].to_numpy(), pv[past].to_numpy()
    k_glob = float(np.dot(x, y) / np.dot(x, x)) if x.size >= 24 else 8.0
    return np.where(np.isfinite(k), k, k_glob)


def fit_hdd_cdd(load_kwh: np.ndarray, hdd: np.ndarray, cdd: np.ndarray) -> np.ndarray | None:
    if len(load_kwh) < 20:
        return None
    X = np.column_stack([np.ones(len(load_kwh)), hdd, cdd])
    beta, *_ = np.linalg.lstsq(X, load_kwh, rcond=None)
    return beta if np.all(np.isfinite(beta)) else None


def shape_daily_to_hourly(daily_kwh: float, hod_w: np.ndarray) -> np.ndarray:
    s = float(hod_w.sum())
    if s <= 1:
        return np.full(24, daily_kwh * 1000.0 / 24.0)
    return hod_w / s * daily_kwh * 1000.0


def load_hourly_influx(url: str, token: str, org: str, bucket: str) -> pd.DataFrame:
    from influxdb_client import InfluxDBClient

    flux = _FLUX.format(bucket=bucket, start=HIST_START)
    with InfluxDBClient(url=url, token=token, org=org, timeout=60_000) as client:
        df = client.query_api().query_data_frame(query=flux, org=org)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
    if df is None or df.empty:
        raise RuntimeError("Influx hourly query returned no rows")
    drop = [c for c in df.columns if c in ("result", "table") or str(c).startswith("Unnamed")]
    df = df.drop(columns=drop, errors="ignore")
    df["_time"] = pd.to_datetime(df["_time"], utc=True).dt.tz_convert(JST)
    df = df.set_index("_time").sort_index()
    for c in ("pv1_power", "pv2_power", "pv3_power", "pv4_power",
              "load_active_l1", "load_active_l2", "battery_soc"):
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["pv_w"] = df[["pv1_power", "pv2_power", "pv3_power", "pv4_power"]].fillna(0).sum(axis=1)
    df["load_w"] = df[["load_active_l1", "load_active_l2"]].fillna(0).sum(axis=1)
    df["soc"] = df["battery_soc"].interpolate(limit=3)
    return df


def fetch_open_meteo_forecast(lat: float = LAT, lon: float = LON) -> pd.DataFrame:
    r = requests.get(
        OM_FORECAST,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "shortwave_radiation,temperature_2m,cloud_cover,weather_code,precipitation",
            "timezone": "Asia/Tokyo",
            "forecast_days": 3,
            "models": "jma_msm",
        },
        timeout=20,
    )
    r.raise_for_status()
    h = r.json()["hourly"]
    df = pd.DataFrame(h)
    df["time"] = pd.to_datetime(df["time"]).dt.tz_localize(JST)
    return df.set_index("time")


def fetch_previous_day1_ghi(start: date, end: date, lat: float = LAT, lon: float = LON) -> pd.Series:
    r = requests.get(
        OM_PREV,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "shortwave_radiation_previous_day1",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone": "Asia/Tokyo",
            "models": "jma_msm",
        },
        timeout=30,
    )
    r.raise_for_status()
    h = r.json()["hourly"]
    t = pd.to_datetime(h["time"]).tz_localize(JST)
    return pd.Series(h["shortwave_radiation_previous_day1"], index=t, dtype=float)


def fetch_archive_ghi_temp(end: date, lat: float = LAT, lon: float = LON) -> tuple[pd.Series, pd.Series]:
    r = requests.get(
        OM_ARCHIVE,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "shortwave_radiation,temperature_2m",
            "start_date": HIST_START,
            "end_date": end.isoformat(),
            "timezone": "Asia/Tokyo",
        },
        timeout=60,
    )
    r.raise_for_status()
    h = r.json()["hourly"]
    t = pd.to_datetime(h["time"]).tz_localize(JST)
    ghi = pd.Series(h["shortwave_radiation"], index=t, dtype=float)
    temp = pd.Series(h["temperature_2m"], index=t, dtype=float)
    return ghi, temp


def fetch_jma() -> dict:
    r = requests.get(JMA_FORECAST, timeout=15)
    r.raise_for_status()
    blob = r.json()
    south = next(a for a in blob[0]["timeSeries"][0]["areas"] if a["area"]["name"] == "南部")
    pops_ts = blob[0]["timeSeries"][1]
    pops_south = next(a for a in pops_ts["areas"] if a["area"]["name"] == "南部")
    days = {}
    for t, c, w in zip(
        blob[0]["timeSeries"][0]["timeDefines"],
        south["weatherCodes"],
        south["weathers"],
    ):
        d = datetime.fromisoformat(t).date()
        days[d] = {"code": int(c), "text": w.replace("\u3000", " ")}
    pops = [
        (datetime.fromisoformat(t), None if p == "" else int(p))
        for t, p in zip(pops_ts["timeDefines"], pops_south["pops"])
    ]
    return {"issued": blob[0]["reportDatetime"], "days": days, "pops": pops}


def midday_pops_for(plan_day: date, pops: list) -> dict[str, int | None]:
    out: dict[str, int | None] = {"06-12": None, "12-18": None}
    for t, p in pops:
        local = t.astimezone(JST) if t.tzinfo else t.replace(tzinfo=JST)
        if local.date() != plan_day:
            continue
        if local.hour == 6:
            out["06-12"] = p
        if local.hour == 12:
            out["12-18"] = p
    return out


def compute_plan(
    hist: pd.DataFrame,
    plan_day: date,
    asof: datetime,
    last_full: date | None,
    lat: float = LAT,
    lon: float = LON,
) -> Plan:
    notes: list[str] = []
    fcst = fetch_open_meteo_forecast(lat, lon)
    jma = fetch_jma()
    jma_day = jma["days"].get(plan_day, {})
    pops = midday_pops_for(plan_day, jma["pops"])

    end = asof.date()
    ghi_d1 = fetch_previous_day1_ghi(end - timedelta(days=16), end, lat, lon)
    ghi_obs, temp_obs = fetch_archive_ghi_temp(end, lat, lon)

    hist = hist.copy()
    hist["ghi_fcst"] = ghi_d1.reindex(hist.index)
    hist["ghi_obs"] = ghi_obs.reindex(hist.index).fillna(0.0)
    hist["temp_obs"] = temp_obs.reindex(hist.index)

    asof_ts = pd.Timestamp(asof)
    if asof_ts.tzinfo is None:
        asof_ts = asof_ts.tz_localize(JST)
    else:
        asof_ts = asof_ts.tz_convert(JST)

    k = fit_k_hod(hist["ghi_obs"], hist["pv_w"], hist["soc"], asof_ts)
    hours = hist.index.hour.to_numpy()
    hist["pv_pot"] = np.maximum(hist["pv_w"].to_numpy(), k[hours] * hist["ghi_obs"].to_numpy())
    hist["pv_pot"] = np.where(hist["ghi_obs"] <= 5, hist["pv_w"], hist["pv_pot"])

    hist["is_ev"] = detect_ev(hist["load_w"])
    hist["load_base"] = hist["load_w"].copy()
    for h in range(24):
        m = hist.index.hour == h
        med = hist.loc[m, "load_w"].shift(1).rolling(21, min_periods=7).median()
        ev_h = m & hist["is_ev"]
        hist.loc[ev_h, "load_base"] = med.reindex(hist.index)[ev_h]
    hist["load_base"] = hist["load_base"].fillna(hist["load_w"])

    past = hist.loc[(hist.index >= asof_ts - pd.Timedelta(days=14)) & (hist.index < asof_ts)]
    ghi_fcst_past = past["ghi_fcst"].fillna(past["ghi_obs"])
    past_hat = np.array([k[ts.hour] * float(ghi_fcst_past.loc[ts]) for ts in past.index])
    hat_sum = float(np.nansum(past_hat))
    scale = float(past["pv_pot"].sum() / hat_sum) if hat_sum > 1 else 1.0
    scale = float(np.clip(scale, 0.7, 1.6))

    day_fcst = fcst[fcst.index.date == plan_day]
    if day_fcst.empty:
        raise RuntimeError(f"Open-Meteo forecast has no hours for {plan_day}")
    ghi = np.zeros(24)
    temp = np.full(24, np.nan)
    cloud = np.full(24, np.nan)
    for ts, row in day_fcst.iterrows():
        ghi[ts.hour] = float(row["shortwave_radiation"] or 0.0)
        if pd.notna(row["temperature_2m"]):
            temp[ts.hour] = float(row["temperature_2m"])
        if pd.notna(row["cloud_cover"]):
            cloud[ts.hour] = float(row["cloud_cover"])
    pv_hat = k * ghi
    midday_ghi_mean = float(np.mean(ghi[9:16]))

    load_med = hod_median_asof(hist["load_base"], asof_ts, 28)
    if float(load_med.sum()) == 0:
        load_med = np.full(24, 1000.0)

    winter = plan_day.month in WINTER_MONTHS
    if winter:
        daily_load = hist["load_base"].resample("D").sum() / 1000.0
        daily_t = hist["temp_obs"].resample("D").mean()
        joined = pd.concat([daily_load.rename("load"), daily_t.rename("t")], axis=1)
        joined = joined.loc[
            (joined.index >= asof_ts - pd.Timedelta(days=60)) & (joined.index < asof_ts)
        ].dropna()
        t_tom = float(np.nanmean(temp)) if np.isfinite(np.nanmean(temp)) else 10.0
        beta = fit_hdd_cdd(
            joined["load"].to_numpy(),
            np.maximum(HDD_BASE - joined["t"].to_numpy(), 0.0),
            np.maximum(joined["t"].to_numpy() - CDD_BASE, 0.0),
        ) if len(joined) >= 20 else None
        if beta is not None:
            daily_pred = float(np.clip(
                beta[0]
                + beta[1] * max(0.0, HDD_BASE - t_tom)
                + beta[2] * max(0.0, t_tom - CDD_BASE),
                12.0, 70.0,
            ))
            load_hat = shape_daily_to_hourly(daily_pred, load_med)
            notes.append(
                f"winter load OLS: {beta[0]:.1f} + {beta[1]:.2f}·HDD + {beta[2]:.2f}·CDD "
                f"→ {daily_pred:.1f} kWh (T̄={t_tom:.1f}°C)"
            )
        else:
            load_hat = load_med
            notes.append("winter OLS unavailable — using 28-day hour-of-day median")
        pv_cal = pv_hat * min(scale, 1.0)
        floor = morning_floor_from_drain(pv_cal, load_hat)
        target, clip_safe, evening, floor = clip_safe_target(
            pv_cal, load_hat, morning_floor=floor,
            pv_scale_eve=0.90, load_scale_eve=1.10,
        )
        season = "winter (Nov–Mar): capped PV scale, HDD/CDD load"
    else:
        pv_cal = pv_hat * scale
        load_hat = load_med
        target, clip_safe, evening, floor = clip_safe_target(
            pv_cal, load_hat, morning_floor=MORNING_FLOOR_SUMMER,
        )
        season = "surplus (Apr–Oct): calibrated hourly GHI, clip-safe"

    pv_hat_kwh = float(pv_hat.sum()) / 1000.0
    pv_cal_kwh = float(pv_cal.sum()) / 1000.0
    load_hat_kwh = float(load_hat[7:23].sum()) / 1000.0

    days_since = (asof.date() - last_full).days if last_full else None
    midday_poor = midday_ghi_mean < MIDDAY_GHI_MEAN_MAX
    poor_pv = pv_cal_kwh < POOR_PV_KWH
    auto_ok_days = days_since is None or days_since >= FULL_CHARGE_MIN_DAYS
    full_charge = bool(auto_ok_days and poor_pv and midday_poor)
    if full_charge:
        fc_reason = (
            f"auto: {days_since}d since last SYNC, predicted PV {pv_cal_kwh:.1f} kWh "
            f"< {POOR_PV_KWH}, midday GHI {midday_ghi_mean:.0f} W/m² < {MIDDAY_GHI_MEAN_MAX:.0f}"
        )
    else:
        reasons = []
        if days_since is not None and days_since < FULL_CHARGE_MIN_DAYS:
            reasons.append(f"only {days_since}d since last SYNC (need {FULL_CHARGE_MIN_DAYS})")
        if not poor_pv:
            reasons.append(f"predicted PV {pv_cal_kwh:.1f} kWh is not poor (need < {POOR_PV_KWH})")
        if not midday_poor:
            reasons.append(f"midday GHI {midday_ghi_mean:.0f} W/m² is not weak")
        fc_reason = "no auto full-charge: " + "; ".join(reasons)

    pop_am, pop_pm = pops.get("06-12"), pops.get("12-18")
    if pop_pm is not None or pop_am is not None:
        notes.append(
            f"JMA 6h pops: 06–12={pop_am}%  12–18={pop_pm}% (not used for SoC)"
        )

    hourly = pd.DataFrame({
        "hour": np.arange(24),
        "ghi_wm2": ghi,
        "pv_hat_w": pv_cal,
        "load_hat_w": load_hat,
        "cloud_pct": cloud,
        "temp_c": temp,
    })

    return Plan(
        plan_day=plan_day,
        season=season,
        target_soc=target,
        full_charge=full_charge,
        full_charge_reason=fc_reason,
        clip_safe=clip_safe,
        evening_cover=evening,
        morning_floor=floor,
        pv_hat_kwh=pv_hat_kwh,
        pv_cal_kwh=pv_cal_kwh,
        load_hat_kwh=load_hat_kwh,
        scale=scale,
        midday_ghi_mean=midday_ghi_mean,
        days_since_full=days_since,
        last_full_charge=last_full.isoformat() if last_full else "never",
        jma_code=jma_day.get("code"),
        jma_text=jma_day.get("text", ""),
        jma_issued=jma["issued"],
        hourly=hourly,
        notes=notes,
    )


def fetch_registers() -> dict | None:
    try:
        r = requests.get(LIMITED_REGISTERS_URL, timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        log.error("Register fetch failed: %s", e)
        return None


def _load_last_full_charge() -> date | None:
    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
        s = targets.get("last_full_charge")
        if not s:
            return None
        return date.fromisoformat(s)
    except Exception as e:
        log.debug("Could not load last_full_charge: %s", e)
        return None


def calculate_required_current(
    battery_soc: float,
    target_soc: float,
    charging_hours: float,
) -> float:
    soc_diff = target_soc - battery_soc
    if soc_diff <= 0:
        log.warning(
            "SoC %.1f%% already meets or exceeds target %.0f%% — no charging required",
            battery_soc, target_soc,
        )
        return 0.0
    required_wh = soc_diff * WH_PER_SOC_CHG
    required_amps = (required_wh / charging_hours) / BATTERY_NOMINAL_VOLTAGE_V
    rounded = max(math.ceil(required_amps), 10)
    log.info(
        "Required charge: %.1f%% × %.0f Wh/%% = %.0f Wh  ÷ %.2f h  ÷ %.0f V  "
        "= %.2f A  → %d A (min 10 A)",
        soc_diff, WH_PER_SOC_CHG, required_wh,
        charging_hours, BATTERY_NOMINAL_VOLTAGE_V, required_amps, rounded,
    )
    return float(rounded)


def estimate_soc_at_2259(current_soc: float) -> float:
    now = datetime.now()
    target_time = now.replace(hour=22, minute=59, second=0, microsecond=0)
    if target_time <= now:
        target_time += timedelta(days=1)
    hours_until_2259 = (target_time - now).total_seconds() / 3600.0
    energy_consumed = AVERAGE_LOAD_W * hours_until_2259
    soc_decrease = energy_consumed / WH_PER_SOC_DIS
    estimated = max(0.0, current_soc - soc_decrease)
    log.info(
        "SoC estimate at 22:59: current=%.1f%%  hours=%.2f h  → %.1f%%",
        current_soc, hours_until_2259, estimated,
    )
    return estimated


def calculate_charging_hours(until_time_str: str | None = None) -> float:
    now = datetime.now()
    if until_time_str:
        try:
            hour, minute = map(int, until_time_str.split(":"))
        except ValueError:
            raise ValueError(f"Invalid time format '{until_time_str}'. Use HH:MM.")
        until = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if until <= now:
            until += timedelta(days=1)
    else:
        until = (now + timedelta(days=1)).replace(hour=5, minute=30, second=0, microsecond=0)
        if now.hour < 5 or (now.hour == 5 and now.minute < 30):
            until = now.replace(hour=5, minute=30, second=0, microsecond=0)
    hours = max((until - now).total_seconds() / 3600.0, 0.0)
    log.info(
        "Charging window: now=%s  until=%s  → %.2f h",
        now.strftime("%H:%M"), until.strftime("%Y-%m-%d %H:%M"), hours,
    )
    return hours


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate and write the overnight charging target."
    )
    parser.add_argument(
        "--estimate-start-soc",
        action="store_true",
        help="Estimate SoC at 22:59 from current SoC using average load.",
    )
    parser.add_argument("--start-soc", type=int, help="Use this SoC instead of fetching.")
    parser.add_argument("--target-soc", type=int, help="Override planner target SoC.")
    parser.add_argument("--charging-hours", type=float, help="Override charging window.")
    parser.add_argument(
        "--weather-code",
        type=int,
        help="Ignored (kept for CLI compatibility). Target is GHI-based.",
    )
    parser.add_argument(
        "--until-time",
        help="Charge until this time (HH:MM). Default: 05:30 next morning.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results without writing to targets.json.",
    )
    parser.add_argument(
        "--print-weather",
        action="store_true",
        help="Run the planner, log the plan, then exit (no file write).",
    )
    return parser.parse_args()


def _log_plan(plan: Plan) -> None:
    log.info("Season          : %s", plan.season)
    log.info("Plan day        : %s", plan.plan_day.isoformat())
    log.info("JMA issued      : %s  code=%s  %s", plan.jma_issued, plan.jma_code, plan.jma_text)
    log.info("14d PV scale    : %.2f", plan.scale)
    log.info(
        "Predicted PV    : %.1f kWh raw → %.1f kWh calibrated",
        plan.pv_hat_kwh, plan.pv_cal_kwh,
    )
    log.info("Predicted load  : %.1f kWh (07–23, EV-stripped)", plan.load_hat_kwh)
    log.info("Midday GHI      : %.0f W/m² mean 09–15", plan.midday_ghi_mean)
    log.info(
        "clip_safe / evening / floor : %.0f%% / %.0f%% / %.0f%%",
        plan.clip_safe, plan.evening_cover, plan.morning_floor,
    )
    log.info("Target SoC      : %d%%", plan.target_soc)
    log.info("full_charge     : %s  (%s)", plan.full_charge, plan.full_charge_reason)
    log.info(
        "last SYNC       : %s (%s days ago)",
        plan.last_full_charge, plan.days_since_full,
    )
    for n in plan.notes:
        log.info("  note: %s", n)
    for _, row in plan.hourly.iterrows():
        h = int(row["hour"])
        if 6 <= h <= 18:
            log.debug(
                "  %02d  GHI=%.0f  PV=%.0f W  load=%.0f W",
                h, row["ghi_wm2"], row["pv_hat_w"], row["load_hat_w"],
            )


def main() -> None:
    args = parse_args()
    now = datetime.now(JST)
    plan_day = (now + timedelta(days=1)).date()

    log.info("=" * 60)
    log.info("Daily target calculation started")
    log.info("  Now / plan day: %s / %s", now.isoformat(timespec="minutes"), plan_day)
    log.info("  Config file   : %s", CONFIG_PATH)
    log.info("  API base      : %s", _API_BASE)
    log.info("  Influx        : %s  bucket=%s", INFLUX_URL, INFLUX_BUCKET)
    log.info("  GHI grid      : %.4f N, %.4f E (JMA MSM via Open-Meteo)", PLANNER_LAT, PLANNER_LON)
    log.info("  Dry run       : %s", args.dry_run or args.print_weather)
    log.info("=" * 60)

    if args.weather_code is not None:
        log.warning("--weather-code is ignored; target is computed from hourly GHI")

    try:
        with open(CONFIG_PATH) as f:
            existing = json.load(f)
    except Exception:
        existing = {}

    if not args.print_weather and existing.get("skip_next_auto") == now.date().isoformat():
        log.info("skip_next_auto matches today — exiting without modifying targets.json")
        if not args.dry_run:
            existing.pop("skip_next_auto", None)
            try:
                with open(CONFIG_PATH, "w") as f:
                    json.dump(existing, f)
                log.info("Cleared skip_next_auto from %s", CONFIG_PATH)
            except Exception as e:
                log.error("Failed to clear skip_next_auto: %s", e)
        return

    if not INFLUX_TOKEN or not INFLUX_ORG or not INFLUX_BUCKET:
        log.error("INFLUX_TOKEN / INFLUX_ORG / INFLUX_BUCKET must be set")
        sys.exit(1)

    try:
        log.info("Loading hourly history from Influx…")
        hist = load_hourly_influx(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET)
        log.info("  %s → %s  (%d hours)", hist.index.min(), hist.index.max(), len(hist))
        plan = compute_plan(
            hist, plan_day, now, _load_last_full_charge(),
            lat=PLANNER_LAT, lon=PLANNER_LON,
        )
    except Exception as e:
        log.error("Planner failed — leaving targets.json unchanged: %s", e)
        sys.exit(1)

    _log_plan(plan)

    if args.print_weather:
        return

    if args.target_soc is not None:
        target_soc = args.target_soc
        full_charge = False
        log.info("Using CLI target SoC: %d%% (full_charge forced off)", target_soc)
    else:
        target_soc = plan.target_soc
        full_charge = plan.full_charge

    if args.start_soc is not None:
        battery_soc = float(args.start_soc)
        log.info("Using CLI start SoC: %.0f%%", battery_soc)
    else:
        data = fetch_registers()
        if not data:
            log.error("Failed to fetch registers — cannot calculate current, exiting")
            sys.exit(1)
        battery_soc = float(int(data["0x0100"]))
        log.info("Current SoC from inverter: %.0f%%", battery_soc)

    if args.estimate_start_soc and args.start_soc is None:
        battery_soc = estimate_soc_at_2259(battery_soc)

    if args.charging_hours is not None and args.until_time is not None:
        log.error("--charging-hours and --until-time are mutually exclusive")
        sys.exit(1)
    if args.charging_hours is not None:
        charging_hours = args.charging_hours
        log.info("Using CLI charging hours: %.2f h", charging_hours)
    else:
        charging_hours = calculate_charging_hours(args.until_time)

    daily_charge_current = calculate_required_current(battery_soc, target_soc, charging_hours)
    log.info(
        "Result: target_soc=%d%%  daily_charge_current=%.0f A  full_charge=%s  (over %.2f h)",
        target_soc, daily_charge_current, full_charge, charging_hours,
    )

    if args.dry_run:
        log.info("Dry run — targets.json not modified")
        return

    try:
        with open(CONFIG_PATH) as f:
            targets = json.load(f)
    except Exception:
        targets = {}
    targets["target_soc"] = target_soc
    targets["daily_charge_current"] = daily_charge_current
    targets["full_charge"] = full_charge
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(targets, f)
        log.info(
            "Wrote targets to %s: target_soc=%d%%  daily_charge_current=%.0f A  full_charge=%s",
            CONFIG_PATH, target_soc, daily_charge_current, full_charge,
        )
    except Exception as e:
        log.error("Failed to write targets.json: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
