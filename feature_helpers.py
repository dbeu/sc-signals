#!/usr/bin/env python3
"""Feature helpers used by the live Stage 2 signal engine."""

from __future__ import annotations

import numpy as np
import pandas as pd


PRE_START = "04:00:00"
RTH_START = "09:30:00"
RTH_END = "16:00:00"
AH_END = "20:00:00"


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
        return np.nan
    return float(numerator / denominator - 1.0)


def compute_ema5(closes: pd.Series, init_value: float) -> pd.Series:
    alpha = 2.0 / 6.0
    ema = float(init_value)
    values = []
    for close in closes:
        ema = alpha * float(close) + (1.0 - alpha) * ema
        values.append(ema)
    return pd.Series(values, index=closes.index)


def time_bucket_1h(tod: str) -> str:
    hour = int(str(tod)[:2])
    minute = int(str(tod)[3:5])
    if hour == 9 and minute >= 30:
        return "09:30-10:29"
    return f"{hour:02d}:00-{hour:02d}:59"


def session(data: pd.DataFrame | dict[str, pd.DataFrame], date: str, start: str, end: str) -> pd.DataFrame:
    if isinstance(data, dict):
        day = data.get(date)
        if day is None:
            return pd.DataFrame()
        day = day.copy()
    else:
        day = data[data["date"].eq(date)].copy()
    if day.empty:
        return day
    return day[(day["tod"] >= start) & (day["tod"] < end)].copy()


def bars5(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["dt", "tod", "open", "high", "low", "close", "volume", "transactions"])
    source = df.copy()
    if "transactions" not in source.columns:
        source["transactions"] = np.nan
    bars = (
        source.set_index("dt")
        .resample("5min")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            transactions=("transactions", "sum"),
        )
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    bars["tod"] = bars["dt"].dt.strftime("%H:%M:%S")
    return bars


def scalars(day_cache: dict[str, pd.DataFrame], date: str) -> dict[str, float | str] | None:
    all_session = session(day_cache, date, PRE_START, AH_END)
    rth = session(day_cache, date, RTH_START, RTH_END)
    pm = session(day_cache, date, PRE_START, RTH_START)
    ah = session(day_cache, date, RTH_END, AH_END)
    if all_session.empty or rth.empty:
        return None
    all_bars = bars5(all_session)
    if all_bars.empty:
        return None
    all_high_row = all_bars.loc[all_bars["high"].idxmax()]
    return {
        "o": float(rth.iloc[0]["open"]),
        "c": float(rth.iloc[-1]["close"]),
        "h": float(rth["high"].max()),
        "allc": float(all_session.iloc[-1]["close"]),
        "allh": float(all_bars["high"].max()),
        "allh_time": str(all_high_row["tod"]),
        "pmh": float(pm["high"].max()) if not pm.empty else np.nan,
        "pmc": float(pm.iloc[-1]["close"]) if not pm.empty else np.nan,
        "pm_volume": float(pm["volume"].sum()) if not pm.empty else np.nan,
        "ahh": float(ah["high"].max()) if not ah.empty else np.nan,
    }
