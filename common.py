#!/usr/bin/env python3
"""Shared constants for the staged robust-v3 live/replay pipeline."""

from __future__ import annotations

from pathlib import Path


HERE = Path(__file__).resolve().parent

DEFAULT_PARAM_CSV = HERE / "config" / "selected_params.csv"

PRE_START = "04:00:00"
RTH_START = "09:30:00"
RTH_END = "16:00:00"
AH_END = "20:00:00"

MIN_ENTRY_PRICE = 1.0
MAX_ENTRY_PRICE = 100.0
GE_MIN_PM_HIGH_EXT = 0.20

STRATEGIES = ("GE", "RE", "D2E", "D2O")

KNOWN_NON_COMMON_TICKERS = {
    "MSOX",
    "QVCGP",
}

TIME_RANGES = {
    "morning_0930_1059": ("09:30:00", "10:59:59"),
    "before_1400": ("09:30:00", "13:59:59"),
    "midday_1100_1359": ("11:00:00", "13:59:59"),
    "rth_0930_1600": ("09:30:00", "16:00:00"),
    "open_0930": ("09:30:00", "09:30:00"),
}


def likely_non_common(ticker: str) -> bool:
    ticker = str(ticker).upper()
    return (
        ticker in KNOWN_NON_COMMON_TICKERS
        or ticker.endswith(("W", "R", "U"))
        or (len(ticker) == 5 and ticker.endswith("P"))
    )
