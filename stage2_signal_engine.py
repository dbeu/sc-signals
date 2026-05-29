#!/usr/bin/env python3
"""Stage 2 replay signal engine.

Reads Stage 1 event files, maintains in-memory bars/context, and emits
robust-v3 clean-common signal rows. It does not call Polygon.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from common import (
    DEFAULT_PARAM_CSV,
    GE_MIN_PM_HIGH_EXT,
    MAX_ENTRY_PRICE,
    MIN_ENTRY_PRICE,
    RTH_END,
    RTH_START,
    TIME_RANGES,
    likely_non_common,
)

from feature_helpers import (
    AH_END,
    bars5,
    compute_ema5,
    safe_ratio,
    scalars,
    session,
    time_bucket_1h,
)


class SignalState:
    def __init__(self, params: dict[str, dict]) -> None:
        self.params = params
        self.context = pd.DataFrame()
        self.bars = pd.DataFrame()
        self.sent_keys: set[tuple[str, str, str, str]] = set()
        self.signals: list[dict] = []

    def ingest_context(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        frame = frame.copy()
        frame["ticker"] = frame["ticker"].astype(str).str.upper()
        frame["date"] = frame["date"].astype(str)
        self.context = pd.concat([self.context, frame], ignore_index=True)
        self.context = self.context.drop_duplicates(["ticker", "date"], keep="last").reset_index(drop=True)

    def ingest_bars(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        frame = frame.copy()
        frame["ticker"] = frame["ticker"].astype(str).str.upper()
        frame["date"] = frame["date"].astype(str)
        frame["dt"] = pd.to_datetime(frame["dt"])
        self.bars = pd.concat([self.bars, frame], ignore_index=True)
        self.bars = self.bars.drop_duplicates(["ticker", "dt"], keep="last").sort_values(["ticker", "dt"]).reset_index(drop=True)

    def process(self, asof_et: pd.Timestamp) -> None:
        if self.context.empty or self.bars.empty:
            return
        for row in self.context.to_dict("records"):
            ticker = str(row["ticker"]).upper()
            if likely_non_common(ticker):
                continue
            self._process_ticker_date(ticker, str(row["date"]), row, asof_et)

    def _day_cache(self, ticker: str, needed_dates: set[str]) -> dict[str, pd.DataFrame]:
        source = self.bars[self.bars["ticker"].eq(ticker) & self.bars["date"].isin(needed_dates)].copy()
        if source.empty:
            return {}
        return {date: day.copy() for date, day in source.groupby("date", sort=False)}

    def _process_ticker_date(self, ticker: str, date: str, context_row: dict, asof_et: pd.Timestamp) -> None:
        prev_date = context_row.get("prev_date")
        needed_dates = {date}
        if pd.notna(prev_date):
            needed_dates.add(str(prev_date))
        day_cache = self._day_cache(ticker, needed_dates)
        if date not in day_cache:
            return

        scalar_cache = {d: scalars(day_cache, d) for d in needed_dates}
        today_scalars = scalar_cache.get(date)
        if not today_scalars:
            return

        self._process_re_like(ticker, date, "RE", {}, day_cache, asof_et)

        if pd.isna(prev_date) or str(prev_date) not in day_cache:
            return
        prev_scalars = scalar_cache.get(str(prev_date))
        if not prev_scalars:
            return

        prev_close = float(context_row["prev_close"]) if pd.notna(context_row.get("prev_close")) else np.nan
        day_open = float(today_scalars["o"])
        pm_high = float(today_scalars["pmh"]) if pd.notna(today_scalars.get("pmh")) else np.nan
        gap = safe_ratio(day_open, prev_close)
        pm_selloff = safe_ratio(day_open, pm_high)
        pm_high_ext = safe_ratio(pm_high, prev_close)
        if "GO" in self.params:
            self._process_go(ticker, date, gap, pm_selloff, today_scalars, day_cache, asof_et)
        ge = self.params["GE"]
        if (
            pd.notna(pm_high_ext)
            and pm_high_ext > GE_MIN_PM_HIGH_EXT
            and gap >= float(ge["gap_ge"])
            and pm_selloff <= float(ge["pm_selloff_le"])
        ):
            self._process_re_like(
                ticker,
                date,
                "GE",
                {"gap": gap, "pm_selloff": pm_selloff, "pm_high_ext": pm_high_ext},
                day_cache,
                asof_et,
            )

        d2_context = self._d2_context(prev_scalars, today_scalars)
        if d2_context is None:
            return
        self._process_d2o(ticker, date, d2_context, day_cache, asof_et)
        self._process_re_like(ticker, date, "D2E", d2_context, day_cache, asof_et)

    def _process_go(
        self,
        ticker: str,
        date: str,
        gap: float,
        pm_selloff: float,
        today_scalars: dict,
        day_cache: dict[str, pd.DataFrame],
        asof_et: pd.Timestamp,
    ) -> None:
        params = self.params["GO"]
        pm_volume = float(today_scalars.get("pm_volume", np.nan))
        day_open = float(today_scalars["o"])
        pm_dollar_volume = day_open * pm_volume if pd.notna(pm_volume) else np.nan
        if not (
            pd.notna(gap)
            and pd.notna(pm_selloff)
            and pd.notna(pm_dollar_volume)
            and gap >= float(params["go_gap_min"])
            and gap < float(params["go_gap_max"])
            and pm_selloff >= float(params["go_selloff_min"])
            and pm_selloff < float(params["go_selloff_max"])
            and pm_dollar_volume >= float(params.get("go_min_pm_dollar_volume", 200_000.0))
        ):
            return
        rth = session(day_cache, date, RTH_START, RTH_END)
        if rth.empty:
            return
        first = rth.iloc[0]
        if pd.Timestamp(first["dt"]) > asof_et:
            return
        entry = float(first["open"])
        if not MIN_ENTRY_PRICE <= entry <= MAX_ENTRY_PRICE:
            return
        self._add_signal(
            {
                "strategy": "GO",
                "ticker": ticker,
                "date": date,
                "time": RTH_START,
                "time_bucket_1h": time_bucket_1h(RTH_START),
                "gap": gap,
                "selloff": pm_selloff,
                "pm_dollar_volume": pm_dollar_volume,
                "entry_price": entry,
                "generated_at_et": asof_et.isoformat(),
            }
        )

    def _d2_context(self, prev_scalars: dict, today_scalars: dict) -> dict | None:
        context = {
            "allc1_o1": safe_ratio(float(prev_scalars["allc"]), float(prev_scalars["o"])),
            "o2_allc1": safe_ratio(float(today_scalars["o"]), float(prev_scalars["allc"])),
            "pmc2_pmh2": safe_ratio(float(today_scalars["pmc"]), float(today_scalars["pmh"])),
            "is_d2": int(safe_ratio(float(prev_scalars["h"]), float(prev_scalars["o"])) >= 0.20),
            "is_ah": int(
                safe_ratio(float(prev_scalars["h"]), float(prev_scalars["o"])) < 0.20
                and pd.notna(prev_scalars.get("ahh"))
                and safe_ratio(float(prev_scalars["ahh"]), float(prev_scalars["o"])) >= 0.20
            ),
        }
        return context

    def _d2_context_passes(self, strategy: str, context: dict) -> bool:
        params = self.params[strategy]
        return (
            context["is_d2"] == 1
            and context["is_ah"] == 0
            and context["allc1_o1"] >= float(params["allc1_o1_ge"])
            and context["o2_allc1"] >= float(params["o2_allc1_ge"])
            and context["pmc2_pmh2"] <= float(params["pmc2_pmh2_le"])
        )

    def _process_d2o(
        self,
        ticker: str,
        date: str,
        context: dict,
        day_cache: dict[str, pd.DataFrame],
        asof_et: pd.Timestamp,
    ) -> None:
        if not self._d2_context_passes("D2O", context):
            return
        params = self.params["D2O"]
        if str(params["time_window"]) != "open_0930":
            return
        rth = session(day_cache, date, RTH_START, RTH_END)
        if rth.empty:
            return
        first = rth.iloc[0]
        if pd.Timestamp(first["dt"]) > asof_et:
            return
        entry = float(first["open"])
        if not MIN_ENTRY_PRICE <= entry <= MAX_ENTRY_PRICE:
            return
        self._add_signal(
            {
                "strategy": "D2O",
                "ticker": ticker,
                "date": date,
                "time": RTH_START,
                "time_bucket_1h": time_bucket_1h(RTH_START),
                **context,
                "entry_price": entry,
                "generated_at_et": asof_et.isoformat(),
            }
        )

    def _process_re_like(
        self,
        ticker: str,
        date: str,
        strategy: str,
        context: dict,
        day_cache: dict[str, pd.DataFrame],
        asof_et: pd.Timestamp,
    ) -> None:
        params = self.params[strategy]
        if strategy in {"D2E", "D2O"} and not self._d2_context_passes(strategy, context):
            return
        time_lo, time_hi = TIME_RANGES[str(params["time_window"])]
        start = RTH_START
        end = AH_END if strategy == "D2E" else ("16:01:00" if strategy == "GE" else RTH_END)
        one_minute = session(day_cache, date, start, end)
        five = bars5(one_minute)
        if five.empty:
            return
        five = five[five["dt"] + pd.Timedelta(minutes=5) <= asof_et].copy()
        if five.empty:
            return
        day_open = float(five.iloc[0]["open"])
        if day_open <= 0:
            return
        five["ema5"] = compute_ema5(five["close"], day_open)
        ema_ext_values = []
        for i in five.index:
            window = five.iloc[max(0, i - 4) : i + 1]
            ema_ext_values.append(float(((window["high"] - window["ema5"]) / window["ema5"]).max()))
        five["ema_5_ext"] = ema_ext_values
        five["extension"] = five["close"] / day_open - 1.0
        mask = (
            five["tod"].between(time_lo, time_hi)
            & five["close"].lt(five["open"])
            & five["extension"].ge(float(params["extension_ge"]))
            & five["ema_5_ext"].ge(float(params["ema_5_ext_ge"]))
            & five["close"].between(MIN_ENTRY_PRICE, MAX_ENTRY_PRICE, inclusive="both")
        )
        for _, bar in five[mask].iterrows():
            self._add_signal(
                {
                    "strategy": strategy,
                    "ticker": ticker,
                    "date": date,
                    "time": str(bar["tod"]),
                    "time_bucket_1h": time_bucket_1h(str(bar["tod"])),
                    "extension": float(bar["extension"]),
                    "ema_5_ext": float(bar["ema_5_ext"]),
                    **context,
                    "entry_price": float(bar["close"]),
                    "generated_at_et": asof_et.isoformat(),
                }
            )

    def _add_signal(self, row: dict) -> None:
        key = (str(row["strategy"]), str(row["ticker"]), str(row["date"]), str(row["time"]))
        if key in self.sent_keys:
            return
        self.sent_keys.add(key)
        self.signals.append(row)


def load_params(path: Path) -> dict[str, dict]:
    df = pd.read_csv(path)
    return {str(row["strategy"]).upper(): row for row in df.to_dict("records")}


def event_dirs(events_dir: Path) -> list[Path]:
    return sorted(path for path in events_dir.iterdir() if path.is_dir())


def process_event_dir(state: SignalState, event_dir: Path) -> list[dict]:
    manifest_path = event_dir / "manifest.json"
    if not manifest_path.exists():
        return []
    before = len(state.signals)
    manifest = json.loads(manifest_path.read_text())
    asof_et = pd.Timestamp(manifest["asof_et"])
    context_path = event_dir / "universe_context.parquet"
    bars_path = event_dir / "bar_delta.parquet"
    if context_path.exists():
        state.ingest_context(pd.read_parquet(context_path))
    if bars_path.exists():
        state.ingest_bars(pd.read_parquet(bars_path))
    state.process(asof_et)
    return state.signals[before:]


def signals_frame(state: SignalState) -> pd.DataFrame:
    signals = pd.DataFrame(state.signals)
    if not signals.empty:
        signals = signals.sort_values(["date", "time", "strategy", "ticker"]).reset_index(drop=True)
    return signals


def save_signals(state: SignalState, out_dir: Path) -> pd.DataFrame:
    out_dir.mkdir(parents=True, exist_ok=True)
    signals = signals_frame(state)
    signals.to_parquet(out_dir / "signals.parquet", index=False)
    signals.to_csv(out_dir / "signals.csv", index=False)
    return signals


def run(args: argparse.Namespace) -> None:
    state = SignalState(load_params(args.param_csv))
    for cycle in event_dirs(args.events_dir):
        process_event_dir(state, cycle)
    signals = save_signals(state, args.out_dir)
    print(f"Saved {len(signals):,} signals to {args.out_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-dir", type=Path, required=True)
    parser.add_argument("--param-csv", type=Path, default=DEFAULT_PARAM_CSV)
    parser.add_argument("--out-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
