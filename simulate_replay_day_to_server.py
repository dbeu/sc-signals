#!/usr/bin/env python3
"""Replay one historical day into a Stage 2 receiver.

This is a Stage 1 simulator for integration testing. It reads local historical
parquet data, writes event directories, and posts each event to the receiver in
chronological order.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path

import pandas as pd

from common import AH_END, PRE_START, RTH_END, RTH_START, likely_non_common
from env_loader import load_dotenv
from event_transport import post_event_dir


DEFAULT_LOCAL_SC = Path("/home/daniel/Documents/codebox/algotrading/sc")
DEFAULT_UNIVERSE = DEFAULT_LOCAL_SC / "polygon_oos" / "universe_table_v2.parquet"
DEFAULT_MINUTE_DIR = DEFAULT_LOCAL_SC / "polygon_oos" / "organized_minute_data"


def parse_tickers(value: str) -> list[str]:
    return [part.strip().upper() for part in value.split(",") if part.strip()]


def to_eastern(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "dt" not in out.columns:
        out["dt"] = pd.to_datetime(out["window_start"], unit="ns", utc=True).dt.tz_convert("America/New_York")
    else:
        out["dt"] = pd.to_datetime(out["dt"])
        if out["dt"].dt.tz is None:
            out["dt"] = out["dt"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
    out["date"] = out["date"].astype(str)
    out["tod"] = out["dt"].dt.strftime("%H:%M:%S")
    return out.sort_values("dt")


def load_universe_context(universe_path: Path, tickers: list[str], dates: list[str]) -> pd.DataFrame:
    use_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
    universe = pd.read_parquet(universe_path, columns=use_cols)
    universe["ticker"] = universe["ticker"].astype(str).str.upper()
    universe["date"] = universe["date"].astype(str)
    universe = universe[~universe["ticker"].map(likely_non_common)].copy()
    universe = universe.sort_values(["ticker", "date"]).reset_index(drop=True)
    grouped = universe.groupby("ticker", sort=False)
    universe["prev_date"] = grouped["date"].shift(1)
    universe["prev_open"] = grouped["open"].shift(1)
    universe["prev_high"] = grouped["high"].shift(1)
    universe["prev_low"] = grouped["low"].shift(1)
    universe["prev_close"] = grouped["close"].shift(1)
    universe["prev_volume"] = grouped["volume"].shift(1)
    context = universe[universe["ticker"].isin(tickers) & universe["date"].isin(dates)].copy()
    cols = [
        "ticker",
        "date",
        "prev_date",
        "prev_open",
        "prev_high",
        "prev_low",
        "prev_close",
        "prev_volume",
    ]
    return context[cols].reset_index(drop=True)


def normalize_minute_bars(minute_path: Path, dates: set[str]) -> pd.DataFrame:
    minute = pd.read_parquet(minute_path)
    minute["ticker"] = minute["ticker"].astype(str).str.upper()
    minute["date"] = minute["date"].astype(str)
    minute = minute[minute["date"].isin(dates)].copy()
    if minute.empty:
        return minute
    minute = to_eastern(minute)
    if "transactions" not in minute.columns:
        minute["transactions"] = pd.NA
    cols = ["ticker", "date", "dt", "tod", "open", "high", "low", "close", "volume", "transactions"]
    return minute[cols].sort_values(["ticker", "dt"]).reset_index(drop=True)


def write_cycle(out_dir: Path, index: int, label: str, asof_et: str, frames: dict[str, pd.DataFrame]) -> Path:
    cycle = out_dir / f"{index:04d}_{label}"
    cycle.mkdir(parents=True, exist_ok=True)
    manifest = {"cycle": index, "label": label, "asof_et": asof_et}
    (cycle / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for name, frame in frames.items():
        if frame is not None and not frame.empty:
            frame.to_parquet(cycle / f"{name}.parquet", index=False)
    return cycle


def build_events(args: argparse.Namespace) -> list[Path]:
    tickers = parse_tickers(args.tickers)
    context = load_universe_context(args.universe, tickers, [args.date])
    if context.empty:
        raise SystemExit("No universe context found for requested ticker/date.")

    if args.clean and args.out_dir.exists():
        shutil.rmtree(args.out_dir)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    all_bars = []
    needed_dates = set(context["date"].astype(str)) | set(context["prev_date"].dropna().astype(str))
    for ticker in tickers:
        path = args.minute_dir / f"{ticker}.parquet"
        if not path.exists():
            raise SystemExit(f"Missing minute file: {path}")
        bars = normalize_minute_bars(path, needed_dates)
        if not bars.empty:
            all_bars.append(bars)
    minute = pd.concat(all_bars, ignore_index=True) if all_bars else pd.DataFrame()
    if minute.empty:
        raise SystemExit("No minute bars found for requested ticker/date.")

    active_date = args.date
    prior_dates = set(context["prev_date"].dropna().astype(str))
    seed_prior = minute[minute["date"].isin(prior_dates) & minute["tod"].between(PRE_START, AH_END)].copy()
    seed_pm = minute[minute["date"].eq(active_date) & minute["tod"].between(PRE_START, RTH_START, inclusive="left")].copy()
    seed_open = minute[minute["date"].eq(active_date) & minute["tod"].eq(RTH_START)].copy()
    seed_bars = pd.concat([seed_prior, seed_pm, seed_open], ignore_index=True).sort_values(["ticker", "dt"])
    events = [
        write_cycle(
            args.out_dir,
            0,
            "seed",
            pd.Timestamp(f"{active_date} {RTH_START}", tz="America/New_York").isoformat(),
            {"universe_context": context, "bar_delta": seed_bars},
        )
    ]

    rth = minute[minute["date"].eq(active_date) & minute["tod"].between(RTH_START, RTH_END, inclusive="left")].copy()
    cutoffs = pd.date_range(
        f"{active_date} {RTH_START}",
        f"{active_date} {RTH_END}",
        freq=f"{args.interval_minutes}min",
        tz="America/New_York",
    )
    last_cutoff = pd.Timestamp(f"{active_date} {RTH_START}", tz="America/New_York")
    cycle_index = 1
    for cutoff in cutoffs[1:]:
        delta = rth[(rth["dt"] >= last_cutoff) & (rth["dt"] < cutoff)].copy()
        if not delta.empty:
            events.append(
                write_cycle(
                    args.out_dir,
                    cycle_index,
                    cutoff.strftime("%H%M"),
                    cutoff.isoformat(),
                    {"bar_delta": delta},
                )
            )
            cycle_index += 1
        last_cutoff = cutoff
    return events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, default=Path(".env"))
    parser.add_argument("--date", required=True)
    parser.add_argument("--tickers", required=True)
    parser.add_argument("--url", default="")
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--minute-dir", type=Path, default=DEFAULT_MINUTE_DIR)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--interval-minutes", type=int, default=5)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(args.env)
    events = build_events(args)
    if args.build_only:
        print(f"Built {len(events):,} events under {args.out_dir}")
        return
    if not args.url:
        raise SystemExit("--url is required unless --build-only is set.")
    token = os.environ.get("SC_STAGE1_TOKEN", "")
    for idx, event_dir in enumerate(events, start=1):
        receipt = post_event_dir(event_dir, args.url, token=token, source="historical_replay")
        print(f"{idx:03d}/{len(events):03d} sent {event_dir.name}: new_signals={receipt.get('new_signals')}")
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
