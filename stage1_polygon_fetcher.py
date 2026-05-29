#!/usr/bin/env python3
"""Live Stage 1 Polygon/Massive fetcher.

This writes the same event contract as ``stage1_replay_fetcher.py``. It does
not compute signals, send notifications, or call Stage 2.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from common import (
    AH_END,
    GE_MIN_PM_HIGH_EXT,
    MAX_ENTRY_PRICE,
    MIN_ENTRY_PRICE,
    PRE_START,
    RTH_START,
    RTH_END,
    likely_non_common,
)
from stage1_polygon_api_probe import (
    BASE_URL,
    DEFAULT_ENV,
    fetch_grouped_daily,
    fetch_minute_aggs,
    fetch_reference_tickers,
    fetch_snapshots,
    load_api_key,
)
from env_loader import load_dotenv
from event_transport import post_event_dir


ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class RouteThresholds:
    re_route_extension: float
    ge_gap_min: float
    ge_pm_high_ext_min: float
    ge_pm_selloff_max: float
    d2_prev_high_open_min: float
    go_gap_min: float
    go_gap_max: float
    go_pm_selloff_min: float
    go_pm_selloff_max: float
    go_min_pm_dollar_volume: float


def parse_tickers(value: str) -> list[str]:
    return [ticker.strip().upper() for ticker in value.split(",") if ticker.strip()]


def previous_weekday(date: str) -> str:
    day = pd.Timestamp(date).date()
    day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day.isoformat()


def ms_to_et(value: int | float | None) -> pd.Timestamp | pd.NaT:
    if value is None or pd.isna(value):
        return pd.NaT
    return pd.to_datetime(int(value), unit="ms", utc=True).tz_convert("America/New_York")


def ns_to_et(value: int | float | None) -> pd.Timestamp | pd.NaT:
    if value is None or pd.isna(value):
        return pd.NaT
    return pd.to_datetime(int(value), unit="ns", utc=True).tz_convert("America/New_York")


def snapshot_bar_value(snapshot: dict, section: str, key: str) -> float | None:
    value = snapshot.get(section, {})
    if not isinstance(value, dict):
        return None
    return value.get(key)


def normalize_reference(payload: dict) -> pd.DataFrame:
    rows = payload.get("results", [])
    if not rows:
        return pd.DataFrame(columns=["ticker", "type", "active", "market", "locale", "primary_exchange"])
    df = pd.DataFrame(rows)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df


def normalize_grouped_daily(payload: dict, date: str, prefix: str = "prev") -> pd.DataFrame:
    rows = payload.get("results", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    out = pd.DataFrame(
        {
            "ticker": df["T"].astype(str).str.upper(),
            f"{prefix}_date": date,
            f"{prefix}_open": pd.to_numeric(df["o"], errors="coerce"),
            f"{prefix}_high": pd.to_numeric(df["h"], errors="coerce"),
            f"{prefix}_low": pd.to_numeric(df["l"], errors="coerce"),
            f"{prefix}_close": pd.to_numeric(df["c"], errors="coerce"),
            f"{prefix}_volume": pd.to_numeric(df["v"], errors="coerce"),
        }
    )
    return out


def normalize_snapshots(payload: dict) -> pd.DataFrame:
    rows = payload.get("tickers", []) or payload.get("results", [])
    normalized = []
    for row in rows:
        ticker = str(row.get("ticker", "")).upper()
        if not ticker:
            continue
        last_trade = row.get("lastTrade", {}) if isinstance(row.get("lastTrade"), dict) else {}
        normalized.append(
            {
                "ticker": ticker,
                "snapshot_price": last_trade.get("p"),
                "snapshot_updated": row.get("updated"),
                "day_open": snapshot_bar_value(row, "day", "o"),
                "day_high": snapshot_bar_value(row, "day", "h"),
                "day_low": snapshot_bar_value(row, "day", "l"),
                "day_close": snapshot_bar_value(row, "day", "c"),
                "day_volume": snapshot_bar_value(row, "day", "v"),
                "prev_open_snapshot": snapshot_bar_value(row, "prevDay", "o"),
                "prev_high_snapshot": snapshot_bar_value(row, "prevDay", "h"),
                "prev_low_snapshot": snapshot_bar_value(row, "prevDay", "l"),
                "prev_close_snapshot": snapshot_bar_value(row, "prevDay", "c"),
                "min_open": snapshot_bar_value(row, "min", "o"),
                "min_high": snapshot_bar_value(row, "min", "h"),
                "min_low": snapshot_bar_value(row, "min", "l"),
                "min_close": snapshot_bar_value(row, "min", "c"),
                "min_volume": snapshot_bar_value(row, "min", "v"),
            }
        )
    return pd.DataFrame(normalized)


def normalize_minute_aggs(payload: dict, ticker: str) -> pd.DataFrame:
    rows = payload.get("results", [])
    if not rows:
        return pd.DataFrame(columns=["ticker", "date", "dt", "tod", "open", "high", "low", "close", "volume", "transactions"])
    df = pd.DataFrame(rows)
    dt = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert("America/New_York")
    out = pd.DataFrame(
        {
            "ticker": ticker.upper(),
            "date": dt.dt.strftime("%Y-%m-%d"),
            "dt": dt,
            "tod": dt.dt.strftime("%H:%M:%S"),
            "open": pd.to_numeric(df["o"], errors="coerce"),
            "high": pd.to_numeric(df["h"], errors="coerce"),
            "low": pd.to_numeric(df["l"], errors="coerce"),
            "close": pd.to_numeric(df["c"], errors="coerce"),
            "volume": pd.to_numeric(df["v"], errors="coerce"),
            "transactions": pd.to_numeric(df.get("n"), errors="coerce") if "n" in df else pd.NA,
        }
    )
    return out.sort_values("dt").reset_index(drop=True)


def clean_reference_universe(reference: pd.DataFrame) -> pd.DataFrame:
    if reference.empty:
        return reference
    df = reference.copy()
    df = df[
        df["ticker"].notna()
        & df["active"].eq(True)
        & df["market"].eq("stocks")
        & df["locale"].eq("us")
        & df["type"].eq("CS")
        & ~df["ticker"].map(likely_non_common)
    ].copy()
    return df.drop_duplicates("ticker").reset_index(drop=True)


def polygon_get_url(url: str, api_key: str, timeout: int = 30) -> dict[str, Any]:
    separator = "&" if "?" in url else "?"
    request_url = url if "apiKey=" in url else f"{url}{separator}{urllib.parse.urlencode({'apiKey': api_key})}"
    with urllib.request.urlopen(request_url, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
        payload["_http_status"] = response.status
        return payload


def fetch_reference_universe(api_key: str, limit: int = 0) -> pd.DataFrame:
    if limit > 0:
        return normalize_reference(fetch_reference_tickers(api_key, limit=limit))

    params = urllib.parse.urlencode({"market": "stocks", "active": "true", "limit": 1000, "apiKey": api_key})
    url = f"{BASE_URL}/v3/reference/tickers?{params}"
    rows = []
    page = 0
    while url:
        page += 1
        payload = polygon_get_url(url, api_key)
        rows.extend(payload.get("results", []))
        print(f"reference page {page}: total={len(rows):,}", flush=True)
        url = payload.get("next_url")
        if url and url.startswith("/"):
            url = f"{BASE_URL}{url}"
    return normalize_reference({"results": rows})


def fetch_snapshot_frame(api_key: str, tickers: list[str], batch_size: int) -> pd.DataFrame:
    if not tickers:
        return normalize_snapshots(fetch_snapshots(api_key, []))
    frames = []
    for start in range(0, len(tickers), batch_size):
        batch = tickers[start : start + batch_size]
        frame = normalize_snapshots(fetch_snapshots(api_key, batch))
        if not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_context(prev_daily: pd.DataFrame, snapshots: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    context = prev_daily.merge(snapshots, on="ticker", how="left")
    context["date"] = trade_date
    # Snapshot prevDay is a fallback if grouped daily is unavailable for a ticker.
    for name in ["open", "high", "low", "close"]:
        col = f"prev_{name}"
        fallback = f"prev_{name}_snapshot"
        if fallback in context:
            context[col] = context[col].fillna(context[fallback])
    context["prev_volume"] = context["prev_volume"].fillna(0.0)
    cols = [
        "ticker",
        "date",
        "prev_date",
        "prev_open",
        "prev_high",
        "prev_low",
        "prev_close",
        "prev_volume",
        "day_open",
        "day_high",
        "day_low",
        "day_close",
        "snapshot_price",
        "estimated_open",
    ]
    for col in cols:
        if col not in context:
            context[col] = pd.NA
    context["estimated_open"] = pd.to_numeric(context["day_open"], errors="coerce").fillna(
        pd.to_numeric(context["snapshot_price"], errors="coerce")
    )
    return context[cols].copy()


def summarize_premarket(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame(columns=["ticker", "pm_high", "pm_close", "pm_volume"])
    pm = bars[bars["tod"].between(PRE_START, RTH_START, inclusive="left")].copy()
    if pm.empty:
        return pd.DataFrame(columns=["ticker", "pm_high", "pm_close", "pm_volume"])
    grouped = pm.groupby("ticker", sort=False)
    out = grouped.agg(pm_high=("high", "max"), pm_volume=("volume", "sum")).reset_index()
    closes = pm.sort_values("dt").groupby("ticker", sort=False)["close"].last().rename("pm_close").reset_index()
    return out.merge(closes, on="ticker", how="left")


def route_tickers(context: pd.DataFrame, premarket: pd.DataFrame, thresholds: RouteThresholds) -> pd.DataFrame:
    routed = context.merge(premarket, on="ticker", how="left")
    prev_open = pd.to_numeric(routed["prev_open"], errors="coerce")
    prev_high = pd.to_numeric(routed["prev_high"], errors="coerce")
    prev_close = pd.to_numeric(routed["prev_close"], errors="coerce")
    day_open = pd.to_numeric(routed.get("estimated_open", routed["day_open"]), errors="coerce")
    day_high = pd.to_numeric(routed["day_high"], errors="coerce").fillna(day_open)
    pm_high = pd.to_numeric(routed["pm_high"], errors="coerce")

    routed["gap"] = day_open / prev_close - 1.0
    routed["high_open"] = day_high / day_open - 1.0
    routed["pm_high_ext"] = pm_high / prev_close - 1.0
    routed["pm_selloff"] = day_open / pm_high - 1.0
    routed["pm_dollar_volume"] = day_open * pd.to_numeric(routed["pm_volume"], errors="coerce")
    routed["prev_high_open"] = prev_high / prev_open - 1.0
    routed["route_re"] = routed["high_open"].ge(thresholds.re_route_extension) & day_high.ge(MIN_ENTRY_PRICE)
    routed["route_ge"] = (
        routed["gap"].ge(thresholds.ge_gap_min)
        & routed["pm_high_ext"].gt(thresholds.ge_pm_high_ext_min)
        & routed["pm_selloff"].le(thresholds.ge_pm_selloff_max)
        & day_high.ge(MIN_ENTRY_PRICE)
    )
    routed["route_d2"] = routed["prev_high_open"].ge(thresholds.d2_prev_high_open_min)
    routed["route_go"] = (
        routed["gap"].ge(thresholds.go_gap_min)
        & routed["gap"].lt(thresholds.go_gap_max)
        & routed["pm_selloff"].ge(thresholds.go_pm_selloff_min)
        & routed["pm_selloff"].le(thresholds.go_pm_selloff_max)
        & routed["pm_dollar_volume"].ge(thresholds.go_min_pm_dollar_volume)
        & day_high.ge(MIN_ENTRY_PRICE)
    )
    routed = routed[
        (routed["route_re"] | routed["route_ge"] | routed["route_d2"] | routed["route_go"])
        & day_open.le(MAX_ENTRY_PRICE).fillna(True)
    ].copy()
    return routed.sort_values("ticker").reset_index(drop=True)


def write_cycle(out_dir: Path, index: int, label: str, asof_et: pd.Timestamp, frames: dict[str, pd.DataFrame], extra: dict | None = None) -> Path:
    cycle = out_dir / f"{index:04d}_{label}"
    cycle.mkdir(parents=True, exist_ok=True)
    manifest = {"cycle": index, "label": label, "asof_et": asof_et.isoformat()}
    if extra:
        manifest.update(extra)
    (cycle / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for name, frame in frames.items():
        if frame is not None and not frame.empty:
            frame.to_parquet(cycle / f"{name}.parquet", index=False)
    return cycle


def fetch_bars_for_tickers(api_key: str, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    frames = []
    for idx, ticker in enumerate(tickers, start=1):
        payload = fetch_minute_aggs(api_key, ticker, start_date, end_date, limit=50000)
        bars = normalize_minute_aggs(payload, ticker)
        if not bars.empty:
            frames.append(bars)
        print(f"bars {idx:,}/{len(tickers):,} {ticker}: {len(bars):,}", flush=True)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def parse_hms(value: str) -> tuple[int, int, int]:
    hour, minute, second = [int(part) for part in value.split(":")]
    return hour, minute, second


def local_day_dir(root: Path, trade_date: str) -> Path:
    return root / trade_date


def cleanup_old_day_dirs(root: Path, retention_days: int) -> None:
    if retention_days <= 0 or not root.exists():
        return
    cutoff = pd.Timestamp.now(tz=ET).normalize() - pd.Timedelta(days=retention_days)
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            child_day = pd.Timestamp(child.name).tz_localize(ET)
        except Exception:
            continue
        if child_day < cutoff:
            shutil.rmtree(child)
            print(f"Removed old Stage 1 archive: {child}", flush=True)


def sleep_until_next_poll(poll_seconds: int) -> None:
    now = time.time()
    delay = poll_seconds - (now % poll_seconds)
    time.sleep(max(1.0, delay))


def seconds_until_next_day(now_et: pd.Timestamp, hour: int = 0, minute: int = 5) -> float:
    next_day = (now_et + pd.Timedelta(days=1)).normalize().replace(hour=hour, minute=minute)
    return max(60.0, float((next_day - now_et).total_seconds()))


def preliminary_candidates(context: pd.DataFrame, thresholds: RouteThresholds) -> pd.DataFrame:
    df = context.copy()
    prev_open = pd.to_numeric(df["prev_open"], errors="coerce")
    prev_high = pd.to_numeric(df["prev_high"], errors="coerce")
    prev_close = pd.to_numeric(df["prev_close"], errors="coerce")
    day_open = pd.to_numeric(df.get("estimated_open", df["day_open"]), errors="coerce")
    day_high = pd.to_numeric(df["day_high"], errors="coerce").fillna(day_open)
    df["gap"] = day_open / prev_close - 1.0
    df["high_open"] = day_high / day_open - 1.0
    df["prev_high_open"] = prev_high / prev_open - 1.0
    possible = (
        df["prev_high_open"].ge(thresholds.d2_prev_high_open_min)
        | df["high_open"].ge(thresholds.re_route_extension)
        | df["gap"].ge(min(thresholds.ge_gap_min, thresholds.go_gap_min))
    )
    price_ok = day_high.ge(MIN_ENTRY_PRICE) & day_open.le(MAX_ENTRY_PRICE).fillna(True)
    return df[possible & price_ok].copy()


def event_context_columns(frame: pd.DataFrame) -> list[str]:
    wanted = [
        "ticker",
        "date",
        "prev_date",
        "prev_open",
        "prev_high",
        "prev_low",
        "prev_close",
        "prev_volume",
        "estimated_open",
        "day_open",
        "day_high",
        "day_low",
        "day_close",
        "snapshot_price",
        "pm_high",
        "pm_close",
        "pm_volume",
        "pm_dollar_volume",
        "gap",
        "pm_selloff",
        "route_go",
        "route_d2",
        "route_ge",
        "route_re",
    ]
    return [col for col in wanted if col in frame.columns]


def post_cycle(event_dir: Path, post_url: str, post_token: str, source: str) -> None:
    if not post_url:
        return
    receipt = post_event_dir(event_dir, post_url, token=post_token, source=source)
    print(
        f"posted {event_dir.name}: new_signals={receipt.get('new_signals')} total={receipt.get('signals_total')}",
        flush=True,
    )


def run_loop(args: argparse.Namespace) -> None:
    load_dotenv(args.env)
    if not args.post_token:
        args.post_token = os.environ.get("SC_STAGE1_TOKEN", "")
    api_key = load_api_key(args.env)
    trade_date = args.date or datetime.now(ET).date().isoformat()
    prev_date = args.prev_date or previous_weekday(trade_date)
    thresholds = RouteThresholds(
        re_route_extension=args.re_route_extension,
        ge_gap_min=args.ge_gap_min,
        ge_pm_high_ext_min=args.ge_pm_high_ext_min,
        ge_pm_selloff_max=args.ge_pm_selloff_max,
        d2_prev_high_open_min=args.d2_prev_high_open_min,
        go_gap_min=args.go_gap_min,
        go_gap_max=args.go_gap_max,
        go_pm_selloff_min=args.go_pm_selloff_min,
        go_pm_selloff_max=args.go_pm_selloff_max,
        go_min_pm_dollar_volume=args.go_min_pm_dollar_volume,
    )

    cleanup_old_day_dirs(args.out_dir, args.local_retention_days)
    day_dir = local_day_dir(args.out_dir, trade_date)
    day_dir.mkdir(parents=True, exist_ok=True)

    if args.tickers:
        universe_tickers = parse_tickers(args.tickers)
        print(f"Using explicit ticker allowlist: {len(universe_tickers):,}", flush=True)
    else:
        print("Fetching reference universe", flush=True)
        reference = clean_reference_universe(fetch_reference_universe(api_key, limit=args.reference_limit))
        if args.max_universe and len(reference) > args.max_universe:
            reference = reference.head(args.max_universe).copy()
        universe_tickers = reference["ticker"].drop_duplicates().tolist()
        print(f"Clean reference universe: {len(universe_tickers):,}", flush=True)

    print(f"Fetching grouped daily {prev_date}", flush=True)
    prev_daily = normalize_grouped_daily(fetch_grouped_daily(api_key, prev_date), prev_date)
    prev_daily = prev_daily[prev_daily["ticker"].isin(universe_tickers)].copy()

    active_tickers: set[str] = set()
    d2_prior_loaded: set[str] = set()
    last_sent_dt: dict[str, pd.Timestamp] = {}
    cycle_index = 0
    start_hms = parse_hms(args.start_time)
    stop_hms = parse_hms(args.stop_time)
    start_at = pd.Timestamp(datetime.combine(pd.Timestamp(trade_date).date(), datetime.min.time()), tz=ET).replace(
        hour=start_hms[0], minute=start_hms[1], second=start_hms[2]
    )
    stop_at = pd.Timestamp(datetime.combine(pd.Timestamp(trade_date).date(), datetime.min.time()), tz=ET).replace(
        hour=stop_hms[0], minute=stop_hms[1], second=stop_hms[2]
    )
    print(f"Stage 1 live loop window: {start_at.isoformat()} -> {stop_at.isoformat()}", flush=True)

    while True:
        now_et = pd.Timestamp.now(tz=ET) if not args.date else pd.Timestamp.now(tz=ET)
        if now_et < start_at:
            print(f"Waiting for start window: now={now_et.isoformat()}", flush=True)
            sleep_until_next_poll(args.poll_seconds)
            continue
        if now_et > stop_at:
            print(f"Stop window reached: now={now_et.isoformat()}", flush=True)
            cleanup_old_day_dirs(args.out_dir, args.local_retention_days)
            return

        print(f"cycle {cycle_index:04d} {now_et.strftime('%H:%M:%S')} fetching snapshots", flush=True)
        snapshots = fetch_snapshot_frame(api_key, [] if not args.tickers else universe_tickers, args.snapshot_batch_size)
        snapshots = snapshots[snapshots["ticker"].isin(universe_tickers)].copy() if not snapshots.empty else snapshots
        context = build_context(prev_daily, snapshots, trade_date)
        candidates = preliminary_candidates(context, thresholds)
        if args.max_active_tickers and len(candidates) > args.max_active_tickers:
            candidates = candidates.head(args.max_active_tickers).copy()
        candidate_tickers = candidates["ticker"].drop_duplicates().tolist()

        current_bars = fetch_bars_for_tickers(api_key, candidate_tickers, trade_date, trade_date) if candidate_tickers else pd.DataFrame()
        if not current_bars.empty:
            current_bars = current_bars[current_bars["dt"].le(now_et)].copy()
        premarket = summarize_premarket(current_bars)
        routed = route_tickers(context[context["ticker"].isin(candidate_tickers)].copy(), premarket, thresholds)
        routed_tickers = routed["ticker"].drop_duplicates().tolist()
        new_tickers = [ticker for ticker in routed_tickers if ticker not in active_tickers]
        active_tickers.update(routed_tickers)

        prior_bars = pd.DataFrame()
        new_d2 = routed.loc[routed["route_d2"].fillna(False), "ticker"].drop_duplicates().tolist()
        new_d2 = [ticker for ticker in new_d2 if ticker not in d2_prior_loaded]
        if new_d2:
            prior_bars = fetch_bars_for_tickers(api_key, new_d2, prev_date, prev_date)
            d2_prior_loaded.update(new_d2)

        active_current = pd.DataFrame()
        if active_tickers:
            active_current = fetch_bars_for_tickers(api_key, sorted(active_tickers), trade_date, trade_date)
            if not active_current.empty:
                active_current = active_current[active_current["dt"].le(now_et)].copy()
                keep = []
                for ticker, frame in active_current.groupby("ticker", sort=False):
                    last_dt = last_sent_dt.get(ticker)
                    delta = frame if last_dt is None else frame[frame["dt"].gt(last_dt)].copy()
                    if not delta.empty:
                        keep.append(delta)
                        last_sent_dt[ticker] = pd.Timestamp(delta["dt"].max())
                active_current = pd.concat(keep, ignore_index=True) if keep else pd.DataFrame()

        bars = pd.concat([prior_bars, active_current], ignore_index=True)
        if not bars.empty:
            bars = bars[bars["tod"].between(PRE_START, AH_END)].copy()
        routed_context = routed.copy()
        event_context = routed_context[routed_context["ticker"].isin(new_tickers)].copy()
        if cycle_index == 0:
            event_context = routed_context[routed_context["ticker"].isin(active_tickers)].copy()
        if not event_context.empty:
            event_context = event_context[event_context_columns(event_context)]

        label = "seed" if cycle_index == 0 else now_et.strftime("%H%M")
        event_dir = write_cycle(
            day_dir,
            cycle_index,
            label,
            now_et,
            {"universe_context": event_context, "bar_delta": bars},
            {
                "trade_date": trade_date,
                "prev_date": prev_date,
                "active_tickers": len(active_tickers),
                "new_tickers": len(new_tickers),
                "candidate_tickers": len(candidate_tickers),
                "routed_tickers": len(routed_tickers),
            },
        )
        routed.to_csv(day_dir / f"{cycle_index:04d}_{label}_routed.csv", index=False)
        post_cycle(event_dir, args.post_url, args.post_token, "stage1_polygon_loop")
        print(
            f"cycle {cycle_index:04d}: candidates={len(candidate_tickers):,} active={len(active_tickers):,} "
            f"new={len(new_tickers):,} bars={len(bars):,}",
            flush=True,
        )
        cycle_index += 1
        sleep_until_next_poll(args.poll_seconds)


def run_forever(args: argparse.Namespace) -> None:
    original_date = args.date
    while True:
        now_et = pd.Timestamp.now(tz=ET)
        if original_date:
            args.date = original_date
            run_loop(args)
            return
        if now_et.weekday() >= 5:
            sleep_seconds = min(seconds_until_next_day(now_et), 3600.0)
            print(f"Weekend/off day wait: now={now_et.isoformat()} sleeping={sleep_seconds:.0f}s", flush=True)
            time.sleep(sleep_seconds)
            continue
        args.date = now_et.date().isoformat()
        print(f"Starting Stage 1 day loop for {args.date}", flush=True)
        run_loop(args)
        args.date = ""
        now_et = pd.Timestamp.now(tz=ET)
        sleep_seconds = seconds_until_next_day(now_et)
        print(f"Stage 1 day complete. Sleeping until next day check: {sleep_seconds:.0f}s", flush=True)
        time.sleep(sleep_seconds)


def run_seed(args: argparse.Namespace) -> None:
    load_dotenv(args.env)
    if not args.post_token:
        args.post_token = os.environ.get("SC_STAGE1_TOKEN", "")
    api_key = load_api_key(args.env)
    trade_date = args.date or datetime.now(ET).date().isoformat()
    prev_date = args.prev_date or previous_weekday(trade_date)
    asof = pd.Timestamp.now(tz=ET)
    thresholds = RouteThresholds(
        re_route_extension=args.re_route_extension,
        ge_gap_min=args.ge_gap_min,
        ge_pm_high_ext_min=args.ge_pm_high_ext_min,
        ge_pm_selloff_max=args.ge_pm_selloff_max,
        d2_prev_high_open_min=args.d2_prev_high_open_min,
        go_gap_min=args.go_gap_min,
        go_gap_max=args.go_gap_max,
        go_pm_selloff_min=args.go_pm_selloff_min,
        go_pm_selloff_max=args.go_pm_selloff_max,
        go_min_pm_dollar_volume=args.go_min_pm_dollar_volume,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    if args.tickers:
        tickers = parse_tickers(args.tickers)
        print(f"Using explicit ticker allowlist: {len(tickers):,}", flush=True)
    else:
        print(f"Fetching reference tickers limit={args.reference_limit}", flush=True)
        reference = normalize_reference(fetch_reference_tickers(api_key, limit=args.reference_limit))
        reference = clean_reference_universe(reference)
        if args.max_universe and len(reference) > args.max_universe:
            reference = reference.head(args.max_universe).copy()
        tickers = reference["ticker"].drop_duplicates().tolist()
    if not tickers:
        raise SystemExit("No tickers in clean reference universe.")
    print(f"Clean reference universe: {len(tickers):,}", flush=True)

    print(f"Fetching grouped daily {prev_date}", flush=True)
    prev_daily = normalize_grouped_daily(fetch_grouped_daily(api_key, prev_date), prev_date)
    prev_daily = prev_daily[prev_daily["ticker"].isin(tickers)].copy()

    print("Fetching selected snapshots", flush=True)
    snapshots = normalize_snapshots(fetch_snapshots(api_key, tickers))
    context = build_context(prev_daily, snapshots, trade_date)

    # Seed premarket bars for a bounded initial route. For a live broad run this
    # will be replaced by snapshot-based premarket routing plus incremental bars.
    premarket_seed = context[
        pd.to_numeric(context["prev_high"], errors="coerce").div(pd.to_numeric(context["prev_open"], errors="coerce")).sub(1.0).ge(
            thresholds.d2_prev_high_open_min
        )
        | pd.to_numeric(context["day_high"], errors="coerce").ge(MIN_ENTRY_PRICE)
    ].copy()
    if args.max_bar_tickers and len(premarket_seed) > args.max_bar_tickers:
        premarket_seed = premarket_seed.head(args.max_bar_tickers).copy()
    seed_tickers = premarket_seed["ticker"].drop_duplicates().tolist()
    print(f"Fetching seed bars for {len(seed_tickers):,} tickers", flush=True)
    current_bars = fetch_bars_for_tickers(api_key, seed_tickers, trade_date, trade_date)
    premarket = summarize_premarket(current_bars)
    routed = route_tickers(context[context["ticker"].isin(seed_tickers)].copy(), premarket, thresholds)
    routed_tickers = routed["ticker"].drop_duplicates().tolist()
    print(f"Routed tickers: {len(routed_tickers):,}", flush=True)

    prior_bars = pd.DataFrame()
    d2_tickers = routed.loc[routed["route_d2"].fillna(False), "ticker"].drop_duplicates().tolist()
    if d2_tickers:
        print(f"Fetching prior extended bars for D2 route: {len(d2_tickers):,}", flush=True)
        prior_bars = fetch_bars_for_tickers(api_key, d2_tickers, prev_date, prev_date)

    if routed_tickers:
        routed_current = current_bars[current_bars["ticker"].isin(routed_tickers)].copy()
    else:
        routed_current = pd.DataFrame()
    bars = pd.concat([prior_bars, routed_current], ignore_index=True)
    bars = bars[bars["tod"].between(PRE_START, AH_END)].copy() if not bars.empty else bars
    event_context = context[context["ticker"].isin(routed_tickers)].copy()
    event_context = event_context[["ticker", "date", "prev_date", "prev_open", "prev_high", "prev_low", "prev_close", "prev_volume"]]

    event_dir = write_cycle(
        args.out_dir,
        0,
        "seed",
        asof,
        {"universe_context": event_context, "bar_delta": bars},
        {
            "trade_date": trade_date,
            "prev_date": prev_date,
            "routed_tickers": len(routed_tickers),
            "seed_bar_tickers": len(seed_tickers),
        },
    )
    routed.to_csv(args.out_dir / "routed_tickers.csv", index=False)
    print(f"Saved seed event to {args.out_dir}", flush=True)
    if args.post_url:
        print(f"Posting seed event to {args.post_url}", flush=True)
        receipt = post_event_dir(event_dir, args.post_url, token=args.post_token, source="stage1_polygon_fetcher")
        print(json.dumps(receipt, indent=2, sort_keys=True), flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, default=DEFAULT_ENV)
    parser.add_argument("--date", default="")
    parser.add_argument("--prev-date", default="")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--tickers", default="", help="Optional comma-separated ticker allowlist for smoke tests.")
    parser.add_argument("--reference-limit", type=int, default=1000)
    parser.add_argument("--max-universe", type=int, default=0)
    parser.add_argument("--max-bar-tickers", type=int, default=25)
    parser.add_argument("--re-route-extension", type=float, default=0.30)
    parser.add_argument("--ge-gap-min", type=float, default=0.40)
    parser.add_argument("--ge-pm-high-ext-min", type=float, default=GE_MIN_PM_HIGH_EXT)
    parser.add_argument("--ge-pm-selloff-max", type=float, default=0.0)
    parser.add_argument("--d2-prev-high-open-min", type=float, default=0.20)
    parser.add_argument("--post-url", default="", help="Optional receiver URL, e.g. http://1.2.3.4:8080/events.")
    parser.add_argument("--post-token", default="")
    parser.add_argument("--loop", action="store_true", help="Run the production multi-day polling loop instead of a one-shot seed fetch.")
    parser.add_argument("--single-day", action="store_true", help="With --loop, run one trade date and exit after the stop time.")
    parser.add_argument("--start-time", default="09:20:00")
    parser.add_argument("--stop-time", default="14:05:00")
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--snapshot-batch-size", type=int, default=500)
    parser.add_argument("--max-active-tickers", type=int, default=0)
    parser.add_argument("--local-retention-days", type=int, default=7)
    parser.add_argument("--go-gap-min", type=float, default=0.80)
    parser.add_argument("--go-gap-max", type=float, default=9.99)
    parser.add_argument("--go-pm-selloff-min", type=float, default=-1.0)
    parser.add_argument("--go-pm-selloff-max", type=float, default=0.0)
    parser.add_argument("--go-min-pm-dollar-volume", type=float, default=200_000.0)
    parser.set_defaults(func=run_seed)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.loop:
        if args.single_day:
            run_loop(args)
        else:
            run_forever(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
