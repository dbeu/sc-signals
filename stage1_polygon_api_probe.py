#!/usr/bin/env python3
"""Probe the Polygon/Massive endpoints needed by Stage 1.

This is intentionally small and dependency-light. It verifies endpoint access
and response shapes without printing API keys.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_ENV = Path(".env")
BASE_URL = "https://api.polygon.io"


def load_api_key(env_path: Path = DEFAULT_ENV) -> str:
    env_value = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
    if env_value:
        return env_value.strip()
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.strip().startswith(("MASSIVE_API_KEY", "POLYGON_API_KEY")):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("No MASSIVE_API_KEY/POLYGON_API_KEY found.")


def polygon_get(path: str, params: dict[str, Any], api_key: str, timeout: int = 30) -> dict[str, Any]:
    query = urllib.parse.urlencode({**params, "apiKey": api_key})
    url = f"{BASE_URL}{path}?{query}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
        payload["_http_status"] = response.status
        return payload


def fetch_reference_tickers(api_key: str, limit: int = 5) -> dict[str, Any]:
    return polygon_get(
        "/v3/reference/tickers",
        {"market": "stocks", "active": "true", "limit": limit},
        api_key,
    )


def fetch_grouped_daily(api_key: str, date: str, adjusted: bool = True) -> dict[str, Any]:
    return polygon_get(
        f"/v2/aggs/grouped/locale/us/market/stocks/{date}",
        {"adjusted": str(adjusted).lower()},
        api_key,
    )


def fetch_snapshots(api_key: str, tickers: list[str]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if tickers:
        params["tickers"] = ",".join(tickers)
    return polygon_get("/v2/snapshot/locale/us/markets/stocks/tickers", params, api_key)


def fetch_minute_aggs(api_key: str, ticker: str, start_date: str, end_date: str, limit: int = 10) -> dict[str, Any]:
    return polygon_get(
        f"/v2/aggs/ticker/{ticker}/range/1/minute/{start_date}/{end_date}",
        {"adjusted": "true", "sort": "asc", "limit": limit},
        api_key,
    )


def summarize_reference(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload.get("results", [])
    first = results[0] if results else {}
    return {
        "http": payload.get("_http_status"),
        "status": payload.get("status"),
        "count": len(results),
        "sample": {key: first.get(key) for key in ["ticker", "name", "market", "locale", "primary_exchange", "type", "active"]},
    }


def summarize_grouped_daily(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload.get("results", [])
    first = results[0] if results else {}
    return {
        "http": payload.get("_http_status"),
        "status": payload.get("status"),
        "resultsCount": payload.get("resultsCount"),
        "count": len(results),
        "sample": {key: first.get(key) for key in ["T", "o", "h", "l", "c", "v", "n", "t"]},
    }


def summarize_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload.get("tickers", []) or payload.get("results", [])
    first = results[0] if results else {}
    return {
        "http": payload.get("_http_status"),
        "status": payload.get("status"),
        "count": len(results),
        "sample_ticker": first.get("ticker") or first.get("T"),
        "has_day": "day" in first,
        "has_prevDay": "prevDay" in first,
        "has_min": "min" in first,
        "has_lastTrade": "lastTrade" in first,
    }


def summarize_aggs(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload.get("results", [])
    first = results[0] if results else {}
    return {
        "http": payload.get("_http_status"),
        "status": payload.get("status"),
        "resultsCount": payload.get("resultsCount"),
        "count": len(results),
        "first_bar": {key: first.get(key) for key in ["o", "h", "l", "c", "v", "n", "t"]},
    }


def run(args: argparse.Namespace) -> None:
    api_key = load_api_key(args.env)
    tickers = [ticker.strip().upper() for ticker in args.tickers.split(",") if ticker.strip()]
    checks = {
        "reference_tickers": summarize_reference(fetch_reference_tickers(api_key, limit=args.reference_limit)),
        "grouped_daily": summarize_grouped_daily(fetch_grouped_daily(api_key, args.date)),
        "snapshot": summarize_snapshot(fetch_snapshots(api_key, tickers)),
        "minute_aggs": summarize_aggs(fetch_minute_aggs(api_key, tickers[0], args.date, args.date, limit=args.agg_limit)),
    }
    print(json.dumps(checks, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, default=DEFAULT_ENV)
    parser.add_argument("--date", default="2026-05-27")
    parser.add_argument("--tickers", default="AAPL,MSFT")
    parser.add_argument("--reference-limit", type=int, default=3)
    parser.add_argument("--agg-limit", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
