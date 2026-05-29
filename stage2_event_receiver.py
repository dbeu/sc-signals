#!/usr/bin/env python3
"""Stage 2 event receiver.

It accepts Stage 1 event directories, writes them to an inbox, processes them
with the signal engine, and optionally sends Discord notifications.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pandas as pd

from common import DEFAULT_PARAM_CSV
from discord_notifier import DiscordNotifier, send_signal_notifications
from env_loader import load_dotenv
from stage2_signal_engine import SignalState, load_params, process_event_dir, save_signals


ALLOWED_FILES = {
    "manifest.json",
    "universe_context.parquet",
    "bar_delta.parquet",
    "routed_tickers.csv",
}
MAX_BODY_BYTES = 100 * 1024 * 1024


class ReceiverConfig:
    inbox_dir: Path
    signals_dir: Path
    param_csv: Path
    event_retention_days: int
    signal_retention_days: int
    token: str
    signal_state: SignalState
    discord: DiscordNotifier | None
    trade_date: str


def safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(value))
    return cleaned.strip("._") or "event"


def json_response(handler: BaseHTTPRequestHandler, status: HTTPStatus, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def unauthorized(handler: BaseHTTPRequestHandler) -> bool:
    token = ReceiverConfig.token
    if not token:
        return False
    header = handler.headers.get("Authorization", "")
    if header == f"Bearer {token}":
        return False
    json_response(handler, HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "unauthorized"})
    return True


def cleanup_old_children(root: Path, retention_days: int) -> None:
    if retention_days <= 0 or not root.exists():
        return
    cutoff = datetime.now(timezone.utc).timestamp() - retention_days * 86400
    for child in root.iterdir():
        try:
            mtime = child.stat().st_mtime
        except FileNotFoundError:
            continue
        if mtime >= cutoff:
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
        print(f"removed old archive: {child}", flush=True)


def event_trade_date(event_dir: Path) -> str:
    manifest_path = event_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("trade_date"):
            return str(manifest["trade_date"])
    context_path = event_dir / "universe_context.parquet"
    if context_path.exists():
        context = pd.read_parquet(context_path, columns=["date"])
        if not context.empty:
            return str(context["date"].iloc[0])
    bars_path = event_dir / "bar_delta.parquet"
    if bars_path.exists():
        bars = pd.read_parquet(bars_path, columns=["date"])
        if not bars.empty:
            return str(bars["date"].iloc[-1])
    return ""


def state_for_trade_date(trade_date: str) -> None:
    if trade_date and trade_date != ReceiverConfig.trade_date:
        ReceiverConfig.signal_state = SignalState(load_params(ReceiverConfig.param_csv))
        ReceiverConfig.trade_date = trade_date
        print(f"reset signal state for trade_date={trade_date}", flush=True)


def signals_out_dir(trade_date: str) -> Path:
    return ReceiverConfig.signals_dir / trade_date if trade_date else ReceiverConfig.signals_dir


class Stage2EventHandler(BaseHTTPRequestHandler):
    server_version = "Stage2EventReceiver/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.log_date_time_string()} {self.client_address[0]} {fmt % args}", flush=True)

    def do_GET(self) -> None:
        if self.path != "/health":
            json_response(self, HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
            return
        json_response(
            self,
            HTTPStatus.OK,
            {
                "ok": True,
                "service": "stage2_event_receiver",
                "inbox_dir": str(ReceiverConfig.inbox_dir),
                "signals_dir": str(ReceiverConfig.signals_dir),
                "auth_enabled": bool(ReceiverConfig.token),
                "discord_enabled": ReceiverConfig.discord is not None,
                "signals_generated": len(ReceiverConfig.signal_state.signals),
                "trade_date": ReceiverConfig.trade_date,
            },
        )

    def do_POST(self) -> None:
        if self.path != "/events":
            json_response(self, HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
            return
        if unauthorized(self):
            return

        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length <= 0:
            json_response(self, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "empty_body"})
            return
        if content_length > MAX_BODY_BYTES:
            json_response(self, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"ok": False, "error": "body_too_large"})
            return

        try:
            payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
            receipt = self.save_event(payload)
            trade_date = event_trade_date(Path(receipt["event_dir"]))
            state_for_trade_date(trade_date)
            new_signals = process_event_dir(ReceiverConfig.signal_state, Path(receipt["event_dir"]))
            save_signals(ReceiverConfig.signal_state, signals_out_dir(trade_date))
            send_signal_notifications(ReceiverConfig.discord, new_signals)
            cleanup_old_children(ReceiverConfig.inbox_dir, ReceiverConfig.event_retention_days)
            cleanup_old_children(ReceiverConfig.signals_dir, ReceiverConfig.signal_retention_days)
            receipt["trade_date"] = trade_date
            receipt["new_signals"] = len(new_signals)
            receipt["signals_total"] = len(ReceiverConfig.signal_state.signals)
            (Path(receipt["event_dir"]) / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        except Exception as exc:
            json_response(self, HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            return

        json_response(self, HTTPStatus.ACCEPTED, receipt)

    def save_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        source = safe_name(payload.get("source", "stage1"))
        event_name = safe_name(payload.get("event_name", "event"))
        files = payload.get("files", [])
        if not isinstance(files, list) or not files:
            raise ValueError("payload.files must be a non-empty list")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        event_dir = ReceiverConfig.inbox_dir / f"{stamp}_{source}_{event_name}"
        event_dir.mkdir(parents=True, exist_ok=False)

        saved_files = []
        total_bytes = 0
        for item in files:
            if not isinstance(item, dict):
                raise ValueError("file item must be an object")
            name = safe_name(item.get("name", ""))
            if name not in ALLOWED_FILES:
                raise ValueError(f"file not allowed: {name}")
            raw = base64.b64decode(str(item.get("content_b64", "")), validate=True)
            expected_size = item.get("size")
            if expected_size is not None and int(expected_size) != len(raw):
                raise ValueError(f"size mismatch for {name}")
            (event_dir / name).write_bytes(raw)
            saved_files.append({"name": name, "size": len(raw)})
            total_bytes += len(raw)

        receipt = {
            "ok": True,
            "accepted_at_utc": datetime.now(timezone.utc).isoformat(),
            "event_dir": str(event_dir),
            "event_name": event_name,
            "source": source,
            "files": saved_files,
            "total_bytes": total_bytes,
            "client": self.client_address[0],
        }
        (event_dir / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, default=Path(".env"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--inbox-dir", type=Path, default=Path("stage2_inbox"))
    parser.add_argument("--signals-dir", type=Path, default=Path("stage2_signals"))
    parser.add_argument("--param-csv", type=Path, default=DEFAULT_PARAM_CSV)
    parser.add_argument("--event-retention-days", type=int, default=14)
    parser.add_argument("--signal-retention-days", type=int, default=90)
    parser.add_argument("--token", default=os.environ.get("SC_STAGE1_TOKEN", ""))
    parser.add_argument("--no-discord", action="store_true")
    parser.add_argument("--dry-run-discord", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(args.env)
    if not args.token:
        args.token = os.environ.get("SC_STAGE1_TOKEN", "")
    ReceiverConfig.inbox_dir = args.inbox_dir.resolve()
    ReceiverConfig.inbox_dir.mkdir(parents=True, exist_ok=True)
    ReceiverConfig.signals_dir = args.signals_dir.resolve()
    ReceiverConfig.signals_dir.mkdir(parents=True, exist_ok=True)
    ReceiverConfig.param_csv = args.param_csv
    ReceiverConfig.event_retention_days = args.event_retention_days
    ReceiverConfig.signal_retention_days = args.signal_retention_days
    ReceiverConfig.token = args.token
    ReceiverConfig.signal_state = SignalState(load_params(args.param_csv))
    ReceiverConfig.discord = None if args.no_discord else DiscordNotifier.from_env(dry_run=args.dry_run_discord)
    ReceiverConfig.trade_date = ""
    cleanup_old_children(ReceiverConfig.inbox_dir, ReceiverConfig.event_retention_days)
    cleanup_old_children(ReceiverConfig.signals_dir, ReceiverConfig.signal_retention_days)

    server = ThreadingHTTPServer((args.host, args.port), Stage2EventHandler)
    print(
        (
            f"Listening on http://{args.host}:{args.port} "
            f"inbox={ReceiverConfig.inbox_dir} signals={ReceiverConfig.signals_dir} "
            f"auth={bool(args.token)} discord={ReceiverConfig.discord is not None}"
        ),
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
