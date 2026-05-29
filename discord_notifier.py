#!/usr/bin/env python3
"""Discord notifications for live signals."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Any


DISCORD_API_BASE = "https://discord.com/api/v10"


class DiscordNotifier:
    def __init__(self, token: str, channel_id: str, dry_run: bool = False) -> None:
        self.token = token
        self.channel_id = str(channel_id)
        self.dry_run = dry_run

    @classmethod
    def from_env(cls, dry_run: bool = False) -> "DiscordNotifier | None":
        token = os.environ.get("DISCORD_TOKEN", "").strip()
        channel_id = os.environ.get("DISCORD_CHANNEL_ID", "").strip()
        if not token or not channel_id:
            return None
        return cls(token=token, channel_id=channel_id, dry_run=dry_run)

    def send_message(self, message: str) -> None:
        if self.dry_run:
            print(f"[Discord dry-run]\n{message}", flush=True)
            return
        payload = json.dumps({"content": message}).encode("utf-8")
        request = urllib.request.Request(
            f"{DISCORD_API_BASE}/channels/{self.channel_id}/messages",
            data=payload,
            headers={
                "Authorization": f"Bot {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "sc-signals/0.1",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Discord send failed with HTTP {exc.code}: {detail}") from exc


def format_signal_line(signal: dict[str, Any]) -> str:
    ticker = str(signal.get("ticker", "")).upper()
    date = signal.get("date", "")
    time = signal.get("time", "")
    entry = float(signal.get("entry_price", 0.0))
    phase = str(signal.get("signal_phase", "")).strip()
    parts = [f"**{ticker}**", f"{date} {time}"]
    if phase:
        parts.append(phase)
    parts.append(f"entry ${entry:.2f}")
    if "extension" in signal:
        parts.append(f"ext {float(signal['extension']) * 100:.1f}%")
    if "ema_5_ext" in signal:
        parts.append(f"ema5ext {float(signal['ema_5_ext']) * 100:.1f}%")
    if "gap" in signal:
        parts.append(f"gap {float(signal['gap']) * 100:.1f}%")
    if "pm_selloff" in signal:
        parts.append(f"pm_selloff {float(signal['pm_selloff']) * 100:.1f}%")
    return " | ".join(parts)


def format_signal_message(signals: list[dict[str, Any]]) -> str:
    if not signals:
        return ""
    lines: list[str] = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        grouped[str(signal.get("strategy", "")).upper()].append(signal)
    for strategy in sorted(grouped):
        rows = grouped[strategy]
        lines.append(f"**{strategy}** ({len(rows)})")
        lines.extend(f"- {format_signal_line(signal)}" for signal in rows)
        lines.append("")
    if lines[-1] == "":
        lines.pop()
    return "\n".join(lines)


def send_signal_notifications(notifier: DiscordNotifier | None, signals: list[dict[str, Any]]) -> None:
    if notifier is None or not signals:
        return
    # Keep well below Discord's 2,000 character message limit.
    batch: list[dict[str, Any]] = []
    for signal in signals:
        candidate = batch + [signal]
        if len(format_signal_message(candidate)) > 1800:
            notifier.send_message(format_signal_message(batch))
            batch = [signal]
        else:
            batch = candidate
    if batch:
        notifier.send_message(format_signal_message(batch))
