#!/usr/bin/env python3
"""Small JSON transport for Stage 1 event directories."""

from __future__ import annotations

import base64
import json
import mimetypes
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ALLOWED_EVENT_FILES = {
    "manifest.json",
    "universe_context.parquet",
    "bar_delta.parquet",
    "routed_tickers.csv",
}


def pack_event_dir(event_dir: Path, source: str = "stage1") -> dict[str, Any]:
    event_dir = event_dir.resolve()
    if not event_dir.is_dir():
        raise ValueError(f"Event directory does not exist: {event_dir}")

    files = []
    for path in sorted(event_dir.iterdir()):
        if not path.is_file() or path.name not in ALLOWED_EVENT_FILES:
            continue
        raw = path.read_bytes()
        files.append(
            {
                "name": path.name,
                "content_type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                "size": len(raw),
                "content_b64": base64.b64encode(raw).decode("ascii"),
            }
        )

    if not files:
        raise ValueError(f"No allowed event files found in {event_dir}")

    return {
        "source": source,
        "event_name": event_dir.name,
        "sent_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": files,
    }


def post_event_dir(event_dir: Path, url: str, token: str = "", source: str = "stage1") -> dict[str, Any]:
    payload = pack_event_dir(event_dir, source=source)
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            text = response.read().decode("utf-8")
            return json.loads(text) if text else {"status": response.status}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"POST {url} failed with HTTP {exc.code}: {detail}") from exc
