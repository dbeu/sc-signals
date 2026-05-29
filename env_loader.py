#!/usr/bin/env python3
"""Tiny .env loader for deployment without extra dependencies."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: Path = Path(".env"), override: bool = False) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if override or key not in os.environ:
            os.environ[key] = value
