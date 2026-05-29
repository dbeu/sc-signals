#!/usr/bin/env python3
"""Send one Stage 1 event directory to a Stage 2 receiver."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from event_transport import post_event_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-dir", type=Path, required=True)
    parser.add_argument("--url", required=True, help="Receiver URL, e.g. http://1.2.3.4:8080/events")
    parser.add_argument("--token", default=os.environ.get("SC_STAGE1_TOKEN", ""))
    parser.add_argument("--source", default="manual_send_event_dir")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    receipt = post_event_dir(args.event_dir, args.url, token=args.token, source=args.source)
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
