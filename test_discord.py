#!/usr/bin/env python3
"""Send a small Discord smoke-test message."""

from __future__ import annotations

import argparse
from pathlib import Path

from discord_notifier import DiscordNotifier
from env_loader import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=Path, default=Path(".env"))
    parser.add_argument("--message", default="SC Signals Discord smoke test")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(args.env)
    notifier = DiscordNotifier.from_env(dry_run=args.dry_run)
    if notifier is None:
        raise SystemExit("DISCORD_TOKEN and DISCORD_CHANNEL_ID are required.")
    notifier.send_message(args.message)
    print("Discord test message sent.")


if __name__ == "__main__":
    main()
