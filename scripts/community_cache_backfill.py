#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engine.community_publish_backfill import run_publish_backfill
from engine.core import load_config
from engine.paths import build_engine_paths, resolve_config_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill community cache proposals from the canonical music library.")
    parser.add_argument("--config", default=None, help="Optional config file path relative to CONFIG_DIR or absolute.")
    parser.add_argument("--dry-run", action="store_true", help="Inspect and repair metadata without writing publish proposals.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of audio files to inspect.")
    args = parser.parse_args()

    config_path = resolve_config_path(args.config)
    if not os.path.exists(config_path):
        raise SystemExit(f"Config file not found: {config_path}")

    paths = build_engine_paths()
    config = load_config(config_path, write_back_defaults=True)
    summary = run_publish_backfill(
        db_path=paths.db_path,
        config=config,
        dry_run=bool(args.dry_run),
        limit=args.limit,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
