#!/usr/bin/env python3
from __future__ import annotations

import sys

from app.musicbrainz import search_release_groups


def main() -> int:
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        print("Usage: scripts/musicbrainz_smoke.py <query>")
        return 1
    rows = search_release_groups(query, limit=5)
    print(f"query={query!r} candidates={len(rows)}")
    for idx, row in enumerate(rows, start=1):
        print(
            f"{idx}. {row.get('title')} | {row.get('artist_credit')} | "
            f"{row.get('first_release_date')} | score={row.get('score')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
