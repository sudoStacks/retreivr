"""Persistence helpers for downloaded Spotify tracks by ISRC."""

from __future__ import annotations

import os
import sqlite3

from db.migrations import ensure_downloaded_music_tracks_table

_DEFAULT_DB_ENV_KEY = "RETREIVR_DB_PATH"


def _resolve_db_path() -> str:
    return os.environ.get(_DEFAULT_DB_ENV_KEY, os.path.join(os.getcwd(), "retreivr.sqlite3"))


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or _resolve_db_path(), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    ensure_downloaded_music_tracks_table(conn)
    return conn


def has_downloaded_isrc(playlist_id: str, isrc: str) -> bool:
    """Return True when an ISRC is already recorded for a playlist."""
    pid = (playlist_id or "").strip()
    track_isrc = (isrc or "").strip()
    if not pid or not track_isrc:
        return False

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM downloaded_music_tracks
            WHERE playlist_id=? AND isrc=?
            LIMIT 1
            """,
            (pid, track_isrc),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def record_downloaded_track(playlist_id: str, isrc: str, file_path: str) -> None:
    """Insert a downloaded track record for playlist/idempotency tracking."""
    pid = (playlist_id or "").strip()
    track_isrc = (isrc or "").strip()
    path = (file_path or "").strip()
    if not pid:
        raise ValueError("playlist_id is required")
    if not track_isrc:
        raise ValueError("isrc is required")
    if not path:
        raise ValueError("file_path is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO downloaded_music_tracks (playlist_id, isrc, file_path)
            VALUES (?, ?, ?)
            """,
            (pid, track_isrc, path),
        )
        conn.commit()
    finally:
        conn.close()

