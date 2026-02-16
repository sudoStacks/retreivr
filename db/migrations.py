"""SQLite migrations for playlist snapshot storage."""

from __future__ import annotations

import sqlite3


def ensure_playlist_snapshot_tables(conn: sqlite3.Connection) -> None:
    """Ensure playlist snapshot tables and indexes exist."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            playlist_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            track_count INTEGER NOT NULL,
            raw_json TEXT,
            UNIQUE (source, playlist_id, snapshot_id)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_playlist_snapshots_lookup "
        "ON playlist_snapshots (source, playlist_id, id DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_playlist_snapshots_fetched_at "
        "ON playlist_snapshots (fetched_at)"
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_snapshot_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_db_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            track_uri TEXT,
            track_id TEXT,
            added_at TEXT,
            added_by TEXT,
            is_local INTEGER NOT NULL DEFAULT 0,
            name TEXT,
            FOREIGN KEY (snapshot_db_id) REFERENCES playlist_snapshots(id) ON DELETE CASCADE,
            UNIQUE (snapshot_db_id, position)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_playlist_snapshot_items_snapshot_position "
        "ON playlist_snapshot_items (snapshot_db_id, position)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_playlist_snapshot_items_track_uri "
        "ON playlist_snapshot_items (track_uri)"
    )
    conn.commit()

