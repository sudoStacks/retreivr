"""SQLite migrations for Spotify playlist snapshot persistence."""

from __future__ import annotations

import sqlite3


def ensure_playlist_snapshot_tables(conn: sqlite3.Connection) -> None:
    """Create snapshot tables and indexes when they do not already exist."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_playlist_snapshots_playlist_snapshot "
        "ON playlist_snapshots (playlist_id, snapshot_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_playlist_snapshots_snapshot_lookup "
        "ON playlist_snapshots (playlist_id, snapshot_id)"
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_snapshot_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            spotify_track_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            added_at TEXT,
            FOREIGN KEY (snapshot_id) REFERENCES playlist_snapshots(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_playlist_snapshot_items_unique_position "
        "ON playlist_snapshot_items (snapshot_id, spotify_track_id, position)"
    )
    conn.commit()
