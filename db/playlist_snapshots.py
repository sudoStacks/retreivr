"""Persistence for playlist snapshots and normalized snapshot items."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from db.migrations import ensure_playlist_snapshot_tables


@dataclass(frozen=True)
class SnapshotWriteResult:
    """Result payload for snapshot writes."""

    inserted: bool
    snapshot_db_id: int | None
    reason: str | None = None


class PlaylistSnapshotStore:
    """SQLite-backed playlist snapshot store."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def ensure_schema(self) -> None:
        """Create required snapshot tables if missing."""
        conn = self._connect()
        try:
            ensure_playlist_snapshot_tables(conn)
        finally:
            conn.close()

    def get_latest_snapshot(self, source: str, playlist_id: str) -> dict[str, Any] | None:
        """Return latest snapshot and items for a playlist."""
        conn = self._connect()
        try:
            ensure_playlist_snapshot_tables(conn)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, source, playlist_id, snapshot_id, fetched_at, track_count, raw_json
                FROM playlist_snapshots
                WHERE source=? AND playlist_id=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (source, playlist_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            snapshot = dict(row)
            snapshot["items"] = self._get_snapshot_items(cur, int(row["id"]))
            return snapshot
        finally:
            conn.close()

    def get_latest_track_uris(self, source: str, playlist_id: str) -> list[str]:
        """Return ordered track URIs for the latest snapshot."""
        snapshot = self.get_latest_snapshot(source, playlist_id)
        if not snapshot:
            return []
        return [item["track_uri"] for item in snapshot["items"] if item.get("track_uri")]

    def insert_snapshot(
        self,
        *,
        source: str,
        playlist_id: str,
        snapshot_id: str,
        items: list[dict[str, Any]],
        fetched_at: str | None = None,
        raw_json: str | None = None,
    ) -> SnapshotWriteResult:
        """Insert a new snapshot and its normalized item rows."""
        now = fetched_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        conn = self._connect()
        try:
            ensure_playlist_snapshot_tables(conn)
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                """
                SELECT id, snapshot_id
                FROM playlist_snapshots
                WHERE source=? AND playlist_id=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (source, playlist_id),
            )
            previous = cur.fetchone()
            if previous and previous["snapshot_id"] == snapshot_id:
                conn.commit()
                return SnapshotWriteResult(
                    inserted=False,
                    snapshot_db_id=int(previous["id"]),
                    reason="snapshot_unchanged",
                )

            try:
                cur.execute(
                    """
                    INSERT INTO playlist_snapshots (
                        source, playlist_id, snapshot_id, fetched_at, track_count, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (source, playlist_id, snapshot_id, now, len(items), raw_json),
                )
            except sqlite3.IntegrityError:
                cur.execute(
                    """
                    SELECT id
                    FROM playlist_snapshots
                    WHERE source=? AND playlist_id=? AND snapshot_id=?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (source, playlist_id, snapshot_id),
                )
                existing = cur.fetchone()
                conn.commit()
                return SnapshotWriteResult(
                    inserted=False,
                    snapshot_db_id=int(existing["id"]) if existing else None,
                    reason="snapshot_exists",
                )

            snapshot_db_id = int(cur.lastrowid)
            cur.executemany(
                """
                INSERT INTO playlist_snapshot_items (
                    snapshot_db_id, position, track_uri, track_id, added_at, added_by, is_local, name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_db_id,
                        int(item.get("position", index)),
                        item.get("uri"),
                        item.get("track_id"),
                        item.get("added_at"),
                        item.get("added_by"),
                        1 if bool(item.get("is_local")) else 0,
                        item.get("name"),
                    )
                    for index, item in enumerate(items)
                ],
            )
            conn.commit()
            return SnapshotWriteResult(inserted=True, snapshot_db_id=snapshot_db_id)
        finally:
            conn.close()

    def _get_snapshot_items(self, cur: sqlite3.Cursor, snapshot_db_id: int) -> list[dict[str, Any]]:
        cur.execute(
            """
            SELECT
                id, snapshot_db_id, position, track_uri, track_id, added_at, added_by, is_local, name
            FROM playlist_snapshot_items
            WHERE snapshot_db_id=?
            ORDER BY position ASC
            """,
            (snapshot_db_id,),
        )
        items = [dict(row) for row in cur.fetchall()]
        for item in items:
            item["is_local"] = bool(item.get("is_local"))
        return items
