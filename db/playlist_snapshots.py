"""Persistence helpers for Spotify playlist snapshots."""

from __future__ import annotations

import os
import sqlite3
import hashlib
from dataclasses import dataclass
from typing import Any

from db.migrations import ensure_playlist_snapshot_tables

_DEFAULT_DB_ENV_KEY = "RETREIVR_DB_PATH"


def _resolve_db_path() -> str:
    return os.environ.get(_DEFAULT_DB_ENV_KEY, os.path.join(os.getcwd(), "retreivr.sqlite3"))


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or _resolve_db_path(), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    ensure_playlist_snapshot_tables(conn)
    return conn


def _normalize_snapshot_rows(items: list[dict[str, Any]]) -> list[tuple[str, int, Any]]:
    rows: list[tuple[str, int, int, Any]] = []
    for idx, item in enumerate(items or []):
        if not isinstance(item, dict):
            continue
        track_id = str(item.get("spotify_track_id") or "").strip()
        if not track_id:
            continue
        try:
            position = int(item.get("position", idx))
        except Exception:
            position = int(idx)
        rows.append((track_id, position, idx, item.get("added_at")))
    rows.sort(key=lambda row: (row[1], row[2], row[0]))
    return [(track_id, position, added_at) for track_id, position, _idx, added_at in rows]


def _snapshot_hash_from_rows(rows: list[tuple[str, int, Any]]) -> str:
    payload = "\n".join(
        f"{idx}|{track_id}|{position}|{added_at or ''}"
        for idx, (track_id, position, added_at) in enumerate(rows)
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class SnapshotWriteResult:
    """Result payload for class-based snapshot writes."""

    inserted: bool
    snapshot_db_id: int | None
    reason: str | None = None


def get_latest_snapshot(playlist_id: str) -> tuple[str | None, list[dict[str, Any]]]:
    """Return latest `(snapshot_id, items)` for a playlist, or `(None, [])` when missing."""
    pid = (playlist_id or "").strip()
    if not pid:
        return None, []

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, snapshot_id
            FROM playlist_snapshots
            WHERE playlist_id=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (pid,),
        )
        row = cur.fetchone()
        if not row:
            return None, []

        snapshot_row_id = int(row["id"])
        snapshot_id = str(row["snapshot_id"])
        cur.execute(
            """
            SELECT spotify_track_id, position, added_at
            FROM playlist_snapshot_items
            WHERE snapshot_id=?
            ORDER BY position ASC, id ASC
            """,
            (snapshot_row_id,),
        )
        items = [dict(item) for item in cur.fetchall()]
        return snapshot_id, items
    finally:
        conn.close()


def store_snapshot(playlist_id: str, snapshot_id: str, items: list[dict[str, Any]]) -> None:
    """Store a snapshot and items only when `snapshot_id` differs from the latest snapshot."""
    pid = (playlist_id or "").strip()
    sid = (snapshot_id or "").strip()
    if not pid:
        raise ValueError("playlist_id is required")
    if not sid:
        raise ValueError("snapshot_id is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        normalized_rows = _normalize_snapshot_rows(items)
        current_hash = _snapshot_hash_from_rows(normalized_rows)
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            SELECT id, snapshot_id
            FROM playlist_snapshots
            WHERE playlist_id=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (pid,),
        )
        latest = cur.fetchone()
        if latest and str(latest["snapshot_id"]) == sid:
            conn.commit()
            return
        if latest:
            cur.execute(
                """
                SELECT spotify_track_id, position, added_at
                FROM playlist_snapshot_items
                WHERE snapshot_id=?
                ORDER BY position ASC, id ASC
                """,
                (int(latest["id"]),),
            )
            previous_rows = [
                (
                    str(item["spotify_track_id"]),
                    int(item["position"]),
                    item["added_at"],
                )
                for item in cur.fetchall()
            ]
            if _snapshot_hash_from_rows(previous_rows) == current_hash:
                conn.commit()
                return

        cur.execute(
            """
            INSERT INTO playlist_snapshots (playlist_id, snapshot_id)
            VALUES (?, ?)
            """,
            (pid, sid),
        )
        snapshot_row_id = int(cur.lastrowid)

        rows = [(snapshot_row_id, track_id, position, added_at) for track_id, position, added_at in normalized_rows]

        if rows:
            cur.executemany(
                """
                INSERT INTO playlist_snapshot_items (
                    snapshot_id, spotify_track_id, position, added_at
                ) VALUES (?, ?, ?, ?)
                """,
                rows,
            )

        conn.commit()
    finally:
        conn.close()


class PlaylistSnapshotStore:
    """Compatibility wrapper around module-level snapshot helpers."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        ensure_playlist_snapshot_tables(conn)
        return conn

    def ensure_schema(self) -> None:
        """Ensure snapshot schema exists."""
        conn = self._connect()
        conn.close()

    def get_latest_snapshot(self, playlist_id: str) -> dict[str, Any] | None:
        """Return latest snapshot metadata and ordered items for `playlist_id`."""
        pid = (playlist_id or "").strip()
        if not pid:
            return None
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, playlist_id, snapshot_id, timestamp
                FROM playlist_snapshots
                WHERE playlist_id=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (pid,),
            )
            row = cur.fetchone()
            if not row:
                return None
            snapshot = dict(row)
            cur.execute(
                """
                SELECT spotify_track_id, position, added_at
                FROM playlist_snapshot_items
                WHERE snapshot_id=?
                ORDER BY position ASC, id ASC
                """,
                (int(row["id"]),),
            )
            items = [dict(item) for item in cur.fetchall()]
            snapshot["items"] = items
            snapshot["track_count"] = len(items)
            snapshot["fetched_at"] = snapshot.get("timestamp")
            snapshot["raw_json"] = None
            return snapshot
        finally:
            conn.close()

    def store_snapshot(
        self,
        playlist_id: str,
        snapshot_id: str,
        items: list[dict[str, Any]],
    ) -> SnapshotWriteResult:
        """Store snapshot with fast-path skip when unchanged."""
        pid = (playlist_id or "").strip()
        sid = (snapshot_id or "").strip()
        if not pid:
            raise ValueError("playlist_id is required")
        if not sid:
            raise ValueError("snapshot_id is required")

        conn = self._connect()
        try:
            cur = conn.cursor()
            normalized_rows = _normalize_snapshot_rows(items)
            current_hash = _snapshot_hash_from_rows(normalized_rows)
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                """
                SELECT id, snapshot_id
                FROM playlist_snapshots
                WHERE playlist_id=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (pid,),
            )
            latest = cur.fetchone()
            if latest and str(latest["snapshot_id"]) == sid:
                conn.commit()
                return SnapshotWriteResult(
                    inserted=False,
                    snapshot_db_id=int(latest["id"]),
                    reason="snapshot_unchanged",
                )
            if latest:
                cur.execute(
                    """
                    SELECT spotify_track_id, position, added_at
                    FROM playlist_snapshot_items
                    WHERE snapshot_id=?
                    ORDER BY position ASC, id ASC
                    """,
                    (int(latest["id"]),),
                )
                previous_rows = [
                    (
                        str(item["spotify_track_id"]),
                        int(item["position"]),
                        item["added_at"],
                    )
                    for item in cur.fetchall()
                ]
                if _snapshot_hash_from_rows(previous_rows) == current_hash:
                    conn.commit()
                    return SnapshotWriteResult(
                        inserted=False,
                        snapshot_db_id=int(latest["id"]),
                        reason="snapshot_hash_unchanged",
                    )

            cur.execute(
                """
                INSERT INTO playlist_snapshots (playlist_id, snapshot_id)
                VALUES (?, ?)
                """,
                (pid, sid),
            )
            snapshot_row_id = int(cur.lastrowid)
            rows = [(snapshot_row_id, track_id, position, added_at) for track_id, position, added_at in normalized_rows]
            if rows:
                cur.executemany(
                    """
                    INSERT INTO playlist_snapshot_items (
                        snapshot_id, spotify_track_id, position, added_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    rows,
                )
            conn.commit()
            return SnapshotWriteResult(inserted=True, snapshot_db_id=snapshot_row_id)
        finally:
            conn.close()
