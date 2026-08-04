from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable


MUSIC_LIBRARY_INDEX_LIMIT = 100_000


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_music_library_index(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS music_library_index (
            path TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            artist TEXT NOT NULL,
            artist_key TEXT NOT NULL,
            album TEXT NOT NULL,
            album_key TEXT NOT NULL,
            stream_url TEXT NOT NULL,
            downloaded_at INTEGER NOT NULL DEFAULT 0,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            file_ext TEXT,
            media_type TEXT,
            artwork_local_path TEXT,
            recording_mbid TEXT,
            mb_release_id TEXT,
            mb_release_group_id TEXT,
            indexed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_library_recent "
        "ON music_library_index(downloaded_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_library_artist_album "
        "ON music_library_index(artist_key, album_key)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS music_library_index_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            status TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            item_count INTEGER NOT NULL DEFAULT 0,
            error TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO music_library_index_state
            (singleton, status, item_count)
        VALUES (1, 'never_built', 0)
        """
    )
    conn.commit()


def get_music_library_index_state(db_path: str) -> dict[str, Any]:
    with connect(db_path) as conn:
        ensure_music_library_index(conn)
        row = conn.execute(
            "SELECT status, started_at, completed_at, item_count, error "
            "FROM music_library_index_state WHERE singleton=1"
        ).fetchone()
    return dict(row) if row is not None else {
        "status": "never_built",
        "started_at": None,
        "completed_at": None,
        "item_count": 0,
        "error": None,
    }


def mark_music_library_index_stale(db_path: str) -> None:
    with connect(db_path) as conn:
        ensure_music_library_index(conn)
        conn.execute(
            """
            UPDATE music_library_index_state
            SET status=CASE WHEN status='building' THEN status ELSE 'stale' END
            WHERE singleton=1
            """
        )
        conn.commit()


def list_indexed_music(db_path: str, *, limit: int = 250) -> list[dict[str, Any]]:
    normalized_limit = max(1, min(int(limit), 5000))
    with connect(db_path) as conn:
        ensure_music_library_index(conn)
        rows = conn.execute(
            """
            SELECT path, title, artist, artist_key, album, album_key, stream_url,
                   downloaded_at, size_bytes, file_ext, media_type,
                   artwork_local_path, recording_mbid, mb_release_id,
                   mb_release_group_id
            FROM music_library_index
            ORDER BY downloaded_at DESC, artist_key, album_key, title
            LIMIT ?
            """,
            (normalized_limit,),
        ).fetchall()
    return [
        {
            "id": row["path"],
            "local_path": row["path"],
            "kind": "local",
            **{key: row[key] for key in row.keys() if key != "path"},
        }
        for row in rows
    ]


def rebuild_music_library_index(
    db_path: str,
    config: dict[str, Any],
    *,
    scanner: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any]:
    started_at = utc_now()
    with connect(db_path) as conn:
        ensure_music_library_index(conn)
        conn.execute(
            "UPDATE music_library_index_state SET status='building', started_at=?, error=NULL WHERE singleton=1",
            (started_at,),
        )
        conn.commit()

    try:
        items = scanner(config, limit=MUSIC_LIBRARY_INDEX_LIMIT)
        indexed_at = utc_now()
        rows = []
        for item in items:
            path = str(item.get("local_path") or item.get("id") or "").strip()
            if not path:
                continue
            rows.append(
                (
                    path,
                    str(item.get("title") or "").strip() or "Unknown Track",
                    str(item.get("artist") or "").strip() or "Unknown Artist",
                    str(item.get("artist_key") or item.get("artist") or "").strip().lower(),
                    str(item.get("album") or "").strip() or "Unknown Album",
                    str(item.get("album_key") or item.get("album") or "").strip().lower(),
                    str(item.get("stream_url") or "").strip(),
                    int(item.get("downloaded_at") or 0),
                    int(item.get("size_bytes") or 0),
                    str(item.get("file_ext") or "").strip() or None,
                    str(item.get("media_type") or "").strip() or None,
                    str(item.get("artwork_local_path") or "").strip() or None,
                    str(item.get("recording_mbid") or "").strip() or None,
                    str(item.get("mb_release_id") or "").strip() or None,
                    str(item.get("mb_release_group_id") or "").strip() or None,
                    indexed_at,
                )
            )
        with connect(db_path) as conn:
            ensure_music_library_index(conn)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM music_library_index")
            conn.executemany(
                """
                INSERT INTO music_library_index (
                    path, title, artist, artist_key, album, album_key, stream_url,
                    downloaded_at, size_bytes, file_ext, media_type,
                    artwork_local_path, recording_mbid, mb_release_id,
                    mb_release_group_id, indexed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.execute(
                """
                UPDATE music_library_index_state
                SET status='ready', completed_at=?, item_count=?, error=NULL
                WHERE singleton=1
                """,
                (indexed_at, len(rows)),
            )
            conn.commit()
        return {"status": "ready", "item_count": len(rows), "completed_at": indexed_at}
    except Exception as exc:
        error = str(exc)[:1000]
        with connect(db_path) as conn:
            ensure_music_library_index(conn)
            conn.execute(
                "UPDATE music_library_index_state SET status='error', error=? WHERE singleton=1",
                (error,),
            )
            conn.commit()
        raise
