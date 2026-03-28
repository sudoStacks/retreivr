from __future__ import annotations

import json
import os
import random
import sqlite3
from pathlib import Path
from urllib.parse import quote
from typing import Any


AUDIO_EXTENSIONS = {".mp3", ".m4a", ".flac", ".aac", ".ogg", ".opus", ".wav", ".alac"}


def ensure_music_player_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS music_player_stations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            seed_type TEXT NOT NULL,
            seed_value TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS music_player_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id TEXT,
            title TEXT,
            artist TEXT,
            stream_url TEXT,
            local_path TEXT,
            source_kind TEXT NOT NULL,
            played_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS music_player_playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS music_player_playlist_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_id INTEGER NOT NULL,
            item_id TEXT,
            title TEXT,
            artist TEXT,
            album TEXT,
            stream_url TEXT,
            local_path TEXT,
            source_kind TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(playlist_id, item_id, local_path, stream_url),
            FOREIGN KEY(playlist_id) REFERENCES music_player_playlists(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def _music_roots(config: dict[str, Any]) -> list[Path]:
    roots: list[Path] = []
    music_cfg = config.get("music") if isinstance(config.get("music"), dict) else {}
    library_path = str(music_cfg.get("library_path") or "").strip()
    if library_path:
        roots.append(Path(library_path).expanduser())
    downloads_root = Path("/downloads")
    music_folder = str(config.get("home_music_download_folder") or config.get("music_download_folder") or "Music").strip()
    if music_folder:
        roots.append(downloads_root / music_folder)
    return roots


def scan_local_library(config: dict[str, Any], *, limit: int = 250) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for root in _music_roots(config):
        try:
            if not root.exists():
                continue
            for path in root.rglob("*"):
                if len(items) >= limit:
                    return items
                if not path.is_file() or path.suffix.lower() not in AUDIO_EXTENSIONS:
                    continue
                resolved = str(path.resolve())
                if resolved in seen:
                    continue
                seen.add(resolved)
                artist = path.parent.parent.name if len(path.parts) >= 3 else ""
                album = path.parent.name
                title = path.stem
                items.append(
                    {
                        "id": resolved,
                        "title": title,
                        "artist": artist,
                        "artist_key": artist.lower(),
                        "album": album,
                        "album_key": album.lower(),
                        "kind": "local",
                        "stream_url": f"/api/player/stream/local?path={quote(resolved, safe='')}",
                        "local_path": resolved,
                    }
                )
        except Exception:
            continue
    return items


def summarize_library(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    tracks = list(items or [])
    artists_map: dict[str, dict[str, Any]] = {}
    albums_map: dict[tuple[str, str], dict[str, Any]] = {}
    for item in tracks:
        artist = str(item.get("artist") or "Unknown Artist").strip() or "Unknown Artist"
        album = str(item.get("album") or "Unknown Album").strip() or "Unknown Album"
        artist_key = artist.lower()
        album_key = album.lower()
        artist_entry = artists_map.setdefault(
            artist_key,
            {"artist": artist, "artist_key": artist_key, "album_count": 0, "track_count": 0},
        )
        artist_entry["track_count"] += 1
        album_entry = albums_map.setdefault(
            (artist_key, album_key),
            {
                "artist": artist,
                "artist_key": artist_key,
                "album": album,
                "album_key": album_key,
                "track_count": 0,
            },
        )
        album_entry["track_count"] += 1
    for artist_key, artist_entry in artists_map.items():
        artist_entry["album_count"] = sum(1 for (a_key, _), _album in albums_map.items() if a_key == artist_key)
    artists = sorted(artists_map.values(), key=lambda entry: (-int(entry["track_count"]), entry["artist"].lower()))
    albums = sorted(albums_map.values(), key=lambda entry: (-int(entry["track_count"]), entry["artist"].lower(), entry["album"].lower()))
    tracks = sorted(tracks, key=lambda entry: (str(entry.get("artist") or "").lower(), str(entry.get("album") or "").lower(), str(entry.get("title") or "").lower()))
    return {"artists": artists, "albums": albums, "tracks": tracks}


def list_stations(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, seed_type, seed_value, created_at, updated_at FROM music_player_stations ORDER BY updated_at DESC, id DESC"
    )
    return [dict(row) for row in cur.fetchall()]


def create_station(conn: sqlite3.Connection, *, name: str, seed_type: str, seed_value: str) -> dict[str, Any]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO music_player_stations (name, seed_type, seed_value)
        VALUES (?, ?, ?)
        """,
        (name.strip() or seed_value.strip(), seed_type.strip() or "artist", seed_value.strip()),
    )
    conn.commit()
    station_id = int(cur.lastrowid)
    cur.execute(
        "SELECT id, name, seed_type, seed_value, created_at, updated_at FROM music_player_stations WHERE id=? LIMIT 1",
        (station_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else {}


def delete_station(conn: sqlite3.Connection, station_id: int) -> None:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute("DELETE FROM music_player_stations WHERE id=?", (int(station_id),))
    conn.commit()


def build_station_queue(conn: sqlite3.Connection, config: dict[str, Any], *, station: dict[str, Any], limit: int = 25) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    seed_value = str(station.get("seed_value") or "").strip().lower()
    local_tracks = scan_local_library(config, limit=max(limit * 3, 50))
    ranked_local = []
    for item in local_tracks:
        haystack = " ".join(
            [
                str(item.get("title") or ""),
                str(item.get("artist") or ""),
                str(item.get("album") or ""),
            ]
        ).lower()
        score = 0
        if seed_value and seed_value in haystack:
            score += 5
        if seed_value and str(item.get("artist") or "").lower() == seed_value:
            score += 4
        if str(station.get("seed_type") or "") == "favorites":
            score += 1
        ranked_local.append((score, random.random(), item))
    ranked_local.sort(key=lambda entry: (-entry[0], entry[1]))
    queue = [entry[2] for entry in ranked_local[:limit]]
    if len(queue) >= limit:
        return queue

    cur = conn.cursor()
    seed_like = f"%{seed_value}%"
    cur.execute(
        """
        SELECT recording_mbid, source, source_url, source_payload_json, verification_status, verification_count
        FROM resolution_sources
        WHERE verification_status IN ('verified', 'pending_verification', 'pending')
          AND (
            lower(source_payload_json) LIKE ?
            OR lower(source_url) LIKE ?
          )
        ORDER BY verification_status='verified' DESC, verification_count DESC, updated_at DESC
        LIMIT ?
        """,
        (seed_like, seed_like, max(limit * 3, 30)),
    )
    seen_urls = {str(item.get("stream_url") or "") for item in queue}
    for row in cur.fetchall():
        url = str(row["source_url"] or "").strip()
        if not url or url in seen_urls:
            continue
        payload = {}
        try:
            payload = json.loads(str(row["source_payload_json"] or "{}"))
        except Exception:
            payload = {}
        queue.append(
            {
                "id": f"cached:{row['recording_mbid']}:{url}",
                "title": str(payload.get("title") or payload.get("track") or row["recording_mbid"] or "Cached Match"),
                "artist": str(payload.get("artist") or ""),
                "album": str(payload.get("album") or ""),
                "kind": "cached",
                "stream_url": url,
                "recording_mbid": str(row["recording_mbid"] or ""),
                "source": str(row["source"] or ""),
            }
        )
        seen_urls.add(url)
        if len(queue) >= limit:
            break
    return queue[:limit]


def add_history_entry(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO music_player_history (item_id, title, artist, stream_url, local_path, source_kind)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            str(payload.get("item_id") or "").strip() or None,
            str(payload.get("title") or "").strip() or None,
            str(payload.get("artist") or "").strip() or None,
            str(payload.get("stream_url") or "").strip() or None,
            str(payload.get("local_path") or "").strip() or None,
            str(payload.get("source_kind") or "unknown").strip() or "unknown",
        ),
    )
    conn.commit()


def list_history(conn: sqlite3.Connection, *, limit: int = 50) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, item_id, title, artist, stream_url, local_path, source_kind, played_at
        FROM music_player_history
        ORDER BY played_at DESC, id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return [dict(row) for row in cur.fetchall()]


def list_playlists(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.id, p.name, p.created_at, p.updated_at, COUNT(i.id) AS item_count
        FROM music_player_playlists p
        LEFT JOIN music_player_playlist_items i ON i.playlist_id = p.id
        GROUP BY p.id
        ORDER BY lower(p.name) ASC, p.id ASC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def create_playlist(conn: sqlite3.Connection, *, name: str) -> dict[str, Any]:
    ensure_music_player_tables(conn)
    playlist_name = str(name or "").strip()
    if not playlist_name:
        raise ValueError("playlist name is required")
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO music_player_playlists (name)
        VALUES (?)
        """,
        (playlist_name,),
    )
    conn.commit()
    playlist_id = int(cur.lastrowid)
    cur.execute(
        """
        SELECT id, name, created_at, updated_at
        FROM music_player_playlists
        WHERE id=?
        LIMIT 1
        """,
        (playlist_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else {}


def delete_playlist(conn: sqlite3.Connection, playlist_id: int) -> None:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute("DELETE FROM music_player_playlists WHERE id=?", (int(playlist_id),))
    conn.commit()


def playlist_items(conn: sqlite3.Connection, playlist_id: int) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, playlist_id, item_id, title, artist, album, stream_url, local_path, source_kind, position, created_at
        FROM music_player_playlist_items
        WHERE playlist_id=?
        ORDER BY position ASC, id ASC
        """,
        (int(playlist_id),),
    )
    return [dict(row) for row in cur.fetchall()]


def add_playlist_item(conn: sqlite3.Connection, playlist_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(position), -1) + 1 FROM music_player_playlist_items WHERE playlist_id=?", (int(playlist_id),))
    next_position = int((cur.fetchone() or [0])[0] or 0)
    item = {
        "item_id": str(payload.get("item_id") or payload.get("id") or "").strip() or None,
        "title": str(payload.get("title") or "").strip() or None,
        "artist": str(payload.get("artist") or "").strip() or None,
        "album": str(payload.get("album") or "").strip() or None,
        "stream_url": str(payload.get("stream_url") or "").strip() or None,
        "local_path": str(payload.get("local_path") or "").strip() or None,
        "source_kind": str(payload.get("source_kind") or payload.get("kind") or "local").strip() or "local",
    }
    cur.execute(
        """
        INSERT OR IGNORE INTO music_player_playlist_items
        (playlist_id, item_id, title, artist, album, stream_url, local_path, source_kind, position)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(playlist_id),
            item["item_id"],
            item["title"],
            item["artist"],
            item["album"],
            item["stream_url"],
            item["local_path"],
            item["source_kind"],
            next_position,
        ),
    )
    conn.commit()
    cur.execute(
        """
        UPDATE music_player_playlists
        SET updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (int(playlist_id),),
    )
    conn.commit()
    cur.execute(
        """
        SELECT id, playlist_id, item_id, title, artist, album, stream_url, local_path, source_kind, position, created_at
        FROM music_player_playlist_items
        WHERE playlist_id=? AND (
          (item_id IS ? OR item_id = ?) AND
          (local_path IS ? OR local_path = ?) AND
          (stream_url IS ? OR stream_url = ?)
        )
        ORDER BY id DESC
        LIMIT 1
        """,
        (
            int(playlist_id),
            item["item_id"], item["item_id"],
            item["local_path"], item["local_path"],
            item["stream_url"], item["stream_url"],
        ),
    )
    row = cur.fetchone()
    return dict(row) if row else {}


def remove_playlist_item(conn: sqlite3.Connection, playlist_id: int, item_row_id: int) -> None:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM music_player_playlist_items WHERE playlist_id=? AND id=?",
        (int(playlist_id), int(item_row_id)),
    )
    cur.execute(
        "UPDATE music_player_playlists SET updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (int(playlist_id),),
    )
    conn.commit()
