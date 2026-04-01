from __future__ import annotations

import calendar
import mimetypes
import json
import os
import sqlite3
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from typing import Any

from engine.musicbrainz_binding import search_artists_by_genre

try:
    from mutagen import File as MutagenFile
except Exception:  # pragma: no cover - optional dependency in some environments
    MutagenFile = None


AUDIO_EXTENSIONS = {".mp3", ".m4a", ".flac", ".aac", ".ogg", ".opus", ".wav", ".alac"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
PREFERRED_ART_NAMES = (
    "cover",
    "folder",
    "front",
    "album",
    "artwork",
    "poster",
    "thumb",
    "thumbnail",
)
STATION_READY_TARGET = 3
STATION_PRIME_TAIL_TARGET = 12
STATION_TOTAL_TARGET = 18
STATION_CANDIDATE_LIMIT = 240
YOUTUBE_SOURCE_KEYS = {"youtube", "youtube_music"}

_SEARCH_DB_PATH: str | None = None


def configure_search_db_path(db_path: str | None) -> None:
    global _SEARCH_DB_PATH
    _SEARCH_DB_PATH = str(db_path or "").strip() or None


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
    cur.execute("PRAGMA table_info(music_player_stations)")
    station_columns = {str(row[1]) for row in cur.fetchall()}
    if "station_mode" not in station_columns:
        cur.execute("ALTER TABLE music_player_stations ADD COLUMN station_mode TEXT NOT NULL DEFAULT 'mix'")
    if "seed_identity_json" not in station_columns:
        cur.execute("ALTER TABLE music_player_stations ADD COLUMN seed_identity_json TEXT NOT NULL DEFAULT '{}'")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS music_player_station_runtime (
            station_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'idle',
            queue_json TEXT NOT NULL DEFAULT '[]',
            current_index INTEGER NOT NULL DEFAULT -1,
            active_item_json TEXT NOT NULL DEFAULT '{}',
            ready_count INTEGER NOT NULL DEFAULT 0,
            unresolved_count INTEGER NOT NULL DEFAULT 0,
            local_count INTEGER NOT NULL DEFAULT 0,
            cached_count INTEGER NOT NULL DEFAULT 0,
            queue_depth INTEGER NOT NULL DEFAULT 0,
            last_refill_at TEXT,
            last_played_at TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(station_id) REFERENCES music_player_stations(id) ON DELETE CASCADE
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


def _find_album_art(album_dir: Path, cache: dict[str, str | None]) -> str | None:
    key = str(album_dir.resolve())
    if key in cache:
        return cache[key]
    found: str | None = None
    for candidate_dir in (album_dir, album_dir.parent):
        try:
            entries = [entry for entry in candidate_dir.iterdir() if entry.is_file() and entry.suffix.lower() in IMAGE_EXTENSIONS]
            if entries:
                preferred = []
                fallback = []
                for entry in entries:
                    stem = entry.stem.lower()
                    if any(stem == name or stem.startswith(f"{name}.") or stem.startswith(f"{name}_") or stem.startswith(f"{name}-") for name in PREFERRED_ART_NAMES):
                        preferred.append(entry)
                    else:
                        fallback.append(entry)
                chosen = (preferred or fallback)[0]
                found = str(chosen.resolve())
                break
        except Exception:
            continue
    cache[key] = found
    return found


def _first_music_tag(tags: Any, *keys: str) -> str:
    if not tags:
        return ""
    for key in keys:
        try:
            value = tags.get(key)
        except Exception:
            value = None
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                text = str(item or "").strip()
                if text:
                    return text
            continue
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _read_local_music_tags(path: Path) -> dict[str, Any]:
    if MutagenFile is None:
        return {}
    try:
        audio = MutagenFile(str(path), easy=False)
    except Exception:
        return {}
    if audio is None:
        return {}
    tags = getattr(audio, "tags", None)
    if not tags:
        return {}
    return {
        "title": _first_music_tag(tags, "TIT2", "\xa9nam", "title"),
        "artist": _first_music_tag(tags, "TPE1", "\xa9ART", "artist"),
        "album": _first_music_tag(tags, "TALB", "\xa9alb", "album"),
        "album_artist": _first_music_tag(tags, "TPE2", "aART", "albumartist", "album_artist"),
        "recording_mbid": _first_music_tag(
            tags,
            "TXXX:MBID",
            "----:com.apple.iTunes:MBID",
            "musicbrainz_trackid",
            "musicbrainz_recordingid",
            "mbid",
        ),
        "mb_release_id": _first_music_tag(
            tags,
            "TXXX:MUSICBRAINZ_RELEASEID",
            "----:com.apple.iTunes:MUSICBRAINZ_RELEASEID",
            "musicbrainz_releaseid",
        ),
        "mb_release_group_id": _first_music_tag(
            tags,
            "TXXX:MUSICBRAINZ_RELEASEGROUPID",
            "----:com.apple.iTunes:MUSICBRAINZ_RELEASEGROUPID",
            "musicbrainz_releasegroupid",
        ),
    }


def scan_local_library(config: dict[str, Any], *, limit: int = 250) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    artwork_cache: dict[str, str | None] = {}
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
                tag_data = _read_local_music_tags(path)
                artist = (
                    str(tag_data.get("album_artist") or "").strip()
                    or str(tag_data.get("artist") or "").strip()
                    or (path.parent.parent.name if len(path.parts) >= 3 else "")
                )
                album = str(tag_data.get("album") or "").strip() or path.parent.name
                title = str(tag_data.get("title") or "").strip() or path.stem
                stat = path.stat()
                artwork_local_path = _find_album_art(path.parent, artwork_cache)
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
                        "downloaded_at": int(stat.st_mtime),
                        "size_bytes": int(stat.st_size),
                        "file_ext": path.suffix.lower(),
                        "media_type": str(mimetypes.guess_type(resolved)[0] or "").strip() or None,
                        "artwork_local_path": artwork_local_path,
                        "recording_mbid": str(tag_data.get("recording_mbid") or "").strip() or None,
                        "mb_release_id": str(tag_data.get("mb_release_id") or "").strip() or None,
                        "mb_release_group_id": str(tag_data.get("mb_release_group_id") or "").strip() or None,
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
            {"artist": artist, "artist_key": artist_key, "album_count": 0, "track_count": 0, "latest_downloaded_at": 0},
        )
        artist_entry["track_count"] += 1
        artist_entry["latest_downloaded_at"] = max(int(artist_entry.get("latest_downloaded_at") or 0), int(item.get("downloaded_at") or 0))
        album_entry = albums_map.setdefault(
            (artist_key, album_key),
            {
                "artist": artist,
                "artist_key": artist_key,
                "album": album,
                "album_key": album_key,
                "track_count": 0,
                "latest_downloaded_at": 0,
                "artwork_local_path": str(item.get("artwork_local_path") or "").strip() or None,
            },
        )
        album_entry["track_count"] += 1
        album_entry["latest_downloaded_at"] = max(int(album_entry.get("latest_downloaded_at") or 0), int(item.get("downloaded_at") or 0))
        if not album_entry.get("artwork_local_path"):
            album_entry["artwork_local_path"] = str(item.get("artwork_local_path") or "").strip() or None
        if not artist_entry.get("artwork_local_path"):
            artist_entry["artwork_local_path"] = str(item.get("artwork_local_path") or "").strip() or None
    for artist_key, artist_entry in artists_map.items():
        artist_entry["album_count"] = sum(1 for (a_key, _), _album in albums_map.items() if a_key == artist_key)
    artists = sorted(artists_map.values(), key=lambda entry: (-int(entry.get("latest_downloaded_at") or 0), entry["artist"].lower()))
    albums = sorted(albums_map.values(), key=lambda entry: (-int(entry.get("latest_downloaded_at") or 0), entry["artist"].lower(), entry["album"].lower()))
    tracks = sorted(tracks, key=lambda entry: (-int(entry.get("downloaded_at") or 0), str(entry.get("artist") or "").lower(), str(entry.get("album") or "").lower(), str(entry.get("title") or "").lower()))
    return {"artists": artists, "albums": albums, "tracks": tracks}


def _safe_json_loads(raw: Any, *, default: Any) -> Any:
    try:
        parsed = json.loads(str(raw or ""))
    except Exception:
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_key(value: Any) -> str:
    normalized = _normalize_text(value)
    return "".join(ch for ch in normalized if ch.isalnum() or ch == " ").strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _extract_year(value: Any) -> int:
    text = str(value or "").strip()
    for token in text.replace("/", "-").split("-"):
        if len(token) == 4 and token.isdigit():
            return int(token)
    return 0


def _extract_youtube_video_id(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = urlparse(text)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if "youtube.com" in host:
        query = parse_qs(parsed.query)
        for key in ("v", "vi"):
            values = query.get(key) or []
            if values:
                candidate = str(values[0] or "").strip()
                if candidate:
                    return candidate
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] in {"embed", "shorts", "live", "watch"}:
            candidate = str(parts[1] or "").strip()
            if candidate:
                return candidate
    if "youtu.be" in host:
        candidate = str(path.lstrip("/").split("/")[0] or "").strip()
        if candidate:
            return candidate
    return None


def _build_youtube_embed_url(value: Any) -> str | None:
    video_id = _extract_youtube_video_id(value)
    if not video_id:
        return None
    return f"https://www.youtube.com/embed/{quote(video_id, safe='')}?autoplay=1&rel=0&modestbranding=1&playsinline=1"


def _normalized_music_preferences(config: dict[str, Any]) -> dict[str, Any]:
    prefs = config.get("music_preferences") if isinstance(config.get("music_preferences"), dict) else {}
    favorite_genres = [
        str(value or "").strip()
        for value in (prefs.get("favorite_genres") if isinstance(prefs.get("favorite_genres"), list) else [])
        if str(value or "").strip()
    ]
    favorite_artists: list[dict[str, Any]] = []
    raw_favorite_artists = prefs.get("favorite_artists") if isinstance(prefs.get("favorite_artists"), list) else []
    for entry in raw_favorite_artists:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or entry.get("artist_name") or "").strip()
        if not name:
            continue
        favorite_artists.append(
            {
                "name": name,
                "artist_mbid": str(entry.get("artist_mbid") or "").strip() or None,
            }
        )
    return {
        "favorite_genres": favorite_genres,
        "favorite_artists": favorite_artists,
    }


def _station_seed_identity(seed_type: str, seed_value: str, seed_identity: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = dict(seed_identity or {})
    normalized_type = str(seed_type or "artist").strip() or "artist"
    normalized_value = str(seed_value or "").strip()
    payload["seed_type"] = normalized_type
    payload["seed_value"] = normalized_value
    payload["normalized_seed"] = _normalize_key(normalized_value)
    if normalized_type == "genre":
        payload["genre_key"] = payload["normalized_seed"]
    if normalized_type == "artist":
        payload["artist_name"] = str(payload.get("artist_name") or normalized_value).strip()
        payload["artist_key"] = _normalize_key(payload["artist_name"])
    if normalized_type == "album":
        payload["album_name"] = str(payload.get("album_name") or normalized_value).strip()
        payload["album_key"] = _normalize_key(payload["album_name"])
        payload["artist_name"] = str(payload.get("artist_name") or "").strip()
        payload["artist_key"] = _normalize_key(payload["artist_name"])
    return payload


def _hydrate_station_row(row: dict[str, Any]) -> dict[str, Any]:
    station = dict(row)
    station["station_mode"] = str(station.get("station_mode") or "mix").strip() or "mix"
    station["seed_identity"] = _station_seed_identity(
        str(station.get("seed_type") or "artist"),
        str(station.get("seed_value") or ""),
        _safe_json_loads(station.get("seed_identity_json"), default={}),
    )
    return station


def _candidate_identity(item: dict[str, Any]) -> str:
    local_path = str(item.get("local_path") or "").strip()
    if local_path:
        return f"local:{local_path}"
    source_url = str(item.get("source_url") or "").strip()
    if source_url:
        return f"url:{source_url}"
    item_id = str(item.get("id") or "").strip()
    return item_id or json.dumps(item, sort_keys=True)


def _load_station_runtime(conn: sqlite3.Connection, station_id: int) -> dict[str, Any]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT station_id, status, queue_json, current_index, active_item_json, ready_count, unresolved_count,
               local_count, cached_count, queue_depth, last_refill_at, last_played_at, updated_at
        FROM music_player_station_runtime
        WHERE station_id=? LIMIT 1
        """,
        (int(station_id),),
    )
    row = cur.fetchone()
    if not row:
        return {
            "station_id": int(station_id),
            "status": "idle",
            "queue": [],
            "current_index": -1,
            "active_item": None,
            "ready_count": 0,
            "unresolved_count": 0,
            "local_count": 0,
            "cached_count": 0,
            "queue_depth": 0,
            "last_refill_at": None,
            "last_played_at": None,
            "updated_at": None,
        }
    runtime = dict(row)
    runtime["queue"] = _safe_json_loads(runtime.get("queue_json"), default=[])
    active_item = _safe_json_loads(runtime.get("active_item_json"), default={})
    runtime["active_item"] = active_item if active_item else None
    return runtime


def _save_station_runtime(conn: sqlite3.Connection, station_id: int, runtime: dict[str, Any]) -> dict[str, Any]:
    ensure_music_player_tables(conn)
    queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    active_item = runtime.get("active_item") if isinstance(runtime.get("active_item"), dict) else {}
    payload = {
        "station_id": int(station_id),
        "status": str(runtime.get("status") or ("ready" if queue else "needs_matches")).strip(),
        "queue_json": json.dumps(queue),
        "current_index": _safe_int(runtime.get("current_index"), -1),
        "active_item_json": json.dumps(active_item or {}),
        # ready_count = items that have a stream_url (playable immediately).
        # unresolved_count = items without a stream_url (need resolution before play).
        "ready_count": sum(1 for item in queue if str(item.get("stream_url") or "").strip()),
        "unresolved_count": sum(1 for item in queue if not str(item.get("stream_url") or "").strip()),
        "local_count": sum(1 for item in queue if str(item.get("kind") or item.get("source_kind") or "").strip().lower() == "local"),
        "cached_count": sum(1 for item in queue if str(item.get("kind") or item.get("source_kind") or "").strip().lower() == "cached"),
        "queue_depth": len(queue),
        "last_refill_at": runtime.get("last_refill_at"),
        "last_played_at": runtime.get("last_played_at"),
        "updated_at": runtime.get("updated_at") or _utc_now(),
    }
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO music_player_station_runtime (
            station_id, status, queue_json, current_index, active_item_json, ready_count, unresolved_count,
            local_count, cached_count, queue_depth, last_refill_at, last_played_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(station_id) DO UPDATE SET
            status=excluded.status,
            queue_json=excluded.queue_json,
            current_index=excluded.current_index,
            active_item_json=excluded.active_item_json,
            ready_count=excluded.ready_count,
            unresolved_count=excluded.unresolved_count,
            local_count=excluded.local_count,
            cached_count=excluded.cached_count,
            queue_depth=excluded.queue_depth,
            last_refill_at=excluded.last_refill_at,
            last_played_at=excluded.last_played_at,
            updated_at=excluded.updated_at
        """,
        (
            payload["station_id"],
            payload["status"],
            payload["queue_json"],
            payload["current_index"],
            payload["active_item_json"],
            payload["ready_count"],
            payload["unresolved_count"],
            payload["local_count"],
            payload["cached_count"],
            payload["queue_depth"],
            payload["last_refill_at"],
            payload["last_played_at"],
            payload["updated_at"],
        ),
    )
    conn.commit()
    return _load_station_runtime(conn, station_id)


def _fetch_history(conn: sqlite3.Connection, *, limit: int = 120) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT title, artist, stream_url, local_path, source_kind, played_at
        FROM music_player_history
        ORDER BY played_at DESC, id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return [dict(row) for row in cur.fetchall()]


def _fetch_resolution_candidates(conn: sqlite3.Connection, terms: list[str], *, limit: int = 160) -> list[dict[str, Any]]:
    normalized_terms = [_normalize_key(term) for term in terms if _normalize_key(term)]
    if not normalized_terms:
        return []
    clauses = []
    params: list[Any] = []
    for term in normalized_terms[:18]:
        like = f"%{term}%"
        clauses.append("(lower(source_payload_json) LIKE ? OR lower(source_url) LIKE ?)")
        params.extend([like, like])
    query = f"""
        SELECT recording_mbid, source, source_url, source_payload_json, verification_status, verification_count, updated_at
        FROM resolution_sources
        WHERE verification_status IN ('verified', 'pending_verification', 'pending')
          AND COALESCE(source_url, '') <> ''
          AND ({' OR '.join(clauses)})
        ORDER BY verification_status='verified' DESC, verification_count DESC, updated_at DESC
        LIMIT ?
    """
    params.append(int(limit))
    # resolution_sources lives in the search DB (search_jobs.sqlite), not the main DB.
    # Open a dedicated connection when the path is configured; fall back to conn for tests.
    search_path = _SEARCH_DB_PATH
    target_conn = sqlite3.connect(search_path) if search_path else conn
    target_conn.row_factory = sqlite3.Row
    try:
        cur = target_conn.cursor()
        try:
            cur.execute(query, params)
        except sqlite3.OperationalError:
            return []
        return [dict(row) for row in cur.fetchall()]
    finally:
        if search_path:
            target_conn.close()


def _station_context(conn: sqlite3.Connection, config: dict[str, Any], station: dict[str, Any]) -> dict[str, Any]:
    summary = summarize_library(scan_local_library(config, limit=1200))
    history = _fetch_history(conn, limit=120)
    prefs = _normalized_music_preferences(config)
    favorite_artist_names = [str(entry.get("name") or "").strip() for entry in prefs.get("favorite_artists", []) if str(entry.get("name") or "").strip()]
    history_artists: list[str] = []
    seen_history = set()
    for entry in history:
        artist = str(entry.get("artist") or "").strip()
        key = _normalize_key(artist)
        if artist and key and key not in seen_history:
            seen_history.add(key)
            history_artists.append(artist)
    # Build a fast lookup of recently-played (title, artist) pairs used by the scorer
    # to penalise tracks that have been heard recently, reducing session repetition.
    recently_played_keys: set[str] = set()
    for entry in history[:40]:
        t_key = _normalize_key(entry.get("title") or "")
        a_key = _normalize_key(entry.get("artist") or "")
        if t_key and a_key:
            recently_played_keys.add(f"{a_key}::{t_key}")
    return {
        "summary": summary,
        "history": history,
        "favorite_genres": prefs.get("favorite_genres", []),
        "favorite_artists": favorite_artist_names,
        "history_artists": history_artists,
        "recently_played_keys": recently_played_keys,
        "station": station,
    }


def _derive_related_terms(station: dict[str, Any], context: dict[str, Any]) -> tuple[list[str], list[str]]:
    seed_identity = station.get("seed_identity") if isinstance(station.get("seed_identity"), dict) else {}
    seed_type = str(station.get("seed_type") or "artist")
    seed_value = str(station.get("seed_value") or "").strip()
    artist_terms: list[str] = []
    free_terms: list[str] = []
    if seed_type == "artist":
        artist_name = str(seed_identity.get("artist_name") or seed_value).strip()
        if artist_name:
            artist_terms.append(artist_name)
            free_terms.append(artist_name)
    elif seed_type == "album":
        artist_name = str(seed_identity.get("artist_name") or "").strip()
        album_name = str(seed_identity.get("album_name") or seed_value).strip()
        if artist_name:
            artist_terms.append(artist_name)
            free_terms.append(artist_name)
        if album_name:
            free_terms.append(album_name)
    elif seed_type == "genre":
        genre_value = str(seed_identity.get("seed_value") or seed_value).strip()
        if genre_value:
            free_terms.append(genre_value)
        try:
            related_artists = search_artists_by_genre(genre=genre_value, limit=18, offset=0) or []
        except Exception:
            related_artists = []
        for item in related_artists:
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                if name:
                    artist_terms.append(name)
    elif seed_type == "favorites":
        for artist_name in context.get("favorite_artists", [])[:12]:
            artist_terms.append(str(artist_name))
        for genre_name in context.get("favorite_genres", [])[:6]:
            free_terms.append(str(genre_name))
    if seed_type != "favorites":
        for artist_name in context.get("favorite_artists", [])[:8]:
            artist_terms.append(str(artist_name))
        for artist_name in context.get("history_artists", [])[:8]:
            artist_terms.append(str(artist_name))
    deduped_artists: list[str] = []
    seen_artists = set()
    for value in artist_terms:
        key = _normalize_key(value)
        if key and key not in seen_artists:
            seen_artists.add(key)
            deduped_artists.append(value)
    deduped_terms: list[str] = []
    seen_terms = set()
    for value in free_terms:
        key = _normalize_key(value)
        if key and key not in seen_terms:
            seen_terms.add(key)
            deduped_terms.append(value)
    return deduped_artists, deduped_terms


def _score_station_candidate(item: dict[str, Any], *, station: dict[str, Any], context: dict[str, Any], artist_terms: list[str], free_terms: list[str], local_artist_counts: dict[str, int], favorite_artist_keys: set[str]) -> float:
    seed_identity = station.get("seed_identity") if isinstance(station.get("seed_identity"), dict) else {}
    seed_type = str(station.get("seed_type") or "artist")
    station_mode = str(station.get("station_mode") or "mix")
    title = str(item.get("title") or "").strip()
    artist = str(item.get("artist") or "").strip()
    album = str(item.get("album") or "").strip()
    haystack = _normalize_key(" ".join([title, artist, album, str(item.get("genre") or "")]))
    artist_key = _normalize_key(artist)
    album_key = _normalize_key(album)
    score = 0.0

    # --- Seed relevance (primary signal) ---
    # These scores establish the baseline relevance of the track to the station seed.
    if seed_type == "artist":
        target_artist_key = _normalize_key(seed_identity.get("artist_name") or station.get("seed_value") or "")
        if artist_key and artist_key == target_artist_key:
            score += 120
        elif target_artist_key and target_artist_key in haystack:
            score += 70
    elif seed_type == "album":
        target_album_key = _normalize_key(seed_identity.get("album_name") or station.get("seed_value") or "")
        target_artist_key = _normalize_key(seed_identity.get("artist_name") or "")
        if album_key and album_key == target_album_key:
            score += 120
        elif target_album_key and target_album_key in haystack:
            score += 84
        if target_artist_key and artist_key == target_artist_key:
            score += 36
    elif seed_type == "genre":
        genre_key = _normalize_key(seed_identity.get("genre_key") or station.get("seed_value") or "")
        if genre_key and genre_key in haystack:
            score += 90
    elif seed_type == "favorites" and artist_key and artist_key in favorite_artist_keys:
        score += 88

    # --- Related-term expansion (secondary signal) ---
    # Artist and free terms come from favorites, history, and MusicBrainz expansion.
    # Exact artist key match outweighs a substring hit to reduce false positives.
    for artist_name in artist_terms[:18]:
        related_key = _normalize_key(artist_name)
        if not related_key:
            continue
        if artist_key == related_key:
            score += 46
        elif related_key in haystack:
            score += 22
    for term in free_terms[:12]:
        normalized = _normalize_key(term)
        if normalized and normalized in haystack:
            score += 18

    # --- Source quality and library depth ---
    verification_count = _safe_int(item.get("verification_count"), 0)
    is_local = str(item.get("kind") or item.get("source_kind") or "").strip().lower() == "local"
    if is_local:
        score += 34
    else:
        score += min(verification_count, 12) * 2.0

    library_weight = local_artist_counts.get(artist_key, 0)
    score += min(library_weight, 25) * 1.1
    if artist_key in favorite_artist_keys:
        score += 16

    # --- Freshness and release year ---
    downloaded_at = _safe_int(item.get("downloaded_at"), 0)
    if downloaded_at > 0:
        age_days = max(0.0, (time.time() - float(downloaded_at)) / 86400.0)
        freshness = max(0.0, 18.0 - min(age_days / 30.0, 18.0))
        score += freshness * (2.6 if station_mode == "latest" else 0.5)
    release_year = _extract_year(item.get("release_date"))
    if release_year > 0:
        score += max(0, release_year - 1990) * (0.2 if station_mode == "latest" else 0.05)

    # --- Recent-play penalty ---
    # Penalise tracks heard in the last ~40 plays so the same songs don't dominate
    # every session. Applied before mode adjustments so modes can't override it.
    recently_played_keys = context.get("recently_played_keys") if isinstance(context.get("recently_played_keys"), set) else set()
    title_key = _normalize_key(title)
    if artist_key and title_key and f"{artist_key}::{title_key}" in recently_played_keys:
        score -= 40

    # --- Station mode adjustments ---
    # Each mode shifts emphasis on top of the base score above.
    if station_mode == "top_hits":
        # Amplify popularity signals: verification count and library depth.
        score += min(verification_count, 25) * 2.2
        score += min(library_weight, 25) * 1.4
    elif station_mode == "essentials":
        # Favour verified tracks. Extra boost for exact seed-artist match so
        # the artist's own catalogue stays dominant.
        score += min(verification_count, 25) * 1.8
        if seed_type == "artist" and artist_key == _normalize_key(seed_identity.get("artist_name") or station.get("seed_value") or ""):
            score += 24
    elif station_mode == "deep_cuts":
        # Penalise popularity and broad library presence. Prefer local tracks
        # that are under-represented in the library (long-tail obscure items).
        score -= min(verification_count, 25) * 1.1
        score -= min(library_weight, 20) * 0.6
        if is_local and library_weight <= 3:
            score += 14  # reward obscure local tracks specifically
    # mix and latest use base scores as-is (latest already gets freshness boost above)

    return score


def _build_local_candidates(config: dict[str, Any], *, station: dict[str, Any], context: dict[str, Any], artist_terms: list[str], free_terms: list[str]) -> list[dict[str, Any]]:
    summary = context.get("summary") if isinstance(context.get("summary"), dict) else {}
    tracks = summary.get("tracks") if isinstance(summary.get("tracks"), list) else []
    local_artist_counts = {
        _normalize_key(entry.get("artist") or entry.get("artist_key") or ""): _safe_int(entry.get("track_count"), 0)
        for entry in (summary.get("artists") if isinstance(summary.get("artists"), list) else [])
        if isinstance(entry, dict)
    }
    favorite_artist_keys = {_normalize_key(name) for name in context.get("favorite_artists", []) if _normalize_key(name)}
    items: list[dict[str, Any]] = []
    for track in tracks:
        if not isinstance(track, dict):
            continue
        candidate = {
            **track,
            "kind": "local",
            "source_kind": "local",
            "source": "local",
            "playback_kind": "audio",
            "ready": True,
        }
        score = _score_station_candidate(candidate, station=station, context=context, artist_terms=artist_terms, free_terms=free_terms, local_artist_counts=local_artist_counts, favorite_artist_keys=favorite_artist_keys)
        if score <= 0:
            continue
        candidate["station_score"] = round(score, 4)
        items.append(candidate)
    items.sort(key=lambda item: (float(item.get("station_score") or 0), int(item.get("downloaded_at") or 0)), reverse=True)
    return items[:STATION_CANDIDATE_LIMIT]


def _build_cached_candidates(conn: sqlite3.Connection, *, station: dict[str, Any], context: dict[str, Any], artist_terms: list[str], free_terms: list[str]) -> list[dict[str, Any]]:
    local_artist_counts = {
        _normalize_key(entry.get("artist") or entry.get("artist_key") or ""): _safe_int(entry.get("track_count"), 0)
        for entry in (((context.get("summary") or {}).get("artists")) if isinstance(context.get("summary"), dict) else [])
        if isinstance(entry, dict)
    }
    favorite_artist_keys = {_normalize_key(name) for name in context.get("favorite_artists", []) if _normalize_key(name)}
    query_terms = artist_terms[:10] + free_terms[:8]
    if not query_terms:
        query_terms = [str(station.get("seed_value") or "")]
    rows = _fetch_resolution_candidates(conn, query_terms, limit=200)
    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for row in rows:
        url = str(row.get("source_url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        payload = _safe_json_loads(row.get("source_payload_json"), default={})
        source = str(row.get("source") or payload.get("source") or "").strip().lower() or "cached"
        if source not in YOUTUBE_SOURCE_KEYS:
            continue
        video_id = _extract_youtube_video_id(url)
        if not video_id:
            continue
        candidate = {
            "id": f"cached:{row.get('recording_mbid') or ''}:{url}",
            "title": str(payload.get("title") or payload.get("track") or row.get("recording_mbid") or "Cached Match").strip() or "Cached Match",
            "artist": str(payload.get("artist") or payload.get("uploader") or "").strip(),
            "album": str(payload.get("album") or "").strip(),
            "kind": "youtube",
            "source_kind": "youtube",
            "source": source,
            "source_url": url,
            "recording_mbid": str(row.get("recording_mbid") or "").strip() or None,
            "verification_status": str(row.get("verification_status") or "").strip() or None,
            "verification_count": _safe_int(row.get("verification_count"), 0),
            "artwork_url": str(payload.get("thumbnail") or payload.get("thumbnail_url") or "").strip() or None,
            "release_date": str(payload.get("release_date") or payload.get("release_year") or "").strip() or None,
            "stream_url": url,
            "video_id": video_id,
            "playback_kind": "youtube",
            "ready": True,
            "video_embed_url": _build_youtube_embed_url(url),
        }
        score = _score_station_candidate(candidate, station=station, context=context, artist_terms=artist_terms, free_terms=free_terms, local_artist_counts=local_artist_counts, favorite_artist_keys=favorite_artist_keys)
        if score <= 0:
            continue
        candidate["station_score"] = round(score, 4)
        items.append(candidate)
    items.sort(key=lambda item: (float(item.get("station_score") or 0), int(item.get("verification_count") or 0)), reverse=True)
    return items[:STATION_CANDIDATE_LIMIT]


def _assemble_station_queue(station: dict[str, Any], local_candidates: list[dict[str, Any]], cached_candidates: list[dict[str, Any]], *, existing_queue: list[dict[str, Any]] | None = None, target: int = STATION_TOTAL_TARGET) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    seen = {_candidate_identity(item) for item in (existing_queue or []) if isinstance(item, dict)}
    station_mode = str(station.get("station_mode") or "mix")
    # essentials: no artist cap — seed artist's catalogue fills the queue by design.
    # top_hits: cap=3 enforced so no single artist dominates a popularity station.
    # mix / latest / deep_cuts: cap=2 for broad variety.
    enforce_caps = station_mode != "essentials"
    per_artist_cap = 3 if station_mode == "top_hits" else 2
    per_artist: dict[str, int] = {}
    # Per-album cap: max 2 tracks from the same album across all modes (except essentials),
    # preventing a single album from clustering at the top of the scored list.
    per_album_cap = 2
    per_album: dict[tuple[str, str], int] = {}
    merged = list(local_candidates) + list(cached_candidates)
    merged.sort(key=lambda item: (float(item.get("station_score") or 0), 1 if str(item.get("kind") or "") == "local" else 0), reverse=True)
    for item in merged:
        identity = _candidate_identity(item)
        if identity in seen:
            continue
        artist_key = _normalize_key(item.get("artist") or "")
        album_key = _normalize_key(item.get("album") or "")
        album_id = (artist_key, album_key) if album_key else None
        if enforce_caps and artist_key:
            if per_artist.get(artist_key, 0) >= per_artist_cap:
                continue
            if album_id and per_album.get(album_id, 0) >= per_album_cap:
                continue
        per_artist[artist_key] = per_artist.get(artist_key, 0) + 1
        if album_id:
            per_album[album_id] = per_album.get(album_id, 0) + 1
        queue.append(item)
        seen.add(identity)
        if len(queue) >= target:
            break
    return queue


def _runtime_preview(runtime: dict[str, Any], *, limit: int = 10) -> dict[str, Any]:
    queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    current_index = _safe_int(runtime.get("current_index"), -1)
    start_index = current_index + 1 if current_index >= 0 else 0
    return {
        "count": len(queue),
        "local_count": _safe_int(runtime.get("local_count"), 0),
        "cached_count": _safe_int(runtime.get("cached_count"), 0),
        "ready_count": _safe_int(runtime.get("ready_count"), 0),
        "unresolved_count": _safe_int(runtime.get("unresolved_count"), 0),
        "queue_depth": _safe_int(runtime.get("queue_depth"), len(queue)),
        "items": queue[start_index:start_index + int(limit)],
    }


def list_stations(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, seed_type, seed_value, station_mode, seed_identity_json, created_at, updated_at
        FROM music_player_stations
        ORDER BY updated_at DESC, id DESC
        """
    )
    return [_hydrate_station_row(dict(row)) for row in cur.fetchall()]


def create_station(
    conn: sqlite3.Connection,
    *,
    name: str,
    seed_type: str,
    seed_value: str,
    station_mode: str = "mix",
    seed_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_music_player_tables(conn)
    normalized_seed_type = str(seed_type or "artist").strip() or "artist"
    normalized_seed_value = str(seed_value or "").strip()
    normalized_identity = _station_seed_identity(normalized_seed_type, normalized_seed_value, seed_identity)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO music_player_stations (name, seed_type, seed_value, station_mode, seed_identity_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            name.strip() or normalized_seed_value or normalized_seed_type.title(),
            normalized_seed_type,
            normalized_seed_value,
            str(station_mode or "mix").strip() or "mix",
            json.dumps(normalized_identity),
        ),
    )
    conn.commit()
    station_id = int(cur.lastrowid)
    cur.execute(
        """
        SELECT id, name, seed_type, seed_value, station_mode, seed_identity_json, created_at, updated_at
        FROM music_player_stations
        WHERE id=? LIMIT 1
        """,
        (station_id,),
    )
    row = cur.fetchone()
    return _hydrate_station_row(dict(row)) if row else {}


def delete_station(conn: sqlite3.Connection, station_id: int) -> None:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute("DELETE FROM music_player_stations WHERE id=?", (int(station_id),))
    cur.execute("DELETE FROM music_player_station_runtime WHERE station_id=?", (int(station_id),))
    conn.commit()


def _prime_station_runtime(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    *,
    station: dict[str, Any],
    queue_target: int = STATION_TOTAL_TARGET,
) -> dict[str, Any]:
    hydrated_station = station if isinstance(station.get("seed_identity"), dict) else _hydrate_station_row(station)
    runtime = _load_station_runtime(conn, int(hydrated_station.get("id") or 0))
    queue_target = max(int(queue_target), STATION_TOTAL_TARGET)
    runtime_queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    last_refill_raw = str(runtime.get("last_refill_at") or "").strip()
    current_index = _safe_int(runtime.get("current_index"), -1)
    # Guard: skip rebuild only when enough upcoming (unplayed) tracks remain and queue was recently refilled.
    # Check remaining items after current position — NOT total queue length — to avoid locking up an
    # exhausted queue that still has a non-zero len() but nothing left to play.
    remaining = len(runtime_queue) - (current_index + 1)
    if remaining >= STATION_READY_TARGET and last_refill_raw:
        try:
            # Use calendar.timegm (not time.mktime) so the UTC timestamp stored by _utc_now() is
            # parsed correctly regardless of the server's local timezone.
            refill_ts = calendar.timegm(time.strptime(last_refill_raw, "%Y-%m-%dT%H:%M:%SZ"))
        except Exception:
            refill_ts = 0
        if refill_ts > 0 and (time.time() - refill_ts) < 900:
            return runtime
    context = _station_context(conn, config, hydrated_station)
    artist_terms, free_terms = _derive_related_terms(hydrated_station, context)
    local_candidates = _build_local_candidates(config, station=hydrated_station, context=context, artist_terms=artist_terms, free_terms=free_terms)
    cached_candidates = _build_cached_candidates(conn, station=hydrated_station, context=context, artist_terms=artist_terms, free_terms=free_terms)
    # Isolate the unplayed portion of the existing queue.
    # Items before current_index have been consumed — they are excluded from `existing_queue`
    # so they can cycle back as candidates (prevents the queue locking up after exhaustion).
    upcoming_start = max(0, current_index + 1)
    existing_upcoming = runtime_queue[upcoming_start:]
    # Assemble fresh candidates that don't duplicate anything still in the upcoming list.
    # Pass existing_upcoming as the dedup reference; the result contains only NEW items.
    new_candidates = _assemble_station_queue(
        hydrated_station,
        local_candidates,
        cached_candidates,
        existing_queue=existing_upcoming,
        target=queue_target,
    )
    # Merge strategy: keep the existing upcoming order intact and append new candidates.
    # This preserves continuity for the listener and avoids wiping playable items when the
    # library is sparse and the fresh candidate pool is small.
    merged_queue = existing_upcoming + new_candidates
    merged_queue = merged_queue[:queue_target]
    active_item = runtime.get("active_item") if isinstance(runtime.get("active_item"), dict) else None
    if active_item:
        active_identity = _candidate_identity(active_item)
        for index, item in enumerate(merged_queue):
            if _candidate_identity(item) == active_identity:
                current_index = index
                break
        else:
            merged_queue.insert(0, active_item)
            current_index = 0
    return _save_station_runtime(
        conn,
        int(hydrated_station.get("id") or 0),
        {
            **runtime,
            "status": "ready" if merged_queue else "needs_matches",
            "queue": merged_queue,
            "current_index": current_index,
            "active_item": active_item or {},
            "last_refill_at": _utc_now(),
            "updated_at": _utc_now(),
        },
    )


def get_station_detail(conn: sqlite3.Connection, config: dict[str, Any], *, station_id: int, preview_limit: int = 10) -> dict[str, Any]:
    station = next((entry for entry in list_stations(conn) if int(entry.get("id") or 0) == int(station_id)), None)
    if not station:
        return {}
    runtime = _prime_station_runtime(conn, config, station=station, queue_target=max(int(preview_limit) + STATION_READY_TARGET, STATION_TOTAL_TARGET))
    return {
        **station,
        "runtime": {
            "status": runtime.get("status"),
            "current_index": runtime.get("current_index"),
            "queue_depth": runtime.get("queue_depth"),
            "ready_count": runtime.get("ready_count"),
            "unresolved_count": runtime.get("unresolved_count"),
            "local_count": runtime.get("local_count"),
            "cached_count": runtime.get("cached_count"),
            "last_refill_at": runtime.get("last_refill_at"),
            "last_played_at": runtime.get("last_played_at"),
            "active_item": runtime.get("active_item"),
        },
        "preview": _runtime_preview(runtime, limit=preview_limit),
    }


def prime_station(conn: sqlite3.Connection, config: dict[str, Any], *, station: dict[str, Any], queue_target: int = STATION_TOTAL_TARGET) -> dict[str, Any]:
    runtime = _prime_station_runtime(conn, config, station=station, queue_target=queue_target)
    return {
        "queue": runtime.get("queue") if isinstance(runtime.get("queue"), list) else [],
        "runtime": {
            "status": runtime.get("status"),
            "current_index": runtime.get("current_index"),
            "queue_depth": runtime.get("queue_depth"),
            "ready_count": runtime.get("ready_count"),
            "unresolved_count": runtime.get("unresolved_count"),
            "local_count": runtime.get("local_count"),
            "cached_count": runtime.get("cached_count"),
            "last_refill_at": runtime.get("last_refill_at"),
            "last_played_at": runtime.get("last_played_at"),
            "active_item": runtime.get("active_item"),
        },
        "preview": _runtime_preview(runtime, limit=10),
    }


def start_station(conn: sqlite3.Connection, config: dict[str, Any], *, station: dict[str, Any], queue_target: int = STATION_TOTAL_TARGET) -> dict[str, Any]:
    runtime = _prime_station_runtime(conn, config, station=station, queue_target=queue_target)
    queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    current_item = queue[0] if queue else None
    runtime = _save_station_runtime(
        conn,
        int(station.get("id") or 0),
        {
            **runtime,
            "status": "ready" if current_item else "needs_matches",
            "current_index": 0 if current_item else -1,
            "active_item": current_item or {},
            "last_played_at": _utc_now() if current_item else runtime.get("last_played_at"),
            "updated_at": _utc_now(),
        },
    )
    refreshed_queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    current_item = refreshed_queue[0] if refreshed_queue else None
    return {
        "current_item": current_item,
        "ready_items": refreshed_queue[1:1 + STATION_READY_TARGET],
        "queue": refreshed_queue,
        "runtime": {
            "status": runtime.get("status"),
            "current_index": runtime.get("current_index"),
            "queue_depth": runtime.get("queue_depth"),
            "ready_count": runtime.get("ready_count"),
            "unresolved_count": runtime.get("unresolved_count"),
            "local_count": runtime.get("local_count"),
            "cached_count": runtime.get("cached_count"),
            "last_refill_at": runtime.get("last_refill_at"),
            "last_played_at": runtime.get("last_played_at"),
        },
    }


def advance_station(conn: sqlite3.Connection, config: dict[str, Any], *, station: dict[str, Any], queue_target: int = STATION_TOTAL_TARGET) -> dict[str, Any]:
    runtime = _prime_station_runtime(conn, config, station=station, queue_target=queue_target)
    queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    current_index = _safe_int(runtime.get("current_index"), -1)
    next_index = current_index + 1
    if next_index >= len(queue):
        runtime = _prime_station_runtime(conn, config, station=station, queue_target=max(int(queue_target), len(queue) + STATION_READY_TARGET + STATION_PRIME_TAIL_TARGET))
        queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
        next_index = current_index + 1
    next_item = queue[next_index] if 0 <= next_index < len(queue) else None
    runtime = _save_station_runtime(
        conn,
        int(station.get("id") or 0),
        {
            **runtime,
            "status": "ready" if next_item else "needs_matches",
            "current_index": next_index if next_item else current_index,
            "active_item": next_item or runtime.get("active_item") or {},
            "last_played_at": _utc_now() if next_item else runtime.get("last_played_at"),
            "updated_at": _utc_now(),
        },
    )
    refreshed_queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    active_index = _safe_int(runtime.get("current_index"), -1)
    current_item = refreshed_queue[active_index] if 0 <= active_index < len(refreshed_queue) else None
    return {
        "current_item": current_item,
        "ready_items": refreshed_queue[active_index + 1:active_index + 1 + STATION_READY_TARGET] if current_item else [],
        "queue": refreshed_queue,
        "runtime": {
            "status": runtime.get("status"),
            "current_index": runtime.get("current_index"),
            "queue_depth": runtime.get("queue_depth"),
            "ready_count": runtime.get("ready_count"),
            "unresolved_count": runtime.get("unresolved_count"),
            "local_count": runtime.get("local_count"),
            "cached_count": runtime.get("cached_count"),
            "last_refill_at": runtime.get("last_refill_at"),
            "last_played_at": runtime.get("last_played_at"),
        },
    }


def build_station_queue(conn: sqlite3.Connection, config: dict[str, Any], *, station: dict[str, Any], limit: int = 25) -> list[dict[str, Any]]:
    runtime = _prime_station_runtime(conn, config, station=station, queue_target=max(int(limit), STATION_TOTAL_TARGET))
    queue = runtime.get("queue") if isinstance(runtime.get("queue"), list) else []
    return queue[: int(limit)]


def build_station_preview(conn: sqlite3.Connection, config: dict[str, Any], *, station: dict[str, Any], limit: int = 10) -> dict[str, Any]:
    runtime = _prime_station_runtime(conn, config, station=station, queue_target=max(int(limit) + STATION_READY_TARGET, STATION_TOTAL_TARGET))
    return _runtime_preview(runtime, limit=limit)


def list_cached_matches(conn: sqlite3.Connection, *, limit: int = 60) -> list[dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT recording_mbid, source, source_url, source_payload_json, verification_status, verification_count, updated_at
            FROM resolution_sources
            WHERE verification_status IN ('verified', 'pending_verification', 'pending')
              AND COALESCE(source_url, '') <> ''
            ORDER BY verification_status='verified' DESC, verification_count DESC, updated_at DESC
            LIMIT ?
            """,
            (int(max(1, limit)),),
        )
    except sqlite3.OperationalError:
        return []
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in cur.fetchall():
        url = str(row["source_url"] or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        try:
            payload = json.loads(str(row["source_payload_json"] or "{}"))
        except Exception:
            payload = {}
        artist = str(payload.get("artist") or payload.get("uploader") or "").strip()
        album = str(payload.get("album") or "").strip()
        title = str(payload.get("title") or payload.get("track") or row["recording_mbid"] or "Cached Match").strip() or "Cached Match"
        artwork_url = str(payload.get("thumbnail") or payload.get("thumbnail_url") or "").strip()
        source = str(row["source"] or "").strip().lower()
        if source not in YOUTUBE_SOURCE_KEYS:
            continue
        video_id = _extract_youtube_video_id(url)
        if not video_id:
            continue
        items.append(
            {
                "id": f"cached:{row['recording_mbid']}:{url}",
                "title": title,
                "artist": artist,
                "album": album,
                "kind": "youtube",
                "stream_url": url,
                "video_id": video_id,
                "recording_mbid": str(row["recording_mbid"] or ""),
                "source": source,
                "verification_status": str(row["verification_status"] or ""),
                "verification_count": int(row["verification_count"] or 0),
                "updated_at": str(row["updated_at"] or ""),
                "artwork_url": artwork_url or None,
            }
        )
    return items


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


def delete_history_entry(conn: sqlite3.Connection, history_id: int) -> None:
    ensure_music_player_tables(conn)
    cur = conn.cursor()
    cur.execute("DELETE FROM music_player_history WHERE id=?", (int(history_id),))
    conn.commit()


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


def reorder_playlist_items(conn: sqlite3.Connection, playlist_id: int, ordered_item_ids: list[int]) -> list[dict[str, Any]]:
    ensure_music_player_tables(conn)
    normalized_ids = [int(item_id) for item_id in ordered_item_ids if str(item_id).strip()]
    cur = conn.cursor()
    existing = playlist_items(conn, playlist_id)
    if not existing:
      return []
    existing_ids = [int(item["id"]) for item in existing]
    remainder = [item_id for item_id in existing_ids if item_id not in normalized_ids]
    final_order = [item_id for item_id in normalized_ids if item_id in existing_ids] + remainder
    for position, item_id in enumerate(final_order):
        cur.execute(
            """
            UPDATE music_player_playlist_items
            SET position=?
            WHERE playlist_id=? AND id=?
            """,
            (int(position), int(playlist_id), int(item_id)),
        )
    cur.execute(
        "UPDATE music_player_playlists SET updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (int(playlist_id),),
    )
    conn.commit()
    return playlist_items(conn, playlist_id)
