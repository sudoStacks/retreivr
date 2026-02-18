"""Intent execution dispatcher for API-layer intent plumbing."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict
from urllib.parse import quote

from engine.paths import DB_PATH
from input.intent_router import IntentType
from playlist.rebuild import rebuild_playlist_from_tracks
from scheduler.jobs.spotify_playlist_watch import enqueue_spotify_track, playlist_watch_job


async def execute_intent(
    intent_type: str,
    identifier: str,
    config,
    db,
    queue,
    spotify_client,
) -> Dict[str, Any]:
    """Dispatch intent execution to existing Spotify ingestion behaviors.

    This function keeps intent execution thin by delegating to established
    watcher/enqueue helpers where possible.
    """
    raw_intent = str(intent_type or "").strip()
    raw_identifier = str(identifier or "").strip()
    if not raw_intent:
        return _error_response(raw_intent, raw_identifier, "intent_type is required")
    if not raw_identifier:
        return _error_response(raw_intent, raw_identifier, "identifier is required")

    try:
        intent = IntentType(raw_intent)
    except ValueError:
        return _error_response(raw_intent, raw_identifier, "unsupported intent_type")

    if intent == IntentType.SPOTIFY_PLAYLIST:
        playlist_name = _resolve_playlist_name(raw_identifier, config)
        result = playlist_watch_job(
            spotify_client,
            db,
            queue,
            raw_identifier,
            playlist_name=playlist_name,
            config=config if isinstance(config, dict) else None,
        )
        status = "accepted" if result.get("status") in {"updated", "unchanged"} else "error"
        return {
            "status": status,
            "intent_type": intent.value,
            "identifier": raw_identifier,
            "message": f"playlist sync {result.get('status', 'completed')}",
            "enqueued_count": int(result.get("enqueued") or 0),
        }

    if intent == IntentType.SPOTIFY_TRACK:
        search_service = _resolve_search_service(config)
        if search_service is None:
            return _error_response(
                intent.value,
                raw_identifier,
                "search_service is required for spotify_track execution",
            )
        track = _fetch_spotify_track(spotify_client, raw_identifier)
        if not track:
            return _error_response(intent.value, raw_identifier, "track not found")
        await enqueue_spotify_track(
            queue=queue,
            spotify_track=track,
            search_service=search_service,
            playlist_id=f"spotify_track_{raw_identifier}",
        )
        return {
            "status": "accepted",
            "intent_type": intent.value,
            "identifier": raw_identifier,
            "message": "track enqueue attempted",
            "enqueued_count": 1,
        }

    if intent == IntentType.SPOTIFY_ALBUM:
        result = await run_spotify_album_sync(
            album_id=raw_identifier,
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
        )
        result["intent_type"] = intent.value
        result["identifier"] = raw_identifier
        return result

    if intent == IntentType.SPOTIFY_ARTIST:
        return {
            "status": "accepted",
            "intent_type": intent.value,
            "identifier": raw_identifier,
            "message": "artist intent requires selection before enqueue",
            "enqueued_count": 0,
        }

    return _error_response(intent.value, raw_identifier, "intent type not implemented")


def _error_response(intent_type: str, identifier: str, message: str) -> Dict[str, Any]:
    return {
        "status": "error",
        "intent_type": intent_type,
        "identifier": identifier,
        "message": message,
        "enqueued_count": 0,
    }


def _resolve_search_service(config: Any) -> Any:
    if isinstance(config, dict):
        return config.get("search_service")
    return getattr(config, "search_service", None)


def _resolve_playlist_name(playlist_id: str, config: Any) -> str:
    if not isinstance(config, dict):
        return playlist_id
    entries = config.get("spotify_playlists") or []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        candidate = str(entry.get("playlist_id") or "").strip()
        if candidate and candidate == playlist_id:
            name = str(entry.get("name") or "").strip()
            if name:
                return name
    return playlist_id


def _fetch_spotify_track(spotify_client: Any, track_id: str) -> dict[str, Any] | None:
    encoded = quote(track_id, safe="")
    payload = spotify_client._request_json(
        f"https://api.spotify.com/v1/tracks/{encoded}",
        params={"market": "from_token"},
    )
    if not isinstance(payload, dict) or not payload.get("id"):
        return None
    return _normalize_track_payload(payload, album_name=(payload.get("album") or {}).get("name"))


def _fetch_spotify_album_tracks(spotify_client: Any, album_id: str) -> tuple[str, list[dict[str, Any]]]:
    encoded = quote(album_id, safe="")
    album = spotify_client._request_json(
        f"https://api.spotify.com/v1/albums/{encoded}",
        params={"fields": "name,tracks(items(id,name,duration_ms,artists(name),disc_number,track_number),next)"},
    )
    title = str(album.get("name") or "")
    tracks_page = album.get("tracks") or {}
    items: list[dict[str, Any]] = []
    while True:
        for raw in tracks_page.get("items") or []:
            if not raw or not raw.get("id"):
                continue
            items.append(_normalize_track_payload(raw, album_name=title))
        next_url = tracks_page.get("next")
        if not next_url:
            break
        tracks_page = spotify_client._request_json(str(next_url))
    return title, items


async def run_spotify_album_sync(
    album_id: str,
    config,
    db,
    queue,
    spotify_client,
) -> Dict[str, Any]:
    """Run a one-shot Spotify album sync using existing enqueue and rebuild pipelines.

    Behavior mirrors playlist sync style orchestration:
    - fetch album metadata + ordered tracks,
    - enqueue each track via ``enqueue_spotify_track``,
    - best-effort rebuild of an album-scoped M3U from downloaded canonical paths.
    """
    album_identifier = str(album_id or "").strip()
    if not album_identifier:
        return {
            "status": "error",
            "intent_type": IntentType.SPOTIFY_ALBUM.value,
            "identifier": album_identifier,
            "message": "album_id is required",
            "enqueued_count": 0,
        }

    search_service = _resolve_search_service(config)
    if search_service is None:
        return {
            "status": "error",
            "intent_type": IntentType.SPOTIFY_ALBUM.value,
            "identifier": album_identifier,
            "message": "search_service is required for spotify_album execution",
            "enqueued_count": 0,
        }

    try:
        album_title, album_tracks, album_artist = _fetch_spotify_album_tracks_with_artist(
            spotify_client,
            album_identifier,
        )
    except Exception as exc:
        return {
            "status": "error",
            "intent_type": IntentType.SPOTIFY_ALBUM.value,
            "identifier": album_identifier,
            "message": f"album fetch failed: {exc}",
            "enqueued_count": 0,
        }

    if not album_tracks:
        return {
            "status": "error",
            "intent_type": IntentType.SPOTIFY_ALBUM.value,
            "identifier": album_identifier,
            "message": "album contains no tracks",
            "enqueued_count": 0,
        }

    playlist_id = f"spotify_album_{album_identifier}"
    enqueued = 0
    for track in album_tracks:
        await enqueue_spotify_track(
            queue=queue,
            spotify_track=track,
            search_service=search_service,
            playlist_id=playlist_id,
        )
        enqueued += 1

    try:
        downloaded_paths = _load_downloaded_paths_for_playlist(playlist_id)
        playlist_root, music_root = _resolve_playlist_dirs(config)
        artist_name = album_artist or "Unknown Artist"
        album_name = album_title or album_identifier
        playlist_name = f"Spotify - Album - {artist_name} - {album_name}"
        rebuild_playlist_from_tracks(
            playlist_name=playlist_name,
            playlist_root=playlist_root,
            music_root=music_root,
            track_file_paths=downloaded_paths,
        )
    except Exception:
        logging.exception("Album M3U rebuild failed for album %s", album_identifier)

    return {
        "status": "accepted",
        "intent_type": IntentType.SPOTIFY_ALBUM.value,
        "identifier": album_identifier,
        "message": f"album sync completed: {album_title or album_identifier}",
        "enqueued_count": enqueued,
    }


def _fetch_spotify_album_tracks_with_artist(
    spotify_client: Any,
    album_id: str,
) -> tuple[str, list[dict[str, Any]], str]:
    encoded = quote(album_id, safe="")
    album = spotify_client._request_json(
        f"https://api.spotify.com/v1/albums/{encoded}",
        params={
            "fields": (
                "name,artists(name),"
                "tracks(items(id,name,duration_ms,artists(name),disc_number,track_number,external_ids(isrc)),next)"
            )
        },
    )
    title = str(album.get("name") or "")
    album_artists = album.get("artists") or []
    album_artist = (
        album_artists[0].get("name")
        if album_artists and isinstance(album_artists[0], dict)
        else ""
    )

    tracks_page = album.get("tracks") or {}
    items: list[dict[str, Any]] = []
    while True:
        for raw in tracks_page.get("items") or []:
            if not raw or not raw.get("id"):
                continue
            items.append(_normalize_track_payload(raw, album_name=title))
        next_url = tracks_page.get("next")
        if not next_url:
            break
        tracks_page = spotify_client._request_json(str(next_url))
    return title, items, str(album_artist or "")


def _load_downloaded_paths_for_playlist(playlist_id: str) -> list[str]:
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT file_path
            FROM downloaded_music_tracks
            WHERE playlist_id=?
            ORDER BY downloaded_at ASC, id ASC
            """,
            (playlist_id,),
        )
        rows = cur.fetchall()
        return [str(row["file_path"]) for row in rows if row["file_path"]]
    except sqlite3.Error:
        logging.exception("Failed to load downloaded tracks for playlist %s", playlist_id)
        return []
    finally:
        if conn is not None:
            conn.close()


def _resolve_playlist_dirs(config: Any) -> tuple[Path, Path]:
    cfg = config if isinstance(config, dict) else {}
    music_root = Path(str(cfg.get("music_download_folder") or "Music"))
    playlist_root = Path(
        str(cfg.get("playlists_folder") or cfg.get("playlist_export_folder") or (music_root / "Playlists"))
    )
    return playlist_root, music_root


def _normalize_track_payload(track: dict[str, Any], *, album_name: str | None = None) -> dict[str, Any]:
    artists = track.get("artists") or []
    first_artist = artists[0].get("name") if artists and isinstance(artists[0], dict) else None
    external_ids = track.get("external_ids") or {}
    return {
        "spotify_track_id": track.get("id"),
        "artist": first_artist,
        "title": track.get("name"),
        "album": album_name or ((track.get("album") or {}).get("name")),
        "duration_ms": track.get("duration_ms"),
        "isrc": external_ids.get("isrc"),
        "track_num": track.get("track_number"),
        "disc_num": track.get("disc_number"),
    }
