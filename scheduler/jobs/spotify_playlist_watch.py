"""Scheduler job for Spotify playlist snapshot monitoring."""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Callable

from db.downloaded_tracks import has_downloaded_isrc
from metadata.merge import merge_metadata
from playlist.rebuild import rebuild_playlist_from_tracks
from spotify.client import SpotifyPlaylistClient, get_playlist_items
from spotify.diff import diff_playlist
from spotify.oauth_store import SpotifyOAuthStore
from spotify.resolve import resolve_spotify_track

SPOTIFY_LIKED_SONGS_PLAYLIST_ID = "__spotify_liked_songs__"
SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID = "__spotify_saved_albums__"
SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID = "__spotify_user_playlists__"


def _load_previous_snapshot(db: Any, playlist_id: str) -> tuple[str | None, list[dict[str, Any]]]:
    if not hasattr(db, "get_latest_snapshot"):
        return None, []
    latest = db.get_latest_snapshot(playlist_id)
    if latest is None:
        return None, []
    if isinstance(latest, tuple) and len(latest) == 2:
        snapshot_id, items = latest
        return snapshot_id, list(items or [])
    if isinstance(latest, dict):
        return latest.get("snapshot_id"), list(latest.get("items") or [])
    return None, []


def get_liked_songs_playlist_name() -> str:
    """Return the virtual playlist display name for Spotify Liked Songs."""
    return "Spotify - Liked Songs"


def run_liked_songs_sync() -> None:
    """Placeholder for future OAuth-based liked songs sync.

    Will:
    - Fetch /me/tracks
    - Diff snapshot
    - Enqueue new tracks
    - Rebuild M3U
    Currently not implemented.
    """
    logging.info("Liked Songs sync not enabled (OAuth required)")


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return None


def _resolve_db_path() -> str:
    return os.environ.get("RETREIVR_DB_PATH", os.path.join(os.getcwd(), "retreivr.sqlite3"))


def _load_downloaded_track_paths(playlist_id: str) -> list[str]:
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(_resolve_db_path(), check_same_thread=False, timeout=30)
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
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _load_downloaded_track_paths_for_playlist_ids(playlist_ids: list[str]) -> list[str]:
    cleaned = [str(pid).strip() for pid in playlist_ids if str(pid).strip()]
    if not cleaned:
        return []

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(_resolve_db_path(), check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        placeholders = ", ".join(["?"] * len(cleaned))
        cur.execute(
            f"""
            SELECT file_path
            FROM downloaded_music_tracks
            WHERE playlist_id IN ({placeholders})
            ORDER BY downloaded_at ASC, id ASC
            """,
            tuple(cleaned),
        )
        rows = cur.fetchall()
        return [str(row["file_path"]) for row in rows if row["file_path"]]
    except sqlite3.Error:
        logging.exception("Failed to load downloaded tracks for playlist IDs: %s", cleaned)
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _resolve_playlist_dirs(config: dict[str, Any] | None) -> tuple[Path, Path]:
    cfg = config or {}
    music_root = Path(str(cfg.get("music_download_folder") or "Music"))
    playlist_root = Path(
        str(
            cfg.get("playlists_folder")
            or cfg.get("playlist_export_folder")
            or (music_root / "Playlists")
        )
    )
    return playlist_root, music_root


def _spotify_client_credentials_from_config(config: dict[str, Any] | None) -> tuple[str, str]:
    cfg = config or {}
    spotify_cfg = (cfg.get("spotify") or {}) if isinstance(cfg, dict) else {}
    client_id = str(spotify_cfg.get("client_id") or cfg.get("SPOTIFY_CLIENT_ID") or "").strip()
    client_secret = str(spotify_cfg.get("client_secret") or cfg.get("SPOTIFY_CLIENT_SECRET") or "").strip()
    return client_id, client_secret


def _resolve_db_path_from_runtime(db: Any) -> str:
    if hasattr(db, "db_path"):
        value = str(getattr(db, "db_path") or "").strip()
        if value:
            return value
    return _resolve_db_path()


def _best_effort_rebuild_playlist_m3u(
    *,
    playlist_id: str,
    playlist_name: str,
    config: dict[str, Any] | None,
) -> None:
    """Rebuild playlist M3U from downloaded canonical paths without raising errors."""
    try:
        track_paths = _load_downloaded_track_paths(playlist_id)
        playlist_root, music_root = _resolve_playlist_dirs(config)
        rebuild_playlist_from_tracks(
            playlist_name=(playlist_name or playlist_id).strip() or playlist_id,
            playlist_root=playlist_root,
            music_root=music_root,
            track_file_paths=track_paths,
        )
        logging.info("Playlist M3U updated: %s (%d tracks)", playlist_name, len(track_paths))
    except Exception:
        logging.exception("Playlist M3U rebuild failed for playlist %s", playlist_id)


def _enqueue_added_track(queue: Any, item: dict[str, Any]) -> None:
    if callable(queue):
        queue(item)
        return
    for method_name in ("enqueue", "put", "add", "enqueue_track"):
        method = getattr(queue, method_name, None)
        if callable(method):
            method(item)
            return
    raise TypeError("queue does not expose a supported enqueue method")


async def enqueue_spotify_track(queue, spotify_track: dict, search_service, playlist_id: str):
    """Resolve a Spotify track, merge metadata, build payload, and enqueue it.

    Idempotency skip is applied only when a non-empty ISRC exists and that
    `(playlist_id, isrc)` has already been recorded as downloaded. Tracks with
    missing/empty ISRC are always treated as normal enqueue candidates.
    """
    track_isrc = str((spotify_track or {}).get("isrc") or "").strip()
    if track_isrc and has_downloaded_isrc(playlist_id, track_isrc):
        logging.info("skip duplicate isrc=%s playlist=%s", track_isrc, playlist_id)
        return

    resolved_media = await resolve_spotify_track(spotify_track, search_service)
    merged_metadata = merge_metadata(spotify_track or {}, {}, resolved_media.get("extra") or {})
    payload = {
        "playlist_id": playlist_id,
        "spotify_track_id": (spotify_track or {}).get("spotify_track_id"),
        "resolved_media": resolved_media,
        "music_metadata": merged_metadata,
    }
    queue.enqueue(payload)


async def spotify_liked_songs_watch_job(config, db, queue, spotify_client, search_service):
    """Sync Spotify Liked Songs using OAuth-backed `/v1/me/tracks` snapshots."""
    client_id, client_secret = _spotify_client_credentials_from_config(config if isinstance(config, dict) else None)
    if not client_id or not client_secret:
        logging.info("Liked Songs sync skipped: Spotify credentials not configured")
        return {"status": "skipped", "playlist_id": SPOTIFY_LIKED_SONGS_PLAYLIST_ID, "enqueued": 0}

    oauth_store = SpotifyOAuthStore(Path(_resolve_db_path_from_runtime(db)))
    token = oauth_store.get_valid_token(client_id, client_secret, config=config if isinstance(config, dict) else None)
    if token is None:
        logging.info("Liked Songs sync skipped: no valid Spotify OAuth token")
        return {"status": "skipped", "playlist_id": SPOTIFY_LIKED_SONGS_PLAYLIST_ID, "enqueued": 0}

    liked_client: Any = spotify_client
    if isinstance(liked_client, SpotifyPlaylistClient):
        liked_client._provided_access_token = token.access_token
    elif not hasattr(liked_client, "get_liked_songs"):
        liked_client = SpotifyPlaylistClient(
            client_id=client_id,
            client_secret=client_secret,
            access_token=token.access_token,
        )

    try:
        current_snapshot_id, current_items = await liked_client.get_liked_songs()
    except Exception as exc:
        logging.exception("Liked Songs fetch failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
            "error": f"spotify_fetch_failed: {exc}",
        }

    try:
        previous_snapshot_id, previous_items = _load_previous_snapshot(db, SPOTIFY_LIKED_SONGS_PLAYLIST_ID)
    except Exception as exc:
        logging.exception("Liked Songs snapshot load failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
            "error": f"snapshot_read_failed: {exc}",
        }

    if previous_snapshot_id == current_snapshot_id:
        return {
            "status": "unchanged",
            "playlist_id": SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
            "snapshot_id": current_snapshot_id,
            "enqueued": 0,
        }

    diff = diff_playlist(previous_items, current_items)
    added_items = list(diff["added"])
    enqueued = 0
    enqueue_errors: list[str] = []
    for track in added_items:
        try:
            await enqueue_spotify_track(
                queue,
                track,
                search_service,
                playlist_id=SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
            )
            enqueued += 1
        except Exception as exc:
            track_id = track.get("spotify_track_id")
            enqueue_errors.append(f"{track_id}: {exc}")
            logging.exception("Failed to enqueue Liked Songs track %s", track_id)

    try:
        db.store_snapshot(
            SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
            str(current_snapshot_id),
            current_items,
        )
    except Exception as exc:
        logging.exception("Liked Songs snapshot store failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
            "snapshot_id": current_snapshot_id,
            "error": f"snapshot_store_failed: {exc}",
            "enqueued": enqueued,
            "added_count": len(added_items),
            "removed_count": len(diff["removed"]),
            "moved_count": len(diff["moved"]),
            "enqueue_errors": enqueue_errors,
        }

    _best_effort_rebuild_playlist_m3u(
        playlist_id=SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
        playlist_name=get_liked_songs_playlist_name(),
        config=config if isinstance(config, dict) else None,
    )

    return {
        "status": "updated",
        "playlist_id": SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
        "snapshot_id": current_snapshot_id,
        "enqueued": enqueued,
        "added_count": len(added_items),
        "removed_count": len(diff["removed"]),
        "moved_count": len(diff["moved"]),
        "enqueue_errors": enqueue_errors,
    }


async def spotify_saved_albums_watch_job(config, db, queue, spotify_client, search_service):
    """Sync Spotify Saved Albums via OAuth and enqueue newly added albums."""
    client_id, client_secret = _spotify_client_credentials_from_config(config if isinstance(config, dict) else None)
    if not client_id or not client_secret:
        logging.info("Saved Albums sync skipped: Spotify credentials not configured")
        return {"status": "skipped", "playlist_id": SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID, "enqueued": 0}

    oauth_store = SpotifyOAuthStore(Path(_resolve_db_path_from_runtime(db)))
    token = oauth_store.get_valid_token(client_id, client_secret, config=config if isinstance(config, dict) else None)
    if token is None:
        logging.info("Saved Albums sync skipped: no valid Spotify OAuth token")
        return {"status": "skipped", "playlist_id": SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID, "enqueued": 0}

    saved_albums_client: Any = spotify_client
    if isinstance(saved_albums_client, SpotifyPlaylistClient):
        saved_albums_client._provided_access_token = token.access_token
    elif not hasattr(saved_albums_client, "get_saved_albums"):
        saved_albums_client = SpotifyPlaylistClient(
            client_id=client_id,
            client_secret=client_secret,
            access_token=token.access_token,
        )

    try:
        current_snapshot_id, current_albums = await saved_albums_client.get_saved_albums()
    except Exception as exc:
        logging.exception("Saved Albums fetch failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID,
            "error": f"spotify_fetch_failed: {exc}",
        }

    current_snapshot_items: list[dict[str, Any]] = []
    album_map: dict[str, dict[str, Any]] = {}
    for idx, album in enumerate(current_albums or []):
        album_id = str((album or {}).get("album_id") or "").strip()
        if not album_id:
            continue
        current_snapshot_items.append(
            {
                "spotify_track_id": album_id,
                "position": idx,
                "added_at": (album or {}).get("added_at"),
            }
        )
        album_map[album_id] = dict(album)

    try:
        previous_snapshot_id, previous_items = _load_previous_snapshot(db, SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID)
    except Exception as exc:
        logging.exception("Saved Albums snapshot load failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID,
            "error": f"snapshot_read_failed: {exc}",
        }

    if previous_snapshot_id == current_snapshot_id:
        return {
            "status": "unchanged",
            "playlist_id": SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID,
            "snapshot_id": current_snapshot_id,
            "enqueued": 0,
        }

    diff = diff_playlist(previous_items, current_snapshot_items)
    added_albums = list(diff["added"])
    enqueued = 0
    enqueue_errors: list[str] = []

    dispatcher_config = dict(config) if isinstance(config, dict) else {}
    dispatcher_config["search_service"] = search_service

    # Local import avoids a module import cycle with api.intent_dispatcher.
    from api.intent_dispatcher import run_spotify_album_sync

    for album_item in added_albums:
        album_id = str((album_item or {}).get("spotify_track_id") or "").strip()
        if not album_id:
            continue
        try:
            await run_spotify_album_sync(
                album_id=album_id,
                config=dispatcher_config,
                db=db,
                queue=queue,
                spotify_client=saved_albums_client,
            )
            enqueued += 1
        except Exception as exc:
            enqueue_errors.append(f"{album_id}: {exc}")
            logging.exception("Saved Albums enqueue failed for album %s", album_id)

    try:
        db.store_snapshot(
            SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID,
            str(current_snapshot_id),
            current_snapshot_items,
        )
    except Exception as exc:
        logging.exception("Saved Albums snapshot store failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID,
            "snapshot_id": current_snapshot_id,
            "error": f"snapshot_store_failed: {exc}",
            "enqueued": enqueued,
            "added_count": len(added_albums),
            "removed_count": len(diff["removed"]),
            "moved_count": len(diff["moved"]),
            "enqueue_errors": enqueue_errors,
        }

    # Best effort: rebuild a virtual "Saved Albums" M3U from album-scoped downloads.
    try:
        album_playlist_ids = [
            f"spotify_album_{str((item or {}).get('spotify_track_id') or '').strip()}"
            for item in current_snapshot_items
            if str((item or {}).get("spotify_track_id") or "").strip()
        ]
        track_paths = _load_downloaded_track_paths_for_playlist_ids(album_playlist_ids)
        playlist_root, music_root = _resolve_playlist_dirs(config if isinstance(config, dict) else None)
        rebuild_playlist_from_tracks(
            playlist_name="Spotify - Saved Albums",
            playlist_root=playlist_root,
            music_root=music_root,
            track_file_paths=track_paths,
        )
        logging.info("Playlist M3U updated: Spotify - Saved Albums (%d tracks)", len(track_paths))
    except Exception:
        logging.exception("Saved Albums M3U rebuild failed")

    return {
        "status": "updated",
        "playlist_id": SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID,
        "snapshot_id": current_snapshot_id,
        "enqueued": enqueued,
        "added_count": len(added_albums),
        "removed_count": len(diff["removed"]),
        "moved_count": len(diff["moved"]),
        "enqueue_errors": enqueue_errors,
    }


async def spotify_user_playlists_watch_job(config, db, queue, spotify_client, search_service):
    """Sync authenticated user's Spotify playlists and trigger sync for new playlists."""
    client_id, client_secret = _spotify_client_credentials_from_config(config if isinstance(config, dict) else None)
    if not client_id or not client_secret:
        logging.info("User Playlists sync skipped: Spotify credentials not configured")
        return {"status": "skipped", "playlist_id": SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID, "enqueued": 0}

    oauth_store = SpotifyOAuthStore(Path(_resolve_db_path_from_runtime(db)))
    token = oauth_store.get_valid_token(client_id, client_secret, config=config if isinstance(config, dict) else None)
    if token is None:
        logging.info("User Playlists sync skipped: no valid Spotify OAuth token")
        return {"status": "skipped", "playlist_id": SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID, "enqueued": 0}

    playlists_client: Any = spotify_client
    if isinstance(playlists_client, SpotifyPlaylistClient):
        playlists_client._provided_access_token = token.access_token
    elif not hasattr(playlists_client, "get_user_playlists"):
        playlists_client = SpotifyPlaylistClient(
            client_id=client_id,
            client_secret=client_secret,
            access_token=token.access_token,
        )

    try:
        current_snapshot_id, current_playlists = await playlists_client.get_user_playlists()
    except Exception as exc:
        logging.exception("User Playlists fetch failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID,
            "error": f"spotify_fetch_failed: {exc}",
        }

    current_snapshot_items: list[dict[str, Any]] = []
    playlist_name_by_id: dict[str, str] = {}
    for idx, playlist in enumerate(current_playlists or []):
        playlist_id = str((playlist or {}).get("id") or "").strip()
        if not playlist_id:
            continue
        playlist_name_by_id[playlist_id] = str((playlist or {}).get("name") or "").strip() or playlist_id
        current_snapshot_items.append(
            {
                "spotify_track_id": playlist_id,
                "position": idx,
                "added_at": None,
            }
        )

    try:
        previous_snapshot_id, previous_items = _load_previous_snapshot(db, SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID)
    except Exception as exc:
        logging.exception("User Playlists snapshot load failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID,
            "error": f"snapshot_read_failed: {exc}",
        }

    if previous_snapshot_id == current_snapshot_id:
        return {
            "status": "unchanged",
            "playlist_id": SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID,
            "snapshot_id": current_snapshot_id,
            "enqueued": 0,
        }

    diff = diff_playlist(previous_items, current_snapshot_items)
    added_playlists = list(diff["added"])
    synced = 0
    sync_errors: list[str] = []
    for playlist_item in added_playlists:
        playlist_id = str((playlist_item or {}).get("spotify_track_id") or "").strip()
        if not playlist_id:
            continue
        playlist_name = playlist_name_by_id.get(playlist_id, playlist_id)
        try:
            playlist_watch_job(
                spotify_client=playlists_client,
                db=db,
                queue=queue,
                playlist_id=playlist_id,
                playlist_name=playlist_name,
                config=config if isinstance(config, dict) else None,
            )
            synced += 1
        except Exception as exc:
            sync_errors.append(f"{playlist_id}: {exc}")
            logging.exception("User Playlists sync failed for playlist %s", playlist_id)

    try:
        db.store_snapshot(
            SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID,
            str(current_snapshot_id),
            current_snapshot_items,
        )
    except Exception as exc:
        logging.exception("User Playlists snapshot store failed")
        return {
            "status": "error",
            "playlist_id": SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID,
            "snapshot_id": current_snapshot_id,
            "error": f"snapshot_store_failed: {exc}",
            "enqueued": synced,
            "added_count": len(added_playlists),
            "removed_count": len(diff["removed"]),
            "moved_count": len(diff["moved"]),
            "enqueue_errors": sync_errors,
        }

    return {
        "status": "updated",
        "playlist_id": SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID,
        "snapshot_id": current_snapshot_id,
        "enqueued": synced,
        "added_count": len(added_playlists),
        "removed_count": len(diff["removed"]),
        "moved_count": len(diff["moved"]),
        "enqueue_errors": sync_errors,
    }


def playlist_watch_job(
    spotify_client,
    db,
    queue,
    playlist_id: str,
    *,
    playlist_name: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch playlist snapshot, diff with DB state, enqueue added tracks, and persist new snapshot."""
    pid = (playlist_id or "").strip()
    if not pid:
        return {"status": "error", "playlist_id": playlist_id, "error": "playlist_id is required"}

    try:
        if hasattr(spotify_client, "get_playlist_items") and callable(spotify_client.get_playlist_items):
            current_snapshot_id, current_items = spotify_client.get_playlist_items(pid)
        else:
            result = _run_async(get_playlist_items(spotify_client, pid))
            if result is None:
                raise RuntimeError("Cannot run async Spotify fetch inside active event loop")
            current_snapshot_id, current_items = result
    except Exception as exc:
        logging.exception("Spotify fetch failed for playlist %s", pid)
        return {"status": "error", "playlist_id": pid, "error": f"spotify_fetch_failed: {exc}"}

    try:
        previous_snapshot_id, previous_items = _load_previous_snapshot(db, pid)
    except Exception as exc:
        logging.exception("Snapshot load failed for playlist %s", pid)
        return {"status": "error", "playlist_id": pid, "error": f"snapshot_read_failed: {exc}"}

    if previous_snapshot_id == current_snapshot_id:
        return {"status": "unchanged", "playlist_id": pid, "snapshot_id": current_snapshot_id, "enqueued": 0}

    diff = diff_playlist(previous_items, current_items)
    added_items = list(diff["added"])
    enqueued = 0
    enqueue_errors: list[str] = []
    for item in added_items:
        try:
            _enqueue_added_track(queue, item)
            enqueued += 1
        except Exception as exc:
            track_id = item.get("spotify_track_id")
            enqueue_errors.append(f"{track_id}: {exc}")
            logging.exception("Enqueue failed for added Spotify track %s", track_id)

    try:
        db.store_snapshot(pid, str(current_snapshot_id), current_items)
    except Exception as exc:
        logging.exception("Snapshot store failed for playlist %s", pid)
        return {
            "status": "error",
            "playlist_id": pid,
            "snapshot_id": current_snapshot_id,
            "error": f"snapshot_store_failed: {exc}",
            "enqueued": enqueued,
            "added_count": len(added_items),
            "removed_count": len(diff["removed"]),
            "moved_count": len(diff["moved"]),
            "enqueue_errors": enqueue_errors,
        }

    _best_effort_rebuild_playlist_m3u(
        playlist_id=pid,
        playlist_name=(playlist_name or pid).strip() or pid,
        config=config,
    )

    return {
        "status": "updated",
        "playlist_id": pid,
        "snapshot_id": current_snapshot_id,
        "enqueued": enqueued,
        "added_count": len(added_items),
        "removed_count": len(diff["removed"]),
        "moved_count": len(diff["moved"]),
        "enqueue_errors": enqueue_errors,
    }


def run_spotify_playlist_watch_job(
    *,
    playlist_id: str,
    spotify_client: SpotifyPlaylistClient,
    snapshot_store: Any,
    enqueue_track: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    """Compatibility wrapper around `playlist_watch_job` for existing call sites."""
    return playlist_watch_job(spotify_client, snapshot_store, enqueue_track, playlist_id)
