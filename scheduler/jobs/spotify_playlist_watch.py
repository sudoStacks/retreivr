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
from spotify.resolve import resolve_spotify_track

SPOTIFY_LIKED_SONGS_PLAYLIST_ID = "__spotify_liked_songs__"


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

    # Best effort: refresh the playlist M3U from canonical downloaded file paths.
    try:
        track_paths = _load_downloaded_track_paths(pid)
        resolved_playlist_name = (playlist_name or pid).strip() or pid
        playlist_root, music_root = _resolve_playlist_dirs(config)
        rebuild_playlist_from_tracks(
            playlist_name=resolved_playlist_name,
            playlist_root=playlist_root,
            music_root=music_root,
            track_file_paths=track_paths,
        )
        logging.info("Playlist M3U updated: %s (%d tracks)", resolved_playlist_name, len(track_paths))
    except Exception:
        logging.exception("Playlist M3U rebuild failed for playlist %s", pid)

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
