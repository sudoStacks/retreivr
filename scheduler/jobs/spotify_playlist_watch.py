"""Scheduler job for Spotify playlist snapshot monitoring."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from metadata.merge import merge_metadata
from spotify.client import SpotifyPlaylistClient, get_playlist_items
from spotify.diff import diff_playlist
from spotify.resolve import resolve_spotify_track


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


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return None


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
    """Resolve a Spotify track, merge metadata, build payload, and enqueue it."""
    resolved_media = await resolve_spotify_track(spotify_track, search_service)
    merged_metadata = merge_metadata(spotify_track or {}, {}, resolved_media.get("extra") or {})
    payload = {
        "playlist_id": playlist_id,
        "spotify_track_id": (spotify_track or {}).get("spotify_track_id"),
        "resolved_media": resolved_media,
        "music_metadata": merged_metadata,
    }
    queue.enqueue(payload)


def playlist_watch_job(spotify_client, db, queue, playlist_id: str) -> dict[str, Any]:
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
