"""Scheduler job for Spotify playlist change detection via snapshots."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

from db.playlist_snapshots import PlaylistSnapshotStore
from spotify.client import SpotifyPlaylistClient
from spotify.diff import diff_playlist


def run_spotify_playlist_watch_job(
    *,
    playlist_id: str,
    spotify_client: SpotifyPlaylistClient,
    snapshot_store: PlaylistSnapshotStore,
    enqueue_track: Callable[[dict[str, Any]], None],
    source: str = "spotify",
) -> dict[str, Any]:
    """Run one playlist watch cycle and enqueue newly added tracks only."""
    previous_uris = snapshot_store.get_latest_track_uris(source, playlist_id)

    snapshot_id, items = spotify_client.get_playlist_items(playlist_id)
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    write_result = snapshot_store.insert_snapshot(
        source=source,
        playlist_id=playlist_id,
        snapshot_id=snapshot_id,
        items=items,
        fetched_at=fetched_at,
    )
    if not write_result.inserted:
        return {
            "status": "unchanged",
            "playlist_id": playlist_id,
            "snapshot_id": snapshot_id,
            "snapshot_db_id": write_result.snapshot_db_id,
            "enqueued": 0,
            "diff": {"added": [], "removed": [], "moved": []},
        }

    current_uris = [item.get("uri") for item in items if item.get("uri")]
    changes = diff_playlist(previous_uris, current_uris)

    prev_counts = Counter(previous_uris)
    observed_counts = Counter()
    enqueued = 0
    for item in items:
        uri = item.get("uri")
        if not uri:
            continue
        observed_counts[uri] += 1
        if observed_counts[uri] <= prev_counts.get(uri, 0):
            continue
        enqueue_track(item)
        enqueued += 1

    return {
        "status": "updated",
        "playlist_id": playlist_id,
        "snapshot_id": snapshot_id,
        "snapshot_db_id": write_result.snapshot_db_id,
        "enqueued": enqueued,
        "diff": changes,
    }
