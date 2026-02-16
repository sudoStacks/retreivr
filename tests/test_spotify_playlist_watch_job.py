from __future__ import annotations

from typing import Any

from scheduler.jobs.spotify_playlist_watch import run_spotify_playlist_watch_job


def _item(track_id: str, position: int) -> dict[str, Any]:
    return {
        "spotify_track_id": track_id,
        "position": position,
        "added_at": f"2026-02-16T00:0{position}:00Z",
        "artist": f"artist-{track_id}",
        "title": f"title-{track_id}",
        "album": f"album-{track_id}",
        "duration_ms": 1000 + position,
        "isrc": f"isrc-{track_id}",
    }


class _MockSpotifyClient:
    def __init__(self, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.snapshot_id = snapshot_id
        self.items = items
        self.calls: list[str] = []

    def get_playlist_items(self, playlist_id: str) -> tuple[str, list[dict[str, Any]]]:
        self.calls.append(playlist_id)
        return self.snapshot_id, list(self.items)


class _MockSnapshotStore:
    def __init__(self, latest_snapshot: dict[str, Any] | None) -> None:
        self.latest_snapshot = latest_snapshot
        self.get_calls: list[str] = []
        self.store_calls: list[tuple[str, str, list[dict[str, Any]]]] = []

    def get_latest_snapshot(self, playlist_id: str) -> dict[str, Any] | None:
        self.get_calls.append(playlist_id)
        return self.latest_snapshot

    def store_snapshot(self, playlist_id: str, snapshot_id: str, items: list[dict[str, Any]]) -> Any:
        self.store_calls.append((playlist_id, snapshot_id, list(items)))
        return type("WriteResult", (), {"snapshot_db_id": 42})()


def test_watch_job_unchanged_snapshot_exits_without_enqueue() -> None:
    prev_items = [_item("a", 0)]
    store = _MockSnapshotStore({"snapshot_id": "snap-1", "items": prev_items})
    client = _MockSpotifyClient("snap-1", [_item("a", 0), _item("b", 1)])
    enqueued: list[str] = []

    result = run_spotify_playlist_watch_job(
        playlist_id="playlist-1",
        spotify_client=client,
        snapshot_store=store,
        enqueue_track=lambda item: enqueued.append(str(item["spotify_track_id"])),
    )

    assert result["status"] == "unchanged"
    assert result["enqueued"] == 0
    assert enqueued == []
    assert store.store_calls == []


def test_watch_job_enqueues_only_added_items_in_order() -> None:
    prev_items = [_item("a", 0), _item("b", 1)]
    curr_items = [_item("a", 0), _item("b", 1), _item("c", 2), _item("d", 3)]
    store = _MockSnapshotStore({"snapshot_id": "snap-1", "items": prev_items})
    client = _MockSpotifyClient("snap-2", curr_items)
    enqueued: list[str] = []

    result = run_spotify_playlist_watch_job(
        playlist_id="playlist-1",
        spotify_client=client,
        snapshot_store=store,
        enqueue_track=lambda item: enqueued.append(str(item["spotify_track_id"])),
    )

    assert result["status"] == "updated"
    assert result["added_count"] == 2
    assert result["enqueued"] == 2
    assert enqueued == ["c", "d"]
    assert len(store.store_calls) == 1
    assert store.store_calls[0][1] == "snap-2"


def test_watch_job_moved_items_do_not_enqueue() -> None:
    prev_items = [_item("a", 0), _item("b", 1), _item("c", 2)]
    curr_items = [_item("b", 0), _item("a", 1), _item("c", 2)]
    store = _MockSnapshotStore({"snapshot_id": "snap-1", "items": prev_items})
    client = _MockSpotifyClient("snap-2", curr_items)
    enqueued: list[str] = []

    result = run_spotify_playlist_watch_job(
        playlist_id="playlist-1",
        spotify_client=client,
        snapshot_store=store,
        enqueue_track=lambda item: enqueued.append(str(item["spotify_track_id"])),
    )

    assert result["status"] == "updated"
    assert result["added_count"] == 0
    assert result["moved_count"] == 2
    assert result["enqueued"] == 0
    assert enqueued == []
    assert len(store.store_calls) == 1

