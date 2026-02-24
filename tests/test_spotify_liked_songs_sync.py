from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from scheduler.jobs.spotify_playlist_watch import (
    SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
    get_liked_songs_playlist_name,
    spotify_liked_songs_watch_job,
)


class _FakeSpotifyClient:
    def __init__(self, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.snapshot_id = snapshot_id
        self.items = items

    async def get_liked_songs(self) -> tuple[str, list[dict[str, Any]]]:
        return self.snapshot_id, list(self.items)


class _FakeSnapshotStore:
    def __init__(self, latest_snapshot: dict[str, Any] | None) -> None:
        self.latest_snapshot = latest_snapshot
        self.store_calls: list[tuple[str, str, list[dict[str, Any]]]] = []

    def get_latest_snapshot(self, playlist_id: str) -> dict[str, Any] | None:
        if self.latest_snapshot is None:
            return None
        return self.latest_snapshot

    def store_snapshot(self, playlist_id: str, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.store_calls.append((playlist_id, snapshot_id, list(items)))


def _item(track_id: str, position: int) -> dict[str, Any]:
    return {
        "spotify_track_id": track_id,
        "position": position,
        "added_at": f"2026-02-16T00:0{position}:00Z",
        "artist": f"Artist {track_id}",
        "title": f"Title {track_id}",
        "album": "Album",
        "duration_ms": 123000,
        "isrc": f"ISRC{track_id}",
    }


def test_liked_songs_sync_enqueues_added_tracks_and_rebuilds_m3u(monkeypatch) -> None:
    config = {"spotify": {"client_id": "cid", "client_secret": "csec"}}
    db = _FakeSnapshotStore(latest_snapshot=None)
    queue = object()
    search_service = object()
    spotify_client = _FakeSpotifyClient("snap-liked-1", [_item("a", 0), _item("b", 1)])

    class _FakeOAuthStore:
        def __init__(self, _db_path):
            pass

        def get_valid_token(self, _client_id, _client_secret, config=None):
            return SimpleNamespace(access_token="oauth-token")

    enqueue_calls: list[tuple[str, dict[str, Any]]] = []

    async def _spy_enqueue_spotify_track(queue, spotify_track: dict, search_service, playlist_id: str):
        enqueue_calls.append((playlist_id, dict(spotify_track)))

    rebuild_calls: list[dict[str, Any]] = []

    def _spy_rebuild_playlist_from_tracks(playlist_name, playlist_root, music_root, track_file_paths):
        rebuild_calls.append(
            {
                "playlist_name": playlist_name,
                "playlist_root": playlist_root,
                "music_root": music_root,
                "track_file_paths": list(track_file_paths),
            }
        )
        return playlist_root / f"{playlist_name}.m3u"

    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.SpotifyOAuthStore", _FakeOAuthStore)
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.enqueue_spotify_track",
        _spy_enqueue_spotify_track,
    )
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch._load_downloaded_track_paths",
        lambda _playlist_id: [],
    )
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.rebuild_playlist_from_tracks",
        _spy_rebuild_playlist_from_tracks,
    )

    result = asyncio.run(
        spotify_liked_songs_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
        )
    )

    assert result["status"] == "updated"
    assert result["playlist_id"] == SPOTIFY_LIKED_SONGS_PLAYLIST_ID
    assert result["enqueued"] == 2

    assert [call[0] for call in enqueue_calls] == [
        SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
        SPOTIFY_LIKED_SONGS_PLAYLIST_ID,
    ]
    assert [call[1]["spotify_track_id"] for call in enqueue_calls] == ["a", "b"]

    assert len(db.store_calls) == 1
    assert db.store_calls[0][0] == SPOTIFY_LIKED_SONGS_PLAYLIST_ID
    assert db.store_calls[0][1] == "snap-liked-1"

    assert len(rebuild_calls) == 1
    assert rebuild_calls[0]["playlist_name"] == get_liked_songs_playlist_name()


def test_liked_songs_sync_exits_cleanly_when_oauth_token_missing(monkeypatch) -> None:
    config = {"spotify": {"client_id": "cid", "client_secret": "csec"}}
    db = _FakeSnapshotStore(latest_snapshot=None)
    queue = object()
    search_service = object()
    spotify_client = _FakeSpotifyClient("snap-liked-1", [_item("a", 0)])

    class _FakeOAuthStore:
        def __init__(self, _db_path):
            pass

        def get_valid_token(self, _client_id, _client_secret, config=None):
            return None

    enqueue_calls: list[dict[str, Any]] = []

    async def _spy_enqueue_spotify_track(queue, spotify_track: dict, search_service, playlist_id: str):
        enqueue_calls.append(dict(spotify_track))

    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.SpotifyOAuthStore", _FakeOAuthStore)
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.enqueue_spotify_track",
        _spy_enqueue_spotify_track,
    )

    result = asyncio.run(
        spotify_liked_songs_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
        )
    )

    assert result["status"] == "skipped"
    assert result["playlist_id"] == SPOTIFY_LIKED_SONGS_PLAYLIST_ID
    assert result["enqueued"] == 0
    assert enqueue_calls == []
    assert db.store_calls == []


def test_liked_songs_sync_counts_skipped_from_enqueue_result(monkeypatch) -> None:
    config = {"spotify": {"client_id": "cid", "client_secret": "csec"}}
    db = _FakeSnapshotStore(latest_snapshot=None)
    queue = object()
    search_service = object()
    spotify_client = _FakeSpotifyClient("snap-liked-2", [_item("a", 0), _item("b", 1)])

    class _FakeOAuthStore:
        def __init__(self, _db_path):
            pass

        def get_valid_token(self, _client_id, _client_secret, config=None):
            return SimpleNamespace(access_token="oauth-token")

    async def _spy_enqueue_spotify_track(queue, spotify_track: dict, search_service, playlist_id: str):
        if spotify_track.get("spotify_track_id") == "a":
            return {"created": False, "reason": "duplicate_isrc"}
        return {"created": True}

    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.SpotifyOAuthStore", _FakeOAuthStore)
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.enqueue_spotify_track",
        _spy_enqueue_spotify_track,
    )
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch._load_downloaded_track_paths",
        lambda _playlist_id: [],
    )

    result = asyncio.run(
        spotify_liked_songs_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
        )
    )

    assert result["status"] == "updated"
    assert result["enqueued"] == 1
    assert result["skipped"] == 1
