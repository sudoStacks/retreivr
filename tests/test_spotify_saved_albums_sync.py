from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from scheduler.jobs.spotify_playlist_watch import (
    SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID,
    spotify_saved_albums_watch_job,
)


class _FakeSpotifyClient:
    def __init__(self, snapshot_id: str, albums: list[dict[str, Any]]) -> None:
        self.snapshot_id = snapshot_id
        self.albums = albums

    async def get_saved_albums(self) -> tuple[str, list[dict[str, Any]]]:
        return self.snapshot_id, list(self.albums)


class _FakeSnapshotStore:
    def __init__(self, latest_snapshot: dict[str, Any] | None) -> None:
        self.latest_snapshot = latest_snapshot
        self.store_calls: list[tuple[str, str, list[dict[str, Any]]]] = []

    def get_latest_snapshot(self, playlist_id: str) -> dict[str, Any] | None:
        return self.latest_snapshot

    def store_snapshot(self, playlist_id: str, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.store_calls.append((playlist_id, snapshot_id, list(items)))


def _album(album_id: str, position: int) -> dict[str, Any]:
    return {
        "album_id": album_id,
        "position": position,
        "added_at": f"2026-02-16T00:0{position}:00Z",
        "name": f"Album {album_id}",
        "artist": f"Artist {album_id}",
        "artists": [f"Artist {album_id}"],
        "release_date": "2024-01-01",
        "total_tracks": 10,
        "tracks": [],
    }


def test_saved_albums_sync_triggers_album_sync_only_for_new_albums(monkeypatch) -> None:
    config = {"spotify": {"client_id": "cid", "client_secret": "csec"}}
    previous_items = [
        {"spotify_track_id": "album-a", "position": 0, "added_at": "2026-02-16T00:00:00Z"},
    ]
    db = _FakeSnapshotStore({"snapshot_id": "snap-prev", "items": previous_items})
    queue = object()
    search_service = object()
    spotify_client = _FakeSpotifyClient(
        "snap-next",
        [_album("album-a", 0), _album("album-b", 1), _album("album-c", 2)],
    )

    class _FakeOAuthStore:
        def __init__(self, _db_path):
            pass

        def get_valid_token(self, _client_id, _client_secret, config=None):
            return SimpleNamespace(access_token="oauth-token")

    album_sync_calls: list[str] = []

    async def _spy_run_spotify_album_sync(*, album_id, config, db, queue, spotify_client):
        album_sync_calls.append(str(album_id))
        return {
            "status": "accepted",
            "intent_type": "spotify_album",
            "identifier": str(album_id),
            "message": "ok",
            "enqueued_count": 1,
        }

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
    monkeypatch.setattr("api.intent_dispatcher.run_spotify_album_sync", _spy_run_spotify_album_sync)
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch._load_downloaded_track_paths_for_playlist_ids",
        lambda _playlist_ids: [],
    )
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.rebuild_playlist_from_tracks",
        _spy_rebuild_playlist_from_tracks,
    )

    result = asyncio.run(
        spotify_saved_albums_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
        )
    )

    assert result["status"] == "updated"
    assert result["playlist_id"] == SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID
    assert album_sync_calls == ["album-b", "album-c"]

    assert len(db.store_calls) == 1
    assert db.store_calls[0][0] == SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID
    assert db.store_calls[0][1] == "snap-next"

    assert len(rebuild_calls) == 1
    assert rebuild_calls[0]["playlist_name"] == "Spotify - Saved Albums"


def test_saved_albums_sync_skips_when_oauth_token_missing(monkeypatch) -> None:
    config = {"spotify": {"client_id": "cid", "client_secret": "csec"}}
    db = _FakeSnapshotStore(latest_snapshot=None)
    queue = object()
    search_service = object()
    spotify_client = _FakeSpotifyClient("snap-next", [_album("album-a", 0)])

    class _FakeOAuthStore:
        def __init__(self, _db_path):
            pass

        def get_valid_token(self, _client_id, _client_secret, config=None):
            return None

    album_sync_calls: list[str] = []

    async def _spy_run_spotify_album_sync(*, album_id, config, db, queue, spotify_client):
        album_sync_calls.append(str(album_id))
        return {"status": "accepted", "enqueued_count": 1}

    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.SpotifyOAuthStore", _FakeOAuthStore)
    monkeypatch.setattr("api.intent_dispatcher.run_spotify_album_sync", _spy_run_spotify_album_sync)

    result = asyncio.run(
        spotify_saved_albums_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
        )
    )

    assert result["status"] == "skipped"
    assert result["playlist_id"] == SPOTIFY_SAVED_ALBUMS_PLAYLIST_ID
    assert result["enqueued"] == 0
    assert album_sync_calls == []
    assert db.store_calls == []
