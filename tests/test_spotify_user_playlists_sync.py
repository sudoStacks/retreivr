from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from scheduler.jobs.spotify_playlist_watch import (
    SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID,
    spotify_user_playlists_watch_job,
)


class _FakeSpotifyClient:
    def __init__(self, snapshot_id: str, playlists: list[dict[str, Any]]) -> None:
        self.snapshot_id = snapshot_id
        self.playlists = playlists

    async def get_user_playlists(self) -> tuple[str, list[dict[str, Any]]]:
        return self.snapshot_id, list(self.playlists)


class _FakeSnapshotStore:
    def __init__(self, latest_snapshot: dict[str, Any] | None) -> None:
        self.latest_snapshot = latest_snapshot
        self.store_calls: list[tuple[str, str, list[dict[str, Any]]]] = []

    def get_latest_snapshot(self, playlist_id: str) -> dict[str, Any] | None:
        return self.latest_snapshot

    def store_snapshot(self, playlist_id: str, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.store_calls.append((playlist_id, snapshot_id, list(items)))


def _playlist(playlist_id: str, name: str, track_count: int) -> dict[str, Any]:
    return {
        "id": playlist_id,
        "name": name,
        "track_count": track_count,
    }


def test_user_playlists_sync_triggers_existing_watch_job_for_new_playlists(monkeypatch) -> None:
    config = {"spotify": {"client_id": "cid", "client_secret": "csec"}}
    previous_items = [
        {"spotify_track_id": "pl-a", "position": 0, "added_at": None},
    ]
    db = _FakeSnapshotStore({"snapshot_id": "snap-prev", "items": previous_items})
    queue = object()
    search_service = object()
    spotify_client = _FakeSpotifyClient(
        "snap-next",
        [_playlist("pl-a", "Existing", 10), _playlist("pl-b", "New One", 20), _playlist("pl-c", "New Two", 30)],
    )

    class _FakeOAuthStore:
        def __init__(self, _db_path):
            pass

        def get_valid_token(self, _client_id, _client_secret, config=None):
            return SimpleNamespace(access_token="oauth-token")

    watch_calls: list[dict[str, Any]] = []

    def _spy_playlist_watch_job(*, spotify_client, db, queue, playlist_id, playlist_name=None, config=None):
        watch_calls.append(
            {
                "playlist_id": playlist_id,
                "playlist_name": playlist_name,
                "config": config,
            }
        )
        return {
            "status": "updated",
            "playlist_id": playlist_id,
            "enqueued": 0,
        }

    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.SpotifyOAuthStore", _FakeOAuthStore)
    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.playlist_watch_job", _spy_playlist_watch_job)

    result = asyncio.run(
        spotify_user_playlists_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
        )
    )

    assert result["status"] == "updated"
    assert result["playlist_id"] == SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID
    assert [call["playlist_id"] for call in watch_calls] == ["pl-b", "pl-c"]
    assert [call["playlist_name"] for call in watch_calls] == ["New One", "New Two"]

    assert len(db.store_calls) == 1
    assert db.store_calls[0][0] == SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID
    assert db.store_calls[0][1] == "snap-next"


def test_user_playlists_sync_skips_cleanly_when_oauth_token_missing(monkeypatch) -> None:
    config = {"spotify": {"client_id": "cid", "client_secret": "csec"}}
    db = _FakeSnapshotStore(latest_snapshot=None)
    queue = object()
    search_service = object()
    spotify_client = _FakeSpotifyClient("snap-next", [_playlist("pl-a", "Any", 1)])

    class _FakeOAuthStore:
        def __init__(self, _db_path):
            pass

        def get_valid_token(self, _client_id, _client_secret, config=None):
            return None

    watch_calls: list[str] = []

    def _spy_playlist_watch_job(*, spotify_client, db, queue, playlist_id, playlist_name=None, config=None):
        watch_calls.append(playlist_id)
        return {"status": "updated", "playlist_id": playlist_id, "enqueued": 0}

    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.SpotifyOAuthStore", _FakeOAuthStore)
    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.playlist_watch_job", _spy_playlist_watch_job)

    result = asyncio.run(
        spotify_user_playlists_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
        )
    )

    assert result["status"] == "skipped"
    assert result["playlist_id"] == SPOTIFY_USER_PLAYLISTS_PLAYLIST_ID
    assert result["enqueued"] == 0
    assert watch_calls == []
    assert db.store_calls == []
