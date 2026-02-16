from __future__ import annotations

import asyncio
from typing import Any

from api.intent_dispatcher import execute_intent


def test_execute_intent_spotify_artist_requires_selection() -> None:
    result = asyncio.run(
        execute_intent(
            intent_type="spotify_artist",
            identifier="artist-123",
            config={},
            db=object(),
            queue=object(),
            spotify_client=object(),
        )
    )

    assert result["status"] == "accepted"
    assert result["intent_type"] == "spotify_artist"
    assert result["identifier"] == "artist-123"
    assert "selection" in result["message"].lower()
    assert result["enqueued_count"] == 0


def test_execute_intent_spotify_playlist_triggers_playlist_sync(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def _fake_playlist_watch_job(spotify_client, db, queue, playlist_id, *, playlist_name=None, config=None):
        calls.append(
            {
                "spotify_client": spotify_client,
                "db": db,
                "queue": queue,
                "playlist_id": playlist_id,
                "playlist_name": playlist_name,
                "config": config,
            }
        )
        return {"status": "updated", "enqueued": 2}

    monkeypatch.setattr("api.intent_dispatcher.playlist_watch_job", _fake_playlist_watch_job)

    db = object()
    queue = object()
    spotify_client = object()
    result = asyncio.run(
        execute_intent(
            intent_type="spotify_playlist",
            identifier="playlist-abc",
            config={"spotify_playlists": []},
            db=db,
            queue=queue,
            spotify_client=spotify_client,
        )
    )

    assert len(calls) == 1
    assert calls[0]["playlist_id"] == "playlist-abc"
    assert calls[0]["db"] is db
    assert calls[0]["queue"] is queue
    assert calls[0]["spotify_client"] is spotify_client
    assert result["status"] == "accepted"
    assert result["enqueued_count"] == 2


def test_execute_intent_spotify_album_triggers_album_sync(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    async def _fake_album_sync(album_id, config, db, queue, spotify_client):
        calls.append(
            {
                "album_id": album_id,
                "config": config,
                "db": db,
                "queue": queue,
                "spotify_client": spotify_client,
            }
        )
        return {
            "status": "accepted",
            "intent_type": "spotify_album",
            "identifier": album_id,
            "message": "album sync completed",
            "enqueued_count": 4,
        }

    monkeypatch.setattr("api.intent_dispatcher.run_spotify_album_sync", _fake_album_sync)

    db = object()
    queue = object()
    spotify_client = object()
    result = asyncio.run(
        execute_intent(
            intent_type="spotify_album",
            identifier="album-xyz",
            config={"search_service": object()},
            db=db,
            queue=queue,
            spotify_client=spotify_client,
        )
    )

    assert len(calls) == 1
    assert calls[0]["album_id"] == "album-xyz"
    assert calls[0]["db"] is db
    assert calls[0]["queue"] is queue
    assert calls[0]["spotify_client"] is spotify_client
    assert result["status"] == "accepted"
    assert result["enqueued_count"] == 4


def test_execute_intent_spotify_track_enqueues_once(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        "api.intent_dispatcher._fetch_spotify_track",
        lambda _spotify_client, track_id: {
            "spotify_track_id": track_id,
            "artist": "Artist",
            "title": "Title",
            "album": "Album",
            "duration_ms": 123000,
            "isrc": "USABC1234567",
        },
    )

    async def _fake_enqueue_spotify_track(*, queue, spotify_track, search_service, playlist_id):
        calls.append(
            {
                "queue": queue,
                "spotify_track": spotify_track,
                "search_service": search_service,
                "playlist_id": playlist_id,
            }
        )

    monkeypatch.setattr("api.intent_dispatcher.enqueue_spotify_track", _fake_enqueue_spotify_track)

    queue = object()
    search_service = object()
    result = asyncio.run(
        execute_intent(
            intent_type="spotify_track",
            identifier="track-777",
            config={"search_service": search_service},
            db=object(),
            queue=queue,
            spotify_client=object(),
        )
    )

    assert len(calls) == 1
    assert calls[0]["queue"] is queue
    assert calls[0]["spotify_track"]["spotify_track_id"] == "track-777"
    assert calls[0]["search_service"] is search_service
    assert calls[0]["playlist_id"] == "spotify_track_track-777"
    assert result["status"] == "accepted"
    assert result["enqueued_count"] == 1
