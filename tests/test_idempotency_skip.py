from __future__ import annotations

import asyncio
import sys
import types
from typing import Any

google = types.ModuleType("google")
google_auth = types.ModuleType("google.auth")
google_auth_exceptions = types.ModuleType("google.auth.exceptions")


class _RefreshError(Exception):
    pass


google_auth_exceptions.RefreshError = _RefreshError
google_auth.exceptions = google_auth_exceptions
google.auth = google_auth
sys.modules.setdefault("google", google)
sys.modules.setdefault("google.auth", google_auth)
sys.modules.setdefault("google.auth.exceptions", google_auth_exceptions)

from scheduler.jobs.spotify_playlist_watch import enqueue_spotify_track


class _MockQueue:
    def __init__(self) -> None:
        self.enqueued: list[dict[str, Any]] = []

    def enqueue(self, payload: dict[str, Any]) -> None:
        self.enqueued.append(payload)


class _MockSearchService:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self._results = results
        self.calls: list[str] = []

    async def search(self, query: str) -> list[dict[str, Any]]:
        self.calls.append(query)
        return list(self._results)


def test_enqueue_spotify_track_skips_when_isrc_already_downloaded(monkeypatch) -> None:
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.has_downloaded_isrc",
        lambda playlist_id, isrc: True,
    )
    queue = _MockQueue()
    search_service = _MockSearchService(
        [
            {
                "media_url": "https://example.com/track",
                "title": "Track One",
                "duration": 210,
                "source_id": "youtube_music",
                "extra": {"lyrics": "la la"},
            }
        ]
    )
    spotify_track = {
        "spotify_track_id": "sp-track-1",
        "artist": "Artist One",
        "title": "Track One",
        "isrc": "USABC1234567",
        "duration_ms": 210000,
    }

    asyncio.run(
        enqueue_spotify_track(
            queue=queue,
            spotify_track=spotify_track,
            search_service=search_service,
            playlist_id="playlist-a",
        )
    )

    assert queue.enqueued == []
    assert search_service.calls == []


def test_enqueue_spotify_track_enqueues_when_isrc_not_downloaded(monkeypatch) -> None:
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.has_downloaded_isrc",
        lambda playlist_id, isrc: False,
    )
    queue = _MockQueue()
    search_service = _MockSearchService(
        [
            {
                "media_url": "https://example.com/track",
                "title": "Track Two",
                "duration": 205,
                "source_id": "youtube_music",
                "extra": {"genre": "Pop"},
            }
        ]
    )
    spotify_track = {
        "spotify_track_id": "sp-track-2",
        "artist": "Artist Two",
        "title": "Track Two",
        "isrc": "USZZZ9999999",
        "duration_ms": 205000,
    }

    asyncio.run(
        enqueue_spotify_track(
            queue=queue,
            spotify_track=spotify_track,
            search_service=search_service,
            playlist_id="playlist-b",
        )
    )

    assert len(queue.enqueued) == 1
    payload = queue.enqueued[0]
    assert payload["playlist_id"] == "playlist-b"
    assert payload["spotify_track_id"] == "sp-track-2"
    assert payload["resolved_media"]["media_url"] == "https://example.com/track"
    assert search_service.calls == ["Artist Two - Track Two official audio"]


def test_enqueue_spotify_track_skips_when_isrc_already_downloaded_globally(monkeypatch) -> None:
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.has_downloaded_isrc",
        lambda playlist_id, isrc: False,
    )
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.has_downloaded_isrc_anywhere",
        lambda isrc: True,
    )
    queue = _MockQueue()
    search_service = _MockSearchService(
        [
            {
                "media_url": "https://example.com/track",
                "title": "Track Three",
                "duration": 204,
                "source_id": "youtube_music",
                "extra": {},
            }
        ]
    )
    spotify_track = {
        "spotify_track_id": "sp-track-3",
        "artist": "Artist Three",
        "title": "Track Three",
        "isrc": "USYYY8888888",
        "duration_ms": 204000,
    }

    result = asyncio.run(
        enqueue_spotify_track(
            queue=queue,
            spotify_track=spotify_track,
            search_service=search_service,
            playlist_id="playlist-c",
        )
    )

    assert result["created"] is False
    assert result["reason"] == "duplicate_isrc"
    assert queue.enqueued == []
    assert search_service.calls == []
