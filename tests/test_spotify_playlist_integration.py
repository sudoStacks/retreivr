from __future__ import annotations

import asyncio
from typing import Any

from metadata.types import MusicMetadata
from scheduler.jobs.spotify_playlist_watch import enqueue_spotify_track


class _MockSpotifyClient:
    def __init__(self, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.snapshot_id = snapshot_id
        self.items = items

    def get_playlist_items(self, _playlist_id: str) -> tuple[str, list[dict[str, Any]]]:
        return self.snapshot_id, list(self.items)


class _MockSearchService:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self._results = results
        self.queries: list[str] = []

    async def search(self, query: str) -> list[dict[str, Any]]:
        self.queries.append(query)
        return list(self._results)


class _MockQueue:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def enqueue(self, payload: dict[str, Any]) -> None:
        self.items.append(payload)


def test_enqueue_spotify_track_integration_single_result() -> None:
    spotify_client = _MockSpotifyClient(
        "snap-1",
        [
            {
                "spotify_track_id": "sp-track-1",
                "artist": "Artist One",
                "title": "Track One",
                "duration_ms": 200000,
            }
        ],
    )
    _snapshot_id, tracks = spotify_client.get_playlist_items("playlist-1")
    spotify_track = tracks[0]

    search_service = _MockSearchService(
        [
            {
                "media_url": "https://example.com/media-1",
                "title": "Track One",
                "duration": 200,
                "source_id": "youtube_music",
                "extra": {"lyrics": "la la"},
            }
        ]
    )
    queue = _MockQueue()

    asyncio.run(
        enqueue_spotify_track(
            queue=queue,
            spotify_track=spotify_track,
            search_service=search_service,
            playlist_id="playlist-1",
        )
    )

    assert len(queue.items) == 1
    payload = queue.items[0]
    assert payload["playlist_id"] == "playlist-1"
    assert payload["spotify_track_id"] == "sp-track-1"
    assert payload["resolved_media"]["media_url"] == "https://example.com/media-1"
    assert isinstance(payload["music_metadata"], MusicMetadata)
    assert payload["music_metadata"].title == "Track One"
    assert payload["music_metadata"].artist == "Artist One"
    assert search_service.queries == ["Artist One - Track One official audio"]


def test_enqueue_spotify_track_integration_best_result_selected() -> None:
    spotify_client = _MockSpotifyClient(
        "snap-2",
        [
            {
                "spotify_track_id": "sp-track-2",
                "artist": "Artist Two",
                "title": "Track Two",
                "duration_ms": 210000,
            }
        ],
    )
    _snapshot_id, tracks = spotify_client.get_playlist_items("playlist-2")
    spotify_track = tracks[0]

    search_service = _MockSearchService(
        [
            {
                "media_url": "https://example.com/worse",
                "title": "Track Two (live)",
                "duration": 260,
                "source_id": "youtube_music",
                "extra": {},
            },
            {
                "media_url": "https://example.com/best",
                "title": "Track Two",
                "duration": 210,
                "source_id": "youtube",
                "extra": {"genre": "Rock"},
            },
        ]
    )
    queue = _MockQueue()

    asyncio.run(
        enqueue_spotify_track(
            queue=queue,
            spotify_track=spotify_track,
            search_service=search_service,
            playlist_id="playlist-2",
        )
    )

    assert len(queue.items) == 1
    payload = queue.items[0]
    assert payload["resolved_media"]["media_url"] == "https://example.com/best"
    assert payload["resolved_media"]["source_id"] == "youtube"
    assert isinstance(payload["music_metadata"], MusicMetadata)
    assert payload["music_metadata"].title == "Track Two"
    assert payload["music_metadata"].artist == "Artist Two"

