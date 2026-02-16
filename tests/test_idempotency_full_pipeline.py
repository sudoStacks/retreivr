from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from db.downloaded_tracks import has_downloaded_isrc
from download.worker import DownloadWorker
from scheduler.jobs.spotify_playlist_watch import enqueue_spotify_track


class _MockQueue:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def enqueue(self, payload: dict[str, Any]) -> None:
        self.items.append(payload)


class _MockSearchService:
    def __init__(self, results_by_query: dict[str, list[dict[str, Any]]]) -> None:
        self._results_by_query = results_by_query
        self.calls: list[str] = []

    async def search(self, query: str) -> list[dict[str, Any]]:
        self.calls.append(query)
        return list(self._results_by_query.get(query, []))


class _MockDownloader:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def download(self, media_url: str) -> str:
        self.calls.append(media_url)
        tail = media_url.rsplit("/", 1)[-1] or "track"
        path = Path(f"/tmp/{tail}.mp3")
        path.write_bytes(b"mock-audio")
        return str(path)


def test_idempotency_full_pipeline_two_tracks(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "idempotency.sqlite"
    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))
    monkeypatch.setattr("download.worker.tag_file", lambda _file_path, _metadata: None)

    playlist_id = "playlist-42"
    tracks = [
        {
            "spotify_track_id": "sp-track-1",
            "artist": "Artist One",
            "title": "Track One",
            "isrc": "USAAA1111111",
            "duration_ms": 200000,
        },
        {
            "spotify_track_id": "sp-track-2",
            "artist": "Artist Two",
            "title": "Track Two",
            "isrc": "USBBB2222222",
            "duration_ms": 210000,
        },
    ]
    search_service = _MockSearchService(
        {
            "Artist One - Track One official audio": [
                {
                    "media_url": "https://example.test/one",
                    "title": "Track One",
                    "duration": 200,
                    "source_id": "youtube_music",
                    "extra": {},
                }
            ],
            "Artist Two - Track Two official audio": [
                {
                    "media_url": "https://example.test/two",
                    "title": "Track Two",
                    "duration": 210,
                    "source_id": "youtube_music",
                    "extra": {},
                }
            ],
        }
    )
    queue = _MockQueue()

    # First pass: enqueue and process both tracks, recording downloaded ISRCs.
    for track in tracks:
        asyncio.run(enqueue_spotify_track(queue, track, search_service, playlist_id))
    assert len(queue.items) == 2

    downloader = _MockDownloader()
    worker = DownloadWorker(downloader)
    for payload in list(queue.items):
        worker.process_job(SimpleNamespace(payload=payload))

    assert has_downloaded_isrc(playlist_id, "USAAA1111111") is True
    assert has_downloaded_isrc(playlist_id, "USBBB2222222") is True
    first_pass_queries = list(search_service.calls)
    assert len(first_pass_queries) == 2

    # Second pass: same playlist + ISRC should be skipped before resolve/enqueue.
    for track in tracks:
        asyncio.run(enqueue_spotify_track(queue, track, search_service, playlist_id))

    assert len(queue.items) == 2
    assert search_service.calls == first_pass_queries
