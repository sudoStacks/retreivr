from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from db.downloaded_tracks import has_downloaded_isrc
from download.worker import DownloadWorker, JOB_STATUS_VALIDATION_FAILED
from scheduler.jobs.spotify_playlist_watch import enqueue_spotify_track


class _MockQueue:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def enqueue(self, payload: dict[str, Any]) -> None:
        self.items.append(payload)


class _MockSearchService:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self._results = results
        self.calls: list[str] = []

    async def search(self, query: str) -> list[dict[str, Any]]:
        self.calls.append(query)
        return list(self._results)


class _MockDownloader:
    def download(self, media_url: str) -> str:
        return "/tmp/resolved-track.mp3"


def test_full_pipeline_validation_failure_does_not_enable_idempotent_skip(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "validation_pipeline.sqlite"
    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))

    # Force worker validation to fail before tagging/recording.
    monkeypatch.setattr("download.worker.validate_duration", lambda *_args, **_kwargs: False)
    monkeypatch.setattr("download.worker.get_media_duration", lambda _path: 1.0)
    monkeypatch.setattr("download.worker.tag_file", lambda _path, _metadata: None)

    # Ensure queued payload metadata includes expected_ms for validation gating.
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch.merge_metadata",
        lambda spotify_data, _mb, _ytdlp: {
            "title": spotify_data.get("title", "Unknown"),
            "artist": spotify_data.get("artist", "Unknown"),
            "album": "Unknown",
            "album_artist": spotify_data.get("artist", "Unknown"),
            "track_num": 1,
            "disc_num": 1,
            "date": "Unknown",
            "genre": "Unknown",
            "isrc": spotify_data.get("isrc"),
            "expected_ms": spotify_data.get("duration_ms"),
        },
    )

    playlist_id = "playlist-validation"
    spotify_track = {
        "spotify_track_id": "sp-track-1",
        "artist": "Artist One",
        "title": "Track One",
        "isrc": "USVAL1234567",
        "duration_ms": 200_000,
    }
    search_service = _MockSearchService(
        [
            {
                "media_url": "https://example.test/track-one",
                "title": "Track One",
                "duration": 200,
                "source_id": "youtube_music",
                "extra": {},
            }
        ]
    )
    queue = _MockQueue()

    # First pass: enqueue + worker processing with forced validation failure.
    asyncio.run(enqueue_spotify_track(queue, spotify_track, search_service, playlist_id))
    assert len(queue.items) == 1

    worker = DownloadWorker(_MockDownloader())
    job = SimpleNamespace(payload=queue.items[0])
    worker.process_job(job)
    assert job.status == JOB_STATUS_VALIDATION_FAILED
    assert has_downloaded_isrc(playlist_id, spotify_track["isrc"]) is False
    first_pass_calls = list(search_service.calls)

    # Second pass: should not be skipped because ISRC was never recorded.
    asyncio.run(enqueue_spotify_track(queue, spotify_track, search_service, playlist_id))
    assert len(queue.items) == 2
    assert len(search_service.calls) == len(first_pass_calls) + 1
