from __future__ import annotations

from types import SimpleNamespace

from download.worker import (
    DownloadWorker,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_VALIDATION_FAILED,
)


class _MockDownloader:
    def download(self, media_url: str) -> str:
        return "/tmp/mock-audio.mp3"


def _job() -> SimpleNamespace:
    return SimpleNamespace(
        payload={
            "playlist_id": "playlist-1",
            "spotify_track_id": "track-1",
            "resolved_media": {"media_url": "https://example.test/audio"},
            "music_metadata": {
                "title": "Track",
                "artist": "Artist",
                "isrc": "USABC1234567",
                "expected_ms": 1_000,
            },
        }
    )


def test_duration_tolerance_config_changes_validation_outcome(monkeypatch) -> None:
    recorded: list[tuple[str, str, str]] = []

    monkeypatch.setattr("download.worker.ENABLE_DURATION_VALIDATION", True)
    monkeypatch.setattr(
        "download.worker.record_downloaded_track",
        lambda playlist_id, isrc, file_path: recorded.append((playlist_id, isrc, file_path)),
    )
    monkeypatch.setattr("download.worker.tag_file", lambda _path, _metadata: None)

    # Deterministic validator model: actual=1.20s, expected=1.00s (delta=0.20s).
    monkeypatch.setattr(
        "download.worker.validate_duration",
        lambda _file_path, expected_ms, tolerance_seconds: abs(1.2 - (expected_ms / 1000.0))
        <= tolerance_seconds,
    )

    worker = DownloadWorker(_MockDownloader())

    # Baseline tolerance: passes.
    monkeypatch.setattr("download.worker.SPOTIFY_DURATION_TOLERANCE_SECONDS", 0.30)
    first_job = _job()
    worker.process_job(first_job)
    assert first_job.status == JOB_STATUS_COMPLETED
    assert len(recorded) == 1

    # Very small tolerance: same track now fails validation.
    monkeypatch.setattr("download.worker.SPOTIFY_DURATION_TOLERANCE_SECONDS", 0.05)
    second_job = _job()
    worker.process_job(second_job)
    assert second_job.status == JOB_STATUS_VALIDATION_FAILED
    assert len(recorded) == 1

    # Increased tolerance again: track passes.
    monkeypatch.setattr("download.worker.SPOTIFY_DURATION_TOLERANCE_SECONDS", 0.30)
    third_job = _job()
    worker.process_job(third_job)
    assert third_job.status == JOB_STATUS_COMPLETED
    assert len(recorded) == 2
