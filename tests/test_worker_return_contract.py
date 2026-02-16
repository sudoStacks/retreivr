from __future__ import annotations

from types import SimpleNamespace

from download.worker import JOB_STATUS_VALIDATION_FAILED, DownloadWorker, safe_int


class _MockDownloader:
    def download(self, media_url: str) -> str:
        return "/tmp/mock-track.mp3"


def test_process_job_returns_validation_failed_without_file_path(monkeypatch) -> None:
    recorded: list[tuple[str, str, str]] = []

    monkeypatch.setattr("download.worker.validate_duration", lambda *_args, **_kwargs: False)
    monkeypatch.setattr("download.worker.get_media_duration", lambda _path: 1.0)
    monkeypatch.setattr("download.worker.tag_file", lambda _path, _metadata: None)
    monkeypatch.setattr(
        "download.worker.record_downloaded_track",
        lambda playlist_id, isrc, file_path: recorded.append((playlist_id, isrc, file_path)),
    )

    worker = DownloadWorker(_MockDownloader())
    job = SimpleNamespace(
        payload={
            "playlist_id": "playlist-1",
            "spotify_track_id": "track-1",
            "resolved_media": {"media_url": "https://example.test/audio"},
            "music_metadata": {
                "title": "Track One",
                "artist": "Artist One",
                "isrc": "USABC1234567",
                "expected_ms": 180_000,
            },
        }
    )

    result = worker.process_job(job)

    assert result == {"status": JOB_STATUS_VALIDATION_FAILED, "file_path": None}
    assert job.status == JOB_STATUS_VALIDATION_FAILED
    assert recorded == []


def test_safe_int_parses_or_returns_none_for_malformed_values() -> None:
    assert safe_int("01/12") == 1
    assert safe_int("") is None
    assert safe_int(None) is None
    assert safe_int("Disc 1") == 1
    assert safe_int("no number") is None
