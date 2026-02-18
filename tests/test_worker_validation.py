from __future__ import annotations

import wave
from pathlib import Path
from types import SimpleNamespace

from download.worker import DownloadWorker, JOB_STATUS_VALIDATION_FAILED


class _MockDownloader:
    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path

    def download(self, media_url: str) -> str:
        with wave.open(str(self.output_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(44_100)
            wav_file.writeframes(b"\x00\x00" * 44_100)  # 1 second of silence
        return str(self.output_path)


def test_worker_sets_validation_failed_and_skips_record(monkeypatch, tmp_path: Path) -> None:
    recorded_calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        "download.worker.record_downloaded_track",
        lambda playlist_id, isrc, file_path: recorded_calls.append((playlist_id, isrc, file_path)),
    )
    monkeypatch.setattr(
        "download.worker.validate_duration",
        lambda file_path, expected_ms, tolerance_seconds: False,
    )
    monkeypatch.setattr("download.worker.get_media_duration", lambda file_path: 1.0)
    monkeypatch.setattr("download.worker.tag_file", lambda _path, _metadata: None)

    file_path = tmp_path / "short.wav"
    worker = DownloadWorker(_MockDownloader(file_path))
    job = SimpleNamespace(
        payload={
            "playlist_id": "playlist-1",
            "spotify_track_id": "track-1",
            "resolved_media": {"media_url": "https://example.test/audio"},
            "music_metadata": {
                "title": "Track",
                "artist": "Artist",
                "isrc": "USABC1234567",
                "expected_ms": 180_000,  # far from 1-second file
            },
        }
    )

    worker.process_job(job)

    assert recorded_calls == []
    assert job.status == JOB_STATUS_VALIDATION_FAILED
