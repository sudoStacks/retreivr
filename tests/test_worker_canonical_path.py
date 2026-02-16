from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from download.worker import DownloadWorker, JOB_STATUS_COMPLETED


class _MockDownloader:
    def __init__(self, temp_path: Path) -> None:
        self.temp_path = temp_path

    def download(self, media_url: str) -> str:
        self.temp_path.write_bytes(b"mock-audio")
        return str(self.temp_path)


def test_worker_moves_to_canonical_path_and_returns_it(tmp_path, monkeypatch) -> None:
    root = tmp_path / "Music"
    temp_file = tmp_path / "download-temp.mp3"

    monkeypatch.setattr("download.worker.tag_file", lambda _path, _metadata: None)

    worker = DownloadWorker(_MockDownloader(temp_file))
    job = SimpleNamespace(
        payload={
            "music_root": str(root),
            "resolved_media": {"media_url": "https://example.test/audio"},
            "music_metadata": {
                "album_artist": "Artist",
                "artist": "Artist",
                "album": "Album",
                "date": "2020",
                "disc_num": 2,
                "track_num": 3,
                "title": "Song",
                "genre": "Pop",
            },
        }
    )

    result = worker.process_job(job)

    expected = root / "Artist" / "Album (2020)" / "Disc 2" / "03 - Song.mp3"
    assert result == {"status": JOB_STATUS_COMPLETED, "file_path": str(expected)}
    assert expected.exists() is True
    assert temp_file.exists() is False
