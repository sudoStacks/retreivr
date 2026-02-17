from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from download.worker import DownloadWorker, JOB_STATUS_COMPLETED

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@pytest.fixture()
def isolated_runtime(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "integration_music.sqlite"
    music_root = tmp_path / "library_root"
    music_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))

    return {
        "tmp_path": tmp_path,
        "db_path": db_path,
        "music_root": music_root,
    }


@pytest.fixture()
def api_module(monkeypatch):
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return module


@pytest.fixture()
def api_client(api_module) -> TestClient:
    return TestClient(api_module.app)


def test_integration_full_music_pipeline(
    isolated_runtime,
    api_module,
    api_client: TestClient,
    monkeypatch,
) -> None:
    db_path = str(isolated_runtime["db_path"])
    music_root = isolated_runtime["music_root"]
    conn = sqlite3.connect(db_path)
    try:
        api_module.ensure_download_jobs_table(conn)
    finally:
        conn.close()

    store = api_module.DownloadJobStore(db_path)
    api_module.app.state.worker_engine = SimpleNamespace(store=store)
    api_module.app.state.search_request_overrides = {}
    api_module.app.state.music_cover_art_cache = {}

    canonical_metadata = {
        "title": "My Song",
        "artist": "The Artist",
        "album": "The Album",
        "album_artist": "The Artist",
        "track_num": 1,
        "disc_num": 1,
        "date": "2024-01-15",
        "genre": "Rock",
        "mbid": "mbid-track-123",
        "isrc": "USABC1234567",
        "lyrics": None,
        "artwork": None,
    }

    class _FakeMusicBrainzService:
        def search_release_groups(self, query, limit=5):
            return [
                {
                    "release_group_id": "rg-123",
                    "title": "The Album",
                    "artist_credit": "The Artist",
                    "first_release_date": "2024-01-15",
                    "primary_type": "Album",
                    "secondary_types": [],
                    "score": 99,
                    "track_count": 10,
                }
            ]

        def search_recordings(self, artist, title, *, album=None, limit=1):
            return {
                "recording-list": [
                    {
                        "id": "rec-123",
                        "title": title,
                        "artist-credit": [{"artist": {"name": artist}}],
                        "release-list": [{"id": "rel-123", "title": album or "The Album", "date": "2024-01-15"}],
                    }
                ]
            }

        def get_release(self, release_id, *, includes=None):
            return {
                "release": {
                    "id": release_id,
                    "medium-list": [
                        {
                            "track-list": [
                                {"position": "1", "recording": {"id": "rec-123"}},
                            ]
                        }
                    ],
                }
            }

    fake_mb = _FakeMusicBrainzService()
    monkeypatch.setattr(api_module, "_mb_service", lambda: fake_mb)

    class _FakeSearchService:
        def __init__(self, module):
            self.adapters = {"youtube_music": object()}
            self._module = module

        def create_search_request(self, payload):
            _ = fake_mb.search_recordings(
                payload.get("artist"),
                payload.get("track") or payload.get("album") or "",
                album=payload.get("album"),
                limit=1,
            )
            _ = fake_mb.get_release("rel-123", includes=["recordings"])
            self._module._IntentQueueAdapter().enqueue(
                {
                    "media_intent": "music_track",
                    "artist": canonical_metadata["artist"],
                    "track": canonical_metadata["title"],
                    "album": canonical_metadata["album"],
                    "track_number": canonical_metadata["track_num"],
                    "disc_number": canonical_metadata["disc_num"],
                    "release_date": canonical_metadata["date"],
                    "duration_ms": 210000,
                    "playlist_id": "integration_playlist",
                }
            )
            return "req-integration-1"

    api_module.app.state.search_service = _FakeSearchService(api_module)

    response = api_client.post(
        "/api/search/requests",
        json={
            "query": "The Artist My Song",
            "intent": "track",
            "artist": "The Artist",
            "track": "My Song",
            "music_mode": True,
            "search_only": False,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["request_id"] == "req-integration-1"
    assert payload["music_mode"] is True
    assert isinstance(payload["music_candidates"], list)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT id, source, media_intent, output_template
            FROM download_jobs
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[1] == "youtube_music"
    assert row[2] == "music_track"
    output_template = json.loads(row[3]) if row[3] else {}
    assert "spotify" not in json.dumps(output_template).lower()

    claimed = store.claim_next_job("youtube_music")
    assert claimed is not None

    class _FakeDownloader:
        def __init__(self, root: Path):
            self._root = root

        def download(self, media_url: str) -> str:
            temp_file = self._root / "tmp-yt-source-abc123.mp3"
            temp_file.write_bytes(b"fake-audio")
            return str(temp_file)

    captured_tags = {}

    def _capture_tag_file(path: str, metadata_obj) -> None:
        captured_tags["path"] = path
        captured_tags["title"] = metadata_obj.title
        captured_tags["artist"] = metadata_obj.artist
        captured_tags["album"] = metadata_obj.album
        captured_tags["year"] = metadata_obj.date
        captured_tags["mbid"] = metadata_obj.mbid
        captured_tags["isrc"] = metadata_obj.isrc

    monkeypatch.setattr("download.worker.tag_file", _capture_tag_file)

    job_for_worker = SimpleNamespace(
        payload={
            "playlist_id": "integration_playlist",
            "music_root": str(music_root),
            "resolved_media": {"media_url": claimed.url},
            "music_metadata": canonical_metadata,
        }
    )
    worker = DownloadWorker(_FakeDownloader(isolated_runtime["tmp_path"]))
    worker_result = worker.process_job(job_for_worker)

    assert worker_result["status"] == JOB_STATUS_COMPLETED
    output_file = Path(worker_result["file_path"])
    assert output_file.exists() is True
    assert "spotify" not in output_file.name.lower()
    assert "youtube" not in output_file.name.lower()
    assert "abc123" not in output_file.name.lower()
    assert output_file.name == "01 - My Song.mp3"
    assert captured_tags["title"] == "My Song"
    assert captured_tags["artist"] == "The Artist"
    assert captured_tags["album"] == "The Album"
    assert captured_tags["year"] == "2024-01-15"
    assert captured_tags["mbid"] == "mbid-track-123"
    assert captured_tags["isrc"] == "USABC1234567"
    assert "spotify" not in json.dumps(captured_tags).lower()

    api_module.record_download_history(
        db_path,
        claimed,
        str(output_file),
        meta={
            "title": canonical_metadata["title"],
            "video_id": "vid-canonical-001",
        },
    )

    conn = sqlite3.connect(db_path)
    try:
        history_row = conn.execute(
            """
            SELECT title, filename, source, status
            FROM download_history
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        downloaded_row = conn.execute(
            """
            SELECT playlist_id, isrc, file_path
            FROM downloaded_music_tracks
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert history_row is not None
    assert history_row[0] == "My Song"
    assert history_row[1] == "01 - My Song.mp3"
    assert history_row[2] == "youtube_music"
    assert history_row[3] == "completed"

    assert downloaded_row is not None
    assert downloaded_row[0] == "integration_playlist"
    assert downloaded_row[1] == "USABC1234567"
    assert downloaded_row[2].endswith("01 - My Song.mp3")
