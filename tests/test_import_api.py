from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    import engine.core  # noqa: F401
    import threading
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.worker_engine = SimpleNamespace(store=object())
    module.app.state.paths = SimpleNamespace(db_path="/tmp/retreivr_test.db", single_downloads_dir="/tmp/downloads")
    module.app.state.playlist_import_jobs = {}
    module.app.state.playlist_import_jobs_lock = threading.Lock()
    monkeypatch.setattr(module, "_read_config_or_404", lambda: {})
    return TestClient(module.app)


def test_import_api_valid_upload(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    calls = []
    monkeypatch.setattr("api.main._run_playlist_import_job", lambda *args, **_kwargs: calls.append(args))

    response = client.post(
        "/api/import/playlist",
        data={"max_concurrent_downloads": "3"},
        files={"file": ("sample.m3u", b"#EXTM3U\n#EXTINF:123,Artist - Title\ntrack.mp3\n", "audio/x-mpegurl")},
    )

    payload = response.json()
    assert response.status_code == 202
    assert isinstance(payload.get("job_id"), str)
    assert payload["status"]["state"] == "queued"
    assert payload["status"]["job_id"] == payload["job_id"]
    assert payload["status"]["max_concurrent_downloads"] == 3
    assert calls[0][-1] == 3


def test_import_api_preflight_valid_upload(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/import/playlist/preflight",
        files={"file": ("sample.csv", b"artist,title,album\nArtist,Title,Album\nArtist,Title,Album\n", "text/csv")},
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["detected_format"] == "csv"
    assert payload["total_tracks"] == 2
    assert payload["unique_track_estimate"] == 1
    assert payload["duplicate_in_file_count"] == 1
    assert payload["metadata_richness"]["album"] == 2


def test_import_api_invalid_download_cap(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/import/playlist",
        data={"max_concurrent_downloads": "9"},
        files={"file": ("sample.csv", b"artist,title\nArtist,Title\n", "text/csv")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "invalid_max_concurrent_downloads"


def test_import_api_rejects_concurrent_import(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    import api.main as module
    module.app.state.playlist_import_active_count = 1

    response = client.post(
        "/api/import/playlist",
        files={"file": ("sample.csv", b"artist,title\nArtist,Title\n", "text/csv")},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "import_already_running"


def test_import_api_invalid_format(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/import/playlist",
        files={"file": ("sample.txt", b"hello", "text/plain")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "unsupported_file_extension"


def test_import_api_empty_file(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/import/playlist",
        files={"file": ("sample.csv", b"", "text/csv")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "empty_file"


def test_import_api_oversize_file(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    oversized = b"a" * (10 * 1024 * 1024 + 1)
    response = client.post(
        "/api/import/playlist",
        files={"file": ("sample.csv", oversized, "text/csv")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "file_too_large"
