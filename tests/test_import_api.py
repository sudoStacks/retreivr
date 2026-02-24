from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.worker_engine = SimpleNamespace(store=object())
    module._read_config_or_404 = lambda: {}
    return TestClient(module.app)


def test_import_api_valid_upload(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    monkeypatch.setattr("api.main._run_playlist_import_job", lambda *_args, **_kwargs: None)

    response = client.post(
        "/api/import/playlist",
        files={"file": ("sample.m3u", b"#EXTM3U\n#EXTINF:123,Artist - Title\ntrack.mp3\n", "audio/x-mpegurl")},
    )

    payload = response.json()
    assert response.status_code == 202
    assert isinstance(payload.get("job_id"), str)
    assert payload["status"]["state"] == "queued"
    assert payload["status"]["job_id"] == payload["job_id"]


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

    oversized = b"a" * (5 * 1024 * 1024 + 1)
    response = client.post(
        "/api/import/playlist",
        files={"file": ("sample.csv", oversized, "text/csv")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "file_too_large"
