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

    monkeypatch.setattr(
        "api.main.import_playlist_file_bytes",
        lambda file_bytes, filename: [SimpleNamespace(artist="A", title="B", album=None, raw_line="A - B", source_format="m3u")],
    )
    monkeypatch.setattr(
        "api.main.process_imported_tracks",
        lambda track_intents, config: SimpleNamespace(
            total_tracks=1,
            resolved_count=1,
            unresolved_count=0,
            enqueued_count=1,
            failed_count=0,
        ),
    )

    response = client.post(
        "/api/import/playlist",
        files={"file": ("sample.m3u", b"#EXTM3U\n#EXTINF:123,Artist - Title\ntrack.mp3\n", "audio/x-mpegurl")},
    )

    assert response.status_code == 200
    assert response.json() == {
        "total_tracks": 1,
        "resolved": 1,
        "unresolved": 0,
        "enqueued": 1,
        "failed": 0,
        "import_batch_id": "",
    }


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
