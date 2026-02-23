from __future__ import annotations

import importlib
import sys
import types

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    if "google_auth_oauthlib" not in sys.modules:
        google_auth_oauthlib = types.ModuleType("google_auth_oauthlib")
        sys.modules["google_auth_oauthlib"] = google_auth_oauthlib
    if "google_auth_oauthlib.flow" not in sys.modules:
        flow_mod = types.ModuleType("google_auth_oauthlib.flow")
        flow_mod.InstalledAppFlow = object
        sys.modules["google_auth_oauthlib.flow"] = flow_mod
    if "googleapiclient" not in sys.modules:
        googleapiclient = types.ModuleType("googleapiclient")
        sys.modules["googleapiclient"] = googleapiclient
    if "googleapiclient.errors" not in sys.modules:
        errors_mod = types.ModuleType("googleapiclient.errors")
        errors_mod.HttpError = Exception
        sys.modules["googleapiclient.errors"] = errors_mod
    if "google" not in sys.modules:
        google_mod = types.ModuleType("google")
        sys.modules["google"] = google_mod
    if "google.auth" not in sys.modules:
        google_auth_mod = types.ModuleType("google.auth")
        sys.modules["google.auth"] = google_auth_mod
    if "google.auth.exceptions" not in sys.modules:
        google_auth_exc_mod = types.ModuleType("google.auth.exceptions")
        google_auth_exc_mod.RefreshError = Exception
        sys.modules["google.auth.exceptions"] = google_auth_exc_mod
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")

    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return TestClient(module.app)


def test_music_enqueue_rejects_missing_recording_mbid(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    response = client.post("/api/music/enqueue", json={"artist": "Artist", "track": "Song"})
    assert response.status_code == 400
    assert "recording_mbid" in str(response.json().get("detail"))


def test_music_enqueue_allows_optional_missing_fields(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    monkeypatch.setattr("api.main._read_config_or_404", lambda: {})

    captured: dict = {}

    class _FakeStore:
        def __init__(self, _db_path):
            pass

        def enqueue_job(self, **kwargs):
            captured["enqueue_payload"] = dict(kwargs)
            return "job-1", True, None

    def _fake_builder(**kwargs):
        captured["builder_kwargs"] = dict(kwargs)
        return {"id": "job-1", "url": "musicbrainz://recording/rec-1"}

    monkeypatch.setattr("api.main.DownloadJobStore", _FakeStore)
    monkeypatch.setattr("api.main.build_download_job_payload", _fake_builder)

    response = client.post(
        "/api/music/enqueue",
        json={
            "recording_mbid": "rec-1",
            "artist": "Artist",
            "track": "Song",
        },
    )

    assert response.status_code == 200
    canonical = captured["builder_kwargs"]["resolved_metadata"]
    assert canonical["recording_mbid"] == "rec-1"
    assert canonical["artist"] == "Artist"
    assert canonical["track"] == "Song"
    assert canonical["album"] == ""
    assert canonical["track_number"] is None
    assert canonical["disc_number"] is None
    assert canonical["duration_ms"] is None


def test_music_enqueue_returns_structured_binding_failure(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    monkeypatch.setattr("api.main._read_config_or_404", lambda: {})

    class _FakeStore:
        def __init__(self, _db_path):
            pass

        def enqueue_job(self, **kwargs):
            _ = kwargs
            return "job-1", True, None

    def _failing_builder(**kwargs):
        _ = kwargs
        raise ValueError("music_track_requires_mb_bound_metadata", ["no_valid_release_for_recording"])

    monkeypatch.setattr("api.main.DownloadJobStore", _FakeStore)
    monkeypatch.setattr("api.main.build_download_job_payload", _failing_builder)

    response = client.post(
        "/api/music/enqueue",
        json={
            "recording_mbid": "rec-1",
            "artist": "Artist",
            "track": "Song",
        },
    )
    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "music_mode_mb_binding_failed"
    assert "no_valid_release_for_recording" in body["reason"]
