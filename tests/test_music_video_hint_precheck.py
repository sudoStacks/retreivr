from __future__ import annotations

import importlib
import sys
import types

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> tuple[TestClient, object]:
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
    return TestClient(module.app), module


def test_quick_youtube_mv_precheck_matches_common_official_title(monkeypatch) -> None:
    _client, module = _build_client(monkeypatch)

    class _FakeAdapter:
        def search_music_track(self, query, limit=5):
            _ = (query, limit)
            return [
                {
                    "title": "Brad Paisley - Alcohol (Official Video) ft. Guest",
                    "url": "https://youtube.com/watch?v=abc",
                    "uploader": "BradPaisleyVEVO",
                }
            ]

    monkeypatch.setattr(module, "YouTubeAdapter", _FakeAdapter)
    monkeypatch.setattr(module, "_bounded_call", lambda _timeout, fn: fn())

    result = module._quick_youtube_mv_precheck("Brad Paisley", "Alcohol")
    assert result["matched"] is True
    assert result["reason"] == "token_match"
    assert "Official Video" in str(result.get("title") or "")


def test_music_video_availability_uses_youtube_precheck_signal(monkeypatch) -> None:
    client, module = _build_client(monkeypatch)

    monkeypatch.setattr(module, "_quick_youtube_mv_precheck", lambda artist, track, album=None: {  # noqa: ARG005
        "matched": True,
        "reason": "token_match",
        "title": "Artist - Track (Official Music Video)",
        "url": "https://youtube.com/watch?v=abc",
    })

    response = client.post(
        "/api/music/video/availability",
        json={
            "recording_mbid": "",
            "artist": "Artist",
            "track": "Track",
            "album": "",
            "include_youtube_probe": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["likelihood"] in {"low", "medium", "high"}
    assert payload["signals"]["youtube_precheck"]["matched"] is True

