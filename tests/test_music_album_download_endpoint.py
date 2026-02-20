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
    # Optional runtime dependencies used by api.main imports.
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


def test_album_download_returns_partial_success_instead_of_500(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    class _MB:
        def _call_with_retry(self, func):
            return func()

    monkeypatch.setattr("api.main._mb_service", lambda: _MB())
    monkeypatch.setattr("api.main._read_config_or_404", lambda: {"music_mb_binding_threshold": 0.78})

    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_group_by_id",
        lambda _rg, includes=None: {
            "release-group": {
                "release-list": [
                    {"id": "rel-1", "status": "Official", "country": "US"},
                ]
            }
        },
    )
    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_by_id",
        lambda _rid, includes=None: {
            "release": {
                "id": "rel-1",
                "title": "Album Name",
                "date": "2010-01-01",
                "artist-credit": [{"name": "Artist Name"}],
                "medium-list": [
                    {
                        "position": "1",
                        "track-list": [
                            {
                                "position": "1",
                                "title": "Track A",
                                "recording": {"id": "rec-1", "title": "Track A", "length": "210000"},
                            },
                            {
                                "position": "2",
                                "title": "Track B",
                                "recording": {"id": "rec-2", "title": "Track B", "length": "211000"},
                            },
                        ],
                    }
                ],
            }
        },
    )

    def _fake_resolve(_mb, *, artist=None, track=None, album=None, duration_ms=None, threshold=None, max_duration_delta_ms=None, **kwargs):
        _ = artist, album, duration_ms, threshold, max_duration_delta_ms, kwargs
        if track == "Track B":
            raise RuntimeError("forced_track_failure")
        return {
            "recording_mbid": "rec-1",
            "mb_release_id": "rel-1",
            "mb_release_group_id": "rg-1",
            "artist": "Artist Name",
            "album": "Album Name",
            "track_number": 1,
            "disc_number": 1,
            "release_date": "2010",
            "duration_ms": 210000,
        }

    monkeypatch.setattr("api.main.resolve_best_mb_pair", _fake_resolve)

    enqueued: list[dict] = []

    def _capture_enqueue(self, payload: dict) -> None:
        enqueued.append(dict(payload))

    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", _capture_enqueue)

    response = client.post(
        "/api/music/album/download",
        json={"release_group_mbid": "rg-1"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["tracks_enqueued"] == 1
    assert payload["failed_tracks_count"] == 1
    assert len(payload["failed_tracks"]) == 1
    assert payload["failed_tracks"][0]["track"] == "Track B"
    assert "forced_track_failure" in payload["failed_tracks"][0]["reason"]
    assert len(enqueued) == 1

