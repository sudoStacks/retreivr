from __future__ import annotations

import importlib
import sys

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
    return TestClient(module.app)


def test_album_download_returns_error_when_no_tracks_from_musicbrainz(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    class _MB:
        def fetch_release_tracks(self, _rid):
            return None

    monkeypatch.setattr("api.main._mb_service", lambda: _MB())

    response = client.post(
        "/api/music/album/download",
        json={"release_group_id": "rg-1"},
    )

    assert response.status_code == 200
    assert response.json() == {"error": "unable to fetch tracks"}


def test_album_download_enqueues_tracks_without_legacy_fallback(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    class _MB:
        def pick_best_release_with_reason(self, _rgid, prefer_country=None):
            return {"release_id": "rel-1", "reason": "test"}

        def fetch_release_tracks(self, _rid):
            return [
                {
                    "artist": "Artist",
                    "album": "Album",
                    "title": "Track A",
                    "track_number": 1,
                    "disc_number": 1,
                    "release_date": "2024-01-01",
                    "duration_ms": 123000,
                    "artwork_url": None,
                },
                {
                    "artist": "Artist",
                    "album": "Album",
                    "title": "Track B",
                    "track_number": 2,
                    "disc_number": 1,
                    "release_date": "2024-01-01",
                    "duration_ms": 125000,
                    "artwork_url": None,
                },
            ]

    monkeypatch.setattr("api.main._mb_service", lambda: _MB())

    enqueued: list[dict] = []

    def _capture_enqueue(self, payload: dict) -> None:
        enqueued.append(payload)

    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", _capture_enqueue)

    response = client.post(
        "/api/music/album/download",
        json={"release_group_id": "rg-1"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "tracks_enqueued": 2}
    assert len(enqueued) == 2
    assert all(item.get("media_intent") == "music_track" for item in enqueued)
