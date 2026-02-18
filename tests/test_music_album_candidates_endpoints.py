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


def test_album_candidates_endpoints_share_canonical_search(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    payload = [
        {
            "release_group_id": "rg-1",
            "title": "Album One",
            "artist_credit": "Artist One",
            "first_release_date": "2001-01-01",
            "primary_type": "Album",
            "secondary_types": [],
            "score": 95,
            "track_count": None,
        }
    ]

    monkeypatch.setattr("api.main._search_music_album_candidates", lambda query, limit: payload if query else [])

    get_resp = client.get("/api/music/albums/search", params={"q": "Album One", "limit": 10})
    assert get_resp.status_code == 200
    assert get_resp.json() == payload

    post_resp = client.post("/api/music/album/candidates", json={"query": "Album One"})
    assert post_resp.status_code == 200
    assert post_resp.json() == {
        "status": "ok",
        "album_candidates": [
            {
                "album_id": "rg-1",
                "title": "Album One",
                "artist": "Artist One",
                "first_released": "2001-01-01",
                "track_count": None,
                "score": 95,
            }
        ],
    }


def test_album_search_get_empty_query_returns_empty_list(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    monkeypatch.setattr("api.main._search_music_album_candidates", lambda query, limit: [])
    resp = client.get("/api/music/albums/search", params={"q": ""})
    assert resp.status_code == 200
    assert resp.json() == []


def test_album_candidates_post_empty_query_returns_legacy_envelope(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    monkeypatch.setattr("api.main._search_music_album_candidates", lambda query, limit: [])
    resp = client.post("/api/music/album/candidates", json={"query": ""})
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "album_candidates": []}
