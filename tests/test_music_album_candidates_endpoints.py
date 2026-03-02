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


def test_album_search_get_uses_artist_mbid_strict_lookup(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    called = {"artist_mbid": None}

    def _fake_by_mbid(artist_mbid: str, *, limit: int):
        called["artist_mbid"] = artist_mbid
        return [{"release_group_id": "rg-strict", "title": "Strict Album"}]

    monkeypatch.setattr(module, "_search_music_album_candidates_for_artist_mbid", _fake_by_mbid)
    monkeypatch.setattr(module, "_search_music_album_candidates", lambda query, limit: [{"release_group_id": "rg-loose"}])

    resp = client.get(
        "/api/music/albums/search",
        params={"q": "ERNEST", "artist_mbid": "artist-mbid-123", "limit": 10},
    )
    assert resp.status_code == 200
    assert called["artist_mbid"] == "artist-mbid-123"
    assert resp.json() == [{"release_group_id": "rg-strict", "title": "Strict Album"}]


def test_artist_mbid_album_search_filters_out_singles(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    monkeypatch.setattr(
        module.musicbrainzngs,
        "search_release_groups",
        lambda query, limit: {
            "release-group-list": [
                {
                    "id": "rg-album-1",
                    "title": "Real Album",
                    "primary-type": "Album",
                    "secondary-type-list": [],
                    "artist-credit": [{"name": "ERNEST"}],
                    "ext:score": "97",
                },
                {
                    "id": "rg-ep-1",
                    "title": "Real EP",
                    "primary-type": "EP",
                    "secondary-type-list": [],
                    "artist-credit": [{"name": "ERNEST"}],
                    "ext:score": "95",
                },
                {
                    "id": "rg-single-1",
                    "title": "Single Leak",
                    "primary-type": "Single",
                    "secondary-type-list": [],
                    "artist-credit": [{"name": "ERNEST"}],
                    "ext:score": "99",
                },
            ]
        },
    )

    resp = client.get(
        "/api/music/albums/search",
        params={"q": "ERNEST", "artist_mbid": "artist-mbid-123", "limit": 10},
    )
    assert resp.status_code == 200
    payload = resp.json()
    ids = {str(item.get("release_group_id") or "") for item in payload}
    assert "rg-album-1" in ids
    assert "rg-ep-1" in ids
    assert "rg-single-1" not in ids
