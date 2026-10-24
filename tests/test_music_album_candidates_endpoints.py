from __future__ import annotations

import importlib
import sys

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    import engine.core  # noqa: F401
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9 (main, Jan  1 2024, 00:00:00) [Clang 14.0.0]", raising=False)
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
        lambda query, limit, offset=0: {
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


def test_artist_mbid_album_search_overfetches_before_filtering(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    def _fake_search_release_groups(*, query, limit, offset=0):
        assert query == "arid:artist-mbid-123"
        assert limit > 10
        return {
            "release-group-list": [
                {
                    "id": f"single-{idx}",
                    "title": f"Single {idx}",
                    "primary-type": "Single",
                    "artist-credit": [{"artist": {"id": "artist-mbid-123"}, "name": "ERNEST"}],
                    "ext:score": "99",
                }
                for idx in range(12)
            ]
            + [
                {
                    "id": "rg-album-late",
                    "title": "Late Album",
                    "primary-type": "Album",
                    "artist-credit": [{"artist": {"id": "artist-mbid-123"}, "name": "ERNEST"}],
                    "ext:score": "98",
                }
            ]
        }

    monkeypatch.setattr(module.musicbrainzngs, "search_release_groups", _fake_search_release_groups)

    resp = client.get(
        "/api/music/albums/search",
        params={"q": "ERNEST", "artist_mbid": "artist-mbid-123", "limit": 10},
    )

    assert resp.status_code == 200
    ids = {str(item.get("release_group_id") or "") for item in resp.json()}
    assert ids == {"rg-album-late"}


def test_album_search_by_artist_name_falls_back_to_strict_artist_mbid(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    monkeypatch.setattr(module, "_mb_service", lambda: type("MB", (), {"search_release_groups": lambda self, query, limit: []})())
    monkeypatch.setattr(
        module,
        "search_music_metadata",
        lambda **kwargs: {
            "artists": [
                {
                    "artist_mbid": "artist-mbid-123",
                    "name": "ERNEST",
                }
            ]
        },
    )
    monkeypatch.setattr(
        module,
        "_search_music_album_candidates_for_artist_mbid",
        lambda artist_mbid, *, limit: [{"release_group_id": "rg-strict", "title": "Strict Album"}],
    )

    resp = client.get("/api/music/albums/search", params={"q": "ERNEST", "limit": 10})

    assert resp.status_code == 200
    assert resp.json() == [{"release_group_id": "rg-strict", "title": "Strict Album"}]


def test_music_intent_preview_returns_only_verified_us_candidates(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    monkeypatch.setattr(
        module,
        "search_music_metadata",
        lambda **kwargs: {"artists": [{"name": "Green Day", "artist_mbid": "green-day"}]},
    )
    monkeypatch.setattr(
        module,
        "_search_music_album_candidates_for_artist_mbid",
        lambda artist_mbid, *, limit: [
            {"release_group_id": "bootleg", "title": "Greatest Hits", "secondary_types": []},
            {"release_group_id": "official", "title": "International Superhits!", "secondary_types": ["Compilation"]},
        ],
    )

    monkeypatch.setattr(module, "_mb_get_release_group_by_id_raw", lambda rgid, includes: {"release-group": {"release-list": [] if rgid == "bootleg" else [{"id": "us-release", "status": "Official", "country": "US", "title": "International Superhits!", "artist-credit": [{"name": "Green Day"}]}]}})
    resp = client.post(
        "/api/music/intent/preview",
        json={"media_intent": "music_album", "intent": "download album of Greatest Hits of Green Day"},
    )

    assert resp.status_code == 200
    assert resp.json()["candidates"] == [{
        "status": "ok", "release_group_mbid": "official", "release_mbid": "us-release", "title": "International Superhits!", "artist": "Green Day", "country": "US", "date": None, "track_count": None, "artwork_url": "https://coverartarchive.org/release/us-release/front",
        "intent": {"artist": "Green Day", "album": "Greatest Hits"},
    }]


def test_music_intent_preview_pages_verified_candidates(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")
    monkeypatch.setattr(
        module, "search_music_metadata",
        lambda **kwargs: {"artists": [{"name": "Artist", "artist_mbid": "artist"}]},
    )
    monkeypatch.setattr(
        module, "_search_music_album_candidates_for_artist_mbid",
        lambda artist_mbid, *, limit: [
            {"release_group_id": f"rg-{idx}", "title": f"Greatest Hits {idx}", "secondary_types": ["Compilation"]}
            for idx in range(4)
        ],
    )
    monkeypatch.setattr(module, "_mb_get_release_group_by_id_raw", lambda rgid, includes: {"release-group": {"release-list": [{"id": f"us-{rgid}", "status": "Official", "country": "US"}]}})
    resp = client.post(
        "/api/music/intent/preview",
        json={"media_intent": "music_album", "intent": "download album of Greatest Hits of Artist", "limit": 2, "offset": 2},
    )
    assert resp.status_code == 200
    assert [item["release_group_mbid"] for item in resp.json()["candidates"]] == ["rg-2", "rg-3"]
    assert resp.json()["next_offset"] is None


@pytest.mark.parametrize("intent, expected", [
    ("download album of Greatest Hits of Green Day", ("Greatest Hits", "Green Day")),
    ("Please get me the album Dookie by Green Day", ("Dookie", "Green Day")),
    ("Green Day's album Dookie", ("Dookie", "Green Day")),
    ("Green Day - Dookie", ("Dookie", "Green Day")),
])
def test_music_intent_parser_common_wording(monkeypatch, intent, expected) -> None:
    _build_client(monkeypatch)
    module = importlib.import_module("api.main")
    assert module._parse_music_album_intent(intent) == expected


def test_music_intent_preview_rejects_unrelated_alternatives(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")
    monkeypatch.setattr(module, "search_music_metadata", lambda **kwargs: {"artists": [{"name": "Green Day", "artist_mbid": "green-day"}]})
    monkeypatch.setattr(module, "_search_music_album_candidates_for_artist_mbid", lambda artist_mbid, *, limit: [
        {"release_group_id": "hits", "title": "Greatest Hits: God's Favorite Band", "secondary_types": ["Compilation"]},
        {"release_group_id": "video", "title": "International Supervideos!", "secondary_types": ["Video"]},
        {"release_group_id": "sessions", "title": "BBC Sessions", "secondary_types": ["Live"]},
    ])
    looked_up = []

    def release_group(rgid, includes):
        looked_up.append(rgid)
        return {"release-group": {"release-list": [{"id": "us-" + rgid, "status": "Official", "country": "US"}]}}

    monkeypatch.setattr(module, "_mb_get_release_group_by_id_raw", release_group)
    resp = client.post("/api/music/intent/preview", json={
        "media_intent": "music_album", "artist": "Green Day", "album": "Greatest Hits", "limit": 3,
    })
    assert resp.status_code == 200
    assert [item["release_group_mbid"] for item in resp.json()["candidates"]] == ["hits"]
    assert resp.json()["next_offset"] is None
    assert looked_up == ["hits"]
