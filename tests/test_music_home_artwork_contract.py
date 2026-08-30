from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@pytest.fixture()
def api_module(tmp_path: Path, monkeypatch):
    import engine.core  # noqa: F401

    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.paths = SimpleNamespace(db_path=str(tmp_path / "retreivr.sqlite3"))
    module.app.state.loaded_config = {}
    module.app.state.config = {}
    monkeypatch.setattr(module, "_read_config_or_404", lambda: {"music_preferences": {}})
    return module


def test_music_home_prefers_local_artwork_for_continue_and_library(api_module, monkeypatch) -> None:
    indexed = [
        {
            "title": "Track One",
            "artist": "Artist One",
            "album": "Album One",
            "local_path": "/downloads/Music/Artist One/Album One/Track One.mp3",
            "artwork_local_path": "/data/artwork_cache/local_embedded/album-one.jpg",
            "downloaded_at": 20,
        }
    ]

    monkeypatch.setattr(api_module, "list_indexed_music", lambda *_args, **_kwargs: indexed)
    monkeypatch.setattr(api_module, "list_history", lambda *_args, **_kwargs: [
        {
            "title": "Track One",
            "artist": "Artist One",
            "album": "Album One",
            "stream_url": "",
            "local_path": "/downloads/Music/Artist One/Album One/Track One.mp3",
            "source_kind": "local",
            "played_at": "2026-08-26T12:00:00Z",
        }
    ])
    monkeypatch.setattr(api_module, "_music_home_spotify_cards", lambda *_args, **_kwargs: [])

    resp = TestClient(api_module.app).get("/api/music/home")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["artwork_contract"]["local_first"] is True
    assert payload["continue_listening"][0]["artwork_status"] == "ok"
    assert payload["continue_listening"][0]["artwork_source"] == "local_embedded"
    assert payload["continue_listening"][0]["artwork_url"].startswith("/api/player/art/local?path=")
    assert payload["library_albums"][0]["artwork_url"].startswith("/api/player/art/local?path=")


def test_music_home_genres_always_have_artwork_contract(api_module, monkeypatch) -> None:
    monkeypatch.setattr(api_module, "_read_config_or_404", lambda: {
        "music_preferences": {"favorite_genres": ["Country"]}
    })
    monkeypatch.setattr(api_module, "list_indexed_music", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(api_module, "list_history", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(api_module, "_music_home_spotify_cards", lambda *_args, **_kwargs: [])

    resp = TestClient(api_module.app).get("/api/music/home")

    assert resp.status_code == 200
    genre = resp.json()["genres"][0]
    assert genre["genre"] == "Country"
    assert genre["artwork_status"] in {"ok", "fallback"}
    assert "artwork_urls" in genre


def test_music_home_genre_prefers_matching_local_artwork(api_module, monkeypatch) -> None:
    monkeypatch.setattr(api_module, "_read_config_or_404", lambda: {
        "music_preferences": {"favorite_genres": ["Country"]}
    })
    monkeypatch.setattr(api_module, "list_indexed_music", lambda *_args, **_kwargs: [
        {
            "title": "Rock Track",
            "artist": "Rock Artist",
            "album": "Rock Album",
            "genre": "Rock",
            "local_path": "/downloads/Music/Rock/Track.mp3",
            "artwork_local_path": "/data/artwork_cache/local_embedded/rock.jpg",
        },
        {
            "title": "Country Track",
            "artist": "Country Artist",
            "album": "Country Album",
            "genre": "Country",
            "local_path": "/downloads/Music/Country/Track.mp3",
            "artwork_local_path": "/data/artwork_cache/local_embedded/country.jpg",
        },
    ])
    monkeypatch.setattr(api_module, "list_history", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(api_module, "_music_home_spotify_cards", lambda *_args, **_kwargs: [])

    payload = TestClient(api_module.app).get("/api/music/home").json()

    genre = payload["genres"][0]
    assert genre["genre"] == "Country"
    assert genre["artwork_status"] == "ok"
    assert "country.jpg" in genre["artwork_urls"][0]


def test_music_home_respects_hidden_music_preferences(api_module, monkeypatch) -> None:
    monkeypatch.setattr(api_module, "_read_config_or_404", lambda: {
        "music_preferences": {
            "favorite_genres": ["Country", "Rock"],
            "favorite_artists": [{"name": "Hidden Artist"}, {"name": "Visible Artist"}],
        },
        "ui_preferences": {
            "music_hidden_genres": ["Country"],
            "music_hidden_artists": [{"name": "Hidden Artist"}],
        },
    })
    monkeypatch.setattr(api_module, "list_indexed_music", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(api_module, "list_history", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(api_module, "_music_home_spotify_cards", lambda *_args, **_kwargs: [])

    payload = TestClient(api_module.app).get("/api/music/home").json()

    assert [item["genre"] for item in payload["genres"]] == ["Rock"]
    assert [item["name"] for item in payload["favorite_artists"]] == ["Visible Artist"]


def test_spotify_cards_reuse_persistent_summary_without_live_resolve(api_module, monkeypatch) -> None:
    monkeypatch.setattr(api_module.time, "time", lambda: 1000)
    seed = api_module.GENERAL_SPOTIFY_PLAYLIST_SEEDS[0]
    api_module._set_music_artwork_cache_entry(
        "spotify_playlist",
        seed.playlist_id,
        {
            "summary": {
                "playlist_id": seed.playlist_id,
                "playlist_url": seed.playlist_url,
                "title": "Cached Playlist",
                "image_url": "/api/music/art/cache?path=cached",
                "track_count": 12,
                "total_tracks": 12,
                "tracks_preview": [],
            }
        },
    )

    class _Resolver:
        def resolve(self, *_args, **_kwargs):  # pragma: no cover - should not be called
            raise AssertionError("live Spotify resolution should not run for fresh cache")

    monkeypatch.setattr(api_module, "_spotify_public_resolver", lambda _config: _Resolver())

    cards = api_module._music_home_spotify_cards({}, {"favorite_genres": []}, limit=1)

    assert cards[0]["title"] == "Cached Playlist"
    assert cards[0]["cache"] == "persistent"


def test_spotify_playlist_preflight_returns_album_view_sized_preview() -> None:
    source = Path("api/main.py").read_text()
    helper_start = source.index("def _resolve_spotify_playlist_for_import")
    helper_end = source.index("def _trim_playlist_import_jobs_locked", helper_start)
    helper_source = source[helper_start:helper_end]

    assert "resolved.to_summary(preview_limit=100)" in helper_source


def test_music_browse_index_table_persists_typed_rows(api_module) -> None:
    api_module._set_music_browse_index_entry(
        "genre_artists",
        "rock:24:0",
        {"artists": [{"name": "Artist One"}]},
        source_provider="unit",
        ttl_seconds=3600,
        stale_seconds=600,
    )

    entry = api_module._get_music_browse_index_entry("genre_artists", "rock:24:0")

    assert entry is not None
    assert entry["artists"] == [{"name": "Artist One"}]
    assert entry["source_provider"] == "unit"
    assert entry["status"] == "ok"
    assert api_module._music_browse_entry_is_usable(entry) is True
    assert api_module._music_browse_entry_is_fresh(entry) is True


def test_music_home_snapshot_uses_browse_index_without_rebuilding(api_module, monkeypatch) -> None:
    api_module._set_music_browse_index_entry(
        "home",
        "v2",
        {"payload": {"snapshot_at": 123, "genres": [], "spotify_playlists": []}},
        source_provider="unit",
        ttl_seconds=3600,
        stale_seconds=600,
    )

    monkeypatch.setattr(api_module, "list_indexed_music", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("home should use browse snapshot")))

    resp = TestClient(api_module.app).get("/api/music/home")

    assert resp.status_code == 200
    assert resp.json()["cache"] == "snapshot"
    assert resp.json()["snapshot_at"] == 123


def test_fast_album_search_returns_browse_index_and_schedules_refresh(api_module, monkeypatch) -> None:
    api_module._set_music_browse_index_entry(
        "artist_albums",
        "artist one:24",
        {"albums": [{"release_group_mbid": "rg-1", "title": "Cached Album"}]},
        source_provider="unit",
        ttl_seconds=3600,
        stale_seconds=600,
    )
    monkeypatch.setattr(api_module, "_search_music_album_candidates", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("fast cached path should not search")))

    resp = TestClient(api_module.app).get(
        "/api/music/albums/search",
        params={"q": "Artist One", "limit": 24, "fast": "true"},
    )

    assert resp.status_code == 200
    assert resp.json() == [{"release_group_mbid": "rg-1", "title": "Cached Album"}]


def test_genre_artists_returns_browse_index_without_musicbrainz(api_module, monkeypatch) -> None:
    api_module._set_music_browse_index_entry(
        "genre_artists",
        "rock:24:0",
        {"artists": [{"name": "Cached Rock", "artist_mbid": "artist-1"}]},
        source_provider="unit",
        ttl_seconds=3600,
        stale_seconds=600,
    )
    monkeypatch.setattr(api_module, "search_artists_by_genre", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("cached genre path should not search")))

    resp = TestClient(api_module.app).get("/api/music/genres/Rock/artists", params={"limit": 24})

    assert resp.status_code == 200
    assert resp.json()["cached"] is True
    assert resp.json()["artists"] == [{"name": "Cached Rock", "artist_mbid": "artist-1"}]
