from __future__ import annotations

import importlib
import json
import sqlite3
import sys
import time
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
    return module


def test_music_preview_returns_iframe_video_for_youtube_source(api_module, monkeypatch) -> None:
    monkeypatch.setattr(
        api_module,
        "_resolve_music_preview_candidate",
        lambda **_kwargs: {
            "source": "youtube_music",
            "source_url": "https://www.youtube.com/watch?v=abc123XYZ99",
            "title": "Example Track",
            "resolved_via": "search_fallback",
            "video_id": "abc123XYZ99",
        },
    )
    client = TestClient(api_module.app)

    resp = client.post(
        "/api/music/preview",
        json={
            "recording_mbid": "recording-1",
            "artist": "Artist",
            "track": "Example Track",
            "media_mode": "music",
        },
    )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["preview_type"] == "video"
    assert payload["source_url"] == "https://www.youtube.com/watch?v=abc123XYZ99"
    assert payload["video_id"] == "abc123XYZ99"
    assert payload["playback_adapter"] == "youtube_iframe"
    assert payload["requires_visible_player"] is True
    assert payload["artwork_url"] == "https://i.ytimg.com/vi/abc123XYZ99/hqdefault.jpg"
    assert "stream_url" not in payload


def test_runtime_resolution_uses_publisher_outbox_and_enforces_score(api_module, tmp_path: Path) -> None:
    main_dir = tmp_path / "main"
    search_dir = tmp_path / "search"
    main_dir.mkdir()
    search_dir.mkdir()
    main_db = main_dir / "retreivr.sqlite3"
    search_db = search_dir / "search.sqlite3"
    sqlite3.connect(main_db).close()
    sqlite3.connect(search_db).close()
    api_module.app.state.paths = SimpleNamespace(db_path=str(main_db))
    api_module.app.state.search_db_path = str(search_db)
    api_module.app.state.config = {
        "community_cache_publish_enabled": True,
        "community_cache_publish_mode": "write_outbox",
        "community_cache_publish_min_score": 0.78,
    }
    api_module.app.state.loaded_config = dict(api_module.app.state.config)
    client = TestClient(api_module.app)

    high = client.post("/api/music/runtime-resolution", json={
        "recording_mbid": "11111111-1111-4111-8111-111111111111",
        "release_mbid": "22222222-2222-4222-8222-222222222222",
        "release_group_mbid": "33333333-3333-4333-8333-333333333333",
        "source": "youtube",
        "source_url": "https://www.youtube.com/watch?v=highScore01",
        "selected_score": 0.93,
        "duration_ms": 200000,
        "resolved_via": "lookahead",
    })
    assert high.status_code == 200
    assert high.json()["community_publish"]["status"] == "written"
    outbox = main_dir / "run_summaries" / "community_publish_outbox"
    lines = [json.loads(line) for path in outbox.glob("*.jsonl") for line in path.read_text().splitlines() if line]
    assert lines[0]["release_mbid"] == "22222222-2222-4222-8222-222222222222"
    assert lines[0]["release_group_mbid"] == "33333333-3333-4333-8333-333333333333"
    assert not (search_dir / "run_summaries" / "community_publish_outbox").exists()

    low = client.post("/api/music/runtime-resolution", json={
        "recording_mbid": "44444444-4444-4444-8444-444444444444",
        "source": "youtube",
        "source_url": "https://www.youtube.com/watch?v=lowScore001",
        "selected_score": 0.55,
        "resolved_via": "lookahead",
    })
    assert low.status_code == 200
    assert low.json()["community_publish"]["reason"] == "selected_score_below_min"
    assert len([line for path in outbox.glob("*.jsonl") for line in path.read_text().splitlines() if line]) == 1


def test_music_preview_prefers_musicbrainz_bound_source_without_network_resolution(api_module, monkeypatch) -> None:
    def fail_if_called(**_kwargs):
        raise AssertionError("fallback resolver should not be called")

    monkeypatch.setattr(api_module.YouTubeAdapter, "search_music_track", fail_if_called)
    client = TestClient(api_module.app)

    resp = client.post(
        "/api/music/preview",
        json={
            "recording_mbid": "recording-1",
            "artist": "Artist",
            "track": "Example Track",
            "mb_youtube_urls": ["https://www.youtube.com/watch?v=bound123XYZ"],
            "media_mode": "music",
        },
    )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["source_url"] == "https://www.youtube.com/watch?v=bound123XYZ"
    assert payload["resolved_via"] == "musicbrainz_bound_metadata"


def test_youtube_family_url_validation_rejects_deceptive_hosts(api_module) -> None:
    assert api_module._is_youtube_family_url("https://youtu.be/abc123") is True
    assert api_module._is_youtube_family_url("https://music.youtube.com/watch?v=abc123") is True
    assert api_module._is_youtube_family_url("https://youtube.com.example.test/watch?v=abc123") is False
    assert api_module._is_youtube_family_url("https://example.test/youtube.com/watch?v=abc123") is False


def test_musicbrainz_youtube_relationships_accept_real_api_shapes(api_module) -> None:
    payload = {
        "url-relation-list": [
            {"type": "youtube", "target": "https://www.youtube.com/watch?v=direct12345"},
            {"type": "streaming", "target": {"resource": "https://music.youtube.com/watch?v=nested12345"}},
            {"type": "other", "url": {"resource": "https://youtu.be/urlobj12345"}},
            {"type": "homepage", "target": "https://example.test/not-youtube"},
        ],
        "relation-list": [
            {
                "target-type": "url",
                "relation": [
                    {"type": "video", "target": "https://www.youtube.com/watch?v=bucket12345"},
                ],
            }
        ],
    }

    assert api_module._extract_mb_youtube_urls(payload) == [
        "https://www.youtube.com/watch?v=direct12345",
        "https://music.youtube.com/watch?v=nested12345",
        "https://youtu.be/urlobj12345",
    ]


def test_music_preview_fast_search_uses_lightweight_youtube_result(api_module, monkeypatch) -> None:
    calls = []

    class _FakeYouTubeAdapter:
        def search_track(self, artist, track, album=None, limit=5, *, lightweight=False, timeout_budget_sec=None):
            calls.append((artist, track, album, limit, lightweight, timeout_budget_sec))
            return [
                {
                    "source": "youtube",
                    "video_id": "qSorUl1pBbg",
                    "url": "https://www.youtube.com/watch?v=qSorUl1pBbg",
                    "title": "When God Paints",
                    "uploader": "Alan Jackson",
                }
            ]

    monkeypatch.setattr(api_module, "YouTubeAdapter", _FakeYouTubeAdapter)
    monkeypatch.setattr(api_module, "youtube_fast_search", lambda *_args, **_kwargs: [])
    api_module._MUSIC_PREVIEW_CACHE.clear()

    preview = api_module._resolve_music_preview_candidate(
        recording_mbid="",
        artist="Alan Jackson",
        track="When God Paints",
        album="Angels and Alcohol",
        media_mode="music",
    )

    assert preview is not None
    assert preview["video_id"] == "qSorUl1pBbg"
    assert preview["resolved_via"] == "youtube_fast_search:yt_dlp_flat"
    assert calls[0][2] is None
    assert calls[0][4] is True


def test_music_preview_uses_community_cache_before_fresh_youtube_search(api_module, monkeypatch) -> None:
    calls = []
    api_module.app.state.loaded_config = {"community_cache_lookup_enabled": True}
    api_module._MUSIC_PREVIEW_CACHE.clear()
    monkeypatch.setattr(
        api_module.community_cache,
        "cached_lookup",
        lambda recording_mbid, **_kwargs: {
            "recording_mbid": recording_mbid,
            "source": "youtube",
            "candidate_url": "https://www.youtube.com/watch?v=cache123XYZ",
            "video_id": "cache123XYZ",
            "thumbnail_url": "https://img.example/cache-cover.jpg",
        },
    )
    monkeypatch.setattr(
        api_module,
        "_search_fast_music_preview",
        lambda **_kwargs: calls.append("fresh-search") or None,
    )

    preview = api_module._resolve_music_preview_candidate(
        recording_mbid="recording-cache-1",
        artist="Artist",
        track="Track",
        album="Album",
        media_mode="music",
    )

    assert preview is not None
    assert preview["resolved_via"] == "community_cache"
    assert preview["artwork_url"] == "https://img.example/cache-cover.jpg"
    assert calls == []


def test_bounded_call_does_not_wait_for_timed_out_worker(api_module) -> None:
    started_at = time.monotonic()
    with pytest.raises(TimeoutError):
        api_module._bounded_call(0.2, lambda: time.sleep(0.7))
    assert time.monotonic() - started_at < 0.5


def test_music_preview_ranking_prefers_exact_track_title(api_module) -> None:
    ranked = api_module._rank_music_preview_candidates(
        [
            {
                "source": "youtube",
                "video_id": "lyric-video",
                "url": "https://youtube.com/watch?v=lyric-video",
                "title": "Alan Jackson - When God Paints (Lyric Video)",
                "uploader": "Alan Jackson",
            },
            {
                "source": "youtube",
                "video_id": "exact-track",
                "url": "https://youtube.com/watch?v=exact-track",
                "title": "When God Paints",
                "uploader": "Alan Jackson",
            },
        ],
        artist="Alan Jackson",
        track="When God Paints",
    )

    assert ranked[0]["video_id"] == "exact-track"
