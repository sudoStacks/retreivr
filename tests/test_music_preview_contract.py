from __future__ import annotations

import importlib
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
    assert "stream_url" not in payload


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
