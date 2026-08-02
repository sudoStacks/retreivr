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
