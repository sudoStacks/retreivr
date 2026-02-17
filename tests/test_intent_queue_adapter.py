from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")


def _load_module(monkeypatch):
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    return importlib.import_module("api.main")


def test_intent_queue_adapter_enqueues_resolved_media_payload(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    captured = []

    class _Store:
        def enqueue_job(self, **kwargs):
            captured.append(kwargs)
            return "job-1"

    module.app.state.worker_engine = SimpleNamespace(store=_Store())
    adapter = module._IntentQueueAdapter()
    adapter.enqueue(
        {
            "playlist_id": "pl-1",
            "spotify_track_id": "trk-1",
            "resolved_media": {
                "media_url": "https://example.test/audio",
                "source_id": "youtube",
                "duration_ms": 180000,
            },
            "music_metadata": {
                "title": "Song",
                "artist": "Artist",
                "album": "Album",
                "track_num": 1,
                "disc_num": 1,
                "isrc": "USABC123",
            },
        }
    )

    assert len(captured) == 1
    job = captured[0]
    assert job["origin"] == "spotify_playlist"
    assert job["origin_id"] == "pl-1"
    assert job["url"] == "https://example.test/audio"
    assert job["media_intent"] == "track"
    assert job["media_type"] == "music"
    assert job["output_template"]["track"] == "Song"


def test_intent_queue_adapter_converts_watch_payload_to_music_track_job(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    captured = []

    class _Store:
        def enqueue_job(self, **kwargs):
            captured.append(kwargs)
            return "job-2"

    module.app.state.worker_engine = SimpleNamespace(store=_Store())
    adapter = module._IntentQueueAdapter()
    adapter.enqueue(
        {
            "playlist_id": "pl-2",
            "spotify_track_id": "trk-2",
            "artist": "Example Artist",
            "title": "Example Track",
            "album": "Example Album",
            "duration_ms": 205000,
        }
    )

    assert len(captured) == 1
    job = captured[0]
    assert job["origin"] == "spotify_playlist"
    assert job["origin_id"] == "pl-2"
    assert job["media_intent"] == "music_track"
    assert job["source"] == "youtube_music"
    assert job["url"].startswith("https://music.youtube.com/search?q=")
    assert job["output_template"]["artist"] == "Example Artist"
    assert job["output_template"]["track"] == "Example Track"
    assert job["output_template"]["album"] == "Example Album"


def test_intent_queue_adapter_skips_non_searchable_payload(monkeypatch, caplog) -> None:
    module = _load_module(monkeypatch)

    class _Store:
        def enqueue_job(self, **kwargs):  # pragma: no cover - should not be called
            raise AssertionError("enqueue_job should not be called")

    module.app.state.worker_engine = SimpleNamespace(store=_Store())
    adapter = module._IntentQueueAdapter()
    adapter.enqueue({"playlist_id": "pl-3"})

    assert "no media URL or searchable artist/title available" in caplog.text
