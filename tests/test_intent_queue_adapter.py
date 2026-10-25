from __future__ import annotations

from types import SimpleNamespace

import pytest

from fastapi import HTTPException

from api.intent_queue import IntentQueueAdapter


@pytest.fixture(autouse=True)
def _stub_mb_enrichment(monkeypatch):
    import engine.job_queue as jq

    monkeypatch.setattr(jq, "ensure_mb_bound_music_track", lambda *a, **kw: None)


def _build_adapter(*, store, config=None):
    state = SimpleNamespace(
        worker_engine=SimpleNamespace(store=store),
        config_path="/does/not/matter.json",
        paths=SimpleNamespace(single_downloads_dir="/tmp/downloads"),
    )
    return IntentQueueAdapter(state=state, config_loader=lambda _path: dict(config or {}))


def test_album_music_mode_uses_music_destination_and_format_not_video_defaults() -> None:
    captured = []

    class _Store:
        def enqueue_job(self, **kwargs):
            captured.append(kwargs)
            return ("album-job", True, None)

    adapter = _build_adapter(store=_Store(), config={
        "single_download_folder": "Videos/YouTube-Downloads",
        "home_music_download_folder": "Music",
        "home_music_final_format": "m4a",
        "music_final_format": "mp3",
        "final_format": "mkv",
    })
    adapter.enqueue({
        "origin": "music_album", "origin_id": "album-run", "media_mode": "music",
        "media_intent": "music_track", "artist": "Green Day", "album": "Dookie",
        "track": "Burnout", "recording_mbid": "0f8d07a7-e873-4cd8-a64c-72356db8e771",
        "mb_release_id": "3efb7564-6b7c-4bc3-9f10-f0f5b8f5ee61",
        "mb_release_group_id": "af2f9e70-a752-4bd2-b817-a8ac4769dcab",
    })

    assert len(captured) == 1
    job = captured[0]
    assert job["media_type"] == "music"
    assert job["output_template"]["output_dir"] == "/tmp/downloads/Music"
    assert job["resolved_destination"] == "/tmp/downloads/Music"
    assert job["output_template"]["music_final_format"] == "m4a"
    assert job["output_template"]["audio_mode"] is True


def test_intent_queue_adapter_enqueues_resolved_media_payload() -> None:
    captured = []

    class _Store:
        def enqueue_job(self, **kwargs):
            captured.append(kwargs)
            return ("job-1", True, None)

    adapter = _build_adapter(store=_Store())
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


def test_intent_queue_adapter_converts_watch_payload_to_music_track_job() -> None:
    captured = []

    class _Store:
        def enqueue_job(self, **kwargs):
            captured.append(kwargs)
            return ("job-2", True, None)

    adapter = _build_adapter(store=_Store())
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


def test_intent_queue_adapter_skips_non_searchable_payload(caplog) -> None:
    class _Store:
        def enqueue_job(self, **kwargs):  # pragma: no cover - should not be called
            raise AssertionError("enqueue_job should not be called")

    adapter = _build_adapter(store=_Store())
    with pytest.raises(HTTPException):
        adapter.enqueue({"playlist_id": "pl-3"})

    assert "no media URL or searchable artist/title available" in caplog.text


def test_intent_queue_adapter_music_track_prefers_mb_canonical_id() -> None:
    from engine.canonical_ids import build_music_track_canonical_id

    captured = []

    class _Store:
        def enqueue_job(self, **kwargs):
            captured.append(kwargs)
            return ("job-3", True, None)

    adapter = _build_adapter(store=_Store())
    adapter.enqueue(
        {
            "playlist_id": "pl-4",
            "spotify_track_id": "trk-4",
            "media_intent": "music_track",
            "artist": "Artist",
            "track": "Track",
            "album": "Album",
            "recording_mbid": "REC-1",
            "mb_release_id": "REL-1",
            "disc_number": 2,
            "track_number": 7,
        }
    )

    assert len(captured) == 1
    assert captured[0]["canonical_id"] == build_music_track_canonical_id(
        "Artist",
        "Album",
        7,
        "Track",
        recording_mbid="REC-1",
        mb_release_id="REL-1",
        disc_number=2,
    )
