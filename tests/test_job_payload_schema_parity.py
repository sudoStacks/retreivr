from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_job_queue_module():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    return _load_module("engine_job_queue_schema_parity", _ROOT / "engine" / "job_queue.py")


@pytest.fixture(scope="module")
def jq():
    return _load_job_queue_module()


def _build_import_payload(jq):
    return jq.build_download_job_payload(
        config={"final_format": "mp3"},
        origin="import",
        origin_id="batch-1",
        media_type="music",
        media_intent="music_track",
        source="music_import",
        url="musicbrainz://recording/mbid-1",
        input_url="musicbrainz://recording/mbid-1",
        base_dir="/downloads",
        final_format_override="mp3",
        resolved_metadata={
            "artist": "Artist",
            "track": "Track",
            "album": "Album",
            "recording_mbid": "mbid-1",
        },
        output_template_overrides={
            "kind": "music_track",
            "source": "import",
            "import_batch": "batch-1",
            "import_batch_id": "batch-1",
            "source_index": 0,
            "recording_mbid": "mbid-1",
            "mb_recording_id": "mbid-1",
            "mb_release_id": "release-1",
            "audio_mode": True,
        },
        canonical_id="music_track:mbid-1",
    )


def _build_spotify_payload(jq):
    return jq.build_download_job_payload(
        config={"final_format": "mp3"},
        origin="spotify_playlist",
        origin_id="playlist-1",
        media_type="music",
        media_intent="music_track",
        source="youtube_music",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        input_url="https://www.youtube.com/watch?v=abc123xyz00",
        base_dir="/downloads",
        final_format_override="mp3",
        resolved_metadata={
            "artist": "Artist",
            "track": "Track",
            "album": "Album",
        },
        output_template_overrides={
            "audio_mode": True,
            "track_number": 1,
            "disc_number": 1,
            "duration_ms": 180000,
        },
        canonical_id="music_track:artist:album:1:track",
    )


def _build_direct_payload(jq):
    return jq.build_download_job_payload(
        config={"final_format": "mp3", "media_type": "music"},
        origin="manual",
        origin_id="abc123xyz00",
        media_type="music",
        media_intent="track",
        source="youtube",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        base_dir="/downloads",
        final_format_override="mp3",
    )


def _build_scheduler_payload(jq):
    return jq.build_download_job_payload(
        config={"final_format": "mp3"},
        origin="playlist",
        origin_id="PL123",
        media_type="music",
        media_intent="track",
        source="youtube",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        playlist_entry={
            "playlist_id": "PL123",
            "folder": "/downloads",
            "media_type": "music",
            "final_format": "mp3",
            "playlistItemId": "pli-1",
            "account": None,
        },
        base_dir="/downloads",
    )


@pytest.mark.parametrize(
    "factory_name",
    ["import", "spotify", "direct", "scheduler"],
)
def test_job_payload_schema_parity(monkeypatch, jq, factory_name: str) -> None:
    factories = {
        "import": _build_import_payload,
        "spotify": _build_spotify_payload,
        "direct": _build_direct_payload,
        "scheduler": _build_scheduler_payload,
    }
    payload = factories[factory_name](jq)
    assert payload["media_type"] == "music"
    assert payload["output_template"]["final_format"] == "mp3"
    expected_keys = {
        "output_dir",
        "final_format",
        "filename_template",
        "audio_filename_template",
        "remove_after_download",
        "playlist_item_id",
        "source_account",
        "canonical_metadata",
        "artist",
        "album",
        "track",
        "track_number",
        "disc_number",
        "release_date",
        "audio_mode",
        "duration_ms",
        "artwork_url",
        "recording_mbid",
        "mb_recording_id",
        "mb_release_id",
        "mb_release_group_id",
        "kind",
        "source",
        "import_batch",
        "import_batch_id",
        "source_index",
    }
    assert set(payload["output_template"].keys()) == expected_keys
