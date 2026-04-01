from __future__ import annotations

import importlib.util
import sqlite3
import sys
import types
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_music_modules():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    binding_module = sys.modules.get("engine.musicbrainz_binding")
    if binding_module is None:
        binding_module = types.ModuleType("engine.musicbrainz_binding")
        sys.modules["engine.musicbrainz_binding"] = binding_module
    binding_module.search_artists_by_genre = lambda *args, **kwargs: []
    music_player = _load_module("engine_music_player_cached_tests", _ROOT / "engine" / "music_player.py")
    resolution_api = _load_module("engine_resolution_api_cached_tests", _ROOT / "engine" / "resolution_api.py")
    return music_player, resolution_api


def test_list_cached_matches_returns_youtube_items_with_video_id_only(tmp_path: Path) -> None:
    music_player, resolution_api = _load_music_modules()
    db_path = tmp_path / "search_jobs.sqlite"
    sqlite3.connect(db_path).close()

    resolution_api.submit_mapping(
        str(db_path),
        mbid="11111111-1111-1111-1111-111111111111",
        source_url="https://www.youtube.com/watch?v=abc123DEF45",
        source="youtube_music",
        node_id="node-a",
        source_id="abc123DEF45",
        raw_payload={"artist": "Artist A", "album": "Album A", "track": "Track A"},
    )
    resolution_api.submit_mapping(
        str(db_path),
        mbid="22222222-2222-2222-2222-222222222222",
        source_url="https://soundcloud.com/example/track-b",
        source="soundcloud",
        node_id="node-b",
        raw_payload={"artist": "Artist B", "album": "Album B", "track": "Track B"},
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        items = music_player.list_cached_matches(conn, limit=10)

    assert len(items) == 1
    item = items[0]
    assert item["recording_mbid"] == "11111111-1111-1111-1111-111111111111"
    assert item["stream_url"] == "https://www.youtube.com/watch?v=abc123DEF45"
    assert item["video_id"] == "abc123DEF45"
    assert item["kind"] == "youtube"
    assert item["source"] == "youtube_music"
