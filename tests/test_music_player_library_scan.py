from __future__ import annotations

import importlib.util
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


def _load_music_player_module():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    binding_module = sys.modules.get("engine.musicbrainz_binding")
    if binding_module is None:
        binding_module = types.ModuleType("engine.musicbrainz_binding")
        sys.modules["engine.musicbrainz_binding"] = binding_module
    binding_module.search_artists_by_genre = lambda *args, **kwargs: []
    binding_module._normalize_title_for_mb_lookup = lambda value, **kwargs: str(value or "")
    binding_module.resolve_best_mb_pair = lambda *args, **kwargs: None
    return _load_module("engine_music_player_tests", _ROOT / "engine" / "music_player.py")


class _FakeTags(dict):
    pass


class _FakeAudio:
    def __init__(self, tags):
        self.tags = tags


def test_scan_local_library_prefers_embedded_tags_and_mbids(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    track_path = tmp_path / "Folder Artist" / "Folder Album" / "01 - Track.mp3"
    track_path.parent.mkdir(parents=True, exist_ok=True)
    track_path.write_bytes(b"test-audio")

    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [tmp_path])
    monkeypatch.setattr(
        music_player,
        "MutagenFile",
        lambda _path, easy=False: _FakeAudio(
            _FakeTags(
                {
                    "TIT2": ["red"],
                    "TPE1": ["HARDY feat. Morgan Wallen"],
                    "TPE2": ["HARDY"],
                    "TALB": ["the mockingbird & THE CROW"],
                    "musicbrainz_trackid": ["f069fca4-97f8-4bb1-a627-8881a0bf5240"],
                    "musicbrainz_releaseid": ["fb6279fc-91dc-4a27-93e2-03864f92b96d"],
                    "musicbrainz_releasegroupid": ["082002ba-ab38-4da4-8ea5-000a203dda49"],
                }
            )
        ),
    )

    items = music_player.scan_local_library({}, limit=10)

    assert len(items) == 1
    item = items[0]
    assert item["title"] == "red"
    assert item["artist"] == "HARDY"
    assert item["album"] == "the mockingbird & THE CROW"
    assert item["recording_mbid"] == "f069fca4-97f8-4bb1-a627-8881a0bf5240"
    assert item["mb_release_id"] == "fb6279fc-91dc-4a27-93e2-03864f92b96d"
    assert item["mb_release_group_id"] == "082002ba-ab38-4da4-8ea5-000a203dda49"


def test_scan_local_library_falls_back_to_path_when_tags_unavailable(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    track_path = tmp_path / "Fallback Artist" / "Fallback Album" / "02 - Song.mp3"
    track_path.parent.mkdir(parents=True, exist_ok=True)
    track_path.write_bytes(b"test-audio")

    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [tmp_path])
    monkeypatch.setattr(music_player, "MutagenFile", lambda _path, easy=False: None)

    items = music_player.scan_local_library({}, limit=10)

    assert len(items) == 1
    item = items[0]
    assert item["artist"] == "Fallback Artist"
    assert item["album"] == "Fallback Album"
    assert item["title"] == "02 - Song"
    assert item["recording_mbid"] is None
