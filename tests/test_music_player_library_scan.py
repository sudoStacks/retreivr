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


class _FakePicture:
    mime = "image/jpeg"

    def __init__(self, data: bytes):
        self.data = data


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


def test_scan_local_library_decodes_byte_tags(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    track_path = tmp_path / "Artist" / "Album" / "01 - Track.m4a"
    track_path.parent.mkdir(parents=True, exist_ok=True)
    track_path.write_bytes(b"test-audio")

    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [tmp_path])
    monkeypatch.setattr(
        music_player,
        "MutagenFile",
        lambda _path, easy=False: _FakeAudio(
            _FakeTags(
                {
                    "\xa9nam": [b"Track"],
                    "\xa9ART": [b"Artist"],
                    "\xa9alb": [b"Album"],
                    "musicbrainz_trackid": [b"6b0b1b38-4f26-4abc-9915-d7ece6633438"],
                    "musicbrainz_releaseid": [b"dac3eddc-cdce-4b93-9226-bc600cfda54f"],
                }
            )
        ),
    )

    items = music_player.scan_local_library({}, limit=10)

    assert items[0]["recording_mbid"] == "6b0b1b38-4f26-4abc-9915-d7ece6633438"
    assert items[0]["mb_release_id"] == "dac3eddc-cdce-4b93-9226-bc600cfda54f"


def test_scan_local_library_caches_embedded_artwork(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    track_path = tmp_path / "Artist" / "Album" / "01 - Track.mp3"
    data_dir = tmp_path / "data"
    track_path.parent.mkdir(parents=True, exist_ok=True)
    track_path.write_bytes(b"test-audio")

    monkeypatch.setenv("RETREIVR_DATA_DIR", str(data_dir))
    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [tmp_path])
    monkeypatch.setattr(
        music_player,
        "MutagenFile",
        lambda _path, easy=False: _FakeAudio(
            _FakeTags(
                {
                    "TIT2": ["Track"],
                    "TPE1": ["Artist"],
                    "TALB": ["Album"],
                    "APIC:": _FakePicture(b"image-bytes"),
                }
            )
        ),
    )

    items = music_player.scan_local_library({}, limit=10)

    artwork_path = Path(items[0]["artwork_local_path"])
    assert artwork_path.is_file()
    assert artwork_path.read_bytes() == b"image-bytes"
    assert artwork_path.parent == data_dir / "artwork_cache" / "local_embedded"


def test_scan_local_library_does_not_share_flat_folder_embedded_artwork_across_albums(
    tmp_path: Path,
    monkeypatch,
) -> None:
    music_player = _load_music_player_module()
    flat_dir = tmp_path / "Singles"
    data_dir = tmp_path / "data"
    flat_dir.mkdir(parents=True, exist_ok=True)
    first = flat_dir / "Song A.m4a"
    second = flat_dir / "Song B.m4a"
    first.write_bytes(b"test-audio-a")
    second.write_bytes(b"test-audio-b")

    payloads = {
        str(first): _FakeAudio(
            _FakeTags(
                {
                    "\xa9nam": ["Song A"],
                    "\xa9ART": ["Artist A"],
                    "\xa9alb": ["Album A"],
                    "covr": [b"artist-a-art"],
                }
            )
        ),
        str(second): _FakeAudio(
            _FakeTags(
                {
                    "\xa9nam": ["Song B"],
                    "\xa9ART": ["Artist B"],
                    "\xa9alb": ["Album B"],
                    "covr": [b"artist-b-art"],
                }
            )
        ),
    }

    monkeypatch.setenv("RETREIVR_DATA_DIR", str(data_dir))
    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [tmp_path])
    monkeypatch.setattr(music_player, "MutagenFile", lambda path, easy=False: payloads[str(path)])

    items = music_player.scan_local_library({}, limit=10)

    artwork_by_artist = {item["artist"]: Path(item["artwork_local_path"]).read_bytes() for item in items}
    assert artwork_by_artist == {
        "Artist A": b"artist-a-art",
        "Artist B": b"artist-b-art",
    }


def test_scan_local_library_skips_applemusic_staging_folder(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    staging_track = tmp_path / "_AppleMusic" / "Staged Song.m4a"
    library_track = tmp_path / "Artist" / "Album" / "Library Song.m4a"
    staging_track.parent.mkdir(parents=True, exist_ok=True)
    library_track.parent.mkdir(parents=True, exist_ok=True)
    staging_track.write_bytes(b"staged-audio")
    library_track.write_bytes(b"library-audio")

    def fake_audio(path, easy=False):
        if str(path) == str(staging_track):
            return _FakeAudio(_FakeTags({"\xa9nam": ["Staged Song"], "\xa9ART": ["Wrong"], "\xa9alb": ["Wrong"]}))
        return _FakeAudio(_FakeTags({"\xa9nam": ["Library Song"], "\xa9ART": ["Artist"], "\xa9alb": ["Album"]}))

    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [tmp_path])
    monkeypatch.setattr(music_player, "MutagenFile", fake_audio)

    items = music_player.scan_local_library({}, limit=10)

    assert [item["title"] for item in items] == ["Library Song"]
    assert all("/_AppleMusic/" not in item["local_path"] for item in items)


def test_summarize_library_prefers_structured_album_artwork_over_flat_import_artwork() -> None:
    music_player = _load_music_player_module()

    summary = music_player.summarize_library(
        [
            {
                "title": "Imported",
                "artist": "Kenny Chesney",
                "artist_key": "kenny chesney",
                "album": "Songs for the Saints",
                "album_key": "songs for the saints",
                "downloaded_at": 20,
                "local_path": "/downloads/Music/_AppleMusic/Imported.m4a",
                "artwork_local_path": "/data/artwork_cache/local_embedded/cody.jpg",
            },
            {
                "title": "Structured",
                "artist": "Kenny Chesney",
                "artist_key": "kenny chesney",
                "album": "Songs for the Saints",
                "album_key": "songs for the saints",
                "downloaded_at": 10,
                "local_path": "/downloads/Music/Kenny Chesney/Songs for the Saints (2018)/Structured.m4a",
                "artwork_local_path": "/data/artwork_cache/local_embedded/kenny.jpg",
            },
        ]
    )

    assert summary["artists"][0]["artwork_local_path"].endswith("/kenny.jpg")
    assert summary["albums"][0]["artwork_local_path"].endswith("/kenny.jpg")


def test_summarize_library_excludes_applemusic_staging_rows() -> None:
    music_player = _load_music_player_module()

    summary = music_player.summarize_library(
        [
            {
                "title": "Staged",
                "artist": "Staged Artist",
                "artist_key": "staged artist",
                "album": "Staged Album",
                "album_key": "staged album",
                "downloaded_at": 20,
                "local_path": "/downloads/Music/_AppleMusic/Staged.m4a",
                "artwork_local_path": "/data/artwork_cache/local_embedded/cody.jpg",
            },
            {
                "title": "Library",
                "artist": "Library Artist",
                "artist_key": "library artist",
                "album": "Library Album",
                "album_key": "library album",
                "downloaded_at": 10,
                "local_path": "/downloads/Music/Library Artist/Library Album/Library.m4a",
                "artwork_local_path": "/data/artwork_cache/local_embedded/library.jpg",
            },
        ]
    )

    assert [item["title"] for item in summary["tracks"]] == ["Library"]
    assert [item["artist"] for item in summary["artists"]] == ["Library Artist"]
    assert [item["album"] for item in summary["albums"]] == ["Library Album"]


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
    assert item["title"] == "Song"
    assert item["recording_mbid"] is None


def test_local_player_path_check_rejects_sibling_prefix_path(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    root = tmp_path / "Music"
    sibling = tmp_path / "Music2"
    root.mkdir()
    sibling.mkdir()
    outside_track = sibling / "outside.mp3"
    outside_track.write_bytes(b"audio")

    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [root])

    assert music_player.is_local_player_path_allowed(
        {},
        outside_track,
        allowed_extensions=music_player.AUDIO_EXTENSIONS,
    ) is False


def test_scan_local_library_skips_symlink_that_resolves_outside_root(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    root = tmp_path / "Music"
    outside = tmp_path / "Outside"
    root.mkdir()
    outside.mkdir()
    outside_track = outside / "escaped.mp3"
    outside_track.write_bytes(b"audio")
    symlink_track = root / "escaped.mp3"
    try:
        symlink_track.symlink_to(outside_track)
    except OSError:
        return

    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [root])
    monkeypatch.setattr(music_player, "MutagenFile", lambda _path, easy=False: None)

    assert music_player.scan_local_library({}, limit=10) == []


def test_resolve_local_player_file_allows_real_file_under_root(tmp_path: Path, monkeypatch) -> None:
    music_player = _load_music_player_module()
    root = tmp_path / "Music"
    track = root / "Artist" / "Album" / "Song.mp3"
    track.parent.mkdir(parents=True)
    track.write_bytes(b"audio")

    monkeypatch.setattr(music_player, "_music_roots", lambda _config: [root])

    resolved = music_player.resolve_local_player_file(
        {},
        track,
        allowed_extensions=music_player.AUDIO_EXTENSIONS,
    )

    assert resolved == track.resolve()
