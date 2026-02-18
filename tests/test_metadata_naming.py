from __future__ import annotations

from metadata.naming import build_album_directory, build_track_filename, sanitize_component


def test_sanitize_component_strips_unsafe_chars_and_trailing_dot_space() -> None:
    assert sanitize_component('  A<>:"/\\|?*rtist.  ') == "Artist"


def test_build_track_filename_zero_pads_track_number() -> None:
    filename = build_track_filename({"title": "Song", "track_num": 7, "ext": "mp3"})
    assert filename == "07 - Song.mp3"


def test_build_track_filename_missing_track_number_defaults_to_00() -> None:
    filename = build_track_filename({"title": "Song", "track_num": None, "ext": "flac"})
    assert filename == "00 - Song.flac"


def test_build_album_directory_missing_album_and_year_fields() -> None:
    album_dir = build_album_directory({"album": None, "date": ""})
    assert album_dir == "Unknown Album"
