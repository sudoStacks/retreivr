from __future__ import annotations

from pathlib import Path

from media.path_builder import build_music_path
from metadata.types import MusicMetadata


def _metadata(**overrides) -> MusicMetadata:
    base = {
        "title": "Track Title",
        "artist": "Artist Name",
        "album": "Album Name",
        "album_artist": "Artist Name",
        "track_num": 1,
        "disc_num": 1,
        "date": "2024-01-10",
        "genre": "Pop",
    }
    base.update(overrides)
    return MusicMetadata(**base)


def test_single_disc_album_with_year() -> None:
    path = build_music_path(Path("/library"), _metadata(), "mp3")

    assert path == Path("/library/Music/Artist Name/Album Name (2024)/Disc 1/01 - Track Title.mp3")


def test_multi_disc_album() -> None:
    path = build_music_path(Path("/library"), _metadata(disc_num=2, track_num=7), "flac")

    assert path == Path("/library/Music/Artist Name/Album Name (2024)/Disc 2/07 - Track Title.flac")


def test_missing_year_omits_parentheses() -> None:
    metadata = _metadata()
    metadata.date = ""

    path = build_music_path(Path("/library"), metadata, "m4a")

    assert path == Path("/library/Music/Artist Name/Album Name/Disc 1/01 - Track Title.m4a")


def test_missing_disc_num_defaults_to_disc_1() -> None:
    metadata = _metadata()
    metadata.disc_num = None  # type: ignore[assignment]

    path = build_music_path(Path("/library"), metadata, "mp3")

    assert path == Path("/library/Music/Artist Name/Album Name (2024)/Disc 1/01 - Track Title.mp3")


def test_missing_track_num_defaults_to_00() -> None:
    metadata = _metadata()
    metadata.track_num = None  # type: ignore[assignment]

    path = build_music_path(Path("/library"), metadata, "mp3")

    assert path == Path("/library/Music/Artist Name/Album Name (2024)/Disc 1/00 - Track Title.mp3")


def test_unicode_characters_are_preserved() -> None:
    metadata = _metadata(
        album_artist="Beyoncé",
        title="Café del Mar",
        album="Été",
    )

    path = build_music_path(Path("/library"), metadata, "mp3")

    assert path == Path("/library/Music/Beyoncé/Été (2024)/Disc 1/01 - Café del Mar.mp3")


def test_invalid_filesystem_characters_are_removed() -> None:
    metadata = _metadata(
        album_artist='A<>:"/\\|?*rtist',
        album='Alb<>:"/\\|?*um',
        title='Ti<>:"/\\|?*tle',
    )

    path = build_music_path(Path("/library"), metadata, "mp3")

    assert path == Path("/library/Music/Artist/Album (2024)/Disc 1/01 - Title.mp3")
