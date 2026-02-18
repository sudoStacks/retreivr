from __future__ import annotations

import unicodedata

from metadata.normalize import normalize_music_metadata
from metadata.types import MusicMetadata


def _metadata(**overrides) -> MusicMetadata:
    base = {
        "title": "Song",
        "artist": "Artist",
        "album": "Album",
        "album_artist": "Album Artist",
        "track_num": 1,
        "disc_num": 1,
        "date": "2024",
        "genre": "Pop",
        "isrc": "USABC1234567",
        "mbid": "mbid-1",
        "artwork": None,
        "lyrics": None,
    }
    base.update(overrides)
    return MusicMetadata(**base)


def test_title_cleanup_rules() -> None:
    metadata = _metadata(title="  Song Name (Official Audio) - Topic -  ")

    normalized = normalize_music_metadata(metadata)

    assert normalized.title == "Song Name"


def test_featured_artist_moves_into_title() -> None:
    metadata = _metadata(artist="Main Artist ft. Guest Artist", title="My Track")

    normalized = normalize_music_metadata(metadata)

    assert normalized.artist == "Main Artist"
    assert normalized.title == "My Track (feat. Guest Artist)"


def test_album_artist_fallback_and_primary_artist_grouping() -> None:
    missing_album_artist = _metadata(artist="Lead Artist")
    missing_album_artist.album_artist = ""

    normalized_missing = normalize_music_metadata(missing_album_artist)
    assert normalized_missing.album_artist == "Lead Artist"

    multi_album_artist = _metadata(album_artist="Lead Artist, Guest One, Guest Two")
    normalized_multi = normalize_music_metadata(multi_album_artist)
    assert normalized_multi.album_artist == "Lead Artist"


def test_date_normalization_cases() -> None:
    year_only = normalize_music_metadata(_metadata(date="2024"))
    year_month = normalize_music_metadata(_metadata(date="2024-07"))
    full_date = normalize_music_metadata(_metadata(date="2024-07-09"))
    invalid_with_year = normalize_music_metadata(_metadata(date="2024-99-99"))
    invalid_no_year = normalize_music_metadata(_metadata(date="Unknown date string"))

    assert year_only.date == "2024"
    assert year_month.date == "2024"
    assert full_date.date == "2024-07-09"
    assert invalid_with_year.date == "2024"
    assert invalid_no_year.date == "Unknown date string"


def test_genre_deduplication_and_casing_from_first_occurrence() -> None:
    metadata = _metadata(genre=" Pop ; pop, ROCK, Rock , Jazz ")

    normalized = normalize_music_metadata(metadata)

    assert normalized.genre == "Pop, ROCK, Jazz"


def test_unicode_normalization_nfc_applies_to_core_grouping_fields() -> None:
    decomposed = "Cafe\u0301"
    metadata = _metadata(
        title=f"{decomposed} Song",
        artist=decomposed,
        album=decomposed,
        album_artist=decomposed,
        genre=decomposed,
    )

    normalized = normalize_music_metadata(metadata)

    expected = unicodedata.normalize("NFC", decomposed)
    assert expected == "Café"
    assert normalized.title == "Café Song"
    assert normalized.artist == "Café"
    assert normalized.album == "Café"
    assert normalized.album_artist == "Café"
    assert normalized.genre == "Café"
