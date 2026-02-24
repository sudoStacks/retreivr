from __future__ import annotations

from media.music_contract import (
    coerce_canonical_music_metadata,
    format_zero_padded_track_number,
    parse_first_positive_int,
)


def test_parse_first_positive_int_handles_mixed_values() -> None:
    assert parse_first_positive_int(None) is None
    assert parse_first_positive_int("") is None
    assert parse_first_positive_int("Disc 1") == 1
    assert parse_first_positive_int("01/10") == 1
    assert parse_first_positive_int(7) == 7
    assert parse_first_positive_int("no digits") is None


def test_format_zero_padded_track_number() -> None:
    assert format_zero_padded_track_number("1") == "01"
    assert format_zero_padded_track_number("Disc 2") == "02"
    assert format_zero_padded_track_number(None) == ""


def test_coerce_canonical_music_metadata_from_loose_payload() -> None:
    metadata = coerce_canonical_music_metadata(
        {
            "track": "Song Name",
            "artist": "Lead Artist",
            "album": "Album Name",
            "track_number": "03/10",
            "disc_number": "1/2",
            "release_date": "2024-05-01",
            "recording_id": "rec-123",
        }
    )
    assert metadata.title == "Song Name"
    assert metadata.artist == "Lead Artist"
    assert metadata.album_artist == "Lead Artist"
    assert metadata.track_num == 3
    assert metadata.disc_num == 1
    assert metadata.date == "2024-05-01"
    assert metadata.mbid == "rec-123"
