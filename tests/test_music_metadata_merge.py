from __future__ import annotations

from metadata.music_metadata import merge_metadata


def test_merge_metadata_precedence_and_normalization(caplog) -> None:
    spotify = {
        "title": '  Song:/Name*  ',
        "artists": [{"name": "Main Artist"}, {"name": "Guest One"}],
        "album": " Album  ",
        "album_artist": " Main Artist ",
        "track_number": "03/10",
        "disc_number": "1/2",
        "release_date": "2025-01-01",
        "genre": [" Pop ", "Dance "],
        "isrc": "usabc1234567",
        "artwork_url": "https://img.example/cover.jpg",
    }
    mb = {
        "title": "MB Title",
        "artist": "MB Artist",
        "mbid": "mbid-1",
        "lyrics": "MB lyrics",
    }
    ytdlp = {
        "title": "YT Title",
        "artist": "YT Artist",
        "lyrics": "YT lyrics",
    }

    with caplog.at_level("INFO"):
        merged = merge_metadata(spotify, mb, ytdlp)

    assert merged.title == "Song--Name-"
    assert merged.artist == "Main Artist feat. Guest One"
    assert merged.album == "Album"
    assert merged.album_artist == "Main Artist"
    assert merged.track_num == 3
    assert merged.disc_num == 1
    assert merged.date == "2025-01-01"
    assert merged.genre == "Pop; Dance"
    assert merged.isrc == "USABC1234567"
    assert merged.mbid == "mbid-1"
    assert merged.artwork == "https---img.example-cover.jpg"
    assert merged.lyrics == "MB lyrics"

    # Verify source logging happens per merged field.
    field_logs = [r.message for r in caplog.records if "metadata_field_source field=" in r.message]
    assert len(field_logs) == 12
    assert any("field=title source=spotify" in msg for msg in field_logs)
    assert any("field=mbid source=musicbrainz" in msg for msg in field_logs)
    assert any("field=lyrics source=musicbrainz" in msg for msg in field_logs)


def test_merge_metadata_fallback_and_featured_artist_parsing() -> None:
    spotify = {
        "title": "",
        "artists": [],
        "album": None,
        "album_artist": None,
    }
    mb = {
        "title": None,
        "artist": "",
        "album": "",
        "album_artist": "",
        "genre": "",
    }
    ytdlp = {
        "title": " Live  Track ",
        "artist": "Lead Artist ft. Guest A & Guest B",
        "album": "YT Album",
        "album_artist": "Lead Artist",
        "date": "2024",
        "genre": "Alt / Rock",
        "isrc": "gbxyz7654321",
        "recording_id": "mb-recording-xyz",
        "thumbnail": "https://cdn.example/a:b.jpg",
        "lyrics": " line1 \n line2 ",
    }

    merged = merge_metadata(spotify, mb, ytdlp)

    assert merged.title == "Live Track"
    assert merged.artist == "Lead Artist feat. Guest A, Guest B"
    assert merged.album == "YT Album"
    assert merged.album_artist == "Lead Artist"
    assert merged.date == "2024"
    assert merged.genre == "Alt - Rock"
    assert merged.isrc == "GBXYZ7654321"
    assert merged.mbid == "mb-recording-xyz"
    assert merged.artwork == "https---cdn.example-a-b.jpg"
    assert merged.lyrics == "line1 line2"

