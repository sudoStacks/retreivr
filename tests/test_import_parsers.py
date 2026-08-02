from __future__ import annotations

import pytest

from metadata.importers.dispatcher import import_playlist


def test_m3u_basic() -> None:
    payload = """#EXTM3U
#EXTINF:123,Daft Punk - One More Time
music/daft_punk_one_more_time.mp3
""".encode("utf-8")

    intents = import_playlist(payload, "playlist.m3u")

    assert len(intents) == 1
    item = intents[0]
    assert item.source_format == "m3u"
    assert item.artist == "Daft Punk"
    assert item.title == "One More Time"
    assert item.album is None
    assert item.duration_ms == 123000


def test_csv_basic() -> None:
    payload = "artist,title,album\nTaylor Swift,Style,1989\n".encode("utf-8")

    intents = import_playlist(payload, "playlist.csv")

    assert len(intents) == 1
    item = intents[0]
    assert item.source_format == "csv"
    assert item.artist == "Taylor Swift"
    assert item.title == "Style"
    assert item.album == "1989"


def test_csv_preserves_common_export_metadata_aliases() -> None:
    payload = (
        "Track Name,Artist Name(s),Album Name,Album Artist,Track Number,Disc Number,Release Date,Genre,Duration (ms)\n"
        "Teardrop,Massive Attack,Mezzanine,Massive Attack,1,1,1998-04-20,Trip Hop,330000\n"
    ).encode("utf-8")

    item = import_playlist(payload, "service-export.csv")[0]

    assert item.title == "Teardrop"
    assert item.artist == "Massive Attack"
    assert item.album == "Mezzanine"
    assert item.album_artist == "Massive Attack"
    assert item.track_number == 1
    assert item.disc_number == 1
    assert item.release_date == "1998-04-20"
    assert item.genre == "Trip Hop"
    assert item.duration_ms == 330000


def test_apple_xml_sample() -> None:
    payload = b'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Tracks</key>
    <dict>
      <key>1</key>
      <dict>
        <key>Name</key><string>Dreams</string>
        <key>Artist</key><string>Fleetwood Mac</string>
        <key>Album Artist</key><string>Fleetwood Mac</string>
        <key>Album</key><string>Rumours</string>
        <key>Track Number</key><integer>2</integer>
        <key>Disc Number</key><integer>1</integer>
        <key>Year</key><integer>1977</integer>
        <key>Genre</key><string>Rock</string>
        <key>Total Time</key><integer>257000</integer>
      </dict>
    </dict>
  </dict>
</plist>
'''

    intents = import_playlist(payload, "library.xml")

    assert len(intents) == 1
    item = intents[0]
    assert item.source_format == "apple_xml"
    assert item.artist == "Fleetwood Mac"
    assert item.title == "Dreams"
    assert item.album == "Rumours"
    assert item.album_artist == "Fleetwood Mac"
    assert item.track_number == 2
    assert item.disc_number == 1
    assert item.release_date == "1977"
    assert item.genre == "Rock"
    assert item.duration_ms == 257000


def test_soundizz_json_sample() -> None:
    payload = b'''[
  {"artist": "Nirvana", "title": "Come As You Are", "album": "Nevermind"},
  {"artist": "Massive Attack", "title": "Teardrop"}
]'''

    intents = import_playlist(payload, "export.json")

    assert len(intents) == 2
    assert intents[0].source_format == "soundiiz_json"
    assert intents[0].artist == "Nirvana"
    assert intents[0].title == "Come As You Are"
    assert intents[0].album == "Nevermind"
    assert intents[1].artist == "Massive Attack"
    assert intents[1].title == "Teardrop"
    assert intents[1].album is None


def test_soundiiz_json_preserves_nested_and_release_metadata() -> None:
    payload = b'''{
      "tracks": [{
        "name": "Midnight City",
        "artists": [{"name": "M83"}],
        "album": {"name": "Hurry Up, We're Dreaming"},
        "albumArtist": {"name": "M83"},
        "trackNumber": 2,
        "discNumber": 1,
        "releaseDate": "2011-10-18",
        "genres": ["Synth-pop", "Electronic"],
        "durationMs": 244000
      }]
    }'''

    item = import_playlist(payload, "soundiiz.json")[0]

    assert item.title == "Midnight City"
    assert item.artist == "M83"
    assert item.album == "Hurry Up, We're Dreaming"
    assert item.album_artist == "M83"
    assert item.track_number == 2
    assert item.disc_number == 1
    assert item.release_date == "2011-10-18"
    assert item.genre == "Synth-pop, Electronic"
    assert item.duration_ms == 244000


def test_invalid_format_error() -> None:
    payload = b"not a recognized format"

    with pytest.raises(ValueError, match="unsupported playlist format"):
        import_playlist(payload, "playlist.bin")
