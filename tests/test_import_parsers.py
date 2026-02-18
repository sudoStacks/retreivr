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


def test_csv_basic() -> None:
    payload = "artist,title,album\nTaylor Swift,Style,1989\n".encode("utf-8")

    intents = import_playlist(payload, "playlist.csv")

    assert len(intents) == 1
    item = intents[0]
    assert item.source_format == "csv"
    assert item.artist == "Taylor Swift"
    assert item.title == "Style"
    assert item.album == "1989"


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
        <key>Album</key><string>Rumours</string>
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


def test_invalid_format_error() -> None:
    payload = b"not a recognized format"

    with pytest.raises(ValueError, match="unsupported playlist format"):
        import_playlist(payload, "playlist.bin")
