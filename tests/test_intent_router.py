from __future__ import annotations

from input.intent_router import IntentType, detect_intent


def test_detect_spotify_album_with_query_string() -> None:
    intent = detect_intent("https://open.spotify.com/album/1A2B3C4D5E?si=abc123")
    assert intent.type == IntentType.SPOTIFY_ALBUM
    assert intent.identifier == "1A2B3C4D5E"


def test_detect_spotify_playlist_url() -> None:
    intent = detect_intent("https://open.spotify.com/playlist/37i9dQZF1DX1lVhptIYRda")
    assert intent.type == IntentType.SPOTIFY_PLAYLIST
    assert intent.identifier == "37i9dQZF1DX1lVhptIYRda"


def test_detect_spotify_track_url() -> None:
    intent = detect_intent("https://open.spotify.com/track/6rqhFgbbKwnb9MLmUQDhG6")
    assert intent.type == IntentType.SPOTIFY_TRACK
    assert intent.identifier == "6rqhFgbbKwnb9MLmUQDhG6"


def test_detect_spotify_artist_url() -> None:
    intent = detect_intent("https://open.spotify.com/artist/1dfeR4HaWDbWqFHLkxsg1d")
    assert intent.type == IntentType.SPOTIFY_ARTIST
    assert intent.identifier == "1dfeR4HaWDbWqFHLkxsg1d"


def test_detect_youtube_playlist_url() -> None:
    intent = detect_intent("https://www.youtube.com/watch?v=abc123&list=PL1234567890XYZ")
    assert intent.type == IntentType.YOUTUBE_PLAYLIST
    assert intent.identifier == "PL1234567890XYZ"


def test_detect_plain_text_search() -> None:
    intent = detect_intent("best synthwave tracks")
    assert intent.type == IntentType.SEARCH
    assert intent.identifier == "best synthwave tracks"


def test_detect_malformed_url_falls_back_to_search() -> None:
    intent = detect_intent("https://open.spotify.com/album")
    assert intent.type == IntentType.SEARCH
    assert intent.identifier == "https://open.spotify.com/album"
