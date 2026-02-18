"""Intent routing helpers for raw homepage input."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional
from urllib.parse import parse_qs, urlparse


class IntentType(Enum):
    SPOTIFY_ALBUM = "spotify_album"
    SPOTIFY_PLAYLIST = "spotify_playlist"
    SPOTIFY_TRACK = "spotify_track"
    SPOTIFY_ARTIST = "spotify_artist"
    YOUTUBE_PLAYLIST = "youtube_playlist"
    SEARCH = "search"


@dataclass
class Intent:
    type: IntentType
    identifier: str  # ID extracted or original search string


def detect_intent(user_input: str) -> Intent:
    """Detect intent from user input without network calls.

    Rules:
    - Detect Spotify URLs for album/playlist/track/artist.
    - Detect YouTube playlist URLs via ``list=`` query parameter.
    - Otherwise treat input as plain ``SEARCH``.
    - Extract clean IDs without query strings.
    """
    raw = (user_input or "").strip()
    if not raw:
        return Intent(type=IntentType.SEARCH, identifier="")

    spotify_album = _extract_spotify_id(raw, "album")
    if spotify_album:
        return Intent(type=IntentType.SPOTIFY_ALBUM, identifier=spotify_album)

    spotify_playlist = _extract_spotify_id(raw, "playlist")
    if spotify_playlist:
        return Intent(type=IntentType.SPOTIFY_PLAYLIST, identifier=spotify_playlist)

    spotify_track = _extract_spotify_id(raw, "track")
    if spotify_track:
        return Intent(type=IntentType.SPOTIFY_TRACK, identifier=spotify_track)

    spotify_artist = _extract_spotify_id(raw, "artist")
    if spotify_artist:
        return Intent(type=IntentType.SPOTIFY_ARTIST, identifier=spotify_artist)

    youtube_playlist = _extract_youtube_playlist_id(raw)
    if youtube_playlist:
        return Intent(type=IntentType.YOUTUBE_PLAYLIST, identifier=youtube_playlist)

    return Intent(type=IntentType.SEARCH, identifier=raw)


def _extract_spotify_id(raw: str, resource: str) -> Optional[str]:
    parsed = urlparse(raw)
    if parsed.scheme and "spotify.com" in (parsed.netloc or "").lower():
        parts = [segment for segment in (parsed.path or "").split("/") if segment]
        if len(parts) >= 2 and parts[0].lower() == resource:
            return _clean_identifier(parts[1])
    return None


def _extract_youtube_playlist_id(raw: str) -> Optional[str]:
    parsed = urlparse(raw)
    if not parsed.scheme:
        return None
    netloc = (parsed.netloc or "").lower()
    if "youtube.com" not in netloc and "youtu.be" not in netloc:
        return None
    values = parse_qs(parsed.query).get("list")
    if not values:
        return None
    return _clean_identifier(values[0])


def _clean_identifier(value: str) -> str:
    return (value or "").split("?", 1)[0].strip().strip("/")
