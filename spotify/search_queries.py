"""Deterministic search-query builders for Spotify track lookups."""

from __future__ import annotations


def build_search_query(spotify_track: dict) -> str:
    """Build a deterministic query in the format `Artist - Title official audio`."""
    track = spotify_track or {}
    artist = _extract_artist(track) or "Unknown Artist"
    title = _extract_title(track) or "Unknown Title"
    return f"{artist} - {title} official audio"


def _extract_artist(track: dict) -> str | None:
    artists = track.get("artists")
    if isinstance(artists, list):
        names = []
        for entry in artists:
            if isinstance(entry, dict):
                name = entry.get("name")
            else:
                name = entry
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
        if names:
            return ", ".join(names)
    artist = track.get("artist")
    if isinstance(artist, str) and artist.strip():
        return artist.strip()
    return None


def _extract_title(track: dict) -> str | None:
    for key in ("title", "name", "track"):
        value = track.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None

