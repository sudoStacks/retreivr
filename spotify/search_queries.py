"""Deterministic search-query builders for Spotify track lookups."""

from __future__ import annotations


def build_search_query(spotify_track: dict, prefer_official: bool = True) -> str:
    """Build a deterministic search query in the form `Artist - Title {keywords}`.

    Behavior:
    - Always starts with `Artist - Title`.
    - Appends `official audio` when `prefer_official` is `True`.
    - Appends `official music video` when `prefer_official` is `False`.

    Examples:
    - `build_search_query({"artist": "Daft Punk", "title": "One More Time"})`
      -> `"Daft Punk - One More Time official audio"`
    - `build_search_query({"artist": "Daft Punk", "title": "One More Time"}, prefer_official=False)`
      -> `"Daft Punk - One More Time official music video"`
    """
    track = spotify_track or {}
    artist = _extract_artist(track) or "Unknown Artist"
    title = _extract_title(track) or "Unknown Title"
    keywords = "official audio" if prefer_official else "official music video"
    return f"{artist} - {title} {keywords}"


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
