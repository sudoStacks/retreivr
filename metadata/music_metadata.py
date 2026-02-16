"""Deterministic music metadata model and merge helpers."""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

_LOG = logging.getLogger(__name__)
_FS_FORBIDDEN_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
_WHITESPACE_RE = re.compile(r"\s+")
_FEAT_RE = re.compile(r"\s+(?:feat\.?|featuring|ft\.?)\s+", re.IGNORECASE)


@dataclass(frozen=True)
class MusicMetadata:
    """Normalized metadata ready for tags and filesystem-safe naming."""

    title: str | None
    artist: str | None
    album: str | None
    album_artist: str | None
    track_num: int | None
    disc_num: int | None
    date: str | None
    genre: str | None
    isrc: str | None
    mbid: str | None
    artwork: str | None
    lyrics: str | None


def merge_metadata(
    spotify_data: dict[str, Any] | None,
    mb_data: dict[str, Any] | None,
    ytdlp_data: dict[str, Any] | None,
) -> MusicMetadata:
    """Merge Spotify, MusicBrainz, and yt-dlp metadata using deterministic precedence."""
    spotify = spotify_data or {}
    musicbrainz = mb_data or {}
    ytdlp = ytdlp_data or {}

    def pick(field_name: str, resolver) -> Any:
        for source_name, source_data in (
            ("spotify", spotify),
            ("musicbrainz", musicbrainz),
            ("ytdlp", ytdlp),
        ):
            value = resolver(source_data)
            if _has_value(value):
                _LOG.info("metadata_field_source field=%s source=%s", field_name, source_name)
                return value
        _LOG.info("metadata_field_source field=%s source=missing", field_name)
        return None

    metadata = MusicMetadata(
        title=pick("title", _resolve_title),
        artist=pick("artist", _resolve_artist),
        album=pick("album", _resolve_album),
        album_artist=pick("album_artist", _resolve_album_artist),
        track_num=pick("track_num", _resolve_track_num),
        disc_num=pick("disc_num", _resolve_disc_num),
        date=pick("date", _resolve_date),
        genre=pick("genre", _resolve_genre),
        isrc=pick("isrc", _resolve_isrc),
        mbid=pick("mbid", _resolve_mbid),
        artwork=pick("artwork", _resolve_artwork),
        lyrics=pick("lyrics", _resolve_lyrics),
    )
    return metadata


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value))
    text = "".join(ch for ch in text if ord(ch) >= 32)
    text = _FS_FORBIDDEN_CHARS_RE.sub("-", text)
    text = _WHITESPACE_RE.sub(" ", text).strip(" .")
    return text or None


def _parse_artist_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        names = [_normalize_string(v.get("name") if isinstance(v, dict) else v) for v in value]
        names = [name for name in names if name]
        if not names:
            return None
        if len(names) == 1:
            return names[0]
        return f"{names[0]} feat. {', '.join(names[1:])}"

    text = _normalize_string(value)
    if not text:
        return None
    parts = _FEAT_RE.split(text, maxsplit=1)
    main = _normalize_string(parts[0])
    if len(parts) == 1:
        return main
    featured_raw = parts[1]
    featured_split = re.split(r"\s*(?:,|&| and | x )\s*", featured_raw, flags=re.IGNORECASE)
    featured = [_normalize_string(name) for name in featured_split]
    featured = [name for name in featured if name]
    if not featured:
        return main
    return f"{main} feat. {', '.join(featured)}"


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    match = re.match(r"^(\d+)", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _resolve_title(data: dict[str, Any]) -> str | None:
    return _normalize_string(data.get("title") or data.get("track"))


def _resolve_artist(data: dict[str, Any]) -> str | None:
    artists = data.get("artists")
    if artists is not None:
        parsed = _parse_artist_text(artists)
        if parsed:
            return parsed
    return _parse_artist_text(data.get("artist"))


def _resolve_album(data: dict[str, Any]) -> str | None:
    return _normalize_string(data.get("album"))


def _resolve_album_artist(data: dict[str, Any]) -> str | None:
    explicit = _parse_artist_text(data.get("album_artist"))
    if explicit:
        return explicit
    return _resolve_artist(data)


def _resolve_track_num(data: dict[str, Any]) -> int | None:
    return _parse_int(data.get("track_num") or data.get("track_number"))


def _resolve_disc_num(data: dict[str, Any]) -> int | None:
    return _parse_int(data.get("disc_num") or data.get("disc_number"))


def _resolve_date(data: dict[str, Any]) -> str | None:
    return _normalize_string(data.get("date") or data.get("release_date") or data.get("year"))


def _resolve_genre(data: dict[str, Any]) -> str | None:
    genre = data.get("genre")
    if isinstance(genre, list):
        normalized = [_normalize_string(entry) for entry in genre]
        values = [entry for entry in normalized if entry]
        if not values:
            return None
        return "; ".join(values)
    return _normalize_string(genre)


def _resolve_isrc(data: dict[str, Any]) -> str | None:
    value = data.get("isrc")
    return _normalize_string(value).upper() if value else None


def _resolve_mbid(data: dict[str, Any]) -> str | None:
    return _normalize_string(
        data.get("mbid")
        or data.get("musicbrainz_recording_id")
        or data.get("recording_id")
        or data.get("musicbrainz_release_id")
    )


def _resolve_artwork(data: dict[str, Any]) -> str | None:
    artwork = data.get("artwork") or data.get("artwork_url") or data.get("thumbnail")
    if isinstance(artwork, dict):
        return _normalize_string(artwork.get("url"))
    if artwork:
        return _normalize_string(artwork)
    thumbs = data.get("thumbnails")
    if isinstance(thumbs, list):
        for thumb in thumbs:
            if isinstance(thumb, dict):
                url = _normalize_string(thumb.get("url"))
                if url:
                    return url
    return None


def _resolve_lyrics(data: dict[str, Any]) -> str | None:
    return _normalize_string(data.get("lyrics"))

