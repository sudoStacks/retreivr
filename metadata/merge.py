"""Metadata merge logic for Spotify, MusicBrainz, and yt-dlp sources."""

from __future__ import annotations

import logging
import re
from typing import Any

from metadata.types import MusicMetadata

_LOG = logging.getLogger(__name__)
_WS_RE = re.compile(r"\s+")
_TITLE_SPLIT_RE = re.compile(r"([\s\-\(\)\[\]/:&])")
_LOWER_WORDS = {"a", "an", "and", "as", "at", "by", "for", "in", "of", "on", "or", "the", "to", "vs"}


def merge_metadata(spotify: dict, mb: dict, ytdlp: dict) -> MusicMetadata:
    """Merge metadata with precedence Spotify -> MusicBrainz -> yt-dlp and normalized outputs."""
    sp = spotify or {}
    mbd = mb or {}
    ytd = ytdlp or {}

    def pick(field: str, extractor) -> tuple[Any, str]:
        for source_name, source in (("spotify", sp), ("musicbrainz", mbd), ("ytdlp", ytd)):
            value = extractor(source)
            if _has_value(value):
                _LOG.info("metadata_field_source field=%s source=%s", field, source_name)
                return value, source_name
        _LOG.info("metadata_field_source field=%s source=missing", field)
        return None, "missing"

    title, _ = pick("title", lambda s: s.get("title") or s.get("track"))
    artist, _ = pick("artist", lambda s: s.get("artist"))
    album, _ = pick("album", lambda s: s.get("album"))
    album_artist, _ = pick("album_artist", lambda s: s.get("album_artist"))
    track_num, _ = pick("track_num", lambda s: s.get("track_num") or s.get("track_number"))
    disc_num, _ = pick("disc_num", lambda s: s.get("disc_num") or s.get("disc_number"))
    date, _ = pick("date", lambda s: s.get("date") or s.get("release_date") or s.get("year"))
    genre, _ = pick("genre", lambda s: s.get("genre"))
    isrc, _ = pick("isrc", lambda s: s.get("isrc"))
    mbid, _ = pick(
        "mbid",
        lambda s: s.get("mbid") or s.get("recording_id") or s.get("musicbrainz_recording_id"),
    )
    artwork, _ = pick("artwork", lambda s: s.get("artwork"))
    lyrics, _ = pick("lyrics", lambda s: s.get("lyrics"))

    return MusicMetadata(
        title=_normalize_title(title) or "Unknown Title",
        artist=_normalize_string(artist) or "Unknown Artist",
        album=_normalize_title(album) or "Unknown Album",
        album_artist=_normalize_string(album_artist) or _normalize_string(artist) or "Unknown Artist",
        track_num=_parse_positive_int(track_num, default=1),
        disc_num=_parse_positive_int(disc_num, default=1),
        date=_normalize_string(date) or "Unknown",
        genre=_normalize_title(_genre_to_string(genre)) or "Unknown",
        isrc=_normalize_string(isrc),
        mbid=_normalize_string(mbid),
        artwork=_coerce_artwork_bytes(artwork),
        lyrics=_normalize_string(lyrics),
    )


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (bytes, bytearray)):
        return len(value) > 0
    if isinstance(value, list):
        return len(value) > 0
    return True


def _normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    text = _WS_RE.sub(" ", str(value)).strip()
    return text or None


def _normalize_title(value: Any) -> str | None:
    base = _normalize_string(value)
    if not base:
        return None
    parts = _TITLE_SPLIT_RE.split(base)
    out: list[str] = []
    major_seen = False
    for token in parts:
        if not token:
            continue
        if _TITLE_SPLIT_RE.fullmatch(token):
            out.append(token)
            continue
        lower = token.lower()
        if major_seen and lower in _LOWER_WORDS:
            out.append(lower)
        elif token.isupper() and len(token) > 1:
            out.append(token)
        else:
            out.append(token[:1].upper() + token[1:].lower())
        major_seen = True
    return "".join(out)


def _parse_positive_int(value: Any, *, default: int) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    if "/" in text:
        text = text.split("/", 1)[0].strip()
    try:
        parsed = int(text)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _genre_to_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        parts = [_normalize_string(v) for v in value]
        cleaned = [p for p in parts if p]
        return ", ".join(cleaned) if cleaned else None
    return _normalize_string(value)


def _coerce_artwork_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value or None
    if isinstance(value, bytearray):
        data = bytes(value)
        return data or None
    return None

