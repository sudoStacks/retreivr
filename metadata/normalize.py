"""Normalization helpers for structured music metadata."""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date
from typing import Any

from metadata.types import CanonicalMetadata

logger = logging.getLogger(__name__)

_WHITESPACE_RE = re.compile(r"\s+")
_YEAR_RE = re.compile(r"^(\d{4})")
_DATE_RE = re.compile(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$")
_YEAR_MONTH_RE = re.compile(r"^(\d{4})[-/](\d{1,2})$")
_TITLE_SUFFIX_RE = re.compile(
    r"\s*(?:\((?:official audio|official video|audio)\)|\[(?:hd)\])\s*$",
    re.IGNORECASE,
)
_TOPIC_SUFFIX_RE = re.compile(r"\s*-\s*topic\s*$", re.IGNORECASE)
_TRAILING_HYPHENS_RE = re.compile(r"(?:\s*-\s*)+$")
_FEAT_SPLIT_RE = re.compile(r"^(?P<main>.+?)\s+(?:feat\.|ft\.)\s+(?P<feat>.+)$", re.IGNORECASE)
_TITLE_FEAT_RE = re.compile(r"\(\s*feat\.\s*([^)]+)\)", re.IGNORECASE)


def normalize_music_metadata(metadata: CanonicalMetadata) -> CanonicalMetadata:
    """Return a normalized copy of ``CanonicalMetadata`` without mutating the input.

    Responsibilities:
    - Normalize all string fields to Unicode NFC.
      This matters for media-library grouping because visually identical Unicode
      strings can have different binary forms; NFC avoids duplicate album/artist
      buckets caused by mixed normalization forms.
    - Strip leading/trailing whitespace.
    - Collapse repeated internal whitespace to single spaces.
    - Normalize ``track_num`` and ``disc_num`` to integers.
    - Normalize ``date`` to ``YYYY`` or ``YYYY-MM-DD`` when parseable.
    - Ensure ``album_artist`` is non-empty by falling back to ``artist``.

    The returned value is always a newly constructed ``CanonicalMetadata`` instance.
    """
    # NFC normalization is applied via _normalize_text for stable player grouping.
    title = clean_title(_normalize_text(metadata.title)) or "Unknown Title"
    artist = _normalize_text(metadata.artist) or "Unknown Artist"
    artist, title = normalize_featured_artists(artist, title)
    album = _normalize_text(metadata.album) or "Unknown Album"
    # Media players group albums by album_artist; blank/variant values fragment one album.
    album_artist_raw = _normalize_optional_text(metadata.album_artist)
    if not album_artist_raw:
        # Fallback to track artist so all tracks in the same release can group together.
        album_artist = artist
    else:
        album_artist = album_artist_raw
    # When artist fields include comma-separated collaborators, keep primary artist for grouping.
    album_artist = _primary_artist(album_artist)
    genre = _normalize_genre(metadata.genre) or "Unknown"
    normalized_date = _normalize_release_date(metadata.date) or "Unknown"

    isrc = _normalize_optional_text(metadata.isrc)
    mbid = _normalize_optional_text(metadata.mbid)
    lyrics = _normalize_optional_text(metadata.lyrics)
    artwork = bytes(metadata.artwork) if metadata.artwork is not None else None

    track_num = _normalize_positive_int(metadata.track_num, default=1)
    disc_num = _normalize_positive_int(metadata.disc_num, default=1)

    return CanonicalMetadata(
        title=title,
        artist=artist,
        album=album,
        album_artist=album_artist,
        track_num=track_num,
        disc_num=disc_num,
        date=normalized_date,
        genre=genre,
        isrc=isrc,
        mbid=mbid,
        artwork=artwork,
        lyrics=lyrics,
    )


def clean_title(title: str) -> str:
    """Return a deterministically cleaned track title.

    Cleanup rules:
    - Remove trailing ``(Official Audio)``, ``(Official Video)``, ``(Audio)``, and ``[HD]``.
    - Remove trailing ``- Topic``.
    - Remove trailing hyphen artifacts.
    - Preserve other parenthetical context such as ``(Live)``.
    """
    cleaned = _normalize_text(title)
    while True:
        updated = _TITLE_SUFFIX_RE.sub("", cleaned)
        updated = _TOPIC_SUFFIX_RE.sub("", updated)
        updated = _TRAILING_HYPHENS_RE.sub("", updated)
        updated = _normalize_text(updated) if updated else ""
        if updated == cleaned:
            break
        cleaned = updated
    return cleaned


def normalize_featured_artists(artist: str, title: str) -> tuple[str, str]:
    """Normalize featured artist credits between artist and title fields.

    If ``artist`` includes ``feat.``/``ft.`` credits, move the featured segment
    into ``title`` as ``(feat. X)`` and keep only the main artist name in
    ``artist``. Existing title feat credits are preserved and not duplicated.
    Matching is case-insensitive.
    """
    normalized_artist = _normalize_text(artist)
    normalized_title = _normalize_text(title)

    match = _FEAT_SPLIT_RE.match(normalized_artist)
    if not match:
        return normalized_artist, normalized_title

    main_artist = _normalize_text(match.group("main"))
    featured_segment = _normalize_text(match.group("feat"))
    if not featured_segment:
        return main_artist, normalized_title

    existing = {_normalize_text(item).lower() for item in _TITLE_FEAT_RE.findall(normalized_title)}
    if featured_segment.lower() in existing:
        return main_artist, normalized_title

    return main_artist, f"{normalized_title} (feat. {featured_segment})"


def _normalize_text(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", unicodedata.normalize("NFC", value).strip())


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _normalize_text(value)
    return normalized or None


def _primary_artist(value: str) -> str:
    primary = value.split(",", 1)[0]
    normalized = _normalize_text(primary)
    return normalized or value


def _normalize_genre(value: Any) -> str | None:
    if value is None:
        return None

    raw_parts: list[str]
    if isinstance(value, list):
        raw_parts = [str(part) for part in value]
    else:
        raw_parts = re.split(r"[;,]", str(value))

    seen: set[str] = set()
    ordered: list[str] = []
    for part in raw_parts:
        normalized = _normalize_text(part)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(normalized)

    if not ordered:
        return None
    return ", ".join(ordered)


def _normalize_positive_int(value: int, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _normalize_release_date(value: str) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None

    # YYYY
    if normalized.isdigit() and len(normalized) == 4:
        return normalized

    # YYYY-MM -> YYYY
    year_month_match = _YEAR_MONTH_RE.match(normalized)
    if year_month_match:
        year_s, month_s = year_month_match.groups()
        month = int(month_s)
        if 1 <= month <= 12:
            return year_s

    # YYYY-MM-DD (or slash-separated equivalent) -> YYYY-MM-DD
    match = _DATE_RE.match(normalized)
    if match:
        year_s, month_s, day_s = match.groups()
        try:
            parsed = date(int(year_s), int(month_s), int(day_s))
        except ValueError:
            return _YEAR_RE.match(normalized).group(1) if _YEAR_RE.match(normalized) else None
        return parsed.isoformat()

    # Invalid formats: strip to first 4 digits when present.
    year_match = _YEAR_RE.match(normalized)
    if year_match:
        return year_match.group(1)

    # No usable year; keep the source value but surface inconsistency.
    logger.warning("unparseable release date; preserving original value=%s", normalized)
    return normalized
