"""Shared music contract helpers used by both worker pipelines."""

from __future__ import annotations

import re

from metadata.types import CanonicalMetadata


def parse_first_positive_int(value):
    """Parse the first integer token from mixed values, returning None when missing."""
    if value is None:
        return None
    if isinstance(value, bool):
        parsed = int(value)
        return parsed if parsed > 0 else None
    if isinstance(value, int):
        return value if value > 0 else None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"\d+", text)
    if not match:
        return None
    try:
        parsed = int(match.group(0))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def format_zero_padded_track_number(value):
    """Format a parsed track number as zero-padded two digits, or empty when unavailable."""
    parsed = parse_first_positive_int(value)
    if parsed is None:
        return ""
    return f"{parsed:02d}"


def coerce_canonical_music_metadata(payload):
    """Coerce loose metadata payloads into CanonicalMetadata using canonical defaults."""
    if isinstance(payload, CanonicalMetadata):
        return payload
    source = payload if isinstance(payload, dict) else {}
    track_num = parse_first_positive_int(source.get("track_num") or source.get("track_number"))
    disc_num = parse_first_positive_int(source.get("disc_num") or source.get("disc_number"))
    return CanonicalMetadata(
        title=str(source.get("title") or source.get("track") or "Unknown Title"),
        artist=str(source.get("artist") or "Unknown Artist"),
        album=str(source.get("album") or "Unknown Album"),
        album_artist=str(source.get("album_artist") or source.get("artist") or "Unknown Artist"),
        track_num=track_num or 1,
        disc_num=disc_num or 1,
        date=str(source.get("date") or source.get("release_date") or "Unknown"),
        genre=str(source.get("genre") or "Unknown"),
        isrc=(str(source.get("isrc")).strip() if source.get("isrc") else None),
        mbid=(
            str(source.get("mbid") or source.get("recording_id") or source.get("mb_recording_id")).strip()
            if (source.get("mbid") or source.get("recording_id") or source.get("mb_recording_id"))
            else None
        ),
        artwork=source.get("artwork"),
        lyrics=(str(source.get("lyrics")).strip() if source.get("lyrics") else None),
    )
