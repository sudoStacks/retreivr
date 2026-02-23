from __future__ import annotations

from typing import Any


def _pos_or_zero(value: Any) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _strip_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def build_music_track_canonical_id(
    artist: Any,
    album: Any,
    track_number: Any,
    track: Any,
    *,
    recording_mbid: Any = None,
    mb_release_id: Any = None,
    mb_release_group_id: Any = None,
    disc_number: Any = None,
) -> str:
    """Build a deterministic canonical id for music-track dedupe across enqueue paths."""
    normalized_recording_mbid = str(recording_mbid or "").strip().lower()
    normalized_release = (
        str(mb_release_id or mb_release_group_id or "").strip().lower() or "unknown-release"
    )

    normalized_track_number = _pos_or_zero(track_number)
    normalized_disc_number = _pos_or_zero(disc_number)

    if normalized_recording_mbid:
        return (
            f"music_track:{normalized_recording_mbid}:{normalized_release}:"
            f"d{normalized_disc_number}:t{normalized_track_number}"
        )

    normalized_artist = str(artist or "").strip().lower()
    normalized_album = str(album or "").strip().lower()
    normalized_track = str(track or "").strip().lower()
    return (
        f"music_track:{normalized_artist}:{normalized_album}:"
        f"{normalized_track_number}:{normalized_track}"
    )


def extract_external_track_canonical_id(
    external_ids: Any,
    *,
    fallback_spotify_id: Any = None,
) -> str | None:
    """Extract a stable external-id canonical key with deterministic priority."""
    ids = external_ids if isinstance(external_ids, dict) else {}
    for key in ("spotify_id", "isrc", "musicbrainz_recording_id", "musicbrainz_release_id"):
        value = _strip_or_none(ids.get(key))
        if value:
            return value
    return _strip_or_none(fallback_spotify_id)
