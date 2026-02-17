"""Canonical tagging service for runtime music metadata writes."""

from __future__ import annotations

from metadata.tagger import apply_tags as _apply_tags
from metadata.types import CanonicalMetadata


def apply_tags(
    file_path,
    tags,
    artwork,
    *,
    source_title=None,
    allow_overwrite=False,
    dry_run=False,
):
    """Pass-through for dict-based tag payloads used by metadata worker flows."""
    return _apply_tags(
        file_path,
        tags,
        artwork,
        source_title=source_title,
        allow_overwrite=allow_overwrite,
        dry_run=dry_run,
    )


def tag_file(
    path: str,
    metadata: CanonicalMetadata,
    *,
    source_title: str | None = None,
    allow_overwrite: bool = True,
    dry_run: bool = False,
) -> None:
    """Apply canonical metadata tags to a media file using the unified tagger backend."""
    tags = {
        "artist": metadata.artist,
        "album": metadata.album,
        "title": metadata.title,
        "album_artist": metadata.album_artist,
        "track_number": metadata.track_num,
        "year": metadata.date,
        "genre": metadata.genre,
        "recording_id": metadata.mbid,
        "lyrics": metadata.lyrics,
    }
    artwork = None
    if metadata.artwork:
        artwork = {
            "data": bytes(metadata.artwork),
            "mime": "image/jpeg",
        }
    _apply_tags(
        path,
        tags,
        artwork,
        source_title=source_title,
        allow_overwrite=allow_overwrite,
        dry_run=dry_run,
    )
