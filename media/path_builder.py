"""Canonical music path construction utilities."""

from __future__ import annotations

from pathlib import Path

from metadata.naming import build_album_directory, build_track_filename, sanitize_component
from metadata.types import CanonicalMetadata


def sanitize_for_filesystem(value: str) -> str:
    """Return a filesystem-safe string with invalid characters removed."""
    return sanitize_component(value)


def build_music_path(root: Path, metadata: CanonicalMetadata, ext: str) -> Path:
    """Build and return a canonical music path without creating directories.

    Layout:
        Music/
          {album_artist}/
            {album} ({year})/
              Disc {disc_num}/
                {track_num:02d} - {title}.{ext}
    """
    album_artist = sanitize_for_filesystem(metadata.album_artist or metadata.artist or "Unknown Artist")
    album_folder = build_album_directory(metadata)

    disc_num_raw = getattr(metadata, "disc_num", None)
    disc_num = int(disc_num_raw) if isinstance(disc_num_raw, int) and disc_num_raw > 0 else 1

    extension = str(ext or "").lstrip(".")
    filename = build_track_filename(
        {
            "title": metadata.title,
            "track_num": metadata.track_num,
            "ext": extension,
        }
    )

    return root / "Music" / album_artist / album_folder / f"Disc {disc_num}" / filename


def ensure_parent_dir(path: Path) -> None:
    """Ensure the parent directory for a file path exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
