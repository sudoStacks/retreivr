"""Canonical music path construction utilities."""

from __future__ import annotations

import re
from pathlib import Path

from metadata.types import MusicMetadata

_INVALID_FS_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
_MULTISPACE_RE = re.compile(r"\s+")


def sanitize_for_filesystem(value: str) -> str:
    """Return a filesystem-safe string with invalid characters removed."""
    sanitized = _INVALID_FS_CHARS_RE.sub("", str(value))
    sanitized = _MULTISPACE_RE.sub(" ", sanitized).strip()
    sanitized = sanitized.rstrip(" .")
    return sanitized or "Unknown"


def build_music_path(root: Path, metadata: MusicMetadata, ext: str) -> Path:
    """Build and return a canonical music path without creating directories.

    Layout:
        Music/
          {album_artist}/
            {album} ({year})/
              Disc {disc_num}/
                {track_num:02d} - {title}.{ext}
    """
    album_artist = sanitize_for_filesystem(metadata.album_artist or metadata.artist or "Unknown Artist")
    album = sanitize_for_filesystem(metadata.album or "Unknown Album")
    title = sanitize_for_filesystem(metadata.title or "Unknown Title")

    date_value = (metadata.date or "").strip()
    year = date_value[:4] if len(date_value) >= 4 and date_value[:4].isdigit() else ""
    album_folder = f"{album} ({year})" if year else album

    track_num_raw = getattr(metadata, "track_num", None)
    track_num = int(track_num_raw) if isinstance(track_num_raw, int) else 0
    if track_num < 0:
        track_num = 0

    disc_num_raw = getattr(metadata, "disc_num", None)
    disc_num = int(disc_num_raw) if isinstance(disc_num_raw, int) and disc_num_raw > 0 else 1

    extension = str(ext or "").lstrip(".")
    filename = f"{track_num:02d} - {title}"
    if extension:
        filename = f"{filename}.{extension}"

    return root / "Music" / album_artist / album_folder / f"Disc {disc_num}" / filename


def ensure_parent_dir(path: Path) -> None:
    """Ensure the parent directory for a file path exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
