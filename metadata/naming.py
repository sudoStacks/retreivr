"""Canonical music naming helpers used by runtime path construction."""

from __future__ import annotations

import re
from typing import Any

_INVALID_FS_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
_MULTISPACE_RE = re.compile(r"\s+")


def _get_field(metadata: Any, field: str, default: Any = None) -> Any:
    if isinstance(metadata, dict):
        return metadata.get(field, default)
    return getattr(metadata, field, default)


def sanitize_component(text: Any) -> str:
    """Return an OS-safe filesystem component with stable fallback."""
    sanitized = _INVALID_FS_CHARS_RE.sub("", str(text or ""))
    sanitized = _MULTISPACE_RE.sub(" ", sanitized).strip()
    sanitized = sanitized.rstrip(" .")
    return sanitized or "Unknown"


def build_album_directory(metadata: Any) -> str:
    """Build canonical album directory name, including year when available."""
    album = sanitize_component(_get_field(metadata, "album") or "Unknown Album")
    date_value = str(_get_field(metadata, "date") or "").strip()
    year = date_value[:4] if len(date_value) >= 4 and date_value[:4].isdigit() else ""
    return f"{album} ({year})" if year else album


def build_track_filename(metadata: Any) -> str:
    """Build canonical track filename with track number for filesystem sorting."""
    title = sanitize_component(_get_field(metadata, "title") or "Unknown Title")
    track_num = _get_field(metadata, "track_num") or _get_field(metadata, "track_number")
    ext = str(_get_field(metadata, "ext") or "").lstrip(".")
    try:
        track_int = int(str(track_num).split("/", 1)[0])
    except (TypeError, ValueError):
        track_int = 0
    prefix = f"{track_int:02d} - " if track_int > 0 else ""
    filename = f"{prefix}{title}"
    if ext:
        return f"{filename}.{ext}"
    return filename
