"""Playlist export helpers."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

_INVALID_FS_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
_MULTISPACE_RE = re.compile(r"\s+")
_DEFAULT_MUSIC_ROOT = Path("Music")


def write_m3u(playlist_root: Path, playlist_name: str, track_paths: Iterable[Path]) -> Path:
    """Create or overwrite an M3U playlist file.

    Rules:
    - Playlist files live under ``playlist_root``.
    - Filename format is ``{playlist_name}.m3u``.
    - Paths are written relative to configured music root.
    - Missing tracks are skipped.
    - Writes are atomic (temp file then replace).
    """
    root = Path(playlist_root)
    root.mkdir(parents=True, exist_ok=True)

    safe_name = sanitize_playlist_name(playlist_name) or "playlist"
    target_path = root / f"{safe_name}.m3u"
    temp_path = root / f".{safe_name}.m3u.tmp"

    music_root = _configured_music_root().resolve()
    lines: list[str] = ["#EXTM3U"]
    for track_path in track_paths:
        candidate = Path(track_path)
        if not candidate.exists():
            continue
        try:
            rel_path = candidate.resolve().relative_to(music_root)
        except ValueError:
            continue
        lines.append(rel_path.as_posix())

    content = "\n".join(lines) + "\n"
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(target_path)
    return target_path


def _configured_music_root() -> Path:
    value = (os.environ.get("RETREIVR_MUSIC_ROOT") or "").strip()
    if value:
        return Path(value)
    return _DEFAULT_MUSIC_ROOT


def sanitize_playlist_name(name: str) -> str:
    """Return a filesystem-safe playlist name."""
    value = name
    text = _INVALID_FS_CHARS_RE.sub("", str(value))
    text = _MULTISPACE_RE.sub(" ", text).strip()
    return text.rstrip(" .")
