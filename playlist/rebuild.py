"""Playlist rebuild helpers."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator

from playlist.export import write_m3u


def rebuild_playlist_from_tracks(
    playlist_name: str,
    playlist_root: Path,
    music_root: Path,
    track_file_paths: Iterable[str],
) -> Path:
    """Rebuild a playlist M3U file from canonical track file paths.

    Args:
        playlist_name: Playlist display name used to derive M3U filename.
        playlist_root: Directory where the resulting M3U file is stored.
        music_root: Root directory used for relative path entries.
        track_file_paths: Absolute canonical file paths loaded from storage.

    Returns:
        Final path to the rebuilt M3U file.
    """
    normalized_playlist_name = str(playlist_name or "").strip() or "playlist"
    track_paths = [Path(path) for path in track_file_paths if str(path).strip()]
    with _music_root_env(music_root):
        return write_m3u(
            playlist_root=playlist_root,
            playlist_name=normalized_playlist_name,
            track_paths=track_paths,
        )


@contextmanager
def _music_root_env(music_root: Path) -> Iterator[None]:
    previous = os.environ.get("RETREIVR_MUSIC_ROOT")
    os.environ["RETREIVR_MUSIC_ROOT"] = str(music_root)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("RETREIVR_MUSIC_ROOT", None)
        else:
            os.environ["RETREIVR_MUSIC_ROOT"] = previous
