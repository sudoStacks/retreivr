"""Canonical music path construction utilities."""

from __future__ import annotations

from pathlib import Path

from metadata.naming import build_album_directory, build_track_filename, sanitize_component
from metadata.types import CanonicalMetadata


def sanitize_for_filesystem(value: str) -> str:
    """Return a filesystem-safe string with invalid characters removed."""
    return sanitize_component(value)


def build_music_relative_layout(
    *,
    album_artist: str,
    album_folder: str,
    track_label: str,
    disc_number: int | None = None,
    disc_total: int | None = None,
) -> str:
    """Build the canonical relative music layout from normalized components."""
    safe_album_artist = sanitize_for_filesystem(album_artist or "") or "Unknown Artist"
    safe_album_folder = sanitize_for_filesystem(album_folder or "") or "Unknown Album"
    safe_track_label = str(track_label or "").strip() or "00 - media"
    safe_disc_number = int(disc_number) if isinstance(disc_number, int) and disc_number > 0 else 1
    safe_disc_total = int(disc_total) if isinstance(disc_total, int) and disc_total > 0 else None

    segments = ["Music", safe_album_artist, safe_album_folder]
    include_disc_folder = bool((safe_disc_total and safe_disc_total > 1) or safe_disc_number > 1)
    if include_disc_folder:
        segments.append(f"Disc {safe_disc_number}")
    segments.append(safe_track_label)
    return "/".join(segments)


def resolve_music_root_path(payload: dict) -> Path:
    """Resolve a base root path for canonical music placement from payload/config values."""
    config = payload.get("config") if isinstance(payload, dict) else None
    root_value = (
        payload.get("music_root")
        or payload.get("destination")
        or payload.get("destination_dir")
        or payload.get("output_dir")
        or (config.get("music_download_folder") if isinstance(config, dict) else None)
        or "."
    )
    root = Path(str(root_value))
    # Canonical layout builders already include a leading "Music/" segment.
    if root.name.lower() == "music":
        return root.parent if str(root.parent) != "" else Path(".")
    return root


def build_music_path(root: Path, metadata: CanonicalMetadata, ext: str) -> Path:
    """Build and return a canonical music path without creating directories.

    Layout:
        Music/
          {album_artist}/
            {album} ({year})/
              [Disc {disc_num}/ only when multi-disc]
                {track_num:02d} - {title}.{ext}
    """
    album_artist = sanitize_for_filesystem(metadata.album_artist or metadata.artist or "Unknown Artist")
    album_folder = build_album_directory(metadata)

    disc_num_raw = getattr(metadata, "disc_num", None)
    disc_num = int(disc_num_raw) if isinstance(disc_num_raw, int) and disc_num_raw > 0 else 1
    disc_total_raw = getattr(metadata, "disc_total", None)
    disc_total = int(disc_total_raw) if isinstance(disc_total_raw, int) and disc_total_raw > 0 else None

    extension = str(ext or "").lstrip(".")
    filename = build_track_filename(
        {
            "title": metadata.title,
            "track_num": metadata.track_num,
            "ext": extension,
        }
    )

    relative_layout = build_music_relative_layout(
        album_artist=album_artist,
        album_folder=album_folder,
        track_label=filename,
        disc_number=disc_num,
        disc_total=disc_total,
    )
    return root / Path(relative_layout)


def ensure_parent_dir(path: Path) -> None:
    """Ensure the parent directory for a file path exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
