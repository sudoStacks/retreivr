"""Post-finalization music export helpers."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


def _normalize_export_targets(config: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(config, dict):
        return []
    music_cfg = config.get("music")
    if not isinstance(music_cfg, dict):
        return []
    exports = music_cfg.get("exports")
    if not isinstance(exports, list):
        return []
    return [item for item in exports if isinstance(item, dict)]


def _extract_metadata_args(track_metadata: dict[str, Any] | None) -> list[str]:
    if not isinstance(track_metadata, dict):
        return []
    mapping = {
        "title": track_metadata.get("title") or track_metadata.get("track"),
        "artist": track_metadata.get("artist"),
        "album": track_metadata.get("album"),
        "album_artist": track_metadata.get("album_artist"),
        "genre": track_metadata.get("genre"),
        "date": track_metadata.get("release_date") or track_metadata.get("date") or track_metadata.get("year"),
    }
    args: list[str] = []
    for key, value in mapping.items():
        text = str(value or "").strip()
        if text:
            args.extend(["-metadata", f"{key}={text}"])
    track_number = track_metadata.get("track_number") or track_metadata.get("track_num")
    disc_number = track_metadata.get("disc_number") or track_metadata.get("disc_num")
    if track_number is not None and str(track_number).strip():
        args.extend(["-metadata", f"track={str(track_number).strip()}"])
    if disc_number is not None and str(disc_number).strip():
        args.extend(["-metadata", f"disc={str(disc_number).strip()}"])
    return args


def run_music_exports(canonical_file_path: str, track_metadata: dict, config: dict) -> dict:
    """Execute enabled export targets for a finalized canonical music file."""
    canonical_path = Path(str(canonical_file_path or "").strip())
    if not canonical_path.exists() or not canonical_path.is_file():
        raise FileNotFoundError(f"canonical music file not found: {canonical_file_path}")

    results: dict[str, dict[str, Any]] = {}
    for export in _normalize_export_targets(config):
        if not bool(export.get("enabled")):
            continue

        name = str(export.get("name") or "").strip()
        export_type = str(export.get("type") or "").strip().lower()
        export_root = str(export.get("path") or "").strip()
        if not name or not export_root:
            continue

        try:
            destination_dir = Path(export_root)
            destination_dir.mkdir(parents=True, exist_ok=True)
            if export_type == "copy":
                destination = destination_dir / canonical_path.name
                shutil.copy2(canonical_path, destination)
                results[name] = {"status": "copied", "path": str(destination)}
                continue

            if export_type == "transcode":
                destination = destination_dir / f"{canonical_path.stem}.m4a"
                codec = str(export.get("codec") or "aac").strip() or "aac"
                bitrate = str(export.get("bitrate") or "256k").strip() or "256k"
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(canonical_path),
                    "-map_metadata",
                    "0",
                    "-vn",
                    "-c:a",
                    codec,
                    "-b:a",
                    bitrate,
                    *(_extract_metadata_args(track_metadata)),
                    str(destination),
                ]
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                results[name] = {
                    "status": "transcoded",
                    "path": str(destination),
                    "codec": codec,
                    "bitrate": bitrate,
                    "container": "m4a",
                }
                continue

            results[name] = {"status": "failed", "error": f"unsupported_export_type:{export_type or 'missing'}"}
        except Exception as exc:
            logger.warning("music_export_failed name=%s canonical=%s error=%s", name, canonical_path, exc)
            results[name] = {"status": "failed", "error": str(exc) or "export_failed"}
    return results
