"""Download worker behavior for resolved Spotify media jobs."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Optional, Protocol

from config.settings import ENABLE_DURATION_VALIDATION, SPOTIFY_DURATION_TOLERANCE_SECONDS
from db.downloaded_tracks import record_downloaded_track
from media.ffprobe import get_media_duration
from media.music_contract import coerce_canonical_music_metadata, parse_first_positive_int
from media.path_builder import build_music_relative_layout, ensure_parent_dir, resolve_music_root_path
from media.validation import validate_duration
from metadata.naming import build_album_directory, build_track_filename
from metadata.normalize import normalize_music_metadata
from metadata.tagging_service import tag_file
from metadata.types import CanonicalMetadata

logger = logging.getLogger(__name__)

JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELLED = "cancelled"
JOB_STATUS_VALIDATION_FAILED = "validation_failed"
JOB_ALLOWED_STATUSES = {
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
    JOB_STATUS_CANCELLED,
    JOB_STATUS_VALIDATION_FAILED,
}


class _Downloader(Protocol):
    def download(self, media_url: str) -> str:
        """Download a media URL and return the local file path."""


class DownloadWorker:
    """Worker that downloads media and applies optional music metadata tagging."""

    def __init__(self, downloader: _Downloader) -> None:
        self._downloader = downloader

    def process_job(self, job: Any) -> dict[str, str | None]:
        """Process one job and return a structured status/file-path result.

        Returns:
            A dict with keys:
            - ``status``: one of ``completed``, ``failed``, ``validation_failed``.
            - ``file_path``: output path when completed, otherwise ``None``.
        """
        payload = getattr(job, "payload", None) or {}

        if payload.get("music_metadata"):
            # Music metadata payloads are expected to include a resolved media URL.
            resolved_media = payload.get("resolved_media") or {}
            media_url = resolved_media.get("media_url")
            metadata = payload.get("music_metadata")
            if media_url:
                try:
                    # Download from the resolved media URL, then tag with attached metadata.
                    file_path = self._downloader.download(media_url)
                    # Optionally enforce duration validation before any file tagging/write side effects.
                    if ENABLE_DURATION_VALIDATION:
                        expected_ms = None
                        if isinstance(metadata, dict):
                            expected_ms = metadata.get("expected_ms")
                        else:
                            expected_ms = getattr(metadata, "expected_ms", None)

                        if expected_ms is not None:
                            if not validate_duration(
                                file_path,
                                int(expected_ms),
                                SPOTIFY_DURATION_TOLERANCE_SECONDS,
                            ):
                                expected_seconds = int(expected_ms) / 1000.0
                                actual_seconds = float("nan")
                                try:
                                    actual_seconds = get_media_duration(file_path)
                                except Exception:
                                    logger.exception("failed to retrieve actual duration for validation log")
                                logger.warning(
                                    "validation_failed actual=%.2fs expected=%.2fs tolerance=%.2f",
                                    actual_seconds,
                                    expected_seconds,
                                    SPOTIFY_DURATION_TOLERANCE_SECONDS,
                                )
                                self._set_job_status(job, payload, JOB_STATUS_VALIDATION_FAILED)
                                return {"status": JOB_STATUS_VALIDATION_FAILED, "file_path": None}

                    metadata_obj = self._coerce_music_metadata(metadata)
                    normalized_metadata = normalize_music_metadata(metadata_obj)
                    # === Canonical Path Enforcement Starts Here ===
                    temp_path = Path(file_path)
                    ext = temp_path.suffix.lstrip(".")
                    root_path = self._resolve_music_root(payload)
                    album_folder = build_album_directory(normalized_metadata)
                    filename = build_track_filename(
                        {
                            "title": normalized_metadata.title,
                            "track_num": normalized_metadata.track_num,
                            "ext": ext,
                        }
                    )
                    disc_total_raw = getattr(normalized_metadata, "disc_total", None)
                    disc_total = int(disc_total_raw) if isinstance(disc_total_raw, int) and disc_total_raw > 0 else None
                    relative_layout = build_music_relative_layout(
                        album_artist=normalized_metadata.album_artist or normalized_metadata.artist,
                        album_folder=album_folder,
                        track_label=filename,
                        disc_number=normalized_metadata.disc_num,
                        disc_total=disc_total,
                    )
                    canonical_path = root_path / Path(relative_layout)
                    ensure_parent_dir(canonical_path)
                    try:
                        shutil.move(str(temp_path), str(canonical_path))
                    except Exception:
                        logger.exception("failed to move file to canonical path path=%s", canonical_path)
                        self._set_job_status(job, payload, JOB_STATUS_FAILED)
                        return {"status": JOB_STATUS_FAILED, "file_path": None}
                    try:
                        tag_file(str(canonical_path), normalized_metadata)
                    except Exception:
                        logger.exception("failed to tag canonical file path=%s", canonical_path)
                        self._set_job_status(job, payload, JOB_STATUS_FAILED)
                        return {"status": JOB_STATUS_FAILED, "file_path": None}
                    # Record idempotency state only after download and tagging both succeed.
                    playlist_id = payload.get("playlist_id")
                    isrc = getattr(metadata, "isrc", None)
                    if not isrc and isinstance(metadata, dict):
                        isrc = metadata.get("isrc")
                    if playlist_id and isrc:
                        record_downloaded_track(str(playlist_id), str(isrc), str(canonical_path))
                    self._set_job_status(job, payload, JOB_STATUS_COMPLETED)
                    return {"status": JOB_STATUS_COMPLETED, "file_path": str(canonical_path)}
                except Exception:
                    logger.exception("music job processing failed")
                    self._set_job_status(job, payload, JOB_STATUS_FAILED)
                    return {"status": JOB_STATUS_FAILED, "file_path": None}

        # Non-music or incomplete payloads use the existing default worker behavior.
        file_path = self.default_download_and_tag(job)
        return {"status": JOB_STATUS_COMPLETED, "file_path": file_path}

    def default_download_and_tag(self, job: Any) -> str:
        """Fallback behavior implemented by existing worker flows."""
        raise NotImplementedError

    @staticmethod
    def _set_job_status(job: Any, payload: Any, status: str) -> None:
        """Set worker job status using the supported terminal status values."""
        if status not in JOB_ALLOWED_STATUSES:
            raise ValueError(f"unsupported job status: {status}")
        setattr(job, "status", status)
        if isinstance(payload, dict):
            payload["status"] = status

    @staticmethod
    def _coerce_music_metadata(metadata: Any) -> CanonicalMetadata:
        """Coerce payload metadata into ``CanonicalMetadata`` for normalization/tagging."""
        return coerce_canonical_music_metadata(metadata)

    @staticmethod
    def _resolve_music_root(payload: dict[str, Any]) -> Path:
        """Resolve music root path from existing payload/config fields."""
        return resolve_music_root_path(payload)


def safe_int(value: Any) -> Optional[int]:
    """Parse an integer from mixed input, returning ``None`` when unavailable.

    The parser extracts the first numeric portion from string inputs, e.g.
    ``"01/12" -> 1`` and ``"Disc 1" -> 1``. ``None`` and non-numeric values
    return ``None``.
    """
    return parse_first_positive_int(value)
