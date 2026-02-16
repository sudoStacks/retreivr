"""Download worker behavior for resolved Spotify media jobs."""

from __future__ import annotations

import logging
from typing import Any, Protocol

from config.settings import ENABLE_DURATION_VALIDATION, SPOTIFY_DURATION_TOLERANCE_SECONDS
from db.downloaded_tracks import record_downloaded_track
from media.ffprobe import get_media_duration
from media.validation import validate_duration
from metadata.tagging import tag_file

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

    def process_job(self, job: Any) -> str:
        """Process one job with music-metadata-aware flow and safe fallback behavior."""
        payload = getattr(job, "payload", None) or {}

        if payload.get("music_metadata"):
            # Music metadata payloads are expected to include a resolved media URL.
            resolved_media = payload.get("resolved_media") or {}
            media_url = resolved_media.get("media_url")
            metadata = payload.get("music_metadata")
            if media_url:
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
                            return file_path

                tag_file(file_path, metadata)
                # Record idempotency state only after download and tagging both succeed.
                playlist_id = payload.get("playlist_id")
                isrc = getattr(metadata, "isrc", None)
                if not isrc and isinstance(metadata, dict):
                    isrc = metadata.get("isrc")
                if playlist_id and isrc:
                    record_downloaded_track(str(playlist_id), str(isrc), file_path)
                self._set_job_status(job, payload, JOB_STATUS_COMPLETED)
                return file_path

        # Non-music or incomplete payloads use the existing default worker behavior.
        return self.default_download_and_tag(job)

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
