"""Download worker behavior for resolved Spotify media jobs."""

from __future__ import annotations

from typing import Any, Protocol

from metadata.tagging import tag_file


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
                tag_file(file_path, metadata)
                return file_path

        # Non-music or incomplete payloads use the existing default worker behavior.
        return self.default_download_and_tag(job)

    def default_download_and_tag(self, job: Any) -> str:
        """Fallback behavior implemented by existing worker flows."""
        raise NotImplementedError

