from __future__ import annotations

import sys
import types
import sqlite3
from pathlib import Path

# Keep queue tests independent from optional Google deps pulled in by engine package imports.
for module_name in (
    "google",
    "google.auth",
    "google.auth.exceptions",
    "google.auth.transport",
    "google.auth.transport.requests",
    "google.oauth2",
    "google.oauth2.credentials",
    "googleapiclient",
    "googleapiclient.discovery",
    "googleapiclient.errors",
    "rapidfuzz",
    "mutagen",
    "yt_dlp",
    "yt_dlp.utils",
    "yt_dlp.version",
    "metadata.queue",
    "metadata.services.musicbrainz_service",
    "musicbrainzngs",
):
    if module_name not in sys.modules:
        sys.modules[module_name] = types.ModuleType(module_name)
if not hasattr(sys.modules["rapidfuzz"], "fuzz"):
    class _Fuzz:
        @staticmethod
        def ratio(*_args, **_kwargs):
            return 0

    sys.modules["rapidfuzz"].fuzz = _Fuzz()
if not hasattr(sys.modules["mutagen"], "File"):
    sys.modules["mutagen"].File = lambda *_args, **_kwargs: None
if not hasattr(sys.modules["yt_dlp"], "YoutubeDL"):
    class _YoutubeDL:
        def __init__(self, *_args, **_kwargs):
            pass

    sys.modules["yt_dlp"].YoutubeDL = _YoutubeDL
if not hasattr(sys.modules["yt_dlp.version"], "__version__"):
    sys.modules["yt_dlp.version"].__version__ = "0.0-test"
if not hasattr(sys.modules["yt_dlp.utils"], "DownloadError"):
    class _DownloadError(Exception):
        pass

    class _ExtractorError(Exception):
        pass

    sys.modules["yt_dlp.utils"].DownloadError = _DownloadError
    sys.modules["yt_dlp.utils"].ExtractorError = _ExtractorError
if not hasattr(sys.modules["metadata.queue"], "enqueue_metadata"):
    sys.modules["metadata.queue"].enqueue_metadata = lambda *_args, **_kwargs: None
if not hasattr(sys.modules["metadata.services.musicbrainz_service"], "get_musicbrainz_service"):
    sys.modules["metadata.services.musicbrainz_service"].get_musicbrainz_service = (
        lambda *_args, **_kwargs: None
    )
sys.modules["google.auth.exceptions"].RefreshError = Exception
sys.modules["google.auth.transport.requests"].Request = object
sys.modules["googleapiclient.errors"].HttpError = Exception
sys.modules["google.oauth2.credentials"].Credentials = object
sys.modules["googleapiclient.discovery"].build = lambda *args, **kwargs: None

from engine.job_queue import (
    DownloadJobStore,
    JOB_STATUS_CANCELLED,
    JOB_STATUS_CLAIMED,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_DOWNLOADING,
    JOB_STATUS_POSTPROCESSING,
    JOB_STATUS_QUEUED,
    ensure_download_jobs_table,
)


def _store(tmp_path: Path) -> DownloadJobStore:
    db_path = tmp_path / "queue_integrity.sqlite"
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_download_jobs_table(conn)
    finally:
        conn.close()
    return DownloadJobStore(str(db_path))


def _enqueue_job(store: DownloadJobStore, *, suffix: str = "1") -> str:
    job_id, created, _reason = store.enqueue_job(
        origin="playlist",
        origin_id=f"pl-{suffix}",
        media_type="video",
        media_intent="playlist",
        source="youtube",
        url=f"https://www.youtube.com/watch?v=video{suffix}",
        output_template={"output_dir": "downloads", "final_format": "mp4"},
    )
    assert created is True
    return job_id


def test_mark_downloading_requires_claimed_state(tmp_path: Path) -> None:
    store = _store(tmp_path)
    job_id = _enqueue_job(store, suffix="downloading-guard")

    transitioned = store.mark_downloading(job_id)

    assert transitioned is False
    assert store.get_job_status(job_id) == JOB_STATUS_QUEUED


def test_terminal_cancelled_state_cannot_be_overwritten(tmp_path: Path) -> None:
    store = _store(tmp_path)
    job_id = _enqueue_job(store, suffix="terminal-guard")

    claimed = store.claim_next_job("youtube")
    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.status == JOB_STATUS_CLAIMED

    assert store.mark_downloading(job_id) is True
    assert store.mark_canceled(job_id, reason="Cancelled by user") is True

    assert store.mark_postprocessing(job_id) is False
    assert store.mark_completed(job_id, file_path="/tmp/ignored.mp4") is False
    assert store.get_job_status(job_id) == JOB_STATUS_CANCELLED


def test_record_failure_does_not_override_cancelled(tmp_path: Path) -> None:
    store = _store(tmp_path)
    job_id = _enqueue_job(store, suffix="failure-guard")

    claimed = store.claim_next_job("youtube")
    assert claimed is not None

    assert store.mark_downloading(job_id) is True
    assert store.mark_canceled(job_id, reason="Cancelled by user") is True

    result_status = store.record_failure(
        claimed,
        error_message="RuntimeError: should_not_override_cancelled",
        retryable=False,
        retry_delay_seconds=0,
    )

    assert result_status == JOB_STATUS_CANCELLED
    assert store.get_job_status(job_id) == JOB_STATUS_CANCELLED


def test_happy_path_transition_chain_reaches_completed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    job_id = _enqueue_job(store, suffix="happy-path")

    claimed = store.claim_next_job("youtube")
    assert claimed is not None

    assert store.mark_downloading(job_id) is True
    assert store.mark_postprocessing(job_id) is True
    assert store.mark_completed(job_id, file_path="/tmp/final.mp4") is True
    assert store.get_job_status(job_id) == JOB_STATUS_COMPLETED
