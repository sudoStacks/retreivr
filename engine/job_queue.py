import json
import copy
import importlib.util
import logging
import os
import re
import shutil
import shlex
import sqlite3
import subprocess
import tempfile
import traceback
import threading
import time
import urllib.parse
import unicodedata
import hashlib
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

import requests
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError, ExtractorError

from engine.json_utils import json_sanity_check, safe_json, safe_json_dumps
from engine.music_title_normalization import has_live_intent, relaxed_search_title
from engine.paths import EnginePaths, TOKENS_DIR, resolve_dir
from engine.search_scoring import rank_candidates, score_candidate
from media.music_contract import format_zero_padded_track_number, parse_first_positive_int
from media.path_builder import build_music_relative_layout
from metadata.naming import sanitize_component
from metadata.queue import enqueue_metadata
from metadata.services.musicbrainz_service import get_musicbrainz_service

try:
    from engine.musicbrainz_binding import _normalize_title_for_mb_lookup, resolve_best_mb_pair
except Exception:
    _BINDING_PATH = os.path.join(os.path.dirname(__file__), "musicbrainz_binding.py")
    _BINDING_SPEC = importlib.util.spec_from_file_location("engine_musicbrainz_binding_job_queue", _BINDING_PATH)
    _BINDING_MODULE = importlib.util.module_from_spec(_BINDING_SPEC)
    assert _BINDING_SPEC and _BINDING_SPEC.loader
    _BINDING_SPEC.loader.exec_module(_BINDING_MODULE)
    _normalize_title_for_mb_lookup = _BINDING_MODULE._normalize_title_for_mb_lookup
    resolve_best_mb_pair = _BINDING_MODULE.resolve_best_mb_pair

logger = logging.getLogger(__name__)

JOB_STATUS_QUEUED = "queued"
JOB_STATUS_CLAIMED = "claimed"
JOB_STATUS_DOWNLOADING = "downloading"
JOB_STATUS_POSTPROCESSING = "postprocessing"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_SKIPPED_DUPLICATE = "skipped_duplicate"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELLED = "cancelled"

TERMINAL_STATUSES = (
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
    JOB_STATUS_SKIPPED_DUPLICATE,
    JOB_STATUS_CANCELLED,
)

# --- CancelledError for job cancellation
class CancelledError(Exception):
    """Raised to abort an in-flight download due to user cancellation."""

class PostprocessingError(Exception):
    pass


class CookieFallbackError(RuntimeError):
    """Raised when the optional YouTube cookie fallback fails."""

_FORMAT_VIDEO = (
    "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
    "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
    "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/"
    "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/"
    "bestvideo*+bestaudio/best"
)
# Prefer audio-only formats first; fall back to any best format only if needed.
# Keep this quoted/argument-safe in CLI execution (we do NOT use shell=True).
_FORMAT_AUDIO = "bestaudio/best"

_AUDIO_FORMATS = {"mp3", "m4a", "flac"}

_AUDIO_TITLE_CLEAN_RE = re.compile(
    r"\s*[\(\[\{][^)\]\}]*?(official|music video|video|lyric|audio|visualizer|full video|hd|4k)[^)\]\}]*?[\)\]\}]\s*",
    re.IGNORECASE,
)
_AUDIO_TITLE_TRAIL_RE = re.compile(
    r"\s*-\s*(official|music video|video|lyric|audio|visualizer|full video).*$",
    re.IGNORECASE,
)
_AUDIO_ARTIST_VEVO_RE = re.compile(r"(vevo)$", re.IGNORECASE)
_WORD_TOKEN_RE = re.compile(r"[a-z0-9]+")

_YTDLP_DOWNLOAD_UNSAFE_KEYS = {"download", "skip_download", "simulate", "extract_flat"}

_YTDLP_DOWNLOAD_ALLOWLIST = {
    "concurrent_fragment_downloads",
    "cookiefile",
    "cookiesfrombrowser",
    "forceipv4",
    "forceipv6",
    "fragment_retries",
    "geo_verification_proxy",
    "http_headers",
    "max_sleep_interval",
    "nocheckcertificate",
    "noproxy",
    "proxy",
    "ratelimit",
    "retries",
    "sleep_interval",
    "socket_timeout",
    "source_address",
    "throttledratelimit",
    "user_agent",
}

# Deterministic mapping of known yt-dlp unavailability signals to classes.
# NOTE: Transient network failures are explicitly excluded from classification.
_YTDLP_UNAVAILABLE_SIGNAL_MAP: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "removed_or_deleted",
        (
            "video unavailable. this video has been removed by the uploader",
            "has been removed by the uploader",
            "video has been removed",
            "this video is unavailable",
        ),
    ),
    (
        "private_or_members_only",
        (
            "private video",
            "members-only",
            "members only",
            "join this channel",
            "this video is private",
        ),
    ),
    (
        "age_restricted",
        (
            "sign in to confirm your age",
            "age-restricted",
            "age restricted",
            "age restriction",
        ),
    ),
    (
        "region_restricted",
        (
            "not available in your country",
            "video unavailable in your country",
            "geo-restricted",
            "geoblocked",
            "geo blocked",
            "the uploader has not made this video available in your country",
        ),
    ),
    (
        "format_unavailable",
        (
            "requested format is not available",
            "requested format not available",
            "requested format is unavailable",
        ),
    ),
    (
        "drm_protected",
        (
            "this video is drm protected",
            "drm protected",
            "drm",
        ),
    ),
)


def _is_ep_release_context(release_primary_type: str | None, release_secondary_types: list[str] | None = None) -> bool:
    primary = str(release_primary_type or "").strip().lower()
    if primary == "ep":
        return True
    if isinstance(release_secondary_types, (list, tuple, set)):
        for value in release_secondary_types:
            if str(value or "").strip().lower() == "ep":
                return True
    return False

_YTDLP_TRANSIENT_ERROR_MARKERS: tuple[str, ...] = (
    "timed out",
    "timeout",
    "connection reset",
    "temporary failure",
    "network error",
    "unable to download webpage",
    "couldn't download webpage",
    "http error 5",
    "service unavailable",
    "too many requests",
)

_MUSIC_TRACK_SOURCE_PRIORITY = ("youtube_music", "youtube", "soundcloud", "bandcamp")
_DEFAULT_MATCH_THRESHOLD = 0.92
_MUSIC_TRACK_THRESHOLD = min(_DEFAULT_MATCH_THRESHOLD * 0.8, 0.70)
_MUSIC_TRACK_PENALTY_TERMS = ("live", "cover", "karaoke", "remix")
_MUSIC_TRACK_PENALIZE_TOKENS = ("live", "cover", "karaoke", "remix", "reaction", "ft.", "feat.", "instrumental")
_MUSIC_SOURCE_PRIORITY_WEIGHTS = {
    "youtube_music": 10,
    "youtube": 7,
    "soundcloud": 4,
    "bandcamp": 2,
}
_VIDEO_CONTAINERS = {"mkv", "mp4", "webm"}
_MUSIC_AUDIO_FORMAT_WARNED = False
_MUSIC_REVIEW_FOLDER_NAME = "Needs Review"


@dataclass(frozen=True)
class DownloadJob:
    id: str
    origin: str
    origin_id: str
    media_type: str
    media_intent: str
    source: str
    url: str
    input_url: str | None
    canonical_url: str | None
    external_id: str | None
    status: str
    queued: str | None
    claimed: str | None
    downloading: str | None
    postprocessing: str | None
    completed: str | None
    failed: str | None
    canceled: str | None
    attempts: int
    max_attempts: int
    created_at: str | None
    updated_at: str | None
    last_error: str | None
    trace_id: str
    output_template: dict | None
    resolved_destination: str | None
    canonical_id: str | None
    file_path: str | None


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _log_event(level, message, **fields):
    payload = {"message": message, **fields}
    try:
        logging.log(level, safe_json_dumps(payload, sort_keys=True))
    except Exception as exc:
        logging.log(level, f"log_event_serialization_failed: {exc} message={message}")


def ensure_download_jobs_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS download_jobs (
            id TEXT PRIMARY KEY,
            origin TEXT NOT NULL,
            origin_id TEXT NOT NULL,
            media_type TEXT NOT NULL,
            media_intent TEXT NOT NULL,
            source TEXT NOT NULL,
            url TEXT NOT NULL,
            input_url TEXT,
            canonical_url TEXT,
            external_id TEXT,
            status TEXT NOT NULL,
            queued TEXT,
            claimed TEXT,
            downloading TEXT,
            postprocessing TEXT,
            completed TEXT,
            failed TEXT,
            canceled TEXT,
            attempts INTEGER NOT NULL,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_error TEXT,
            trace_id TEXT NOT NULL UNIQUE,
            output_template TEXT,
            resolved_destination TEXT,
            canonical_id TEXT,
            file_path TEXT,
            progress_downloaded_bytes INTEGER,
            progress_total_bytes INTEGER,
            progress_percent REAL,
            progress_speed_bps REAL,
            progress_eta_seconds INTEGER,
            progress_updated_at TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(download_jobs)")
    existing_columns = {row[1] for row in cur.fetchall()}
    for column in (
        "claimed",
        "downloading",
        "postprocessing",
        "resolved_destination",
        "canonical_id",
        "file_path",
        "input_url",
        "canonical_url",
        "external_id",
        "progress_downloaded_bytes",
        "progress_total_bytes",
        "progress_percent",
        "progress_speed_bps",
        "progress_eta_seconds",
        "progress_updated_at",
    ):
        if column not in existing_columns:
            if column in {"progress_downloaded_bytes", "progress_total_bytes", "progress_eta_seconds"}:
                cur.execute(f"ALTER TABLE download_jobs ADD COLUMN {column} INTEGER")
            elif column in {"progress_percent", "progress_speed_bps"}:
                cur.execute(f"ALTER TABLE download_jobs ADD COLUMN {column} REAL")
            else:
                cur.execute(f"ALTER TABLE download_jobs ADD COLUMN {column} TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_status ON download_jobs (status)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_jobs_source_status ON download_jobs (source, status)"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_created ON download_jobs (created_at)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_jobs_canonical_dest_status_created "
        "ON download_jobs (canonical_id, resolved_destination, status, created_at DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_jobs_url_dest_status_created "
        "ON download_jobs (url, resolved_destination, status, created_at DESC)"
    )
    # Migration: de-duplicate canonical IDs before adding uniqueness enforcement.
    # Keep the newest row by created_at; break ties by rowid (latest inserted wins).
    cur.execute(
        """
        DELETE FROM download_jobs
        WHERE canonical_id IS NOT NULL
          AND canonical_id != ''
          AND rowid IN (
              SELECT older.rowid
              FROM download_jobs AS older
              JOIN download_jobs AS newer
                ON older.canonical_id = newer.canonical_id
               AND (
                    COALESCE(newer.created_at, '') > COALESCE(older.created_at, '')
                    OR (
                        COALESCE(newer.created_at, '') = COALESCE(older.created_at, '')
                        AND newer.rowid > older.rowid
                    )
               )
             WHERE older.canonical_id IS NOT NULL
               AND older.canonical_id != ''
          )
        """
    )
    # Additive uniqueness constraint for canonical_id (null/empty values are excluded).
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_download_jobs_canonical_id "
        "ON download_jobs (canonical_id) "
        "WHERE canonical_id IS NOT NULL AND canonical_id != ''"
    )
    conn.commit()

# --- ensure_downloads_table
def ensure_downloads_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS downloads (
            video_id TEXT PRIMARY KEY,
            playlist_id TEXT,
            downloaded_at TEXT NOT NULL,
            filepath TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_downloaded_at ON downloads (downloaded_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_playlist_id ON downloads (playlist_id)")
    conn.commit()


def ensure_download_history_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS download_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT,
            title TEXT,
            filename TEXT,
            destination TEXT,
            source TEXT,
            status TEXT,
            created_at TEXT,
            completed_at TEXT,
            file_size_bytes INTEGER,
            input_url TEXT,
            canonical_url TEXT,
            external_id TEXT,
            channel_id TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(download_history)")
    existing_columns = {row[1] for row in cur.fetchall()}
    for column in ("input_url", "canonical_url", "external_id", "source", "channel_id"):
        if column not in existing_columns:
            cur.execute(f"ALTER TABLE download_history ADD COLUMN {column} TEXT")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_history_source_extid "
        "ON download_history (source, external_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_history_canonical_url "
        "ON download_history (canonical_url)"
    )
    conn.commit()

def is_music_media_type(value):
    if value is None:
        return False
    value = str(value).strip().lower()
    return value in {"music", "audio"}

def _normalize_audio_format(value: str | None) -> str | None:
    if not value:
        return None
    v = str(value).strip().lower()
    # Accept mp4 as a user-facing synonym for m4a (AAC in MP4 container)
    if v == "mp4":
        return "m4a"
    return v


def _normalize_format(value: str | None) -> str | None:
    if not value:
        return None
    return str(value).strip().lower()


def _extract_music_binding_ids(job: DownloadJob) -> tuple[str | None, str | None]:
    payload = job.output_template if isinstance(job.output_template, dict) else {}
    canonical = payload.get("canonical_metadata") if isinstance(payload.get("canonical_metadata"), dict) else {}
    recording_mbid = str(
        payload.get("recording_mbid")
        or payload.get("mb_recording_id")
        or canonical.get("recording_mbid")
        or canonical.get("mb_recording_id")
        or ""
    ).strip() or None
    release_mbid = str(
        payload.get("mb_release_id")
        or payload.get("release_id")
        or canonical.get("mb_release_id")
        or canonical.get("release_id")
        or ""
    ).strip() or None
    return recording_mbid, release_mbid

class DownloadJobStore:
    def __init__(self, db_path):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _row_to_job(self, row):
        if not row:
            return None
        if isinstance(row, sqlite3.Row):
            row = dict(row)
        output_template = row["output_template"]
        parsed_output = None
        if output_template:
            try:
                parsed_output = json.loads(output_template)
            except json.JSONDecodeError:
                parsed_output = None
        return DownloadJob(
            id=row["id"],
            origin=row["origin"],
            origin_id=row["origin_id"],
            media_type=row["media_type"],
            media_intent=row["media_intent"],
            source=row["source"],
            url=row["url"],
            input_url=row.get("input_url"),
            canonical_url=row.get("canonical_url"),
            external_id=row.get("external_id"),
            status=row["status"],
            queued=row["queued"],
            claimed=row["claimed"],
            downloading=row["downloading"],
            postprocessing=row["postprocessing"],
            completed=row["completed"],
            failed=row["failed"],
            canceled=row["canceled"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_error=row["last_error"],
            trace_id=row["trace_id"],
            output_template=parsed_output,
            resolved_destination=row["resolved_destination"],
            canonical_id=row["canonical_id"],
            file_path=row.get("file_path"),
        )

    def list_sources_with_queued_jobs(self, *, now=None):
        now = now or utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT source FROM download_jobs WHERE status=? AND (queued IS NULL OR queued<=?)",
                (JOB_STATUS_QUEUED, now),
            )
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def get_job_status(self, job_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM download_jobs WHERE id=?", (job_id,))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def merge_output_template_fields(self, job_id, updates):
        if not isinstance(updates, dict) or not updates:
            return
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute("SELECT output_template FROM download_jobs WHERE id=? LIMIT 1", (job_id,))
            row = cur.fetchone()
            if not row:
                conn.commit()
                return
            existing = {}
            raw = row[0]
            if isinstance(raw, str) and raw.strip():
                try:
                    loaded = json.loads(raw)
                    if isinstance(loaded, dict):
                        existing = loaded
                except Exception:
                    existing = {}
            existing.update(updates)
            cur.execute(
                "UPDATE download_jobs SET output_template=?, updated_at=? WHERE id=?",
                (safe_json_dumps(existing), utc_now(), job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def job_exists(self, origin, origin_id, url):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM download_jobs WHERE origin=? AND origin_id=? AND url=? LIMIT 1",
                (origin, origin_id, url),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()

    def find_active_job(self, origin, origin_id, url):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT * FROM download_jobs
                WHERE origin=? AND origin_id=? AND url=? AND status NOT IN ({', '.join('?' for _ in TERMINAL_STATUSES)})
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (origin, origin_id, url, *TERMINAL_STATUSES),
            )
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_job(row)
        finally:
            conn.close()

    def _row_has_valid_output(self, row):
        if not row:
            return False
        if isinstance(row, sqlite3.Row):
            row = dict(row)
        if row["status"] != JOB_STATUS_COMPLETED:
            return False
        path = row.get("file_path")
        if not path:
            return False
        try:
            if not os.path.exists(path):
                return False
            return os.path.getsize(path) > 0
        except OSError:
            return False

    def find_duplicate_job(self, *, canonical_id=None, url=None, destination=None):
        if not canonical_id and not url:
            return None
        conn = self._connect()
        try:
            cur = conn.cursor()
            clauses = []
            params = []
            dest_clause = "resolved_destination=?" if destination is not None else "resolved_destination IS NULL"
            if canonical_id:
                clauses.append(f"(canonical_id=? AND {dest_clause})")
                params.append(canonical_id)
                if destination is not None:
                    params.append(destination)
            if url:
                clauses.append(f"(url=? AND {dest_clause})")
                params.append(url)
                if destination is not None:
                    params.append(destination)
            if not clauses:
                return None
            query = f"""
                SELECT * FROM download_jobs
                WHERE ({' OR '.join(clauses)})
                  AND status IN (?, ?, ?, ?, ?)
                ORDER BY created_at DESC
                LIMIT 1
            """
            params.extend(
                [
                    JOB_STATUS_COMPLETED,
                    JOB_STATUS_QUEUED,
                    JOB_STATUS_CLAIMED,
                    JOB_STATUS_DOWNLOADING,
                    JOB_STATUS_POSTPROCESSING,
                ]
            )
            cur.execute(query, tuple(params))
            row = cur.fetchone()
            if not row:
                return None
            if row["status"] == JOB_STATUS_COMPLETED and not self._row_has_valid_output(row):
                return None
            return self._row_to_job(row)
        finally:
            conn.close()

    def get_job_by_canonical_id(self, canonical_id):
        cid = str(canonical_id or "").strip()
        if not cid:
            return None
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM download_jobs
                WHERE canonical_id=?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (cid,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_job(row)
        finally:
            conn.close()

    def _is_requeueable_duplicate(self, job):
        if not job:
            return False
        if job.status in {JOB_STATUS_FAILED, JOB_STATUS_CANCELLED, JOB_STATUS_SKIPPED_DUPLICATE}:
            return True
        if job.status == JOB_STATUS_COMPLETED and not self._row_has_valid_output(job.__dict__):
            return True
        return False

    def _requeue_existing_job(
        self,
        *,
        job_id,
        origin,
        origin_id,
        media_type,
        media_intent,
        source,
        url,
        input_url,
        canonical_url,
        external_id,
        max_attempts,
        output_template_json,
        destination,
        canonical_id,
    ):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                """
                UPDATE download_jobs
                SET origin=?,
                    origin_id=?,
                    media_type=?,
                    media_intent=?,
                    source=?,
                    url=?,
                    input_url=?,
                    canonical_url=?,
                    external_id=?,
                    status=?,
                    queued=?,
                    claimed=NULL,
                    downloading=NULL,
                    postprocessing=NULL,
                    completed=NULL,
                    failed=NULL,
                    canceled=NULL,
                    attempts=0,
                    max_attempts=?,
                    updated_at=?,
                    last_error=NULL,
                    output_template=?,
                    resolved_destination=?,
                    canonical_id=?,
                    file_path=NULL,
                    progress_downloaded_bytes=NULL,
                    progress_total_bytes=NULL,
                    progress_percent=NULL,
                    progress_speed_bps=NULL,
                    progress_eta_seconds=NULL,
                    progress_updated_at=NULL
                WHERE id=?
                """,
                (
                    origin,
                    origin_id,
                    media_type,
                    media_intent,
                    source,
                    url,
                    input_url,
                    canonical_url,
                    external_id,
                    JOB_STATUS_QUEUED,
                    now,
                    max_attempts,
                    now,
                    output_template_json,
                    destination,
                    canonical_id,
                    job_id,
                ),
            )
            conn.commit()
            return cur.rowcount == 1
        finally:
            conn.close()

    def claim_job_by_id(self, job_id, *, now=None):
        now = now or utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT * FROM download_jobs WHERE id=? AND status=? LIMIT 1",
                (job_id, JOB_STATUS_QUEUED),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, claimed=?, updated_at=?
                WHERE id=? AND status=?
                """,
                (JOB_STATUS_CLAIMED, now, now, job_id, JOB_STATUS_QUEUED),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            updated_row = dict(row)
            updated_row["status"] = JOB_STATUS_CLAIMED
            updated_row["claimed"] = now
            updated_row["updated_at"] = now
            return self._row_to_job(updated_row)
        finally:
            conn.close()

    def enqueue_job(
        self,
        *,
        origin,
        origin_id,
        media_type,
        media_intent,
        source,
        url,
        input_url=None,
        canonical_url=None,
        external_id=None,
        output_template=None,
        max_attempts=3,
        trace_id=None,
        resolved_destination=None,
        canonical_id=None,
        log_duplicate_event=True,
        force_requeue=False,
    ):
        origin_id = origin_id or ""
        destination = resolved_destination
        if destination is None and output_template:
            destination = (output_template or {}).get("output_dir")

        input_url = input_url or url
        if external_id is None and source in {"youtube", "youtube_music"}:
            external_id = extract_video_id(url)
        if canonical_url is None:
            canonical_url = canonicalize_url(source, input_url, external_id)

        if force_requeue and canonical_id:
            duplicate_job = self.get_job_by_canonical_id(canonical_id)
            if duplicate_job:
                if duplicate_job.status in {
                    JOB_STATUS_QUEUED,
                    JOB_STATUS_CLAIMED,
                    JOB_STATUS_DOWNLOADING,
                    JOB_STATUS_POSTPROCESSING,
                }:
                    if log_duplicate_event:
                        _log_event(
                            logging.INFO,
                            "job_skipped_duplicate",
                            job_id=duplicate_job.id,
                            trace_id=duplicate_job.trace_id,
                            origin=origin,
                            origin_id=origin_id,
                            source=source,
                            url=url,
                            destination=destination,
                            canonical_id=canonical_id,
                            status=duplicate_job.status,
                        )
                    return duplicate_job.id, False, "duplicate"

                requeued = self._requeue_existing_job(
                    job_id=duplicate_job.id,
                    origin=origin,
                    origin_id=origin_id,
                    media_type=media_type,
                    media_intent=media_intent,
                    source=source,
                    url=url,
                    input_url=input_url,
                    canonical_url=canonical_url,
                    external_id=external_id,
                    max_attempts=max_attempts,
                    output_template_json=safe_json_dumps(output_template) if output_template else None,
                    destination=destination,
                    canonical_id=canonical_id,
                )
                if requeued:
                    _log_event(
                        logging.INFO,
                        "job_forced_requeue",
                        job_id=duplicate_job.id,
                        trace_id=duplicate_job.trace_id,
                        origin=origin,
                        origin_id=origin_id,
                        source=source,
                        url=url,
                        destination=destination,
                        canonical_id=canonical_id,
                        previous_status=duplicate_job.status,
                    )
                    return duplicate_job.id, True, "requeued"

        duplicate = self.find_duplicate_job(
            canonical_id=canonical_id,
            url=url,
            destination=destination,
        )
        if duplicate:
            if log_duplicate_event:
                _log_event(
                    logging.INFO,
                    "job_skipped_duplicate",
                    job_id=duplicate.id,
                    trace_id=duplicate.trace_id,
                    origin=origin,
                    origin_id=origin_id,
                    source=source,
                    url=url,
                    destination=destination,
                    canonical_id=canonical_id,
                    status=duplicate.status,
                )
            return duplicate.id, False, "duplicate"

        job_id = uuid4().hex
        now = utc_now()
        trace_id = trace_id or uuid4().hex
        output_template_json = safe_json_dumps(output_template) if output_template else None

        conn = self._connect()
        try:
            cur = conn.cursor()
            # Retry a few times on transient SQLite write contention (rapid multi-click enqueue).
            for attempt in range(5):
                try:
                    cur.execute("BEGIN IMMEDIATE")
                    cur.execute(
                        """
                INSERT INTO download_jobs (
                    id, origin, origin_id, media_type, media_intent, source, url,
                    input_url, canonical_url, external_id,
                    status, queued, claimed, downloading, postprocessing, completed,
                    failed, canceled, attempts, max_attempts, created_at, updated_at,
                    last_error, trace_id, output_template, resolved_destination, canonical_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    origin,
                    origin_id,
                    media_type,
                    media_intent,
                    source,
                    url,
                    input_url,
                    canonical_url,
                    external_id,
                    JOB_STATUS_QUEUED,
                    now,
                    None,
                    None,
                            None,
                            None,
                            None,
                            None,
                            0,
                            max_attempts,
                            now,
                            now,
                            None,
                            trace_id,
                            output_template_json,
                            destination,
                            canonical_id,
                        ),
                    )
                    conn.commit()
                    return job_id, True, None
                except sqlite3.IntegrityError as exc:
                    conn.rollback()
                    err = str(exc).lower()
                    if canonical_id and "canonical_id" in err:
                        duplicate_job = self.get_job_by_canonical_id(canonical_id)
                        if duplicate_job:
                            if self._is_requeueable_duplicate(duplicate_job):
                                requeued = self._requeue_existing_job(
                                    job_id=duplicate_job.id,
                                    origin=origin,
                                    origin_id=origin_id,
                                    media_type=media_type,
                                    media_intent=media_intent,
                                    source=source,
                                    url=url,
                                    input_url=input_url,
                                    canonical_url=canonical_url,
                                    external_id=external_id,
                                    max_attempts=max_attempts,
                                    output_template_json=output_template_json,
                                    destination=destination,
                                    canonical_id=canonical_id,
                                )
                                if requeued:
                                    _log_event(
                                        logging.INFO,
                                        "job_requeued_from_terminal_duplicate",
                                        job_id=duplicate_job.id,
                                        trace_id=duplicate_job.trace_id,
                                        origin=origin,
                                        origin_id=origin_id,
                                        source=source,
                                        url=url,
                                        destination=destination,
                                        canonical_id=canonical_id,
                                        previous_status=duplicate_job.status,
                                    )
                                    return duplicate_job.id, True, "requeued"
                            if log_duplicate_event:
                                _log_event(
                                    logging.INFO,
                                    "job_skipped_duplicate",
                                    job_id=duplicate_job.id,
                                    trace_id=duplicate_job.trace_id,
                                    origin=origin,
                                    origin_id=origin_id,
                                    source=source,
                                    url=url,
                                    destination=destination,
                                    canonical_id=canonical_id,
                                    status=duplicate_job.status,
                                )
                            return duplicate_job.id, False, "duplicate"
                        return job_id, False, "duplicate"
                    raise
                except sqlite3.OperationalError as exc:
                    msg = str(exc).lower()
                    if "locked" in msg or "busy" in msg:
                        conn.rollback()
                        time.sleep(0.05 * (2**attempt))
                        continue
                    raise
        finally:
            conn.close()

    def claim_next_job(self, source, *, now=None):
        now = now or utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT 1 FROM download_jobs WHERE status=? AND source=? LIMIT 1",
                (JOB_STATUS_DOWNLOADING, source),
            )
            if cur.fetchone():
                conn.commit()
                return None
            cur.execute(
                """
                SELECT * FROM download_jobs
                WHERE status=? AND source=? AND (queued IS NULL OR queued<=?)
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (JOB_STATUS_QUEUED, source, now),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            job_id = row["id"]
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, claimed=?, updated_at=?
                WHERE id=? AND status=?
                """,
                (JOB_STATUS_CLAIMED, now, now, job_id, JOB_STATUS_QUEUED),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            updated_row = dict(row)
            updated_row["status"] = JOB_STATUS_CLAIMED
            updated_row["claimed"] = now
            updated_row["updated_at"] = now
            return self._row_to_job(updated_row)
        finally:
            conn.close()
            
    def mark_completed(self, job_id, *, file_path=None):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            if file_path:
                cur.execute(
                    """
                    UPDATE download_jobs
                    SET status=?, completed=?, updated_at=?, file_path=?
                    WHERE id=? AND status!=?
                    """,
                    (JOB_STATUS_COMPLETED, now, now, file_path, job_id, JOB_STATUS_FAILED),
                )
            else:
                cur.execute(
                    """
                    UPDATE download_jobs
                    SET status=?, completed=?, updated_at=?
                    WHERE id=? AND status!=?
                    """,
                    (JOB_STATUS_COMPLETED, now, now, job_id, JOB_STATUS_FAILED),
                )
            conn.commit()
        finally:
            conn.close()

    def mark_canceled(self, job_id, *, reason=None):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, canceled=?, updated_at=?, last_error=?
                WHERE id=?
                """,
                (JOB_STATUS_CANCELLED, now, now, reason, job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def cancel_active_jobs(self, *, reason=None):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, canceled=?, updated_at=?, last_error=?
                WHERE status IN (?, ?, ?, ?)
                """,
                (
                    JOB_STATUS_CANCELLED,
                    now,
                    now,
                    reason,
                    JOB_STATUS_QUEUED,
                    JOB_STATUS_CLAIMED,
                    JOB_STATUS_DOWNLOADING,
                    JOB_STATUS_POSTPROCESSING,
                ),
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def record_failure(self, job, *, error_message, retryable, retry_delay_seconds):
        attempts = job.attempts + 1
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            if retryable and attempts < job.max_attempts:
                next_ready = (datetime.now(timezone.utc) + timedelta(seconds=retry_delay_seconds))
                queued_at = next_ready.replace(microsecond=0).isoformat()
                cur.execute(
                    """
                    UPDATE download_jobs
                    SET status=?, queued=?, updated_at=?, attempts=?, last_error=?
                    WHERE id=?
                    """,
                    (JOB_STATUS_QUEUED, queued_at, now, attempts, error_message, job.id),
                )
                conn.commit()
                return JOB_STATUS_QUEUED

            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, failed=?, updated_at=?, attempts=?, last_error=?
                WHERE id=?
                """,
                (JOB_STATUS_FAILED, now, now, attempts, error_message, job.id),
            )
            conn.commit()
            return JOB_STATUS_FAILED
        finally:
            conn.close()

    def mark_downloading(self, job_id):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, downloading=?, updated_at=?,
                    progress_downloaded_bytes=NULL,
                    progress_total_bytes=NULL,
                    progress_percent=NULL,
                    progress_speed_bps=NULL,
                    progress_eta_seconds=NULL,
                    progress_updated_at=NULL
                WHERE id=?
                """,
                (JOB_STATUS_DOWNLOADING, now, now, job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def update_download_progress(
        self,
        job_id,
        *,
        downloaded_bytes=None,
        total_bytes=None,
        progress_percent=None,
        speed_bps=None,
        eta_seconds=None,
    ):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET progress_downloaded_bytes=?,
                    progress_total_bytes=?,
                    progress_percent=?,
                    progress_speed_bps=?,
                    progress_eta_seconds=?,
                    progress_updated_at=?,
                    updated_at=?
                WHERE id=? AND status IN (?, ?, ?)
                """,
                (
                    downloaded_bytes,
                    total_bytes,
                    progress_percent,
                    speed_bps,
                    eta_seconds,
                    now,
                    now,
                    job_id,
                    JOB_STATUS_DOWNLOADING,
                    JOB_STATUS_CLAIMED,
                    JOB_STATUS_POSTPROCESSING,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_postprocessing(self, job_id):
        now = utc_now()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE download_jobs
                SET status=?, postprocessing=?, updated_at=?
                WHERE id=? AND status!=?
                """,
                (JOB_STATUS_POSTPROCESSING, now, now, job_id, JOB_STATUS_FAILED),
            )
            conn.commit()
        finally:
            conn.close()


class DownloadWorkerEngine:
    def __init__(
        self,
        db_path,
        config,
        paths: EnginePaths,
        *,
        retry_delay_seconds=30,
        adapters=None,
        search_service=None,
    ):
        self.db_path = db_path
        self.config = config or {}
        self.paths = paths
        self.retry_delay_seconds = retry_delay_seconds
        self.store = DownloadJobStore(db_path)
        self.adapters = adapters or default_adapters()
        self.search_service = search_service
        # Ensure required DB tables exist (idempotent).
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            ensure_download_jobs_table(conn)
            ensure_downloads_table(conn)
        finally:
            conn.close()
        self._locks = {}
        self._locks_lock = threading.Lock()
        self._cancel_flags = {}
        self._cancel_lock = threading.Lock()

    def _extract_resolved_candidate(self, resolved):
        if not resolved:
            return None, None
        if isinstance(resolved, dict):
            return resolved.get("url"), resolved.get("source")
        url = getattr(resolved, "url", None)
        source = getattr(resolved, "source", None)
        return url, source

    def _music_tokens(self, value):
        return _WORD_TOKEN_RE.findall(str(value or "").lower())

    def _music_track_is_live(self, artist, track, album):
        return has_live_intent(artist, track, album)

    def _normalize_score_100(self, candidate):
        raw_score = candidate.get("adapter_score")
        if raw_score is None:
            raw_score = candidate.get("raw_score")
        if raw_score is None:
            raw_score = candidate.get("final_score")
        max_score = candidate.get("adapter_max_possible")
        if max_score is None:
            max_score = candidate.get("max_score")
        if max_score is None:
            max_score = 1.0
        try:
            raw_value = float(raw_score or 0.0)
            max_value = float(max_score or 0.0)
            if max_value <= 0:
                return 0.0
            normalized = (raw_value / max_value) * 100.0
            return max(0.0, min(100.0, normalized))
        except Exception:
            return 0.0

    def _build_music_track_query(self, artist, track, album=None, *, is_live=False):
        ladder = self._build_music_track_query_ladder(artist, track, album)
        return ladder[0]["query"] if ladder else ""

    def _build_music_track_query_ladder(self, artist, track, album=None):
        artist_v = str(artist or "").strip()
        track_v = str(track or "").strip()
        album_v = str(album or "").strip()
        relaxed_track = relaxed_search_title(track_v) or track_v
        ladder = [
            {"rung": 0, "label": "canonical_full", "query": " ".join(part for part in [f'"{artist_v}"', f'"{track_v}"', f'"{album_v}"' if album_v else ""] if part).strip()},
            {"rung": 1, "label": "canonical_no_album", "query": " ".join(part for part in [f'"{artist_v}"', f'"{track_v}"'] if part).strip()},
            {"rung": 2, "label": "relaxed_no_album", "query": " ".join(part for part in [f'"{artist_v}"', f'"{relaxed_track}"'] if part).strip()},
            {"rung": 3, "label": "official_audio_fallback", "query": " ".join(part for part in [artist_v, relaxed_track, "official audio"] if part).strip()},
            {"rung": 4, "label": "legacy_topic_fallback", "query": " ".join(part for part in [artist_v, "-", track_v, "topic"] if part).strip()},
            {"rung": 5, "label": "legacy_audio_fallback", "query": " ".join(part for part in [artist_v, "-", track_v, "audio"] if part).strip()},
        ]
        seen = set()
        unique_ladder = []
        for entry in ladder:
            query = str(entry.get("query") or "").strip()
            if not query or query in seen:
                continue
            seen.add(query)
            unique_ladder.append(entry)
        return unique_ladder

    def _music_track_adjust_score(self, expected, candidate, *, allow_live=False):
        title = str(candidate.get("title") or "")
        uploader = str(candidate.get("uploader") or candidate.get("artist_detected") or "")
        source = str(candidate.get("source") or "")
        title_tokens = self._music_tokens(title)
        uploader_tokens = set(self._music_tokens(uploader))
        track_tokens = set(self._music_tokens(expected.get("track")))
        artist_tokens = set(self._music_tokens(expected.get("artist")))
        expected_track_tokens = self._music_tokens(expected.get("track"))
        candidate_title_tokens = self._music_tokens(title)

        adjustment = 0.0
        reasons = []

        if track_tokens and track_tokens.issubset(set(title_tokens)):
            adjustment += 12.0
            reasons.append("exact_track_tokens")
        title_match_increment = 0.0
        if expected_track_tokens and candidate_title_tokens:
            if expected_track_tokens == candidate_title_tokens:
                title_match_increment = 25.0
            else:
                shared_count = len(set(expected_track_tokens) & set(candidate_title_tokens))
                title_match_increment = float(shared_count * 2)
            adjustment += title_match_increment
            reasons.append(f"title_match_{title_match_increment:.0f}")
            logger.debug(
                f"[MUSIC] title_match score_increase={title_match_increment:.0f} "
                f"for candidate={candidate.get('url')}"
            )

        if artist_tokens and uploader_tokens:
            overlap = len(artist_tokens & uploader_tokens) / max(len(artist_tokens), 1)
            if overlap >= 0.60:
                adjustment += 10.0
                reasons.append("artist_uploader_overlap_high")
            elif overlap >= 0.30:
                adjustment += 5.0
                reasons.append("artist_uploader_overlap")

        expected_duration = expected.get("duration_hint_sec")
        candidate_duration = candidate.get("duration_sec")
        try:
            if expected_duration is not None and candidate_duration is not None:
                diff_ms = abs((int(candidate_duration) * 1000) - (int(expected_duration) * 1000))
                duration_increment = 0.0
                if diff_ms <= 3000:
                    duration_increment = 20.0
                elif diff_ms <= 8000:
                    duration_increment = 10.0
                elif diff_ms <= 15000:
                    duration_increment = 5.0
                if duration_increment > 0.0:
                    adjustment += duration_increment
                    reasons.append(f"duration_bonus_{duration_increment:.0f}")
                logger.debug(
                    f"[MUSIC] duration_bonus diff={diff_ms} score={duration_increment:.0f}"
                )
        except Exception:
            pass

        title_lower = title.lower()
        if "provided to youtube" in title_lower:
            adjustment += 8.0
            reasons.append("provided_to_youtube")
        if "topic" in uploader.lower() and source in {"youtube", "youtube_music"}:
            adjustment += 8.0
            reasons.append("topic_channel")
        if "lyrics" in title_lower:
            adjustment += 2.0
            reasons.append("lyrics_hint")

        for token in _MUSIC_TRACK_PENALIZE_TOKENS:
            if allow_live and token == "live":
                continue
            if token in title_lower:
                adjustment -= 10.0
                reasons.append(f"penalty_{token}")
                logger.debug(
                    f"[MUSIC] penalizing token={token} new_score={adjustment:.0f} "
                    f"for {candidate.get('url')}"
                )
        return adjustment, reasons

    def _resolve_music_track_with_adapters(self, artist, track, album=None, *, duration_hint_sec=None, allow_live=False):
        expected = {
            "artist": artist,
            "track": track,
            "album": album,
            "duration_hint_sec": duration_hint_sec,
        }
        scored = []
        source_priority = [name for name in _MUSIC_TRACK_SOURCE_PRIORITY if name in self.adapters]
        source_priority.extend([name for name in self.adapters.keys() if name not in source_priority])
        query_ladder = self._build_music_track_query_ladder(artist, track, album)
        for source in source_priority:
            adapter = self.adapters.get(source)
            if not adapter:
                continue
            candidates = []
            for ladder_entry in query_ladder:
                query = str(ladder_entry.get("query") or "").strip()
                try:
                    candidates = adapter.search_music_track(query, 6)
                except Exception:
                    logging.exception("Music track search adapter failed source=%s query=%s", source, query)
                    candidates = []
                if candidates:
                    break
            for candidate in candidates or []:
                url = candidate.get("url") if isinstance(candidate, dict) else None
                if not _is_http_url(url):
                    continue
                candidate = dict(candidate)
                candidate["source"] = candidate.get("source") or source
                modifier = adapter.source_modifier(candidate)
                candidate.update(score_candidate(expected, candidate, source_modifier=modifier))
                base_score = self._normalize_score_100(candidate)
                source_weight = int(_MUSIC_SOURCE_PRIORITY_WEIGHTS.get(source, 0))
                logger.debug(f"[MUSIC] source_priority={source} weight={source_weight}")
                adjustment, reasons = self._music_track_adjust_score(expected, candidate, allow_live=allow_live)
                if source_weight:
                    adjustment += float(source_weight)
                    reasons.append(f"source_priority_{source_weight}")
                candidate["music_adjustment"] = adjustment
                candidate["music_adjustment_reasons"] = ",".join(reasons)
                candidate["base_score"] = base_score
                candidate["final_score_100"] = max(0.0, min(100.0, base_score + adjustment))
                candidate["final_score"] = candidate["final_score_100"] / 100.0
                scored.append(candidate)
        if not scored:
            return None
        ranked = rank_candidates(scored, source_priority=source_priority)
        for candidate in ranked:
            candidate_score = float(candidate.get("final_score") or 0.0)
            logger.debug(f"[MUSIC] threshold_used={_MUSIC_TRACK_THRESHOLD:.2f} candidate_score={candidate_score:.3f}")
            if candidate_score >= _MUSIC_TRACK_THRESHOLD:
                return candidate
        logger.warning(f"[MUSIC] top 5 candidates for track={track} scores:")
        for candidate in ranked[:5]:
            logger.warning(
                "  score=%.3f source=%s url=%s title=%s",
                float(candidate.get("final_score") or 0.0),
                candidate.get("source"),
                candidate.get("url"),
                candidate.get("title"),
            )
        return None

    def _resolve_music_track_job(self, job):
        payload = job.output_template if isinstance(job.output_template, dict) else {}
        canonical = payload.get("canonical_metadata") if isinstance(payload.get("canonical_metadata"), dict) else {}
        # Prefer bound MB canonical metadata for scoring expectations.
        artist = str(canonical.get("artist") or payload.get("artist") or "").strip()
        track = str(canonical.get("track") or canonical.get("title") or payload.get("track") or "").strip()
        album = str(canonical.get("album") or payload.get("album") or "").strip() or None
        recording_mbid, release_mbid = _extract_music_binding_ids(job)
        release_group_mbid = str(
            payload.get("mb_release_group_id")
            or payload.get("release_group_id")
            or canonical.get("mb_release_group_id")
            or canonical.get("release_group_id")
            or ""
        ).strip() or None
        duration_ms_raw = canonical.get("duration_ms")
        if duration_ms_raw is None:
            duration_ms_raw = payload.get("duration_ms")
        if duration_ms_raw is None:
            duration_ms_raw = canonical.get("duration")
        track_aliases_raw = (
            canonical.get("track_aliases")
            or canonical.get("title_aliases")
            or payload.get("track_aliases")
            or payload.get("title_aliases")
        )
        track_aliases = []
        if isinstance(track_aliases_raw, (list, tuple, set)):
            for value in track_aliases_raw:
                text = str(value or "").strip()
                if text:
                    track_aliases.append(text)
        track_disambiguation = str(
            canonical.get("track_disambiguation")
            or payload.get("track_disambiguation")
            or ""
        ).strip() or None
        mb_youtube_urls_raw = (
            canonical.get("mb_youtube_urls")
            or payload.get("mb_youtube_urls")
        )
        mb_youtube_urls = []
        if isinstance(mb_youtube_urls_raw, (list, tuple, set)):
            seen_urls = set()
            for value in mb_youtube_urls_raw:
                text = str(value or "").strip()
                if not text:
                    continue
                lowered = text.lower()
                if lowered in seen_urls:
                    continue
                seen_urls.add(lowered)
                mb_youtube_urls.append(text)
        if not mb_youtube_urls and recording_mbid:
            try:
                rels = get_musicbrainz_service().fetch_youtube_relationship_urls(
                    recording_mbid,
                    release_id=release_mbid,
                    limit=3,
                )
            except Exception:
                rels = []
            if isinstance(rels, list):
                seen_urls = set()
                for rel in rels:
                    if isinstance(rel, dict):
                        text = str(rel.get("url") or "").strip()
                    else:
                        text = str(rel or "").strip()
                    if not text:
                        continue
                    lowered = text.lower()
                    if lowered in seen_urls:
                        continue
                    seen_urls.add(lowered)
                    mb_youtube_urls.append(text)
        release_primary_type = str(
            canonical.get("release_primary_type")
            or canonical.get("primary_type")
            or payload.get("release_primary_type")
            or payload.get("primary_type")
            or ""
        ).strip()
        release_secondary_types_raw = (
            canonical.get("release_secondary_types")
            or payload.get("release_secondary_types")
        )
        release_secondary_types = []
        if isinstance(release_secondary_types_raw, (list, tuple, set)):
            for value in release_secondary_types_raw:
                text = str(value or "").strip()
                if text:
                    release_secondary_types.append(text)
        is_ep_release = _is_ep_release_context(release_primary_type, release_secondary_types)
        duration_hint_sec = None
        try:
            if duration_ms_raw is not None:
                duration_hint_sec = max(int(duration_ms_raw) // 1000, 1)
        except Exception:
            duration_hint_sec = None
        if not recording_mbid or not release_mbid:
            self.store.record_failure(
                job,
                error_message="music_track_binding_missing",
                retryable=False,
                retry_delay_seconds=self.retry_delay_seconds,
            )
            return None
        allow_live = self._music_track_is_live(artist, track, album)
        if not artist or not track:
            logging.error("Music track search failed")
            self.store.record_failure(
                job,
                error_message="music_track_metadata_missing",
                retryable=False,
                retry_delay_seconds=self.retry_delay_seconds,
            )
            return None
        logger.info(f"[WORKER] processing music_track artist={artist} track={track}")
        logger.info(
            "[MUSIC] job_ids recording_mbid=%s release_mbid=%s release_group_mbid=%s",
            recording_mbid,
            release_mbid,
            release_group_mbid,
        )

        search_query = self._build_music_track_query(artist, track, album, is_live=allow_live)
        logger.debug(f"[MUSIC] built search_query={search_query} for music_track")
        resolved = None
        retry_start_rung = 0
        track_total_value = None
        for candidate_total in (
            payload.get("track_total"),
            canonical.get("track_total"),
            payload.get("total_tracks"),
        ):
            try:
                if candidate_total is not None:
                    parsed_total = int(candidate_total)
                    if parsed_total > 0:
                        track_total_value = parsed_total
                        break
            except Exception:
                continue
        coherence_context = None
        if release_mbid and track_total_value and track_total_value > 1:
            coherence_context = {
                "mb_release_id": release_mbid,
                "mb_release_group_id": release_group_mbid,
                "track_total": track_total_value,
            }
        if isinstance(job.last_error, str) and (
            "duration_filtered" in job.last_error
            or "no_candidate_above_threshold" in job.last_error
            or "all_filtered_by_gate" in job.last_error
            or "album_similarity_blocked" in job.last_error
            or "no_candidates_retrieved" in job.last_error
        ):
            try:
                retry_start_rung = max(0, min(int(job.attempts or 0), 3))
            except Exception:
                retry_start_rung = 0
        # Music-track acquisition must go through SearchService for deterministic orchestration/logging.
        if self.search_service:
            try:
                resolved = self.search_service.search_music_track_best_match(
                    artist,
                    track,
                    album=album,
                    duration_ms=(duration_hint_sec * 1000) if duration_hint_sec else None,
                    limit=6,
                    start_rung=retry_start_rung,
                    coherence_context=coherence_context,
                    track_aliases=track_aliases,
                    track_disambiguation=track_disambiguation,
                    mb_youtube_urls=mb_youtube_urls,
                    recording_mbid=recording_mbid,
                    is_ep_release=is_ep_release,
                )
            except Exception as exc:
                logging.exception("Music track search service failed query=%s", search_query)
                retryable = is_retryable_error(exc)
                _log_event(
                    logging.ERROR,
                    "music_track_adapter_search_failed",
                    job_id=job.id,
                    source=job.source,
                    failure_domain="adapter_search",
                    error_message=str(exc),
                    candidate_id=None,
                    retryable=retryable,
                )
                self.store.record_failure(
                    job,
                    error_message=f"music_track_adapter_search_exception:{exc}",
                    retryable=retryable,
                    retry_delay_seconds=self.retry_delay_seconds,
                )
                return None

        resolved_url, resolved_source = self._extract_resolved_candidate(resolved)
        search_meta = getattr(self.search_service, "last_music_track_search", {}) if self.search_service else {}
        if isinstance(search_meta, dict):
            injected_rejections = search_meta.get("mb_injected_rejections")
            injected_selected = bool(search_meta.get("mb_injected_selected"))
            injected_candidates = int(search_meta.get("mb_injected_candidates") or 0)
            album_success_count = int(search_meta.get("mb_injected_album_success_count") or 0)
            try:
                self.store.merge_output_template_fields(
                    job.id,
                    {
                        "runtime_search_meta": {
                            "failure_reason": search_meta.get("failure_reason"),
                            "mb_injected_candidates": injected_candidates,
                            "mb_injected_selected": injected_selected,
                            "mb_injected_rejections": injected_rejections or {},
                            "mb_injected_album_success_count": album_success_count,
                            "ep_refinement_attempted": bool(search_meta.get("ep_refinement_attempted")),
                            "ep_refinement_candidates_considered": int(search_meta.get("ep_refinement_candidates_considered") or 0),
                            "decision_edge": search_meta.get("decision_edge") if isinstance(search_meta.get("decision_edge"), dict) else {},
                        }
                    },
                )
            except Exception:
                logger.exception("[MUSIC] failed to persist runtime_search_meta job_id=%s", job.id)
            if injected_candidates > 0 or injected_selected or injected_rejections:
                _log_event(
                    logging.INFO,
                    "music_track_mb_injected_outcome",
                    job_id=job.id,
                    recording_mbid=recording_mbid,
                    release_mbid=release_mbid,
                    injected_candidates=injected_candidates,
                    injected_selected=injected_selected,
                    injected_rejections=injected_rejections or {},
                    album_run_success_count=album_success_count,
                )
        if not _is_http_url(resolved_url):
            failure_reason = str((search_meta or {}).get("failure_reason") or "").strip()
            if not failure_reason and isinstance(job.last_error, str):
                unavailable_class = _classify_ytdlp_unavailability(job.last_error)
                if "yt_dlp_source_unavailable:" in job.last_error.lower() and unavailable_class:
                    failure_reason = f"source_unavailable:{unavailable_class}"
            failure_reason = failure_reason or "all_filtered_by_gate"
            review_job_enqueued = False
            if self._review_quarantine_enabled_for_job(job, payload):
                review_candidate = self._select_low_confidence_review_candidate(search_meta)
                if isinstance(review_candidate, dict):
                    review_job_enqueued = self._enqueue_low_confidence_review_job(
                        job=job,
                        payload=payload,
                        canonical=canonical,
                        review_candidate=review_candidate,
                        failure_reason=failure_reason,
                    )
                    if review_job_enqueued:
                        try:
                            self.store.merge_output_template_fields(
                                job.id,
                                {
                                    "runtime_search_meta": {
                                        "review_job_enqueued": True,
                                        "review_candidate_id": review_candidate.get("candidate_id"),
                                        "review_candidate_url": review_candidate.get("url"),
                                        "review_candidate_gate": review_candidate.get("top_failed_gate"),
                                    }
                                },
                            )
                        except Exception:
                            logger.exception("[MUSIC] failed to persist review enqueue metadata job_id=%s", job.id)
            retryable_no_candidate = failure_reason in {
                "duration_filtered",
                "no_candidate_above_threshold",
                "all_filtered_by_gate",
                "album_similarity_blocked",
                "no_candidates_retrieved",
            }
            if review_job_enqueued:
                retryable_no_candidate = False
            logging.error("Music track search failed")
            self.store.record_failure(
                job,
                error_message=failure_reason,
                retryable=retryable_no_candidate,
                retry_delay_seconds=self.retry_delay_seconds,
            )
            return None
        selected_score = None
        duration_delta_ms = None
        if isinstance(resolved, dict):
            selected_score = resolved.get("final_score")
            try:
                resolved_duration = resolved.get("duration_ms")
                if resolved_duration is None and resolved.get("duration_sec") is not None:
                    resolved_duration = int(resolved.get("duration_sec")) * 1000
                if resolved_duration is not None and duration_hint_sec is not None:
                    duration_delta_ms = abs(int(resolved_duration) - (int(duration_hint_sec) * 1000))
            except Exception:
                duration_delta_ms = None
        logger.debug(
            f"[MUSIC] threshold={_MUSIC_TRACK_THRESHOLD:.2f} "
            f"selected_score={selected_score if selected_score is not None else 'n/a'} "
            f"candidate={resolved_url}"
        )
        logger.debug("[MUSIC] selected_duration_delta_ms=%s", duration_delta_ms)

        source = resolved_source or resolve_source(resolved_url)
        if str(source or "").strip().lower() == "mb_relationship":
            source = resolve_source(resolved_url)
        candidate_id = None
        if isinstance(resolved, dict):
            candidate_id = resolved.get("candidate_id")
        logger.info(
            '[MUSIC] acquisition recording_mbid=%s release_mbid=%s search_query="%s" source=%s candidate_id=%s duration_delta_ms=%s final_path=%s',
            recording_mbid,
            release_mbid,
            search_query,
            source,
            candidate_id,
            duration_delta_ms,
            "<pending>",
        )
        external_id = extract_video_id(resolved_url) if source in {"youtube", "youtube_music"} else None
        canonical_url = canonicalize_url(source, resolved_url, external_id)
        return replace(
            job,
            source=source,
            url=resolved_url,
            input_url=resolved_url,
            canonical_url=canonical_url,
            external_id=external_id,
        )

    def _review_quarantine_enabled_for_job(self, job, payload):
        if not bool(self.config.get("music_low_confidence_review_enabled", True)):
            return False
        origin = str(getattr(job, "origin", "") or "").strip().lower()
        if origin != "import":
            return False
        if not isinstance(payload, dict):
            return False
        return bool(str(payload.get("import_batch_id") or payload.get("import_batch") or "").strip())

    def _select_low_confidence_review_candidate(self, search_meta):
        if not isinstance(search_meta, dict):
            return None
        edge = search_meta.get("decision_edge")
        if not isinstance(edge, dict):
            return None
        rejected = edge.get("rejected_candidates")
        if not isinstance(rejected, list):
            return None

        def _as_float(value, default):
            try:
                return float(value)
            except Exception:
                return float(default)

        def _as_int(value, default):
            try:
                return int(value)
            except Exception:
                return int(default)

        def _margin(item):
            metric = item.get("nearest_pass_margin") if isinstance(item.get("nearest_pass_margin"), dict) else {}
            return _as_float(metric.get("margin_to_pass"), 1e9)

        def _eligible(item):
            if not isinstance(item, dict):
                return False
            if not _is_http_url(item.get("url")):
                return False
            reason = str(item.get("rejection_reason") or "").strip().lower()
            gate = str(item.get("top_failed_gate") or "").strip().lower()
            if gate == "variant_alignment":
                return False
            if reason in {"disallowed_variant", "preview_variant", "session_variant", "cover_artist_mismatch"}:
                return False
            margin_value = _margin(item)
            if gate in {"score_threshold", "title_similarity", "artist_similarity", "album_similarity"}:
                return margin_value <= 0.08
            if gate in {"duration_delta_ms", "duration_hard_cap_ms"}:
                return margin_value <= 3000.0
            if gate == "authority_channel_match":
                return (
                    _as_float(item.get("title_similarity"), 0.0) >= 0.94
                    and _as_float(item.get("artist_similarity"), 0.0) >= 0.94
                    and _as_int(item.get("duration_delta_ms"), 999999) <= 8000
                )
            return False

        eligible = [item for item in rejected if _eligible(item)]
        if not eligible:
            return None
        ranked = sorted(
            eligible,
            key=lambda item: (
                _margin(item),
                -_as_float(item.get("final_score"), 0.0),
                str(item.get("candidate_id") or ""),
            ),
        )
        return ranked[0] if ranked else None

    def _enqueue_low_confidence_review_job(self, *, job, payload, canonical, review_candidate, failure_reason):
        if not isinstance(payload, dict):
            payload = {}
        if not isinstance(canonical, dict):
            canonical = {}
        candidate_url = str(review_candidate.get("url") or "").strip()
        if not _is_http_url(candidate_url):
            return False

        recording_mbid = str(
            canonical.get("recording_mbid")
            or canonical.get("mb_recording_id")
            or payload.get("recording_mbid")
            or payload.get("mb_recording_id")
            or ""
        ).strip()
        candidate_id = str(review_candidate.get("candidate_id") or "").strip()
        review_key = candidate_id or hashlib.sha1(candidate_url.encode("utf-8")).hexdigest()[:12]
        review_canonical_id = f"review:{recording_mbid or job.id}:{review_key}"

        music_root = resolve_dir(self.config.get("music_download_folder"), self.paths.single_downloads_dir)
        needs_review_root = os.path.join(music_root, "Music", _MUSIC_REVIEW_FOLDER_NAME)
        review_metadata = {
            "artist": str(canonical.get("artist") or payload.get("artist") or "").strip() or "Unknown Artist",
            "track": str(canonical.get("track") or payload.get("track") or "").strip() or "Unknown Track",
            "album": _MUSIC_REVIEW_FOLDER_NAME,
            "album_artist": str(
                canonical.get("album_artist")
                or canonical.get("artist")
                or payload.get("artist")
                or ""
            ).strip() or "Unknown Artist",
            "release_date": str(canonical.get("release_date") or payload.get("release_date") or "0000").strip() or "0000",
            "track_number": canonical.get("track_number") or payload.get("track_number") or 1,
            "disc_number": canonical.get("disc_number") or payload.get("disc_number") or 1,
            "duration_ms": canonical.get("duration_ms") or payload.get("duration_ms"),
            "recording_mbid": recording_mbid or None,
            "mb_recording_id": recording_mbid or None,
            "mb_release_id": canonical.get("mb_release_id") or payload.get("mb_release_id"),
            "mb_release_group_id": canonical.get("mb_release_group_id") or payload.get("mb_release_group_id"),
        }
        final_format_override = payload.get("music_final_format") or self.config.get("music_final_format") or "mp3"
        review_source = resolve_source(candidate_url)
        external_id = extract_video_id(candidate_url) if review_source in {"youtube", "youtube_music"} else None
        canonical_url = canonicalize_url(review_source, candidate_url, external_id)
        enqueue_payload = build_download_job_payload(
            config=self.config if isinstance(self.config, dict) else {},
            origin="music_review",
            origin_id=str(getattr(job, "origin_id", "") or "").strip() or job.id,
            media_type="music",
            media_intent="music_track_review",
            source=review_source,
            url=candidate_url,
            input_url=candidate_url,
            destination=needs_review_root,
            base_dir=self.paths.single_downloads_dir,
            final_format_override=final_format_override,
            resolved_metadata=review_metadata,
            output_template_overrides={
                "kind": "music_track_review",
                "source": "music_review",
                "import_batch": payload.get("import_batch"),
                "import_batch_id": payload.get("import_batch_id"),
                "review_candidate_id": review_candidate.get("candidate_id"),
                "review_candidate_url": candidate_url,
                "review_failure_reason": failure_reason,
                "review_top_failed_gate": review_candidate.get("top_failed_gate"),
                "review_nearest_pass_margin": review_candidate.get("nearest_pass_margin")
                if isinstance(review_candidate.get("nearest_pass_margin"), dict)
                else {},
            },
            canonical_id=review_canonical_id,
            canonical_url=canonical_url,
            external_id=external_id,
        )
        _job_id, created, _reason = self.store.enqueue_job(**enqueue_payload)
        if created:
            _log_event(
                logging.INFO,
                "music_review_job_enqueued",
                failed_job_id=job.id,
                review_canonical_id=review_canonical_id,
                candidate_id=review_candidate.get("candidate_id"),
                candidate_url=candidate_url,
                top_failed_gate=review_candidate.get("top_failed_gate"),
                failure_reason=failure_reason,
            )
        return bool(created)

    def run_once(self, *, stop_event=None):
        sources = self.store.list_sources_with_queued_jobs()
        threads = []
        for source in sources:
            if stop_event and stop_event.is_set():
                break
            lock = self._get_source_lock(source)
            if not lock.acquire(blocking=False):
                continue
            thread = threading.Thread(
                target=self._run_source_once,
                args=(source, lock, stop_event),
                daemon=False,
            )
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def run_loop(self, *, poll_seconds=5, stop_event=None):
        json_sanity_check()
        _log_event(logging.INFO, "worker_started")
        _log_event(logging.INFO, "queue_polling_started")
        while True:
            if stop_event and stop_event.is_set():
                return
            self.run_once(stop_event=stop_event)
            time.sleep(poll_seconds)

    def _get_source_lock(self, source):
        with self._locks_lock:
            lock = self._locks.get(source)
            if not lock:
                lock = threading.Lock()
                self._locks[source] = lock
            return lock

    def cancel_job(self, job_id: str, *, reason: str | None = None) -> bool:
        """
        Request cancellation of a specific job.
        - Marks the job as CANCELLED in the DB (so UI updates immediately).
        - If the job is actively downloading, the progress hook will abort the yt-dlp run.
        """
        reason = reason or "Cancelled by user"
        try:
            # Mark cancelled immediately for UI/state correctness.
            self.store.mark_canceled(job_id, reason=reason)
        except Exception:
            logging.exception("Failed to mark job cancelled in store: %s", job_id)

        with self._cancel_lock:
            evt = self._cancel_flags.get(job_id)
            if evt is None:
                evt = threading.Event()
                self._cancel_flags[job_id] = evt
            evt.set()
        return True

    def _is_job_cancelled(self, job_id: str, stop_event: threading.Event | None) -> bool:
        if stop_event and stop_event.is_set():
            return True
        try:
            if self.store.get_job_status(job_id) == JOB_STATUS_CANCELLED:
                return True
        except Exception:
            pass
        with self._cancel_lock:
            evt = self._cancel_flags.get(job_id)
            return bool(evt and evt.is_set())

    def _run_source_once(self, source, lock, stop_event):
        try:
            if stop_event and stop_event.is_set():
                return
            job = self.store.claim_next_job(source)
            if not job:
                return
            # If a cancel request came in while this job was queued, honor it immediately.
            if self._is_job_cancelled(job.id, stop_event):
                try:
                    self.store.mark_canceled(job.id, reason="Cancelled by user")
                    _log_event(logging.INFO, "job_cancelled", job_id=job.id, trace_id=job.trace_id, source=job.source)
                except Exception as persist_exc:
                    logging.error(
                        "[WORKER] persistence_failed job_id=%s status=%s err=%s",
                        job.id,
                        JOB_STATUS_CANCELLED,
                        persist_exc,
                    )
                    try:
                        self.store.record_failure(
                            job,
                            error_message=f"cancel_persistence_failed:{persist_exc}",
                            retryable=False,
                            retry_delay_seconds=self.retry_delay_seconds,
                        )
                    except Exception as fallback_exc:
                        logging.error(
                            "[WORKER] persistence_failed job_id=%s status=%s err=%s",
                            job.id,
                            JOB_STATUS_FAILED,
                            fallback_exc,
                        )
                return
            _log_event(
                logging.INFO,
                "job_claimed",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
            )
            self._execute_job(job, stop_event=stop_event)
        finally:
            lock.release()

    def _write_album_run_summary_artifact(self, job, *, final_path=None):
        try:
            if str(getattr(job, "origin", "") or "").strip().lower() != "music_album":
                return
            if str(getattr(job, "media_intent", "") or "").strip().lower() != "music_track":
                return
            album_run_id = str(getattr(job, "origin_id", "") or "").strip()
            if not album_run_id:
                return
            explicit_output_dir = _album_output_dir_from_track_path(final_path)
            if not explicit_output_dir:
                explicit_output_dir = _album_output_dir_from_job(job)
            write_music_album_run_summary(
                self.db_path,
                album_run_id,
                output_dir=explicit_output_dir,
            )
        except Exception:
            logger.exception("[MUSIC] album run summary write failed job_id=%s", getattr(job, "id", None))

    def _execute_job(self, job, *, stop_event=None):
        if hasattr(job, "keys"):
            job_keys = list(job.keys())
        else:
            try:
                job_keys = list(vars(job).keys())
            except Exception:
                job_keys = []
        logger.debug(f"[WORKER] received job payload keys={job_keys}")
        if job.status != JOB_STATUS_CLAIMED:
            _log_event(
                logging.ERROR,
                "job_not_running",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
            )
            return
        if hasattr(job, "get"):
            intent = job.get("media_intent") or job.get("payload", {}).get("media_intent")
        else:
            payload = getattr(job, "payload", {}) or {}
            if not isinstance(payload, dict):
                payload = {}
            intent = getattr(job, "media_intent", None) or payload.get("media_intent")

        try:
            if self._is_job_cancelled(job.id, stop_event):
                self.store.mark_canceled(job.id, reason="Cancelled by user")
                self._write_album_run_summary_artifact(job)
                _log_event(
                    logging.INFO,
                    "job_cancelled",
                    job_id=job.id,
                    trace_id=job.trace_id,
                    source=job.source,
                    origin=job.origin,
                    media_intent=job.media_intent,
                )
                return
            if intent == "music_track":
                logger.info(f"[WORKER] processing music_track: {job}")
                recording_mbid, release_mbid = _extract_music_binding_ids(job)
                if not recording_mbid or not release_mbid:
                    self.store.record_failure(
                        job,
                        error_message="music_track_binding_missing",
                        retryable=False,
                        retry_delay_seconds=self.retry_delay_seconds,
                    )
                    self._write_album_run_summary_artifact(job)
                    return
                unresolved_job = job
                job = self._resolve_music_track_job(job)
                if job is None:
                    self._write_album_run_summary_artifact(unresolved_job)
                    return
            adapter = self.adapters.get(job.source)
            if not adapter:
                _log_event(
                    logging.ERROR,
                    "adapter_missing",
                    job_id=job.id,
                    trace_id=job.trace_id,
                    source=job.source,
                )
                self.store.record_failure(
                    job,
                    error_message=f"adapter_missing:{job.source}",
                    retryable=False,
                    retry_delay_seconds=self.retry_delay_seconds,
                )
                self._write_album_run_summary_artifact(job)
                return
            self.store.mark_downloading(job.id)
            _log_event(
                logging.INFO,
                "job_started",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
            )
            last_progress_write = {"ts": 0.0}

            def _on_progress(snapshot):
                now = time.time()
                # Throttle DB writes to avoid excessive sqlite contention.
                if now - last_progress_write["ts"] < 0.4:
                    return
                last_progress_write["ts"] = now
                self.store.update_download_progress(
                    job.id,
                    downloaded_bytes=snapshot.get("downloaded_bytes"),
                    total_bytes=snapshot.get("total_bytes"),
                    progress_percent=snapshot.get("progress_percent"),
                    speed_bps=snapshot.get("speed_bps"),
                    eta_seconds=snapshot.get("eta_seconds"),
                )

            result = adapter.execute(
                job,
                self.config,
                self.paths,
                stop_event=None,
                cancel_check=lambda: self._is_job_cancelled(job.id, stop_event),
                cancel_reason="Cancelled by user",
                media_type=job.media_type,
                media_intent=job.media_intent,
                progress_callback=_on_progress,
            )
            if not result:
                raise RuntimeError("adapter_execute_failed")
            final_path, meta = result
            if self.store.get_job_status(job.id) == JOB_STATUS_FAILED:
                return
            if self._is_job_cancelled(job.id, stop_event):
                self.store.mark_canceled(job.id, reason="Cancelled by user")
                self._write_album_run_summary_artifact(job)
                _log_event(
                    logging.INFO,
                    "job_cancelled",
                    job_id=job.id,
                    trace_id=job.trace_id,
                    source=job.source,
                    origin=job.origin,
                    media_intent=job.media_intent,
                )
                return
            self.store.mark_postprocessing(job.id)
            record_download_history(
                self.db_path,
                job,
                final_path,
                meta=meta,
            )
            self.store.mark_completed(job.id, file_path=final_path)
            self._write_album_run_summary_artifact(job, final_path=final_path)
            _log_event(
                logging.INFO,
                "job_completed",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
                path=final_path,
            )
        except Exception as exc:
            if isinstance(exc, CancelledError):
                try:
                    self.store.mark_canceled(job.id, reason=str(exc) or "Cancelled by user")
                    self._write_album_run_summary_artifact(job)
                    _log_event(
                        logging.INFO,
                        "job_cancelled",
                        job_id=job.id,
                        trace_id=job.trace_id,
                        source=job.source,
                        origin=job.origin,
                        media_intent=job.media_intent,
                    )
                except Exception as persist_exc:
                    logging.error(
                        "[WORKER] persistence_failed job_id=%s status=%s err=%s",
                        job.id,
                        JOB_STATUS_CANCELLED,
                        persist_exc,
                    )
                    try:
                        self.store.record_failure(
                            job,
                            error_message=f"cancel_persistence_failed:{persist_exc}",
                            retryable=False,
                            retry_delay_seconds=self.retry_delay_seconds,
                        )
                    except Exception as fallback_exc:
                        logging.error(
                            "[WORKER] persistence_failed job_id=%s status=%s err=%s",
                            job.id,
                            JOB_STATUS_FAILED,
                            fallback_exc,
                        )
                return
            if (
                isinstance(exc, TypeError)
                and "set" in str(exc)
                and "JSON" in str(exc)
            ):
                _log_event(
                    logging.ERROR,
                    "json_set_serialization_trace",
                    job_id=job.id,
                    url=job.url,
                    error=str(exc),
                    traceback=traceback.format_exc(),
                )
            error_message = f"{type(exc).__name__}: {exc}"
            retryable = is_retryable_error(exc)
            error_text_lower = error_message.lower()
            unavailable_class = _classify_ytdlp_unavailability(error_message)
            if "yt_dlp_source_unavailable:" in error_text_lower or unavailable_class:
                failure_domain = "source_unavailable"
            elif "metadata_probe" in error_text_lower or "yt_dlp_metadata_probe_failed" in error_text_lower:
                failure_domain = "metadata_probe"
            elif "adapter" in error_text_lower or "search" in error_text_lower:
                failure_domain = "adapter_search"
            else:
                failure_domain = "download_execution"
            candidate_id = getattr(job, "external_id", None) or extract_video_id(getattr(job, "url", None))
            try:
                new_status = self.store.record_failure(
                    job,
                    error_message=error_message,
                    retryable=retryable,
                    retry_delay_seconds=self.retry_delay_seconds,
                )
                self._write_album_run_summary_artifact(job)
            except Exception as persist_exc:
                logging.error(
                    "[WORKER] persistence_failed job_id=%s status=%s err=%s",
                    job.id,
                    JOB_STATUS_FAILED,
                    persist_exc,
                )
                return
            _log_event(
                logging.ERROR,
                "job_failed",
                job_id=job.id,
                trace_id=job.trace_id,
                source=job.source,
                origin=job.origin,
                media_intent=job.media_intent,
                retryable=retryable,
                status=new_status,
                error=error_message,
                failure_domain=failure_domain,
                error_message=error_message,
                candidate_id=candidate_id,
                unavailable_class=unavailable_class,
            )


class YouTubeAdapter:
    _missing_final_format_warned = False

    def execute(
        self,
        job,
        config,
        paths,
        *,
        stop_event=None,
        cancel_check=None,
        cancel_reason=None,
        media_type=None,
        media_intent=None,
        progress_callback=None,
    ):
        output_template = job.output_template if isinstance(job.output_template, dict) else None
        if output_template is None:
            logger.error(
                "[WORKER] invariant_failed job_id=%s missing_output_template",
                getattr(job, "id", None),
            )
            raise RuntimeError("invariant_missing_output_template")
        effective_media_type = media_type or getattr(job, "media_type", None)
        if not effective_media_type:
            logger.error(
                "[WORKER] invariant_failed job_id=%s missing_media_type",
                getattr(job, "id", None),
            )
            raise RuntimeError("invariant_missing_media_type")
        output_dir = output_template.get("output_dir") or paths.single_downloads_dir
        raw_final_format = output_template.get("final_format")
        if raw_final_format is None and isinstance(config, dict):
            raw_final_format = config.get("final_format")
            if raw_final_format is not None:
                output_template["final_format"] = raw_final_format
                if not self._missing_final_format_warned:
                    logger.warning(
                        "[WORKER] missing final_format in job output_template; falling back to config.final_format=%s",
                        raw_final_format,
                    )
                    self._missing_final_format_warned = True
        # Strict separation:
        # - music_mode controls whether we run music metadata enrichment.
        # - audio_only controls whether we download audio-only / extract audio via ffmpeg.
        # IMPORTANT: Do NOT let a global/default final_format="mp3" force *video* jobs into audio-only.
        music_mode = is_music_media_type(effective_media_type)
        audio_only_requested = bool(output_template.get("audio_only")) or bool(output_template.get("audio_mode"))
        audio_mode = True if music_mode else bool(audio_only_requested)

        # If we're in audio_mode, final_format is the requested audio codec (mp3/m4a/flac).
        # Otherwise final_format is treated as a container preference (webm/mp4/mkv) or None.
        if audio_mode:
            final_format = _resolve_target_audio_format(
                {"final_format": raw_final_format},
                config,
                output_template,
            )
        else:
            final_format = _resolve_target_video_container(
                {"final_format": raw_final_format},
                config,
                output_template,
            )
        if final_format is None:
            logger.error(
                "[WORKER] invariant_failed job_id=%s unresolved_final_format",
                getattr(job, "id", None),
            )
            raise RuntimeError("invariant_missing_final_format")
        filename_template = output_template.get("filename_template")
        audio_template = output_template.get("audio_filename_template")

        resolved_dir = resolve_dir(output_dir, paths.single_downloads_dir)
        temp_dir = os.path.join(paths.temp_downloads_dir, job.id)
        os.makedirs(temp_dir, exist_ok=True)

        try:
            cookie_file = resolve_cookie_file(config)
            info, local_file = download_with_ytdlp(
                job.url,
                temp_dir,
                config,
                audio_mode=audio_mode,
                final_format=final_format,
                cookie_file=cookie_file,
                stop_event=stop_event,
                media_type=effective_media_type,
                media_intent=media_intent,
                job_id=job.id,
                origin=job.origin,
                resolved_destination=job.resolved_destination,
                cancel_check=cancel_check,
                cancel_reason=cancel_reason,
                output_template_meta=output_template,
                progress_callback=progress_callback,
            )
            if not info or not local_file:
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            meta = extract_meta(info, fallback_url=job.url)
            if not audio_mode:
                meta = _hydrate_meta_from_output_template(meta, output_template)
                meta = _hydrate_meta_from_local_filename(
                    meta,
                    local_file=local_file,
                    fallback_id=(
                        getattr(job, "external_id", None)
                        or extract_video_id(getattr(job, "url", None))
                        or getattr(job, "id", None)
                    ),
                )
            canonical = (
                output_template.get("canonical_metadata")
                if isinstance(output_template.get("canonical_metadata"), dict)
                else {}
            )
            recording_mbid = str(
                output_template.get("recording_mbid")
                or output_template.get("mb_recording_id")
                or canonical.get("recording_mbid")
                or canonical.get("mb_recording_id")
                or ""
            ).strip()
            if recording_mbid:
                meta["recording_mbid"] = recording_mbid
                meta["mb_recording_id"] = recording_mbid
            release_mbid = str(
                output_template.get("mb_release_id")
                or output_template.get("release_id")
                or canonical.get("mb_release_id")
                or canonical.get("release_id")
                or ""
            ).strip()
            if release_mbid:
                meta["mb_release_id"] = release_mbid
            release_group_mbid = str(
                output_template.get("mb_release_group_id")
                or output_template.get("release_group_id")
                or canonical.get("mb_release_group_id")
                or canonical.get("release_group_id")
                or ""
            ).strip()
            if release_group_mbid:
                meta["mb_release_group_id"] = release_group_mbid
            effective_media_intent = media_intent or getattr(job, "media_intent", None)
            if is_music_media_type(effective_media_type) and str(effective_media_intent or "").strip().lower() == "music_track":
                pre_fields = _release_fields_from_template(output_template, canonical)
                if not _release_fields_complete(pre_fields):
                    _log_event(
                        logging.WARNING,
                        "missing_bound_release_metadata",
                        job_id=getattr(job, "id", None),
                        recording_mbid=recording_mbid,
                    )
                _ensure_release_enriched(job)
                refreshed_template = job.output_template if isinstance(job.output_template, dict) else {}
                refreshed_canonical = (
                    refreshed_template.get("canonical_metadata")
                    if isinstance(refreshed_template.get("canonical_metadata"), dict)
                    else {}
                )
                # For music_track jobs, canonical path/tag fields come from bound canonical metadata only.
                canonical_artist = refreshed_canonical.get("artist")
                canonical_album_artist = refreshed_canonical.get("album_artist") or canonical_artist
                canonical_track = refreshed_canonical.get("track") or refreshed_canonical.get("title")
                if canonical_artist:
                    meta["artist"] = canonical_artist
                if canonical_album_artist:
                    meta["album_artist"] = canonical_album_artist
                if canonical_track:
                    meta["track"] = canonical_track
                    meta["title"] = canonical_track
                meta["album"] = refreshed_canonical.get("album")
                meta["release_date"] = refreshed_canonical.get("release_date")
                meta["track_number"] = refreshed_canonical.get("track_number")
                meta["disc_number"] = refreshed_canonical.get("disc_number")
                meta["track_total"] = refreshed_canonical.get("track_total")
                meta["disc_total"] = refreshed_canonical.get("disc_total")
                meta["genre"] = refreshed_canonical.get("genre")
                meta["artwork_url"] = (
                    refreshed_canonical.get("artwork_url")
                    or refreshed_template.get("artwork_url")
                    or meta.get("artwork_url")
                )
                meta["mb_release_id"] = refreshed_canonical.get("mb_release_id")
                meta["mb_release_group_id"] = refreshed_canonical.get("mb_release_group_id")
            video_id = meta.get("video_id") or job.id
            template = audio_template if audio_mode else filename_template
            enforce_music_contract = bool(
                audio_mode
                and is_music_media_type(effective_media_type)
                and str(effective_media_intent or "").strip().lower() == "music_track"
            )

            if stop_event and stop_event.is_set():
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            final_path, meta = finalize_download_artifact(
                local_file=local_file,
                meta=meta,
                fallback_id=video_id,
                destination_dir=resolved_dir,
                audio_mode=audio_mode,
                final_format=final_format,
                template=template,
                paths=paths,
                config=config if isinstance(config, dict) else {},
                enforce_music_contract=enforce_music_contract,
                enqueue_audio_metadata=bool(music_mode),
            )
            runtime_media_profile = meta.get("runtime_media_profile") if isinstance(meta.get("runtime_media_profile"), dict) else None
            if runtime_media_profile:
                self.store.merge_output_template_fields(
                    job.id,
                    {"runtime_media_profile": runtime_media_profile},
                )
            logger.info(f"[MUSIC] finalized file: {final_path}")
            shutil.rmtree(temp_dir, ignore_errors=True)

            size = None
            try:
                size = os.path.getsize(final_path)
            except OSError:
                size = None
            if size is None or size == 0:
                try:
                    os.remove(final_path)
                except OSError:
                    pass
                raise RuntimeError("empty_output_file")

            return final_path, meta
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise


def default_adapters():
    adapter = YouTubeAdapter()
    return {
        "youtube": adapter,
        "youtube_music": adapter,
        "soundcloud": adapter,
        "bandcamp": adapter,
        "direct": adapter,
        "unknown": adapter,
    }


def _extract_release_value(output_template, canonical, *keys):
    for key in keys:
        value = canonical.get(key) if isinstance(canonical, dict) else None
        if value is None and isinstance(output_template, dict):
            value = output_template.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
        if value in (None, ""):
            continue
        return value
    return None


def _normalize_positive_int(value):
    parsed = normalize_track_number(value)
    if parsed is None:
        return None
    return parsed if parsed > 0 else None


def _release_fields_from_template(output_template, canonical):
    album = _extract_release_value(output_template, canonical, "album")
    release_date = _extract_release_value(output_template, canonical, "release_date", "date")
    track_number = _normalize_positive_int(
        _extract_release_value(output_template, canonical, "track_number", "track_num")
    )
    disc_number = _normalize_positive_int(
        _extract_release_value(output_template, canonical, "disc_number", "disc", "disc_num")
    )
    mb_release_id = _extract_release_value(output_template, canonical, "mb_release_id", "release_id")
    mb_release_group_id = _extract_release_value(
        output_template,
        canonical,
        "mb_release_group_id",
        "release_group_id",
    )
    return {
        "album": album,
        "release_date": release_date,
        "track_number": track_number,
        "disc_number": disc_number,
        "mb_release_id": mb_release_id,
        "mb_release_group_id": mb_release_group_id,
    }


def _release_fields_complete(fields):
    return all(
        fields.get(key) not in (None, "")
        for key in (
            "album",
            "release_date",
            "track_number",
            "disc_number",
            "mb_release_id",
            "mb_release_group_id",
        )
    )


def _fetch_release_enrichment(recording_mbid: str, release_id_hint: Optional[str]) -> dict:
    recording_mbid = str(recording_mbid or "").strip()
    hint_release_id = str(release_id_hint or "").strip() or None
    if not recording_mbid:
        raise RuntimeError("no_valid_release_for_recording")

    service = get_musicbrainz_service()
    def _collect_aliases(*values):
        aliases = []
        seen = set()

        def _append(value):
            text = str(value or "").strip()
            if not text:
                return
            key = text.lower()
            if key in seen:
                return
            seen.add(key)
            aliases.append(text)

        for value in values:
            if value is None:
                continue
            if isinstance(value, str):
                _append(value)
                continue
            if isinstance(value, dict):
                _append(value.get("name"))
                _append(value.get("sort-name"))
                _append(value.get("alias"))
                continue
            if isinstance(value, list):
                for entry in value:
                    if isinstance(entry, dict):
                        _append(entry.get("name"))
                        _append(entry.get("sort-name"))
                        _append(entry.get("alias"))
                    else:
                        _append(entry)
        return aliases

    # recording entity valid includes: releases/artists/isrcs
    recording_payload = service.get_recording(
        recording_mbid,
        includes=["releases", "artists", "isrcs", "aliases"],
    )
    recording_data = recording_payload.get("recording", {}) if isinstance(recording_payload, dict) else {}
    release_list = recording_data.get("release-list", []) if isinstance(recording_data, dict) else []
    releases = [item for item in release_list if isinstance(item, dict) and str(item.get("id") or "").strip()]
    if not releases:
        raise RuntimeError("no_valid_release_for_recording")

    def _release_sort_key(item):
        release_id = str(item.get("id") or "")
        date_text = str(item.get("date") or "")
        year = _extract_release_year(date_text)
        date_key = year or "9999"
        hint_rank = 0 if hint_release_id and release_id == hint_release_id else 1
        return (hint_rank, date_key, release_id)

    sorted_releases = sorted(releases, key=_release_sort_key)

    selected_release = None
    selected_release_data = None
    selected_release_group_id = None
    selected_date_text = None

    for release_item in sorted_releases:
        release_id = str(release_item.get("id") or "").strip()
        if not release_id:
            continue
        release_payload = service.get_release(
            release_id,
            includes=["recordings", "release-groups", "artist-credits", "media", "aliases"],
        )
        release_data = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
        if not isinstance(release_data, dict):
            continue
        status = str(release_data.get("status") or release_item.get("status") or "").strip().lower()
        release_group = release_data.get("release-group")
        if not isinstance(release_group, dict):
            release_group = release_item.get("release-group") if isinstance(release_item.get("release-group"), dict) else {}
        primary_type = str(release_group.get("primary-type") or "").strip().lower()

        # valid release filter: Official Album/EP only
        if status != "official":
            continue
        if primary_type not in {"album", "ep"}:
            continue

        selected_release = release_id
        selected_release_data = release_data
        selected_release_group_id = str(release_group.get("id") or "").strip() or None
        selected_date_text = str(release_data.get("date") or release_item.get("date") or "").strip() or None
        break

    if not selected_release or not isinstance(selected_release_data, dict):
        raise RuntimeError("no_valid_release_for_recording")

    track_number = None
    disc_number = None
    matched_track = {}
    media = selected_release_data.get("medium-list", [])
    if not isinstance(media, list):
        media = []
    for medium in media:
        if not isinstance(medium, dict):
            continue
        medium_position = _normalize_positive_int(medium.get("position"))
        track_list = medium.get("track-list", [])
        if not isinstance(track_list, list):
            track_list = []
        for track in track_list:
            if not isinstance(track, dict):
                continue
            recording = track.get("recording") if isinstance(track.get("recording"), dict) else {}
            candidate_recording_mbid = str(recording.get("id") or "").strip() or None
            if candidate_recording_mbid != recording_mbid:
                continue
            disc_number = medium_position
            track_number = _normalize_positive_int(track.get("position"))
            matched_track = track
            break
        if track_number is not None and disc_number is not None:
            break

    if track_number is None or disc_number is None:
        raise RuntimeError("recording_not_found_in_release")

    year_only = _extract_release_year(selected_date_text)
    recording_disambiguation = str(recording_data.get("disambiguation") or "").strip() or None
    track_disambiguation = str(matched_track.get("disambiguation") or "").strip() or None
    title_aliases = _collect_aliases(
        recording_data.get("title"),
        recording_data.get("alias-list"),
        recording_data.get("aliases"),
        matched_track.get("title"),
        matched_track.get("alias-list"),
        matched_track.get("aliases"),
        selected_release_data.get("title"),
        selected_release_data.get("alias-list"),
        selected_release_data.get("aliases"),
    )
    if recording_disambiguation:
        title_aliases = _collect_aliases(title_aliases, recording_disambiguation)
    if track_disambiguation and matched_track.get("title"):
        title_aliases = _collect_aliases(
            title_aliases,
            f"{matched_track.get('title')} {track_disambiguation}",
        )
    enriched = {
        "album": selected_release_data.get("title"),
        "release_date": year_only,
        "track_number": track_number,
        "disc_number": disc_number,
        "mb_release_id": selected_release,
        "mb_release_group_id": selected_release_group_id,
        "track_aliases": title_aliases,
        "track_disambiguation": recording_disambiguation or track_disambiguation,
        "mb_recording_title": recording_data.get("title"),
    }
    _log_event(
        logging.INFO,
        "release_enrichment_applied",
        recording_mbid=recording_mbid,
        release_mbid=selected_release,
        release_group_mbid=selected_release_group_id,
        track_number=track_number,
        disc_number=disc_number,
        release_date=year_only,
    )
    return enriched


def _ensure_release_enriched(job):
    output_template = job.output_template if isinstance(job.output_template, dict) else {}
    canonical = output_template.get("canonical_metadata")
    if not isinstance(canonical, dict):
        canonical = {}
        output_template["canonical_metadata"] = canonical

    fields = _release_fields_from_template(output_template, canonical)
    if _release_fields_complete(fields):
        return

    recording_mbid = _extract_release_value(
        output_template,
        canonical,
        "recording_mbid",
        "mb_recording_id",
    )
    release_id = _extract_release_value(
        output_template,
        canonical,
        "mb_release_id",
        "release_id",
    )

    try:
        enriched = _fetch_release_enrichment(recording_mbid, release_id)
    except Exception:
        _log_event(
            logging.ERROR,
            "release_enrichment_failed",
            recording_mbid=recording_mbid,
            release_id=release_id,
        )
        raise RuntimeError("release_enrichment_incomplete")

    if isinstance(enriched, dict):
        for key in (
            "album",
            "release_date",
            "track_number",
            "disc_number",
            "mb_release_id",
            "mb_release_group_id",
            "track_aliases",
            "track_disambiguation",
            "mb_recording_title",
        ):
            value = enriched.get(key)
            if value not in (None, ""):
                canonical[key] = value
                output_template[key] = value

    fields = _release_fields_from_template(output_template, canonical)
    if not _release_fields_complete(fields):
        _log_event(
            logging.ERROR,
            "release_enrichment_failed",
            recording_mbid=recording_mbid,
            release_id=fields.get("mb_release_id") or release_id,
        )
        raise RuntimeError("release_enrichment_incomplete")


def resolve_cookie_file(config):
    cfg = config if isinstance(config, dict) else {}
    cookie_value = cfg.get("yt_dlp_cookies") or cfg.get("cookiefile")
    if not cookie_value:
        return None
    try:
        resolved = resolve_dir(cookie_value, TOKENS_DIR)
    except ValueError as exc:
        logging.error("Invalid yt-dlp cookies path: %s", exc)
        return None
    if not os.path.exists(resolved):
        logging.warning("yt-dlp cookies file not found: %s", resolved)
        return None
    return resolved


def resolve_cookiefile_for_context(context, config):
    ctx = context if isinstance(context, dict) else {}
    cfg = config if isinstance(config, dict) else {}
    if bool(ctx.get("disable_cookies")):
        _log_event(
            logging.INFO,
            "cookies_missing_or_disabled",
            reason="explicitly_disabled",
            job_id=ctx.get("job_id"),
            operation=ctx.get("operation"),
            media_type=ctx.get("media_type"),
            media_intent=ctx.get("media_intent"),
        )
        return None

    def _as_bool(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value or "").strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return None

    cookiefile = None
    explicit_cookie = ctx.get("cookie_file")
    if explicit_cookie:
        try:
            cookiefile = resolve_dir(explicit_cookie, TOKENS_DIR)
        except ValueError:
            cookiefile = None
        if cookiefile and not os.path.exists(cookiefile):
            cookiefile = None

    if not cookiefile:
        cookiefile = resolve_cookie_file(cfg)

    # Cookies are additive and must be explicitly enabled, or explicitly present by path.
    # This must not alter media mode/extraction policy; it only controls cookie attachment.
    enabled_flag = _as_bool(
        cfg.get("cookies_enabled", cfg.get("allow_cookies", cfg.get("use_cookies")))
    )
    cookie_explicitly_present = bool(explicit_cookie) or bool(
        cfg.get("yt_dlp_cookies") or cfg.get("cookiefile")
    )
    cookies_allowed = bool(cookie_explicitly_present) or bool(enabled_flag)

    if cookiefile and cookies_allowed:
        _log_event(
            logging.INFO,
            "cookies_applied",
            cookiefile=cookiefile,
            job_id=ctx.get("job_id"),
            operation=ctx.get("operation"),
            media_type=ctx.get("media_type"),
            media_intent=ctx.get("media_intent"),
        )
        return cookiefile

    _log_event(
        logging.INFO,
        "cookies_missing_or_disabled",
        reason="missing_or_not_found" if cookies_allowed else "not_enabled",
        job_id=ctx.get("job_id"),
        operation=ctx.get("operation"),
        media_type=ctx.get("media_type"),
        media_intent=ctx.get("media_intent"),
    )
    return None


def resolve_youtube_cookie_fallback_file(config):
    youtube_cfg = (config or {}).get("youtube")
    if not isinstance(youtube_cfg, dict):
        return None
    cookies_cfg = youtube_cfg.get("cookies")
    if not isinstance(cookies_cfg, dict):
        return None
    if not cookies_cfg.get("enabled"):
        return None
    if not cookies_cfg.get("fallback_only"):
        return None
    path = cookies_cfg.get("path")
    if not isinstance(path, str) or not path.strip():
        return None
    try:
        resolved = resolve_dir(path, TOKENS_DIR)
    except ValueError as exc:
        logging.error("Invalid youtube cookies path: %s", exc)
        return None
    if not os.path.exists(resolved):
        logging.warning("youtube cookies file not found: %s", resolved)
        return None
    return resolved


def _is_youtube_access_gate(message: str | None) -> bool:
    if not message:
        return False
    lower_msg = message.lower()
    triggers = [
        "this video is not available",
        "sign in to confirm your age",
        "login required",
        "access denied",
        "age restricted",
        "age-restricted",
        "age restriction",
    ]
    blockers = [
        "timed out",
        "timeout",
        "connection reset",
        "temporary failure",
        "network error",
        "couldn't download webpage",
        "unable to download webpage",
        "http error 403",
        "http error 404",
        "geo-restricted",
        "geoblocked",
        "geo blocked",
        "country",
        "region",
        "private",
        "removed",
    ]
    if not any(trigger in lower_msg for trigger in triggers):
        return False
    if any(blocker in lower_msg for blocker in blockers):
        return False
    return True


def _classify_ytdlp_unavailability(message: str | None) -> str | None:
    if not message:
        return None
    lower_msg = str(message).lower()
    if any(marker in lower_msg for marker in _YTDLP_TRANSIENT_ERROR_MARKERS):
        return None
    for unavailable_class, markers in _YTDLP_UNAVAILABLE_SIGNAL_MAP:
        if any(marker in lower_msg for marker in markers):
            return unavailable_class
    return None

def resolve_media_type(config, *, playlist_entry=None, url=None):
    media_type = None
    if isinstance(playlist_entry, dict):
        media_type = playlist_entry.get("media_type") or playlist_entry.get("media")
    if not media_type and isinstance(config, dict):
        media_type = config.get("media_type")
    if media_type:
        media_type = str(media_type).strip().lower()
        if media_type in {"music", "audio"}:
            return "music"
        if media_type == "video":
            return "video"

    legacy_audio = None
    if isinstance(playlist_entry, dict):
        legacy_audio = playlist_entry.get("audio_only")
        if legacy_audio is None:
            legacy_audio = playlist_entry.get("music_mode")
    if legacy_audio is None and isinstance(config, dict):
        legacy_audio = config.get("audio_only")
        if legacy_audio is None:
            legacy_audio = config.get("music_mode")
    if legacy_audio is True:
        return "music"
    # Invariant A: default to video when no explicit media_type is provided.
    return "video"


def resolve_media_intent(origin, media_type, *, playlist_entry=None):
    if isinstance(playlist_entry, dict):
        value = playlist_entry.get("media_intent")
        if value in {"track", "album", "playlist", "episode", "movie"}:
            return value
    if origin == "playlist":
        return "playlist"
    if is_music_media_type(media_type):
        return "track"
    return "episode"


def build_output_template(config, *, playlist_entry=None, destination=None, base_dir=None):
    base_dir = base_dir or "."
    config = config or {}
    entry = playlist_entry if isinstance(playlist_entry, dict) else {}
    output_dir = destination or entry.get("folder") or entry.get("directory")
    if not output_dir:
        # Prefer the correct default folder based on media type intent.
        # If the run is not explicitly music/audio, do NOT default into the music folder.
        media_type = str(entry.get("media_type") or entry.get("media") or "").strip().lower()
        if media_type in {"music", "audio"}:
            output_dir = config.get("music_download_folder") or config.get("single_download_folder") or base_dir
        else:
            output_dir = config.get("single_download_folder") or config.get("music_download_folder") or base_dir
    output_dir = resolve_dir(output_dir, base_dir)

    final_format = (
        entry.get("final_format")
        or entry.get("video_final_format")
        or config.get("final_format")
        or config.get("video_final_format")
        or "mkv"
    )
    music_final_format = (
        entry.get("music_final_format")
        or entry.get("audio_final_format")
        or config.get("music_final_format")
        or config.get("audio_final_format")
        or "mp3"
    )

    filename_template = entry.get("filename_template") or config.get("filename_template")
    audio_template = entry.get("audio_filename_template") or config.get("audio_filename_template")
    if not audio_template:
        audio_template = config.get("music_filename_template")

    return {
        "output_dir": output_dir,
        "final_format": final_format,
        "music_final_format": music_final_format,
        "filename_template": filename_template,
        "audio_filename_template": audio_template,
        "remove_after_download": bool(entry.get("remove_after_download")),
        "playlist_item_id": entry.get("playlistItemId") or entry.get("playlist_item_id"),
        "source_account": entry.get("account"),
    }


def ensure_mb_bound_music_track(payload_or_intent, *, config, country_preference="US"):
    if not isinstance(payload_or_intent, dict):
        raise ValueError("music_track_requires_mapping_payload")

    template = payload_or_intent.get("output_template")
    if not isinstance(template, dict):
        template = {}
        payload_or_intent["output_template"] = template

    canonical = template.get("canonical_metadata")
    if not isinstance(canonical, dict):
        canonical = {}
        template["canonical_metadata"] = canonical

    required_keys = (
        "recording_mbid",
        "mb_release_id",
        "mb_release_group_id",
        "album",
        "release_date",
        "track_number",
        "disc_number",
        "duration_ms",
    )
    for key in required_keys:
        canonical.setdefault(key, None)

    def _coalesce_str(*values):
        for value in values:
            text = str(value or "").strip()
            if text:
                return text
        return None

    def _coalesce_pos_int(*values):
        for value in values:
            parsed = _normalize_positive_int(value)
            if parsed is not None:
                return parsed
        return None

    def _coalesce_aliases(*values):
        aliases = []
        seen = set()
        for value in values:
            if isinstance(value, str):
                value = [value]
            if not isinstance(value, (list, tuple, set)):
                continue
            for entry in value:
                text = str(entry or "").strip()
                if not text:
                    continue
                key = text.lower()
                if key in seen:
                    continue
                seen.add(key)
                aliases.append(text)
        return aliases

    def _coalesce_urls(*values):
        urls = []
        seen = set()
        for value in values:
            if isinstance(value, str):
                value = [value]
            if not isinstance(value, (list, tuple, set)):
                continue
            for entry in value:
                text = str(entry or "").strip()
                if not text:
                    continue
                key = text.lower()
                if key in seen:
                    continue
                seen.add(key)
                urls.append(text)
        return urls

    def _is_complete(meta):
        for key in required_keys:
            value = meta.get(key)
            if key in {"track_number", "disc_number", "duration_ms"}:
                if _normalize_positive_int(value) is None:
                    return False
            else:
                if str(value or "").strip() == "":
                    return False
        return True

    # Pull direct fields into canonical metadata for uniform validation.
    canonical["recording_mbid"] = _coalesce_str(
        canonical.get("recording_mbid"),
        canonical.get("mb_recording_id"),
        template.get("recording_mbid"),
        template.get("mb_recording_id"),
        payload_or_intent.get("recording_mbid"),
        payload_or_intent.get("mb_recording_id"),
    )
    canonical["mb_release_id"] = _coalesce_str(
        canonical.get("mb_release_id"),
        canonical.get("release_id"),
        template.get("mb_release_id"),
        payload_or_intent.get("mb_release_id"),
        payload_or_intent.get("release_id"),
    )
    canonical["mb_release_group_id"] = _coalesce_str(
        canonical.get("mb_release_group_id"),
        canonical.get("release_group_id"),
        template.get("mb_release_group_id"),
        payload_or_intent.get("mb_release_group_id"),
        payload_or_intent.get("release_group_id"),
    )
    canonical["album"] = _coalesce_str(
        canonical.get("album"),
        template.get("album"),
        payload_or_intent.get("album"),
    )
    canonical["release_date"] = _coalesce_str(
        canonical.get("release_date"),
        canonical.get("date"),
        template.get("release_date"),
        payload_or_intent.get("release_date"),
    )
    canonical["track_number"] = _coalesce_pos_int(
        canonical.get("track_number"),
        canonical.get("track_num"),
        template.get("track_number"),
        payload_or_intent.get("track_number"),
    )
    canonical["disc_number"] = _coalesce_pos_int(
        canonical.get("disc_number"),
        canonical.get("disc_num"),
        template.get("disc_number"),
        payload_or_intent.get("disc_number"),
        1,
    )
    canonical["duration_ms"] = _coalesce_pos_int(
        canonical.get("duration_ms"),
        template.get("duration_ms"),
        payload_or_intent.get("duration_ms"),
    )
    canonical["track_aliases"] = _coalesce_aliases(
        canonical.get("track_aliases"),
        canonical.get("title_aliases"),
        template.get("track_aliases"),
        template.get("title_aliases"),
        payload_or_intent.get("track_aliases"),
        payload_or_intent.get("title_aliases"),
    )
    canonical["track_disambiguation"] = _coalesce_str(
        canonical.get("track_disambiguation"),
        template.get("track_disambiguation"),
        payload_or_intent.get("track_disambiguation"),
    )
    canonical["mb_recording_title"] = _coalesce_str(
        canonical.get("mb_recording_title"),
        template.get("mb_recording_title"),
        payload_or_intent.get("mb_recording_title"),
    )
    canonical["mb_youtube_urls"] = _coalesce_urls(
        canonical.get("mb_youtube_urls"),
        template.get("mb_youtube_urls"),
        payload_or_intent.get("mb_youtube_urls"),
    )

    if not _is_complete(canonical):
        recording_mbid = _coalesce_str(canonical.get("recording_mbid"))
        release_hint = _coalesce_str(canonical.get("mb_release_id"))
        if recording_mbid:
            try:
                enriched = _fetch_release_enrichment(
                    recording_mbid,
                    release_hint,
                )
            except Exception as exc:
                raise ValueError(
                    "music_track_requires_mb_bound_metadata",
                    [str(exc)],
                )
            for key in ("album", "release_date", "track_number", "disc_number", "mb_release_id", "mb_release_group_id"):
                value = enriched.get(key) if isinstance(enriched, dict) else None
                if value not in (None, ""):
                    canonical[key] = value
            try:
                recording_payload = get_musicbrainz_service().get_recording(
                    recording_mbid,
                    includes=["releases", "artists", "isrcs", "aliases"],
                )
                recording_data = recording_payload.get("recording", {}) if isinstance(recording_payload, dict) else {}
                canonical["duration_ms"] = _coalesce_pos_int(
                    canonical.get("duration_ms"),
                    recording_data.get("length"),
                )
            except Exception:
                pass
        else:
            artist = _coalesce_str(canonical.get("artist"), template.get("artist"), payload_or_intent.get("artist"))
            track = _coalesce_str(canonical.get("track"), canonical.get("title"), template.get("track"), payload_or_intent.get("track"))
            album_hint = _coalesce_str(canonical.get("album"), template.get("album"), payload_or_intent.get("album"))
            if artist and track:
                lookup_track = _normalize_title_for_mb_lookup(track)
                service = get_musicbrainz_service()
                pair = resolve_best_mb_pair(
                    service,
                    artist=artist,
                    track=lookup_track or track,
                    album=album_hint,
                    duration_ms=_coalesce_pos_int(canonical.get("duration_ms"), template.get("duration_ms"), payload_or_intent.get("duration_ms")),
                    country_preference=country_preference,
                    allow_non_album_fallback=bool((config or {}).get("allow_non_album_fallback")),
                    debug=bool((config or {}).get("debug_music_scoring")),
                    min_recording_score=float((config or {}).get("min_confidence") or 0.0),
                    threshold=float((config or {}).get("music_mb_binding_threshold", 0.78)),
                )
                if isinstance(pair, dict):
                    canonical.update(pair)

    canonical["track_aliases"] = _coalesce_aliases(
        canonical.get("track_aliases"),
        canonical.get("title_aliases"),
    )
    canonical["title_aliases"] = list(canonical.get("track_aliases") or [])
    canonical["mb_youtube_urls"] = _coalesce_urls(canonical.get("mb_youtube_urls"))

    canonical["release_date"] = _extract_release_year(canonical.get("release_date")) or _coalesce_str(canonical.get("release_date"))
    canonical["track_number"] = _coalesce_pos_int(canonical.get("track_number"))
    canonical["disc_number"] = _coalesce_pos_int(canonical.get("disc_number"), 1)
    canonical["duration_ms"] = _coalesce_pos_int(canonical.get("duration_ms"))
    if not _is_complete(canonical):
        reasons = []
        try:
            last = getattr(resolve_best_mb_pair, "last_failure_reasons", [])
            if isinstance(last, list):
                reasons = [str(item) for item in last if str(item or "").strip()]
        except Exception:
            reasons = []
        raise ValueError("music_track_requires_mb_bound_metadata", reasons)

    # Keep top-level mirror fields in sync for legacy consumers.
    for key in required_keys:
        template[key] = canonical.get(key)
    template["mb_recording_id"] = canonical.get("recording_mbid")
    template["track_aliases"] = canonical.get("track_aliases")
    template["title_aliases"] = canonical.get("track_aliases")
    template["track_disambiguation"] = canonical.get("track_disambiguation")
    template["mb_recording_title"] = canonical.get("mb_recording_title")
    template["mb_youtube_urls"] = canonical.get("mb_youtube_urls")

    return canonical


def build_download_job_payload(
    *,
    config,
    origin,
    origin_id,
    media_type,
    media_intent,
    source,
    url,
    input_url=None,
    destination=None,
    base_dir=None,
    playlist_entry=None,
    final_format_override=None,
    resolved_metadata=None,
    output_template_overrides=None,
    trace_id=None,
    resolved_destination=None,
    canonical_id=None,
    canonical_url=None,
    external_id=None,
):
    config = config if isinstance(config, dict) else {}
    output_template = build_output_template(
        config,
        playlist_entry=playlist_entry,
        destination=destination,
        base_dir=base_dir,
    )

    canonical_metadata = resolved_metadata if isinstance(resolved_metadata, dict) else None
    if canonical_metadata:
        output_template["canonical_metadata"] = canonical_metadata
        output_template.setdefault("artist", canonical_metadata.get("artist") or canonical_metadata.get("album_artist"))
        output_template.setdefault("album_artist", canonical_metadata.get("album_artist") or canonical_metadata.get("artist"))
        output_template.setdefault("album", canonical_metadata.get("album"))
        output_template.setdefault("track", canonical_metadata.get("track") or canonical_metadata.get("title"))
        output_template.setdefault("track_number", canonical_metadata.get("track_number") or canonical_metadata.get("track_num"))
        output_template.setdefault("disc_number", canonical_metadata.get("disc_number") or canonical_metadata.get("disc_num"))
        output_template.setdefault("track_total", canonical_metadata.get("track_total"))
        output_template.setdefault("disc_total", canonical_metadata.get("disc_total"))
        output_template.setdefault("release_date", canonical_metadata.get("release_date") or canonical_metadata.get("date"))
        output_template.setdefault("artwork_url", canonical_metadata.get("artwork_url"))
        output_template.setdefault("genre", canonical_metadata.get("genre"))
        output_template.setdefault("track_aliases", canonical_metadata.get("track_aliases") or canonical_metadata.get("title_aliases"))
        output_template.setdefault("title_aliases", canonical_metadata.get("track_aliases") or canonical_metadata.get("title_aliases"))
        output_template.setdefault("track_disambiguation", canonical_metadata.get("track_disambiguation"))
        output_template.setdefault("mb_recording_title", canonical_metadata.get("mb_recording_title"))
        output_template.setdefault("mb_youtube_urls", canonical_metadata.get("mb_youtube_urls"))

    if isinstance(output_template_overrides, dict):
        output_template.update(output_template_overrides)

    # Canonical MB pair schema contract for music-track jobs.
    # This is schema availability only (no enrichment behavior here).
    normalized_intent = str(media_intent or "").strip().lower()
    if normalized_intent == "music_track" or (is_music_media_type(media_type) and normalized_intent == "track"):
        canonical = output_template.get("canonical_metadata")
        if not isinstance(canonical, dict):
            canonical = {}
            output_template["canonical_metadata"] = canonical
        for key in (
            "recording_mbid",
            "mb_release_id",
            "mb_release_group_id",
            "album",
            "release_date",
            "track_number",
            "disc_number",
        ):
            canonical.setdefault(key, None)
        ensure_mb_bound_music_track(
            {
                "media_type": media_type,
                "media_intent": media_intent,
                "output_template": output_template,
                "artist": output_template.get("artist"),
                "track": output_template.get("track"),
                "album": output_template.get("album"),
            },
            config=config,
            country_preference=str(config.get("country") or "US"),
        )

    video_container = _resolve_target_video_container(
        {"final_format": final_format_override},
        config,
        output_template,
    )
    audio_format = _resolve_target_audio_format(
        {"final_format": None},
        config,
        output_template,
    )
    override_audio = _normalize_audio_format(final_format_override)
    override_video = _normalize_format(final_format_override)
    if is_music_media_type(media_type):
        if override_audio in _AUDIO_FORMATS:
            audio_format = override_audio
    else:
        if override_video in _VIDEO_CONTAINERS:
            video_container = override_video
        elif override_audio in _AUDIO_FORMATS:
            audio_format = override_audio
    output_template["final_format"] = video_container or "mkv"
    output_template["music_final_format"] = audio_format or "mp3"
    if is_music_media_type(media_type):
        output_template["audio_mode"] = True

    # Canonical output-template schema: keep a stable key set across all enqueue paths.
    for key in (
        "canonical_metadata",
        "artist",
        "album",
        "track",
        "track_number",
        "disc_number",
        "release_date",
        "audio_mode",
        "duration_ms",
        "artwork_url",
        "recording_mbid",
        "mb_recording_id",
        "mb_release_id",
        "mb_release_group_id",
        "kind",
        "source",
        "import_batch",
        "import_batch_id",
        "source_index",
        "mb_youtube_urls",
    ):
        output_template.setdefault(key, None)

    computed_destination = resolved_destination or output_template.get("output_dir")
    return {
        "origin": origin,
        "origin_id": origin_id,
        "media_type": media_type,
        "media_intent": media_intent,
        "source": source,
        "url": url,
        "input_url": input_url or url,
        "canonical_url": canonical_url,
        "external_id": external_id,
        "output_template": output_template,
        "trace_id": trace_id,
        "resolved_destination": computed_destination,
        "canonical_id": canonical_id,
    }


def resolve_source(url):
    if not url:
        return "unknown"
    parsed = urllib.parse.urlparse(url)
    host = (parsed.netloc or "").lower()
    if "music.youtube.com" in host:
        return "youtube_music"
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    return "unknown"


def canonicalize_url(source: str, url: str | None, external_id: str | None) -> str | None:
    """
    Return a stable canonical URL for matching and deduplication.
    """
    try:
        source = (source or "").strip().lower()
        if source in {"youtube", "youtube_music"}:
            video_id = external_id or extract_video_id(url)
            if not video_id:
                return None
            return f"https://www.youtube.com/watch?v={video_id}"
        if source == "soundcloud":
            if not url or not isinstance(url, str):
                return None
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme not in ("http", "https"):
                return None
            return url
        if source == "bandcamp":
            if not url or not isinstance(url, str):
                return None
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme not in ("http", "https"):
                return None
            return urllib.parse.urlunparse(parsed._replace(scheme="https"))
        return None
    except Exception:
        return None


def is_youtube_music_url(url):
    parsed = urllib.parse.urlparse(url)
    return "music.youtube.com" in (parsed.netloc or "").lower()


def _is_http_url(url):
    if not url or not isinstance(url, str):
        return False
    parsed = urllib.parse.urlparse(url)
    return parsed.scheme in {"http", "https"}


def build_ytdlp_opts(context):
    operation = context.get("operation") or "download"
    media_type = context.get("media_type")
    audio_mode = is_music_media_type(media_type)
    context["audio_mode"] = audio_mode
    target_format = context.get("final_format")
    output_template = context.get("output_template")
    output_template_meta = context.get("output_template_meta")
    raw_config = context.get("config")
    allow_empty_config = (
        os.environ.get("RETREIVR_ALLOW_EMPTY_CONFIG", "").strip() == "1"
        or bool(os.environ.get("PYTEST_CURRENT_TEST"))
    )
    if (not isinstance(raw_config, dict) or not raw_config) and not allow_empty_config:
        _log_event(
            logging.ERROR,
            "empty_config_passed_to_build_ytdlp_opts",
            operation=operation,
            media_type=context.get("media_type"),
            media_intent=context.get("media_intent"),
        )
        raise RuntimeError("empty_config_passed_to_build_ytdlp_opts")
    config = raw_config if isinstance(raw_config, dict) else {}
    overrides = context.get("overrides") or {}
    allow_chapter_outtmpl = bool(context.get("allow_chapter_outtmpl"))

    resolved_audio_format = None
    resolved_video_container = None
    if audio_mode:
        resolved_audio_format = _resolve_target_audio_format(context, config, output_template_meta)
        normalized_audio_target = _normalize_audio_format(resolved_audio_format)
        normalized_target = _normalize_format(resolved_audio_format)
    else:
        resolved_video_container = _resolve_target_video_container(context, config, output_template_meta)
        normalized_audio_target = _normalize_audio_format(resolved_video_container)
        normalized_target = _normalize_format(resolved_video_container)

    # If a video job accidentally receives an audio codec as "final_format" (e.g. global config),
    # do NOT interpret it as a yt-dlp "format" selector. That causes invalid downloads.
    # We only treat webm/mp4/mkv as container preferences for video mode here.
    video_container_target = normalized_target if normalized_target in _VIDEO_CONTAINERS else None

    def _url_looks_like_playlist(u: str | None) -> bool:
        if not u:
            return False
        try:
            parsed = urllib.parse.urlparse(u)
            q = urllib.parse.parse_qs(parsed.query)
            # YouTube playlist patterns
            if "list" in q and q.get("list"):
                return True
            # Common non-YouTube playlist patterns (best-effort)
            if any(k in q for k in ("playlist", "pl", "set")):
                return True
            # Path-based playlist URLs
            if "/playlist" in (parsed.path or ""):
                return True
        except Exception:
            return False
        return False

    # Keep extraction policy mode-agnostic; mode-specific logic below controls only
    # format / merge_output_format / postprocessors.
    allow_playlist = False

    if isinstance(output_template, dict) and not allow_chapter_outtmpl:
        default_template = output_template.get("default")
        if isinstance(default_template, str) and default_template.strip():
            output_template = default_template
        else:
            output_template = "%(title).200s-%(id)s.%(ext)s"

    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "outtmpl": output_template,
        # Avoid chapter workflows unless explicitly enabled.
        # (Chapter outtmpl dicts can trigger unexpected behavior in the Python API path.)
        "no_chapters": True if not allow_chapter_outtmpl else False,
        "retries": 3,
        "fragment_retries": 3,
        "force_overwrites": True,
        "overwrites": True,
    }

    if operation == "playlist":
        opts["skip_download"] = True
        opts["extract_flat"] = True
    elif operation == "metadata":
        opts["skip_download"] = True
    else:
        # Only enable addmetadata, embedthumbnail, writethumbnail, and audio postprocessors
        # when both audio_mode and media_type is music/audio
        if audio_mode:
            preferred_audio_codec = _normalize_audio_format(resolved_audio_format) or "mp3"
            opts["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": preferred_audio_codec,
                    "preferredquality": "0",
                }
            ]
            opts["format"] = _FORMAT_AUDIO
            opts["addmetadata"] = True
            opts["embedthumbnail"] = True
            opts["writethumbnail"] = True
        else:
            # Video mode strategy:
            # - Keep download selector stable and quality-first across all final containers.
            # - Use final_format only as post-merge container preference.
            opts["format"] = _FORMAT_VIDEO
            if video_container_target in {"mp4", "mkv"}:
                opts["merge_output_format"] = video_container_target
            # mp4 requires codec-compatible post-conversion. Without this, merged mp4 can
            # still contain opus audio when source streams are webm/opus.
            if video_container_target == "mp4":
                opts["recodevideo"] = "mp4"

    # Only lock down format-related overrides when the target_format was actually applied
    # (audio codec in audio_mode, or video container preference in video mode).
    lock_format = False
    if audio_mode and normalized_audio_target:
        lock_format = True
    if (not audio_mode) and video_container_target:
        lock_format = True

    opts = _merge_overrides(opts, overrides, operation=operation, lock_format=lock_format)

    if operation == "download":
        for key in _YTDLP_DOWNLOAD_UNSAFE_KEYS:
            opts.pop(key, None)

    if audio_mode and ("bestvideo" in str(opts.get("format") or "").lower() or opts.get("merge_output_format")):
        raise RuntimeError("music_job_built_video_opts")
    if not audio_mode:
        postprocessors = opts.get("postprocessors") or []
        if any((pp or {}).get("key") == "FFmpegExtractAudio" for pp in postprocessors if isinstance(pp, dict)):
            raise RuntimeError("video_job_built_audio_opts")

    _log_event(
        logging.INFO,
        "audit_build_ytdlp_opts",
        operation=operation,
        format=opts.get("format"),
        audio_mode=audio_mode,
        media_type=context.get("media_type"),
        media_intent=context.get("media_intent"),
        final_format=target_format,
        resolved_audio_format=resolved_audio_format,
        resolved_video_container=resolved_video_container,
        postprocessors=bool(opts.get("postprocessors")),
        postprocessors_present=bool(opts.get("postprocessors")),
        js_runtimes_present=bool(opts.get("js_runtimes")),
        merge_output_format=opts.get("merge_output_format"),
        addmetadata=opts.get("addmetadata"),
        embedthumbnail=opts.get("embedthumbnail"),
        writethumbnail=opts.get("writethumbnail"),
        overrides=context.get("overrides"),
        allow_playlist=allow_playlist,
    )

    return opts


# Canonical yt-dlp invocation wrapper.
# This function does NOT change behavior.
# It centralizes intent + opts generation for reuse by
# both Python API and CLI execution paths.
def build_ytdlp_invocation(job, context):
    """
    Canonical yt-dlp invocation wrapper.
    This function does NOT change behavior.
    It centralizes intent + opts generation for reuse by
    both Python API and CLI execution paths.
    """
    opts = build_ytdlp_opts(context)
    return {
        "media_type": job.media_type if job else context.get("media_type"),
        "media_intent": job.media_intent if job else context.get("media_intent"),
        "audio_mode": context.get("audio_mode"),
        "final_format": context.get("final_format"),
        "opts": opts,
    }


def _resolve_target_audio_format(context, config, output_template):
    global _MUSIC_AUDIO_FORMAT_WARNED
    output_template = output_template if isinstance(output_template, dict) else {}
    config = config if isinstance(config, dict) else {}
    requested = (
        output_template.get("music_final_format")
        or output_template.get("audio_final_format")
        or config.get("music_final_format")
        or config.get("audio_final_format")
        or "mp3"
    )
    normalized = _normalize_audio_format(requested) or "mp3"
    if normalized in _VIDEO_CONTAINERS:
        if not _MUSIC_AUDIO_FORMAT_WARNED:
            logger.warning(
                "[WORKER] invalid_music_audio_format=%s; forcing mp3",
                normalized,
            )
            _MUSIC_AUDIO_FORMAT_WARNED = True
        return "mp3"
    return normalized


def _resolve_target_video_container(context, config, output_template):
    context = context if isinstance(context, dict) else {}
    output_template = output_template if isinstance(output_template, dict) else {}
    config = config if isinstance(config, dict) else {}
    requested = (
        context.get("final_format")
        or context.get("video_final_format")
        or output_template.get("final_format")
        or output_template.get("video_final_format")
        or config.get("final_format")
        or config.get("video_final_format")
        or "mkv"
    )
    normalized = _normalize_format(requested) or "mkv"
    if normalized in _VIDEO_CONTAINERS:
        return normalized
    return "mkv"


def _merge_overrides(opts, overrides, *, operation, lock_format=False):
    if not isinstance(overrides, dict):
        return opts
    unsafe = [key for key in overrides if key in _YTDLP_DOWNLOAD_UNSAFE_KEYS]
    if unsafe and operation == "download":
        logging.warning("Dropping unsafe yt_dlp_opts for download: %s", unsafe)
    for key, value in overrides.items():
        if operation == "download" and key in _YTDLP_DOWNLOAD_UNSAFE_KEYS:
            continue
        if operation == "download" and key not in _YTDLP_DOWNLOAD_ALLOWLIST:
            continue
        if lock_format and key in {"format", "merge_output_format", "recodevideo"}:
            continue
        opts[key] = value
    return opts



def _redact_ytdlp_opts(opts):
    redacted = {}
    for key, value in (opts or {}).items():
        if key in {"cookiefile", "cookiesfrombrowser"}:
            redacted[key] = "<redacted>"
            continue
        if key in {"http_headers", "headers"}:
            redacted[key] = "<redacted>"
            continue
        redacted[key] = value
    return redacted


# Render yt-dlp CLI argv for subprocess.run(shell=False), supporting cookies and safe quoting.
def _render_ytdlp_cli_argv(opts, url):
    """Return a yt-dlp argv list suitable for subprocess.run(shell=False)."""
    argv = ["yt-dlp"]

    # Core selection/output
    if opts.get("format"):
        argv.extend(["-f", str(opts["format"])])
    if opts.get("merge_output_format"):
        argv.extend(["--merge-output-format", str(opts["merge_output_format"])])
    if opts.get("recodevideo"):
        argv.extend(["--recode-video", str(opts["recodevideo"])])
    if opts.get("outtmpl"):
        argv.extend(["-o", str(opts["outtmpl"])])
    if opts.get("newline"):
        argv.append("--newline")
    if opts.get("nocolor"):
        argv.append("--no-color")
    progress_template = opts.get("progress_template")
    if progress_template:
        argv.extend(["--progress-template", str(progress_template)])

    # Playlist behavior
    if opts.get("noplaylist") is True:
        argv.append("--no-playlist")
    elif opts.get("noplaylist") is False:
        argv.append("--yes-playlist")

    if opts.get("overwrites") is True or opts.get("force_overwrites") is True:
        argv.append("--force-overwrites")

    # Retries
    if opts.get("retries") is not None:
        argv.extend(["--retries", str(opts.get("retries"))])
    if opts.get("fragment_retries") is not None:
        argv.extend(["--fragment-retries", str(opts.get("fragment_retries"))])

    # Cookies (python opts uses cookiefile, CLI uses --cookies)
    if opts.get("cookiefile"):
        argv.extend(["--cookies", str(opts.get("cookiefile"))])

    # JS runtimes: {"node":{"path":"/usr/bin/node"}} -> --js-runtimes node:/usr/bin/node
    js_runtimes = opts.get("js_runtimes")
    if isinstance(js_runtimes, dict):
        for runtime_name, runtime_cfg in js_runtimes.items():
            name = str(runtime_name or "").strip()
            if not name:
                continue
            path = None
            if isinstance(runtime_cfg, dict):
                path = str(runtime_cfg.get("path") or "").strip() or None
            token = f"{name}:{path}" if path else name
            argv.extend(["--js-runtimes", token])

    remote_components = opts.get("remote_components")
    if isinstance(remote_components, (list, tuple, set)):
        for component in remote_components:
            token = str(component or "").strip()
            if token:
                argv.extend(["--remote-components", token])
    elif remote_components:
        token = str(remote_components).strip()
        if token:
            argv.extend(["--remote-components", token])

    # Extractor args: {"youtube":{"key":"value"}} -> --extractor-args youtube:key=value
    extractor_args = opts.get("extractor_args")
    if isinstance(extractor_args, dict):
        for extractor_name, extractor_cfg in extractor_args.items():
            if not isinstance(extractor_cfg, dict):
                continue
            pieces = []
            for arg_key, arg_value in extractor_cfg.items():
                if isinstance(arg_value, (list, tuple)):
                    value_text = ",".join(str(v).strip() for v in arg_value if str(v).strip())
                else:
                    value_text = str(arg_value or "").strip()
                if not value_text:
                    continue
                pieces.append(f"{arg_key}={value_text}")
            if pieces:
                argv.extend(["--extractor-args", f"{extractor_name}:{';'.join(pieces)}"])

    # Audio extraction (CLI parity for FFmpegExtractAudio)
    if opts.get("postprocessors"):
        for pp in opts.get("postprocessors") or []:
            if pp.get("key") == "FFmpegExtractAudio":
                argv.append("-x")
                if pp.get("preferredcodec"):
                    argv.extend(["--audio-format", str(pp.get("preferredcodec"))])
                if pp.get("preferredquality"):
                    argv.extend(["--audio-quality", str(pp.get("preferredquality"))])

    # Sidecar metadata improves post-download naming/tagging resilience.
    if opts.get("writeinfojson"):
        argv.append("--write-info-json")

    argv.append(str(url))
    return argv


def _argv_to_redacted_cli(argv):
    """Render argv as a single command string with shell-escaping, redacting sensitive paths."""
    redacted = []
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in {"--cookies"} and i + 1 < len(argv):
            redacted.extend([tok, "<redacted>"])
            i += 2
            continue
        redacted.append(tok)
        i += 1
    return shlex.join(redacted)



def _normalized_config_key(key):
    return str(key or "").strip().lower().replace("_", "").replace(" ", "")


def _extract_config_js_runtime_values(cfg):
    if not isinstance(cfg, dict):
        return []
    raw_value = None
    for raw_key, candidate in cfg.items():
        if _normalized_config_key(raw_key) in {"jsruntime", "jsruntimes"}:
            raw_value = candidate
            break
    if raw_value is None:
        return []
    if isinstance(raw_value, (list, tuple, set)):
        source = list(raw_value)
    else:
        source = [raw_value]
    values = []
    seen = set()
    for item in source:
        text = str(item or "").strip()
        if not text:
            continue
        for part in text.split(","):
            token = part.strip()
            if not token or token in seen:
                continue
            seen.add(token)
            values.append(token)
    return values


def _build_js_runtime_dict(values):
    runtime_map = {}
    for value in values:
        runtime_name = value
        runtime_path = None
        if ":" in value:
            runtime_name, runtime_path = value.split(":", 1)
            runtime_name = runtime_name.strip()
            runtime_path = runtime_path.strip() or None
        if not runtime_name:
            continue
        runtime_map[runtime_name] = {"path": runtime_path} if runtime_path else {}
    return runtime_map


def _format_summary(info):
    if not isinstance(info, dict):
        return {"format_count": 0, "exts": [], "has_webm": False}
    formats = info.get("formats") or []
    ext_set = {fmt.get("ext") for fmt in formats if fmt.get("ext")}
    exts = sorted(ext_set)
    return {
        "format_count": len(formats),
        "exts": exts,
        "has_webm": "webm" in ext_set,
    }


def _select_youtube_cookie_fallback(
    config,
    url,
    stderr_text,
    opts,
    media_type,
):
    fallback_cookie = resolve_youtube_cookie_fallback_file(config)
    if not fallback_cookie:
        return None
    if opts.get("cookiefile"):
        return None
    if is_music_media_type(media_type):
        return None
    source = resolve_source(url)
    if source not in {"youtube", "youtube_music"}:
        return None
    if not _is_youtube_access_gate(stderr_text):
        return None
    return fallback_cookie


def _load_info_json_from_temp_dir(temp_dir, *, fallback_id=None):
    if not temp_dir or not os.path.isdir(temp_dir):
        return None
    candidates = []
    for entry in os.listdir(temp_dir):
        lower_entry = entry.lower()
        if not lower_entry.endswith('.info.json'):
            continue
        path = os.path.join(temp_dir, entry)
        if not os.path.isfile(path):
            continue
        score = 0
        if fallback_id and fallback_id in entry:
            score += 2
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            mtime = 0
        candidates.append((score, mtime, path))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    _, _, selected_path = candidates[0]
    try:
        with open(selected_path, 'r', encoding='utf-8') as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _enrich_info_from_sidecar(info, *, temp_dir, url, job_id=None):
    fallback_id = None
    if isinstance(info, dict):
        fallback_id = str(info.get('id') or '').strip() or None
    if not fallback_id:
        fallback_id = extract_video_id(url)

    sidecar = _load_info_json_from_temp_dir(temp_dir, fallback_id=fallback_id)
    if not isinstance(sidecar, dict):
        return info

    merged = dict(sidecar)
    if isinstance(info, dict):
        for key, value in info.items():
            if key not in merged or merged.get(key) in (None, '', [], {}):
                merged[key] = value

    _log_event(
        logging.INFO,
        'ytdlp_sidecar_metadata_loaded',
        job_id=job_id,
        url=url,
        sidecar_id=sidecar.get('id'),
        has_title=bool(sidecar.get('title')),
        has_channel=bool(sidecar.get('channel') or sidecar.get('uploader')),
    )
    return merged


_PROGRESS_MARKER = "[RETREIVR_PROGRESS]"


def _parse_int_or_none(value):
    raw = str(value or "").strip()
    if not raw or raw.lower() in {"none", "na", "n/a", "null"}:
        return None
    try:
        return int(float(raw))
    except Exception:
        return None


def _parse_float_or_none(value):
    raw = str(value or "").strip()
    if not raw or raw.lower() in {"none", "na", "n/a", "null"}:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _parse_progress_line(line):
    if not line or _PROGRESS_MARKER not in line:
        return None
    payload = line.split(_PROGRESS_MARKER, 1)[1].strip()
    parts = [part.strip() for part in payload.split("|")]
    if len(parts) < 6:
        return None

    downloaded_bytes = _parse_int_or_none(parts[0])
    total_bytes = _parse_int_or_none(parts[1]) or _parse_int_or_none(parts[2])
    speed_bps = _parse_float_or_none(parts[3])
    eta_seconds = _parse_int_or_none(parts[4])

    percent_raw = parts[5].replace("%", "").strip()
    percent = _parse_float_or_none(percent_raw)
    if percent is None and downloaded_bytes is not None and total_bytes and total_bytes > 0:
        percent = (float(downloaded_bytes) / float(total_bytes)) * 100.0
    if percent is not None:
        percent = max(0.0, min(100.0, percent))

    return {
        "downloaded_bytes": downloaded_bytes,
        "total_bytes": total_bytes,
        "progress_percent": percent,
        "speed_bps": speed_bps,
        "eta_seconds": eta_seconds,
    }


def _run_ytdlp_cli(
    cmd_argv,
    *,
    cancel_check=None,
    cancel_reason=None,
    progress_callback=None,
):
    stderr_lines = []

    proc = subprocess.Popen(
        cmd_argv,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    def _read_stderr():
        stream = proc.stderr
        if stream is None:
            return
        for raw_line in iter(stream.readline, ""):
            stderr_lines.append(raw_line)
            parsed = _parse_progress_line(raw_line)
            if parsed is not None and callable(progress_callback):
                try:
                    progress_callback(parsed)
                except Exception:
                    logger.exception("job_progress_callback_failed")
        try:
            stream.close()
        except Exception:
            pass

    reader = threading.Thread(target=_read_stderr, name="ytdlp-stderr-reader", daemon=True)
    reader.start()

    cancelled = False
    while proc.poll() is None:
        if callable(cancel_check) and cancel_check():
            cancelled = True
            try:
                proc.terminate()
            except Exception:
                pass
            break
        time.sleep(0.2)

    if cancelled:
        try:
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            proc.wait()
        reader.join(timeout=1)
        raise CancelledError(cancel_reason or "Cancelled by user")

    return_code = proc.wait()
    reader.join(timeout=1)
    stderr_output = "".join(stderr_lines).strip()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, cmd_argv, stderr=stderr_output)
    return stderr_output


def download_with_ytdlp(
    url,
    temp_dir,
    config,
    *,
    audio_mode,
    final_format,
    cookie_file=None,
    stop_event=None,
    media_type=None,
    media_intent=None,
    job_id=None,
    origin=None,
    resolved_destination=None,
    cancel_check=None,
    cancel_reason=None,
    output_template_meta=None,
    progress_callback=None,
):
    if (stop_event and stop_event.is_set()) or (callable(cancel_check) and cancel_check()):
        raise CancelledError(cancel_reason or "Cancelled by user")
    # Temp template:
    # - video: include title/uploader/id so finalization can recover metadata even if probe fails
    # - music: keep id-based temp naming (final canonical MB naming is applied later)
    if audio_mode:
        output_template = os.path.join(temp_dir, "%(id)s.%(ext)s")
    else:
        output_template = os.path.join(temp_dir, "%(title).200s - %(uploader).120s - %(id)s.%(ext)s")
    context = {
        "operation": "download",
        "url": url,
        "audio_mode": audio_mode,
        "final_format": final_format,
        "output_template": output_template,
        "output_template_meta": output_template_meta,
        "config": config,
        "cookie_file": cookie_file,
        "overrides": (config or {}).get("yt_dlp_opts") or {},
        "media_type": media_type,
        "media_intent": media_intent,
        "origin": origin,
    }
    invocation = build_ytdlp_invocation(
        job=None,
        context=context,
    )
    opts = invocation["opts"]
    resolved_cookiefile = resolve_cookiefile_for_context(context, config)
    if resolved_cookiefile:
        opts["cookiefile"] = resolved_cookiefile

    normalized_intent = str(media_intent or "").strip().lower()
    if audio_mode and is_music_media_type(media_type) and normalized_intent == "music_track":
        if "format" not in opts:
            _log_event(
                logging.ERROR,
                "music_track_opts_invalid",
                job_id=job_id,
                url=url,
                media_type=media_type,
                media_intent=media_intent,
                format=None,
                noplaylist=opts.get("noplaylist"),
                has_extract_audio=False,
                postprocessors=opts.get("postprocessors"),
                error_message="music_track_missing_format",
            )
            raise RuntimeError("music_track_missing_format")
        fmt = str(opts.get("format") or "").strip().lower()
        valid_format = fmt in {"bestaudio/best", "best"}
        postprocessors = opts.get("postprocessors") or []
        has_extract_audio = any(
            isinstance(pp, dict) and pp.get("key") == "FFmpegExtractAudio"
            for pp in postprocessors
        )
        noplaylist_is_true = bool(opts.get("noplaylist")) is True
        if not (valid_format and has_extract_audio and noplaylist_is_true):
            _log_event(
                logging.ERROR,
                "music_track_opts_invalid",
                job_id=job_id,
                url=url,
                media_type=media_type,
                media_intent=media_intent,
                format=opts.get("format"),
                noplaylist=opts.get("noplaylist"),
                has_extract_audio=has_extract_audio,
                postprocessors=opts.get("postprocessors"),
                error_message="music_track_invalid_audio_pipeline_opts",
            )
            raise RuntimeError("music_track_invalid_audio_pipeline_opts")
        _log_event(
            logging.INFO,
            "music_track_opts_validated",
            job_id=job_id,
            url=url,
            media_type=media_type,
            media_intent=media_intent,
            format=opts.get("format"),
            noplaylist=opts.get("noplaylist"),
            has_extract_audio=has_extract_audio,
        )

    _log_event(
        logging.INFO,
        "FINAL_YTDLP_OPTS",
        job_id=job_id,
        url=url,
        origin=origin,
        media_type=media_type,
        media_intent=media_intent,
        audio_mode=audio_mode,
        resolved_destination=resolved_destination,
        final_format=final_format,
        format=opts.get("format"),
        merge_output_format=opts.get("merge_output_format"),
        recodevideo=opts.get("recodevideo"),
        outtmpl=opts.get("outtmpl"),
        noplaylist=opts.get("noplaylist"),
        postprocessors=opts.get("postprocessors"),
        cookiefile=opts.get("cookiefile"),
        opts=_redact_ytdlp_opts(opts),
    )

    # HARD GUARD: yt-dlp download path must use string outtmpl (CLI parity)
    outtmpl = opts.get("outtmpl")
    if isinstance(outtmpl, dict):
        logging.error(
            "COERCING_DICT_OUTTMPL download_with_ytdlp job_id=%s outtmpl=%r",
            job_id,
            outtmpl,
        )
        default_tmpl = outtmpl.get("default") if isinstance(outtmpl, dict) else None
        opts["outtmpl"] = (
            default_tmpl
            if isinstance(default_tmpl, str) and default_tmpl.strip()
            else "%(title).200s-%(id)s.%(ext)s"
        )

    logging.info(
        "PRE_YTDLP_EXEC job_id=%s outtmpl_type=%s outtmpl=%r",
        job_id,
        type(opts.get("outtmpl")).__name__,
        opts.get("outtmpl"),
    )

    def _is_empty_download_error(e: Exception) -> bool:
        msg = str(e) or ""
        msg_l = msg.lower()
        return (
            "downloaded file is empty" in msg_l
            or "http error 403" in msg_l
            or "forbidden" in msg_l
        )

    from subprocess import CalledProcessError
    info = None
    # Always get metadata via API (for output file info, etc.)
    try:
        probe_opts = copy.deepcopy(opts)
        probe_opts.pop("format", None)
        probe_opts.pop("postprocessors", None)
        probe_opts.pop("merge_output_format", None)
        probe_opts.pop("recodevideo", None)
        probe_opts.pop("final_format", None)
        probe_opts.pop("writethumbnail", None)
        probe_opts.pop("embedthumbnail", None)
        probe_opts["skip_download"] = True
        logger.info(
            {
                "message": "metadata_probe_invocation",
                "job_id": job_id,
                "url": url,
                "format_present": "format" in probe_opts,
                "postprocessors_present": "postprocessors" in probe_opts,
            }
        )
        with YoutubeDL(probe_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as exc:
        probe_error = str(exc)
        lower_probe_error = probe_error.lower()
        format_unavailable_probe = "requested format is not available" in lower_probe_error
        unavailable_class = _classify_ytdlp_unavailability(probe_error)
        _log_event(
            logging.WARNING if format_unavailable_probe else logging.ERROR,
            "ytdlp_metadata_probe_failed",
            job_id=job_id,
            url=url,
            error=probe_error,
            failure_domain=(
                "metadata_probe_unavailable"
                if unavailable_class
                else ("metadata_probe_format" if format_unavailable_probe else "metadata_probe")
            ),
            error_message=probe_error,
            candidate_id=extract_video_id(url),
            unavailable_class=unavailable_class,
        )
        should_escalate_probe_js = any(
            marker in lower_probe_error
            for marker in (
                "signature solving failed",
                "n challenge solving failed",
            )
        )
        if should_escalate_probe_js:
            js_runtime_map = _build_js_runtime_dict(_extract_config_js_runtime_values(config))
            if js_runtime_map:
                retry_probe_opts = copy.deepcopy(probe_opts)
                retry_probe_opts["js_runtimes"] = js_runtime_map
                retry_probe_opts["remote_components"] = "ejs:github"
                _log_event(
                    logging.WARNING,
                    "ytdlp_metadata_probe_js_retry",
                    job_id=job_id,
                    url=url,
                    media_type=media_type,
                    media_intent=media_intent,
                    error=probe_error,
                )
                try:
                    with YoutubeDL(retry_probe_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                except Exception as retry_exc:
                    _log_event(
                        logging.WARNING,
                        "ytdlp_metadata_probe_js_retry_failed",
                        job_id=job_id,
                        url=url,
                        media_type=media_type,
                        media_intent=media_intent,
                        error=str(retry_exc),
                    )
                    info = None

        if info is None:
            _log_event(
                logging.WARNING,
                "ytdlp_metadata_probe_nonfatal_proceeding",
                job_id=job_id,
                url=url,
                media_type=media_type,
                media_intent=media_intent,
                error=probe_error,
                unavailable_class=unavailable_class,
            )
            info = {"id": extract_video_id(url), "webpage_url": url}

    def _is_empty_download_error(e: Exception) -> bool:
        msg = str(e) or ""
        msg_l = msg.lower()
        return (
            "downloaded file is empty" in msg_l
            or "http error 403" in msg_l
            or "forbidden" in msg_l
        )

    # Prepare CLI command for download
    opts_for_run = dict(opts)
    # HARD GUARD again on the copy.
    if isinstance(opts_for_run.get("outtmpl"), dict):
        default_tmpl = opts_for_run["outtmpl"].get("default")
        opts_for_run["outtmpl"] = (
            default_tmpl
            if isinstance(default_tmpl, str) and default_tmpl.strip()
            else "%(title).200s-%(id)s.%(ext)s"
        )
    # Remove progress_hooks if present
    if "progress_hooks" in opts_for_run:
        opts_for_run.pop("progress_hooks")
    # First attempt must be clean/minimal (no cookies/js runtime).
    configured_cookiefile = opts_for_run.pop("cookiefile", None)
    opts_for_run.pop("js_runtimes", None)
    opts_for_run.pop("remote_components", None)
    # Always request sidecar metadata from successful download attempts.
    opts_for_run["writeinfojson"] = True
    opts_for_run["newline"] = True
    opts_for_run["nocolor"] = True
    opts_for_run["progress_template"] = (
        f"download:{_PROGRESS_MARKER} "
        "%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.total_bytes_estimate)s|"
        "%(progress.speed)s|%(progress.eta)s|%(progress._percent_str)s"
    )
    # Build CLI argv and run without a shell (prevents globbing of format strings like [acodec!=none])
    cmd_argv = _render_ytdlp_cli_argv(opts_for_run, url)
    cmd_log = _argv_to_redacted_cli(cmd_argv)

    try:
        _run_ytdlp_cli(
            cmd_argv,
            cancel_check=cancel_check,
            cancel_reason=cancel_reason,
            progress_callback=progress_callback,
        )
        # Log AFTER the command has been executed, per requirement.
        _log_event(
            logging.INFO,
            "YTDLP_CLI_EQUIVALENT",
            job_id=job_id,
            url=url,
            cli=cmd_log,
        )
    except CalledProcessError as exc:
        stderr_output = (exc.stderr or "").strip()
        unavailable_class = _classify_ytdlp_unavailability(stderr_output or str(exc))

        lower_error = stderr_output.lower()
        should_escalate_js = any(
            marker in lower_error
            for marker in (
                "signature solving failed",
                "requested format is not available",
                "n challenge solving failed",
            )
        )
        should_try_cookies = any(
            marker in lower_error
            for marker in (
                "sign in to confirm",
                "age-restricted",
                "forbidden",
                "http error 403",
                "private video",
                "members-only",
                "cookie",
            )
        )

        retry_attempts = []
        if should_escalate_js:
            retry_attempts.append("js")
        if should_try_cookies and configured_cookiefile:
            retry_attempts.append("cookies")
        if should_escalate_js and should_try_cookies and configured_cookiefile:
            retry_attempts.append("js+cookies")

        js_runtime_map = _build_js_runtime_dict(_extract_config_js_runtime_values(config))
        for attempt in retry_attempts:
            retry_opts = dict(opts_for_run)
            if attempt in {"js", "js+cookies"} and js_runtime_map:
                retry_opts["js_runtimes"] = js_runtime_map
                retry_opts["remote_components"] = "ejs:github"
            if attempt in {"cookies", "js+cookies"} and configured_cookiefile:
                retry_opts["cookiefile"] = configured_cookiefile

            cmd_retry_argv = _render_ytdlp_cli_argv(retry_opts, url)
            cmd_retry_log = _argv_to_redacted_cli(cmd_retry_argv)
            _log_event(
                logging.WARNING,
                "YTDLP_SMART_RETRY_ATTEMPT",
                job_id=job_id,
                url=url,
                origin=origin,
                media_type=media_type,
                media_intent=media_intent,
                strategy=attempt,
                error=stderr_output,
            )
            try:
                _run_ytdlp_cli(
                    cmd_retry_argv,
                    cancel_check=cancel_check,
                    cancel_reason=cancel_reason,
                    progress_callback=progress_callback,
                )
                _log_event(
                    logging.INFO,
                    "YTDLP_CLI_EQUIVALENT",
                    job_id=job_id,
                    url=url,
                    cli=cmd_retry_log,
                )
                if (stop_event and stop_event.is_set()) or (
                    callable(cancel_check) and cancel_check()
                ):
                    raise CancelledError(cancel_reason or "Cancelled by user")
                info = _enrich_info_from_sidecar(info, temp_dir=temp_dir, url=url, job_id=job_id)
                return info, _select_download_output(temp_dir, info, audio_mode)
            except CalledProcessError as retry_exc:
                stderr_output = (retry_exc.stderr or "").strip() or stderr_output

        fallback_cookie = _select_youtube_cookie_fallback(
            config=config,
            url=url,
            stderr_text=stderr_output,
            opts=opts_for_run,
            media_type=media_type,
        )
        if fallback_cookie:
            _log_event(
                logging.INFO,
                "YTDLP_YOUTUBE_COOKIE_FALLBACK_ATTEMPT",
                job_id=job_id,
                url=url,
                origin=origin,
                media_type=media_type,
                media_intent=media_intent,
                error=stderr_output,
            )
            retry_opts = dict(opts_for_run)
            retry_opts["cookiefile"] = fallback_cookie
            cmd_retry_argv = _render_ytdlp_cli_argv(retry_opts, url)
            cmd_retry_log = _argv_to_redacted_cli(cmd_retry_argv)
            try:
                _run_ytdlp_cli(
                    cmd_retry_argv,
                    cancel_check=cancel_check,
                    cancel_reason=cancel_reason,
                    progress_callback=progress_callback,
                )
                _log_event(
                    logging.INFO,
                    "YTDLP_YOUTUBE_COOKIE_FALLBACK_SUCCEEDED",
                    job_id=job_id,
                    url=url,
                    origin=origin,
                    media_type=media_type,
                    media_intent=media_intent,
                )
                _log_event(
                    logging.INFO,
                    "YTDLP_CLI_EQUIVALENT",
                    job_id=job_id,
                    url=url,
                    cli=cmd_retry_log,
                )
                if (stop_event and stop_event.is_set()) or (
                    callable(cancel_check) and cancel_check()
                ):
                    raise CancelledError(cancel_reason or "Cancelled by user")
                info = _enrich_info_from_sidecar(info, temp_dir=temp_dir, url=url, job_id=job_id)
                return info, _select_download_output(temp_dir, info, audio_mode)
            except CalledProcessError as fallback_exc:
                fallback_message = (fallback_exc.stderr or "").strip()
                _log_event(
                    logging.ERROR,
                    "YTDLP_YOUTUBE_COOKIE_FALLBACK_FAILED",
                    job_id=job_id,
                    url=url,
                    origin=origin,
                    media_type=media_type,
                    media_intent=media_intent,
                    error=fallback_message,
                )
                raise CookieFallbackError(f"yt_dlp_cookie_fallback_failed: {fallback_exc}")
        if configured_cookiefile:
            found = False
            for entry in os.listdir(temp_dir):
                if entry.endswith((".part", ".ytdl", ".temp")):
                    continue
                candidate = os.path.join(temp_dir, entry)
                if os.path.isfile(candidate) and os.path.getsize(candidate) > 0:
                    found = True
                    break

            if not found:
                _log_event(
                    logging.WARNING,
                    "YTDLP_EMPTY_FILE_RETRY_NO_COOKIES",
                    job_id=job_id,
                    url=url,
                    origin=origin,
                    media_type=media_type,
                    media_intent=media_intent,
                    audio_mode=audio_mode,
                    final_format=final_format,
                    format=opts.get("format"),
                    merge_output_format=opts.get("merge_output_format"),
                    outtmpl=opts.get("outtmpl"),
                    noplaylist=opts.get("noplaylist"),
                )

                retry_opts = dict(opts_for_run)
                retry_opts.pop("cookiefile", None)
                cmd_retry_argv = _render_ytdlp_cli_argv(retry_opts, url)
                cmd_retry_log = _argv_to_redacted_cli(cmd_retry_argv)
                try:
                    _run_ytdlp_cli(
                        cmd_retry_argv,
                        cancel_check=cancel_check,
                        cancel_reason=cancel_reason,
                        progress_callback=progress_callback,
                    )
                    _log_event(
                        logging.INFO,
                        "YTDLP_CLI_EQUIVALENT",
                        job_id=job_id,
                        url=url,
                        cli=cmd_retry_log,
                    )
                except CalledProcessError as retry_exc:
                    _log_event(
                        logging.ERROR,
                        "YTDLP_EMPTY_FILE_RETRY_FAILED",
                        job_id=job_id,
                        url=url,
                        origin=origin,
                        media_type=media_type,
                        media_intent=media_intent,
                        audio_mode=audio_mode,
                        final_format=final_format,
                        error=str(retry_exc),
                        opts=_redact_ytdlp_opts(retry_opts),
                        unavailable_class=unavailable_class,
                    )
                    if unavailable_class:
                        raise RuntimeError(f"yt_dlp_source_unavailable:{unavailable_class}: {retry_exc}")
                    raise RuntimeError(f"yt_dlp_download_failed: {retry_exc}")
            else:
                _log_event(
                    logging.ERROR,
                    "ytdlp_download_failed",
                    job_id=job_id,
                    url=url,
                    origin=origin,
                    media_type=media_type,
                    media_intent=media_intent,
                    audio_mode=audio_mode,
                    final_format=final_format,
                    format=opts.get("format"),
                    merge_output_format=opts.get("merge_output_format"),
                    outtmpl=opts.get("outtmpl"),
                    noplaylist=opts.get("noplaylist"),
                    error=str(exc),
                    unavailable_class=unavailable_class,
                )
                if unavailable_class:
                    raise RuntimeError(f"yt_dlp_source_unavailable:{unavailable_class}: {exc}")
                raise RuntimeError(f"yt_dlp_download_failed: {exc}")
        else:
            _log_event(
                logging.ERROR,
                "ytdlp_download_failed",
                job_id=job_id,
                url=url,
                origin=origin,
                media_type=media_type,
                media_intent=media_intent,
                audio_mode=audio_mode,
                final_format=final_format,
                format=opts.get("format"),
                merge_output_format=opts.get("merge_output_format"),
                outtmpl=opts.get("outtmpl"),
                noplaylist=opts.get("noplaylist"),
                error=str(exc),
                unavailable_class=unavailable_class,
            )
            if unavailable_class:
                raise RuntimeError(f"yt_dlp_source_unavailable:{unavailable_class}: {exc}")
            raise RuntimeError(f"yt_dlp_download_failed: {exc}")

    if (stop_event and stop_event.is_set()) or (callable(cancel_check) and cancel_check()):
        raise CancelledError(cancel_reason or "Cancelled by user")
    info = _enrich_info_from_sidecar(info, temp_dir=temp_dir, url=url, job_id=job_id)
    return info, _select_download_output(temp_dir, info, audio_mode)


def _select_download_output(temp_dir, info, audio_mode):
    local_path = None
    if isinstance(info, dict):
        local_path = info.get("_filename")
        if not local_path and info.get("requested_downloads"):
            for req in info.get("requested_downloads"):
                local_path = req.get("filepath") or req.get("filename")
                if local_path:
                    break

    if local_path and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return local_path

    candidates = []
    audio_candidates = []
    for entry in os.listdir(temp_dir):
        lower_entry = entry.lower()
        if lower_entry.endswith((".part", ".ytdl", ".temp")):
            continue
        if lower_entry.endswith((
            ".info.json",
            ".description",
            ".json",
            ".jpg",
            ".jpeg",
            ".png",
            ".webp",
            ".vtt",
            ".srt",
            ".ass",
            ".lrc",
        )):
            continue
        candidate = os.path.join(temp_dir, entry)
        if not os.path.isfile(candidate):
            continue
        try:
            size = os.path.getsize(candidate)
        except OSError:
            size = 0
        if size <= 0:
            continue
        candidates.append((size, candidate))
        if os.path.splitext(candidate)[1].lower() in {
            ".m4a",
            ".webm",
            ".opus",
            ".aac",
            ".mp3",
            ".flac",
        }:
            audio_candidates.append((size, candidate))

    if audio_mode:
        if not audio_candidates:
            raise PostprocessingError(
                "No audio stream resolved (video-only format selected)"
            )
        audio_candidates.sort(reverse=True)
        return audio_candidates[0][1]

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1]

    raise RuntimeError("yt_dlp_no_output")


def preview_direct_url(url, config):
    if not isinstance(config, dict) or not config:
        raise RuntimeError("search_missing_runtime_config")
    context = {
        "operation": "metadata",
        "url": url,
        "config": config,
        "media_type": "video",
        "media_intent": "episode",
    }

    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "retries": 2,
        "fragment_retries": 2,
    }

    cookie_file = resolve_cookiefile_for_context(context, config)
    if cookie_file:
        opts["cookiefile"] = cookie_file

    def _youtube_oembed_fallback(target_url: str, video_id: str | None):
        oembed_title = None
        oembed_author = None
        oembed_thumbnail = None
        candidate_urls = []
        if target_url:
            candidate_urls.append(target_url)
        if video_id:
            watch_url = f"https://www.youtube.com/watch?v={video_id}"
            if watch_url not in candidate_urls:
                candidate_urls.append(watch_url)
            short_url = f"https://youtu.be/{video_id}"
            if short_url not in candidate_urls:
                candidate_urls.append(short_url)
        for candidate_url in candidate_urls:
            try:
                oembed_resp = requests.get(
                    "https://www.youtube.com/oembed",
                    params={"url": candidate_url, "format": "json"},
                    timeout=5,
                )
                if not oembed_resp.ok:
                    continue
                oembed_data = oembed_resp.json() if oembed_resp.content else {}
                if not isinstance(oembed_data, dict):
                    continue
                oembed_title = str(oembed_data.get("title") or "").strip() or oembed_title
                oembed_author = str(oembed_data.get("author_name") or "").strip() or oembed_author
                oembed_thumbnail = str(oembed_data.get("thumbnail_url") or "").strip() or oembed_thumbnail
                if oembed_title and oembed_author:
                    break
            except Exception:
                continue
        return oembed_title, oembed_author, oembed_thumbnail

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        meta = extract_meta(info, fallback_url=url)
        title = meta.get("title")
        uploader = meta.get("channel") or meta.get("artist")
        thumb = meta.get("thumbnail_url")
        if not (title and uploader):
            video_id = extract_video_id(url)
            oembed_title, oembed_author, oembed_thumbnail = _youtube_oembed_fallback(url, video_id)
            title = title or oembed_title or (f"YouTube Video ({video_id})" if video_id else "YouTube Video")
            uploader = uploader or oembed_author or "YouTube"
            thumb = thumb or oembed_thumbnail or (
                f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg" if video_id else None
            )
        return {
            "title": title,
            "uploader": uploader,
            "thumbnail_url": thumb,
            "url": meta.get("url") or url,
            "source": resolve_source(url),
            "duration_sec": info.get("duration") if isinstance(info, dict) else None,
        }
    except Exception as exc:
        js_runtime_map = _build_js_runtime_dict(_extract_config_js_runtime_values(config))
        if js_runtime_map:
            retry_opts = dict(opts)
            retry_opts["js_runtimes"] = js_runtime_map
            retry_opts["remote_components"] = "ejs:github"
            try:
                with YoutubeDL(retry_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
            except Exception:
                info = None
            else:
                meta = extract_meta(info, fallback_url=url)
                return {
                    "title": meta.get("title"),
                    "uploader": meta.get("channel") or meta.get("artist"),
                    "thumbnail_url": meta.get("thumbnail_url"),
                    "url": meta.get("url") or url,
                    "source": resolve_source(url),
                    "duration_sec": info.get("duration") if isinstance(info, dict) else None,
                }

        video_id = extract_video_id(url)
        oembed_title, oembed_author, oembed_thumbnail = _youtube_oembed_fallback(url, video_id)
        fallback_title = oembed_title or (f"YouTube Video ({video_id})" if video_id else "YouTube Video")
        fallback_uploader = oembed_author or "YouTube"
        fallback_thumbnail = (
            oembed_thumbnail
            or (f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg" if video_id else None)
        )
        fallback_has_core = bool(fallback_title and fallback_uploader)
        _log_event(
            logging.INFO if fallback_has_core else logging.WARNING,
            "preview_direct_url_extract_failed_fallback_used" if fallback_has_core else "preview_direct_url_extract_failed",
            url=url,
            source=resolve_source(url),
            error=str(exc),
        )
        return {
            "title": fallback_title,
            "uploader": fallback_uploader,
            "thumbnail_url": fallback_thumbnail,
            "url": url,
            "source": resolve_source(url),
            "duration_sec": None,
        }


def extract_meta(info, *, fallback_url=None):
    if not isinstance(info, dict):
        return {}
    tags = info.get("tags") or []
    if isinstance(tags, set):
        tags = sorted(tags)
    elif isinstance(tags, tuple):
        tags = list(tags)
    elif not isinstance(tags, list):
        tags = [str(tags)]
    return {
        "video_id": info.get("id"),
        "title": info.get("title"),
        "channel": info.get("channel") or info.get("uploader"),
        "channel_id": info.get("channel_id") or info.get("uploader_id"),
        "artist": info.get("artist") or info.get("uploader"),
        "album": info.get("album"),
        "album_artist": info.get("album_artist"),
        "track": info.get("track") or info.get("title"),
        "track_number": info.get("track_number"),
        "disc": info.get("disc_number"),
        "release_date": info.get("release_date"),
        "upload_date": info.get("upload_date"),
        "description": info.get("description") or "",
        "tags": tags,
        "url": info.get("webpage_url") or fallback_url,
        "thumbnail_url": info.get("thumbnail"),
    }


def sanitize_for_filesystem(name, maxlen=180):
    if not name:
        return ""
    safe = sanitize_component(str(name))
    safe = re.sub(r"\s+", " ", safe)
    return safe[:maxlen].strip()


def pretty_filename(title, channel, upload_date):
    safe_title = sanitize_for_filesystem(title or "")
    safe_channel = sanitize_for_filesystem(channel or "")
    if safe_channel:
        return f"{safe_title} - {safe_channel}".strip(" -")
    return safe_title or "media"


def _clean_audio_title(value):
    if not value:
        return ""
    cleaned = _AUDIO_TITLE_CLEAN_RE.sub(" ", value)
    cleaned = _AUDIO_TITLE_TRAIL_RE.sub("", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _clean_audio_artist(value):
    if not value:
        return ""
    cleaned = _AUDIO_ARTIST_VEVO_RE.sub("", value).strip()
    return re.sub(r"\s+", " ", cleaned).strip()


def normalize_track_number(value):
    return parse_first_positive_int(value)


def format_track_number(value):
    return format_zero_padded_track_number(value)


def _normalize_nfc(value):
    return unicodedata.normalize("NFC", str(value or ""))


def _extract_release_year(value):
    text = str(value or "").strip()
    if len(text) >= 4 and text[:4].isdigit():
        return text[:4]
    return ""


def build_audio_filename(meta, ext, *, template=None, fallback_id=None, require_release_metadata=True):
    required_album = str(meta.get("album") or "").strip()
    required_release_date = str(meta.get("release_date") or meta.get("date") or "").strip()
    required_track_number = normalize_track_number(meta.get("track_number"))
    required_release_group_id = str(meta.get("mb_release_group_id") or "").strip()
    if (
        not required_album
        or not required_release_date
        or required_track_number is None
        or required_track_number <= 0
        or not required_release_group_id
    ):
        if require_release_metadata:
            raise RuntimeError("music_release_metadata_incomplete_before_path_build")
        fallback_title = meta.get("track") or meta.get("title") or fallback_id or "media"
        fallback_artist = meta.get("artist") or meta.get("channel") or ""
        return f"{pretty_filename(fallback_title, fallback_artist, None)}.{ext}"

    album_artist = sanitize_for_filesystem(
        _normalize_nfc(_clean_audio_artist(meta.get("album_artist") or ""))
    ) or "Unknown Artist"
    album_title = sanitize_for_filesystem(_normalize_nfc(_clean_audio_title(meta.get("album") or ""))) or "Unknown Album"
    track = sanitize_for_filesystem(_normalize_nfc(_clean_audio_title(meta.get("track") or meta.get("title") or "")))
    track_number = format_track_number(meta.get("track_number")) or "00"
    disc_number = normalize_track_number(meta.get("disc") or meta.get("disc_number"))
    disc_total = normalize_track_number(meta.get("disc_total"))
    release_year = _extract_release_year(meta.get("release_date") or meta.get("date"))
    album_folder = f"{album_title} ({release_year})" if release_year else album_title
    # Audio paths are canonical and intentionally ignore custom templates.
    # This avoids structural drift and duplicate Disc folder segments.
    _ = template
    _ = fallback_id

    track_label = f"{track_number} - {track or 'media'}.{ext}"
    return build_music_relative_layout(
        album_artist=album_artist,
        album_folder=album_folder,
        track_label=track_label,
        disc_number=disc_number or 1,
        disc_total=disc_total,
    )


def build_output_filename(meta, fallback_id, ext, template, audio_mode, *, enforce_music_contract=False):
    if audio_mode:
        return build_audio_filename(
            meta,
            ext,
            template=template,
            fallback_id=fallback_id,
            require_release_metadata=bool(enforce_music_contract),
        )
    if template:
        try:
            rendered = template % {
                "title": sanitize_for_filesystem(meta.get("title") or fallback_id),
                "uploader": sanitize_for_filesystem(meta.get("channel") or ""),
                "upload_date": "",
                "ext": ext,
                "id": "",
            }
            if rendered:
                # Guard against template artifacts when optional fields are blank
                # (e.g. "<id> - - .mp4" or "Title - Channel - .mp4").
                if re.search(r"\s-\s-", rendered) or re.search(r"\s-\s*\.[A-Za-z0-9]+$", rendered):
                    return f"{pretty_filename(meta.get('title') or fallback_id, meta.get('channel'), meta.get('upload_date'))}.{ext}"
                return rendered
        except Exception:
            pass
    return f"{pretty_filename(meta.get('title') or fallback_id, meta.get('channel'), meta.get('upload_date'))}.{ext}"


def _hydrate_meta_from_output_template(meta, output_template):
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(output_template, dict):
        return meta

    template_title = str(output_template.get("title") or output_template.get("track") or "").strip()
    template_channel = str(
        output_template.get("channel")
        or output_template.get("uploader")
        or output_template.get("artist")
        or ""
    ).strip()

    if not str(meta.get("title") or "").strip() and template_title:
        meta["title"] = template_title
        if not str(meta.get("track") or "").strip():
            meta["track"] = template_title

    channel_present = str(meta.get("channel") or meta.get("artist") or "").strip()
    if not channel_present and template_channel:
        meta["channel"] = template_channel
        if not str(meta.get("artist") or "").strip():
            meta["artist"] = template_channel

    return meta


def _hydrate_meta_from_local_filename(meta, *, local_file, fallback_id=None):
    if not isinstance(meta, dict):
        meta = {}
    filename = os.path.basename(str(local_file or "")).strip()
    if not filename:
        return meta

    stem, _ = os.path.splitext(filename)
    if not stem:
        return meta

    title_present = str(meta.get("title") or "").strip()
    channel_present = str(meta.get("channel") or meta.get("artist") or "").strip()
    if title_present and channel_present:
        return meta

    candidate_title = None
    candidate_channel = None
    parts = [part.strip() for part in stem.rsplit(" - ", 2)]
    if len(parts) == 3:
        maybe_title, maybe_channel, maybe_id = parts
        id_hint = str(fallback_id or meta.get("video_id") or "").strip()
        if id_hint and maybe_id and maybe_id != id_hint:
            return meta
        candidate_title = maybe_title
        candidate_channel = maybe_channel
    elif len(parts) == 2:
        candidate_title, candidate_channel = parts

    if not title_present and candidate_title:
        meta["title"] = candidate_title
        if not str(meta.get("track") or "").strip():
            meta["track"] = candidate_title
    if not channel_present and candidate_channel:
        meta["channel"] = candidate_channel
        if not str(meta.get("artist") or "").strip():
            meta["artist"] = candidate_channel

    return meta


def _assert_music_canonical_metadata_contract(meta):
    if not isinstance(meta, dict):
        raise RuntimeError("music_track_requires_mb_bound_metadata")
    try:
        track_number = int(meta.get("track_number"))
    except Exception:
        track_number = 0
    try:
        disc_number = int(meta.get("disc_number") or meta.get("disc") or 0)
    except Exception:
        disc_number = 0
    required_present = all(
        str(meta.get(key) or "").strip()
        for key in ("album", "release_date", "mb_release_id", "mb_release_group_id")
    )
    if not required_present or track_number <= 0 or disc_number <= 0:
        raise RuntimeError("music_track_requires_mb_bound_metadata")


def _probe_media_profile(file_path):
    path = str(file_path or "").strip()
    if not path:
        return {
            "final_container": None,
            "final_video_codec": None,
            "final_audio_codec": None,
        }
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=format_name:stream=codec_type,codec_name",
        "-of",
        "json",
        path,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        payload = json.loads(proc.stdout or "{}")
    except Exception:
        return {
            "final_container": None,
            "final_video_codec": None,
            "final_audio_codec": None,
        }

    container = None
    fmt = payload.get("format") if isinstance(payload, dict) else None
    if isinstance(fmt, dict):
        format_name = str(fmt.get("format_name") or "").strip().lower()
        if format_name:
            container = format_name.split(",")[0].strip() or None

    video_codec = None
    audio_codec = None
    streams = payload.get("streams") if isinstance(payload, dict) else None
    if isinstance(streams, list):
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            codec_type = str(stream.get("codec_type") or "").strip().lower()
            codec_name = str(stream.get("codec_name") or "").strip().lower() or None
            if codec_type == "video" and video_codec is None:
                video_codec = codec_name
            elif codec_type == "audio" and audio_codec is None:
                audio_codec = codec_name

    return {
        "final_container": container,
        "final_video_codec": video_codec,
        "final_audio_codec": audio_codec,
    }


def _enforce_video_codec_container_rules(local_file, *, target_container):
    container = str(target_container or "").strip().lower()
    profile = _probe_media_profile(local_file)
    if container != "mp4":
        return local_file, profile
    audio_codec = str(profile.get("final_audio_codec") or "").strip().lower()
    if audio_codec == "aac":
        return local_file, profile

    mp4_compatible_path = f"{local_file}.mp4compat"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(local_file),
        "-map",
        "0:v?",
        "-map",
        "0:a?",
        "-map",
        "0:s?",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-c:s",
        "copy",
        "-movflags",
        "+faststart",
        mp4_compatible_path,
    ]
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except Exception as exc:
        raise RuntimeError(f"mp4_audio_codec_enforcement_failed: {exc}") from exc

    os.replace(mp4_compatible_path, local_file)
    updated = _probe_media_profile(local_file)
    updated_audio = str(updated.get("final_audio_codec") or "").strip().lower()
    if updated_audio != "aac":
        raise RuntimeError(
            f"mp4_audio_codec_enforcement_failed: expected_aac got={updated_audio or 'unknown'}"
        )
    return local_file, updated


def finalize_download_artifact(
    *,
    local_file,
    meta,
    fallback_id,
    destination_dir,
    audio_mode,
    final_format,
    template,
    paths=None,
    config=None,
    enforce_music_contract=False,
    enqueue_audio_metadata=False,
):
    if not local_file:
        raise RuntimeError("missing_local_file_for_finalize")
    meta = dict(meta or {})
    fallback_id = str(fallback_id or "")
    if not audio_mode:
        target_container = _normalize_format(final_format) or "mkv"
        local_file, media_profile = _enforce_video_codec_container_rules(
            local_file,
            target_container=target_container,
        )
        meta["runtime_media_profile"] = {
            "final_container": media_profile.get("final_container"),
            "final_video_codec": media_profile.get("final_video_codec"),
            "final_audio_codec": media_profile.get("final_audio_codec"),
        }
    ext = os.path.splitext(local_file)[1].lstrip(".")
    if audio_mode:
        actual_ext = str(ext or "").strip().lower()
        normalized_actual_ext = _normalize_audio_format(actual_ext)
        normalized_configured_ext = _normalize_audio_format(final_format)
        ext = normalized_actual_ext or actual_ext or normalized_configured_ext or "mp3"
    elif not ext:
        ext = _normalize_format(final_format) or "mkv"

    cleaned_name = build_output_filename(
        meta,
        fallback_id,
        ext,
        template,
        audio_mode,
        enforce_music_contract=bool(enforce_music_contract),
    )
    if audio_mode and enforce_music_contract:
        _assert_music_canonical_metadata_contract(meta)
        normalized_cleaned = str(cleaned_name or "").replace("\\", "/")
        if not normalized_cleaned.startswith("Music/"):
            raise RuntimeError("music_filename_contract_violation")

    final_path = os.path.join(destination_dir, cleaned_name)
    final_path = resolve_collision_path(final_path)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    if not audio_mode and paths is not None and getattr(paths, "thumbs_dir", None):
        embed_metadata(local_file, meta, fallback_id, paths.thumbs_dir)

    atomic_move(local_file, final_path)

    if audio_mode and enqueue_audio_metadata and isinstance(config, dict):
        try:
            enqueue_media_metadata(final_path, meta, config)
        except Exception:
            pass

    return final_path, meta


def resolve_collision_path(path):
    if not os.path.exists(path):
        return path
    stem, ext = os.path.splitext(path)
    attempt = 2
    while True:
        candidate = f"{stem} ({attempt}){ext}"
        if not os.path.exists(candidate):
            return candidate
        attempt += 1


def atomic_move(src, dst):
    try:
        os.replace(src, dst)
    except OSError:
        shutil.copy2(src, dst)
        os.remove(src)


def embed_metadata(local_file, meta, video_id, thumbs_dir):
    """
    Best-effort metadata embed.

    IMPORTANT:
    - This must NEVER fail the job. Any ffmpeg failure is logged and ignored.
    - Artwork embedding differs by container:
        * MP4/M4A/MOV: use second input + attached_pic stream.
        * MKV/WEBM: use -attach.
        * Otherwise: skip artwork.
    """
    if not meta:
        return

    title = meta.get("title") or video_id
    channel = meta.get("channel") or ""
    channel_id = meta.get("channel_id") or ""
    artist = meta.get("artist") or channel
    album = meta.get("album")
    album_artist = meta.get("album_artist")
    track = meta.get("track")
    track_number = meta.get("track_number")
    disc = meta.get("disc")
    release_date = meta.get("release_date")
    upload_date = meta.get("upload_date") or ""
    description = meta.get("description") or ""
    tags = meta.get("tags") or []
    url = meta.get("url") or f"https://www.youtube.com/watch?v={video_id}"
    thumb_url = meta.get("thumbnail_url")

    # Normalize date tag to YYYY-MM-DD
    date_tag = ""
    raw_date = release_date or upload_date
    if raw_date and len(str(raw_date)) == 8 and str(raw_date).isdigit():
        raw_date = str(raw_date)
        date_tag = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

    # Normalize tags
    try:
        if isinstance(tags, set):
            tags = sorted(tags)
        elif isinstance(tags, tuple):
            tags = list(tags)
        elif not isinstance(tags, list):
            tags = [str(tags)]
    except Exception:
        tags = []

    keywords = ", ".join([str(t) for t in tags if t]) if tags else ""
    comment = f"YouTubeID={video_id} URL={url}"
    if channel_id:
        comment = f"{comment} ChannelID={channel_id}"

    # Truncate potentially huge fields to avoid container/tag limits
    def _truncate(s: str, limit: int) -> str:
        s = (s or "").replace("\x00", " ")
        s = re.sub(r"\s+", " ", s).strip()
        if len(s) > limit:
            return s[:limit] + "…"
        return s

    # Conservative limits; MP4 atoms can be picky.
    title = _truncate(title, 256)
    artist = _truncate(artist, 256)
    album = _truncate(album, 256) if album else None
    album_artist = _truncate(album_artist, 256) if album_artist else None
    track = _truncate(track, 256) if track else None
    description = _truncate(description, 2048)
    keywords = _truncate(keywords, 512) if keywords else ""
    comment = _truncate(comment, 512)

    base_ext = os.path.splitext(local_file)[1] or ".webm"
    ext_lower = base_ext.lower()

    # If this is audio-only, we currently skip embedding (to avoid regressions).
    audio_only = ext_lower in {".mp3", ".m4a", ".opus", ".aac", ".flac"}
    if audio_only:
        return

    is_mp4_family = ext_lower in {".mp4", ".m4v", ".mov", ".m4a"}
    is_mkv_family = ext_lower in {".mkv", ".webm"}

    thumb_path = None
    if thumb_url and thumbs_dir:
        try:
            os.makedirs(thumbs_dir, exist_ok=True)
            thumb_path = os.path.join(thumbs_dir, f"{video_id}.jpg")
            resp = requests.get(thumb_url, timeout=15)
            if resp.ok and resp.content:
                with open(thumb_path, "wb") as handle:
                    handle.write(resp.content)
            else:
                thumb_path = None
        except Exception:
            # Never fail on thumbs
            logging.warning("Thumbnail download failed for %s", video_id)
            thumb_path = None

    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=f".tagged{base_ext}",
        dir=os.path.dirname(local_file),
    )
    os.close(tmp_fd)

    def _add_common_metadata(cmd_list: list[str]):
        if title:
            cmd_list.extend(["-metadata", f"title={title}"])
        if artist:
            cmd_list.extend(["-metadata", f"artist={artist}"])
        if album:
            cmd_list.extend(["-metadata", f"album={album}"])
        if album_artist:
            cmd_list.extend(["-metadata", f"album_artist={album_artist}"])
        if track:
            # Track name
            cmd_list.extend(["-metadata", f"track={track}"])
        if track_number is not None:
            # Prefer common track number tag in a safe form
            try:
                tn = int(track_number)
                cmd_list.extend(["-metadata", f"track_number={tn}"])
            except Exception:
                pass
        if disc is not None:
            try:
                dn = int(disc)
                cmd_list.extend(["-metadata", f"disc={dn}"])
            except Exception:
                pass
        if date_tag:
            cmd_list.extend(["-metadata", f"date={date_tag}"])
        if description:
            cmd_list.extend(["-metadata", f"description={description}"])
        if channel_id:
            cmd_list.extend(["-metadata", f"source_channel_id={channel_id}"])
        if keywords:
            cmd_list.extend(["-metadata", f"keywords={keywords}"])
        if comment:
            cmd_list.extend(["-metadata", f"comment={comment}"])

    try:
        cmd: list[str] = ["ffmpeg", "-y", "-i", local_file]

        # Artwork embedding
        if thumb_path and os.path.exists(thumb_path):
            if is_mp4_family:
                # MP4: add image as attached_pic stream
                cmd.extend(["-i", thumb_path])
                cmd.extend(["-map", "0", "-map", "1"])
                cmd.extend(["-c", "copy"])
                # Ensure the cover stream is a compatible codec
                cmd.extend(["-c:v:1", "mjpeg"])
                cmd.extend(["-disposition:v:1", "attached_pic"])
                cmd.extend(["-metadata:s:v:1", "title=cover", "-metadata:s:v:1", "comment=Cover (front)"])
            elif is_mkv_family:
                # MKV/WEBM: attachments are supported
                cmd.extend([
                    "-attach",
                    thumb_path,
                    "-metadata:s:t",
                    "mimetype=image/jpeg",
                    "-metadata:s:t",
                    "filename=cover.jpg",
                ])
                cmd.extend(["-c", "copy"])
            else:
                # Unknown container: skip artwork
                cmd.extend(["-c", "copy"])
        else:
            cmd.extend(["-c", "copy"])

        _add_common_metadata(cmd)
        cmd.append(tmp_path)

        # Capture stderr for debugging, but NEVER raise out of this function.
        proc = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        if proc.returncode != 0:
            err = (proc.stderr or "").strip()
            if err:
                err = _truncate(err, 800)
            logging.warning(
                "ffmpeg metadata embedding skipped for %s (rc=%s)%s",
                video_id,
                proc.returncode,
                f" err={err}" if err else "",
            )
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return

        os.replace(tmp_path, local_file)
        logging.info("[%s] Metadata embedded successfully", video_id)

    except Exception as exc:
        # Absolute last-resort: never fail the download
        logging.warning("ffmpeg metadata embedding skipped for %s (%s)", video_id, exc)
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
    finally:
        if thumb_path:
            try:
                os.unlink(thumb_path)
            except Exception:
                pass


def enqueue_media_metadata(file_path, meta, config):
    try:
        enqueue_metadata(file_path, meta, config)
    except Exception:
        logging.exception("Metadata enqueue failed for %s", file_path)


def _album_output_dir_from_track_path(file_path):
    path = str(file_path or "").strip()
    if not path:
        return None
    try:
        parent = os.path.dirname(path)
        leaf = os.path.basename(parent).strip().lower()
        if leaf.startswith("disc "):
            return os.path.dirname(parent)
        return parent
    except Exception:
        return None


def _album_output_dir_from_job(job):
    output_template = getattr(job, "output_template", None)
    if not isinstance(output_template, dict):
        return None
    base_dir = str(
        output_template.get("output_dir")
        or getattr(job, "resolved_destination", None)
        or ""
    ).strip()
    if not base_dir:
        return None
    canonical = output_template.get("canonical_metadata") if isinstance(output_template.get("canonical_metadata"), dict) else {}
    album_artist = str(
        output_template.get("album_artist")
        or canonical.get("album_artist")
        or output_template.get("artist")
        or canonical.get("artist")
        or ""
    ).strip()
    album = str(
        output_template.get("album")
        or canonical.get("album")
        or ""
    ).strip()
    if not album_artist or not album:
        return None
    release_date = str(
        output_template.get("release_date")
        or canonical.get("release_date")
        or canonical.get("date")
        or ""
    ).strip()
    year = release_date[:4] if len(release_date) >= 4 and release_date[:4].isdigit() else ""
    album_folder = sanitize_component(album)
    if year:
        album_folder = f"{album_folder} ({year})"
    return os.path.join(base_dir, "Music", sanitize_component(album_artist), album_folder)


def _normalize_runtime_failure_reason(last_error, output_template):
    runtime_search_meta = output_template.get("runtime_search_meta") if isinstance(output_template.get("runtime_search_meta"), dict) else {}
    runtime_reason = str(runtime_search_meta.get("failure_reason") or "").strip().lower()
    if runtime_reason:
        return runtime_reason
    text = str(last_error or "").strip().lower()
    if not text:
        return "no_candidates_retrieved"
    marker = "yt_dlp_source_unavailable:"
    if marker in text:
        tail = text.split(marker, 1)[1]
        unavailable_class = tail.split(":", 1)[0].strip() or "unknown"
        return f"source_unavailable:{unavailable_class}"
    if "source_unavailable:" in text:
        tail = text.split("source_unavailable:", 1)[1]
        unavailable_class = tail.split(":", 1)[0].strip() or "unknown"
        return f"source_unavailable:{unavailable_class}"
    if "duration_filtered" in text:
        return "duration_filtered"
    if "no_candidate_above_threshold" in text:
        return "no_candidate_above_threshold"
    if "no_candidates" in text:
        return "no_candidates_retrieved"
    if "album_similarity_blocked" in text:
        return "album_similarity_blocked"
    if "all_filtered_by_gate" in text:
        return "all_filtered_by_gate"
    return "all_filtered_by_gate"


def _classify_runtime_missing_hint(failure_reason):
    reason = str(failure_reason or "").strip().lower()
    if reason.startswith("source_unavailable:"):
        return ("unavailable", "Unavailable (blocked/removed)")
    if reason == "duration_filtered":
        return (
            "likely_wrong_mb_recording_length",
            "Likely wrong MB recording length (duration mismatch persistent across many candidates)",
        )
    return ("recoverable_ladder_extension", "Recoverable by ladder extension (no candidates)")


def _normalize_decision_edge(value):
    edge = value if isinstance(value, dict) else {}
    accepted = edge.get("accepted_selection") if isinstance(edge.get("accepted_selection"), dict) else None
    final_rejection = edge.get("final_rejection") if isinstance(edge.get("final_rejection"), dict) else None
    rejected_candidates = edge.get("rejected_candidates") if isinstance(edge.get("rejected_candidates"), list) else []
    normalized_rejected = [dict(item) for item in rejected_candidates if isinstance(item, dict)]
    candidate_variant_distribution = edge.get("candidate_variant_distribution") if isinstance(edge.get("candidate_variant_distribution"), dict) else {}
    normalized_distribution = {}
    for key, value in candidate_variant_distribution.items():
        tag = str(key or "").strip()
        if not tag:
            continue
        try:
            normalized_distribution[tag] = int(value or 0)
        except Exception:
            continue
    selected_variant_tags = edge.get("selected_candidate_variant_tags") if isinstance(edge.get("selected_candidate_variant_tags"), list) else []
    normalized_selected_variant_tags = sorted({str(tag or "").strip() for tag in selected_variant_tags if str(tag or "").strip()})
    top_rejected_variant_tags = edge.get("top_rejected_variant_tags") if isinstance(edge.get("top_rejected_variant_tags"), list) else []
    normalized_top_rejected_variant_tags = [str(tag or "").strip() for tag in top_rejected_variant_tags if str(tag or "").strip()]
    return {
        "accepted_selection": accepted,
        "final_rejection": final_rejection,
        "rejected_candidates": normalized_rejected,
        "candidate_variant_distribution": dict(sorted(normalized_distribution.items(), key=lambda item: item[0])),
        "selected_candidate_variant_tags": normalized_selected_variant_tags,
        "top_rejected_variant_tags": normalized_top_rejected_variant_tags,
    }


def build_music_album_run_summary(db_path, album_run_id):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, status, last_error, output_template, file_path
            FROM download_jobs
            WHERE origin=? AND origin_id=? AND media_intent=?
            ORDER BY created_at ASC, id ASC
            """,
            ("music_album", str(album_run_id or ""), "music_track"),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    tracks_total = len(rows)
    tracks_resolved = 0
    wrong_variant_count = 0
    rejection_mix = Counter()
    source_unavailable = 0
    why_missing_counts = Counter()
    why_missing_tracks = []
    per_track = []
    candidate_album_dirs = Counter()

    for row in rows:
        status = str(row["status"] or "").strip().lower()
        output_template = {}
        raw_template = row["output_template"]
        if isinstance(raw_template, str) and raw_template.strip():
            try:
                loaded = json.loads(raw_template)
                if isinstance(loaded, dict):
                    output_template = loaded
            except Exception:
                output_template = {}
        canonical = output_template.get("canonical_metadata") if isinstance(output_template.get("canonical_metadata"), dict) else {}
        track_id = str(canonical.get("recording_mbid") or output_template.get("recording_mbid") or row["id"]).strip()
        runtime_search_meta = output_template.get("runtime_search_meta") if isinstance(output_template.get("runtime_search_meta"), dict) else {}
        ep_refinement_attempted = bool(runtime_search_meta.get("ep_refinement_attempted"))
        ep_refinement_candidates_considered = int(runtime_search_meta.get("ep_refinement_candidates_considered") or 0)
        runtime_media_profile = output_template.get("runtime_media_profile") if isinstance(output_template.get("runtime_media_profile"), dict) else {}
        final_container = str(runtime_media_profile.get("final_container") or "").strip() or None
        final_video_codec = str(runtime_media_profile.get("final_video_codec") or "").strip() or None
        final_audio_codec = str(runtime_media_profile.get("final_audio_codec") or "").strip() or None
        decision_edge = _normalize_decision_edge(runtime_search_meta.get("decision_edge"))
        if bool(runtime_search_meta.get("wrong_variant_flag")):
            wrong_variant_count += 1

        album_dir = _album_output_dir_from_track_path(row["file_path"])
        if album_dir:
            candidate_album_dirs[album_dir] += 1

        resolved = status == JOB_STATUS_COMPLETED
        failure_reason = None
        if resolved:
            tracks_resolved += 1
        else:
            failure_reason = _normalize_runtime_failure_reason(row["last_error"], output_template)
            if status in {JOB_STATUS_FAILED, JOB_STATUS_CANCELLED, JOB_STATUS_SKIPPED_DUPLICATE}:
                rejection_mix[failure_reason] += 1
                if failure_reason.startswith("source_unavailable:"):
                    source_unavailable += 1
                hint_code, hint_label = _classify_runtime_missing_hint(failure_reason)
                why_missing_counts[hint_label] += 1
                why_missing_tracks.append(
                    {
                        "album_id": str(canonical.get("mb_release_group_id") or output_template.get("mb_release_group_id") or "").strip(),
                        "track_id": track_id,
                        "hint_code": hint_code,
                        "hint_label": hint_label,
                        "evidence": {"failure_reason": failure_reason},
                    }
                )

        per_track.append(
            {
                "track_id": track_id,
                "resolved": resolved,
                "failure_reason": failure_reason,
                "decision_edge": decision_edge,
                "ep_refinement_attempted": ep_refinement_attempted,
                "ep_refinement_candidates_considered": ep_refinement_candidates_considered,
                "final_container": final_container,
                "final_video_codec": final_video_codec,
                "final_audio_codec": final_audio_codec,
            }
        )

    unresolved_terminal = sum(1 for item in per_track if (not item.get("resolved")) and str(item.get("failure_reason") or "").strip())
    no_viable = max(0, unresolved_terminal - source_unavailable)
    completion_percent = (tracks_resolved / tracks_total * 100.0) if tracks_total else 0.0
    selected_album_dir = None
    if candidate_album_dirs:
        selected_album_dir = sorted(candidate_album_dirs.items(), key=lambda item: (-int(item[1]), item[0]))[0][0]

    summary = {
        "schema_version": 2,
        "run_type": "music_album",
        "album_run_id": str(album_run_id or ""),
        "telegram_sent": False,
        "telegram_message_id": None,
        "tracks_total": tracks_total,
        "tracks_resolved": tracks_resolved,
        "completion_percent": completion_percent,
        "wrong_variant_flags": wrong_variant_count,
        "rejection_mix": dict(sorted(rejection_mix.items(), key=lambda item: (-int(item[1]), item[0]))),
        "unresolved_classification": {
            "source_unavailable": source_unavailable,
            "no_viable_match": no_viable,
        },
        "why_missing": {
            "hint_counts": dict(sorted(why_missing_counts.items(), key=lambda item: (-int(item[1]), item[0]))),
            "tracks": sorted(why_missing_tracks, key=lambda item: (str(item.get("album_id") or ""), str(item.get("track_id") or ""))),
        },
        "per_track": sorted(per_track, key=lambda item: str(item.get("track_id") or "")),
    }
    return summary, selected_album_dir


def write_music_album_run_summary(db_path, album_run_id, *, output_dir=None):
    summary, inferred_album_dir = build_music_album_run_summary(db_path, album_run_id)
    target_dir = str(output_dir or inferred_album_dir or "").strip()
    if not target_dir:
        return None
    os.makedirs(target_dir, exist_ok=True)
    output_path = os.path.join(target_dir, "run_summary.json")
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
    return output_path


def record_download_history(db_path, job, filepath, *, meta=None):
    if not filepath:
        return
    video_id = None
    if meta:
        video_id = meta.get("video_id")
    if not video_id:
        video_id = extract_video_id(job.url) or job.id
    playlist_id = job.origin_id if job.origin == "playlist" else None
    input_url = job.input_url or job.url
    external_id = job.external_id
    source = job.source
    canonical_url = job.canonical_url
    if not canonical_url:
        canonical_url = canonicalize_url(source, input_url, external_id)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        ensure_downloads_table(conn)
        ensure_download_history_table(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO downloads (video_id, playlist_id, downloaded_at, filepath) VALUES (?, ?, ?, ?)",
            (video_id, playlist_id, utc_now(), filepath),
        )
        file_size_bytes = None
        try:
            file_size_bytes = int(os.stat(filepath).st_size)
        except OSError:
            file_size_bytes = None
        now = utc_now()
        title = meta.get("title") if isinstance(meta, dict) else None
        cur.execute(
            """
            INSERT INTO download_history (
                video_id, title, filename, destination, source, status,
                created_at, completed_at, file_size_bytes,
                input_url, canonical_url, external_id, channel_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                video_id,
                title,
                os.path.basename(filepath),
                os.path.dirname(filepath),
                source,
                "completed",
                now,
                now,
                file_size_bytes,
                input_url,
                canonical_url,
                external_id,
                (meta or {}).get("channel_id") if isinstance(meta, dict) else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def extract_video_id(url):
    if not url:
        return None
    if "youtube.com" in url:
        match = re.search(r"v=([a-zA-Z0-9_-]{6,})", url)
        if match:
            return match.group(1)
    if "youtu.be" in url:
        parsed = urllib.parse.urlparse(url)
        if parsed.path:
            return parsed.path.lstrip("/").split("/")[0]
    return None


def is_retryable_error(error):
    if isinstance(error, TypeError):
        return False
    if isinstance(error, PostprocessingError):
        return False
    if isinstance(error, CookieFallbackError):
        return False
    if isinstance(error, (DownloadError, ExtractorError)):
        message = str(error).lower()
    else:
        message = str(error).lower()
    if "postprocessing" in message or "postprocessor" in message or "ffmpeg" in message:
        return False
    if "yt_dlp_source_unavailable:" in message:
        return False
    if "json serializable" in message or "not json" in message:
        return False
    if "drm" in message:
        return False
    if "http error 403" in message or "http error 404" in message:
        return False
    if "403" in message and "http" in message:
        return False
    if "404" in message and "http" in message:
        return False
    if "not available" in message or "private" in message:
        return False
    if "unavailable" in message and "region" in message:
        return False
    if "extractor" in message or "unable to extract" in message:
        return True
    if "timed out" in message or "timeout" in message:
        return True
    if "temporary failure" in message or "connection reset" in message:
        return True
    return True
