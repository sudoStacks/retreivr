import json
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
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

import requests
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError, ExtractorError

from engine.json_utils import json_sanity_check, safe_json_dumps
from engine.paths import EnginePaths, TOKENS_DIR, resolve_dir
from engine.search_scoring import rank_candidates, score_candidate
from metadata.naming import sanitize_component
from metadata.queue import enqueue_metadata
from metadata.services.musicbrainz_service import get_musicbrainz_service

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
_FORMAT_AUDIO = "bestaudio[acodec!=none]/bestaudio/best"

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
            file_path TEXT
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
    ):
        if column not in existing_columns:
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
                SET status=?, downloading=?, updated_at=?
                WHERE id=?
                """,
                (JOB_STATUS_DOWNLOADING, now, now, job_id),
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
        combined = " ".join([str(artist or ""), str(track or ""), str(album or "")]).lower()
        return " live " in f" {combined} "

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
        search_terms = [f'"{artist}"', f'"{track}"']
        if album:
            search_terms.append(f'"{album}"')
        search_terms.extend(["audio", "official", "topic"])
        return " ".join(part for part in search_terms if part).strip()

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
        for source in source_priority:
            adapter = self.adapters.get(source)
            if not adapter:
                continue
            query = self._build_music_track_query(artist, track, album, is_live=allow_live)
            try:
                candidates = adapter.search_music_track(query, 6)
            except Exception:
                logging.exception("Music track search adapter failed source=%s", source)
                continue
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
        artist = str(payload.get("artist") or canonical.get("artist") or "").strip()
        track = str(payload.get("track") or canonical.get("track") or canonical.get("title") or "").strip()
        album = str(payload.get("album") or canonical.get("album") or "").strip() or None
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
        release_group_mbid = str(
            payload.get("mb_release_group_id")
            or payload.get("release_group_id")
            or canonical.get("mb_release_group_id")
            or canonical.get("release_group_id")
            or ""
        ).strip() or None
        duration_ms_raw = payload.get("duration_ms")
        if duration_ms_raw is None:
            duration_ms_raw = canonical.get("duration_ms")
        if duration_ms_raw is None:
            duration_ms_raw = canonical.get("duration")
        duration_hint_sec = None
        try:
            if duration_ms_raw is not None:
                duration_hint_sec = max(int(duration_ms_raw) // 1000, 1)
        except Exception:
            duration_hint_sec = None
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
        # Music-track acquisition must go through SearchService for deterministic orchestration/logging.
        if self.search_service:
            try:
                resolved = self.search_service.search_music_track_best_match(
                    artist,
                    track,
                    album=album,
                    duration_ms=(duration_hint_sec * 1000) if duration_hint_sec else None,
                    limit=6,
                )
            except Exception:
                logging.exception("Music track search service failed query=%s", search_query)
                self.store.record_failure(
                    job,
                    error_message="music_track_adapter_search_exception",
                    retryable=False,
                    retry_delay_seconds=self.retry_delay_seconds,
                )
                return None

        resolved_url, resolved_source = self._extract_resolved_candidate(resolved)
        if not _is_http_url(resolved_url):
            logging.error("Music track search failed")
            self.store.record_failure(
                job,
                error_message="no_candidate_above_threshold",
                retryable=False,
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
                job = self._resolve_music_track_job(job)
                if job is None:
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
            result = adapter.execute(
                job,
                self.config,
                self.paths,
                stop_event=None,
                cancel_check=lambda: self._is_job_cancelled(job.id, stop_event),
                cancel_reason="Cancelled by user",
                media_type=job.media_type,
                media_intent=job.media_intent,
            )
            if not result:
                raise RuntimeError("adapter_execute_failed")
            final_path, meta = result
            if self.store.get_job_status(job.id) == JOB_STATUS_FAILED:
                return
            if self._is_job_cancelled(job.id, stop_event):
                self.store.mark_canceled(job.id, reason="Cancelled by user")
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
            try:
                new_status = self.store.record_failure(
                    job,
                    error_message=error_message,
                    retryable=retryable,
                    retry_delay_seconds=self.retry_delay_seconds,
                )
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
            )


class YouTubeAdapter:
    _missing_final_format_warned = False

    def execute(self, job, config, paths, *, stop_event=None, cancel_check=None, cancel_reason=None, media_type=None, media_intent=None):
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
            )
            if not info or not local_file:
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            meta = extract_meta(info, fallback_url=job.url)
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
                _ensure_release_enriched(job)
                refreshed_template = job.output_template if isinstance(job.output_template, dict) else {}
                refreshed_canonical = (
                    refreshed_template.get("canonical_metadata")
                    if isinstance(refreshed_template.get("canonical_metadata"), dict)
                    else {}
                )
                for key in (
                    "album",
                    "release_date",
                    "track_number",
                    "disc_number",
                    "mb_release_id",
                    "mb_release_group_id",
                ):
                    value = refreshed_canonical.get(key)
                    if value is None and isinstance(refreshed_template, dict):
                        value = refreshed_template.get(key)
                    if value is not None:
                        meta[key] = value
            video_id = meta.get("video_id") or job.id
            ext = os.path.splitext(local_file)[1].lstrip(".")
            if audio_mode:
                ext = final_format or "mp3"
            elif not ext:
                ext = final_format or "mkv"
            template = audio_template if audio_mode else filename_template
            cleaned_name = build_output_filename(meta, video_id, ext, template, audio_mode)

            if stop_event and stop_event.is_set():
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            if not audio_mode:
                embed_metadata(local_file, meta, video_id, paths.thumbs_dir)

            final_path = os.path.join(resolved_dir, cleaned_name)
            final_path = resolve_collision_path(final_path)
            os.makedirs(os.path.dirname(final_path), exist_ok=True)
            atomic_move(local_file, final_path)
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

            # Only enqueue metadata if music_mode is True
            if music_mode:
                try:
                    enqueue_media_metadata(final_path, meta, config)
                except Exception:
                    # Never raise or affect download success
                    pass

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
    # recording?inc=releases+release-groups+media
    recording_payload = service.get_recording(
        recording_mbid,
        includes=["releases", "release-groups", "media"],
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
            includes=["recordings", "release-groups", "artist-credits", "media"],
        )
        release_data = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
        if not isinstance(release_data, dict):
            continue
        status = str(release_data.get("status") or release_item.get("status") or "").strip().lower()
        release_group = release_data.get("release-group")
        if not isinstance(release_group, dict):
            release_group = release_item.get("release-group") if isinstance(release_item.get("release-group"), dict) else {}
        primary_type = str(release_group.get("primary-type") or "").strip().lower()

        # valid release filter: Official Album only
        if status != "official":
            continue
        if primary_type != "album":
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
            break
        if track_number is not None and disc_number is not None:
            break

    if track_number is None or disc_number is None:
        raise RuntimeError("recording_not_found_in_release")

    year_only = _extract_release_year(selected_date_text)
    enriched = {
        "album": selected_release_data.get("title"),
        "release_date": year_only,
        "track_number": track_number,
        "disc_number": disc_number,
        "mb_release_id": selected_release,
        "mb_release_group_id": selected_release_group_id,
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
    cookie_value = (config or {}).get("yt_dlp_cookies")
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
        "format not available",
        "private",
        "removed",
    ]
    if not any(trigger in lower_msg for trigger in triggers):
        return False
    if any(blocker in lower_msg for blocker in blockers):
        return False
    return True

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
        output_template.setdefault("album", canonical_metadata.get("album"))
        output_template.setdefault("track", canonical_metadata.get("track") or canonical_metadata.get("title"))
        output_template.setdefault("track_number", canonical_metadata.get("track_number") or canonical_metadata.get("track_num"))
        output_template.setdefault("disc_number", canonical_metadata.get("disc_number") or canonical_metadata.get("disc_num"))
        output_template.setdefault("release_date", canonical_metadata.get("release_date") or canonical_metadata.get("date"))

    if isinstance(output_template_overrides, dict):
        output_template.update(output_template_overrides)

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
    config = context.get("config") or {}
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

    # Playlists: let yt-dlp expand playlists itself when the run/job indicates playlist intent.
    # This must override the previous "single item" behavior; forcing noplaylist=True breaks scheduler/watcher.
    allow_playlist = bool(context.get("allow_playlist"))
    if not allow_playlist:
        if context.get("media_intent") == "playlist":
            allow_playlist = True
        elif context.get("origin") == "playlist":
            allow_playlist = True
        elif operation in {"playlist", "playlist_probe", "playlist_metadata"}:
            allow_playlist = True
        elif _url_looks_like_playlist(context.get("url")):
            allow_playlist = True

    if isinstance(output_template, dict) and not allow_chapter_outtmpl:
        default_template = output_template.get("default")
        if isinstance(default_template, str) and default_template.strip():
            output_template = default_template
        else:
            output_template = "%(title).200s-%(id)s.%(ext)s"

    opts = {
        "quiet": True,
        "no_warnings": True,
        # CLI parity: allow playlist expansion when requested; otherwise behave like a single-URL download.
        "noplaylist": False if allow_playlist else True,
        "outtmpl": output_template,
        # Avoid chapter workflows unless explicitly enabled.
        # (Chapter outtmpl dicts can trigger unexpected behavior in the Python API path.)
        "no_chapters": True if not allow_chapter_outtmpl else False,
        "retries": 3,
        "fragment_retries": 3,
        "overwrites": True,
    }

    cookie_file = context.get("cookie_file")
    # Cookies OFF by default. Only enable when explicitly allowed or when running in music mode.
    # This prevents YouTube/SABR edge cases that produced empty/403 fragment downloads in the worker path.
    allow_cookies = bool(context.get("allow_cookies")) or (
        bool(context.get("audio_mode")) and is_music_media_type(context.get("media_type"))
    )
    if cookie_file and allow_cookies:
        opts["cookiefile"] = cookie_file

    if operation == "playlist":
        opts["skip_download"] = True
        opts["extract_flat"] = True
    elif operation == "metadata":
        opts["skip_download"] = True
    else:
        # Only enable addmetadata, embedthumbnail, writethumbnail, and audio postprocessors
        # when both audio_mode and media_type is music/audio
        if audio_mode:
            opts["postprocessors"] = _build_audio_postprocessors(normalized_audio_target)
            opts["format"] = _FORMAT_AUDIO
            opts["addmetadata"] = True
            opts["embedthumbnail"] = True
            opts["writethumbnail"] = True
        else:
            # Video mode: honor only container preferences (webm/mp4/mkv).
            # Never treat an audio codec (mp3/m4a/flac/...) as a yt-dlp format selector.
            if video_container_target:
                opts["format"] = "bestvideo+bestaudio/best"
                opts["merge_output_format"] = video_container_target
            else:
                opts["format"] = "best"

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


def _build_audio_postprocessors(target_format):
    preferred = _normalize_audio_format(target_format) or "mp3"
    if preferred not in _AUDIO_FORMATS:
        logging.warning("Unsupported audio format %s; defaulting to mp3", preferred)
        preferred = "mp3"
    return [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": preferred,
            "preferredquality": "0",
        }
    ]


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
    output_template = output_template if isinstance(output_template, dict) else {}
    config = config if isinstance(config, dict) else {}
    requested = (
        output_template.get("final_format")
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
        if lock_format and key in {"format", "merge_output_format"}:
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
        argv.extend(["--format", str(opts["format"])])
    if opts.get("merge_output_format"):
        argv.extend(["--merge-output-format", str(opts["merge_output_format"])])
    if opts.get("outtmpl"):
        argv.extend(["--output", str(opts["outtmpl"])])

    # Playlist behavior
    if opts.get("noplaylist") is True:
        argv.append("--no-playlist")
    elif opts.get("noplaylist") is False:
        argv.append("--yes-playlist")

    # Retries
    if opts.get("retries") is not None:
        argv.extend(["--retries", str(opts.get("retries"))])
    if opts.get("fragment_retries") is not None:
        argv.extend(["--fragment-retries", str(opts.get("fragment_retries"))])

    # Cookies (python opts uses cookiefile, CLI uses --cookies)
    if opts.get("cookiefile"):
        argv.extend(["--cookies", str(opts.get("cookiefile"))])

    # Audio extraction (CLI parity for FFmpegExtractAudio)
    if opts.get("postprocessors"):
        for pp in opts.get("postprocessors") or []:
            if pp.get("key") == "FFmpegExtractAudio":
                argv.append("--extract-audio")
                if pp.get("preferredcodec"):
                    argv.extend(["--audio-format", str(pp.get("preferredcodec"))])

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
):
    if (stop_event and stop_event.is_set()) or (callable(cancel_check) and cancel_check()):
        raise CancelledError(cancel_reason or "Cancelled by user")
    # Use an ID-based template in temp_dir (CLI parity and avoids title/path edge cases).
    # The final user-facing name is applied later when we move/rename the completed file.
    output_template = os.path.join(temp_dir, "%(id)s.%(ext)s")
    context = {
        "operation": "download",
        "url": url,
        "audio_mode": audio_mode,
        "final_format": final_format,
        "output_template": output_template,
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

    from subprocess import DEVNULL, CalledProcessError
    info = None
    # Always get metadata via API (for output file info, etc.)
    try:
        opts_for_probe = dict(opts)
        opts_for_probe["skip_download"] = True
        with YoutubeDL(opts_for_probe) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as exc:
        _log_event(
            logging.ERROR,
            "ytdlp_metadata_probe_failed",
            job_id=job_id,
            url=url,
            error=str(exc),
        )
        raise RuntimeError(f"yt_dlp_metadata_probe_failed: {exc}")

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
    # Build CLI argv and run without a shell (prevents globbing of format strings like [acodec!=none])
    cmd_argv = _render_ytdlp_cli_argv(opts_for_run, url)
    cmd_log = _argv_to_redacted_cli(cmd_argv)

    try:
        subprocess.run(
            cmd_argv,
            check=True,
            stdout=DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
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
                subprocess.run(
                    cmd_retry_argv,
                    check=True,
                    stdout=DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
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
        if opts.get("cookiefile"):
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
                    subprocess.run(cmd_retry_argv, check=True, stdout=DEVNULL, stderr=DEVNULL)
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
                    )
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
                )
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
            )
            raise RuntimeError(f"yt_dlp_download_failed: {exc}")

    if (stop_event and stop_event.is_set()) or (callable(cancel_check) and cancel_check()):
        raise CancelledError(cancel_reason or "Cancelled by user")
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
        if entry.endswith((".part", ".ytdl", ".temp")):
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
    cookie_file = resolve_cookie_file(config or {})
    context = {
        "operation": "metadata",
        "url": url,
        "audio_mode": False,
        "final_format": None,
        "output_template": None,
        "cookie_file": cookie_file,
        "overrides": (config or {}).get("yt_dlp_opts") or {},
        "media_type": "video",
        "media_intent": "episode",
    }
    opts = build_ytdlp_opts(context)
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
    meta = extract_meta(info, fallback_url=url)
    return {
        "title": meta.get("title"),
        "uploader": meta.get("channel") or meta.get("artist"),
        "thumbnail_url": meta.get("thumbnail_url"),
        "url": meta.get("url") or url,
        "source": resolve_source(url),
        "duration_sec": info.get("duration") if isinstance(info, dict) else None,
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
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value = str(value).strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    match = re.match(r"(\d+)", value)
    if match:
        return int(match.group(1))
    return None


def format_track_number(value):
    normalized = normalize_track_number(value)
    if normalized is None:
        return ""
    return f"{normalized:02d}"


def _normalize_nfc(value):
    return unicodedata.normalize("NFC", str(value or ""))


def _extract_release_year(value):
    text = str(value or "").strip()
    if len(text) >= 4 and text[:4].isdigit():
        return text[:4]
    return ""


def build_audio_filename(meta, ext, *, template=None, fallback_id=None):
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
        raise RuntimeError("music_release_metadata_incomplete_before_path_build")

    album_artist = sanitize_for_filesystem(
        _normalize_nfc(_clean_audio_artist(meta.get("album_artist") or meta.get("artist") or ""))
    ) or "Unknown Artist"
    album_title = sanitize_for_filesystem(_normalize_nfc(_clean_audio_title(meta.get("album") or ""))) or "Unknown Album"
    track = sanitize_for_filesystem(_normalize_nfc(_clean_audio_title(meta.get("track") or meta.get("title") or "")))
    track_number = format_track_number(meta.get("track_number")) or "00"
    disc_number = normalize_track_number(meta.get("disc") or meta.get("disc_number"))
    disc_folder = sanitize_for_filesystem(_normalize_nfc(f"Disc {disc_number or 1}"))
    release_year = _extract_release_year(meta.get("release_date") or meta.get("date"))
    album_folder = f"{album_title} ({release_year})" if release_year else album_title
    # Audio paths are canonical and intentionally ignore custom templates.
    # This avoids structural drift and duplicate Disc folder segments.
    _ = template
    _ = fallback_id

    track_label = f"{track_number} - {track or 'media'}.{ext}"
    return f"Music/{album_artist}/{album_folder}/{disc_folder}/{track_label}"


def build_output_filename(meta, fallback_id, ext, template, audio_mode):
    if audio_mode:
        return build_audio_filename(meta, ext, template=template, fallback_id=fallback_id)
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
                return rendered
        except Exception:
            pass
    return f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}.{ext}"


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
