#!/usr/bin/env python3
import sys


def _require_python_311():
    if sys.version_info[:2] != (3, 11):
        found = sys.version.split()[0]
        raise SystemExit(
            f"ERROR: Retreivr requires Python 3.11.x; found Python {found} "
            f"(executable: {sys.executable})"
        )


_require_python_311()

import asyncio
import functools
import base64
import binascii
import hmac
import json
import logging
import mimetypes
import os
import re
import sqlite3
import subprocess
import shutil
import tempfile
import threading
import time
import requests
import musicbrainzngs
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from uuid import uuid4
from urllib.parse import quote, urlparse
from typing import Optional

import anyio
from fastapi import Body, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
from pydantic import BaseModel
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from google.auth.exceptions import RefreshError

from engine.job_queue import (
    DownloadJobStore,
    DownloadWorkerEngine,
    atomic_move,
    build_download_job_payload,
    build_output_filename,
    ensure_mb_bound_music_track,
    enqueue_media_metadata,
    finalize_download_artifact,
    preview_direct_url,
    canonicalize_url,
    download_with_ytdlp,
    embed_metadata,
    extract_meta,
    resolve_source,
    resolve_media_intent,
    resolve_media_type,
    resolve_collision_path,
    extract_video_id,
    is_music_media_type,
    _normalize_audio_format,
    _normalize_format,
)
from engine.json_utils import json_sanity_check, safe_json, safe_json_dump
from engine.search_engine import SearchJobStore, SearchResolutionService, resolve_search_db_path
from engine.musicbrainz_binding import resolve_best_mb_pair, search_music_metadata
from engine.canonical_ids import build_music_track_canonical_id, extract_external_track_canonical_id
from engine.spotify_playlist_importer import (
    SpotifyPlaylistImportError,
    SpotifyPlaylistImporter,
)
from metadata.services.musicbrainz_service import get_musicbrainz_service

from engine.core import (
    EngineStatus,
    build_youtube_clients,
    extract_playlist_id,
    get_playlist_videos,
    get_status,
    init_db,
    is_video_downloaded,
    is_video_seen,
    _acquire_client_delivery,
    _finalize_client_delivery,
    _mark_client_delivery,
    _register_client_delivery,
    load_config,
    mark_video_seen,
    playlist_has_seen,
    read_history,
    run_direct_url_self_test,
    run_archive,
    run_single_playlist,
    telegram_notify,
    validate_config,
)
from engine.paths import (
    CONFIG_DIR,
    DATA_DIR,
    DOWNLOADS_DIR,
    LOG_DIR,
    TOKENS_DIR,
    build_engine_paths,
    ensure_dir,
    resolve_config_path,
    resolve_dir,
)
from engine.runtime import get_runtime_info
from input.intent_router import IntentType, detect_intent
from api.intent_dispatcher import execute_intent as dispatch_intent
from spotify.oauth_client import SPOTIFY_TOKEN_URL, build_auth_url
from spotify.client import SpotifyPlaylistClient
from spotify.oauth_store import SpotifyOAuthStore, SpotifyOAuthToken
from db.playlist_snapshots import PlaylistSnapshotStore
from scheduler.jobs.spotify_playlist_watch import (
    normalize_spotify_playlist_identifier,
    playlist_watch_job,
    spotify_liked_songs_watch_job,
    spotify_playlists_watch_job,
    spotify_saved_albums_watch_job,
    spotify_user_playlists_watch_job,
)
from metadata.importers.dispatcher import import_playlist as import_playlist_file_bytes
from engine.import_pipeline import process_imported_tracks
from engine.import_m3u_builder import write_import_m3u_from_batch

APP_NAME = "Retreivr API"
STATUS_SCHEMA_VERSION = 2
METRICS_SCHEMA_VERSION = 1
SCHEDULE_SCHEMA_VERSION = 1
_BASIC_AUTH_USER = os.environ.get("YT_ARCHIVER_BASIC_AUTH_USER")
_BASIC_AUTH_PASS = os.environ.get("YT_ARCHIVER_BASIC_AUTH_PASS")
_BASIC_AUTH_ENABLED = bool(_BASIC_AUTH_USER and _BASIC_AUTH_PASS)
_TRUST_PROXY = os.environ.get("YT_ARCHIVER_TRUST_PROXY", "").strip().lower() in {"1", "true", "yes", "on"}
SCHEDULE_JOB_ID = "archive_schedule"
WATCHER_JOB_ID = "playlist_watcher"
LIKED_SONGS_JOB_ID = "spotify_liked_songs_watch"
SAVED_ALBUMS_JOB_ID = "spotify_saved_albums_watch"
USER_PLAYLISTS_JOB_ID = "spotify_user_playlists_watch"
SPOTIFY_PLAYLISTS_WATCH_JOB_ID = "spotify_playlists_watch"
DEFERRED_RUN_JOB_ID = "deferred_run"
WATCHER_QUIET_WINDOW_SECONDS = 60
COVER_ART_CACHE_TTL_SECONDS = 3600
DEFAULT_LIKED_SONGS_SYNC_INTERVAL_MINUTES = 15
DEFAULT_SAVED_ALBUMS_SYNC_INTERVAL_MINUTES = 30
DEFAULT_USER_PLAYLISTS_SYNC_INTERVAL_MINUTES = 30
DEFAULT_SPOTIFY_PLAYLISTS_SYNC_INTERVAL_MINUTES = 15
logger = logging.getLogger(__name__)
OAUTH_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
OAUTH_SESSION_TTL = timedelta(minutes=15)
_OAUTH_SESSIONS = {}
_OAUTH_LOCK = threading.Lock()
_DEPRECATED_FIELDS = {"poll_interval_hours"}
_DEPRECATED_LOGGED = set()
_MULTI_WORKER_ENV_KEYS = ("UVICORN_WORKERS", "WEB_CONCURRENCY", "GUNICORN_WORKERS")
DIRECT_URL_PLAYLIST_ERROR = (
    "Playlist URLs are not supported in Direct URL mode. "
    "Please add this playlist via Scheduler or Playlist settings."
)

WEBUI_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "webUI"))
MAX_IMPORT_FILE_BYTES = 5 * 1024 * 1024
SUPPORTED_IMPORT_EXTENSIONS = {".m3u", ".m3u8", ".csv", ".xml", ".plist", ".json"}
IMPORT_JOB_TTL_SECONDS = 6 * 60 * 60


def _mb_service():
    return get_musicbrainz_service()


def get_loaded_config() -> dict:
    cfg = getattr(app.state, "loaded_config", None) or getattr(app.state, "config", None)
    return cfg if isinstance(cfg, dict) else {}


def _playlist_imports_active_count() -> int:
    try:
        value = int(getattr(app.state, "playlist_import_active_count", 0) or 0)
    except Exception:
        value = 0
    return max(0, value)


def _playlist_imports_active() -> bool:
    return _playlist_imports_active_count() > 0


def _trim_playlist_import_jobs_locked() -> None:
    jobs = getattr(app.state, "playlist_import_jobs", None)
    if not isinstance(jobs, dict):
        return
    now = time.time()
    stale_ids: list[str] = []
    for job_id, entry in jobs.items():
        if not isinstance(entry, dict):
            stale_ids.append(str(job_id))
            continue
        state = str(entry.get("state") or "").strip().lower()
        if state not in {"completed", "failed", "cancelled"}:
            continue
        finished_at = str(entry.get("finished_at") or "").strip()
        finished_ts = None
        if finished_at:
            parsed = _parse_iso(finished_at)
            if parsed is not None:
                finished_ts = parsed.timestamp()
        if finished_ts is None:
            finished_ts = float(entry.get("updated_ts") or 0.0)
        if finished_ts and (now - finished_ts) > IMPORT_JOB_TTL_SECONDS:
            stale_ids.append(str(job_id))
    for job_id in stale_ids:
        jobs.pop(job_id, None)


def _update_playlist_import_job(job_id: str, **fields) -> dict | None:
    lock = getattr(app.state, "playlist_import_jobs_lock", None)
    jobs = getattr(app.state, "playlist_import_jobs", None)
    if lock is None or not isinstance(jobs, dict):
        return None
    with lock:
        entry = jobs.get(job_id)
        if not isinstance(entry, dict):
            return None
        for key, value in fields.items():
            entry[key] = value
        entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        entry["updated_ts"] = time.time()
        jobs[job_id] = entry
        _trim_playlist_import_jobs_locked()
        return dict(entry)


def _get_playlist_import_job(job_id: str) -> dict | None:
    lock = getattr(app.state, "playlist_import_jobs_lock", None)
    jobs = getattr(app.state, "playlist_import_jobs", None)
    if lock is None or not isinstance(jobs, dict):
        return None
    with lock:
        _trim_playlist_import_jobs_locked()
        entry = jobs.get(job_id)
        return dict(entry) if isinstance(entry, dict) else None


def _get_playlist_import_snapshot() -> dict:
    snapshot: dict = {
        "active": _playlist_imports_active(),
        "active_count": _playlist_imports_active_count(),
        "current_job": None,
    }
    lock = getattr(app.state, "playlist_import_jobs_lock", None)
    jobs = getattr(app.state, "playlist_import_jobs", None)
    if lock is None or not isinstance(jobs, dict):
        return snapshot
    with lock:
        _trim_playlist_import_jobs_locked()
        rows = [dict(entry) for entry in jobs.values() if isinstance(entry, dict)]
    if not rows:
        return snapshot
    active_rows = [
        row
        for row in rows
        if str(row.get("state") or "").strip().lower() in {"queued", "parsing", "resolving"}
    ]
    ranked = active_rows if active_rows else rows
    ranked.sort(key=lambda row: float(row.get("updated_ts") or 0.0), reverse=True)
    current = ranked[0]
    snapshot["current_job"] = {
        "job_id": current.get("job_id"),
        "state": current.get("state"),
        "phase": current.get("phase"),
        "message": current.get("message"),
        "total_tracks": current.get("total_tracks"),
        "processed_tracks": current.get("processed_tracks"),
        "resolved": current.get("resolved"),
        "unresolved": current.get("unresolved"),
        "enqueued": current.get("enqueued"),
        "failed": current.get("failed"),
        "error": current.get("error"),
        "started_at": current.get("started_at"),
        "finished_at": current.get("finished_at"),
        "updated_at": current.get("updated_at"),
    }
    return snapshot


def _get_download_queue_snapshot(limit_active_jobs: int = 5) -> dict:
    summary: dict = {
        "counts": {
            "queued": 0,
            "claimed": 0,
            "downloading": 0,
            "postprocessing": 0,
            "failed": 0,
            "cancelled": 0,
            "completed": 0,
        },
        "active_count": 0,
        "active_jobs": [],
    }
    db_path = getattr(getattr(app.state, "paths", None), "db_path", None)
    if not db_path:
        return summary

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        statuses = ("queued", "claimed", "downloading", "postprocessing", "failed", "cancelled", "completed")
        placeholders = ",".join("?" for _ in statuses)
        cur.execute(
            f"SELECT status, COUNT(*) AS count FROM download_jobs WHERE status IN ({placeholders}) GROUP BY status",
            statuses,
        )
        for row in cur.fetchall():
            key = str(row["status"] or "").strip().lower()
            if key in summary["counts"]:
                summary["counts"][key] = int(row["count"] or 0)

        summary["active_count"] = int(
            summary["counts"]["queued"]
            + summary["counts"]["claimed"]
            + summary["counts"]["downloading"]
            + summary["counts"]["postprocessing"]
        )

        active_statuses = ("downloading", "postprocessing", "claimed", "queued")
        active_placeholders = ",".join("?" for _ in active_statuses)
        cur.execute(
            f"""
            SELECT id, status, source, origin, media_intent, attempts, max_attempts, created_at, updated_at, last_error,
                   progress_downloaded_bytes, progress_total_bytes, progress_percent, progress_speed_bps,
                   progress_eta_seconds, progress_updated_at
            FROM download_jobs
            WHERE status IN ({active_placeholders})
            ORDER BY
                CASE status
                    WHEN 'downloading' THEN 0
                    WHEN 'postprocessing' THEN 1
                    WHEN 'claimed' THEN 2
                    ELSE 3
                END,
                COALESCE(updated_at, created_at) DESC
            LIMIT ?
            """,
            (*active_statuses, int(max(1, limit_active_jobs))),
        )
        summary["active_jobs"] = [
            {
                "id": row["id"],
                "status": row["status"],
                "source": row["source"],
                "origin": row["origin"],
                "media_intent": row["media_intent"],
                "attempts": row["attempts"],
                "max_attempts": row["max_attempts"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "last_error": row["last_error"],
                "progress_downloaded_bytes": row["progress_downloaded_bytes"],
                "progress_total_bytes": row["progress_total_bytes"],
                "progress_percent": row["progress_percent"],
                "progress_speed_bps": row["progress_speed_bps"],
                "progress_eta_seconds": row["progress_eta_seconds"],
                "progress_updated_at": row["progress_updated_at"],
            }
            for row in cur.fetchall()
        ]
    except sqlite3.OperationalError as exc:
        msg = str(exc).lower()
        if "no such table" not in msg and "no such column" not in msg:
            logging.exception("Failed to build download queue snapshot")
    finally:
        conn.close()
    return summary


def _run_playlist_import_job(job_id: str, filename: str, payload: bytes) -> None:
    try:
        _update_playlist_import_job(
            job_id,
            state="parsing",
            message="Parsing playlist file...",
        )
        track_intents = import_playlist_file_bytes(payload, filename)
        total_tracks = len(track_intents)
        if total_tracks <= 0:
            _update_playlist_import_job(
                job_id,
                state="failed",
                message="Import failed",
                error="empty_import",
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            return

        _update_playlist_import_job(
            job_id,
            state="resolving",
            message="Resolving tracks and enqueueing jobs...",
            total_tracks=int(total_tracks),
            processed_tracks=0,
        )

        engine = getattr(app.state, "worker_engine", None)
        queue_store = getattr(engine, "store", None) if engine is not None else None
        if queue_store is None:
            _update_playlist_import_job(
                job_id,
                state="failed",
                message="Import failed",
                error="queue_store_unavailable",
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            return

        runtime_config = _read_config_or_404()

        def _progress(snapshot: dict) -> None:
            _update_playlist_import_job(
                job_id,
                phase=snapshot.get("phase") or "resolving",
                total_tracks=int(snapshot.get("total_tracks") or total_tracks),
                processed_tracks=int(snapshot.get("processed_tracks") or 0),
                resolved=int(snapshot.get("resolved_count") or 0),
                unresolved=int(snapshot.get("unresolved_count") or 0),
                enqueued=int(snapshot.get("enqueued_count") or 0),
                failed=int(snapshot.get("failed_count") or 0),
                message="Resolving tracks and enqueueing jobs...",
            )

        result = process_imported_tracks(
            track_intents,
            {
                "queue_store": queue_store,
                "app_config": runtime_config,
                "base_dir": app.state.paths.single_downloads_dir,
                "destination_dir": None,
                "final_format": runtime_config.get("final_format"),
                "progress_callback": _progress,
            },
        )

        import_batch_id = str(getattr(result, "import_batch_id", "") or "").strip()
        _update_playlist_import_job(
            job_id,
            state="completed",
            message="Playlist import completed.",
            phase="completed",
            total_tracks=int(getattr(result, "total_tracks", total_tracks) or total_tracks),
            processed_tracks=int(getattr(result, "total_tracks", total_tracks) or total_tracks),
            resolved=int(getattr(result, "resolved_count", 0) or 0),
            unresolved=int(getattr(result, "unresolved_count", 0) or 0),
            enqueued=int(getattr(result, "enqueued_count", 0) or 0),
            failed=int(getattr(result, "failed_count", 0) or 0),
            import_batch_id=import_batch_id,
            error=None,
            finished_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as exc:
        _update_playlist_import_job(
            job_id,
            state="failed",
            message="Import failed",
            error=str(exc),
            finished_at=datetime.now(timezone.utc).isoformat(),
        )
    finally:
        lock = getattr(app.state, "playlist_import_jobs_lock", None)
        if lock is not None:
            with lock:
                active_count = _playlist_imports_active_count()
                app.state.playlist_import_active_count = max(0, active_count - 1)
                _trim_playlist_import_jobs_locked()


def _search_music_album_candidates(query: str, *, limit: int) -> list[dict]:
    normalized_query = str(query or "").strip()
    if not normalized_query:
        return []
    return _mb_service().search_release_groups(normalized_query, limit=limit)


def _search_music_recording_candidates(query: str, *, limit: int, config: dict | None = None) -> list[dict]:
    normalized_query = str(query or "").strip()
    if not normalized_query:
        return []

    runtime_cfg = config if isinstance(config, dict) else {}
    threshold_raw = runtime_cfg.get("music_mb_binding_threshold", 0.78)
    try:
        threshold = float(threshold_raw)
    except (TypeError, ValueError):
        threshold = 0.78
    if threshold > 1.0:
        threshold = threshold / 100.0
    threshold = max(0.0, min(1.0, threshold))

    prefer_country = str(
        runtime_cfg.get("locale_country")
        or runtime_cfg.get("country")
        or "US"
    ).strip().upper() or "US"
    debug_scoring = bool(runtime_cfg.get("debug_music_scoring"))
    allow_non_album_fallback = bool(runtime_cfg.get("allow_non_album_fallback", True))

    artist_hint = None
    track_hint = normalized_query
    if " - " in normalized_query:
        left, right = normalized_query.split(" - ", 1)
        if left.strip() and right.strip():
            artist_hint = left.strip()
            track_hint = right.strip()

    search_limit = max(int(limit) * 5, 20)
    recordings_payload = _mb_service().search_recordings(
        artist_hint,
        track_hint,
        album=None,
        limit=search_limit,
    )
    recording_list = []
    if isinstance(recordings_payload, dict):
        raw = recordings_payload.get("recording-list")
        if isinstance(raw, list):
            recording_list = [entry for entry in raw if isinstance(entry, dict)]

    def _score_value(recording):
        try:
            raw_score = recording.get("score")
            if raw_score is None:
                raw_score = recording.get("ext:score")
            score = float(raw_score)
            if score > 1.0:
                score = score / 100.0
            return max(0.0, min(1.0, score))
        except Exception:
            return 0.0

    def _artist_credit_text(artist_credit):
        if not isinstance(artist_credit, list):
            return ""
        parts = []
        for part in artist_credit:
            if isinstance(part, str):
                parts.append(part)
                continue
            if isinstance(part, dict):
                artist_obj = part.get("artist") if isinstance(part.get("artist"), dict) else {}
                name = str(part.get("name") or artist_obj.get("name") or "").strip()
                joinphrase = str(part.get("joinphrase") or "").strip()
                if name:
                    parts.append(name)
                if joinphrase:
                    parts.append(joinphrase)
        return "".join(parts).strip()

    ranked_recordings = sorted(
        recording_list,
        key=lambda rec: (-_score_value(rec), str(rec.get("id") or "")),
    )
    bound_results: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()

    for recording in ranked_recordings:
        if len(bound_results) >= int(limit):
            break
        recording_mbid = str(recording.get("id") or "").strip()
        if not recording_mbid:
            continue
        title = str(recording.get("title") or "").strip()
        artist = _artist_credit_text(recording.get("artist-credit")) or (artist_hint or "")
        try:
            duration_ms = int(str(recording.get("length") or "").strip()) if recording.get("length") else None
        except (TypeError, ValueError):
            duration_ms = None

        pair = resolve_best_mb_pair(
            _mb_service(),
            artist=artist or artist_hint,
            track=title or track_hint,
            album=None,
            duration_ms=duration_ms,
            country_preference=prefer_country,
            allow_non_album_fallback=allow_non_album_fallback,
            debug=debug_scoring,
            threshold=threshold,
        )
        if not isinstance(pair, dict):
            continue
        release_mbid = str(pair.get("mb_release_id") or "").strip()
        if not release_mbid:
            continue
        dedupe_key = (recording_mbid, release_mbid)
        if dedupe_key in seen_pairs:
            continue
        seen_pairs.add(dedupe_key)
        release_date = str(pair.get("release_date") or "").strip()
        release_year = release_date[:4] if len(release_date) >= 4 and release_date[:4].isdigit() else None
        track_number = pair.get("track_number")
        disc_number = pair.get("disc_number")
        pair_duration_ms = pair.get("duration_ms")
        try:
            track_number_int = int(track_number) if track_number is not None else None
            disc_number_int = int(disc_number) if disc_number is not None else None
            duration_ms_int = int(pair_duration_ms) if pair_duration_ms is not None else None
        except (TypeError, ValueError):
            continue
        if not release_date or not release_year:
            continue
        if not track_number_int or not disc_number_int or not duration_ms_int:
            continue
        bound_results.append(
            {
                "recording_mbid": recording_mbid,
                "artist": artist or None,
                "track": title or None,
                "release_mbid": release_mbid,
                "release_group_mbid": str(pair.get("mb_release_group_id") or "").strip() or None,
                "album": str(pair.get("album") or "").strip() or None,
                "release_year": release_year,
                "release_date": release_date or None,
                "track_number": track_number_int,
                "disc_number": disc_number_int,
                "duration_ms": duration_ms_int,
                "artwork_url": None,
            }
        )

    return bound_results

def _is_http_url(value: str | None) -> bool:
    if not value or not isinstance(value, str):
        return False
    try:
        return urlparse(value).scheme in ("http", "https")
    except Exception:
        return False


def _sanitize_non_http_urls(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "url" and isinstance(v, str) and v and not _is_http_url(v):
                out[k] = None
            else:
                out[k] = _sanitize_non_http_urls(v)
        return out
    if isinstance(obj, list):
        return [_sanitize_non_http_urls(v) for v in obj]
    return obj


def _ensure_music_failures_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS music_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            origin_batch_id TEXT,
            artist TEXT,
            track TEXT,
            reason_json TEXT,
            recording_mbid_attempted TEXT,
            last_query TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_music_failures_created_at ON music_failures (created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_music_failures_batch ON music_failures (origin_batch_id)")
    conn.commit()

def notify_run_summary(config, *, run_type: str, status, started_at, finished_at):
    if run_type not in {"scheduled", "watcher", "api"}:
        return

    TELEGRAM_MAX_MESSAGE_CHARS = 4096

    def _count(value) -> int:
        if value is None:
            return 0
        if isinstance(value, (list, tuple, set)):
            return len(value)
        try:
            return int(value)
        except Exception:
            return 0

    def _resolve_downloaded_labels(success_values) -> list[str]:
        labels: list[str] = []
        if not success_values:
            return labels

        raw_values: list[str] = []
        pending_job_ids: list[str] = []
        for value in success_values:
            text = str(value or "").strip()
            if not text:
                continue
            raw_values.append(text)
            if re.fullmatch(r"[0-9a-f]{32}", text):
                pending_job_ids.append(text)

        job_id_to_path: dict[str, str] = {}
        if pending_job_ids:
            db_path = getattr(getattr(app.state, "paths", None), "db_path", None)
            if db_path:
                try:
                    conn = sqlite3.connect(db_path)
                    cur = conn.cursor()
                    placeholders = ",".join("?" for _ in pending_job_ids)
                    cur.execute(
                        f"SELECT id, file_path FROM download_jobs WHERE id IN ({placeholders})",
                        pending_job_ids,
                    )
                    for job_id, file_path in cur.fetchall():
                        if job_id and file_path:
                            job_id_to_path[str(job_id)] = str(file_path)
                except Exception:
                    logging.exception("Failed to resolve run success labels from download_jobs")
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

        seen: set[str] = set()
        for raw in raw_values:
            candidate = job_id_to_path.get(raw, raw)
            if os.path.sep in candidate:
                candidate = os.path.basename(candidate)
            candidate = str(candidate or "").strip()
            if not candidate:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            labels.append(candidate)

        return labels

    def _build_message(success_count: int, failure_count: int, duration_text: str, downloaded_labels: list[str]) -> str:
        status_text = "completed" if failure_count <= 0 else "completed_with_errors"
        header = (
            "YouTube Archiver Summary\n"
            f"Status: {status_text}\n"
            f"\u2714 Success: {success_count}\n"
            f"\u2716 Failed: {failure_count}\n"
            f"Duration: {duration_text}"
        )
        footer = "\n\n__________"
        lines = ["", "Downloaded:"]
        if downloaded_labels:
            lines.extend([f"\u2022 {label}" for label in downloaded_labels])
        else:
            lines.append("\u2022 (none)")
        body = "\n".join(lines)
        message = f"{header}\n{body}{footer}"
        if len(message) <= TELEGRAM_MAX_MESSAGE_CHARS:
            return message

        # Trim downloaded list first, preserving header and a clear overflow marker.
        trimmed_lines = ["", "Downloaded:"]
        remaining = list(downloaded_labels)
        while remaining:
            next_line = f"\u2022 {remaining[0]}"
            tentative = f"{header}\n" + "\n".join(trimmed_lines + [next_line]) + footer
            if len(tentative) > TELEGRAM_MAX_MESSAGE_CHARS:
                break
            trimmed_lines.append(next_line)
            remaining.pop(0)
        if remaining:
            overflow_line = f"\u2022 ... (+{len(remaining)} more)"
            tentative = f"{header}\n" + "\n".join(trimmed_lines + [overflow_line]) + footer
            if len(tentative) <= TELEGRAM_MAX_MESSAGE_CHARS:
                trimmed_lines.append(overflow_line)
        capped = f"{header}\n" + "\n".join(trimmed_lines) + footer
        if len(capped) > TELEGRAM_MAX_MESSAGE_CHARS:
            return f"{capped[:TELEGRAM_MAX_MESSAGE_CHARS - 1]}\u2026"
        return capped

    successes = _count(getattr(status, "run_successes", 0))
    failures = _count(getattr(status, "run_failures", 0))
    attempted = successes + failures

    if attempted <= 0:
        return

    duration_label = "unknown"
    if started_at and finished_at:
        start_dt = _parse_iso(started_at)
        finish_dt = _parse_iso(finished_at)
        if start_dt is not None and finish_dt is not None:
            duration_sec = int((finish_dt - start_dt).total_seconds())
            m, s = divmod(max(0, duration_sec), 60)
            duration_label = f"{m}m {s}s" if m else f"{s}s"

    downloaded_labels = _resolve_downloaded_labels(getattr(status, "run_successes", []) or [])
    msg = _build_message(successes, failures, duration_label, downloaded_labels)

    try:
        telegram_notify(config, msg)
    except Exception:
        logging.exception("Telegram notify failed (run_type=%s)", run_type)


def normalize_search_payload(payload: dict | None, *, default_sources: list[str]) -> dict:
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")

    def _clean_str(value, field):
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            return trimmed if trimmed else None
        raise ValueError(f"{field} must be a string")

    def _coerce_bool(value, field):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if value in (0, 1):
                return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y"}:
                return True
            if lowered in {"false", "0", "no", "n"}:
                return False
        raise ValueError(f"{field} must be a boolean")

    def _parse_sources(raw):
        if raw is None:
            return []
        if isinstance(raw, list):
            items = raw
        elif isinstance(raw, str):
            text = raw.strip()
            if not text:
                return []
            if text.startswith("["):
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise ValueError("sources must be a list or comma-separated string") from exc
                if not isinstance(parsed, list):
                    raise ValueError("sources must be a list")
                items = parsed
            else:
                items = [part.strip() for part in text.split(",")]
        else:
            raise ValueError("sources must be a list or comma-separated string")
        return [str(item).strip() for item in items if str(item).strip()]

    raw_query = payload.get("query")
    if raw_query is None:
        artist = _clean_str(payload.get("artist"), "artist")
        track = _clean_str(payload.get("track"), "track")
        album = _clean_str(payload.get("album"), "album")
        parts = [part for part in [artist, track or album] if part]
        query = " ".join(parts).strip()
    elif isinstance(raw_query, str):
        query = raw_query.strip()
    else:
        raise ValueError("query must be a string")

    raw_sources = payload.get("sources")
    if raw_sources is None:
        raw_sources = payload.get("source_priority", payload.get("source_priority_json"))
    sources = _parse_sources(raw_sources)
    allowed = list(default_sources or [])
    allowed_set = set(allowed)
    filtered = [source for source in sources if source in allowed_set]
    sources = filtered or allowed

    raw_search_only = payload.get("search_only")
    search_only = _coerce_bool(raw_search_only, "search_only")
    if search_only is None:
        auto_enqueue = _coerce_bool(payload.get("auto_enqueue"), "auto_enqueue")
        search_only = not auto_enqueue if auto_enqueue is not None else True

    music_mode = _coerce_bool(payload.get("music_mode"), "music_mode")
    if music_mode is None:
        media_type = _clean_str(payload.get("media_type"), "media_type")
        lossless_only = _coerce_bool(payload.get("lossless_only"), "lossless_only")
        music_mode = bool(lossless_only) or (media_type in {"music", "audio"})

    final_format = _clean_str(payload.get("final_format"), "final_format")
    if final_format is None:
        final_format = _clean_str(payload.get("final_format_override"), "final_format")

    destination = _clean_str(payload.get("destination"), "destination")
    if destination is None:
        destination = _clean_str(payload.get("destination_dir"), "destination")
    if destination is None:
        destination = _clean_str(payload.get("destination_path"), "destination_path")

    delivery_mode = _clean_str(payload.get("delivery_mode"), "delivery_mode")
    if delivery_mode is None:
        delivery_mode = _clean_str(payload.get("destination_type"), "destination_type")
    delivery_mode = delivery_mode or "server"
    if delivery_mode not in {"server", "client"}:
        raise ValueError("delivery_mode must be 'server' or 'client'")

    return {
        "query": query or "",
        "sources": sources,
        "search_only": bool(search_only),
        "music_mode": bool(music_mode),
        "final_format": final_format,
        "destination": destination,
        "destination_type": delivery_mode,
        "destination_path": destination,
        "delivery_mode": delivery_mode,
    }

def _env_or_default(name, default):
    value = os.environ.get(name)
    return value if value else default


def _looks_like_playlist_url(value):
    if not value or not isinstance(value, str):
        return False
    if extract_playlist_id(value):
        return True
    lowered = value.lower()
    return (
        "list=" in lowered
        or "/playlist" in lowered
        or "playlist?" in lowered
        or "?playlist" in lowered
    )


def _check_basic_auth(header_value):
    if not header_value or not header_value.startswith("Basic "):
        return False
    token = header_value[6:].strip()
    try:
        decoded = base64.b64decode(token.encode("ascii"), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return False
    if ":" not in decoded:
        return False
    user, password = decoded.split(":", 1)
    return hmac.compare_digest(user, _BASIC_AUTH_USER) and hmac.compare_digest(password, _BASIC_AUTH_PASS)


def _setup_logging(log_dir):
    ensure_dir(log_dir)
    root = logging.getLogger("")
    log_path = os.path.join(log_dir, "archiver.log")
    root.setLevel(logging.INFO)
    has_file = False
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler):
            if os.path.abspath(getattr(handler, "baseFilename", "")) == os.path.abspath(log_path):
                has_file = True
                break
    if not has_file:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        file_handler.setLevel(logging.INFO)
    root.addHandler(file_handler)


def _detect_worker_count():
    counts = []
    for key in _MULTI_WORKER_ENV_KEYS:
        raw = os.environ.get(key)
        if raw and raw.isdigit():
            counts.append(int(raw))
    cmd_args = os.environ.get("GUNICORN_CMD_ARGS", "")
    if cmd_args:
        match = re.search(r"--workers\s+(\d+)", cmd_args)
        if match:
            counts.append(int(match.group(1)))
    return max(counts) if counts else 1


def _warn_deprecated_fields(config):
    if not isinstance(config, dict):
        return
    for field in sorted(_DEPRECATED_FIELDS):
        if field in config and field not in _DEPRECATED_LOGGED:
            logging.warning("Deprecated config field '%s' ignored and will be removed on save", field)
            _DEPRECATED_LOGGED.add(field)


def _strip_deprecated_fields(config):
    if not isinstance(config, dict):
        return config
    updated = dict(config)
    removed = False
    # Deprecated fields are ignored to avoid changing runtime behavior.
    for field in _DEPRECATED_FIELDS:
        if field in updated:
            updated.pop(field, None)
            removed = True
    if removed:
        _warn_deprecated_fields(config)
    return updated


def _acquire_watcher_lock(lock_dir):
    lock_path = os.path.join(lock_dir, "watcher.lock")
    try:
        import fcntl
    except Exception:
        logging.error("Watcher lock unavailable; watcher disabled")
        return None
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode("utf-8"))
    return fd


class RunRequest(BaseModel):
    single_url: str | None = None
    playlist_id: str | None = None
    playlist_account: str | None = None
    destination: str | None = None
    final_format_override: str | None = None
    js_runtime: str | None = None
    music_mode: bool | None = None
    delivery_mode: str | None = None


class DirectUrlPreviewRequest(BaseModel):
    url: str


# Cancel job API request model
class CancelJobRequest(BaseModel):
    reason: str | None = None

# Helper to best-effort terminate a subprocess
def _terminate_subprocess(proc: subprocess.Popen, *, grace_sec: float = 3.0) -> None:
    """Best-effort terminate a subprocess quickly and safely."""
    if proc is None:
        return
    try:
        if proc.poll() is not None:
            return
    except Exception:
        return
    try:
        proc.terminate()
    except Exception:
        pass
    deadline = time.monotonic() + grace_sec
    while time.monotonic() < deadline:
        try:
            if proc.poll() is not None:
                return
        except Exception:
            return
        time.sleep(0.05)
    try:
        proc.kill()
    except Exception:
        pass


class ConfigPathRequest(BaseModel):
    path: str


class ScheduleRequest(BaseModel):
    enabled: bool | None = None
    mode: str | None = None
    interval_hours: int | None = None
    run_on_startup: bool | None = None


class OAuthStartRequest(BaseModel):
    account: str | None = None
    client_secret: str
    token_out: str


class OAuthCompleteRequest(BaseModel):
    session_id: str
    code: str


class SearchRequestPayload(BaseModel):
    created_by: str | None = None
    intent: str
    media_type: str | None = "generic"
    artist: str
    album: str | None = None
    track: str | None = None
    destination_dir: str | None = None
    include_albums: bool = True
    include_singles: bool = True
    min_match_score: float = 0.92
    duration_hint_sec: int | None = None
    quality_min_bitrate_kbps: int | None = None
    lossless_only: bool = False
    music_mode: bool = False
    auto_enqueue: bool = True
    source_priority: list[str] | str | None = None
    max_candidates_per_source: int = 5


def _purge_oauth_sessions():
    now = datetime.now(timezone.utc)
    with _OAUTH_LOCK:
        expired = [key for key, entry in _OAUTH_SESSIONS.items() if entry["expires_at"] <= now]
        for key in expired:
            _OAUTH_SESSIONS.pop(key, None)


class EnqueueCandidatePayload(BaseModel):
    candidate_id: str
    delivery_mode: Optional[str] = None
    final_format: Optional[str] = None


class SpotifyPlaylistImportPayload(BaseModel):
    playlist_url: str


class IntentExecutePayload(BaseModel):
    intent_type: IntentType
    identifier: str


def _build_music_track_canonical_id(
    artist,
    album,
    track_number,
    track,
    *,
    recording_mbid=None,
    mb_release_id=None,
    mb_release_group_id=None,
    disc_number=None,
):
    return build_music_track_canonical_id(
        artist,
        album,
        track_number,
        track,
        recording_mbid=recording_mbid,
        mb_release_id=mb_release_id,
        mb_release_group_id=mb_release_group_id,
        disc_number=disc_number,
    )


class _IntentQueueAdapter:
    """Queue adapter that writes intent payloads into the unified download queue."""

    def enqueue(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            logging.warning("Intent enqueue skipped: payload is not a dict")
            return
        engine = getattr(app.state, "worker_engine", None)
        store = getattr(engine, "store", None) if engine is not None else None
        if store is None:
            logging.warning("Intent enqueue skipped: worker engine store unavailable")
            return
        try:
            runtime_config = load_config(app.state.config_path)
        except Exception:
            runtime_config = {}
        base_dir = app.state.paths.single_downloads_dir

        media_intent = str(payload.get("media_intent") or "").strip() or "track"
        origin = "spotify_playlist" if payload.get("playlist_id") else "intent"
        origin_id = str(payload.get("playlist_id") or payload.get("spotify_track_id") or "manual")
        destination = str(payload.get("destination") or "").strip() or None
        final_format = str(payload.get("final_format") or "").strip() or None
        force_redownload = bool(payload.get("force_redownload"))
        canonical_metadata = payload.get("canonical_metadata") if isinstance(payload.get("canonical_metadata"), dict) else {}
        # Heuristic for Music Mode-origin payloads:
        # explicit music flags OR MusicBrainz identifiers already present in payload/canonical metadata.
        is_music_mode_origin = bool(payload.get("music_mode")) or str(payload.get("media_type") or "").strip().lower() == "music" or any(
            bool(str(payload.get(key) or "").strip())
            for key in (
                "recording_mbid",
                "mb_recording_id",
                "mb_release_id",
                "mb_release_group_id",
                "release_id",
                "release_group_id",
            )
        ) or any(
            bool(str(canonical_metadata.get(key) or "").strip())
            for key in (
                "recording_mbid",
                "mb_recording_id",
                "mb_release_id",
                "mb_release_group_id",
                "release_id",
                "release_group_id",
            )
        )

        def _to_dict(value):
            if isinstance(value, dict):
                return dict(value)
            if value is None:
                return {}
            out = {}
            for key in (
                "title",
                "artist",
                "album",
                "album_artist",
                "track_num",
                "disc_num",
                "date",
                "genre",
                "isrc",
                "mbid",
                "lyrics",
            ):
                if hasattr(value, key):
                    out[key] = getattr(value, key)
            return out

        def _enqueue_music_query_job(
            artist: str,
            track: str,
            album: str | None = None,
            *,
            recording_mbid: str | None = None,
            mb_release_id: str | None = None,
            mb_release_group_id: str | None = None,
        ) -> None:
            def _optional_pos_int(value):
                if value is None or str(value).strip() == "":
                    return None
                try:
                    parsed = int(value)
                except (TypeError, ValueError):
                    return None
                return parsed if parsed > 0 else None

            normalized_artist = str(artist or "").strip()
            normalized_track = str(track or "").strip()
            normalized_album = str(album or "").strip() or None
            normalized_album_artist = str(payload.get("album_artist") or "").strip() or None
            normalized_recording_mbid = str(recording_mbid or "").strip() or None
            normalized_release_mbid = str(mb_release_id or "").strip() or None
            normalized_release_group_mbid = str(mb_release_group_id or "").strip() or None
            normalized_track_number = _optional_pos_int(payload.get("track_number"))
            normalized_disc_number = _optional_pos_int(payload.get("disc_number"))
            normalized_track_total = _optional_pos_int(payload.get("track_total"))
            normalized_disc_total = _optional_pos_int(payload.get("disc_total"))
            normalized_release_date = str(payload.get("release_date") or "").strip() or None
            normalized_artwork_url = str(payload.get("artwork_url") or "").strip() or None
            normalized_genre = str(payload.get("genre") or "").strip() or None
            if not normalized_artist or not normalized_track:
                logging.warning("Intent enqueue skipped: music query missing artist/track")
                return
            query = quote(f"{normalized_artist} {normalized_track}".strip())
            url = f"https://music.youtube.com/search?q={query}"
            canonical_metadata = {
                "artist": normalized_artist,
                "track": normalized_track,
                "album": normalized_album,
                "album_artist": normalized_album_artist,
                "release_date": normalized_release_date,
                "track_number": normalized_track_number,
                "disc_number": normalized_disc_number,
                "track_total": normalized_track_total,
                "disc_total": normalized_disc_total,
                "artwork_url": normalized_artwork_url,
                "genre": normalized_genre,
                "duration_ms": payload.get("duration_ms"),
                "recording_mbid": normalized_recording_mbid,
                "mb_recording_id": normalized_recording_mbid,
                "mb_release_id": normalized_release_mbid,
                "mb_release_group_id": normalized_release_group_mbid,
            }
            canonical_id = _build_music_track_canonical_id(
                normalized_artist,
                normalized_album,
                payload.get("track_number"),
                normalized_track,
                recording_mbid=normalized_recording_mbid,
                mb_release_id=normalized_release_mbid,
                mb_release_group_id=normalized_release_group_mbid,
                disc_number=payload.get("disc_number"),
            )
            enqueue_payload = build_download_job_payload(
                config=runtime_config,
                origin=origin,
                origin_id=origin_id,
                media_type="music",
                media_intent="music_track",
                source="youtube_music",
                url=url,
                input_url=url,
                destination=destination,
                base_dir=base_dir,
                final_format_override=final_format,
                resolved_metadata=canonical_metadata,
                output_template_overrides={
                    "audio_mode": True,
                    "album_artist": normalized_album_artist,
                    "track_number": normalized_track_number,
                    "disc_number": normalized_disc_number,
                    "track_total": normalized_track_total,
                    "disc_total": normalized_disc_total,
                    "release_date": normalized_release_date,
                    "duration_ms": payload.get("duration_ms"),
                    "artwork_url": normalized_artwork_url,
                    "genre": normalized_genre,
                    "recording_mbid": normalized_recording_mbid,
                    "mb_recording_id": normalized_recording_mbid,
                    "mb_release_id": normalized_release_mbid,
                    "mb_release_group_id": normalized_release_group_mbid,
                },
                canonical_id=canonical_id,
            )
            store.enqueue_job(
                **enqueue_payload,
                force_requeue=force_redownload,
            )
            logging.info(
                "Intent payload queued playlist_id=%s spotify_track_id=%s",
                payload.get("playlist_id"),
                payload.get("spotify_track_id"),
            )

        if media_intent == "music_track":
            recording_mbid = str(
                payload.get("recording_mbid")
                or payload.get("mb_recording_id")
                or ""
            ).strip()
            if not recording_mbid:
                logging.error("[MUSIC] enqueue_rejected missing_recording_mbid")
                raise HTTPException(status_code=400, detail="recording_mbid required for music_track enqueue")
            _enqueue_music_query_job(
                str(payload.get("artist") or ""),
                str(payload.get("track") or payload.get("title") or ""),
                str(payload.get("album") or ""),
                recording_mbid=recording_mbid,
                mb_release_id=str(payload.get("mb_release_id") or payload.get("release_id") or ""),
                mb_release_group_id=str(payload.get("mb_release_group_id") or payload.get("release_group_id") or ""),
            )
            return

        resolved_media = payload.get("resolved_media") if isinstance(payload.get("resolved_media"), dict) else {}
        media_url = str(resolved_media.get("media_url") or payload.get("url") or "").strip()
        if not media_url:
            if is_music_mode_origin:
                logging.warning("[MUSIC] enqueue_rejected music_mode_requires_mbid")
                raise HTTPException(status_code=400, detail="music_mode_requires_mbid")
            fallback_artist = str(payload.get("artist") or "").strip()
            fallback_track = str(payload.get("track") or payload.get("title") or "").strip()
            fallback_album = str(payload.get("album") or "").strip() or None
            if fallback_artist and fallback_track:
                _enqueue_music_query_job(fallback_artist, fallback_track, fallback_album)
                return
            logging.warning("Intent enqueue skipped: no media URL or searchable artist/title available")
            return
        source = str(resolved_media.get("source_id") or payload.get("source") or resolve_source(media_url)).strip() or "unknown"
        music_metadata = _to_dict(payload.get("music_metadata"))
        external_ids = music_metadata.get("external_ids") if isinstance(music_metadata.get("external_ids"), dict) else {}
        canonical_id = str(
            music_metadata.get("isrc")
            or music_metadata.get("mbid")
            or ""
        ).strip() or extract_external_track_canonical_id(
            external_ids,
            fallback_spotify_id=payload.get("spotify_track_id"),
        )
        enqueue_payload = build_download_job_payload(
            config=runtime_config,
            origin=origin,
            origin_id=origin_id,
            media_type="music",
            media_intent=media_intent,
            source=source,
            url=media_url,
            input_url=media_url,
            destination=destination,
            base_dir=base_dir,
            final_format_override=final_format,
            resolved_metadata=music_metadata,
            output_template_overrides={
                "audio_mode": True,
                "duration_ms": resolved_media.get("duration_ms"),
            },
            canonical_id=canonical_id,
        )
        store.enqueue_job(
            **enqueue_payload,
            force_requeue=force_redownload,
        )
        logging.info(
            "Intent payload queued playlist_id=%s spotify_track_id=%s",
            payload.get("playlist_id"),
            payload.get("spotify_track_id"),
        )


class SafeJSONResponse(JSONResponse):
    def render(self, content):
        return json.dumps(
            safe_json(content),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")


app = FastAPI(
    title=APP_NAME,
    description="Retreivr API for self-hosted playlist archiving, scheduling, and metrics.",
    default_response_class=SafeJSONResponse,
)

if _TRUST_PROXY:
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")


@app.middleware("http")
async def basic_auth_middleware(request: Request, call_next):
    if not _BASIC_AUTH_ENABLED:
        return await call_next(request)
    if request.method == "OPTIONS":
        return await call_next(request)
    auth_header = request.headers.get("authorization")
    if not _check_basic_auth(auth_header):
        return PlainTextResponse(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": "Basic"},
        )
    return await call_next(request)


@app.on_event("startup")
async def startup():
    app.state.paths = build_engine_paths()
    try:
        app.state.config_path = resolve_config_path(os.environ.get("YT_ARCHIVER_CONFIG"))
    except ValueError as exc:
        logging.error("Invalid config override: %s", exc)
        app.state.config_path = resolve_config_path(None)
    app.state.log_path = os.path.join(LOG_DIR, "archiver.log")
    app.state.running = False
    app.state.state = "idle"
    app.state.run_id = None
    app.state.started_at = None
    app.state.finished_at = None
    app.state.last_error = None
    app.state.status = EngineStatus()
    app.state.run_lock = asyncio.Lock()
    app.state.stop_event = threading.Event()
    app.state.run_task = None
    app.state.loop = asyncio.get_running_loop()
    app.state.schedule_lock = threading.Lock()
    app.state.ytdlp_update_lock = threading.Lock()
    app.state.ytdlp_update_running = False
    app.state.cancel_requested = False
    app.state.scheduler = BackgroundScheduler(timezone="UTC")
    app.state.watch_config_cache = None
    app.state.was_in_downtime = False
    app.state.watcher_task = None
    app.state.worker_count = _detect_worker_count()
    app.state.single_worker_enforced = app.state.worker_count <= 1
    ensure_dir(DATA_DIR)
    ensure_dir(CONFIG_DIR)
    ensure_dir(LOG_DIR)
    ensure_dir(DOWNLOADS_DIR)
    ensure_dir(TOKENS_DIR)
    logging.info(
        "Resolved paths: downloads=%s data=%s config=%s logs=%s tokens=%s",
        DOWNLOADS_DIR,
        DATA_DIR,
        CONFIG_DIR,
        LOG_DIR,
        TOKENS_DIR,
    )
    app.state.browse_roots = _browse_root_map()
    _setup_logging(LOG_DIR)
    _init_schedule_db(app.state.paths.db_path)
    state = _read_schedule_state(app.state.paths.db_path)
    app.state.schedule_last_run = state.get("last_run")
    app.state.schedule_next_run = state.get("next_run")
    schedule_config = _default_schedule_config()
    config = _read_config_for_scheduler()
    app.state.loaded_config = config if isinstance(config, dict) else {}
    app.state.config = app.state.loaded_config
    config_exists = os.path.exists(app.state.config_path)
    config_size = 0
    if config_exists:
        try:
            config_size = int(os.path.getsize(app.state.config_path))
        except OSError:
            config_size = 0
    startup_cfg = get_loaded_config()
    logging.info(
        json.dumps(
            safe_json(
                {
                    "message": "config_loaded",
                    "config_path": app.state.config_path,
                    "exists": bool(config_exists),
                    "bytes": config_size,
                    "keys_count": len(startup_cfg.keys()) if isinstance(startup_cfg, dict) else 0,
                    "has_js_runtime": bool(isinstance(startup_cfg, dict) and "js_runtime" in startup_cfg),
                    "has_js_runtimes": bool(isinstance(startup_cfg, dict) and "js_runtimes" in startup_cfg),
                    "js_runtime_value": startup_cfg.get("js_runtime") if isinstance(startup_cfg, dict) else None,
                    "js_runtimes_value": startup_cfg.get("js_runtimes") if isinstance(startup_cfg, dict) else None,
                }
            ),
            sort_keys=True,
        )
    )
    if startup_cfg:
        schedule_config = _merge_schedule_config(startup_cfg.get("schedule"))
    app.state.schedule_config = schedule_config
    app.state.scheduler.start()
    _apply_schedule_config(schedule_config)
    _apply_spotify_schedule(startup_cfg if isinstance(startup_cfg, dict) else {})
    if schedule_config.get("enabled") and schedule_config.get("run_on_startup"):
        asyncio.create_task(_handle_scheduled_run())
    if schedule_config.get("enabled"):
        logging.info("Scheduler active — fixed interval bulk runs")

    init_db(app.state.paths.db_path)
    _ensure_watch_tables(app.state.paths.db_path)
    app.state.search_db_path = resolve_search_db_path(app.state.paths.db_path, startup_cfg)
    logging.info("Search DB path: %s", app.state.search_db_path)
    app.state.search_service = SearchResolutionService(
        search_db_path=app.state.search_db_path,
        queue_db_path=app.state.paths.db_path,
        adapters=None,
        config=startup_cfg,
        paths=app.state.paths,
    )
    app.state.search_request_overrides = {}
    app.state.search_service.request_overrides = app.state.search_request_overrides
    # Ensure search DB schema exists before any read operations.
    SearchJobStore(app.state.search_db_path).ensure_schema()
    json_sanity_check()
    if os.environ.get("RETREIVR_DIAG", "").strip().lower() in {"1", "true", "yes"}:
        diag_url = os.environ.get("RETREIVR_DIAG_URL", "https://youtu.be/PmtGDk0c-JM")
        logging.info("RETREIVR_DIAG enabled; running direct URL self-test")
        await anyio.to_thread.run_sync(
            run_direct_url_self_test,
            startup_cfg,
            paths=app.state.paths,
            url=diag_url,
            final_format_override="mkv",
        )
    app.state.spotify_playlist_importer = SpotifyPlaylistImporter()
    app.state.spotify_import_status = {}
    app.state.playlist_import_jobs = {}
    app.state.playlist_import_jobs_lock = threading.Lock()
    app.state.playlist_import_active_count = 0
    app.state.music_cover_art_cache = {}

    app.state.worker_stop_event = threading.Event()
    app.state.worker_engine = DownloadWorkerEngine(
        app.state.paths.db_path,
        startup_cfg,
        app.state.paths,
        search_service=app.state.search_service,
    )

    def _worker_runner():
        logging.info("Download worker started")
        logging.info("Polling unified download queue")
        app.state.worker_engine.run_loop(stop_event=app.state.worker_stop_event)

    app.state.worker_thread = threading.Thread(
        target=_worker_runner,
        name="download-worker",
        daemon=True,
    )
    app.state.worker_thread.start()


    watch_policy = normalize_watch_policy(startup_cfg)
    app.state.watch_policy = watch_policy
    app.state.watch_config_cache = startup_cfg
    app.state.watcher_clients_cache = {}
    enable_watcher = bool(startup_cfg.get("enable_watcher")) if isinstance(startup_cfg, dict) else False
    if not enable_watcher:
        app.state.watcher_lock = None
        app.state.watcher_task = None
        logging.info("Watcher disabled by config; not starting")
    else:
        if not app.state.single_worker_enforced:
            logging.info(
                "Watcher disabled due to guardrails (multiple workers detected=%d)",
                app.state.worker_count,
            )
            app.state.watcher_lock = None
        else:
            app.state.watcher_lock = _acquire_watcher_lock(DATA_DIR)
        if app.state.watcher_lock is None:
            logging.info("Watcher disabled due to guardrails (lock unavailable)")
        else:
            app.state.watcher_task = asyncio.create_task(_watcher_supervisor())
            logging.info("Watcher active — adaptive monitoring enabled")

    app.state.watcher_status = {
        "state": "disabled" if app.state.watcher_lock is None else "idle",
        "last_poll_ts": None,
        "next_poll_ts": None,
        "pending_playlists_count": 0,
        "quiet_window_remaining_sec": None,
        "batch_active": False,
    }

    downtime_active, _ = _check_downtime(config or {})
    logging.info(
        "Watcher startup: enabled=%s single_worker=%s downtime_active=%s",
        bool(app.state.watcher_lock),
        app.state.single_worker_enforced,
        downtime_active,
    )


@app.on_event("shutdown")
async def shutdown():
    if app.state.running:
        app.state.stop_event.set()
        task = app.state.run_task
        if task:
            try:
                await asyncio.wait_for(task, timeout=30)
            except asyncio.TimeoutError:
                logging.warning("Shutdown timeout while waiting for archive run to stop")

    worker_stop = getattr(app.state, "worker_stop_event", None)
    if worker_stop:
        worker_stop.set()
    worker_thread = getattr(app.state, "worker_thread", None)
    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=10)

    scheduler = app.state.scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
    watcher_task = getattr(app.state, "watcher_task", None)
    if watcher_task:
        logging.info("Watcher shutdown")
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
    lock_fd = getattr(app.state, "watcher_lock", None)
    if lock_fd is not None:
        try:
            os.close(lock_fd)
        except OSError:
            pass
    logging.shutdown()


def _browse_root_map():
    return {
        "downloads": os.path.realpath(DOWNLOADS_DIR),
        "config": os.path.realpath(CONFIG_DIR),
        "tokens": os.path.realpath(TOKENS_DIR),
    }


def _path_allowed(path, roots):
    real = os.path.realpath(path)
    for root in roots:
        try:
            if os.path.commonpath([real, root]) == root:
                return True
        except ValueError:
            continue
    return False


def _resolve_browse_path(root_base, rel_path):
    rel_path = (rel_path or "").strip()
    if os.path.isabs(rel_path):
        logging.warning("Browse blocked: absolute path %s", rel_path)
        raise HTTPException(status_code=400, detail="path must be relative")
    normalized = os.path.normpath(rel_path)
    if normalized in (".", os.curdir):
        normalized = ""
    if normalized.startswith(".."):
        logging.warning("Browse blocked: path escape attempt %s", rel_path)
        raise HTTPException(status_code=403, detail="path not allowed")
    abs_path = os.path.realpath(os.path.join(root_base, normalized))
    base = os.path.realpath(root_base)
    if os.path.commonpath([abs_path, base]) != base:
        logging.warning("Browse blocked: path outside root %s", rel_path)
        raise HTTPException(status_code=403, detail="path not allowed")
    return normalized, abs_path


def _list_browse_entries(base, directory, mode, ext, limit=None):
    entries = []
    with os.scandir(directory) as it:
        for entry in it:
            if entry.name.startswith("."):
                continue
            is_dir = entry.is_dir(follow_symlinks=False)
            is_file = entry.is_file(follow_symlinks=False)
            if mode == "dir":
                if not is_dir:
                    continue
            else:
                if not (is_dir or is_file):
                    continue
                if is_file and ext and not entry.name.lower().endswith(ext):
                    continue
            rel_entry = os.path.relpath(entry.path, base)
            entries.append(
                {
                    "name": entry.name,
                    "path": rel_entry if rel_entry != "." else "",
                    "abs_path": entry.path,
                    "type": "dir" if is_dir else "file",
                }
            )
            if limit and len(entries) >= limit:
                break
    entries.sort(key=lambda item: (item["type"] != "dir", item["name"].lower()))
    return entries


def _tail_lines(path, lines, max_bytes=1_000_000):
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        block = min(size, max_bytes)
        if block <= 0:
            return ""
        f.seek(-block, os.SEEK_END)
        data = f.read().splitlines()
    tail = data[-lines:] if lines else data
    return b"\n".join(tail).decode("utf-8", errors="replace")


def _normalize_date(value, end_of_day=False):
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        suffix = " 23:59:59" if end_of_day else " 00:00:00"
        return f"{value}{suffix}"
    return value


def _encode_file_id(rel_path):
    token = base64.urlsafe_b64encode(rel_path.encode("utf-8")).decode("ascii")
    return token.rstrip("=")


def _decode_file_id(file_id):
    padded = file_id + "=" * (-len(file_id) % 4)
    raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    return raw


def _safe_filename(name):
    cleaned = name.replace('"', "'").replace("\n", " ").replace("\r", " ").strip()
    return cleaned or "download"


def _resolve_direct_url_mode(
    *,
    media_type: str | None,
    media_intent: str | None,
    music_mode: bool = False,
) -> tuple[str, str, bool]:
    normalized_intent = str(media_intent or "").strip().lower()
    if bool(music_mode) or normalized_intent == "music_track" or is_music_media_type(media_type):
        return "music", "music_track", True

    resolved_media_type = str(media_type or "").strip().lower() or "video"
    resolved_media_intent = str(media_intent or "").strip().lower() or "episode"
    return resolved_media_type, resolved_media_intent, False


def _file_id_from_path(path):
    if not path:
        return None
    full = os.path.abspath(path)
    if not _path_allowed(full, [DOWNLOADS_DIR]):
        return None
    rel = os.path.relpath(full, DOWNLOADS_DIR)
    return _encode_file_id(rel)


def _iter_file(path, chunk_size=1024 * 1024):
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            yield chunk


def _yt_dlp_script_path():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts", "update_yt_dlp.sh"))



# Helper to record direct URL downloads into history
def _record_direct_url_history(db_path, files, source_url):
    if not files:
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Ensure the history table exists (direct URL runs can occur before other
    # flows have created the full schema).
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
            external_id TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(download_history)")
    existing_columns = {row[1] for row in cur.fetchall()}
    for column in ("input_url", "canonical_url", "external_id", "source"):
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

    now = datetime.now(timezone.utc).isoformat()
    input_url = source_url
    source = resolve_source(source_url) if source_url else None
    if source == "unknown":
        source = None
    external_id = extract_video_id(source_url) if source in {"youtube", "youtube_music"} else None
    canonical_url = canonicalize_url(source, input_url, external_id)
    for path in files:
        try:
            stat = os.stat(path)
        except OSError:
            continue

        cur.execute(
            """
            INSERT INTO download_history
                (
                    video_id, title, filename, destination, source, status,
                    created_at, completed_at, file_size_bytes,
                    input_url, canonical_url, external_id
                )
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None,
                os.path.basename(path),
                os.path.basename(path),
                os.path.dirname(path),
                source,
                "completed",
                now,
                now,
                int(stat.st_size),
                input_url,
                canonical_url,
                external_id,
            ),
        )

    conn.commit()
    conn.close()

# Execution router: client delivery runs immediately, server delivery enqueues.
def execute_download(*, delivery_mode: str | None, run_immediate, enqueue):
    mode = (delivery_mode or "server").strip().lower()
    if mode == "client":
        return run_immediate()
    return enqueue()

def _run_immediate_download_to_client(
    *,
    url: str,
    config: dict,
    paths,
    media_type: str | None,
    media_intent: str | None,
    final_format_override: str | None,
    stop_event: threading.Event | None = None,
    status: EngineStatus | None = None,
    origin: str | None = None,
):
    effective_config = get_loaded_config() or config
    if not isinstance(effective_config, dict) or not effective_config:
        raise RuntimeError("direct_url_runtime_config_missing")
    config = effective_config
    job_id = uuid4().hex
    temp_dir = os.path.join(paths.temp_downloads_dir, job_id)
    ensure_dir(temp_dir)

    filename_template = config.get("filename_template")
    audio_template = config.get("audio_filename_template") or config.get("music_filename_template")

    raw_final_format = final_format_override
    resolved_media_type, resolved_media_intent, audio_mode = _resolve_direct_url_mode(
        media_type=media_type,
        media_intent=media_intent,
        music_mode=False,
    )
    normalized_format = _normalize_format(raw_final_format)
    normalized_audio_format = _normalize_audio_format(raw_final_format)
    if audio_mode:
        raise RuntimeError("music_client_delivery_unsupported")
    else:
        final_format = (
            normalized_format
            or _normalize_format(config.get("final_format"))
            or _normalize_format(config.get("video_final_format"))
            or "mkv"
        )

    logging.info(
        json.dumps(
            safe_json(
                {
                    "message": "direct_url_effective_config",
                    "keys_count": len(config.keys()) if isinstance(config, dict) else 0,
                    "has_js_runtime": bool(isinstance(config, dict) and "js_runtime" in config),
                    "has_js_runtimes": bool(isinstance(config, dict) and "js_runtimes" in config),
                }
            ),
            sort_keys=True,
        )
    )

    try:
        info, local_file = download_with_ytdlp(
            url,
            temp_dir,
            config,
            audio_mode=audio_mode,
            final_format=final_format,
            cookie_file=None,
            stop_event=stop_event,
            media_type=resolved_media_type,
            media_intent=resolved_media_intent,
            job_id=job_id,
            origin=origin,
            resolved_destination=None,
        )
        if not info or not local_file:
            raise RuntimeError("yt_dlp_no_output")

        meta = extract_meta(info, fallback_url=url)
        video_id = meta.get("video_id") or job_id
        template = audio_template if audio_mode else filename_template
        final_path, _ = finalize_download_artifact(
            local_file=local_file,
            meta=meta,
            fallback_id=video_id,
            destination_dir=temp_dir,
            audio_mode=audio_mode,
            final_format=final_format,
            template=template,
            paths=paths,
            config=config,
            enforce_music_contract=False,
            enqueue_audio_metadata=False,
        )

        size = 0
        try:
            size = os.path.getsize(final_path)
        except OSError:
            size = 0
        if size <= 0:
            raise RuntimeError("empty_output_file")

        delivery_id, expires_at, _event = _register_client_delivery(
            final_path,
            os.path.basename(final_path),
            cleanup_dir=temp_dir,
        )

        if status is not None:
            lock = getattr(status, "lock", None)
            if lock:
                with lock:
                    status.client_delivery_id = delivery_id
                    status.client_delivery_filename = os.path.basename(final_path)
                    status.client_delivery_expires_at = expires_at.isoformat()
                    status.single_download_ok = True
                    status.last_completed_path = final_path
                    status.last_completed_at = datetime.now(timezone.utc).isoformat()
            else:
                status.client_delivery_id = delivery_id
                status.client_delivery_filename = os.path.basename(final_path)
                status.client_delivery_expires_at = expires_at.isoformat()
                status.single_download_ok = True
                status.last_completed_path = final_path
                status.last_completed_at = datetime.now(timezone.utc).isoformat()

        return {
            "delivery_id": delivery_id,
            "filename": os.path.basename(final_path),
            "expires_at": expires_at.isoformat(),
        }
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

def _run_direct_url_with_cli(
    *,
    url: str,
    paths,
    config: dict,
    destination: str | None,
    final_format_override: str | None,
    media_type: str | None,
    media_intent: str | None,
    music_mode: bool = False,
    stop_event: threading.Event,
    status: EngineStatus | None = None,
):
    """Fast-lane direct URL download via the yt-dlp CLI.

    Rationale: The CLI behavior is the reference implementation (matches user expectations)
    and avoids subtle differences vs the Python API wrapper.

    This function is intentionally minimal: download to temp, then atomically move completed
    files into the destination. Metadata/enrichment can occur post-download.
    """

    effective_config = get_loaded_config() or config
    if not isinstance(effective_config, dict) or not effective_config:
        raise RuntimeError("direct_url_missing_config")
    config = effective_config
    if not url or not isinstance(url, str):
        raise ValueError("single_url is required")

    # Resolve destination (default to configured DOWNLOADS_DIR)
    dest_dir = (destination or DOWNLOADS_DIR).strip() if destination else DOWNLOADS_DIR
    ensure_dir(dest_dir)

    # Direct URL runs are intentionally NOT persisted into the unified download_jobs queue.
    # They bypass adapters/worker and run yt-dlp CLI synchronously (reference behavior).
    job_id = uuid4().hex

    # Always download into an isolated job temp dir
    temp_dir = os.path.join(paths.temp_downloads_dir, job_id)
    ensure_dir(temp_dir)

    logging.info(
        json.dumps(
            safe_json(
                {
                    "message": "direct_url_effective_config",
                    "keys_count": len(config.keys()) if isinstance(config, dict) else 0,
                    "has_js_runtime": bool(isinstance(config, dict) and "js_runtime" in config),
                    "has_js_runtimes": bool(isinstance(config, dict) and "js_runtimes" in config),
                }
            ),
            sort_keys=True,
        )
    )

    # Resolve direct-url mode once and execute through the canonical download_with_ytdlp path.
    cli_media_type, cli_media_intent, audio_mode = _resolve_direct_url_mode(
        media_type=media_type,
        media_intent=media_intent,
        music_mode=music_mode,
    )

    logging.info(
        json.dumps(
            safe_json(
                {
                    "message": "DIRECT_URL_DOWNLOAD_START",
                    "url": url,
                    "job_id": job_id,
                    "destination": dest_dir,
                    "final_format": final_format_override,
                    "media_type": cli_media_type,
                    "media_intent": cli_media_intent,
                    "audio_mode": bool(audio_mode),
                }
            ),
            sort_keys=True,
        )
    )

    try:
        resolved_final_format = final_format_override
        if audio_mode:
            resolved_final_format = (
                _normalize_audio_format(final_format_override)
                or _normalize_audio_format(config.get("music_final_format"))
                or _normalize_audio_format(config.get("audio_final_format"))
                or "mp3"
            )
        else:
            resolved_final_format = (
                _normalize_format(final_format_override)
                or _normalize_format(config.get("final_format"))
                or _normalize_format(config.get("video_final_format"))
                or "mkv"
            )
        info, local_file = download_with_ytdlp(
            url,
            temp_dir,
            config,
            audio_mode=audio_mode,
            final_format=resolved_final_format,
            cookie_file=None,
            stop_event=stop_event,
            media_type=cli_media_type,
            media_intent=cli_media_intent,
            job_id=job_id,
            origin="api",
            resolved_destination=dest_dir,
        )
        if not info or not local_file:
            raise RuntimeError("yt_dlp_no_output")
        meta = extract_meta(info, fallback_url=url)
        if not (meta.get("title") and (meta.get("channel") or meta.get("artist"))):
            preview = preview_direct_url(url, config)
            if isinstance(preview, dict):
                if not meta.get("title"):
                    meta["title"] = preview.get("title")
                if not meta.get("channel") and preview.get("uploader"):
                    meta["channel"] = preview.get("uploader")
                if not meta.get("artist") and preview.get("uploader"):
                    meta["artist"] = preview.get("uploader")
                if not meta.get("thumbnail_url"):
                    meta["thumbnail_url"] = preview.get("thumbnail_url")
        video_id = meta.get("video_id") or extract_video_id(url) or job_id
        if audio_mode:
            duration_sec = info.get("duration") if isinstance(info, dict) else None
            duration_ms = int(float(duration_sec) * 1000) if duration_sec else None
            binding_payload = {
                "media_type": "music",
                "media_intent": "music_track",
                "artist": meta.get("artist") or meta.get("channel"),
                "track": meta.get("track") or meta.get("title"),
                "album": meta.get("album"),
                "duration_ms": duration_ms,
                "output_template": {
                    "canonical_metadata": {
                        "recording_mbid": meta.get("mb_recording_id") or meta.get("recording_mbid"),
                        "mb_release_id": meta.get("mb_release_id"),
                        "mb_release_group_id": meta.get("mb_release_group_id"),
                        "album": meta.get("album"),
                        "release_date": meta.get("release_date"),
                        "track_number": meta.get("track_number"),
                        "disc_number": meta.get("disc") or meta.get("disc_number"),
                        "duration_ms": duration_ms,
                        "artist": meta.get("artist") or meta.get("channel"),
                        "track": meta.get("track") or meta.get("title"),
                    }
                },
            }
            ensure_mb_bound_music_track(
                binding_payload,
                config=config,
                country_preference=str(config.get("country") or "US"),
            )
            canonical = (
                binding_payload.get("output_template", {}).get("canonical_metadata")
                if isinstance(binding_payload.get("output_template", {}), dict)
                else {}
            )
            if isinstance(canonical, dict):
                meta["artist"] = canonical.get("artist") or meta.get("artist")
                meta["album_artist"] = canonical.get("artist") or meta.get("album_artist") or meta.get("artist")
                meta["album"] = canonical.get("album")
                meta["track"] = canonical.get("track") or meta.get("track") or meta.get("title")
                meta["release_date"] = canonical.get("release_date")
                meta["track_number"] = canonical.get("track_number")
                meta["disc_number"] = canonical.get("disc_number")
                meta["disc"] = canonical.get("disc_number")
                meta["recording_mbid"] = canonical.get("recording_mbid")
                meta["mb_recording_id"] = canonical.get("recording_mbid")
                meta["mb_release_id"] = canonical.get("mb_release_id")
                meta["mb_release_group_id"] = canonical.get("mb_release_group_id")

        filename_template = (
            config.get("audio_filename_template") or config.get("music_filename_template")
            if audio_mode
            else config.get("filename_template")
        )
        final_path, meta = finalize_download_artifact(
            local_file=local_file,
            meta=meta,
            fallback_id=video_id,
            destination_dir=dest_dir,
            audio_mode=audio_mode,
            final_format=resolved_final_format,
            template=filename_template,
            paths=paths,
            config=config,
            enforce_music_contract=bool(audio_mode and str(cli_media_intent or "").strip().lower() == "music_track"),
            enqueue_audio_metadata=bool(audio_mode),
        )
        moved = [final_path]

        # Finalize engine status for UI consumers (CLI-equivalent direct URL run)
        if status is not None:
            try:
                status.run_successes = moved
                status.last_completed_path = moved[-1]
                status.run_total = len(moved)
                status.run_failures = []
                status.completed = True
                status.completed_at = datetime.now(timezone.utc).isoformat()
            except Exception:
                pass

        # Ensure in-memory engine status reflects completion for UI polling.
        try:
            if status is not None:
                status.completed = True
                status.completed_at = datetime.now(timezone.utc).isoformat()
                status.run_failures = []
            app.state.state = "idle"
        except Exception:
            pass
        logging.info(
            json.dumps(
                safe_json(
                    {
                        "message": "DIRECT_URL_CLI_DONE",
                        "job_id": job_id,
                        "files": moved,
                        "destination": dest_dir,
                    }
                ),
                sort_keys=True,
            )
        )

        # Call the helper to record direct URL downloads into history
        try:
            _record_direct_url_history(paths.db_path, moved, url)
        except sqlite3.Error:
            logging.exception("Failed to record direct URL history (sqlite)")
        except Exception:
            logging.exception("Failed to record direct URL history")

        # Ensure in-memory engine status reflects completion for UI polling.
        try:
            if status is not None:
                status.completed = True
                status.completed_at = datetime.now(timezone.utc).isoformat()
                status.run_failures = []
        except Exception:
            pass

    finally:
        try:
            app.state.current_download_proc = None
            app.state.current_download_job_id = None
        except Exception:
            pass
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass


def _list_download_files(base_dir):
    if not os.path.isdir(base_dir):
        return []
    results = []
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith("."):
                continue
            full_path = os.path.join(root, name)
            try:
                stat = os.stat(full_path)
            except OSError:
                continue
            rel = os.path.relpath(full_path, base_dir)
            results.append(
                {
                    "id": _encode_file_id(rel),
                    "name": name,
                    "relative_path": rel,
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }
            )
    results.sort(key=lambda item: item["modified_at"], reverse=True)
    return results


def _downloads_metrics(base_dir):
    total_files = 0
    total_bytes = 0
    if not os.path.isdir(base_dir):
        return total_files, total_bytes
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith("."):
                continue
            file_path = os.path.join(root, name)
            try:
                total_bytes += os.path.getsize(file_path)
                total_files += 1
            except OSError:
                continue
    return total_files, total_bytes


def _disk_usage(path):
    try:
        stat = os.statvfs(path)
    except OSError:
        return {
            "total_bytes": None,
            "free_bytes": None,
            "used_bytes": None,
            "free_percent": None,
        }
    total = stat.f_frsize * stat.f_blocks
    free = stat.f_frsize * stat.f_bavail
    used = total - free
    free_percent = (free / total) * 100 if total else None
    return {
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "free_percent": round(free_percent, 1) if free_percent is not None else None,
    }


def _init_schedule_db(db_path):
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS schedule_state (key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.commit()
    conn.close()


def _read_schedule_state(db_path):
    if not os.path.exists(db_path):
        return {"last_run": None, "next_run": None}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM schedule_state WHERE key IN ('last_run', 'next_run')")
    rows = cur.fetchall()
    conn.close()
    state = {"last_run": None, "next_run": None}
    for key, value in rows:
        state[key] = value
    return state


def _write_schedule_state(db_path, *, last_run=None, next_run=None):
    if last_run is None and next_run is None:
        return
    _init_schedule_db(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for key, value in (("last_run", last_run), ("next_run", next_run)):
        if value is None:
            cur.execute("DELETE FROM schedule_state WHERE key=?", (key,))
        else:
            cur.execute(
                "INSERT INTO schedule_state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
    conn.commit()
    conn.close()


def _default_schedule_config():
    return {
        "enabled": False,
        "mode": "interval",
        "interval_hours": 6,
        "run_on_startup": False,
    }


def _default_watch_policy():
    return {
        "min_interval_minutes": 5,
        "max_interval_minutes": 360,
        "idle_backoff_factor": 2,
        "active_reset_minutes": 5,
        "downtime": {
            "enabled": False,
            "start": "23:00",
            "end": "09:00",
            "timezone": "local",
        },
    }


def _merge_schedule_config(schedule):
    merged = _default_schedule_config()
    if isinstance(schedule, dict):
        for key in ("enabled", "mode", "interval_hours", "run_on_startup"):
            if key in schedule:
                merged[key] = schedule[key]
    return merged


def _merge_watch_policy(policy):
    merged = _default_watch_policy()
    if isinstance(policy, dict):
        for key in ("min_interval_minutes", "max_interval_minutes", "idle_backoff_factor", "active_reset_minutes"):
            if key in policy:
                merged[key] = policy[key]
        downtime = policy.get("downtime")
        if isinstance(downtime, dict):
            merged_downtime = dict(merged["downtime"])
            for key in ("enabled", "start", "end", "timezone"):
                if key in downtime:
                    merged_downtime[key] = downtime[key]
            merged["downtime"] = merged_downtime
    return merged


def normalize_watch_policy(raw_config):
    """Apply defaults only when watch_policy is missing; otherwise require full config."""
    last_good = getattr(app.state, "watch_policy", None) or _default_watch_policy()
    normalize_watch_policy.valid = True

    if not isinstance(raw_config, dict):
        logging.error("Invalid watch_policy config; using last-known-good policy")
        normalize_watch_policy.valid = False
        return last_good

    if "watch_policy" not in raw_config:
        # Defaults only when missing entirely.
        return _default_watch_policy()

    policy = raw_config.get("watch_policy")
    if not isinstance(policy, dict):
        logging.error("Invalid watch_policy: must be an object")
        normalize_watch_policy.valid = False
        return last_good

    required = {"min_interval_minutes", "max_interval_minutes", "idle_backoff_factor", "active_reset_minutes", "downtime"}
    missing = sorted(key for key in required if key not in policy)
    if missing:
        logging.error("Invalid watch_policy: missing fields %s", ", ".join(missing))
        normalize_watch_policy.valid = False
        return last_good

    downtime = policy.get("downtime")
    if not isinstance(downtime, dict):
        logging.error("Invalid watch_policy.downtime: must be an object")
        normalize_watch_policy.valid = False
        return last_good

    required_dt = {"enabled", "start", "end", "timezone"}
    missing_dt = sorted(key for key in required_dt if key not in downtime)
    if missing_dt:
        logging.error("Invalid watch_policy.downtime: missing fields %s", ", ".join(missing_dt))
        normalize_watch_policy.valid = False
        return last_good

    errors = _validate_watch_policy(policy)
    if errors:
        logging.error("Invalid watch_policy: %s", errors)
        normalize_watch_policy.valid = False
        return last_good

    tz_value = downtime.get("timezone")
    if tz_value not in {"local", "system", "UTC"}:
        try:
            ZoneInfo(tz_value)
        except Exception:
            logging.error("Invalid watch_policy.downtime.timezone: %s", tz_value)
            normalize_watch_policy.valid = False
            return last_good

    return {
        "min_interval_minutes": policy["min_interval_minutes"],
        "max_interval_minutes": policy["max_interval_minutes"],
        "idle_backoff_factor": policy["idle_backoff_factor"],
        "active_reset_minutes": policy["active_reset_minutes"],
        "downtime": {
            "enabled": downtime["enabled"],
            "start": downtime["start"],
            "end": downtime["end"],
            "timezone": downtime["timezone"],
        },
    }


def _validate_schedule_config(schedule):
    errors = []
    if schedule is None:
        return errors
    if not isinstance(schedule, dict):
        return ["schedule must be an object"]
    enabled = schedule.get("enabled")
    if enabled is not None and not isinstance(enabled, bool):
        errors.append("schedule.enabled must be true/false")
    mode = schedule.get("mode", "interval")
    if mode != "interval":
        errors.append("schedule.mode must be 'interval'")
    interval_hours = schedule.get("interval_hours")
    if interval_hours is not None:
        if not isinstance(interval_hours, int):
            errors.append("schedule.interval_hours must be an integer")
        elif interval_hours < 1:
            errors.append("schedule.interval_hours must be >= 1")
    if enabled and interval_hours is None:
        errors.append("schedule.interval_hours is required when schedule is enabled")
    run_on_startup = schedule.get("run_on_startup")
    if run_on_startup is not None and not isinstance(run_on_startup, bool):
        errors.append("schedule.run_on_startup must be true/false")
    return errors


def _validate_watch_policy(policy):
    if policy is None:
        return []
    if not isinstance(policy, dict):
        return ["watch_policy must be an object"]
    errors = []
    min_interval = policy.get("min_interval_minutes")
    max_interval = policy.get("max_interval_minutes")
    idle_backoff = policy.get("idle_backoff_factor")
    active_reset = policy.get("active_reset_minutes")
    if min_interval is not None and not isinstance(min_interval, int):
        errors.append("watch_policy.min_interval_minutes must be an integer")
    if max_interval is not None and not isinstance(max_interval, int):
        errors.append("watch_policy.max_interval_minutes must be an integer")
    if idle_backoff is not None and not isinstance(idle_backoff, int):
        errors.append("watch_policy.idle_backoff_factor must be an integer")
    if active_reset is not None and not isinstance(active_reset, int):
        errors.append("watch_policy.active_reset_minutes must be an integer")
    if isinstance(min_interval, int) and min_interval < 1:
        errors.append("watch_policy.min_interval_minutes must be >= 1")
    if isinstance(max_interval, int) and max_interval < 1:
        errors.append("watch_policy.max_interval_minutes must be >= 1")
    if isinstance(min_interval, int) and isinstance(max_interval, int) and max_interval < min_interval:
        errors.append("watch_policy.max_interval_minutes must be >= min_interval_minutes")
    if isinstance(idle_backoff, int) and idle_backoff < 1:
        errors.append("watch_policy.idle_backoff_factor must be >= 1")
    if isinstance(active_reset, int) and active_reset < 1:
        errors.append("watch_policy.active_reset_minutes must be >= 1")
    downtime = policy.get("downtime")
    if downtime is not None:
        if not isinstance(downtime, dict):
            errors.append("watch_policy.downtime must be an object")
        else:
            enabled = downtime.get("enabled")
            if enabled is not None and not isinstance(enabled, bool):
                errors.append("watch_policy.downtime.enabled must be true/false")
            for key in ("start", "end"):
                value = downtime.get(key)
                if value is not None and not isinstance(value, str):
                    errors.append(f"watch_policy.downtime.{key} must be a string (HH:MM)")
            timezone_value = downtime.get("timezone")
            if timezone_value is not None and not isinstance(timezone_value, str):
                errors.append("watch_policy.downtime.timezone must be a string")
    return errors


def _cleanup_dir(path):
    deleted_files = 0
    deleted_bytes = 0
    if not os.path.isdir(path):
        return deleted_files, deleted_bytes
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                deleted_bytes += os.path.getsize(file_path)
            except OSError:
                pass
            try:
                os.remove(file_path)
                deleted_files += 1
            except OSError:
                pass
        for name in dirs:
            dir_path = os.path.join(root, name)
            try:
                os.rmdir(dir_path)
            except OSError:
                pass
    ensure_dir(path)
    return deleted_files, deleted_bytes


def _read_config_or_404():
    config_path = app.state.config_path
    if not os.path.exists(config_path):
        raise HTTPException(status_code=404, detail=f"Config not found: {config_path}")
    try:
        config = load_config(config_path)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in config: {exc}") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read config: {exc}") from exc
    errors = validate_config(config)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    _warn_deprecated_fields(config)
    normalized = safe_json(_strip_deprecated_fields(config))
    app.state.loaded_config = normalized if isinstance(normalized, dict) else {}
    app.state.config = app.state.loaded_config
    return normalized


def _spotify_client_credentials(config: dict | None) -> tuple[str, str]:
    cfg = config or {}
    spotify_cfg = (cfg.get("spotify") or {}) if isinstance(cfg, dict) else {}
    client_id = str(spotify_cfg.get("client_id") or cfg.get("SPOTIFY_CLIENT_ID") or "").strip()
    client_secret = str(spotify_cfg.get("client_secret") or cfg.get("SPOTIFY_CLIENT_SECRET") or "").strip()
    return client_id, client_secret


def _build_spotify_client_with_optional_oauth(config: dict | None) -> SpotifyPlaylistClient:
    """Build a Spotify client using OAuth access token when valid, else public mode."""
    client_id, client_secret = _spotify_client_credentials(config)
    if not client_id or not client_secret:
        return SpotifyPlaylistClient()

    store = SpotifyOAuthStore(Path(app.state.paths.db_path))
    existing = store.load()
    try:
        token = store.get_valid_token(client_id, client_secret, config=config if isinstance(config, dict) else None)
    except Exception as exc:
        logging.warning("Spotify OAuth token validation failed; using public mode: %s", exc)
        token = None

    if token is not None:
        return SpotifyPlaylistClient(
            client_id=client_id,
            client_secret=client_secret,
            access_token=token.access_token,
        )

    if existing is not None:
        logging.warning("Spotify OAuth token expired/invalid and was cleared; using public mode")
    return SpotifyPlaylistClient(client_id=client_id, client_secret=client_secret)


def _read_config_for_scheduler():
    config_path = app.state.config_path
    if not os.path.exists(config_path):
        logging.error("Schedule skipped: config not found at %s", config_path)
        return None
    try:
        config = load_config(config_path)
    except json.JSONDecodeError as exc:
        logging.error("Schedule skipped: invalid JSON in config: %s", exc)
        return None
    except OSError as exc:
        logging.error("Schedule skipped: failed to read config: %s", exc)
        return None
    errors = validate_config(config)
    if errors:
        logging.error("Schedule skipped: invalid config: %s", errors)
        return None
    _warn_deprecated_fields(config)
    normalized = _strip_deprecated_fields(config)
    app.state.loaded_config = normalized if isinstance(normalized, dict) else {}
    app.state.config = app.state.loaded_config
    return normalized


def _read_config_for_watcher():
    config_path = app.state.config_path
    cached = app.state.watch_config_cache
    if not os.path.exists(config_path):
        logging.error("Watcher skipped: config not found at %s", config_path)
        return cached
    try:
        with open(config_path, "r") as handle:
            data = handle.read()
        config = json.loads(data)
    except json.JSONDecodeError as exc:
        logging.error("Watcher skipped: invalid JSON in config: %s", exc)
        return cached
    except OSError as exc:
        logging.error("Watcher skipped: failed to read config: %s", exc)
        return cached
    errors = validate_config(config)
    if errors:
        logging.error("Watcher skipped: invalid config: %s", errors)
        return cached
    _warn_deprecated_fields(config)
    config = _strip_deprecated_fields(config)
    app.state.loaded_config = config if isinstance(config, dict) else {}
    app.state.config = app.state.loaded_config
    policy = normalize_watch_policy(config)
    if not getattr(normalize_watch_policy, "valid", True):
        return cached or config
    config["watch_policy"] = policy
    app.state.watch_policy = policy
    app.state.watch_config_cache = config
    return config


async def _start_run_with_config(
    config,
    *,
    single_url=None,
    playlist_id=None,
    playlist_account=None,
    playlist_mode=None,
    destination=None,
    final_format_override=None,
    js_runtime=None,
    music_mode=None,
    run_source="api",
    skip_downtime=False,
    run_id_override=None,
    now=None,
    delivery_mode=None,
):
    runtime_config = get_loaded_config()
    if not runtime_config and isinstance(config, dict) and config:
        runtime_config = safe_json(_strip_deprecated_fields(config))
        app.state.loaded_config = runtime_config
        app.state.config = runtime_config
    if not runtime_config:
        runtime_config = _read_config_or_404()
    config = runtime_config

    async with app.state.run_lock:
        if app.state.running:
            return "busy", None

        app.state.running = True
        app.state.state = "running"
        app.state.run_id = run_id_override or str(uuid4())
        app.state.started_at = datetime.now(timezone.utc).isoformat()
        app.state.finished_at = None
        app.state.last_error = None
        status = EngineStatus()
        app.state.status = status
        app.state.stop_event = threading.Event()

        async def _runner():
            effective_final_format_override = final_format_override
            if effective_final_format_override is None:
                effective_final_format_override = (
                    config.get("default_video_format")
                    or config.get("final_format")
                    or "mkv"
                )
            try:
                logging.info(
                    "Run runner entered run_id=%s source=%s single_url=%s playlist_id=%s",
                    app.state.run_id,
                    run_source,
                    bool(single_url),
                    bool(playlist_id),
                )
                if run_source == "watcher":
                    logging.info("Watcher-triggered run starting")
                elif run_source == "scheduled":
                    logging.info("Scheduled run starting")
                else:
                    logging.info("Manual run starting (source=%s)", run_source)
                if playlist_id:
                    run_callable = functools.partial(
                        run_single_playlist,
                        config,
                        playlist_id,
                        destination,
                        playlist_account,
                        effective_final_format_override,
                        paths=app.state.paths,
                        status=status,
                        js_runtime_override=js_runtime,
                        stop_event=app.state.stop_event,
                        music_mode=bool(music_mode) if music_mode is not None else False,
                        mode=playlist_mode or "full",
                    )
                else:
                    if single_url:
                        runtime_config = get_loaded_config() or config
                        if not isinstance(runtime_config, dict) or not runtime_config:
                            raise RuntimeError("direct_url_runtime_config_missing")
                        resolved_music_mode = bool(music_mode) if music_mode is not None else False
                        if resolved_music_mode:
                            resolved_media_type = "music"
                            resolved_media_intent = "music_track"
                        else:
                            resolved_media_type = resolve_media_type(runtime_config, url=single_url)
                            resolved_media_intent = resolve_media_intent("manual", resolved_media_type)
                        effective_delivery_mode = delivery_mode or "server"
                        if resolved_media_type == "music" and effective_delivery_mode == "client":
                            # Music Mode must pass through queue MB binding; no client fast-lane bypass.
                            effective_delivery_mode = "server"
                        run_callable = execute_download(
                            delivery_mode=effective_delivery_mode,
                            run_immediate=lambda: functools.partial(
                                _run_immediate_download_to_client,
                                url=single_url,
                                config=runtime_config,
                                paths=app.state.paths,
                                media_type=resolved_media_type,
                                media_intent=resolved_media_intent,
                                final_format_override=effective_final_format_override,
                                stop_event=app.state.stop_event,
                                status=status,
                                origin=run_source,
                            ),
                            enqueue=lambda: functools.partial(
                                _run_direct_url_with_cli,
                                url=single_url,
                                paths=app.state.paths,
                                config=runtime_config,
                                destination=destination,
                                final_format_override=effective_final_format_override,
                                media_type=resolved_media_type,
                                media_intent=resolved_media_intent,
                                music_mode=resolved_music_mode,
                                stop_event=app.state.stop_event,
                                status=status,
                            ),
                        )
                    else:
                        run_callable = functools.partial(
                            run_archive,
                            config,
                            paths=app.state.paths,
                            status=status,
                            single_url=single_url,
                            destination=destination,
                            final_format_override=effective_final_format_override,
                            js_runtime_override=js_runtime,
                            stop_event=app.state.stop_event,
                            run_source=run_source,
                            music_mode=bool(music_mode) if music_mode is not None else False,
                            skip_downtime=skip_downtime,
                            delivery_mode=delivery_mode or "server",
                        )
                await anyio.to_thread.run_sync(run_callable)
                if (
                    run_source == "api"
                    and not single_url
                    and not playlist_id
                    and bool((config.get("spotify") or {}).get("sync_user_playlists"))
                ):
                    logging.info("Manual run triggering Spotify playlist sync (override downtime)")
                    try:
                        await _spotify_playlists_schedule_tick(
                            config=config,
                            db=PlaylistSnapshotStore(app.state.paths.db_path),
                            queue=_IntentQueueAdapter(),
                            spotify_client=_build_spotify_client_with_optional_oauth(config),
                            search_service=app.state.search_service,
                            ignore_downtime=True,
                        )
                        logging.info("Manual-run Spotify playlist sync completed")
                    except Exception:
                        logging.exception("Manual-run Spotify playlist sync failed")
                if run_source == "api" and not single_url and not playlist_id:
                    logging.info("Manual run completed (archive + Spotify sync)")
                # Ensure UI state finalization for direct URL runs
                if single_url:
                    try:
                        status.completed = True
                        status.completed_at = datetime.now(timezone.utc).isoformat()
                        app.state.state = "idle"
                        app.state.last_error = None
                    except Exception:
                        pass
                if app.state.stop_event.is_set():
                    if app.state.cancel_requested:
                        app.state.last_error = "Downloads cancelled by user"
                    else:
                        app.state.last_error = "Run stopped"
                    app.state.state = "error"
            except Exception as exc:
                # Direct URL cancel should not surface as a generic error state.
                if str(exc) == "direct_url_cancelled" or getattr(app.state, "cancel_requested", False):
                    app.state.cancel_requested = False
                    app.state.last_error = "Downloads cancelled by user"
                    app.state.state = "idle"
                else:
                    logging.exception("Archive run failed: %s", exc)
                    app.state.last_error = str(exc)
                    app.state.state = "error"
            finally:
                # Ensure CLI process isn't left running
                try:
                    proc = getattr(app.state, "current_download_proc", None)
                    if proc and proc.poll() is None:
                        proc.terminate()
                except Exception:
                    pass
                app.state.running = False
                app.state.finished_at = datetime.now(timezone.utc).isoformat()
                if app.state.cancel_requested:
                    logging.info("Active downloads killed")
                    app.state.cancel_requested = False
                    app.state.state = "idle"
                    app.state.last_error = "Downloads cancelled by user"
                    _cleanup_dir(app.state.paths.temp_downloads_dir)
                    _cleanup_dir(app.state.paths.ytdlp_temp_dir)
                    logging.info("Downloads cancelled by user")
                    logging.info("State reset to idle")
                elif app.state.state in {"running", "completed"}:
                    app.state.state = "idle"
                
                notify_run_summary(
                    config,
                    run_type=run_source,
                    status=status,
                    started_at=app.state.started_at,
                    finished_at=app.state.finished_at,
                )

        app.state.run_task = asyncio.create_task(_runner())

    return "started", None


# Cancel endpoint for jobs (API-level)
@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, payload: CancelJobRequest = Body(default=CancelJobRequest())):
    """
    Cancel a queued/active download job.

    Behavior:
    - If the job corresponds to the direct-URL fast-lane, terminate the active yt-dlp CLI subprocess.
    - If the job is a queued worker job, mark it CANCELLED in the download_jobs table and request
      cancellation from the worker engine.
    """
    reason = (payload.reason or "Cancelled by user").strip() if payload else "Cancelled by user"

    # 1) Direct URL fast-lane: terminate current CLI process (if this matches)
    try:
        current_job_id = getattr(app.state, "current_download_job_id", None)
        current_proc = getattr(app.state, "current_download_proc", None)
        if current_job_id and current_job_id == job_id and current_proc:
            app.state.cancel_requested = True
            await anyio.to_thread.run_sync(_terminate_subprocess, current_proc)
            return {"ok": True, "job_id": job_id, "status": "cancelled", "scope": "direct_url"}
    except Exception:
        logging.exception("Direct URL cancel path failed")

    # 2) Unified queue jobs: mark cancelled and ask worker to stop
    try:
        engine = getattr(app.state, "worker_engine", None)
        if engine is not None and hasattr(engine, "cancel_job") and callable(getattr(engine, "cancel_job")):
            try:
                engine.cancel_job(job_id, reason=reason)
            except TypeError:
                engine.cancel_job(job_id)
            return {"ok": True, "job_id": job_id, "status": "cancelled", "scope": "queue"}
        # Fallback if engine isn't available for some reason: mark cancelled in DB.
        store = DownloadJobStore(app.state.paths.db_path)
        store.mark_canceled(job_id, reason=reason)
        return {"ok": True, "job_id": job_id, "status": "cancelled", "scope": "queue"}
    except Exception as exc:
        logging.exception("Cancel job failed")
        raise HTTPException(status_code=500, detail=f"Cancel failed: {exc}")


def _get_next_run_iso():
    scheduler = app.state.scheduler
    if not scheduler:
        return None
    job = scheduler.get_job(SCHEDULE_JOB_ID)
    if not job or not job.next_run_time:
        return None
    next_run = job.next_run_time
    if next_run.tzinfo is None:
        next_run = next_run.replace(tzinfo=timezone.utc)
    return next_run.astimezone(timezone.utc).isoformat()


_UNSET = object()


def _set_schedule_state(*, last_run=_UNSET, next_run=_UNSET):
    with app.state.schedule_lock:
        if last_run is not _UNSET:
            app.state.schedule_last_run = last_run
        if next_run is not _UNSET:
            app.state.schedule_next_run = next_run
    db_last = None if last_run is _UNSET else last_run
    db_next = None if next_run is _UNSET else next_run
    _write_schedule_state(app.state.paths.db_path, last_run=db_last, next_run=db_next)


def _schedule_tick():
    # Scheduler = periodic full runs based on schedule config.
    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(_handle_scheduled_run(), loop)


def _liked_songs_schedule_tick():
    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(_handle_liked_songs_scheduled_run(), loop)


def _saved_albums_schedule_tick():
    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(_handle_saved_albums_scheduled_run(), loop)


def _user_playlists_schedule_tick():
    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(_handle_user_playlists_scheduled_run(), loop)


def _spotify_playlists_schedule_tick(
    config=None,
    db=None,
    queue=None,
    spotify_client=None,
    search_service=None,
    ignore_downtime: bool = False,
):
    if config is not None:
        return spotify_playlists_watch_job(
            config=config,
            db=db,
            queue=queue,
            spotify_client=spotify_client,
            search_service=search_service,
            ignore_downtime=ignore_downtime,
        )

    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    config = _read_config_for_scheduler()
    downtime_active = False
    if config:
        downtime_active, _ = _check_downtime(config)
    if downtime_active:
        logging.info("Interval Spotify sync tick skipped due to downtime")
        return
    logging.info("Interval Spotify sync tick starting")
    asyncio.run_coroutine_threadsafe(_handle_spotify_playlists_scheduled_run(), loop)


async def _handle_scheduled_run():
    if _playlist_imports_active():
        logging.info("Scheduled run skipped; playlist import active")
        _set_schedule_state(next_run=_get_next_run_iso())
        return
    if app.state.running:
        logging.info("Scheduled run skipped; run already active")
        _set_schedule_state(next_run=_get_next_run_iso())
        return
    config = _read_config_for_scheduler()
    if not config:
        _set_schedule_state(next_run=_get_next_run_iso())
        return
    result, next_allowed = await _start_run_with_config(config, run_source="scheduled")
    if result == "started":
        logging.info("Scheduled run starting")
        now = datetime.now(timezone.utc).isoformat()
        _set_schedule_state(last_run=now, next_run=_get_next_run_iso())
    elif result == "deferred":
        _set_schedule_state(next_run=_format_iso(next_allowed))
    else:
        _set_schedule_state(next_run=_get_next_run_iso())


def _resolve_liked_songs_interval_minutes(config: dict | None) -> int:
    cfg = config or {}
    spotify_cfg = (cfg.get("spotify") or {}) if isinstance(cfg, dict) else {}
    raw_value = spotify_cfg.get("liked_songs_sync_interval_minutes")
    if raw_value is None:
        raw_value = cfg.get("liked_songs_sync_interval_minutes")
    try:
        interval = int(raw_value)
    except (TypeError, ValueError):
        interval = DEFAULT_LIKED_SONGS_SYNC_INTERVAL_MINUTES
    return max(1, interval)


def _resolve_saved_albums_interval_minutes(config: dict | None) -> int:
    cfg = config or {}
    spotify_cfg = (cfg.get("spotify") or {}) if isinstance(cfg, dict) else {}
    raw_value = spotify_cfg.get("saved_albums_sync_interval_minutes")
    if raw_value is None:
        raw_value = cfg.get("saved_albums_sync_interval_minutes")
    try:
        interval = int(raw_value)
    except (TypeError, ValueError):
        interval = DEFAULT_SAVED_ALBUMS_SYNC_INTERVAL_MINUTES
    return max(1, interval)


def _resolve_user_playlists_interval_minutes(config: dict | None) -> int:
    cfg = config or {}
    spotify_cfg = (cfg.get("spotify") or {}) if isinstance(cfg, dict) else {}
    raw_value = spotify_cfg.get("user_playlists_sync_interval_minutes")
    if raw_value is None:
        raw_value = cfg.get("user_playlists_sync_interval_minutes")
    try:
        interval = int(raw_value)
    except (TypeError, ValueError):
        interval = DEFAULT_USER_PLAYLISTS_SYNC_INTERVAL_MINUTES
    return max(1, interval)


def _resolve_spotify_playlists_interval_minutes(config: dict | None) -> int:
    cfg = config or {}
    spotify_cfg = (cfg.get("spotify") or {}) if isinstance(cfg, dict) else {}
    raw_value = spotify_cfg.get("watch_playlists_interval_minutes")
    if raw_value is None:
        raw_value = cfg.get("watch_playlists_interval_minutes")
    try:
        interval = int(raw_value)
    except (TypeError, ValueError):
        interval = DEFAULT_SPOTIFY_PLAYLISTS_SYNC_INTERVAL_MINUTES
    return max(1, interval)


def _normalized_watch_playlists(config: dict | None) -> list[str]:
    cfg = config or {}
    spotify_cfg = (cfg.get("spotify") or {}) if isinstance(cfg, dict) else {}
    raw_values = spotify_cfg.get("watch_playlists")
    if raw_values is None:
        raw_values = cfg.get("watch_playlists", []) if isinstance(cfg, dict) else []
    if not isinstance(raw_values, list):
        return []
    playlist_ids: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        playlist_id = normalize_spotify_playlist_identifier(str(raw_value or ""))
        if not playlist_id or not re.match(r"^[A-Za-z0-9]+$", playlist_id):
            logging.warning("Skipping invalid Spotify playlist identifier: %s", raw_value)
            continue
        if playlist_id in seen:
            continue
        seen.add(playlist_id)
        playlist_ids.append(playlist_id)
    return playlist_ids


def _has_connected_spotify_oauth_token(db_path: str) -> bool:
    try:
        return SpotifyOAuthStore(Path(db_path)).load() is not None
    except Exception:
        return False


async def _handle_liked_songs_scheduled_run() -> None:
    if _playlist_imports_active():
        logging.info("Scheduled Spotify Liked Songs sync skipped; playlist import active")
        return
    config = _read_config_for_scheduler()
    if not config:
        return

    client_id, client_secret = _spotify_client_credentials(config)
    if not client_id or not client_secret:
        return

    store = SpotifyOAuthStore(Path(app.state.paths.db_path))
    token = store.get_valid_token(client_id, client_secret, config=config if isinstance(config, dict) else None)
    if token is None:
        return

    try:
        await spotify_liked_songs_watch_job(
            config=config,
            db=PlaylistSnapshotStore(app.state.paths.db_path),
            queue=_IntentQueueAdapter(),
            spotify_client=SpotifyPlaylistClient(
                client_id=client_id,
                client_secret=client_secret,
                access_token=token.access_token,
            ),
            search_service=app.state.search_service,
        )
    except Exception:
        logging.exception("Scheduled Spotify Liked Songs sync failed")


async def _handle_saved_albums_scheduled_run() -> None:
    if _playlist_imports_active():
        logging.info("Scheduled Spotify Saved Albums sync skipped; playlist import active")
        return
    config = _read_config_for_scheduler()
    if not config:
        return

    client_id, client_secret = _spotify_client_credentials(config)
    if not client_id or not client_secret:
        return

    store = SpotifyOAuthStore(Path(app.state.paths.db_path))
    token = store.get_valid_token(client_id, client_secret, config=config if isinstance(config, dict) else None)
    if token is None:
        return

    try:
        await spotify_saved_albums_watch_job(
            config=config,
            db=PlaylistSnapshotStore(app.state.paths.db_path),
            queue=_IntentQueueAdapter(),
            spotify_client=SpotifyPlaylistClient(
                client_id=client_id,
                client_secret=client_secret,
                access_token=token.access_token,
            ),
            search_service=app.state.search_service,
        )
    except Exception:
        logging.exception("Scheduled Spotify Saved Albums sync failed")


async def _handle_user_playlists_scheduled_run() -> None:
    if _playlist_imports_active():
        logging.info("Scheduled Spotify User Playlists sync skipped; playlist import active")
        return
    config = _read_config_for_scheduler()
    if not config:
        return

    client_id, client_secret = _spotify_client_credentials(config)
    if not client_id or not client_secret:
        return

    store = SpotifyOAuthStore(Path(app.state.paths.db_path))
    token = store.get_valid_token(client_id, client_secret, config=config if isinstance(config, dict) else None)
    if token is None:
        return

    try:
        await spotify_user_playlists_watch_job(
            config=config,
            db=PlaylistSnapshotStore(app.state.paths.db_path),
            queue=_IntentQueueAdapter(),
            spotify_client=SpotifyPlaylistClient(
                client_id=client_id,
                client_secret=client_secret,
                access_token=token.access_token,
            ),
            search_service=app.state.search_service,
        )
    except Exception:
        logging.exception("Scheduled Spotify User Playlists sync failed")


async def _handle_spotify_playlists_scheduled_run() -> None:
    if _playlist_imports_active():
        logging.info("Scheduled Spotify playlists sync skipped; playlist import active")
        return
    config = _read_config_for_scheduler()
    if not config:
        return

    spotify_client = _build_spotify_client_with_optional_oauth(config)
    snapshot_store = PlaylistSnapshotStore(app.state.paths.db_path)
    queue = _IntentQueueAdapter()
    try:
        await spotify_playlists_watch_job(
            config=config,
            db=snapshot_store,
            queue=queue,
            spotify_client=spotify_client,
            search_service=app.state.search_service,
        )
    except Exception:
        logging.exception("Scheduled Spotify playlists sync failed")


def _apply_spotify_schedule(config: dict):
    logger.info("Applying Spotify scheduler configuration")
    scheduler = app.state.scheduler
    if not scheduler:
        return

    # Remove existing Spotify jobs
    for job_id in [
        "spotify_liked_songs_watch",
        "spotify_saved_albums_watch",
        "spotify_user_playlists_watch",
        "spotify_playlists_watch",
    ]:
        try:
            scheduler.remove_job(job_id)
            logger.info(f"Removed job {job_id}")
        except Exception:
            pass

    spotify_cfg = config.get("spotify", {})

    # Liked Songs
    if spotify_cfg.get("sync_liked_songs"):
        interval = int(spotify_cfg.get("liked_songs_sync_interval_minutes", 15))
        scheduler.add_job(
            _liked_songs_schedule_tick,
            "interval",
            minutes=interval,
            id="spotify_liked_songs_watch",
            replace_existing=True,
        )
        logger.info(f"Spotify liked songs sync enabled (interval={interval} min)")
    else:
        logger.info("Spotify liked songs sync disabled by config")

    # Saved Albums
    if spotify_cfg.get("sync_saved_albums"):
        interval = int(spotify_cfg.get("saved_albums_sync_interval_minutes", 30))
        scheduler.add_job(
            _saved_albums_schedule_tick,
            "interval",
            minutes=interval,
            id="spotify_saved_albums_watch",
            replace_existing=True,
        )
        logger.info(f"Spotify saved albums sync enabled (interval={interval} min)")
    else:
        logger.info("Spotify saved albums sync disabled by config")

    # User Playlists (OAuth-based)
    if spotify_cfg.get("sync_user_playlists"):
        interval = int(spotify_cfg.get("user_playlists_sync_interval_minutes", 30))
        scheduler.add_job(
            _user_playlists_schedule_tick,
            "interval",
            minutes=interval,
            id="spotify_user_playlists_watch",
            replace_existing=True,
        )
        logger.info(f"Spotify user playlists sync enabled (interval={interval} min)")
    else:
        logger.info("Spotify user playlists sync disabled by config")

    # Manual playlist polling (watch_playlists)
    if spotify_cfg.get("sync_user_playlists") and spotify_cfg.get("watch_playlists"):
        interval = int(spotify_cfg.get("user_playlists_sync_interval_minutes", 30))
        scheduler.add_job(
            _spotify_playlists_schedule_tick,
            "interval",
            minutes=interval,
            id="spotify_playlists_watch",
            replace_existing=True,
        )
        logger.info("Spotify manual playlist watch enabled")
    else:
        logger.info("Spotify manual playlist watch disabled")


def _apply_schedule_config(schedule):
    scheduler = app.state.scheduler
    if not scheduler:
        return
    job = scheduler.get_job(SCHEDULE_JOB_ID)
    if job:
        scheduler.remove_job(SCHEDULE_JOB_ID)

    if schedule.get("enabled"):
        interval = schedule.get("interval_hours") or 1
        start_date = datetime.now(timezone.utc) + timedelta(hours=interval)
        scheduler.add_job(
            _schedule_tick,
            trigger=IntervalTrigger(hours=interval, start_date=start_date),
            id=SCHEDULE_JOB_ID,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _set_schedule_state(next_run=_get_next_run_iso())
    else:
        _set_schedule_state(next_run=None)


def _schedule_response():
    with app.state.schedule_lock:
        last_run = app.state.schedule_last_run
        next_run = app.state.schedule_next_run
    schedule = app.state.schedule_config
    return {
        "schema_version": SCHEDULE_SCHEMA_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "schedule": schedule,
        "enabled": schedule.get("enabled", False),
        "last_run": last_run,
        "next_run": next_run,
    }


def _parse_iso(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_iso(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_hhmm(value):
    if not value:
        return None
    value = value.strip()
    if not value or ":" not in value:
        return None
    parts = value.split(":", 1)
    if len(parts) != 2:
        return None
    if not parts[0].isdigit() or not parts[1].isdigit():
        return None
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def _resolve_timezone(value, fallback_tzinfo):
    if not value or value.lower() in {"local", "system"}:
        return fallback_tzinfo or timezone.utc
    if value.upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(value)
    except Exception:
        logging.warning("Invalid watch_policy timezone %s; falling back to local", value)
        return fallback_tzinfo or timezone.utc


def in_downtime(now, start_str, end_str):
    start = _parse_hhmm(start_str)
    end = _parse_hhmm(end_str)
    if not start or not end:
        return False, None
    # Compare in the same timezone; handles windows that cross midnight.
    start_dt = now.replace(hour=start[0], minute=start[1], second=0, microsecond=0)
    end_dt = now.replace(hour=end[0], minute=end[1], second=0, microsecond=0)
    if start_dt <= end_dt:
        in_window = start_dt <= now < end_dt
        next_allowed = end_dt if in_window else None
        return in_window, next_allowed
    # Window crosses midnight.
    if now >= start_dt:
        return True, end_dt + timedelta(days=1)
    if now < end_dt:
        return True, end_dt
    return False, None


def _check_downtime(config, *, now=None):
    policy = normalize_watch_policy(config)
    downtime = policy.get("downtime") or {}
    if not downtime.get("enabled"):
        return False, None
    if now is None:
        local_now = datetime.now().astimezone()
    else:
        local_now = now
    tzinfo = _resolve_timezone(downtime.get("timezone"), local_now.tzinfo)
    now = local_now.astimezone(tzinfo)
    return in_downtime(now, downtime.get("start"), downtime.get("end"))


def _deferred_run_tick(payload):
    loop = app.state.loop
    if not loop or loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(_handle_deferred_run(payload), loop)


async def _handle_deferred_run(payload):
    if app.state.running:
        delay = datetime.now(timezone.utc) + timedelta(minutes=1)
        scheduler = app.state.scheduler
        if scheduler:
            scheduler.add_job(
                _deferred_run_tick,
                trigger=DateTrigger(run_date=delay),
                args=[payload],
                id=f"{DEFERRED_RUN_JOB_ID}_{uuid4()}",
                replace_existing=False,
                max_instances=1,
                misfire_grace_time=30,
            )
        return
    config = payload.get("config") if isinstance(payload, dict) else None
    if config:
        in_dt, next_allowed = _check_downtime(config)
        if in_dt and next_allowed:
            _schedule_deferred_run(payload, next_allowed)
            return
    await _start_run_with_config(**payload, skip_downtime=True)


def _schedule_deferred_run(payload, next_allowed):
    scheduler = app.state.scheduler
    if not scheduler or not next_allowed:
        return
    scheduler.add_job(
        _deferred_run_tick,
        trigger=DateTrigger(run_date=next_allowed),
        args=[payload],
        id=f"{DEFERRED_RUN_JOB_ID}_{uuid4()}",
        replace_existing=False,
        max_instances=1,
        misfire_grace_time=30,
    )


def _ensure_watch_tables(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_watch (
            playlist_id TEXT PRIMARY KEY,
            last_check TIMESTAMP,
            next_check TIMESTAMP,
            idle_count INTEGER DEFAULT 0
        )
        """
    )
    cur.execute("PRAGMA table_info(playlist_watch)")
    existing_cols = {row[1] for row in cur.fetchall()}
    if "last_checked_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_checked_at TIMESTAMP")
    if "next_poll_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN next_poll_at TIMESTAMP")
    if "current_interval_min" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN current_interval_min INTEGER")
    if "consecutive_no_change" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN consecutive_no_change INTEGER")
    if "last_change_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_change_at TIMESTAMP")
    if "skip_reason" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN skip_reason TEXT")
    if "last_error" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_error TEXT")
    if "last_error_at" not in existing_cols:
        cur.execute("ALTER TABLE playlist_watch ADD COLUMN last_error_at TIMESTAMP")
    if "last_check" in existing_cols and "last_checked_at" in existing_cols:
        cur.execute(
            "UPDATE playlist_watch SET last_checked_at=COALESCE(last_checked_at, last_check) "
            "WHERE last_checked_at IS NULL"
        )
    if "next_check" in existing_cols and "next_poll_at" in existing_cols:
        cur.execute(
            "UPDATE playlist_watch SET next_poll_at=COALESCE(next_poll_at, next_check) "
            "WHERE next_poll_at IS NULL"
        )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playlist_watch_next ON playlist_watch (next_check)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playlist_watch_next_poll ON playlist_watch (next_poll_at)")
    conn.commit()
    conn.close()


def _read_watch_state(db_path):
    rows = {}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT playlist_id, last_checked_at, next_poll_at, idle_count, "
        "current_interval_min, consecutive_no_change, last_change_at, skip_reason, "
        "last_error, last_error_at "
        "FROM playlist_watch"
    )
    for (
        playlist_id,
        last_checked_at,
        next_poll_at,
        idle_count,
        interval,
        consecutive,
        last_change_at,
        skip_reason,
        last_error,
        last_error_at,
    ) in cur.fetchall():
        effective_consecutive = consecutive if consecutive is not None else (idle_count or 0)
        rows[playlist_id] = {
            "last_checked_at": last_checked_at,
            "next_poll_at": next_poll_at,
            "idle_count": idle_count or 0,
            "current_interval_min": interval,
            "consecutive_no_change": effective_consecutive or 0,
            "last_change_at": last_change_at,
            "skip_reason": skip_reason,
            "last_error": last_error,
            "last_error_at": last_error_at,
        }
    conn.close()
    return rows


def _write_watch_state(
    db_path,
    playlist_id,
    *,
    last_checked_at=None,
    next_poll_at=None,
    idle_count=None,
    current_interval_min=None,
    consecutive_no_change=None,
    last_change_at=None,
    skip_reason=None,
    last_error=None,
    last_error_at=None,
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO playlist_watch ("
        "playlist_id, last_checked_at, next_poll_at, idle_count, "
        "current_interval_min, consecutive_no_change, last_change_at, skip_reason, "
        "last_error, last_error_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(playlist_id) DO UPDATE SET "
        "last_checked_at=excluded.last_checked_at, next_poll_at=excluded.next_poll_at, "
        "idle_count=excluded.idle_count, current_interval_min=excluded.current_interval_min, "
        "consecutive_no_change=excluded.consecutive_no_change, last_change_at=excluded.last_change_at, "
        "skip_reason=excluded.skip_reason, last_error=excluded.last_error, "
        "last_error_at=excluded.last_error_at",
        (
            playlist_id,
            last_checked_at,
            next_poll_at,
            idle_count,
            current_interval_min,
            consecutive_no_change,
            last_change_at,
            skip_reason,
            last_error,
            last_error_at,
        ),
    )
    conn.commit()
    conn.close()


def _playlist_label(playlist_id, playlist_name):
    label = playlist_name or playlist_id
    return label if label else "unknown"


def _log_skip_reason(playlist_id, reason, watch, *, next_check=None):
    prev = watch.get("skip_reason")
    if reason != prev:
        if next_check:
            logging.debug("Watcher skipped (%s) playlist_id=%s next_check=%s", reason, playlist_id, next_check)
        else:
            logging.debug("Watcher skipped (%s) playlist_id=%s", reason, playlist_id)
    return reason


async def _poll_single_playlist(config, now, policy, pl, watch, yt_clients, batch_state):
    playlist_key = pl.get("playlist_id") or pl.get("id")
    if not playlist_key:
        return
    playlist_id = extract_playlist_id(playlist_key) or playlist_key
    playlist_name = pl.get("name") or ""

    min_interval = policy.get("min_interval_minutes") or 5
    max_interval = policy.get("max_interval_minutes") or min_interval
    backoff_factor = policy.get("idle_backoff_factor") or 2
    active_reset = policy.get("active_reset_minutes") or min_interval
    active_interval = max(min_interval, min(active_reset, max_interval))

    consecutive_no_change = watch.get("consecutive_no_change") or 0
    current_interval = watch.get("current_interval_min")
    if not isinstance(current_interval, int) or current_interval < min_interval:
        current_interval = min_interval

    if watch.get("current_interval_min") is None:
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=watch.get("last_checked_at"),
            next_poll_at=watch.get("next_poll_at"),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=watch.get("skip_reason"),
            last_error=watch.get("last_error"),
            last_error_at=watch.get("last_error_at"),
        )

    account = pl.get("account")
    yt = yt_clients.get(account) if account else None
    if not yt:
        skip_reason = _log_skip_reason(playlist_id, "oauth missing", watch)
        error_at = _format_iso(now)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=watch.get("last_checked_at"),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=skip_reason,
            last_error="oauth missing",
            last_error_at=error_at,
        )
        return

    try:
        videos = get_playlist_videos(yt, playlist_id)
    except RefreshError as exc:
        logging.error("Watcher poll error playlist_id=%s error=%s", playlist_id, exc)
        skip_reason = _log_skip_reason(playlist_id, "poll error", watch)
        error_at = _format_iso(now)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=skip_reason,
            last_error=f"api error: {exc}",
            last_error_at=error_at,
        )
        return
    except HttpError as exc:
        logging.error("Watcher poll error playlist_id=%s error=%s", playlist_id, exc)
        skip_reason = _log_skip_reason(playlist_id, "poll error", watch)
        error_at = _format_iso(now)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=skip_reason,
            last_error=f"api error: {exc}",
            last_error_at=error_at,
        )
        return

    if not videos:
        logging.debug("Watcher polled playlist_id=%s items=0", playlist_id)
        consecutive_no_change += 1
        current_interval = min(current_interval * backoff_factor, max_interval)
        logging.debug(
            "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
            playlist_id,
            current_interval,
            consecutive_no_change,
        )
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=watch.get("last_change_at"),
            skip_reason=None,
            last_error=watch.get("last_error"),
            last_error_at=watch.get("last_error_at"),
        )
        return

    if any("position" in v or "playlist_index" in v for v in videos):
        videos = sorted(
            videos,
            key=lambda v: v.get("position") or v.get("playlist_index") or 0,
            reverse=True,
        )
    else:
        videos = list(reversed(videos))

    playlist_mode = (pl.get("mode") or "full").lower()
    if playlist_mode not in {"full", "subscribe"}:
        playlist_mode = "full"
    subscribe_mode = playlist_mode == "subscribe"
    label = _playlist_label(playlist_id, playlist_name)
    logging.debug("Watcher polled playlist_id=%s items=%s", playlist_id, len(videos))

    with sqlite3.connect(app.state.paths.db_path) as conn:
        if subscribe_mode and not playlist_has_seen(conn, playlist_id):
            # First watcher pass for subscribe mode marks items as seen without downloading.
            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                mark_video_seen(conn, playlist_id, vid, downloaded=False)
            conn.commit()
            logging.debug(
                'Watcher subscribe first run playlist_id=%s label="%s" seen=%d download=0',
                playlist_id,
                label,
                len(videos),
            )
            consecutive_no_change = 0
            current_interval = active_interval
            _write_watch_state(
                app.state.paths.db_path,
                playlist_id,
                last_checked_at=_format_iso(now),
                next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
                idle_count=consecutive_no_change,
                current_interval_min=current_interval,
                consecutive_no_change=consecutive_no_change,
                last_change_at=watch.get("last_change_at"),
                skip_reason=None,
                last_error=watch.get("last_error"),
                last_error_at=watch.get("last_error_at"),
            )
            return

    new_ids = []
    with sqlite3.connect(app.state.paths.db_path) as conn:
        if subscribe_mode:
            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                if is_video_seen(conn, playlist_id, vid):
                    break
                new_ids.append(vid)
        else:
            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue
                if not is_video_downloaded(conn, vid):
                    new_ids.append(vid)

    if new_ids:
        pending = batch_state["pending_playlists"]
        pending.add(playlist_id)
        batch_state["last_detection_ts"] = time.monotonic()
        logging.info(
            "Watcher: detected updates playlist_id=%s pending=%s",
            playlist_id,
            len(pending),
        )
        current_interval = active_interval
        consecutive_no_change = 0
        last_change_at = _format_iso(now)
        next_poll_at = now + timedelta(minutes=current_interval)
        _write_watch_state(
            app.state.paths.db_path,
            playlist_id,
            last_checked_at=_format_iso(now),
            next_poll_at=_format_iso(next_poll_at),
            idle_count=consecutive_no_change,
            current_interval_min=current_interval,
            consecutive_no_change=consecutive_no_change,
            last_change_at=last_change_at,
            skip_reason=None,
            last_error=watch.get("last_error"),
            last_error_at=watch.get("last_error_at"),
        )
        return

    consecutive_no_change += 1
    current_interval = min(current_interval * backoff_factor, max_interval)
    logging.debug(
        "Watcher backoff applied playlist_id=%s interval_min=%s no_change=%s",
        playlist_id,
        current_interval,
        consecutive_no_change,
    )
    _write_watch_state(
        app.state.paths.db_path,
        playlist_id,
        last_checked_at=_format_iso(now),
        next_poll_at=_format_iso(now + timedelta(minutes=current_interval)),
        idle_count=consecutive_no_change,
        current_interval_min=current_interval,
        consecutive_no_change=consecutive_no_change,
        last_change_at=watch.get("last_change_at"),
        skip_reason=None,
        last_error=watch.get("last_error"),
        last_error_at=watch.get("last_error_at"),
    )


async def _watcher_supervisor():
    # Watcher = adaptive per-playlist monitoring that triggers runs when changes are detected.
    logging.info("Watcher started")
    _set_watcher_status("idle")
    startup_logged = False
    last_candidate_state = None
    batch_state = {
        "pending_playlists": set(),
        "last_detection_ts": None,
        "batch_active": False,
    }
    while True:
        if getattr(app.state, "watcher_lock", None) is None:
            _set_watcher_status("disabled", batch_active=False, pending_playlists_count=0, quiet_window_remaining_sec=None)
            return
        config = _read_config_for_watcher()
        if not config:
            _set_watcher_status("idle", next_poll_ts=None, pending_playlists_count=0, quiet_window_remaining_sec=None, batch_active=False)
            await asyncio.sleep(60)
            continue
        if _playlist_imports_active():
            _set_watcher_status(
                "paused_import",
                next_poll_ts=None,
                pending_playlists_count=0,
                quiet_window_remaining_sec=None,
                batch_active=False,
            )
            await asyncio.sleep(10)
            continue

        policy = normalize_watch_policy(config)
        downtime = policy.get("downtime") or {}
        local_now = datetime.now().astimezone()
        tzinfo = _resolve_timezone(downtime.get("timezone"), local_now.tzinfo)
        now = local_now.astimezone(tzinfo)

        downtime_active = False
        if downtime.get("enabled"):
            downtime_active, _next_allowed = in_downtime(
                now,
                downtime.get("start"),
                downtime.get("end"),
            )
            if downtime_active and not app.state.was_in_downtime:
                logging.info("Watcher entering downtime window")
            if not downtime_active and app.state.was_in_downtime:
                logging.info("Watcher exiting downtime window")
            if downtime_active:
                app.state.was_in_downtime = True

        playlists = config.get("playlists") or []
        if not playlists:
            if last_candidate_state != "no_playlists":
                logging.info("Watcher: no candidates (playlists=0)")
                last_candidate_state = "no_playlists"
            if not startup_logged:
                logging.info(
                    "Watcher startup diag: playlists=0 downtime_enabled=%s downtime_active=%s "
                    "timezone=%s next_poll_at=%s delta_sec=%s lock=%s",
                    bool(downtime.get("enabled")),
                    bool(downtime_active),
                    downtime.get("timezone") or "local",
                    None,
                    None,
                    bool(getattr(app.state, "watcher_lock", None)),
                )
                startup_logged = True
            _set_watcher_status("idle", next_poll_ts=None, pending_playlists_count=0, quiet_window_remaining_sec=None, batch_active=False)
            await asyncio.sleep(60)
            continue
        playlist_map = {}
        for pl in playlists:
            playlist_key = pl.get("playlist_id") or pl.get("id")
            if not playlist_key:
                continue
            playlist_id = extract_playlist_id(playlist_key) or playlist_key
            playlist_map[playlist_id] = pl

        pending_count = len(batch_state["pending_playlists"])
        quiet_remaining = None
        if batch_state["last_detection_ts"] is not None and not batch_state["batch_active"]:
            quiet_remaining = max(
                0,
                int(WATCHER_QUIET_WINDOW_SECONDS - (time.monotonic() - batch_state["last_detection_ts"])),
            )
        if (pending_count and batch_state["last_detection_ts"] is not None
                and not batch_state["batch_active"]):
            elapsed = time.monotonic() - batch_state["last_detection_ts"]
            if elapsed >= WATCHER_QUIET_WINDOW_SECONDS:
                _set_watcher_status(
                    "batch_ready",
                    pending_playlists_count=pending_count,
                    quiet_window_remaining_sec=0,
                    batch_active=False,
                )
                logging.info(
                    "Watcher: quiet window elapsed (%ss), preparing batch run",
                    WATCHER_QUIET_WINDOW_SECONDS,
                )
                batch_state["batch_active"] = True
                batch_playlists = list(batch_state["pending_playlists"])
                batch_state["pending_playlists"].clear()
                _set_watcher_status(
                    "running_batch",
                    pending_playlists_count=len(batch_playlists),
                    quiet_window_remaining_sec=None,
                    batch_active=True,
                )
                logging.info(
                    "Watcher: starting batch run playlists=%s",
                    ",".join(batch_playlists),
                )
                batch_start = time.monotonic()
                total_downloaded = 0
                for playlist_id in batch_playlists:
                    pl = playlist_map.get(playlist_id)
                    if not pl:
                        logging.warning("Watcher: batch playlist missing config playlist_id=%s", playlist_id)
                        continue
                    logging.info("Watcher: batch downloading playlist_id=%s", playlist_id)
                    result, _next_allowed = await _start_run_with_config(
                        config,
                        playlist_id=playlist_id,
                        playlist_account=pl.get("account"),
                        playlist_mode=(pl.get("mode") or "full"),
                        destination=pl.get("folder") or pl.get("directory"),
                        final_format_override=pl.get("final_format"),
                        music_mode=bool(pl.get("music_mode")),
                        run_source="watcher",
                        now=now,
                    )
                    if result == "started":
                        if app.state.run_task:
                            await app.state.run_task
                        status = app.state.status
                        if status:
                            total_downloaded += len(status.run_successes or [])
                    elif result == "deferred":
                        logging.info("Watcher: batch deferred playlist_id=%s", playlist_id)
                    else:
                        logging.debug("Watcher: batch skipped (run active) playlist_id=%s", playlist_id)
                duration_seconds = max(0, int(time.monotonic() - batch_start))
                logging.info(
                    "Watcher: batch complete playlists=%s videos=%s",
                    len(batch_playlists),
                    total_downloaded,
                )
                if batch_playlists:
                    minutes = duration_seconds // 60
                    seconds = duration_seconds % 60
                    duration_label = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"
                    msg = (
                        "Retreivr Watcher Batch\n"
                        f"Playlists: {len(batch_playlists)}\n"
                        f"Videos downloaded: {total_downloaded}\n"
                        f"Duration: {duration_label}"
                    )
                    telegram_notify(config, msg)
                batch_state["batch_active"] = False
                batch_state["last_detection_ts"] = None
                _set_watcher_status("idle", pending_playlists_count=0, quiet_window_remaining_sec=None, batch_active=False)
                logging.info("Watcher: batch state reset, resuming monitoring")
                continue
            _set_watcher_status(
                "waiting_quiet_window",
                pending_playlists_count=pending_count,
                quiet_window_remaining_sec=quiet_remaining,
                batch_active=False,
            )
        else:
            _set_watcher_status("idle", pending_playlists_count=pending_count, quiet_window_remaining_sec=None, batch_active=batch_state["batch_active"])

        if downtime.get("enabled") and downtime_active:
            await asyncio.sleep(60)
            continue

        watch_state = _read_watch_state(app.state.paths.db_path)
        if app.state.was_in_downtime:
            app.state.was_in_downtime = False
            for pl in playlists:
                playlist_key = pl.get("playlist_id") or pl.get("id")
                if not playlist_key:
                    continue
                playlist_id = extract_playlist_id(playlist_key) or playlist_key
                watch = watch_state.get(playlist_id) or {}
                consecutive_no_change = watch.get("consecutive_no_change") or 0
                current_interval = watch.get("current_interval_min")
                if not isinstance(current_interval, int):
                    current_interval = policy.get("min_interval_minutes") or 5
                _write_watch_state(
                    app.state.paths.db_path,
                    playlist_id,
                    last_checked_at=watch.get("last_checked_at"),
                    next_poll_at=_format_iso(now),
                    idle_count=consecutive_no_change,
                    current_interval_min=current_interval,
                    consecutive_no_change=consecutive_no_change,
                    last_change_at=watch.get("last_change_at"),
                    skip_reason=watch.get("skip_reason"),
                    last_error=watch.get("last_error"),
                    last_error_at=watch.get("last_error_at"),
                )
            watch_state = _read_watch_state(app.state.paths.db_path)

        candidates = []
        for pl in playlists:
            playlist_id = extract_playlist_id(pl.get("playlist_id") or pl.get("id")) or pl.get("playlist_id") or pl.get("id")
            watch = watch_state.get(playlist_id) or {}
            next_poll_at = _parse_iso(watch.get("next_poll_at")) or now
            candidates.append((next_poll_at, pl, watch))

        if not candidates:
            if last_candidate_state != "no_watch_state":
                logging.info("Watcher: no candidates (watch_state=0)")
                last_candidate_state = "no_watch_state"
            if not startup_logged:
                logging.info(
                    "Watcher startup diag: playlists=%s downtime_enabled=%s downtime_active=%s "
                    "timezone=%s next_poll_at=%s delta_sec=%s lock=%s",
                    len(playlists),
                    bool(downtime.get("enabled")),
                    bool(downtime_active),
                    downtime.get("timezone") or "local",
                    None,
                    None,
                    bool(getattr(app.state, "watcher_lock", None)),
                )
                startup_logged = True
            await asyncio.sleep(60)
            continue

        candidates.sort(key=lambda item: item[0])
        next_poll_at, pl, watch = candidates[0]
        if last_candidate_state != "available":
            logging.info("Watcher: candidates available")
            last_candidate_state = "available"
        delta_seconds = int((next_poll_at - now).total_seconds())
        if not startup_logged:
            logging.info(
                "Watcher startup diag: playlists=%s downtime_enabled=%s downtime_active=%s "
                "timezone=%s next_poll_at=%s delta_sec=%s lock=%s",
                len(playlists),
                bool(downtime.get("enabled")),
                bool(downtime_active),
                downtime.get("timezone") or "local",
                _format_iso(next_poll_at),
                delta_seconds,
                bool(getattr(app.state, "watcher_lock", None)),
            )
            startup_logged = True
        min_interval_minutes = policy.get("min_interval_minutes") or 5
        interval_seconds = max(60, int(min_interval_minutes * 60))
        max_sleep_seconds = max(interval_seconds * 3, 900)
        if delta_seconds > max_sleep_seconds:
            clamped = now + timedelta(seconds=min(30, interval_seconds))
            logging.warning(
                "Watcher: next_poll_at skew detected; clamping from %s to %s (delta=%s)",
                _format_iso(next_poll_at),
                _format_iso(clamped),
                delta_seconds,
            )
            playlist_key = pl.get("playlist_id") or pl.get("id")
            playlist_id = extract_playlist_id(playlist_key) or playlist_key
            consecutive_no_change = watch.get("consecutive_no_change") or 0
            current_interval = watch.get("current_interval_min")
            if not isinstance(current_interval, int):
                current_interval = min_interval_minutes
            _write_watch_state(
                app.state.paths.db_path,
                playlist_id,
                last_checked_at=watch.get("last_checked_at"),
                next_poll_at=_format_iso(clamped),
                idle_count=watch.get("idle_count") or 0,
                current_interval_min=current_interval,
                consecutive_no_change=consecutive_no_change,
                last_change_at=watch.get("last_change_at"),
                skip_reason=watch.get("skip_reason"),
                last_error=watch.get("last_error"),
                last_error_at=watch.get("last_error_at"),
            )
            next_poll_at = clamped
        _set_watcher_status(
            state=getattr(app.state, "watcher_status", {}).get("state") or "idle",
            next_poll_ts=_format_iso(next_poll_at),
        )
        if now < next_poll_at:
            sleep_seconds = max(0.0, (next_poll_at - now).total_seconds())
            if (batch_state["pending_playlists"] and batch_state["last_detection_ts"] is not None
                    and not batch_state["batch_active"]):
                quiet_remaining = WATCHER_QUIET_WINDOW_SECONDS - (time.monotonic() - batch_state["last_detection_ts"])
                if quiet_remaining > 0:
                    sleep_seconds = min(sleep_seconds, quiet_remaining)
                else:
                    sleep_seconds = 0
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
            continue

        accounts = config.get("accounts") or {}
        refresh_log_state = set()
        yt_clients = (
            build_youtube_clients(
                accounts,
                config,
                cache=getattr(app.state, "watcher_clients_cache", {}),
                refresh_log_state=refresh_log_state,
            )
            if accounts
            else {}
        )
        pending_before = len(batch_state["pending_playlists"])
        _set_watcher_status(
            "polling",
            last_poll_ts=_format_iso(now),
            pending_playlists_count=pending_before,
            quiet_window_remaining_sec=None,
            batch_active=batch_state["batch_active"],
        )
        await _poll_single_playlist(config, now, policy, pl, watch, yt_clients, batch_state)
        pending_after = len(batch_state["pending_playlists"])
        if pending_after > pending_before:
            logging.info("Watcher poll complete: updates detected")
        else:
            logging.info("Watcher poll complete: no updates detected")
        if batch_state["last_detection_ts"] is not None and not batch_state["batch_active"]:
            quiet_remaining = max(
                0,
                int(WATCHER_QUIET_WINDOW_SECONDS - (time.monotonic() - batch_state["last_detection_ts"])),
            )
            _set_watcher_status(
                "waiting_quiet_window",
                pending_playlists_count=pending_after,
                quiet_window_remaining_sec=quiet_remaining,
                batch_active=False,
            )
        else:
            _set_watcher_status(
                "idle",
                pending_playlists_count=pending_after,
                quiet_window_remaining_sec=None,
                batch_active=batch_state["batch_active"],
            )


def _apply_watch_policy(policy):
    if getattr(app.state, "watcher_lock", None) is None:
        return
    # Supervisor loop reads watch_policy each iteration.
    app.state.watch_policy = policy


def _set_watcher_status(state=None, **fields):
    status = getattr(app.state, "watcher_status", None)
    if status is None:
        status = {}
        app.state.watcher_status = status
    prev_state = status.get("state")
    if state and state != prev_state:
        status["state"] = state
        if state == "polling":
            logging.info("Watcher state → polling")
        elif state == "waiting_quiet_window":
            remaining = fields.get("quiet_window_remaining_sec")
            if remaining is None:
                remaining = WATCHER_QUIET_WINDOW_SECONDS
            logging.info("Watcher state → waiting_quiet_window (%ss)", remaining)
        elif state == "batch_ready":
            logging.info("Watcher state → batch_ready")
        elif state == "running_batch":
            count = fields.get("pending_playlists_count")
            if count is None:
                count = 0
            logging.info("Watcher state → running_batch playlists=%s", count)
        elif state == "disabled":
            logging.info("Watcher state → disabled")
        else:
            logging.info("Watcher state → %s", state)
    for key, value in fields.items():
        status[key] = value


@app.get("/api/status")
async def api_status():
    status = get_status(app.state.status)
    last_path = status.pop("last_completed_path", None)
    status["last_completed_file_id"] = _file_id_from_path(last_path) if last_path else None
    watcher_errors = []
    try:
        watch_state = _read_watch_state(app.state.paths.db_path)
        for playlist_id, entry in watch_state.items():
            last_error = entry.get("last_error")
            if last_error:
                watcher_errors.append({
                    "playlist_id": playlist_id,
                    "last_error": last_error,
                    "last_error_at": entry.get("last_error_at"),
                })
    except Exception:
        logging.exception("Failed to read watcher error state")
    watcher_policy = getattr(app.state, "watch_policy", _default_watch_policy())
    watcher_downtime = watcher_policy.get("downtime") or {}
    local_now = datetime.now().astimezone()
    tzinfo = _resolve_timezone(watcher_downtime.get("timezone"), local_now.tzinfo)
    now = local_now.astimezone(tzinfo)
    paused = False
    if watcher_downtime.get("enabled"):
        paused, _ = in_downtime(now, watcher_downtime.get("start"), watcher_downtime.get("end"))
    watcher_status = dict(getattr(app.state, "watcher_status", {}) or {})
    if not bool(getattr(app.state, "watcher_lock", None)):
        watcher_status["state"] = "disabled"
    playlist_import = _get_playlist_import_snapshot()
    queue_status = _get_download_queue_snapshot()
    return safe_json({
        "schema_version": STATUS_SCHEMA_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "state": app.state.state,
        "running": app.state.running,
        "run_id": app.state.run_id,
        "started_at": app.state.started_at,
        "finished_at": app.state.finished_at,
        "error": app.state.last_error,
        "watcher": {
            "enabled": bool(getattr(app.state, "watcher_lock", None)),
            "paused": bool(paused),
        },
        "watcher_status": watcher_status,
        "scheduler": {
            "enabled": bool((app.state.schedule_config or {}).get("enabled", False)),
        },
        "queue": queue_status,
        "playlist_import": playlist_import,
        "watcher_errors": watcher_errors,
        "status": status,
    })


@app.get("/api/schedule")
async def api_get_schedule():
    return _schedule_response()


@app.post("/api/schedule")
async def api_update_schedule(payload: ScheduleRequest):
    config = _read_config_or_404()
    current = _merge_schedule_config(config.get("schedule"))
    updates = payload.dict(exclude_unset=True)
    current.update(updates)
    errors = _validate_schedule_config(current)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    config["schedule"] = current

    config_path = app.state.config_path
    config_dir = os.path.dirname(config_path) or "."
    os.makedirs(config_dir, exist_ok=True)

    tmp = tempfile.NamedTemporaryFile("w", delete=False, dir=config_dir)
    try:
        safe_json_dump(config, tmp, indent=4)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp.name, config_path)
    finally:
        try:
            tmp.close()
        except Exception:
            pass
        if os.path.exists(tmp.name):
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    app.state.schedule_config = current
    _apply_schedule_config(current)
    _apply_spotify_schedule(config or {})
    return _schedule_response()


@app.get("/api/metrics")
async def api_metrics():
    files_count, bytes_count = _downloads_metrics(DOWNLOADS_DIR)
    disk = _disk_usage(DOWNLOADS_DIR)
    return {
        "schema_version": METRICS_SCHEMA_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
        "downloads_dir": DOWNLOADS_DIR,
        "downloads_files": files_count,
        "downloads_bytes": bytes_count,
        "disk_total_bytes": disk["total_bytes"],
        "disk_free_bytes": disk["free_bytes"],
        "disk_used_bytes": disk["used_bytes"],
        "disk_free_percent": disk["free_percent"],
    }


@app.get("/api/version")
async def api_version():
    return get_runtime_info()


@app.post("/api/yt-dlp/update")
async def api_update_ytdlp():
    script_path = _yt_dlp_script_path()
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="update_yt_dlp.sh not found")

    with app.state.ytdlp_update_lock:
        if app.state.ytdlp_update_running:
            raise HTTPException(status_code=409, detail="yt-dlp update already running")
        app.state.ytdlp_update_running = True

    def _run_update():
        try:
            logging.info("yt-dlp update started")
            subprocess.run(["bash", script_path], check=False)
            logging.info("yt-dlp update finished")
        finally:
            app.state.ytdlp_update_running = False

    asyncio.create_task(anyio.to_thread.run_sync(_run_update))
    return {"status": "started"}


@app.get("/api/paths")
async def api_paths():
    return {
        "config_dir": CONFIG_DIR,
        "data_dir": DATA_DIR,
        "downloads_dir": DOWNLOADS_DIR,
        "log_dir": LOG_DIR,
        "tokens_dir": TOKENS_DIR,
        "browse_roots": app.state.browse_roots,
    }


@app.post("/api/oauth/start")
async def api_oauth_start(payload: OAuthStartRequest):
    _purge_oauth_sessions()
    try:
        client_secret_file = resolve_dir(payload.client_secret, TOKENS_DIR)
        token_file = resolve_dir(payload.token_out, TOKENS_DIR)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ensure_dir(TOKENS_DIR)
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, OAUTH_SCOPES)
    flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
    auth_url, _ = flow.authorization_url(access_type="offline", prompt="consent")
    session_id = uuid4().hex
    with _OAUTH_LOCK:
        _OAUTH_SESSIONS[session_id] = {
            "flow": flow,
            "token_file": token_file,
            "account": payload.account or "",
            "expires_at": datetime.now(timezone.utc) + OAUTH_SESSION_TTL,
        }
    return {"session_id": session_id, "auth_url": auth_url}


@app.post("/api/oauth/complete")
async def api_oauth_complete(payload: OAuthCompleteRequest):
    _purge_oauth_sessions()
    with _OAUTH_LOCK:
        session = _OAUTH_SESSIONS.pop(payload.session_id, None)
    if not session:
        raise HTTPException(status_code=404, detail="OAuth session not found or expired")
    code = (payload.code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="Authorization code is required")
    flow = session["flow"]
    try:
        flow.fetch_token(code=code)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"OAuth failed: {exc}") from exc
    creds = flow.credentials
    token_file = session["token_file"]
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    ensure_dir(os.path.dirname(token_file))
    with open(token_file, "w") as f:
        safe_json_dump(token_data, f, indent=2)
    account = session.get("account") or "unknown"
    logging.info("OAuth token saved for account %s to %s", account, token_file)
    return {"status": "ok", "token_path": token_file}


@app.get("/api/config/path")
async def api_get_config_path():
    return {"path": app.state.config_path}


@app.put("/api/config/path")
async def api_put_config_path(payload: ConfigPathRequest):
    path = payload.path.strip()
    if not path:
        raise HTTPException(status_code=400, detail="Config path is required")
    try:
        target = resolve_config_path(path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail=f"Config not found: {target}")
    try:
        config = load_config(target)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in config: {exc}") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read config: {exc}") from exc
    errors = validate_config(config)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    app.state.config_path = target
    normalized = safe_json(_strip_deprecated_fields(config))
    app.state.loaded_config = normalized if isinstance(normalized, dict) else {}
    app.state.config = app.state.loaded_config
    return {"path": app.state.config_path}


@app.post("/api/run", status_code=202)
async def api_run(request: RunRequest):
    config = get_loaded_config() or _read_config_or_404()
    if request.single_url and request.playlist_id:
        raise HTTPException(status_code=400, detail="Provide either single_url or playlist_id, not both")
    if request.single_url and _looks_like_playlist_url(request.single_url):
        raise HTTPException(status_code=400, detail=DIRECT_URL_PLAYLIST_ERROR)
    if request.playlist_account:
        accounts = (config.get("accounts") or {}) if isinstance(config, dict) else {}
        if request.playlist_account not in accounts:
            raise HTTPException(status_code=400, detail="playlist_account not found in config")
    logging.info(
        "Manual run requested (source=api) single_url=%s playlist_id=%s",
        bool(request.single_url),
        request.playlist_id or "-",
    )
    if request.single_url:
        logging.info(
            json.dumps(
                safe_json(
                    {
                        "message": "direct_url_request_received",
                        "music_mode": bool(request.music_mode),
                        "url": request.single_url,
                    }
                ),
                sort_keys=True,
            )
        )
    result, _next_allowed = await _start_run_with_config(
        config,
        single_url=request.single_url,
        playlist_id=request.playlist_id,
        playlist_account=request.playlist_account,
        destination=request.destination,
        final_format_override=request.final_format_override,
        js_runtime=request.js_runtime,
        music_mode=request.music_mode,
        run_source="api",
        skip_downtime=bool(request.single_url),
        delivery_mode=request.delivery_mode,
    )
    if result == "busy":
        raise HTTPException(status_code=409, detail="Archive run already in progress")
    return {"run_id": app.state.run_id, "status": "started"}


@app.post("/api/direct_url_preview")
async def api_direct_url_preview(request: DirectUrlPreviewRequest):
    url = request.url.strip() if request.url else ""
    if not url:
        raise HTTPException(status_code=400, detail="URL is required")
    if _looks_like_playlist_url(url):
        raise HTTPException(status_code=400, detail=DIRECT_URL_PLAYLIST_ERROR)
    if not _is_http_url(url):
        raise HTTPException(
            status_code=400,
            detail="This search result is not directly downloadable."
        )
    try:
        normalize_search_payload(
            {"query": url, "search_only": True},
            default_sources=list(getattr(app.state.search_service, "adapters", {}).keys()),
        )
    except ValueError:
        pass
    runtime_config = get_loaded_config() or _read_config_or_404()
    if not runtime_config:
        raise RuntimeError("search_missing_runtime_config")
    logging.info(
        json.dumps(
            safe_json(
                {
                    "message": "search_effective_config",
                    "keys_count": len(runtime_config.keys()) if isinstance(runtime_config, dict) else 0,
                    "has_js_runtime": bool(isinstance(runtime_config, dict) and "js_runtime" in runtime_config),
                }
            ),
            sort_keys=True,
        )
    )
    try:
        preview = preview_direct_url(url, runtime_config)
    except Exception as exc:
        logging.exception("Direct URL preview failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return safe_json({"preview": preview})


@app.post("/api/cancel")
async def api_cancel():
    logging.info("Cancel requested by user")
    store = DownloadJobStore(app.state.paths.db_path)
    store.cancel_active_jobs(reason="cancel_requested")
    if not app.state.running:
        return {"status": "idle"}
    app.state.cancel_requested = True
    app.state.stop_event.set()
    status = app.state.status
    if status:
        lock = getattr(status, "lock", None)
        if lock:
            with lock:
                status.last_error_message = "Downloads cancelled by user"
        else:
            status.last_error_message = "Downloads cancelled by user"
    return {"status": "cancelling"}


@app.get("/api/logs", response_class=PlainTextResponse)
async def api_logs(lines: int = Query(200, ge=1, le=5000)):
    return _tail_lines(app.state.log_path, lines)


@app.post("/api/search/requests")
async def create_search_request(request: dict = Body(...)):
    service = app.state.search_service
    raw_payload = request if isinstance(request, dict) else {}
    enabled_sources = list(getattr(service, "adapters", {}).keys())
    try:
        normalized = normalize_search_payload(raw_payload, default_sources=enabled_sources)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if bool(normalized.get("music_mode")):
        raise HTTPException(
            status_code=400,
            detail="music_mode_requests_must_use_api_music_search",
        )
    logging.debug(
        "Home search: music_mode=%s query=%s",
        bool(normalized.get("music_mode")),
        str(normalized.get("query") or ""),
    )
    if normalized["delivery_mode"] == "client" and normalized["destination_path"]:
        raise HTTPException(status_code=400, detail="Client delivery does not use a server destination")
    if normalized["delivery_mode"] == "client" and not normalized["search_only"]:
        raise HTTPException(status_code=400, detail="Search & Download is not available for client delivery")

    intent = detect_intent(str(normalized.get("query") or ""))
    if intent.type != IntentType.SEARCH:
        return {
            "detected_intent": intent.type.value,
            "identifier": intent.identifier,
            "music_mode": bool(normalized["music_mode"]),
            "music_candidates": [],
            "music_resolution": None,
        }

    if "source_priority" not in raw_payload or not raw_payload.get("source_priority"):
        raw_payload["source_priority"] = normalized["sources"]
    if "auto_enqueue" not in raw_payload:
        raw_payload["auto_enqueue"] = not normalized["search_only"]
    if "media_type" not in raw_payload:
        raw_payload["media_type"] = "music" if normalized["music_mode"] else "generic"
    if "music_mode" not in raw_payload:
        raw_payload["music_mode"] = normalized["music_mode"]
    if "destination_dir" not in raw_payload and normalized["destination"] is not None:
        raw_payload["destination_dir"] = normalized["destination"]

    allowed_keys = {
        "created_by",
        "intent",
        "media_type",
        "artist",
        "album",
        "track",
        "destination_dir",
        "include_albums",
        "include_singles",
        "min_match_score",
        "duration_hint_sec",
        "quality_min_bitrate_kbps",
        "lossless_only",
        "music_mode",
        "auto_enqueue",
        "source_priority",
        "max_candidates_per_source",
    }
    request_payload = {key: raw_payload.get(key) for key in allowed_keys if key in raw_payload}

    try:
        parsed = SearchRequestPayload(**request_payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload = parsed.model_dump() if hasattr(parsed, "model_dump") else parsed.dict()
    try:
        request_id = service.create_search_request(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if hasattr(app.state, "search_request_overrides"):
        app.state.search_request_overrides[request_id] = {
            "final_format": normalized["final_format"],
            "delivery_mode": normalized["delivery_mode"],
            "destination_type": normalized["destination_type"],
            "destination_path": normalized["destination_path"],
        }
    logging.debug("Normalized search payload", extra={"payload": normalized, "request_id": request_id})
    return {
        "request_id": request_id,
        "music_mode": bool(normalized["music_mode"]),
        "music_candidates": [],
        "music_resolution": None,
    }


@app.post("/api/import/playlist")
async def import_playlist(file: UploadFile = File(...)):
    filename = str(getattr(file, "filename", "") or "").strip()
    ext = Path(filename).suffix.lower()
    if ext not in SUPPORTED_IMPORT_EXTENSIONS:
        raise HTTPException(status_code=400, detail="unsupported_file_extension")

    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="empty_file")
    if len(payload) > MAX_IMPORT_FILE_BYTES:
        raise HTTPException(status_code=400, detail="file_too_large")

    job_id = uuid4().hex
    now_iso = datetime.now(timezone.utc).isoformat()
    status_entry = {
        "job_id": job_id,
        "filename": filename,
        "state": "queued",
        "phase": "queued",
        "message": "Playlist import queued.",
        "total_tracks": 0,
        "processed_tracks": 0,
        "resolved": 0,
        "unresolved": 0,
        "enqueued": 0,
        "failed": 0,
        "import_batch_id": "",
        "error": None,
        "started_at": now_iso,
        "finished_at": None,
        "updated_at": now_iso,
        "updated_ts": time.time(),
    }
    lock = getattr(app.state, "playlist_import_jobs_lock", None)
    jobs = getattr(app.state, "playlist_import_jobs", None)
    if lock is None or not isinstance(jobs, dict):
        raise HTTPException(status_code=503, detail="import_state_unavailable")
    with lock:
        jobs[job_id] = status_entry
        app.state.playlist_import_active_count = _playlist_imports_active_count() + 1
        _trim_playlist_import_jobs_locked()

    thread = threading.Thread(
        target=_run_playlist_import_job,
        args=(job_id, filename, payload),
        name=f"playlist-import-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    return JSONResponse(status_code=202, content={"job_id": job_id, "status": status_entry})


@app.get("/api/import/playlist/jobs/{job_id}")
async def get_import_playlist_job(job_id: str):
    normalized = str(job_id or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="job_id_required")
    status_entry = _get_playlist_import_job(normalized)
    if not status_entry:
        raise HTTPException(status_code=404, detail="import_job_not_found")
    status_entry["active"] = bool(status_entry.get("state") in {"queued", "parsing", "resolving"})
    return {"job_id": normalized, "status": status_entry}


@app.post("/api/import/playlist/{batch_id}/finalize")
async def finalize_import_playlist(batch_id: str, payload: dict = Body(default=None)):
    import_batch_id = str(batch_id or "").strip()
    if not import_batch_id:
        raise HTTPException(status_code=400, detail="import_batch_id_required")
    playlist_name = str((payload or {}).get("playlist_name") or import_batch_id).strip() or import_batch_id
    try:
        entries_written = write_import_m3u_from_batch(
            import_batch_id=import_batch_id,
            playlist_name=playlist_name,
            db_path=app.state.paths.db_path,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"import_finalize_failed: {exc}") from exc
    return {
        "import_batch_id": import_batch_id,
        "playlist_name": playlist_name,
        "entries_written": int(entries_written),
    }


@app.post("/api/intent/execute")
async def execute_intent(payload: dict = Body(...)):
    """Execute intent requests by routing to the active ingestion pipeline."""
    intent_raw = str((payload or {}).get("intent_type") or "").strip()
    identifier = str((payload or {}).get("identifier") or "").strip()
    if not intent_raw:
        raise HTTPException(status_code=400, detail="intent_type is required")
    if not identifier:
        raise HTTPException(status_code=400, detail="identifier is required")
    try:
        intent_type = IntentType(intent_raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid intent_type") from exc
    config = _read_config_or_404()
    dispatcher_config = dict(config)
    dispatcher_config["search_service"] = app.state.search_service
    db = PlaylistSnapshotStore(app.state.paths.db_path)
    queue = _IntentQueueAdapter()
    spotify_client = _build_spotify_client_with_optional_oauth(config)
    return await dispatch_intent(
        intent_type=intent_type.value,
        identifier=identifier,
        config=dispatcher_config,
        db=db,
        queue=queue,
        spotify_client=spotify_client,
    )


@app.post("/api/intent/preview")
async def preview_intent(payload: dict = Body(...)):
    """Fetch metadata preview for supported intents (plumbing only)."""
    intent_raw = str((payload or {}).get("intent_type") or "").strip()
    identifier = str((payload or {}).get("identifier") or "").strip()
    if not intent_raw:
        raise HTTPException(status_code=400, detail="intent_type is required")
    if not identifier:
        raise HTTPException(status_code=400, detail="identifier is required")
    try:
        intent_type = IntentType(intent_raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid intent_type") from exc

    if intent_type not in {IntentType.SPOTIFY_ALBUM, IntentType.SPOTIFY_PLAYLIST}:
        raise HTTPException(status_code=400, detail="intent preview not supported for this intent_type")

    config = _read_config_or_404()
    client = _build_spotify_client_with_optional_oauth(config)
    encoded = quote(identifier, safe="")
    try:
        if intent_type == IntentType.SPOTIFY_ALBUM:
            data = client._request_json(
                f"https://api.spotify.com/v1/albums/{encoded}",
                params={"fields": "name,artists(name),total_tracks"},
            )
            artists = data.get("artists") or []
            artist = artists[0].get("name") if artists and isinstance(artists[0], dict) else ""
            track_count = int(data.get("total_tracks") or 0)
            return {
                "intent_type": intent_type.value,
                "identifier": identifier,
                "title": str(data.get("name") or ""),
                "artist": str(artist or ""),
                "track_count": track_count,
            }

        data = client._request_json(
            f"https://api.spotify.com/v1/playlists/{encoded}",
            params={"fields": "name,owner(display_name),tracks(total)"},
        )
        owner = (data.get("owner") or {}).get("display_name")
        track_count = int(((data.get("tracks") or {}).get("total")) or 0)
        return {
            "intent_type": intent_type.value,
            "identifier": identifier,
            "title": str(data.get("name") or ""),
            "artist": str(owner or ""),
            "track_count": track_count,
        }
    except Exception as exc:
        logging.exception("Intent preview failed for intent=%s identifier=%s", intent_type.value, identifier)
        raise HTTPException(status_code=502, detail=f"intent preview failed: {exc}") from exc


@app.get("/api/search/requests")
async def list_search_requests(status: str | None = None, limit: int | None = None):
    try:
        store = SearchJobStore(app.state.search_db_path)
        requests = store.list_requests(status=status, limit=limit)
    except Exception:
        logging.exception("Failed to list search requests")
        raise HTTPException(status_code=500, detail="Failed to load search requests")
    return _sanitize_non_http_urls({"requests": requests})


@app.get("/api/search/requests/{request_id}")
async def get_search_request(request_id: str):
    service = app.state.search_service
    result = service.get_search_request(request_id)
    if not result:
        raise HTTPException(status_code=404, detail="Search request not found")
    return _sanitize_non_http_urls(result)


@app.post("/api/search/requests/{request_id}/cancel")
async def cancel_search_request(request_id: str):
    store = SearchJobStore(app.state.search_db_path)
    store.mark_request_cancelled(request_id)
    return {"ok": True, "request_id": request_id, "status": "cancelled"}


@app.post("/api/search/resolve/once")
async def run_search_resolution_once():
    service = app.state.search_service
    request_id = service.run_search_resolution_once()
    return {"request_id": request_id}


@app.post("/api/music/album/download")
def download_full_album(data: dict):
    release_group_mbid = str((data or {}).get("release_group_mbid") or "").strip()
    if not release_group_mbid:
        raise HTTPException(status_code=400, detail="release_group_mbid required")
    force_redownload = bool((data or {}).get("force_redownload"))

    cfg = _read_config_or_404()
    threshold_raw = (cfg or {}).get("music_mb_binding_threshold", 0.78)
    try:
        binding_threshold = float(threshold_raw)
    except (TypeError, ValueError):
        binding_threshold = 0.78
    if binding_threshold > 1.0:
        binding_threshold = binding_threshold / 100.0
    binding_threshold = max(0.0, min(1.0, binding_threshold))

    mb = _mb_service()
    try:
        release_group_payload = mb._call_with_retry(  # noqa: SLF001
            lambda: musicbrainzngs.get_release_group_by_id(
                release_group_mbid,
                # release-group does not accept "genres" include in musicbrainzngs.
                includes=["releases", "tags"],
            )
        )
    except Exception:
        logger.exception("[MUSIC] release_group fetch failed release_group=%s", release_group_mbid)
        raise HTTPException(status_code=502, detail="musicbrainz_release_group_fetch_failed")

    release_group = release_group_payload.get("release-group", {}) if isinstance(release_group_payload, dict) else {}
    releases = release_group.get("release-list", []) if isinstance(release_group, dict) else []
    release_dicts = [rel for rel in releases if isinstance(rel, dict)]
    if not release_dicts:
        raise HTTPException(status_code=404, detail="musicbrainz_release_group_has_no_releases")

    official = [rel for rel in release_dicts if str(rel.get("status") or "").strip().lower() == "official"]
    us_official = [rel for rel in official if str(rel.get("country") or "").strip().upper() == "US"]
    if us_official:
        selected_release = us_official[0]
    elif official:
        selected_release = official[0]
    else:
        selected_release = release_dicts[0]
    release_mbid = str(selected_release.get("id") or "").strip()
    if not release_mbid:
        raise HTTPException(status_code=502, detail="musicbrainz_release_selection_failed")

    try:
        release_payload = mb._call_with_retry(  # noqa: SLF001
            lambda: musicbrainzngs.get_release_by_id(
                release_mbid,
                # Keep include list contract-safe; use tags fallback for genre best-effort.
                includes=["recordings", "media", "artists", "tags"],
            )
        )
    except Exception:
        logger.exception("[MUSIC] release fetch failed release=%s", release_mbid)
        raise HTTPException(status_code=502, detail="musicbrainz_release_fetch_failed")

    release_obj = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
    release_title = str(release_obj.get("title") or "").strip() or None
    release_date = str(release_obj.get("date") or "").strip() or None
    release_artist_credit = release_obj.get("artist-credit", []) if isinstance(release_obj, dict) else []
    media = release_obj.get("medium-list", []) if isinstance(release_obj, dict) else []
    if not isinstance(media, list):
        media = []

    def _safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _best_mb_genre(entity):
        if not isinstance(entity, dict):
            return None
        genres = []
        genre_list = entity.get("genre-list")
        if isinstance(genre_list, list):
            for item in genre_list:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if name:
                    genres.append((_safe_int(item.get("count")), name))
        if genres:
            genres.sort(key=lambda entry: entry[0], reverse=True)
            return genres[0][1]
        tag_list = entity.get("tag-list")
        tags = []
        if isinstance(tag_list, list):
            for item in tag_list:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if name:
                    tags.append((_safe_int(item.get("count")), name))
        if tags:
            tags.sort(key=lambda entry: entry[0], reverse=True)
            return tags[0][1]
        return None

    def _credit_name(artist_credit):
        if not isinstance(artist_credit, list):
            return ""
        parts = []
        for item in artist_credit:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            artist_obj = item.get("artist") if isinstance(item.get("artist"), dict) else {}
            name = str(item.get("name") or artist_obj.get("name") or "").strip()
            joinphrase = str(item.get("joinphrase") or "")
            if name:
                parts.append(name)
            if joinphrase:
                parts.append(joinphrase)
        return "".join(parts).strip()

    def _optional_pos_int(value):
        if value is None or str(value).strip() == "":
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    album_artist = (
        _credit_name(release_artist_credit)
        or _credit_name(selected_release.get("artist-credit"))
        or ""
    )
    disc_total = _optional_pos_int(release_obj.get("medium-count")) or len(
        [item for item in media if isinstance(item, dict)]
    )
    if disc_total <= 0:
        disc_total = 1
    album_artwork_url = None
    try:
        album_artwork_url = mb.fetch_release_group_cover_art_url(release_group_mbid, timeout=8)
    except Exception:
        album_artwork_url = None
    if not album_artwork_url:
        try:
            album_artwork_url = mb.cover_art_url(release_mbid)
        except Exception:
            album_artwork_url = None
    album_genre = _best_mb_genre(release_obj) or _best_mb_genre(release_group)

    queue = _IntentQueueAdapter()
    engine = getattr(app.state, "worker_engine", None)
    store = getattr(engine, "store", None) if engine is not None else None
    tracks_enqueued = 0
    job_ids = []
    failed_tracks = []
    album_duration_delta_limit_ms = 25000

    for medium in media:
        if not isinstance(medium, dict):
            continue
        medium_pos = medium.get("position")
        try:
            disc_number = int(medium_pos) if medium_pos is not None else 1
        except (TypeError, ValueError):
            disc_number = 1
        tracks = medium.get("track-list", []) if isinstance(medium.get("track-list"), list) else []
        track_total = len([item for item in tracks if isinstance(item, dict)])
        for track in tracks:
            if not isinstance(track, dict):
                continue
            recording = track.get("recording") if isinstance(track.get("recording"), dict) else {}
            recording_mbid = str(recording.get("id") or "").strip()
            title = str(track.get("title") or recording.get("title") or "").strip()
            if not recording_mbid or not title:
                continue
            artist = _credit_name(recording.get("artist-credit")) or _credit_name(track.get("artist-credit")) or _credit_name(release_artist_credit)
            duration_ms = recording.get("length") or track.get("length")
            try:
                duration_ms_int = int(duration_ms) if duration_ms is not None else None
            except (TypeError, ValueError):
                duration_ms_int = None
            try:
                track_number = int(track.get("position")) if track.get("position") is not None else None
            except (TypeError, ValueError):
                track_number = None

            try:
                resolved = resolve_best_mb_pair(
                    mb,
                    artist=artist or None,
                    track=title,
                    album=release_title,
                    duration_ms=duration_ms_int,
                    threshold=binding_threshold,
                    max_duration_delta_ms=album_duration_delta_limit_ms,
                )
                if not resolved:
                    reasons = getattr(resolve_best_mb_pair, "last_failure_reasons", [])
                    reason_text = ",".join(reasons) if reasons else "mb_pair_not_resolved"
                    logger.info(
                        "[MUSIC] album track skipped recording=%s track=%s reasons=%s",
                        recording_mbid,
                        title,
                        reasons,
                    )
                    failed_tracks.append(
                        {
                            "recording_mbid": recording_mbid,
                            "track": title,
                            "reason": reason_text,
                        }
                    )
                    continue

                payload = {
                    "media_intent": "music_track",
                    "force_redownload": force_redownload,
                    "artist": resolved.get("artist") or artist,
                    "album": release_title,
                    "album_artist": album_artist or (resolved.get("album_artist") or resolved.get("artist") or artist),
                    "track": title,
                    "recording_mbid": resolved.get("recording_mbid") or recording_mbid,
                    "mb_recording_id": resolved.get("recording_mbid") or recording_mbid,
                    "track_number": track_number or resolved.get("track_number"),
                    "disc_number": disc_number or resolved.get("disc_number"),
                    "track_total": track_total or None,
                    "disc_total": disc_total,
                    "release_date": release_date or resolved.get("release_date"),
                    "mb_release_id": release_mbid,
                    "mb_release_group_id": release_group_mbid,
                    "release_id": release_mbid,
                    "release_group_id": release_group_mbid,
                    "artwork_url": album_artwork_url,
                    "genre": album_genre or (resolved.get("genre") if isinstance(resolved, dict) else None),
                    "duration_ms": resolved.get("duration_ms") or duration_ms_int,
                }
                canonical_id = _build_music_track_canonical_id(
                    payload.get("artist"),
                    payload.get("album"),
                    payload.get("track_number"),
                    payload.get("track"),
                    recording_mbid=payload.get("recording_mbid") or payload.get("mb_recording_id"),
                    mb_release_id=payload.get("mb_release_id") or payload.get("release_id"),
                    mb_release_group_id=payload.get("mb_release_group_id") or payload.get("release_group_id"),
                    disc_number=payload.get("disc_number"),
                )
                queue.enqueue(payload)
                tracks_enqueued += 1
                if store is not None:
                    existing = store.get_job_by_canonical_id(canonical_id)
                    if existing is not None and existing.id:
                        job_ids.append(existing.id)
            except Exception as exc:
                reason_text = str(exc) or "track_enqueue_failed"
                logger.warning(
                    "[MUSIC] album track enqueue failed recording=%s track=%s reason=%s",
                    recording_mbid,
                    title,
                    reason_text,
                )
                failed_tracks.append(
                    {
                        "recording_mbid": recording_mbid,
                        "track": title,
                        "reason": reason_text,
                    }
                )
                continue

    return {
        "status": "enqueued",
        "release_group_mbid": release_group_mbid,
        "release_mbid": release_mbid,
        "tracks_enqueued": tracks_enqueued,
        "failed_tracks_count": len(failed_tracks),
        "failed_tracks": failed_tracks,
        "job_ids": job_ids,
    }


@app.post("/api/music/album/candidates")
def music_album_candidates(payload: dict):
    query = str((payload or {}).get("query") or "").strip()
    raw_candidates = _search_music_album_candidates(query, limit=10)
    candidates = [
        {
            "album_id": item.get("release_group_id"),
            "title": item.get("title"),
            "artist": item.get("artist_credit"),
            "first_released": item.get("first_release_date"),
            "track_count": item.get("track_count"),
            "score": item.get("score"),
        }
        for item in raw_candidates
    ]
    return {
        "status": "ok",
        "album_candidates": candidates or [],
    }


@app.get("/api/music/albums/search")
def music_albums_search(q: str = Query("", alias="q"), limit: int = Query(10, ge=1, le=50)):
    return _search_music_album_candidates(str(q or ""), limit=int(limit))


@app.get("/api/music/search")
def music_search(
    artist: str = Query(""),
    album: str = Query(""),
    track: str = Query(""),
    mode: str = Query("auto"),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    artist_value = str(artist or "").strip()
    album_value = str(album or "").strip()
    track_value = str(track or "").strip()

    return search_music_metadata(
        artist=artist_value,
        album=album_value,
        track=track_value,
        mode=mode,
        offset=int(offset),
        limit=int(limit),
    )


@app.post("/api/music/enqueue")
def enqueue_music_track(data: dict = Body(...)):
    payload = data if isinstance(data, dict) else {}
    recording_mbid = str(payload.get("recording_mbid") or "").strip()
    if not recording_mbid:
        raise HTTPException(status_code=400, detail="missing_fields: recording_mbid")

    mb_release_id = str(
        payload.get("mb_release_id")
        or payload.get("release_mbid")
        or payload.get("release_id")
        or ""
    ).strip()
    mb_release_group_id = str(
        payload.get("mb_release_group_id")
        or payload.get("release_group_mbid")
        or payload.get("release_group_id")
        or ""
    ).strip()

    artist = str(payload.get("artist") or "").strip()
    track = str(payload.get("track") or "").strip()
    album = str(payload.get("album") or "").strip()
    release_date = str(payload.get("release_date") or payload.get("release_year") or "").strip()

    def _optional_pos_int(value):
        if value is None or str(value).strip() == "":
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="track_number/disc_number/duration_ms must be integers")
        return parsed if parsed > 0 else None

    track_number = _optional_pos_int(payload.get("track_number"))
    disc_number = _optional_pos_int(payload.get("disc_number"))
    duration_ms = _optional_pos_int(payload.get("duration_ms"))

    # Enrich optional missing fields from MusicBrainz recording lookup when available.
    if not artist or not track or duration_ms is None or not mb_release_id or not release_date:
        try:
            recording_payload = get_musicbrainz_service().get_recording(
                recording_mbid,
                includes=["artists", "releases", "isrcs"],
            )
            recording_data = recording_payload.get("recording", {}) if isinstance(recording_payload, dict) else {}
            if not track:
                track = str(recording_data.get("title") or "").strip()
            if not artist:
                credits = recording_data.get("artist-credit")
                if isinstance(credits, list):
                    artist_tokens = []
                    for credit in credits:
                        if not isinstance(credit, dict):
                            continue
                        name = str(
                            credit.get("name")
                            or (credit.get("artist") or {}).get("name")
                            or ""
                        ).strip()
                        if name:
                            artist_tokens.append(name)
                    artist = " & ".join(artist_tokens) if artist_tokens else artist
            if duration_ms is None:
                try:
                    rec_len = int(recording_data.get("length"))
                except Exception:
                    rec_len = None
                if rec_len and rec_len > 0:
                    duration_ms = rec_len
            release_list = recording_data.get("release-list")
            if isinstance(release_list, list) and release_list:
                first_release = release_list[0] if isinstance(release_list[0], dict) else {}
                if not mb_release_id:
                    mb_release_id = str(first_release.get("id") or "").strip()
                if not release_date:
                    release_date = str(first_release.get("date") or "").strip()
        except Exception:
            pass

    missing_core = []
    if not artist:
        missing_core.append("artist")
    if not track:
        missing_core.append("track")
    if missing_core:
        raise HTTPException(status_code=400, detail=f"missing_fields: {','.join(sorted(missing_core))}")

    canonical_metadata = {
        "artist": artist,
        "track": track,
        "album": album,
        "release_date": release_date,
        "track_number": track_number,
        "disc_number": disc_number,
        "duration_ms": duration_ms,
        "recording_mbid": recording_mbid,
        "mb_recording_id": recording_mbid,
        "mb_release_id": mb_release_id,
        "mb_release_group_id": mb_release_group_id,
    }

    destination = str(payload.get("destination") or payload.get("destination_dir") or "").strip() or None
    final_format_override = str(payload.get("final_format") or "").strip() or None
    force_redownload = bool(payload.get("force_redownload"))
    runtime_config = _read_config_or_404()
    engine = getattr(app.state, "worker_engine", None)
    queue_store = getattr(engine, "store", None) if engine is not None else None
    if queue_store is None:
        queue_store = DownloadJobStore(app.state.paths.db_path)

    placeholder_url = f"musicbrainz://recording/{recording_mbid}"
    canonical_id = _build_music_track_canonical_id(
        artist,
        album,
        track_number,
        track,
        recording_mbid=recording_mbid,
        mb_release_id=mb_release_id,
        mb_release_group_id=mb_release_group_id,
        disc_number=disc_number,
    )
    try:
        enqueue_payload = build_download_job_payload(
            config=runtime_config,
            origin="music_search",
            origin_id=recording_mbid,
            media_type="music",
            media_intent="music_track",
            source="music_search",
            url=placeholder_url,
            input_url=placeholder_url,
            destination=destination,
            base_dir=app.state.paths.single_downloads_dir,
            final_format_override=final_format_override,
            resolved_metadata=canonical_metadata,
            output_template_overrides={
                "kind": "music_track",
                "recording_mbid": recording_mbid,
                "mb_recording_id": recording_mbid,
                "mb_release_id": canonical_metadata.get("mb_release_id"),
                "mb_release_group_id": canonical_metadata.get("mb_release_group_id"),
                "track_number": track_number,
                "disc_number": disc_number,
                "release_date": canonical_metadata["release_date"],
                "duration_ms": duration_ms,
            },
            canonical_id=canonical_id,
        )
    except ValueError as exc:
        error_code = str(exc.args[0] if exc.args else exc).strip()
        reasons = []
        if len(exc.args) > 1 and isinstance(exc.args[1], list):
            reasons = [str(item) for item in exc.args[1] if str(item or "").strip()]
        if error_code == "music_track_requires_mb_bound_metadata":
            return JSONResponse(
                status_code=422,
                content={
                    "error": "music_mode_mb_binding_failed",
                    "reason": reasons,
                },
            )
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    job_id, created, dedupe_reason = queue_store.enqueue_job(
        **enqueue_payload,
        force_requeue=force_redownload,
    )
    return {
        "status": "ok",
        "job_id": job_id,
        "created": bool(created),
        "dedupe_reason": dedupe_reason,
    }


@app.get("/api/music/album/art/{album_id}")
def music_album_art(album_id: str):
    album_id = str(album_id or "").strip()
    if not album_id:
        raise HTTPException(status_code=400, detail="album_id is required")

    cache = getattr(app.state, "music_cover_art_cache", None)
    now = time.time()
    if isinstance(cache, dict):
        cached = cache.get(album_id)
        if isinstance(cached, dict):
            cached_at = float(cached.get("cached_at") or 0)
            if now - cached_at < COVER_ART_CACHE_TTL_SECONDS:
                return {"status": "ok", "cover_url": cached.get("cover_url")}

    cover_url = _mb_service().fetch_release_group_cover_art_url(album_id, timeout=8)

    if isinstance(cache, dict):
        cache[album_id] = {
            "cover_url": cover_url,
            "cached_at": now,
        }
    return {"status": "ok", "cover_url": cover_url}


@app.post("/api/spotify/playlists/import")
async def import_spotify_playlist(payload: SpotifyPlaylistImportPayload):
    playlist_url = (payload.playlist_url or "").strip()
    if not playlist_url:
        raise HTTPException(status_code=400, detail="playlist_url is required")
    config = _read_config_or_404()
    playlist_entries = config.get("spotify_playlists") or []
    entry = next((pl for pl in playlist_entries if pl.get("playlist_url") == playlist_url), None)
    if not entry:
        raise HTTPException(status_code=404, detail="Spotify playlist not configured")
    status_entry = {
        "state": "importing",
        "message": "Importing playlist metadata...",
        "destination": None,
        "tracks_discovered": 0,
        "tracks_queued": 0,
        "tracks_skipped": 0,
        "tracks_failed": 0,
        "error": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    app.state.spotify_import_status[playlist_url] = status_entry
    try:
        result = await anyio.to_thread.run_sync(
            app.state.spotify_playlist_importer.import_playlist,
            entry,
            app.state.search_service,
            config,
        )
        status_entry.update(
            state="completed",
            message="Import completed",
            destination=result.get("destination"),
            tracks_discovered=result.get("tracks_discovered", 0),
            tracks_queued=result.get("tracks_queued", 0),
            tracks_skipped=result.get("tracks_skipped", 0),
            tracks_failed=result.get("tracks_failed", 0),
            error=None,
        )
        status_entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        return {"status": status_entry}
    except SpotifyPlaylistImportError as exc:
        status_entry.update(
            state="error",
            message="Import failed",
            error=str(exc),
        )
        status_entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logging.exception("Spotify playlist import failed: %s", exc)
        status_entry.update(
            state="error",
            message="Import failed",
            error=str(exc),
        )
        status_entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        app.state.spotify_import_status[playlist_url] = status_entry


@app.get("/api/spotify/playlists/status")
async def spotify_playlist_status():
    return {"statuses": app.state.spotify_import_status}


@app.get("/api/spotify/oauth/connect")
async def spotify_oauth_connect():
    """Build Spotify OAuth connect URL and store anti-CSRF state in memory."""
    config = _read_config_or_404()
    spotify_cfg = (config.get("spotify") or {}) if isinstance(config, dict) else {}
    client_id = (
        str(spotify_cfg.get("client_id") or config.get("SPOTIFY_CLIENT_ID") or "").strip()
        if isinstance(config, dict)
        else ""
    )
    redirect_uri = (
        str(spotify_cfg.get("redirect_uri") or config.get("SPOTIFY_REDIRECT_URI") or "").strip()
        if isinstance(config, dict)
        else ""
    )
    if not client_id:
        raise HTTPException(status_code=400, detail="SPOTIFY_CLIENT_ID is required in config")
    if not redirect_uri:
        raise HTTPException(status_code=400, detail="SPOTIFY_REDIRECT_URI is required in config")

    app.state.spotify_oauth_state = None
    state = str(uuid4())
    app.state.spotify_oauth_state = state
    scope = "user-library-read playlist-read-private playlist-read-collaborative"
    auth_url = build_auth_url(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scope=scope,
        state=state,
    )
    return {"auth_url": auth_url}


@app.get("/api/spotify/oauth/status")
async def spotify_oauth_status():
    """Return Spotify OAuth connection status without exposing sensitive tokens."""
    store = SpotifyOAuthStore(Path(app.state.paths.db_path))
    token = store.load()
    if token is None:
        return {"connected": False}

    scopes = [part for part in str(token.scope or "").split() if part]
    payload: dict[str, object] = {"connected": True}
    if scopes:
        payload["scopes"] = scopes
    payload["expires_at"] = int(token.expires_at)
    return payload


@app.get("/api/spotify/oauth/callback")
async def spotify_oauth_callback(code: str | None = None, state: str | None = None, error: str | None = None):
    """Handle Spotify OAuth callback and persist tokens."""
    if error:
        raise HTTPException(status_code=400, detail=f"spotify_oauth_error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="missing code")
    if not state:
        raise HTTPException(status_code=400, detail="missing state")

    expected_state = str(getattr(app.state, "spotify_oauth_state", "") or "")
    if not expected_state or state != expected_state:
        raise HTTPException(status_code=400, detail="invalid oauth state")

    config = _read_config_or_404()
    spotify_cfg = (config.get("spotify") or {}) if isinstance(config, dict) else {}
    client_id = (
        str(spotify_cfg.get("client_id") or config.get("SPOTIFY_CLIENT_ID") or "").strip()
        if isinstance(config, dict)
        else ""
    )
    client_secret = (
        str(spotify_cfg.get("client_secret") or config.get("SPOTIFY_CLIENT_SECRET") or "").strip()
        if isinstance(config, dict)
        else ""
    )
    redirect_uri = (
        str(spotify_cfg.get("redirect_uri") or config.get("SPOTIFY_REDIRECT_URI") or "").strip()
        if isinstance(config, dict)
        else ""
    )
    if not client_id:
        raise HTTPException(status_code=400, detail="SPOTIFY_CLIENT_ID is required in config")
    if not client_secret:
        raise HTTPException(status_code=400, detail="SPOTIFY_CLIENT_SECRET is required in config")
    if not redirect_uri:
        raise HTTPException(status_code=400, detail="SPOTIFY_REDIRECT_URI is required in config")

    try:
        token_response = requests.post(
            SPOTIFY_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=20,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"token exchange failed: {exc}") from exc

    if token_response.status_code != 200:
        detail = (token_response.text or "").strip() or f"status={token_response.status_code}"
        raise HTTPException(status_code=400, detail=f"token exchange failed: {detail}")

    payload = token_response.json()
    access_token = str(payload.get("access_token") or "").strip()
    refresh_token = str(payload.get("refresh_token") or "").strip()
    expires_in = payload.get("expires_in")
    scope = str(payload.get("scope") or "").strip()
    if not access_token:
        raise HTTPException(status_code=400, detail="token exchange failed: missing access_token")
    if not refresh_token:
        raise HTTPException(status_code=400, detail="token exchange failed: missing refresh_token")
    if expires_in is None:
        raise HTTPException(status_code=400, detail="token exchange failed: missing expires_in")
    if not scope:
        raise HTTPException(status_code=400, detail="token exchange failed: missing scope")

    try:
        expires_at = int(time.time()) + int(expires_in)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="token exchange failed: invalid expires_in") from exc

    store = SpotifyOAuthStore(Path(app.state.paths.db_path))
    store.save(
        SpotifyOAuthToken(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at,
            scope=scope,
        )
    )
    try:
        sync_db = PlaylistSnapshotStore(app.state.paths.db_path)
        sync_queue = _IntentQueueAdapter()
        sync_client = SpotifyPlaylistClient(
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
        )
        await spotify_liked_songs_watch_job(
            config=config,
            db=sync_db,
            queue=sync_queue,
            spotify_client=sync_client,
            search_service=app.state.search_service,
        )
        await spotify_saved_albums_watch_job(
            config=config,
            db=sync_db,
            queue=sync_queue,
            spotify_client=sync_client,
            search_service=app.state.search_service,
        )
        await spotify_user_playlists_watch_job(
            config=config,
            db=sync_db,
            queue=sync_queue,
            spotify_client=sync_client,
            search_service=app.state.search_service,
        )
    except Exception:
        logging.exception("Post-OAuth immediate Spotify sync failed")
    _apply_spotify_schedule(config or {})
    app.state.spotify_oauth_state = None
    return RedirectResponse(url="/#config?spotify=connected", status_code=302)


@app.post("/api/spotify/oauth/disconnect")
async def spotify_oauth_disconnect():
    """Clear stored Spotify OAuth token state."""
    store = SpotifyOAuthStore(Path(app.state.paths.db_path))
    store.clear()
    return {"status": "disconnected"}


@app.get("/api/search/items/{item_id}/candidates")
async def get_search_candidates(item_id: str):
    service = app.state.search_service
    candidates = service.list_item_candidates(item_id)
    return {"candidates": candidates}


@app.post("/api/search/items/{item_id}/enqueue")
async def enqueue_search_candidate(item_id: str, payload: EnqueueCandidatePayload):
    service = app.state.search_service
    candidate_id = (payload.candidate_id or "").strip()
    logging.debug(
        "Search enqueue payload: %s",
        safe_json(
            {
                "item_id": item_id,
                "candidate_id": candidate_id,
                "delivery_mode": getattr(payload, "delivery_mode", None),
                "final_format": getattr(payload, "final_format", None),
            }
        ),
    )

    if not candidate_id:
        logging.warning(
            "Search enqueue failed: missing candidate_id request_id=%s item_id=%s candidate_id=%s source=%s",
            None,
            item_id,
            candidate_id,
            None,
        )
        return JSONResponse(
            status_code=400,
            content={"error": "candidate_id is required", "code": "INVALID_REQUEST"},
        )

    item = service.store.get_item(item_id)
    if not item:
        logging.warning(
            "Search enqueue failed: item not found request_id=%s item_id=%s candidate_id=%s source=%s",
            None,
            item_id,
            candidate_id,
            None,
        )
        return JSONResponse(
            status_code=404,
            content={"error": "Search item not found", "code": "ITEM_NOT_FOUND"},
        )

    candidate = service.store.get_candidate(candidate_id)
    if not candidate or candidate.get("item_id") != item_id:
        logging.warning(
            "Search enqueue failed: candidate not found request_id=%s item_id=%s candidate_id=%s source=%s",
            item.get("request_id"),
            item_id,
            candidate_id,
            None,
        )
        return JSONResponse(
            status_code=404,
            content={"error": "Candidate not found", "code": "CANDIDATE_NOT_FOUND"},
        )

    request_id = item.get("request_id")
    request_row = service.store.get_request_row(request_id) if request_id else None
    if not request_row:
        logging.warning(
            "Search enqueue failed: request not found request_id=%s item_id=%s candidate_id=%s source=%s",
            request_id,
            item_id,
            candidate_id,
            candidate.get("source"),
        )
        return JSONResponse(
            status_code=404,
            content={"error": "Search request not found", "code": "REQUEST_NOT_FOUND"},
        )

    enabled_sources = list(getattr(service, "adapters", {}).keys())
    try:
        normalized_request = normalize_search_payload(
            {
                "source_priority": request_row.get("source_priority_json"),
                "auto_enqueue": request_row.get("auto_enqueue"),
                "media_type": request_row.get("media_type"),
                "destination_dir": request_row.get("destination_dir"),
            },
            default_sources=enabled_sources,
        )
    except ValueError as exc:
        logging.warning(
            "Search enqueue failed: %s request_id=%s item_id=%s candidate_id=%s source=%s",
            str(exc),
            request_id,
            item_id,
            candidate_id,
            candidate.get("source"),
        )
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid search request payload", "code": "INVALID_REQUEST"},
        )
    request_overrides = None
    if hasattr(app.state, "search_request_overrides"):
        request_overrides = app.state.search_request_overrides.get(request_id)

    payload_delivery_mode = str(getattr(payload, "delivery_mode", "") or "").strip().lower()
    if payload_delivery_mode not in {"", "server", "client"}:
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid delivery mode", "code": "INVALID_REQUEST"},
        )
    request_delivery_mode = str(normalized_request.get("delivery_mode") or "").strip().lower()
    overrides_delivery_mode = (
        str(request_overrides.get("delivery_mode") or "").strip().lower()
        if request_overrides
        else ""
    )
    effective_delivery_mode = (
        payload_delivery_mode
        or overrides_delivery_mode
        or request_delivery_mode
        or "server"
    )
    if effective_delivery_mode not in {"server", "client"}:
        effective_delivery_mode = "server"
    logging.info(
        safe_json(
            {
                "message": "candidate_enqueue_delivery_mode",
                "request_id": request_id,
                "item_id": item_id,
                "candidate_id": candidate_id,
                "payload_delivery_mode": payload_delivery_mode or None,
                "effective_delivery_mode": effective_delivery_mode,
            }
        )
    )

    url = candidate.get("url")
    source = candidate.get("source")
    logging.debug(
        "Search enqueue resolved item: %s",
        safe_json(
            {
                "id": item.get("id"),
                "status": item.get("status"),
                "request_id": request_id,
                "media_type": item.get("media_type"),
                "item_type": item.get("item_type"),
            }
        ),
    )
    logging.debug(
        "Search enqueue resolved candidate: %s",
        safe_json(
            {
                "id": candidate.get("id"),
                "source": source,
                "url": url,
                "external_id": candidate.get("external_id"),
            }
        ),
    )
    logging.debug(
        "Search enqueue derived destination: %s",
        safe_json(
            {
                "destination": service._resolve_request_destination(
                    normalized_request.get("destination_path")
                ),
                "final_format": getattr(payload, "final_format", None),
            }
        ),
    )

    if not url or not _is_http_url(url):
        logging.warning(
            "Search enqueue failed: non-downloadable URL request_id=%s item_id=%s candidate_id=%s source=%s",
            request_id,
            item_id,
            candidate_id,
            source,
        )
        return JSONResponse(
            status_code=422,
            content={
                "error": "This search result is not directly downloadable.",
                "code": "CANDIDATE_NOT_DOWNLOADABLE",
            },
        )

    if item.get("status") == "enqueued":
        return JSONResponse(
            status_code=409,
            content={"error": "Candidate already enqueued.", "code": "ALREADY_ENQUEUED"},
        )
    if item.get("status") == "skipped" and item.get("error") == "duplicate":
        return JSONResponse(
            status_code=409,
            content={"error": "Already downloaded.", "code": "ALREADY_DOWNLOADED"},
        )

    active_job = service.queue_store.find_active_job("search", request_id, url)
    if active_job:
        return JSONResponse(
            status_code=409,
            content={"error": "Candidate already enqueued.", "code": "ALREADY_ENQUEUED"},
        )

    final_format_override = getattr(payload, "final_format", None)
    if not final_format_override and request_overrides:
        final_format_override = request_overrides.get("final_format")

    async def _run_immediate():
        config = _read_config_or_404()
        effective_final_format = final_format_override or config.get("final_format")
        media_type = str(
            item.get("media_type")
            or request_row.get("media_type")
            or "generic"
        ).strip().lower() or "generic"
        if media_type == "generic":
            media_type = "video"
        # Do not infer Music Mode from final_format; music enforcement must come from request/item media type.
        if media_type == "music":
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Music downloads require queued MB binding; client delivery is not supported.",
                    "code": "MUSIC_CLIENT_DELIVERY_UNSUPPORTED",
                },
            )
        media_intent = resolve_media_intent("search", media_type)
        try:
            run_client_delivery = functools.partial(
                _run_immediate_download_to_client,
                url=url,
                config=config,
                paths=app.state.paths,
                media_type=media_type,
                media_intent=media_intent,
                final_format_override=effective_final_format,
                origin="search",
            )
            result = await anyio.to_thread.run_sync(run_client_delivery)
        except Exception as exc:
            logging.warning(
                "Search client delivery failed: %s request_id=%s item_id=%s candidate_id=%s source=%s",
                exc,
                request_id,
                item_id,
                candidate_id,
                source,
                exc_info=True,
            )
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Client delivery failed.",
                    "code": "CLIENT_DELIVERY_FAILED",
                    "detail": str(exc),
                },
            )

        service.store.update_item_status(item_id, "enqueued", chosen=candidate)
        if request_row.get("status") not in {"completed", "completed_with_skips", "failed"}:
            service.store.update_request_status(request_id, "completed")

        return JSONResponse(
            status_code=200,
            content={
                "created": True,
                "job_id": result.get("delivery_id"),
                "delivery_id": result.get("delivery_id"),
                "delivery_url": f"/api/deliveries/{result.get('delivery_id')}/download",
                "filename": result.get("filename"),
                "expires_at": result.get("expires_at"),
            },
        )

    def _enqueue():
        if effective_delivery_mode == "client":
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Client delivery is not available for queued downloads.",
                    "code": "CLIENT_DELIVERY_UNSUPPORTED",
                },
            )
        try:
            result = service.enqueue_item_candidate(
                item_id,
                candidate_id,
                final_format_override=final_format_override,
            )
        except ValueError as exc:
            logging.warning(
                "Search enqueue failed: %s request_id=%s item_id=%s candidate_id=%s source=%s",
                str(exc),
                request_id,
                item_id,
                candidate_id,
                source,
            )
            if exc.args and isinstance(exc.args[0], dict):
                payload = exc.args[0]
                if payload.get("error") == "music_mode_mb_binding_failed":
                    return JSONResponse(status_code=422, content=payload)
            error_msg = "Invalid destination"
            code = "INVALID_DESTINATION"
            if "invalid_destination" not in str(exc).lower():
                error_msg = "Invalid request"
                code = "INVALID_REQUEST"
            return JSONResponse(status_code=422, content={"error": error_msg, "code": code})
        except Exception as exc:
            logging.warning(
                "Search enqueue failed: %s request_id=%s item_id=%s candidate_id=%s source=%s",
                exc,
                request_id,
                item_id,
                candidate_id,
                source,
                exc_info=True,
            )
            return JSONResponse(
                status_code=500,
                content={"error": "Enqueue failed.", "code": "ENQUEUE_FAILED"},
            )

        if not result:
            logging.warning(
                "Search enqueue failed: unresolved request_id=%s item_id=%s candidate_id=%s source=%s",
                request_id,
                item_id,
                candidate_id,
                source,
            )
            return JSONResponse(
                status_code=404,
                content={"error": "Search item not found", "code": "ITEM_NOT_FOUND"},
            )

        if not result.get("created") and result.get("job_id"):
            return JSONResponse(
                status_code=409,
                content={"error": "Already downloaded.", "code": "ALREADY_DOWNLOADED"},
            )

        return result

    result = execute_download(
        delivery_mode=effective_delivery_mode,
        run_immediate=_run_immediate,
        enqueue=_enqueue,
    )
    if asyncio.iscoroutine(result):
        return await result
    return result


@app.get("/api/download_jobs")
async def list_download_jobs(limit: int = 100, status: str | None = None):
    conn = sqlite3.connect(app.state.paths.db_path)
    try:
        cur = conn.cursor()
        query = (
            "SELECT id, origin, origin_id, url, source, media_intent, status, attempts, created_at, last_error, "
            "progress_downloaded_bytes, progress_total_bytes, progress_percent, progress_speed_bps, progress_eta_seconds, progress_updated_at "
            "FROM download_jobs"
        )
        params = []
        if status:
            query += " WHERE status=?"
            params.append(status)
        query += " ORDER BY created_at DESC"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        try:
            cur.execute(query, params)
        except sqlite3.OperationalError as exc:
            if "no such column" not in str(exc).lower():
                raise
            # Backward compatibility if DB has not yet migrated progress columns.
            query = (
                "SELECT id, origin, origin_id, url, source, media_intent, status, attempts, created_at, last_error "
                "FROM download_jobs"
            )
            params = []
            if status:
                query += " WHERE status=?"
                params.append(status)
            query += " ORDER BY created_at DESC"
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            cur.execute(query, params)
            rows = [
                {
                    "id": row[0],
                    "origin": row[1],
                    "origin_id": row[2],
                    "url": row[3],
                    "source": row[4],
                    "media_intent": row[5],
                    "status": row[6],
                    "attempts": row[7],
                    "created_at": row[8],
                    "last_error": row[9],
                    "progress_downloaded_bytes": None,
                    "progress_total_bytes": None,
                    "progress_percent": None,
                    "progress_speed_bps": None,
                    "progress_eta_seconds": None,
                    "progress_updated_at": None,
                }
                for row in cur.fetchall()
            ]
            return safe_json({"jobs": rows})
        rows = [
            {
                "id": row[0],
                "origin": row[1],
                "origin_id": row[2],
                "url": row[3],
                "source": row[4],
                "media_intent": row[5],
                "status": row[6],
                "attempts": row[7],
                "created_at": row[8],
                "last_error": row[9],
                "progress_downloaded_bytes": row[10],
                "progress_total_bytes": row[11],
                "progress_percent": row[12],
                "progress_speed_bps": row[13],
                "progress_eta_seconds": row[14],
                "progress_updated_at": row[15],
            }
            for row in cur.fetchall()
        ]
    finally:
        conn.close()
    return safe_json({"jobs": rows})


@app.get("/api/music/failures")
async def list_music_failures(limit: int = Query(50, ge=1, le=500)):
    conn = sqlite3.connect(app.state.paths.db_path)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_music_failures_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM music_failures")
        total_count = int((cur.fetchone() or {"c": 0})["c"])
        cur.execute(
            """
            SELECT id, created_at, origin_batch_id, artist, track, reason_json, recording_mbid_attempted, last_query
            FROM music_failures
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = []
        for row in cur.fetchall():
            reason_json = row["reason_json"]
            reasons = []
            if reason_json:
                try:
                    parsed = json.loads(reason_json)
                    reasons = parsed.get("reasons") if isinstance(parsed, dict) else []
                    if not isinstance(reasons, list):
                        reasons = []
                except Exception:
                    reasons = []
            rows.append(
                {
                    "id": row["id"],
                    "created_at": row["created_at"],
                    "origin_batch_id": row["origin_batch_id"],
                    "artist": row["artist"],
                    "track": row["track"],
                    "reasons": reasons,
                    "recording_mbid_attempted": row["recording_mbid_attempted"],
                    "last_query": row["last_query"],
                }
            )
    finally:
        conn.close()
    return {"count": total_count, "rows": rows}





@app.get("/api/config")
async def api_get_config():
    return _read_config_or_404()


@app.put("/api/config")
async def api_put_config(payload: dict = Body(...)):
    payload = _strip_deprecated_fields(payload)
    errors = validate_config(payload)
    # Saving config should not require Spotify OAuth client credentials.
    # Those are validated only when the Spotify connect flow is invoked.
    errors = [
        err for err in errors
        if not any(
            marker in str(err)
            for marker in (
                "SPOTIFY_CLIENT_ID",
                "SPOTIFY_CLIENT_SECRET",
                "spotify.client_id",
                "spotify.client_secret",
            )
        )
    ]
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    config_path = app.state.config_path
    config_dir = os.path.dirname(config_path) or "."
    os.makedirs(config_dir, exist_ok=True)

    tmp = tempfile.NamedTemporaryFile("w", delete=False, dir=config_dir)
    try:
        safe_json_dump(payload, tmp, indent=4)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp.name, config_path)
    finally:
        try:
            tmp.close()
        except Exception:
            pass
        if os.path.exists(tmp.name):
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    normalized_payload = safe_json(payload)
    app.state.loaded_config = normalized_payload if isinstance(normalized_payload, dict) else {}
    app.state.config = app.state.loaded_config

    if "schedule" in payload:
        schedule = _merge_schedule_config(payload.get("schedule"))
        app.state.schedule_config = schedule
        _apply_schedule_config(schedule)
    _apply_spotify_schedule(payload or {})
    policy = normalize_watch_policy(payload)
    if getattr(normalize_watch_policy, "valid", True):
        app.state.watch_policy = policy
        _apply_watch_policy(policy)
        app.state.watch_config_cache = payload

    return {"status": "updated"}


@app.get("/api/history")
async def api_history(
    limit: int = Query(200, ge=1, le=5000),
    search: str | None = Query(None, max_length=200),
    playlist_id: str | None = Query(None, max_length=200),
    date_from: str | None = Query(None, max_length=32),
    date_to: str | None = Query(None, max_length=32),
    sort_by: str = Query("date", max_length=20),
    sort_dir: str = Query("desc", max_length=4),
):
    sort_by = (sort_by or "date").lower()
    sort_dir = (sort_dir or "desc").lower()
    if sort_by not in {"date", "title", "size"}:
        raise HTTPException(status_code=400, detail="sort_by must be date, title, or size")
    if sort_dir not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="sort_dir must be asc or desc")
    search_value = search.strip() if search else None
    playlist_value = playlist_id.strip() if playlist_id else None
    rows = read_history(
        app.state.paths.db_path,
        limit=limit,
        search=search_value,
        playlist_id=playlist_value,
        date_from=_normalize_date(date_from, end_of_day=False),
        date_to=_normalize_date(date_to, end_of_day=True),
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return [
        {
            "video_id": row[0],
            "playlist_id": row[1],
            "downloaded_at": row[2],
            "filepath": row[3],
            "file_id": _file_id_from_path(row[3]),
        }
        for row in rows
    ]


@app.get("/api/files")
async def api_files():
    return _list_download_files(DOWNLOADS_DIR)


@app.get("/api/files/{file_id}/download")
async def api_file_download(file_id: str):
    try:
        rel = _decode_file_id(file_id)
    except (ValueError, UnicodeDecodeError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid file id")

    candidate = os.path.abspath(os.path.join(DOWNLOADS_DIR, rel))
    if not _path_allowed(candidate, [DOWNLOADS_DIR]):
        raise HTTPException(status_code=403, detail="File not allowed")
    if not os.path.isfile(candidate):
        raise HTTPException(status_code=404, detail="File not found")

    filename = _safe_filename(os.path.basename(candidate))
    content_type, _ = mimetypes.guess_type(candidate)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(_iter_file(candidate), media_type=content_type or "application/octet-stream", headers=headers)


@app.get("/api/deliveries/{delivery_id}/download")
async def api_delivery_download(delivery_id: str):
    entry = _acquire_client_delivery(delivery_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Delivery not available")
    candidate = entry.get("path")
    if not candidate or not os.path.isfile(candidate):
        _mark_client_delivery(delivery_id, delivered=False)
        raise HTTPException(status_code=404, detail="Delivery file not found")

    filename = _safe_filename(entry.get("filename") or os.path.basename(candidate))
    content_type, _ = mimetypes.guess_type(candidate)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    logging.info("HTTP client download started delivery_id=%s", delivery_id)

    def stream():
        completed = False
        try:
            with open(candidate, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk
            completed = True
        except Exception:
            logging.exception("Client delivery stream failed delivery_id=%s", delivery_id)
            raise
        finally:
            _mark_client_delivery(delivery_id, delivered=completed)
            if completed:
                logging.info("HTTP client download complete → cleanup")
            else:
                logging.warning("HTTP client download incomplete → cleanup")
            _finalize_client_delivery(delivery_id, timeout=False)

    return StreamingResponse(stream(), media_type=content_type or "application/octet-stream", headers=headers)


@app.post("/api/cleanup")
async def api_cleanup():
    paths = app.state.paths
    deleted_files = 0
    deleted_bytes = 0
    results = {}
    for label, target in (
        ("temp_downloads", paths.temp_downloads_dir),
        ("ytdlp_temp", paths.ytdlp_temp_dir),
    ):
        files_count, bytes_count = _cleanup_dir(target)
        deleted_files += files_count
        deleted_bytes += bytes_count
        results[label] = {
            "path": target,
            "deleted_files": files_count,
            "deleted_bytes": bytes_count,
        }
    return {
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "details": results,
    }


@app.get("/api/browse")
async def api_browse(
    root: str = Query(..., description="downloads, config, or tokens"),
    path: str = Query("", description="Relative path within the root"),
    mode: str = Query("dir", description="dir or file"),
    ext: str = Query("", description="Optional file extension filter, e.g. .json"),
    limit: int | None = Query(None, ge=1, le=5000, description="Optional max entries"),
):
    root = (root or "").strip().lower()
    roots = app.state.browse_roots
    if root not in roots:
        raise HTTPException(status_code=400, detail="root must be downloads, config, or tokens")

    mode = mode.lower()
    if mode not in {"dir", "file"}:
        raise HTTPException(status_code=400, detail="mode must be dir or file")

    ext = ext.strip().lower()
    if ext and not ext.startswith("."):
        ext = f".{ext}"

    base = roots[root]
    rel_path, target = _resolve_browse_path(base, path)
    logging.info("Browse opened root=%s path=%s mode=%s", root, rel_path or "/", mode)
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail=f"Path not found: {target}")
    if os.path.isfile(target):
        target = os.path.dirname(target)
        rel_path = os.path.relpath(target, base)
        if rel_path == ".":
            rel_path = ""

    parent = None
    if rel_path:
        parent = os.path.dirname(rel_path)
        if parent == ".":
            parent = ""

    try:
        entries = _list_browse_entries(base, target, mode, ext, limit=limit)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read directory: {exc}") from exc

    return {
        "root": root,
        "path": rel_path,
        "abs_path": target,
        "parent": parent,
        "entries": entries,
    }


if os.path.isdir(WEBUI_DIR):
    app.mount("/", StaticFiles(directory=WEBUI_DIR, html=True), name="webui")


if __name__ == "__main__":
    import uvicorn

    host = _env_or_default("YT_ARCHIVER_HOST", "127.0.0.1")
    port = int(_env_or_default("YT_ARCHIVER_PORT", "8000"))
    uvicorn.run("api.main:app", host=host, port=port, reload=False)
