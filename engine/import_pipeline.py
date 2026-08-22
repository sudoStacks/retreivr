from __future__ import annotations

import importlib.util
import logging
import sqlite3
import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from metadata.importers.base import TrackIntent
from engine.job_queue import DownloadJobStore, build_download_job_payload
from engine.canonical_ids import build_music_track_canonical_id
from engine.job_queue import (
    JOB_STATUS_CANCELLED,
    JOB_STATUS_CLAIMED,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_DOWNLOADING,
    JOB_STATUS_FAILED,
    JOB_STATUS_POSTPROCESSING,
    JOB_STATUS_QUEUED,
)
from engine.download_defaults import resolve_effective_download_settings

try:
    from engine.musicbrainz_binding import resolve_best_mb_pair
except Exception:
    _BINDING_PATH = Path(__file__).resolve().parent / "musicbrainz_binding.py"
    _BINDING_SPEC = importlib.util.spec_from_file_location("engine_musicbrainz_binding_import_pipeline", _BINDING_PATH)
    _BINDING_MODULE = importlib.util.module_from_spec(_BINDING_SPEC)
    assert _BINDING_SPEC and _BINDING_SPEC.loader
    _BINDING_SPEC.loader.exec_module(_BINDING_MODULE)
    resolve_best_mb_pair = _BINDING_MODULE.resolve_best_mb_pair

_DEFAULT_CONFIDENCE_THRESHOLD = 0.78
_DEFAULT_MB_BINDING_WORKERS = 4
_MAX_MB_BINDING_WORKERS = 5
_DEFAULT_MUSIC_FAILURES_RETENTION_MAX_ROWS = 2000
_DEFAULT_MUSIC_FAILURES_RETENTION_MAX_AGE_DAYS = 30
_DEFAULT_IMPORT_DURATION_DELTA_MS = 30000
_DEFAULT_IMPORT_CORRECTNESS_FLOOR = 57.0
_DEFAULT_IMPORT_PROGRESS_INTERVAL = 100
_STALE_IMPORT_BATCH_PHASES = {"queued", "parsing", "resolving", "enqueueing", "finalizing"}
logger = logging.getLogger(__name__)


_LIBRARY_YEAR_SUFFIX_RE = re.compile(r"\s*\((?:19|20)\d{2}\)\s*$")
_LIBRARY_PUNCT_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class ImportResult:
    total_tracks: int
    resolved_count: int
    unresolved_count: int
    enqueued_count: int
    duplicate_skipped_count: int
    failed_count: int
    resolved_mbids: list[str] = field(default_factory=list)
    import_batch_id: str = ""
    top_rejection_reasons: dict[str, int] = field(default_factory=dict)
    selected_bucket_counts: dict[str, int] = field(default_factory=dict)
    current_phase_detail: str | None = None


def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except Exception:
        return json.dumps({})


def _normalize_library_dedupe_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = _LIBRARY_PUNCT_RE.sub(" ", text)
    return " ".join(text.split())


def _normalize_library_dedupe_album(value: Any) -> str:
    text = _LIBRARY_YEAR_SUFFIX_RE.sub("", str(value or "").strip())
    return _normalize_library_dedupe_text(text)


def _library_row_path_exists(path: Any) -> bool:
    raw_path = str(path or "").strip()
    if not raw_path:
        return False
    try:
        return Path(raw_path).exists()
    except OSError:
        return False


def _load_library_duplicate_index(db_path: str | None) -> dict[str, Any]:
    empty = {
        "metadata_keys": {},
        "recording_metadata_keys": {},
        "release_metadata_keys": {},
        "release_group_metadata_keys": {},
    }
    if not db_path:
        return empty
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT path, title, artist, album, recording_mbid, mb_release_id, mb_release_group_id
            FROM music_library_index
            WHERE instr(lower(path), '/_applemusic/') = 0
            """
        ).fetchall()
    except sqlite3.Error:
        return empty
    finally:
        try:
            conn.close()
        except Exception:
            pass

    index = {
        "metadata_keys": {},
        "recording_metadata_keys": {},
        "release_metadata_keys": {},
        "release_group_metadata_keys": {},
    }
    for row in rows:
        if not _library_row_path_exists(row["path"]):
            continue
        title_key = _normalize_library_dedupe_text(row["title"])
        artist_key = _normalize_library_dedupe_text(row["artist"])
        album_key = _normalize_library_dedupe_album(row["album"])
        if not (title_key and artist_key and album_key):
            continue
        payload = {
            "path": row["path"],
            "title": row["title"],
            "artist": row["artist"],
            "album": row["album"],
            "recording_mbid": row["recording_mbid"],
            "mb_release_id": row["mb_release_id"],
            "mb_release_group_id": row["mb_release_group_id"],
        }
        recording_mbid = str(row["recording_mbid"] or "").strip()
        metadata_key = (artist_key, album_key, title_key)
        index["metadata_keys"].setdefault(metadata_key, payload)
        if recording_mbid:
            index["recording_metadata_keys"].setdefault((recording_mbid, *metadata_key), payload)
        release_mbid = str(row["mb_release_id"] or "").strip()
        if release_mbid:
            index["release_metadata_keys"].setdefault((release_mbid, *metadata_key), payload)
        release_group_mbid = str(row["mb_release_group_id"] or "").strip()
        if release_group_mbid:
            index["release_group_metadata_keys"].setdefault((release_group_mbid, *metadata_key), payload)
    return index


def _classify_library_duplicate(
    library_duplicate_index: dict[str, Any] | None,
    *,
    artist: str | None,
    album: str | None,
    title: str | None,
    album_artist: str | None = None,
    recording_mbid: str | None = None,
    mb_release_id: str | None = None,
    mb_release_group_id: str | None = None,
) -> dict[str, Any] | None:
    if not library_duplicate_index:
        return None
    title_key = _normalize_library_dedupe_text(title)
    album_key = _normalize_library_dedupe_album(album)
    artist_candidates = [
        _normalize_library_dedupe_text(value)
        for value in (artist, album_artist)
        if _normalize_library_dedupe_text(value)
    ]
    if not (title_key and album_key and artist_candidates):
        return None

    metadata_keys = [(artist_key, album_key, title_key) for artist_key in dict.fromkeys(artist_candidates)]
    recording_mbid = str(recording_mbid or "").strip()
    if recording_mbid:
        for metadata_key in metadata_keys:
            match = library_duplicate_index.get("recording_metadata_keys", {}).get((recording_mbid, *metadata_key))
            if match:
                return {"classification": "library_present", **match}

    release_mbid = str(mb_release_id or "").strip()
    if release_mbid:
        for metadata_key in metadata_keys:
            match = library_duplicate_index.get("release_metadata_keys", {}).get((release_mbid, *metadata_key))
            if match:
                return {"classification": "library_present", **match}

    release_group_mbid = str(mb_release_group_id or "").strip()
    if release_group_mbid:
        for metadata_key in metadata_keys:
            match = library_duplicate_index.get("release_group_metadata_keys", {}).get((release_group_mbid, *metadata_key))
            if match:
                return {"classification": "library_present", **match}

    for metadata_key in metadata_keys:
        match = library_duplicate_index.get("metadata_keys", {}).get(metadata_key)
        if match:
            return {"classification": "library_present", **match}
    return None


def ensure_import_batch_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS import_batches (
            batch_id TEXT PRIMARY KEY,
            source_format TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            phase TEXT,
            current_phase_detail TEXT,
            total_tracks INTEGER NOT NULL DEFAULT 0,
            processed_tracks INTEGER NOT NULL DEFAULT 0,
            resolved_count INTEGER NOT NULL DEFAULT 0,
            unresolved_count INTEGER NOT NULL DEFAULT 0,
            enqueued_count INTEGER NOT NULL DEFAULT 0,
            duplicate_skipped_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            top_rejection_reasons_json TEXT,
            selected_bucket_counts_json TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS import_batch_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            source_index INTEGER NOT NULL,
            item_state TEXT,
            source_resolution_state TEXT,
            resolution_attempts INTEGER NOT NULL DEFAULT 0,
            artist TEXT,
            title TEXT,
            album TEXT,
            album_artist TEXT,
            input_metadata_json TEXT,
            outcome TEXT,
            canonical_id TEXT,
            linked_job_id TEXT,
            linked_job_status TEXT,
            recording_mbid TEXT,
            mb_release_id TEXT,
            mb_release_group_id TEXT,
            rejection_category TEXT,
            scoring_breakdown_json TEXT,
            selected_bucket TEXT,
            failure_reasons_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(batch_id, source_index)
        )
        """
    )
    existing_columns = {
        str(row[1])
        for row in cur.execute("PRAGMA table_info(import_batch_items)").fetchall()
    }
    if "item_state" not in existing_columns:
        cur.execute("ALTER TABLE import_batch_items ADD COLUMN item_state TEXT")
    if "source_resolution_state" not in existing_columns:
        cur.execute("ALTER TABLE import_batch_items ADD COLUMN source_resolution_state TEXT")
    if "resolution_attempts" not in existing_columns:
        cur.execute("ALTER TABLE import_batch_items ADD COLUMN resolution_attempts INTEGER NOT NULL DEFAULT 0")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_batches_started_at ON import_batches (started_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_batches_phase ON import_batches (phase)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_batch_items_batch ON import_batch_items (batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_batch_items_outcome ON import_batch_items (outcome)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_batch_items_state ON import_batch_items (item_state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_batch_items_job ON import_batch_items (linked_job_id)")
    conn.commit()


def _with_import_conn(db_path: str | None) -> sqlite3.Connection | None:
    if not db_path:
        return None
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    ensure_import_batch_tables(conn)
    return conn


def _persist_import_batch(
    *,
    db_path: str | None,
    batch_id: str,
    source_format: str | None,
    phase: str,
    current_phase_detail: str | None,
    started_at: str,
    finished_at: str | None,
    total_tracks: int,
    processed_tracks: int,
    resolved_count: int,
    unresolved_count: int,
    enqueued_count: int,
    duplicate_skipped_count: int,
    failed_count: int,
    top_rejection_reasons: dict[str, int],
    selected_bucket_counts: dict[str, int],
) -> None:
    conn = _with_import_conn(db_path)
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO import_batches (
                batch_id, source_format, started_at, finished_at, phase, current_phase_detail,
                total_tracks, processed_tracks, resolved_count, unresolved_count, enqueued_count,
                duplicate_skipped_count, failed_count, top_rejection_reasons_json,
                selected_bucket_counts_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(batch_id) DO UPDATE SET
                source_format=excluded.source_format,
                finished_at=excluded.finished_at,
                phase=excluded.phase,
                current_phase_detail=excluded.current_phase_detail,
                total_tracks=excluded.total_tracks,
                processed_tracks=excluded.processed_tracks,
                resolved_count=excluded.resolved_count,
                unresolved_count=excluded.unresolved_count,
                enqueued_count=excluded.enqueued_count,
                duplicate_skipped_count=excluded.duplicate_skipped_count,
                failed_count=excluded.failed_count,
                top_rejection_reasons_json=excluded.top_rejection_reasons_json,
                selected_bucket_counts_json=excluded.selected_bucket_counts_json
            """,
            (
                batch_id,
                source_format,
                started_at,
                finished_at,
                phase,
                current_phase_detail,
                int(total_tracks),
                int(processed_tracks),
                int(resolved_count),
                int(unresolved_count),
                int(enqueued_count),
                int(duplicate_skipped_count),
                int(failed_count),
                _safe_json_dumps(top_rejection_reasons),
                _safe_json_dumps(selected_bucket_counts),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _persist_import_batch_item(
    *,
    db_path: str | None,
    batch_id: str,
    source_index: int,
    artist: str | None,
    title: str | None,
    album: str | None,
    album_artist: str | None,
    input_metadata: dict[str, Any],
    outcome: str,
    item_state: str | None,
    source_resolution_state: str | None,
    resolution_attempts: int | None,
    canonical_id: str | None,
    linked_job_id: str | None,
    linked_job_status: str | None,
    recording_mbid: str | None,
    mb_release_id: str | None,
    mb_release_group_id: str | None,
    rejection_category: str | None,
    scoring_breakdown: dict[str, Any] | None,
    selected_bucket: str | None,
    failure_reasons: list[str] | None,
) -> None:
    conn = _with_import_conn(db_path)
    if conn is None:
        return
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO import_batch_items (
                batch_id, source_index, item_state, source_resolution_state, resolution_attempts,
                artist, title, album, album_artist, input_metadata_json,
                outcome, canonical_id, linked_job_id, linked_job_status, recording_mbid,
                mb_release_id, mb_release_group_id, rejection_category, scoring_breakdown_json,
                selected_bucket, failure_reasons_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(batch_id, source_index) DO UPDATE SET
                item_state=excluded.item_state,
                source_resolution_state=excluded.source_resolution_state,
                resolution_attempts=excluded.resolution_attempts,
                artist=excluded.artist,
                title=excluded.title,
                album=excluded.album,
                album_artist=excluded.album_artist,
                input_metadata_json=excluded.input_metadata_json,
                outcome=excluded.outcome,
                canonical_id=excluded.canonical_id,
                linked_job_id=excluded.linked_job_id,
                linked_job_status=excluded.linked_job_status,
                recording_mbid=excluded.recording_mbid,
                mb_release_id=excluded.mb_release_id,
                mb_release_group_id=excluded.mb_release_group_id,
                rejection_category=excluded.rejection_category,
                scoring_breakdown_json=excluded.scoring_breakdown_json,
                selected_bucket=excluded.selected_bucket,
                failure_reasons_json=excluded.failure_reasons_json,
                updated_at=excluded.updated_at
            """,
            (
                batch_id,
                int(source_index),
                item_state,
                source_resolution_state,
                int(resolution_attempts or 0),
                artist,
                title,
                album,
                album_artist,
                _safe_json_dumps(input_metadata),
                outcome,
                canonical_id,
                linked_job_id,
                linked_job_status,
                recording_mbid,
                mb_release_id,
                mb_release_group_id,
                rejection_category,
                _safe_json_dumps(scoring_breakdown or {}),
                selected_bucket,
                _safe_json_dumps(list(failure_reasons or [])),
                now,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _decode_json_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        loaded = json.loads(value)
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _decode_json_list(value: Any) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        loaded = json.loads(value)
    except Exception:
        return []
    return loaded if isinstance(loaded, list) else []


def get_import_batch_summary(db_path: str | None, batch_id: str) -> dict[str, Any] | None:
    conn = _with_import_conn(db_path)
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM import_batches WHERE batch_id=? LIMIT 1", (str(batch_id or "").strip(),))
        row = cur.fetchone()
        if not row:
            return None
        summary = dict(row)
        summary["top_rejection_reasons"] = _decode_json_object(summary.pop("top_rejection_reasons_json", None))
        summary["selected_bucket_counts"] = _decode_json_object(summary.pop("selected_bucket_counts_json", None))
        cur.execute(
            """
            SELECT outcome, COUNT(*) AS count
            FROM import_batch_items
            WHERE batch_id=?
            GROUP BY outcome
            """,
            (str(batch_id or "").strip(),),
        )
        summary["item_outcomes"] = {
            str(item["outcome"] or ""): int(item["count"] or 0)
            for item in cur.fetchall()
            if str(item["outcome"] or "").strip()
        }
        return summary
    finally:
        conn.close()


def list_recent_import_batches(db_path: str | None, *, limit: int = 5) -> list[dict[str, Any]]:
    conn = _with_import_conn(db_path)
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM import_batches
            ORDER BY COALESCE(finished_at, started_at) DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        )
        rows = []
        for row in cur.fetchall():
            payload = dict(row)
            payload["top_rejection_reasons"] = _decode_json_object(payload.pop("top_rejection_reasons_json", None))
            payload["selected_bucket_counts"] = _decode_json_object(payload.pop("selected_bucket_counts_json", None))
            rows.append(payload)
        return rows
    finally:
        conn.close()


def mark_stale_import_batches_abandoned(db_path: str | None) -> int:
    conn = _with_import_conn(db_path)
    if conn is None:
        return 0
    try:
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE import_batches
            SET
                phase='failed',
                current_phase_detail='abandoned_after_restart',
                finished_at=?,
                failed_count=CASE
                    WHEN failed_count > 0 THEN failed_count
                    WHEN total_tracks > processed_tracks THEN total_tracks - processed_tracks
                    ELSE failed_count
                END
            WHERE finished_at IS NULL
              AND LOWER(COALESCE(phase, '')) IN ({})
            """.format(",".join("?" for _ in _STALE_IMPORT_BATCH_PHASES)),
            (now_iso, *sorted(_STALE_IMPORT_BATCH_PHASES)),
        )
        changed = int(cur.rowcount or 0)
        conn.commit()
        return changed
    finally:
        conn.close()


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
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_failures_created_at ON music_failures (created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_failures_batch ON music_failures (origin_batch_id)"
    )
    conn.commit()


def _resolve_failure_db_path(config: Any, queue_store: Any) -> str | None:
    if queue_store is not None:
        db_path = getattr(queue_store, "db_path", None)
        if db_path:
            return str(db_path)
    if isinstance(config, dict):
        db_path = config.get("queue_db_path")
        if db_path:
            return str(db_path)
    return None


class _CachedMusicBrainzService:
    def __init__(self, wrapped: Any):
        self._wrapped = wrapped
        self._lock = Lock()
        self._search_cache: dict[tuple[str | None, str, str | None, int], Any] = {}
        self._recording_cache: dict[tuple[str, tuple[str, ...]], Any] = {}
        self._release_cache: dict[tuple[str, tuple[str, ...]], Any] = {}

    def search_recordings(self, artist, title, *, album=None, limit=5):
        key = (
            str(artist or "").strip().lower() or None,
            str(title or "").strip().lower(),
            str(album or "").strip().lower() or None,
            int(limit or 5),
        )
        with self._lock:
            if key in self._search_cache:
                return self._search_cache[key]
        payload = self._wrapped.search_recordings(artist, title, album=album, limit=limit)
        with self._lock:
            self._search_cache[key] = payload
        return payload

    def get_recording(self, recording_id, *, includes=None):
        include_key = tuple(sorted(str(item).strip() for item in (includes or []) if str(item).strip()))
        key = (str(recording_id or "").strip(), include_key)
        with self._lock:
            if key in self._recording_cache:
                return self._recording_cache[key]
        payload = self._wrapped.get_recording(recording_id, includes=includes)
        with self._lock:
            self._recording_cache[key] = payload
        return payload

    def get_release(self, release_id, *, includes=None):
        include_key = tuple(sorted(str(item).strip() for item in (includes or []) if str(item).strip()))
        key = (str(release_id or "").strip(), include_key)
        with self._lock:
            if key in self._release_cache:
                return self._release_cache[key]
        payload = self._wrapped.get_release(release_id, includes=includes)
        with self._lock:
            self._release_cache[key] = payload
        return payload


def _record_music_failure(
    *,
    db_path: str | None,
    origin_batch_id: str,
    artist: str | None,
    track: str | None,
    reasons: list[str] | None = None,
    recording_mbid_attempted: str | None = None,
    last_query: str | None = None,
    retention_max_rows: int | None = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_ROWS,
    retention_max_age_days: int | None = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_AGE_DAYS,
) -> None:
    if not db_path:
        return
    safe_reasons = [str(r) for r in (reasons or []) if str(r or "").strip()]
    payload = {"reasons": safe_reasons}
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        try:
            _ensure_music_failures_table(conn)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO music_failures (
                    created_at,
                    origin_batch_id,
                    artist,
                    track,
                    reason_json,
                    recording_mbid_attempted,
                    last_query
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    origin_batch_id,
                    str(artist or "").strip() or None,
                    str(track or "").strip() or None,
                    json.dumps(payload, sort_keys=True),
                    str(recording_mbid_attempted or "").strip() or None,
                    str(last_query or "").strip() or None,
                ),
            )
            _prune_music_failures(
                conn,
                max_rows=retention_max_rows,
                max_age_days=retention_max_age_days,
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("music_failure_record_persist_failed")


def _prune_music_failures(
    conn: sqlite3.Connection,
    *,
    max_rows: int | None,
    max_age_days: int | None,
) -> None:
    cur = conn.cursor()
    if isinstance(max_age_days, int) and max_age_days > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).replace(microsecond=0).isoformat()
        cur.execute("DELETE FROM music_failures WHERE created_at < ?", (cutoff,))
    if isinstance(max_rows, int) and max_rows > 0:
        cur.execute(
            """
            DELETE FROM music_failures
            WHERE id IN (
                SELECT id
                FROM music_failures
                ORDER BY id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (int(max_rows),),
        )


def _build_query(intent: TrackIntent) -> str:
    if intent.artist and intent.title:
        return f"{intent.artist} - {intent.title}".strip()
    return str(intent.raw_line or "").strip()


def _get_musicbrainz_service(config: Any):
    if isinstance(config, dict):
        service = config.get("musicbrainz_service")
        if service is not None:
            return service
    from metadata.services.musicbrainz_service import get_musicbrainz_service

    return get_musicbrainz_service()


def _get_queue_store(config: Any):
    if isinstance(config, dict):
        queue_store = config.get("queue_store")
        if queue_store is not None:
            return queue_store
        queue_db_path = config.get("queue_db_path")
        if queue_db_path:
            return DownloadJobStore(str(queue_db_path))
    raise ValueError("queue_store (or queue_db_path) required")


def _get_job_payload_builder(config: Any):
    if isinstance(config, dict):
        builder = config.get("job_payload_builder")
        if callable(builder):
            return builder
    return build_download_job_payload


def _score_value(recording: dict[str, Any]) -> float | None:
    if not isinstance(recording, dict):
        return None
    raw = recording.get("score")
    if raw is None:
        raw = recording.get("ext:score")
    if raw is None:
        return None
    try:
        score = float(raw)
    except (TypeError, ValueError):
        return None
    if score > 1.0:
        score = score / 100.0
    return max(0.0, min(score, 1.0))


def _recording_sort_key(recording: dict[str, Any]):
    score = _score_value(recording)
    normalized_score = score if score is not None else 0.0
    recording_id = str(recording.get("id") or "")
    return (-normalized_score, recording_id)


def _select_recording_candidate(recordings: list[dict[str, Any]], *, threshold: float) -> dict[str, Any] | None:
    if not recordings:
        return None
    ranked = sorted(
        (entry for entry in recordings if isinstance(entry, dict)),
        key=_recording_sort_key,
    )
    if not ranked:
        return None
    selected = ranked[0]
    score = _score_value(selected)
    if score is not None and score < threshold:
        return None
    return selected


def _extract_recordings(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("recording-list"), list):
            return [item for item in payload.get("recording-list") if isinstance(item, dict)]
        if isinstance(payload.get("recordings"), list):
            return [item for item in payload.get("recordings") if isinstance(item, dict)]
    return []


def _extract_release_mbid(recording: dict[str, Any]) -> str | None:
    releases = recording.get("release-list")
    if isinstance(releases, list):
        for rel in releases:
            if isinstance(rel, dict):
                rid = str(rel.get("id") or "").strip()
                if rid:
                    return rid
    return None


def _extract_release_year(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) >= 4 and text[:4].isdigit():
        return text[:4]
    return None


def _normalized_tokens(value: Any) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    cleaned = []
    for ch in text:
        cleaned.append(ch if (ch.isalnum() or ch.isspace()) else " ")
    return {token for token in "".join(cleaned).split() if token}


def _token_overlap_ratio(left: Any, right: Any) -> float:
    lt = _normalized_tokens(left)
    rt = _normalized_tokens(right)
    if not lt or not rt:
        return 0.0
    return len(lt & rt) / max(len(lt), 1)


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _recording_track_position_in_release(release_payload: dict[str, Any], recording_mbid: str) -> tuple[int | None, int | None]:
    release = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
    media = release.get("medium-list", []) if isinstance(release, dict) else []
    if not isinstance(media, list):
        return None, None
    for medium in media:
        if not isinstance(medium, dict):
            continue
        disc_number = _safe_int(medium.get("position"))
        track_list = medium.get("track-list", [])
        if not isinstance(track_list, list):
            continue
        for track in track_list:
            if not isinstance(track, dict):
                continue
            recording = track.get("recording")
            rec = recording if isinstance(recording, dict) else {}
            rec_id = str(rec.get("id") or "").strip()
            if rec_id != recording_mbid:
                continue
            track_number = _safe_int(track.get("position"))
            return track_number, disc_number
    return None, None


def _resolve_bound_mb_pair(
    mb_service: Any,
    *,
    artist: str | None,
    track: str,
    album: str | None = None,
    album_artist: str | None = None,
    duration_ms: int | None = None,
    country_preference: str = "US",
    threshold: float = _DEFAULT_CONFIDENCE_THRESHOLD,
    resolution_profile: str | None = None,
    context_hint: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    return resolve_best_mb_pair(
        mb_service,
        artist=artist,
        track=track,
        album=album,
        album_artist=album_artist,
        duration_ms=duration_ms,
        country_preference=country_preference,
        resolution_profile=resolution_profile,
        context_hint=context_hint,
        allow_non_album_fallback=bool(resolution_profile),
        min_recording_score=float(threshold or 0.0),
        threshold=float(threshold or _DEFAULT_CONFIDENCE_THRESHOLD),
        max_duration_delta_ms=_DEFAULT_IMPORT_DURATION_DELTA_MS if resolution_profile else None,
    )


def _canonical_artist(recording: dict[str, Any], fallback_artist: str | None = None) -> str:
    credits = recording.get("artist-credit")
    if isinstance(credits, list):
        parts: list[str] = []
        for ac in credits:
            if isinstance(ac, str):
                parts.append(ac)
                continue
            if isinstance(ac, dict):
                artist_obj = ac.get("artist") if isinstance(ac.get("artist"), dict) else {}
                name = str(ac.get("name") or artist_obj.get("name") or "").strip()
                if name:
                    parts.append(name)
                joinphrase = str(ac.get("joinphrase") or "").strip()
                if joinphrase:
                    parts.append(joinphrase)
        artist = "".join(parts).strip()
        if artist:
            return artist
    return str(fallback_artist or "").strip()


def _enqueue_music_track_job(
    queue_store: Any,
    job_payload_builder,
    *,
    runtime_config,
    base_dir,
    destination,
    final_format_override,
    import_batch_id: str,
    playlist_name: str | None,
    source_index: int,
    recording_mbid: str,
    release_mbid: str | None,
    release_group_mbid: str | None,
    artist: str,
    title: str,
    album: str | None,
    release_date: str | None,
    track_number: int | None,
    disc_number: int | None,
    disc_total: int | None,
    duration_ms: int | None,
    track_aliases: list[str] | None = None,
    track_disambiguation: str | None = None,
    mb_recording_title: str | None = None,
    mb_youtube_urls: list[str] | None = None,
    media_mode: str = "music",
    import_max_concurrent_downloads: int | None = None,
    force_requeue: bool = False,
) -> tuple[str | None, bool, str | None]:
    canonical_id = build_music_track_canonical_id(
        artist,
        album,
        track_number,
        title,
        recording_mbid=recording_mbid,
        mb_release_id=release_mbid,
        mb_release_group_id=release_group_mbid,
        disc_number=disc_number,
    )
    canonical_metadata = {
        "artist": artist,
        "track": title,
        "album": album,
        "release_date": release_date,
        "track_number": track_number,
        "disc_number": disc_number,
        "disc_total": disc_total,
        "duration_ms": duration_ms,
        "recording_mbid": recording_mbid,
        "mb_recording_id": recording_mbid,
        "mb_release_id": release_mbid,
        "mb_release_group_id": release_group_mbid,
        "track_aliases": list(track_aliases or []),
        "track_disambiguation": track_disambiguation,
        "mb_recording_title": mb_recording_title,
        "mb_youtube_urls": list(mb_youtube_urls or []),
    }
    normalized_media_mode = str(media_mode or "music").strip().lower()
    media_type = "video" if normalized_media_mode == "music_video" else "music"
    placeholder_url = f"musicbrainz://recording/{recording_mbid}"
    enqueue_payload = job_payload_builder(
        config=runtime_config,
        origin="import",
        origin_id=import_batch_id,
        media_type=media_type,
        media_intent="music_track",
        source="music_import",
        url=placeholder_url,
        input_url=placeholder_url,
        destination=destination,
        base_dir=base_dir,
        final_format_override=final_format_override,
        resolved_metadata=canonical_metadata,
        output_template_overrides={
            "kind": "music_track",
            "source": "import",
            "import_batch": import_batch_id,
            "import_batch_id": import_batch_id,
            "playlist_name": str(playlist_name or "").strip() or None,
            "source_index": source_index,
            "track_number": track_number,
            "disc_number": disc_number,
            "disc_total": disc_total,
            "release_date": release_date,
            "duration_ms": duration_ms,
            "recording_mbid": recording_mbid,
            "mb_recording_id": recording_mbid,
            "mb_release_id": release_mbid,
            "mb_release_group_id": release_group_mbid,
            "track_aliases": list(track_aliases or []),
            "track_disambiguation": track_disambiguation,
            "mb_recording_title": mb_recording_title,
            "mb_youtube_urls": list(mb_youtube_urls or []),
            "import_max_concurrent_downloads": int(import_max_concurrent_downloads) if import_max_concurrent_downloads else None,
            "audio_mode": media_type == "music",
        },
        canonical_id=canonical_id,
    )
    enqueue_payload["force_requeue"] = bool(force_requeue)
    return queue_store.enqueue_job(**enqueue_payload)


def process_imported_tracks(track_intents: list[TrackIntent], config) -> ImportResult:
    mb_service = _CachedMusicBrainzService(_get_musicbrainz_service(config))
    queue_store = _get_queue_store(config)
    job_payload_builder = _get_job_payload_builder(config)
    import_batch_id = uuid4().hex
    failure_db_path = _resolve_failure_db_path(config, queue_store)
    confidence_threshold = _DEFAULT_CONFIDENCE_THRESHOLD
    runtime_config = config.get("app_config") if isinstance(config, dict) and isinstance(config.get("app_config"), dict) else (config if isinstance(config, dict) else {})
    media_mode = "music"
    playlist_name = None
    if isinstance(config, dict):
        requested_media_mode = str(config.get("media_mode") or "").strip().lower()
        if requested_media_mode in {"music", "music_video"}:
            media_mode = requested_media_mode
        playlist_name = str(config.get("playlist_name") or "").strip() or None
    base_dir = "/downloads"
    destination = None
    final_format_override = None
    import_max_concurrent_downloads = None
    if isinstance(config, dict):
        threshold_source = config
        if isinstance(runtime_config, dict) and runtime_config:
            threshold_source = runtime_config
        try:
            threshold_value = None
            if isinstance(threshold_source, dict):
                threshold_value = threshold_source.get("music_mb_binding_threshold")
                if threshold_value is None:
                    threshold_value = threshold_source.get("min_confidence")
            if threshold_value is not None:
                confidence_threshold = float(threshold_value)
        except (TypeError, ValueError):
            confidence_threshold = _DEFAULT_CONFIDENCE_THRESHOLD
        if confidence_threshold > 1.0:
            confidence_threshold = confidence_threshold / 100.0
        confidence_threshold = max(0.0, min(confidence_threshold, 1.0))

        base_dir = config.get("base_dir") or base_dir
        destination = config.get("destination_dir")
        final_format_override = config.get("final_format")
        cap_value = config.get("import_max_concurrent_downloads")
        if cap_value is not None:
            try:
                import_max_concurrent_downloads = max(1, int(cap_value))
            except (TypeError, ValueError):
                import_max_concurrent_downloads = None
    if isinstance(runtime_config, dict):
        effective_defaults = resolve_effective_download_settings(
            runtime_config,
            media_mode=media_mode,
            destination=destination,
            final_format_override=final_format_override,
            fallback_destination=base_dir,
        )
        destination = effective_defaults.get("destination")
        final_format_override = effective_defaults.get("final_format")
    progress_callback = config.get("progress_callback") if isinstance(config, dict) else None

    total_tracks = len(track_intents)
    resolved_count = 0
    unresolved_count = 0
    enqueued_count = 0
    duplicate_skipped_count = 0
    failed_count = 0
    resolved_mbids: list[str] = []
    processed_tracks = 0
    source_formats = {
        str(getattr(intent, "source_format", "") or "").strip().lower()
        for intent in (track_intents or [])
        if str(getattr(intent, "source_format", "") or "").strip()
    }
    source_format = sorted(source_formats)[0] if source_formats else "library_import"
    is_library_import = (not source_format) or source_format in {"apple_xml", "library_xml", "csv", "m3u"}
    top_rejection_reasons: Counter[str] = Counter()
    selected_bucket_counts: Counter[str] = Counter()
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    batch_db_path = failure_db_path
    library_duplicate_index = _load_library_duplicate_index(batch_db_path)

    mb_binding_workers = _DEFAULT_MB_BINDING_WORKERS
    if isinstance(runtime_config, dict):
        worker_value = runtime_config.get("import_mb_binding_workers")
        if worker_value is None and isinstance(config, dict):
            worker_value = config.get("import_mb_binding_workers")
        if worker_value is not None:
            try:
                mb_binding_workers = int(worker_value)
            except (TypeError, ValueError):
                mb_binding_workers = _DEFAULT_MB_BINDING_WORKERS
    mb_binding_workers = max(1, min(int(mb_binding_workers), _MAX_MB_BINDING_WORKERS))

    retention_max_rows = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_ROWS
    retention_max_age_days = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_AGE_DAYS
    if isinstance(runtime_config, dict):
        rows_value = runtime_config.get("music_failures_retention_max_rows")
        if rows_value is None and isinstance(config, dict):
            rows_value = config.get("music_failures_retention_max_rows")
        if rows_value is not None:
            try:
                retention_max_rows = int(rows_value)
            except (TypeError, ValueError):
                retention_max_rows = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_ROWS
        if retention_max_rows < 0:
            retention_max_rows = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_ROWS

        days_value = runtime_config.get("music_failures_retention_max_age_days")
        if days_value is None and isinstance(config, dict):
            days_value = config.get("music_failures_retention_max_age_days")
        if days_value is not None:
            try:
                retention_max_age_days = int(days_value)
            except (TypeError, ValueError):
                retention_max_age_days = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_AGE_DAYS
        if retention_max_age_days < 0:
            retention_max_age_days = _DEFAULT_MUSIC_FAILURES_RETENTION_MAX_AGE_DAYS

    def _record_failure(
        *,
        artist: str | None,
        track: str | None,
        reasons: list[str] | None,
        recording_mbid_attempted: str | None,
        last_query: str | None,
    ) -> None:
        _record_music_failure(
            db_path=failure_db_path,
            origin_batch_id=import_batch_id,
            artist=artist,
            track=track,
            reasons=reasons,
            recording_mbid_attempted=recording_mbid_attempted,
            last_query=last_query,
            retention_max_rows=retention_max_rows,
            retention_max_age_days=retention_max_age_days,
        )

    def _persist_batch_state(*, phase: str, current_phase_detail: str | None = None, finished_at: str | None = None) -> None:
        _persist_import_batch(
            db_path=batch_db_path,
            batch_id=import_batch_id,
            source_format=source_format,
            phase=phase,
            current_phase_detail=current_phase_detail,
            started_at=started_at,
            finished_at=finished_at,
            total_tracks=total_tracks,
            processed_tracks=processed_tracks,
            resolved_count=resolved_count,
            unresolved_count=unresolved_count,
            enqueued_count=enqueued_count,
            duplicate_skipped_count=duplicate_skipped_count,
            failed_count=failed_count,
            top_rejection_reasons=dict(top_rejection_reasons.most_common(10)),
            selected_bucket_counts=dict(selected_bucket_counts),
        )

    def _emit_progress(
        *,
        phase: str,
        processed_tracks: int,
        current_phase_detail: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if processed_tracks == total_tracks or processed_tracks == 0 or (processed_tracks % _DEFAULT_IMPORT_PROGRESS_INTERVAL) == 0:
            _persist_batch_state(phase=phase, current_phase_detail=current_phase_detail)
        if not callable(progress_callback):
            return
        try:
            payload = {
                "total_tracks": int(total_tracks),
                "processed_tracks": int(processed_tracks),
                "resolved_count": int(resolved_count),
                "unresolved_count": int(unresolved_count),
                "enqueued_count": int(enqueued_count),
                "duplicate_skipped_count": int(duplicate_skipped_count),
                "failed_count": int(failed_count),
                "phase": str(phase or "resolving"),
                "current_phase_detail": current_phase_detail,
                "top_rejection_reasons": dict(top_rejection_reasons.most_common(5)),
                "selected_bucket_counts": dict(selected_bucket_counts),
                "batch_id": import_batch_id,
            }
            if isinstance(extra, dict):
                payload.update(extra)
            progress_callback(payload)
        except Exception:
            logger.exception("import_progress_callback_failed")

    def _resolve_intent(entry: dict[str, Any], *, context_hint: dict[str, Any] | None = None) -> dict[str, Any]:
        selected_pair = _resolve_bound_mb_pair(
            mb_service,
            artist=entry["artist"],
            track=entry["title"],
            album=entry["album"],
            album_artist=entry.get("album_artist"),
            duration_ms=entry["duration_ms"],
            country_preference="US",
            threshold=confidence_threshold,
            resolution_profile="library_import" if is_library_import else None,
            context_hint=context_hint,
        )
        return {
            "selected_pair": selected_pair,
            "failure_reasons": list(getattr(resolve_best_mb_pair, "last_failure_reasons", []) or []),
            "resolution_diagnostics": dict(getattr(resolve_best_mb_pair, "last_resolution_diagnostics", {}) or {}),
        }

    def _entry_dedupe_key(entry: dict[str, Any]) -> tuple[str, str, str]:
        def _normalized(value: Any) -> str:
            text = str(value or "").strip().lower()
            if not text:
                return ""
            return " ".join(text.split())

        return (
            _normalized(entry.get("artist")),
            _normalized(entry.get("title")),
            _normalized(entry.get("album")),
        )

    def _cluster_key(entry: dict[str, Any]) -> tuple[str, str]:
        artist_key = str(entry.get("album_artist") or entry.get("artist") or "").strip().lower()
        album_key = str(entry.get("album") or "").strip().lower()
        return (artist_key, album_key)

    def _fallback_duplicate_status(canonical_id: str | None) -> dict[str, Any] | None:
        if not canonical_id or not hasattr(queue_store, "get_job_by_canonical_id"):
            return None
        try:
            existing = queue_store.get_job_by_canonical_id(canonical_id)
        except Exception:
            return None
        if existing is None:
            return None
        status = str(getattr(existing, "status", "") or "").strip().lower()
        classification = "active_existing"
        if status == JOB_STATUS_COMPLETED and str(getattr(existing, "file_path", "") or "").strip():
            classification = "completed_valid"
        elif status in {JOB_STATUS_FAILED, JOB_STATUS_CANCELLED}:
            classification = "terminal_retryable"
        return {
            "job_id": getattr(existing, "id", None),
            "status": status,
            "classification": classification,
            "stale": False,
        }

    def _classify_duplicate(canonical_id: str | None, *, placeholder_url: str | None) -> dict[str, Any] | None:
        if hasattr(queue_store, "classify_duplicate_job"):
            try:
                return queue_store.classify_duplicate_job(
                    canonical_id=canonical_id,
                    url=placeholder_url,
                    destination=destination,
                )
            except Exception:
                logger.exception("duplicate_classification_failed")
        return _fallback_duplicate_status(canonical_id)

    def _record_item(
        entry: dict[str, Any],
        *,
        outcome: str,
        item_state: str | None = None,
        source_resolution_state: str | None = None,
        resolution_attempts: int | None = None,
        canonical_id: str | None,
        linked_job_id: str | None,
        linked_job_status: str | None,
        recording_mbid: str | None,
        mb_release_id: str | None,
        mb_release_group_id: str | None,
        rejection_category: str | None,
        scoring_breakdown: dict[str, Any] | None,
        selected_bucket: str | None,
        failure_reasons: list[str] | None,
    ) -> None:
        _persist_import_batch_item(
            db_path=batch_db_path,
            batch_id=import_batch_id,
            source_index=int(entry["idx"]),
            artist=entry.get("artist"),
            title=entry.get("title"),
            album=entry.get("album"),
            album_artist=entry.get("album_artist"),
            input_metadata={
                "artist": entry.get("artist"),
                "title": entry.get("title"),
                "album": entry.get("album"),
                "album_artist": entry.get("album_artist"),
                "track_number": entry.get("track_number"),
                "disc_number": entry.get("disc_number"),
                "duration_ms": entry.get("duration_ms"),
                "release_date": entry.get("release_date"),
                "genre": entry.get("genre"),
                "query": entry.get("query"),
            },
            outcome=outcome,
            item_state=item_state or outcome,
            source_resolution_state=source_resolution_state or "not_started",
            resolution_attempts=resolution_attempts,
            canonical_id=canonical_id,
            linked_job_id=linked_job_id,
            linked_job_status=linked_job_status,
            recording_mbid=recording_mbid,
            mb_release_id=mb_release_id,
            mb_release_group_id=mb_release_group_id,
            rejection_category=rejection_category,
            scoring_breakdown=scoring_breakdown,
            selected_bucket=selected_bucket,
            failure_reasons=failure_reasons,
        )

    logger.info(
        {
            "message": "import_batch_started",
            "batch_id": import_batch_id,
            "source_format": source_format,
            "total_tracks": total_tracks,
            "mb_workers": mb_binding_workers,
        }
    )
    _persist_batch_state(phase="resolving", current_phase_detail="phase_1_initial_resolution")
    _emit_progress(phase="resolving", processed_tracks=processed_tracks, current_phase_detail="phase_1_initial_resolution")
    entries: list[dict[str, Any]] = []
    for idx, intent in enumerate(track_intents, start=1):
        query = _build_query(intent)
        artist = str(intent.artist or "").strip() or None
        title = str(intent.title or "").strip() or query
        album = str(intent.album or "").strip() or None
        duration_ms = getattr(intent, "duration_ms", None)
        try:
            duration_ms = int(duration_ms) if duration_ms is not None else None
        except (TypeError, ValueError):
            duration_ms = None
        entries.append(
            {
                "idx": idx,
                "query": query,
                "artist": artist,
                "title": title,
                "album": album,
                "album_artist": str(getattr(intent, "album_artist", "") or "").strip() or None,
                "track_number": _safe_int(getattr(intent, "track_number", None)),
                "disc_number": _safe_int(getattr(intent, "disc_number", None)),
                "release_date": str(getattr(intent, "release_date", "") or "").strip() or None,
                "genre": str(getattr(intent, "genre", "") or "").strip() or None,
                "duration_ms": duration_ms,
            }
        )

    for entry in entries:
        _record_item(
            entry,
            outcome="pending_metadata_resolution",
            item_state="pending_metadata_resolution",
            source_resolution_state="not_started",
            canonical_id=None,
            linked_job_id=None,
            linked_job_status=None,
            recording_mbid=None,
            mb_release_id=None,
            mb_release_group_id=None,
            rejection_category=None,
            scoring_breakdown=None,
            selected_bucket=None,
            failure_reasons=None,
        )

    dedupe_buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for entry in entries:
        key = _entry_dedupe_key(entry)
        dedupe_buckets.setdefault(key, []).append(entry)

    processed_resolution_keys: set[tuple[str, str, str]] = set()

    def _handle_resolved_bucket(bucket: list[dict[str, Any]], selected_pair: dict[str, Any]) -> None:
        nonlocal resolved_count
        nonlocal unresolved_count
        nonlocal enqueued_count
        nonlocal duplicate_skipped_count
        nonlocal failed_count
        nonlocal processed_tracks

        selected_pair = selected_pair if isinstance(selected_pair, dict) else {}
        selected_pair_error = str(selected_pair.get("_error") or "").strip()
        for entry in bucket:
            query = str(entry["query"] or "").strip()
            artist = entry["artist"]
            title = str(entry["title"] or "").strip() or query
            album = entry["album"]
            try:
                if selected_pair_error:
                    failed_count += 1
                    _record_failure(
                        artist=artist,
                        track=title,
                        reasons=["import_exception", selected_pair_error],
                        recording_mbid_attempted=None,
                        last_query=query,
                    )
                    _record_item(
                        entry,
                        outcome="failed_exception",
                        item_state="failed",
                        source_resolution_state="not_started",
                        resolution_attempts=1,
                        canonical_id=None,
                        linked_job_id=None,
                        linked_job_status=None,
                        recording_mbid=None,
                        mb_release_id=None,
                        mb_release_group_id=None,
                        rejection_category="import_exception",
                        scoring_breakdown=None,
                        selected_bucket=None,
                        failure_reasons=["import_exception", selected_pair_error],
                    )
                    processed_tracks += 1
                    _emit_progress(phase="resolving", processed_tracks=processed_tracks, current_phase_detail="metadata_resolved_streaming")
                    continue

                recording_mbid = str(selected_pair.get("recording_mbid") or "").strip()
                if not recording_mbid:
                    return

                release_mbid = str(selected_pair.get("mb_release_id") or "").strip() or None
                release_group_mbid = str(selected_pair.get("mb_release_group_id") or "").strip() or None
                canonical_artist = artist or str(selected_pair.get("matched_artist") or "").strip()
                canonical_title = title
                canonical_album = str(selected_pair.get("album") or album or "").strip() or None
                release_date_raw = str(selected_pair.get("release_date") or entry.get("release_date") or "").strip() or None
                release_date = _extract_release_year(release_date_raw) or release_date_raw
                track_number = _safe_int(selected_pair.get("track_number")) or _safe_int(entry.get("track_number"))
                disc_number = _safe_int(selected_pair.get("disc_number")) or _safe_int(entry.get("disc_number")) or 1
                disc_total = _safe_int(selected_pair.get("disc_total"))
                resolved_duration_ms = _safe_int(selected_pair.get("duration_ms")) or _safe_int(entry.get("duration_ms"))
                normalized_aliases = [
                    str(value).strip()
                    for value in (selected_pair.get("track_aliases") or [])
                    if str(value or "").strip()
                ]
                selected_track_disambiguation = str(selected_pair.get("track_disambiguation") or "").strip() or None
                selected_recording_title = str(selected_pair.get("mb_recording_title") or "").strip() or None
                normalized_mb_youtube_urls = [
                    str(value).strip()
                    for value in (selected_pair.get("mb_youtube_urls") or [])
                    if str(value or "").strip()
                ]
                selected_bucket = str(selected_pair.get("selected_bucket") or "").strip() or None
                scoring_breakdown = selected_pair.get("scoring_breakdown")
                canonical_id = build_music_track_canonical_id(
                    canonical_artist,
                    canonical_album,
                    track_number,
                    canonical_title,
                    recording_mbid=recording_mbid,
                    mb_release_id=release_mbid,
                    mb_release_group_id=release_group_mbid,
                    disc_number=disc_number,
                )
                library_duplicate = _classify_library_duplicate(
                    library_duplicate_index,
                    artist=canonical_artist,
                    album=canonical_album,
                    title=canonical_title,
                    album_artist=entry.get("album_artist"),
                    recording_mbid=recording_mbid,
                    mb_release_id=release_mbid,
                    mb_release_group_id=release_group_mbid,
                )
                if library_duplicate:
                    duplicate_skipped_count += 1
                    resolved_count += 1
                    resolved_mbids.append(recording_mbid)
                    _record_item(
                        entry,
                        outcome="resolved_duplicate_existing",
                        item_state="duplicate",
                        source_resolution_state="skipped_existing",
                        resolution_attempts=1,
                        canonical_id=canonical_id,
                        linked_job_id=None,
                        linked_job_status="library_present",
                        recording_mbid=recording_mbid,
                        mb_release_id=release_mbid,
                        mb_release_group_id=release_group_mbid,
                        rejection_category=None,
                        scoring_breakdown=scoring_breakdown,
                        selected_bucket=selected_bucket,
                        failure_reasons=None,
                    )
                    processed_tracks += 1
                    _emit_progress(phase="resolving", processed_tracks=processed_tracks, current_phase_detail="metadata_resolved_streaming")
                    continue
                duplicate = _classify_duplicate(canonical_id, placeholder_url=f"musicbrainz://recording/{recording_mbid}")
                force_requeue = bool(duplicate and str(duplicate.get("classification") or "").startswith("stale_"))
                if duplicate and str(duplicate.get("classification") or "") in {"completed_valid", "active_existing"}:
                    duplicate_skipped_count += 1
                    resolved_count += 1
                    resolved_mbids.append(recording_mbid)
                    _record_item(
                        entry,
                        outcome="resolved_duplicate_existing",
                        item_state="duplicate",
                        source_resolution_state="skipped_existing",
                        resolution_attempts=1,
                        canonical_id=canonical_id,
                        linked_job_id=str(duplicate.get("job_id") or "").strip() or None,
                        linked_job_status=str(duplicate.get("status") or "").strip() or None,
                        recording_mbid=recording_mbid,
                        mb_release_id=release_mbid,
                        mb_release_group_id=release_group_mbid,
                        rejection_category=None,
                        scoring_breakdown=scoring_breakdown,
                        selected_bucket=selected_bucket,
                        failure_reasons=None,
                    )
                    processed_tracks += 1
                    _emit_progress(phase="resolving", processed_tracks=processed_tracks, current_phase_detail="metadata_resolved_streaming")
                    continue

                job_id, created, enqueue_reason = _enqueue_music_track_job(
                    queue_store,
                    job_payload_builder,
                    runtime_config=runtime_config,
                    base_dir=base_dir,
                    destination=destination,
                    final_format_override=final_format_override,
                    import_batch_id=import_batch_id,
                    playlist_name=playlist_name,
                    source_index=int(entry["idx"]) - 1,
                    recording_mbid=recording_mbid,
                    release_mbid=release_mbid,
                    release_group_mbid=release_group_mbid,
                    artist=canonical_artist,
                    title=canonical_title,
                    album=canonical_album,
                    release_date=release_date,
                    track_number=track_number,
                    disc_number=disc_number,
                    disc_total=disc_total,
                    duration_ms=resolved_duration_ms,
                    track_aliases=normalized_aliases,
                    track_disambiguation=selected_track_disambiguation,
                    mb_recording_title=selected_recording_title,
                    mb_youtube_urls=normalized_mb_youtube_urls,
                    media_mode=media_mode,
                    import_max_concurrent_downloads=import_max_concurrent_downloads,
                    force_requeue=force_requeue,
                )
                resolved_count += 1
                resolved_mbids.append(recording_mbid)
                linked_job_status = str(duplicate.get("status") or "").strip() or None if duplicate else None
                if created:
                    enqueued_count += 1
                elif enqueue_reason == "duplicate":
                    duplicate_skipped_count += 1
                _record_item(
                    entry,
                    outcome=(
                        "resolved_duplicate_stale"
                        if force_requeue
                        else ("resolved_and_enqueued" if created else "resolved_duplicate_existing")
                    ),
                    item_state="queued_for_download" if created or force_requeue else "duplicate",
                    source_resolution_state="queued_for_source_resolution",
                    resolution_attempts=1,
                    canonical_id=canonical_id,
                    linked_job_id=str(job_id or "").strip() or None,
                    linked_job_status=linked_job_status or ("queued" if created else None),
                    recording_mbid=recording_mbid,
                    mb_release_id=release_mbid,
                    mb_release_group_id=release_group_mbid,
                    rejection_category=None,
                    scoring_breakdown=scoring_breakdown,
                    selected_bucket=selected_bucket,
                    failure_reasons=None,
                )
            except Exception as exc:
                failed_count += 1
                _record_failure(
                    artist=artist,
                    track=title,
                    reasons=["import_exception", str(exc)],
                    recording_mbid_attempted=None,
                    last_query=query,
                )
                _record_item(
                    entry,
                    outcome="failed_exception",
                    item_state="failed",
                    source_resolution_state="not_started",
                    resolution_attempts=1,
                    canonical_id=None,
                    linked_job_id=None,
                    linked_job_status=None,
                    recording_mbid=None,
                    mb_release_id=None,
                    mb_release_group_id=None,
                    rejection_category="import_exception",
                    scoring_breakdown=None,
                    selected_bucket=None,
                    failure_reasons=["import_exception", str(exc)],
                )
            processed_tracks += 1
            _emit_progress(phase="resolving", processed_tracks=processed_tracks, current_phase_detail="metadata_resolved_streaming")

    def _resolve_phase(
        buckets: list[list[dict[str, Any]]],
        *,
        phase_detail: str,
        context_resolver=None,
    ) -> tuple[dict[tuple[str, str, str], dict[str, Any] | None], dict[tuple[str, str], Counter[str]], list[list[dict[str, Any]]]]:
        pending: dict[Any, tuple[tuple[str, str, str], list[dict[str, Any]]]] = {}
        results: dict[tuple[str, str, str], dict[str, Any] | None] = {}
        clusters: dict[tuple[str, str], Counter[str]] = {}
        unresolved_buckets: list[list[dict[str, Any]]] = []
        submitted_tracks = 0
        resolved_tracks = 0
        with ThreadPoolExecutor(max_workers=min(mb_binding_workers, max(1, len(buckets)))) as pool:
            for bucket in buckets:
                exemplar = bucket[0]
                if not exemplar["query"]:
                    unresolved_buckets.append(bucket)
                    continue
                context_hint = context_resolver(exemplar) if callable(context_resolver) else None
                future = pool.submit(_resolve_intent, exemplar, context_hint=context_hint)
                pending[future] = (_entry_dedupe_key(exemplar), bucket)
                submitted_tracks += len(bucket)

            if submitted_tracks > 0:
                _emit_progress(
                    phase="resolving",
                    processed_tracks=processed_tracks,
                    current_phase_detail=phase_detail,
                    extra={
                        "resolution_processed_tracks": 0,
                        "resolution_total_tracks": int(submitted_tracks),
                        "message": f"Resolving metadata 0 / {submitted_tracks}...",
                    },
                )

            for future in as_completed(pending):
                dedupe_key, bucket = pending[future]
                try:
                    resolution_payload = future.result() or {}
                    selected_pair = resolution_payload.get("selected_pair")
                    diagnostics = dict(resolution_payload.get("resolution_diagnostics") or {})
                    failure_reasons = list(resolution_payload.get("failure_reasons") or [])
                    if not selected_pair:
                        selected_pair = {
                            "_failure_reasons": failure_reasons,
                            "_resolution_diagnostics": diagnostics,
                        }
                except Exception as exc:
                    selected_pair = {"_error": str(exc)}
                results[dedupe_key] = selected_pair
                pair = selected_pair if isinstance(selected_pair, dict) else None
                if pair and not pair.get("_error") and pair.get("recording_mbid"):
                    cluster = _cluster_key(bucket[0])
                    release_id = str(pair.get("mb_release_id") or pair.get("mb_release_group_id") or "").strip()
                    if release_id:
                        clusters.setdefault(cluster, Counter())[release_id] += len(bucket)
                    selected_bucket = str(pair.get("selected_bucket") or "").strip()
                    if selected_bucket:
                        selected_bucket_counts[selected_bucket] += len(bucket)
                    _handle_resolved_bucket(bucket, pair)
                    processed_resolution_keys.add(dedupe_key)
                elif pair and pair.get("_error"):
                    _handle_resolved_bucket(bucket, pair)
                    processed_resolution_keys.add(dedupe_key)
                else:
                    unresolved_buckets.append(bucket)
                    failure_reasons = list((pair or {}).get("_failure_reasons") or [])
                    top_rejection_reasons.update(reason for reason in failure_reasons if reason)
                resolved_tracks += len(bucket)
                _emit_progress(
                    phase="resolving",
                    processed_tracks=processed_tracks,
                    current_phase_detail=phase_detail,
                    extra={
                        "resolution_processed_tracks": int(resolved_tracks),
                        "resolution_total_tracks": int(submitted_tracks),
                        "message": f"Resolving metadata {resolved_tracks} / {submitted_tracks}...",
                    },
                )
        return results, clusters, unresolved_buckets

    phase_1_results, cluster_counts, unresolved_buckets = _resolve_phase(
        list(dedupe_buckets.values()),
        phase_detail="phase_1_initial_resolution",
    )

    def _phase_2_context(entry: dict[str, Any]) -> dict[str, Any] | None:
        cluster = _cluster_key(entry)
        counts = cluster_counts.get(cluster)
        if not counts:
            return None
        release_id, weight = counts.most_common(1)[0]
        if weight <= 0:
            return None
        return {"preferred_release_id": release_id, "cluster_hits": int(weight)}

    phase_2_results, _ignored_clusters, _ignored_unresolved = _resolve_phase(
        unresolved_buckets,
        phase_detail="phase_2_contextual_resolution",
        context_resolver=_phase_2_context,
    )
    resolution_results = {**phase_1_results, **phase_2_results}

    logger.info(
        {
            "message": "import_batch_progress",
            "batch_id": import_batch_id,
            "phase": "resolution_complete",
            "total_tracks": total_tracks,
            "resolved_candidates": sum(1 for value in resolution_results.values() if isinstance(value, dict) and value.get("recording_mbid")),
            "unresolved_candidates": sum(1 for value in resolution_results.values() if not (isinstance(value, dict) and value.get("recording_mbid"))),
        }
    )

    _persist_batch_state(phase="enqueueing", current_phase_detail="queueing_resolved_tracks")
    for bucket in dedupe_buckets.values():
        exemplar = bucket[0]
        if _entry_dedupe_key(exemplar) in processed_resolution_keys:
            continue
        selected_pair = resolution_results.get(_entry_dedupe_key(exemplar))
        selected_pair = selected_pair if isinstance(selected_pair, dict) else None
        selected_pair_error = str(selected_pair.get("_error") or "").strip() if selected_pair else ""
        for entry in bucket:
            idx = int(entry["idx"])
            query = str(entry["query"] or "").strip()
            artist = entry["artist"]
            title = str(entry["title"] or "").strip() or query
            album = entry["album"]
            try:
                if selected_pair_error:
                    failed_count += 1
                    _record_failure(
                        artist=artist,
                        track=title,
                        reasons=["import_exception", selected_pair_error],
                        recording_mbid_attempted=None,
                        last_query=query,
                    )
                    _record_item(
                        entry,
                        outcome="failed_exception",
                        canonical_id=None,
                        linked_job_id=None,
                        linked_job_status=None,
                        recording_mbid=None,
                        mb_release_id=None,
                        mb_release_group_id=None,
                        rejection_category="import_exception",
                        scoring_breakdown=None,
                        selected_bucket=None,
                        failure_reasons=["import_exception", selected_pair_error],
                    )
                    processed_tracks += 1
                    _emit_progress(phase="enqueueing", processed_tracks=processed_tracks, current_phase_detail="queueing_resolved_tracks")
                    continue
                if not selected_pair or selected_pair.get("_failure_reasons") is not None:
                    reasons = list((selected_pair or {}).get("_failure_reasons") or ["mb_pair_not_found"])
                    rejection_category = reasons[0] if reasons else "mb_pair_not_found"
                    unresolved_count += 1
                    top_rejection_reasons.update(reasons)
                    _record_failure(
                        artist=artist,
                        track=title,
                        reasons=reasons,
                        recording_mbid_attempted=None,
                        last_query=query,
                    )
                    _record_item(
                        entry,
                        outcome="unresolved_rejected_by_policy" if rejection_category != "no_recording_candidates" else "unresolved_no_candidate",
                        canonical_id=None,
                        linked_job_id=None,
                        linked_job_status=None,
                        recording_mbid=None,
                        mb_release_id=None,
                        mb_release_group_id=None,
                        rejection_category=rejection_category,
                        scoring_breakdown=None,
                        selected_bucket=None,
                        failure_reasons=reasons,
                    )
                    processed_tracks += 1
                    _emit_progress(phase="enqueueing", processed_tracks=processed_tracks, current_phase_detail="queueing_resolved_tracks")
                    continue
                recording_mbid = str(selected_pair.get("recording_mbid") or "").strip()
                if not recording_mbid:
                    unresolved_count += 1
                    top_rejection_reasons.update(["missing_recording_mbid"])
                    _record_failure(
                        artist=artist,
                        track=title,
                        reasons=["missing_recording_mbid"],
                        recording_mbid_attempted=None,
                        last_query=query,
                    )
                    _record_item(
                        entry,
                        outcome="unresolved_rejected_by_policy",
                        canonical_id=None,
                        linked_job_id=None,
                        linked_job_status=None,
                        recording_mbid=None,
                        mb_release_id=None,
                        mb_release_group_id=None,
                        rejection_category="missing_recording_mbid",
                        scoring_breakdown=selected_pair.get("scoring_breakdown"),
                        selected_bucket=selected_pair.get("selected_bucket"),
                        failure_reasons=["missing_recording_mbid"],
                    )
                    processed_tracks += 1
                    _emit_progress(phase="enqueueing", processed_tracks=processed_tracks, current_phase_detail="queueing_resolved_tracks")
                    continue

                release_mbid = str(selected_pair.get("mb_release_id") or "").strip() or None
                release_group_mbid = str(selected_pair.get("mb_release_group_id") or "").strip() or None
                canonical_artist = artist or str(selected_pair.get("matched_artist") or "").strip()
                canonical_title = title
                canonical_album = str(selected_pair.get("album") or album or "").strip() or None
                release_date_raw = str(selected_pair.get("release_date") or entry.get("release_date") or "").strip() or None
                release_date = _extract_release_year(release_date_raw) or release_date_raw
                track_number = _safe_int(selected_pair.get("track_number")) or _safe_int(entry.get("track_number"))
                disc_number = _safe_int(selected_pair.get("disc_number")) or _safe_int(entry.get("disc_number")) or 1
                disc_total = _safe_int(selected_pair.get("disc_total"))
                resolved_duration_ms = _safe_int(selected_pair.get("duration_ms")) or _safe_int(entry.get("duration_ms"))
                selected_track_aliases = selected_pair.get("track_aliases")
                normalized_aliases = [str(value).strip() for value in selected_track_aliases or [] if str(value or "").strip()]
                selected_track_disambiguation = str(selected_pair.get("track_disambiguation") or "").strip() or None
                selected_recording_title = str(selected_pair.get("mb_recording_title") or "").strip() or None
                selected_mb_youtube_urls = selected_pair.get("mb_youtube_urls")
                normalized_mb_youtube_urls = [str(value).strip() for value in selected_mb_youtube_urls or [] if str(value or "").strip()]
                selected_bucket = str(selected_pair.get("selected_bucket") or "").strip() or None
                scoring_breakdown = selected_pair.get("scoring_breakdown")
                canonical_id = build_music_track_canonical_id(
                    canonical_artist,
                    canonical_album,
                    track_number,
                    canonical_title,
                    recording_mbid=recording_mbid,
                    mb_release_id=release_mbid,
                    mb_release_group_id=release_group_mbid,
                    disc_number=disc_number,
                )
                library_duplicate = _classify_library_duplicate(
                    library_duplicate_index,
                    artist=canonical_artist,
                    album=canonical_album,
                    title=canonical_title,
                    album_artist=entry.get("album_artist"),
                    recording_mbid=recording_mbid,
                    mb_release_id=release_mbid,
                    mb_release_group_id=release_group_mbid,
                )
                if library_duplicate:
                    duplicate_skipped_count += 1
                    resolved_count += 1
                    resolved_mbids.append(recording_mbid)
                    _record_item(
                        entry,
                        outcome="resolved_duplicate_existing",
                        item_state="duplicate",
                        source_resolution_state="skipped_existing",
                        resolution_attempts=1,
                        canonical_id=canonical_id,
                        linked_job_id=None,
                        linked_job_status="library_present",
                        recording_mbid=recording_mbid,
                        mb_release_id=release_mbid,
                        mb_release_group_id=release_group_mbid,
                        rejection_category=None,
                        scoring_breakdown=scoring_breakdown,
                        selected_bucket=selected_bucket,
                        failure_reasons=None,
                    )
                    processed_tracks += 1
                    _emit_progress(phase="enqueueing", processed_tracks=processed_tracks, current_phase_detail="queueing_resolved_tracks")
                    continue
                duplicate = _classify_duplicate(canonical_id, placeholder_url=f"musicbrainz://recording/{recording_mbid}")
                force_requeue = bool(duplicate and str(duplicate.get("classification") or "").startswith("stale_"))
                if duplicate and str(duplicate.get("classification") or "") in {"completed_valid", "active_existing"}:
                    duplicate_skipped_count += 1
                    resolved_count += 1
                    resolved_mbids.append(recording_mbid)
                    outcome = "resolved_duplicate_existing"
                    if str(duplicate.get("classification") or "") == "completed_valid":
                        outcome = "resolved_duplicate_existing"
                    _record_item(
                        entry,
                        outcome=outcome,
                        canonical_id=canonical_id,
                        linked_job_id=str(duplicate.get("job_id") or "").strip() or None,
                        linked_job_status=str(duplicate.get("status") or "").strip() or None,
                        recording_mbid=recording_mbid,
                        mb_release_id=release_mbid,
                        mb_release_group_id=release_group_mbid,
                        rejection_category=None,
                        scoring_breakdown=scoring_breakdown,
                        selected_bucket=selected_bucket,
                        failure_reasons=None,
                    )
                    processed_tracks += 1
                    _emit_progress(phase="enqueueing", processed_tracks=processed_tracks, current_phase_detail="queueing_resolved_tracks")
                    continue

                job_id, created, enqueue_reason = _enqueue_music_track_job(
                    queue_store,
                    job_payload_builder,
                    runtime_config=runtime_config,
                    base_dir=base_dir,
                    destination=destination,
                    final_format_override=final_format_override,
                    import_batch_id=import_batch_id,
                    playlist_name=playlist_name,
                    source_index=idx - 1,
                    recording_mbid=recording_mbid,
                    release_mbid=release_mbid,
                    release_group_mbid=release_group_mbid,
                    artist=canonical_artist,
                    title=canonical_title,
                    album=canonical_album,
                    release_date=release_date,
                    track_number=track_number,
                    disc_number=disc_number,
                    disc_total=disc_total,
                    duration_ms=resolved_duration_ms,
                    track_aliases=normalized_aliases,
                    track_disambiguation=selected_track_disambiguation,
                    mb_recording_title=selected_recording_title,
                    mb_youtube_urls=normalized_mb_youtube_urls,
                    media_mode=media_mode,
                    import_max_concurrent_downloads=import_max_concurrent_downloads,
                    force_requeue=force_requeue,
                )
                resolved_count += 1
                resolved_mbids.append(recording_mbid)
                linked_job_status = str(duplicate.get("status") or "").strip() or None if duplicate else None
                if created:
                    enqueued_count += 1
                elif enqueue_reason == "duplicate":
                    duplicate_skipped_count += 1
                _record_item(
                    entry,
                    outcome=(
                        "resolved_duplicate_stale"
                        if force_requeue
                        else ("resolved_and_enqueued" if created else "resolved_duplicate_existing")
                    ),
                    canonical_id=canonical_id,
                    linked_job_id=str(job_id or "").strip() or None,
                    linked_job_status=linked_job_status or ("queued" if created else None),
                    recording_mbid=recording_mbid,
                    mb_release_id=release_mbid,
                    mb_release_group_id=release_group_mbid,
                    rejection_category=None,
                    scoring_breakdown=scoring_breakdown,
                    selected_bucket=selected_bucket,
                    failure_reasons=None,
                )
            except Exception as exc:
                failed_count += 1
                _record_failure(
                    artist=artist,
                    track=title,
                    reasons=["import_exception", str(exc)],
                    recording_mbid_attempted=None,
                    last_query=query,
                )
                _record_item(
                    entry,
                    outcome="failed_exception",
                    canonical_id=None,
                    linked_job_id=None,
                    linked_job_status=None,
                    recording_mbid=None,
                    mb_release_id=None,
                    mb_release_group_id=None,
                    rejection_category="import_exception",
                    scoring_breakdown=None,
                    selected_bucket=None,
                    failure_reasons=["import_exception", str(exc)],
                )
            processed_tracks += 1
            _emit_progress(phase="enqueueing", processed_tracks=processed_tracks, current_phase_detail="queueing_resolved_tracks")

    unresolved_count = max(unresolved_count, total_tracks - resolved_count - failed_count)
    finished_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    logger.info(
        {
            "message": "import_batch_top_failures",
            "batch_id": import_batch_id,
            "top_rejection_reasons": dict(top_rejection_reasons.most_common(10)),
        }
    )
    logger.info(
        {
            "message": "import_batch_completed",
            "batch_id": import_batch_id,
            "source_format": source_format,
            "total_tracks": total_tracks,
            "resolved_count": resolved_count,
            "unresolved_count": unresolved_count,
            "enqueued_count": enqueued_count,
            "duplicate_skipped_count": duplicate_skipped_count,
            "failed_count": failed_count,
            "selected_bucket_counts": dict(selected_bucket_counts),
        }
    )
    _persist_batch_state(phase="completed", current_phase_detail="completed", finished_at=finished_at)
    _emit_progress(phase="finalizing", processed_tracks=total_tracks, current_phase_detail="completed")
    return ImportResult(
        total_tracks=total_tracks,
        resolved_count=resolved_count,
        unresolved_count=unresolved_count,
        enqueued_count=enqueued_count,
        duplicate_skipped_count=duplicate_skipped_count,
        failed_count=failed_count,
        resolved_mbids=resolved_mbids,
        import_batch_id=import_batch_id,
        top_rejection_reasons=dict(top_rejection_reasons.most_common(10)),
        selected_bucket_counts=dict(selected_bucket_counts),
        current_phase_detail="completed",
    )
