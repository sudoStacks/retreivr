from concurrent.futures import ThreadPoolExecutor, as_completed
MAX_PARALLEL_ADAPTERS = 4

# Helper to run one adapter safely
def _run_adapter_search(adapter, item, max_candidates, canonical_payload):
    """
    Execute a single adapter search safely.
    - Adapter exceptions are contained
    - Invalid URLs are dropped here
    - Never raises
    """
    try:
        if item["item_type"] == "album":
            candidates = adapter.search_album(
                item["artist"],
                item.get("album"),
                max_candidates,
            )
        else:
            candidates = adapter.search_track(
                item["artist"],
                item.get("track"),
                item.get("album"),
                max_candidates,
            )
    except Exception as exc:
        logging.exception(
            "adapter_search_exception",
            extra={
                "adapter": getattr(adapter, "name", repr(adapter)),
                "artist": item.get("artist"),
                "album": item.get("album"),
                "track": item.get("track"),
                "error": str(exc),
            },
        )
        return []

    out = []
    for cand in candidates or []:
        try:
            url = cand.get("url")
            if not _is_http_url(url):
                continue
            cand = dict(cand)
            cand["canonical_metadata"] = canonical_payload
            out.append(cand)
        except Exception:
            continue

    return out
import json
import hashlib
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4
from yt_dlp import YoutubeDL

from engine.job_queue import (
    DownloadJobStore,
    build_output_template,
    build_download_job_payload,
    ensure_download_jobs_table,
    canonicalize_url,
    extract_video_id,
)
from engine.json_utils import safe_json_dumps
from engine.paths import DATA_DIR
from engine.search_adapters import default_adapters
from engine.search_scoring import (
    classify_music_title_variants,
    rank_candidates,
    score_candidate,
    select_best_candidate,
    tokenize,
)
from engine.musicbrainz_binding import _normalize_title_for_mb_lookup
from engine.music_title_normalization import has_live_intent, relaxed_search_title
from engine.canonical_ids import build_music_track_canonical_id, extract_external_track_canonical_id
from metadata.canonical import CanonicalMetadataResolver

# Search scoring logic is benchmark-gated.
# Changes require benchmark pass + no precision regression.
# Do not alter thresholds without updating gate config and benchmark baseline.

REQUEST_STATUSES = {"pending", "resolving", "completed", "completed_with_skips", "failed"}
ITEM_STATUSES = {
    "queued",
    "searching",
    "searching_source",
    "candidate_found",
    "selected",
    "enqueued",
    "skipped",
    "failed",
}

DEFAULT_SOURCE_PRIORITY = ["bandcamp", "youtube_music", "soundcloud"]
MUSIC_TRACK_SOURCE_PRIORITY = ("youtube_music", "youtube", "soundcloud", "bandcamp")
MUSIC_TRACK_SOURCE_PRIORITY_WITH_MB = ("mb_relationship",) + MUSIC_TRACK_SOURCE_PRIORITY
MUSIC_TRACK_PENALIZE_TOKENS = ("live", "cover", "karaoke", "remix", "reaction", "ft.", "feat.", "instrumental")
DEFAULT_MATCH_THRESHOLD = 0.92
MUSIC_TRACK_THRESHOLD = 0.78
WORD_TOKEN_RE = re.compile(r"[a-z0-9]+")
_MUSIC_DURATION_STRICT_MAX_DELTA_MS = 12000
_MUSIC_DURATION_EXPANDED_MAX_DELTA_MS = 35000
_MUSIC_DURATION_HARD_CAP_MS = 35000
_MUSIC_PASS_B_MIN_TITLE_SIMILARITY = 0.92
_MUSIC_PASS_B_MIN_ARTIST_SIMILARITY = 0.92
_ALBUM_COHERENCE_MAX_BOOST = 0.03
_ALBUM_COHERENCE_TIE_WINDOW = 0.03
_MB_INJECTED_MAX_URLS = 3


@dataclass(frozen=True)
class SearchRequest:
    id: str
    intent: str
    media_type: str
    artist: str
    album: str | None
    track: str | None
    destination_dir: str | None
    auto_enqueue: bool
    min_match_score: float
    duration_hint_sec: int | None
    source_priority: list[str]
    max_candidates_per_source: int


@dataclass(frozen=True)
class MusicTrackSelectionResult:
    selected: dict | None
    selected_pass: str | None
    ranked: list[dict]
    failure_reason: str
    coherence_boost_applied: int
    mb_injected_rejections: dict[str, int]
    rejected_candidates: list[dict]
    accepted_selection: dict | None
    final_rejection: dict | None
    candidate_variant_distribution: dict[str, int]
    selected_candidate_variant_tags: list[str]
    top_rejected_variant_tags: list[str]



def _utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _is_http_url(value: str | None) -> bool:
    if not value or not isinstance(value, str):
        return False
    value = value.strip().lower()
    return value.startswith("http://") or value.startswith("https://")

# Helper: Coerce to HTTP(S) URL or None
def _coerce_http_url(value: str | None) -> str | None:
    return value if _is_http_url(value) else None

# Helper: Detect if a value is a URL
def _is_url(value: str | None) -> bool:
    if not value or not isinstance(value, str):
        return False
    return bool(re.match(r"^https?://", value.strip(), re.IGNORECASE))

# Helper: Detect if any field in a payload contains a URL
def _payload_contains_url(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    for v in payload.values():
        if _is_url(v):
            return True
    return False


def _log_event(level, message, **fields):
    payload = {"message": message, **fields}
    try:
        logging.log(level, safe_json_dumps(payload, sort_keys=True))
    except Exception as exc:
        logging.log(level, f"log_event_serialization_failed: {exc} message={message}")


def resolve_search_db_path(queue_db_path, config=None):
    return str(DATA_DIR / "database" / "search_jobs.sqlite")


def _normalize_media_type(value, *, default="generic"):
    # Invariant A: media_type defaults to general/generic unless explicitly set.
    if value is None:
        return default
    value = str(value).strip().lower()
    if not value:
        return default
    if value in {"music", "audio"}:
        return "music"
    if value == "video":
        return "video"
    if value in {"generic", "general"}:
        return "generic"
    return None


# Audio formats that require the audio-mode download pipeline
_AUDIO_FINAL_FORMATS = {"mp3", "m4a", "aac", "flac", "wav", "opus", "ogg"}

def _is_audio_final_format(value: str | None) -> bool:
    if not value:
        return False
    try:
        v = str(value).strip().lower()
    except Exception:
        return False
    return v in _AUDIO_FINAL_FORMATS


def _extract_canonical_id(metadata):
    if not isinstance(metadata, dict):
        return None

    external_ids = metadata.get("external_ids") or {}
    recording_mbid = (
        metadata.get("recording_mbid")
        or metadata.get("mb_recording_id")
        or external_ids.get("musicbrainz_recording_id")
    )
    if recording_mbid:
        return build_music_track_canonical_id(
            metadata.get("artist"),
            metadata.get("album"),
            metadata.get("track_number") or metadata.get("track_num"),
            metadata.get("track") or metadata.get("title"),
            recording_mbid=recording_mbid,
            mb_release_id=(
                metadata.get("mb_release_id")
                or metadata.get("release_id")
                or external_ids.get("musicbrainz_release_id")
            ),
            mb_release_group_id=(
                metadata.get("mb_release_group_id")
                or metadata.get("release_group_id")
            ),
            disc_number=metadata.get("disc_number") or metadata.get("disc_num"),
        )

    return extract_external_track_canonical_id(external_ids)


def _normalize_threshold(value, *, default=0.78):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    if parsed > 1.0:
        parsed = parsed / 100.0
    if parsed < 0.0:
        return 0.0
    if parsed > 1.0:
        return 1.0
    return parsed


def _token_set(value: str | None) -> set[str]:
    return {m.group(0) for m in WORD_TOKEN_RE.finditer(str(value or "").lower())}


def _query_contains_artist(query: str | None, artist_guess: str | None) -> bool:
    query_tokens = _token_set(query)
    artist_tokens = _token_set(artist_guess)
    if not query_tokens or not artist_tokens:
        return False
    return artist_tokens.issubset(query_tokens)


def _uploader_artist_similarity_ok(artist_guess: str | None, uploader: str | None) -> bool:
    artist_tokens = _token_set(artist_guess)
    uploader_tokens = _token_set(uploader)
    if not artist_tokens or not uploader_tokens:
        return False
    overlap = len(artist_tokens & uploader_tokens)
    return (overlap / max(len(artist_tokens), 1)) >= 0.5


def _parse_artist_track_from_candidate(title: str, uploader: str | None, query: str | None) -> tuple[str | None, str | None]:
    normalized_title = str(title or "").replace(" – ", " - ").strip()
    uploader_value = str(uploader or "").strip() or None
    query_value = str(query or "").strip() or None

    if " - " in normalized_title:
        left, right = normalized_title.split(" - ", 1)
        artist_guess = left.strip() or None
        track_guess = right.strip() or None
        if artist_guess and track_guess:
            left_token_count = len(_token_set(artist_guess))
            if left_token_count <= 6:
                if _uploader_artist_similarity_ok(artist_guess, uploader_value) or _query_contains_artist(query_value, artist_guess):
                    return artist_guess, track_guess

    fallback_artist = uploader_value
    fallback_track = normalized_title or None
    return fallback_artist, fallback_track



def ensure_search_tables(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_requests (
            id TEXT PRIMARY KEY,
            created_at TEXT,
            updated_at TEXT,
            created_by TEXT,
            intent TEXT,
            media_type TEXT,
            artist TEXT,
            album TEXT,
            track TEXT,
            destination_dir TEXT,
            include_albums INTEGER DEFAULT 1,
            include_singles INTEGER DEFAULT 1,
            min_match_score REAL DEFAULT 0.92,
            duration_hint_sec INTEGER,
            quality_min_bitrate_kbps INTEGER,
            lossless_only INTEGER DEFAULT 0,
            auto_enqueue INTEGER DEFAULT 1,
            source_priority_json TEXT,
            max_candidates_per_source INTEGER DEFAULT 5,
            status TEXT,
            error TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_requests_status ON search_requests (status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_requests_created ON search_requests (created_at)")
    cur.execute("PRAGMA table_info(search_requests)")
    existing_requests = {row[1] for row in cur.fetchall()}
    if "destination_dir" not in existing_requests:
        cur.execute("ALTER TABLE search_requests ADD COLUMN destination_dir TEXT")
    if "auto_enqueue" not in existing_requests:
        cur.execute("ALTER TABLE search_requests ADD COLUMN auto_enqueue INTEGER DEFAULT 1")
    if "adapters_total" not in existing_requests:
        cur.execute("ALTER TABLE search_requests ADD COLUMN adapters_total INTEGER")
    if "adapters_completed" not in existing_requests:
        cur.execute("ALTER TABLE search_requests ADD COLUMN adapters_completed INTEGER")


    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_items (
            id TEXT PRIMARY KEY,
            request_id TEXT,
            position INTEGER,
            item_type TEXT,
            media_type TEXT,
            artist TEXT,
            album TEXT,
            track TEXT,
            duration_hint_sec INTEGER,
            status TEXT,
            chosen_source TEXT,
            chosen_url TEXT,
            chosen_score REAL,
            error TEXT,
            FOREIGN KEY (request_id) REFERENCES search_requests(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_items_request_status ON search_items (request_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_items_status ON search_items (status)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_candidates (
            id TEXT PRIMARY KEY,
            item_id TEXT,
            source TEXT,
            url TEXT,
            title TEXT,
            uploader TEXT,
            artist_detected TEXT,
            album_detected TEXT,
            track_detected TEXT,
            duration_sec INTEGER,
            artwork_url TEXT,
            raw_meta_json TEXT,
            canonical_json TEXT,
            score_artist REAL,
            score_track REAL,
            score_album REAL,
            score_duration REAL,
            source_modifier REAL,
            penalty_multiplier REAL,
            final_score REAL,
            rank INTEGER,
            FOREIGN KEY (item_id) REFERENCES search_items(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_candidates_item_score ON search_candidates (item_id, final_score DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_candidates_source ON search_candidates (source)")

    cur.execute("PRAGMA table_info(search_candidates)")
    existing = {row[1] for row in cur.fetchall()}
    if "canonical_json" not in existing:
        cur.execute("ALTER TABLE search_candidates ADD COLUMN canonical_json TEXT")

    conn.commit()


class SearchJobStore:
    def __init__(self, db_path):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self):
        conn = self._connect()
        try:
            ensure_search_tables(conn)
        finally:
            conn.close()

    def create_request(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        intent = payload.get("intent")
        if intent not in {"track", "album", "artist", "artist_collection"}:
            raise ValueError("intent must be track, album, artist, or artist_collection")
        media_type = _normalize_media_type(payload.get("media_type") or "generic")
        if media_type not in {"music", "video", "generic"}:
            raise ValueError("media_type must be music, video, or generic")
        artist = payload.get("artist")
        if not artist:
            raise ValueError("artist is required")
        if intent == "track" and not payload.get("track"):
            raise ValueError("track is required for track intent")
        if intent == "album" and not payload.get("album"):
            raise ValueError("album is required for album intent")

        req_id = uuid4().hex
        now = _utc_now()
        source_priority = _normalize_source_priority(payload.get("source_priority"))
        min_match_score = float(payload.get("min_match_score") or 0.92)
        max_candidates = int(payload.get("max_candidates_per_source") or 5)
        destination_dir = payload.get("destination_dir")
        if destination_dir is not None:
            destination_dir = str(destination_dir).strip() or None
        auto_enqueue = bool(payload.get("auto_enqueue", True))

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO search_requests (
                    id, created_at, updated_at, created_by, intent, media_type, artist,
                    album, track, destination_dir, include_albums, include_singles, min_match_score,
                    duration_hint_sec, quality_min_bitrate_kbps, lossless_only, auto_enqueue,
                    source_priority_json, max_candidates_per_source, status, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    req_id,
                    now,
                    now,
                    payload.get("created_by") or "api",
                    intent,
                    media_type,
                    artist,
                    payload.get("album"),
                    payload.get("track"),
                    destination_dir,
                    1 if payload.get("include_albums", True) else 0,
                    1 if payload.get("include_singles", True) else 0,
                    min_match_score,
                    payload.get("duration_hint_sec"),
                    payload.get("quality_min_bitrate_kbps"),
                    1 if payload.get("lossless_only") else 0,
                    1 if auto_enqueue else 0,
                    safe_json_dumps(source_priority),
                    max_candidates,
                    "pending",
                    None,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return req_id

    def get_request(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_requests WHERE id=?", (request_id,))
            row = cur.fetchone()
            if not row:
                return None
            request = dict(row)
            request["source_priority"] = _parse_source_priority(row["source_priority_json"])

            cur.execute("SELECT * FROM search_items WHERE request_id=? ORDER BY position", (request_id,))
            items = [dict(item) for item in cur.fetchall()]
            for item in items:
                cur.execute(
                    "SELECT COUNT(*) FROM search_candidates WHERE item_id=?",
                    (item["id"],),
                )
                item["candidate_count"] = cur.fetchone()[0]
            return {"request": request, "items": items}
        finally:
            conn.close()

    def get_request_row(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_requests WHERE id=?", (request_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_items WHERE id=?", (item_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_candidate(self, candidate_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_candidates WHERE id=?", (candidate_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_requests(self, *, status=None, limit=None):
        # Always ensure schema before any SELECT to prevent missing-table errors
        self.ensure_schema()
        conn = self._connect()
        try:
            cur = conn.cursor()
            params = []
            query = "SELECT * FROM search_requests"
            if status:
                query += " WHERE status=?"
                params.append(status)
            query += " ORDER BY created_at ASC"
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            try:
                cur.execute(query, params)
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc):
                    self.ensure_schema()
                    return []
                raise
            rows = []
            for row in cur.fetchall():
                entry = dict(row)
                entry["source_priority"] = _parse_source_priority(row["source_priority_json"])
                rows.append(entry)
            return rows
        finally:
            conn.close()

    def cancel_request(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            now = _utc_now()
            cur.execute(
                """
                UPDATE search_requests
                SET status=?, updated_at=?, error=?
                WHERE id=? AND status NOT IN ('completed', 'completed_with_skips', 'failed')
                """,
                ("failed", now, "request_canceled", request_id),
            )
            cur.execute(
                """
                UPDATE search_items
                SET status=?, error=?
                WHERE request_id=? AND status NOT IN ('enqueued', 'failed')
                """,
                ("skipped", "request_canceled", request_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def claim_next_request(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT * FROM search_requests WHERE status=? ORDER BY created_at ASC LIMIT 1",
                ("pending",),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            now = _utc_now()
            cur.execute(
                "UPDATE search_requests SET status=?, updated_at=? WHERE id=? AND status=?",
                ("resolving", now, row["id"], "pending"),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            return dict(row)
        finally:
            conn.close()

    def claim_request(self, request_id):
        if not request_id:
            return self.claim_next_request()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT * FROM search_requests WHERE id=? AND status=?",
                (request_id, "pending"),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            now = _utc_now()
            cur.execute(
                "UPDATE search_requests SET status=?, updated_at=? WHERE id=? AND status=?",
                ("resolving", now, request_id, "pending"),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            return dict(row)
        finally:
            conn.close()

    def create_items_for_request(self, request_row):
        req_id = request_row["id"]
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM search_items WHERE request_id=?", (req_id,))
            if cur.fetchone()[0] > 0:
                return
            item_type = "track" if request_row["intent"] == "track" else "album"
            item_id = uuid4().hex
            media_type = _normalize_media_type(request_row.get("media_type")) or "generic"
            cur.execute(
                """
                INSERT INTO search_items (
                    id, request_id, position, item_type, media_type, artist,
                    album, track, duration_hint_sec, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    req_id,
                    1,
                    item_type,
                    media_type,
                    request_row["artist"],
                    request_row["album"],
                    request_row["track"],
                    request_row["duration_hint_sec"],
                    "queued",
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def list_candidates(self, item_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM search_candidates WHERE item_id=? ORDER BY rank",
                (item_id,),
            )
            rows = []
            for row in cur.fetchall():
                entry = dict(row)
                canonical_raw = entry.get("canonical_json")
                if canonical_raw:
                    try:
                        entry["canonical_metadata"] = json.loads(canonical_raw)
                    except json.JSONDecodeError:
                        entry["canonical_metadata"] = None
                rows.append(entry)
            return rows
        finally:
            conn.close()

    def list_items(self, request_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM search_items WHERE request_id=? ORDER BY position", (request_id,))
            return [dict(item) for item in cur.fetchall()]
        finally:
            conn.close()

    def reset_candidates_for_item(self, item_id):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM search_candidates WHERE item_id=?", (item_id,))
            conn.commit()
        finally:
            conn.close()

    def insert_candidates(self, item_id, candidates):
        conn = self._connect()
        try:
            cur = conn.cursor()
            for candidate in candidates:
                cur.execute(
                    """
                    INSERT INTO search_candidates (
                        id, item_id, source, url, title, uploader, artist_detected,
                        album_detected, track_detected, duration_sec, artwork_url,
                        raw_meta_json, canonical_json, score_artist, score_track, score_album,
                        score_duration, source_modifier, penalty_multiplier, final_score, rank
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate["id"],
                        item_id,
                        candidate.get("source"),
                        _coerce_http_url(candidate.get("url")),
                        candidate.get("title"),
                        candidate.get("uploader"),
                        candidate.get("artist_detected"),
                        candidate.get("album_detected"),
                        candidate.get("track_detected"),
                        candidate.get("duration_sec"),
                        candidate.get("artwork_url"),
                        candidate.get("raw_meta_json"),
                        candidate.get("canonical_json"),
                        candidate.get("score_artist"),
                        candidate.get("score_track"),
                        candidate.get("score_album"),
                        candidate.get("score_duration"),
                        candidate.get("source_modifier"),
                        candidate.get("penalty_multiplier"),
                        candidate.get("final_score"),
                        candidate.get("rank"),
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def update_item_status(self, item_id, status, *, chosen=None, error=None):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE search_items
                SET status=?, chosen_source=?, chosen_url=?, chosen_score=?, error=?
                WHERE id=?
                """,
                (
                    status,
                    chosen.get("source") if chosen else None,
                    _coerce_http_url(chosen.get("url")) if chosen else None,
                    chosen.get("final_score") if chosen else None,
                    error,
                    item_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def update_request_status(self, request_id, status, *, error=None):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE search_requests SET status=?, updated_at=?, error=? WHERE id=?",
                (status, _utc_now(), error, request_id),
            )
            conn.commit()
        finally:
            conn.close()
    
    def update_request_progress(self, request_id, *, adapters_total=None, adapters_completed=None):
        conn = self._connect()
        try:
            cur = conn.cursor()
            fields = []
            params = []
            if adapters_total is not None:
                fields.append("adapters_total=?")
                params.append(adapters_total)
            if adapters_completed is not None:
                fields.append("adapters_completed=?")
                params.append(adapters_completed)
            if not fields:
                return
            params.append(_utc_now())
            params.append(request_id)
            cur.execute(
                f"""
                UPDATE search_requests
                SET {", ".join(fields)}, updated_at=?
                WHERE id=?
                """,
                params,
            )
            conn.commit()
        finally:
            conn.close()


class SearchResolutionService:
    def __init__(self, *, search_db_path, queue_db_path, adapters=None, config=None, paths=None, canonical_resolver=None):
        self.search_db_path = search_db_path
        self.queue_db_path = queue_db_path
        self.adapters = adapters or default_adapters()
        self.config = config or {}
        self.debug_music_scoring = self._as_bool(self.config.get("debug_music_scoring"))
        self.music_source_match_threshold = _normalize_threshold(
            self.config.get("music_source_match_threshold", MUSIC_TRACK_THRESHOLD),
            default=MUSIC_TRACK_THRESHOLD,
        )
        self.paths = paths
        self.store = SearchJobStore(search_db_path)
        self.store.ensure_schema()
        self.queue_store = DownloadJobStore(queue_db_path)
        self._ensure_queue_schema()
        self.canonical_resolver = canonical_resolver or CanonicalMetadataResolver(config=self.config)
        self._album_coherence_families = {}
        self._album_mb_injection_success = {}
        self._album_coherence_lock = threading.Lock()
        self._mb_injected_probe_cache = {}
        self._mb_injected_probe_lock = threading.Lock()

    def _as_bool(self, value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(value, (int, float)):
            return bool(value)
        return False

    def ensure_schema(self):
        """
        Ensure the search database schema exists.
        This is called during app startup to guarantee tables are present
        before any reads or writes occur.
        """
        self.store.ensure_schema()

    def _music_tokens(self, value):
        return WORD_TOKEN_RE.findall(str(value or "").lower())

    def _parse_raw_meta(self, value):
        if isinstance(value, dict):
            return value
        if not isinstance(value, str) or not value.strip():
            return {}
        try:
            loaded = json.loads(value)
        except Exception:
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def _candidate_authority_family(self, candidate):
        if not isinstance(candidate, dict):
            return None
        source = str(candidate.get("source") or "").strip().lower() or "unknown"
        direct_values = (
            candidate.get("channel_id"),
            candidate.get("uploader_id"),
            candidate.get("channel_url"),
            candidate.get("uploader_url"),
        )
        for value in direct_values:
            normalized = str(value or "").strip()
            if normalized:
                return f"{source}:{normalized.lower()}"

        raw_meta = self._parse_raw_meta(candidate.get("raw_meta_json"))
        for key in ("channel_id", "uploader_id", "channel_url", "uploader_url"):
            value = str(raw_meta.get(key) or "").strip()
            if value:
                return f"{source}:{value.lower()}"

        uploader = str(candidate.get("uploader") or candidate.get("artist_detected") or "").strip().lower()
        if source in {"youtube", "youtube_music"} and uploader.endswith("- topic"):
            return f"{source}:topic"
        return None

    def _coherence_context_key(self, coherence_context):
        if not isinstance(coherence_context, dict):
            return None
        release_id = str(coherence_context.get("mb_release_id") or "").strip().lower()
        if not release_id:
            return None
        track_total = coherence_context.get("track_total")
        try:
            track_total_value = int(track_total) if track_total is not None else None
        except Exception:
            track_total_value = None
        if track_total_value is not None and track_total_value <= 1:
            return None
        return release_id

    def _normalize_mb_youtube_urls(self, mb_youtube_urls):
        urls = []
        seen = set()
        source_values = mb_youtube_urls if isinstance(mb_youtube_urls, (list, tuple, set)) else []
        for value in source_values:
            raw = str(value or "").strip()
            if not raw:
                continue
            video_id = extract_video_id(raw)
            if not video_id:
                parsed = urllib.parse.urlparse(raw)
                host = (parsed.netloc or "").lower()
                parts = [part for part in (parsed.path or "").split("/") if part]
                if "youtube.com" in host and len(parts) >= 2 and parts[0] in {"shorts", "embed", "live"}:
                    video_id = parts[1]
            if not video_id:
                continue
            canonical = canonicalize_url("youtube", raw, video_id)
            if not canonical:
                continue
            key = canonical.lower()
            if key in seen:
                continue
            seen.add(key)
            urls.append(canonical)
            if len(urls) >= _MB_INJECTED_MAX_URLS:
                break
        return urls

    def _probe_mb_relationship_candidate(self, url, *, artist, track, album):
        cache_key = str(url or "").strip().lower()
        if cache_key:
            with self._mb_injected_probe_lock:
                cached = self._mb_injected_probe_cache.get(cache_key, "__missing__")
            if cached != "__missing__":
                return dict(cached) if isinstance(cached, dict) else None
        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
            "cachedir": False,
            "socket_timeout": 8,
        }
        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception as exc:
            failure_kind = self._classify_mb_injected_probe_failure(str(exc))
            _log_event(
                logging.WARNING,
                "mb_youtube_injected_probe_failed",
                url=url,
                error=str(exc),
                classified_reason=failure_kind,
            )
            if cache_key:
                with self._mb_injected_probe_lock:
                    self._mb_injected_probe_cache[cache_key] = None
            return None, failure_kind
        if not isinstance(info, dict):
            if cache_key:
                with self._mb_injected_probe_lock:
                    self._mb_injected_probe_cache[cache_key] = None
            return None, "mb_injected_failed_unavailable"
        resolved_url = str(info.get("webpage_url") or info.get("original_url") or url).strip() or url
        video_id = extract_video_id(resolved_url) or extract_video_id(url)
        canonical_url = canonicalize_url("youtube", resolved_url, video_id) or resolved_url
        title = str(info.get("title") or track or "").strip()
        uploader = str(info.get("uploader") or info.get("channel") or artist or "").strip()
        duration_sec = info.get("duration")
        try:
            duration_sec = int(duration_sec) if duration_sec is not None else None
        except Exception:
            duration_sec = None
        candidate_id = str(info.get("id") or video_id or "").strip()
        if not candidate_id:
            candidate_id = hashlib.sha1(canonical_url.encode("utf-8")).hexdigest()[:16]
        candidate = {
            "source": "mb_relationship",
            "candidate_id": candidate_id,
            "url": canonical_url,
            "title": title,
            "uploader": uploader,
            "artist_detected": str(info.get("artist") or uploader or artist or "").strip(),
            "album_detected": str(info.get("album") or album or "").strip() or None,
            "track_detected": str(info.get("track") or title or track or "").strip() or None,
            "duration_sec": duration_sec,
            "official": bool(info.get("is_official")),
            "raw_meta_json": safe_json_dumps(info),
            "mb_injected": True,
            "mb_relationship_url": canonical_url,
        }
        if cache_key:
            with self._mb_injected_probe_lock:
                self._mb_injected_probe_cache[cache_key] = dict(candidate)
        return candidate, None

    def _resolve_mb_relationship_candidates(self, *, mb_youtube_urls, artist, track, album, recording_mbid=None):
        urls = self._normalize_mb_youtube_urls(mb_youtube_urls)
        injected = []
        rejection_counts = {}
        for url in urls:
            candidate, probe_failure = self._probe_mb_relationship_candidate(url, artist=artist, track=track, album=album)
            if not isinstance(candidate, dict):
                if probe_failure:
                    rejection_counts[probe_failure] = int(rejection_counts.get(probe_failure) or 0) + 1
                continue
            injected.append(candidate)
            _log_event(
                logging.INFO,
                "mb_youtube_injected",
                recording_mbid=str(recording_mbid or "").strip() or None,
                url=url,
                candidate_id=candidate.get("candidate_id"),
            )
        return injected, rejection_counts

    def _classify_mb_injected_probe_failure(self, error_text):
        value = str(error_text or "").strip().lower()
        if not value:
            return "mb_injected_failed_unavailable"
        unavailable_markers = (
            "not available in your country",
            "video unavailable in your country",
            "geo-restricted",
            "geoblocked",
            "private video",
            "members-only",
            "this video is unavailable",
            "video has been removed",
            "sign in to confirm your age",
            "age-restricted",
        )
        if any(marker in value for marker in unavailable_markers):
            return "mb_injected_failed_unavailable"
        return "mb_injected_failed_unavailable"

    def _classify_mb_injected_rejection(self, reason):
        value = str(reason or "").strip().lower()
        if not value:
            return "mb_injected_failed_unknown"
        if value in {"duration_out_of_bounds", "duration_over_hard_cap", "preview_duration"}:
            return "mb_injected_failed_duration"
        if value in {"disallowed_variant", "preview_variant", "session_variant", "cover_artist_mismatch"}:
            return "mb_injected_failed_variant"
        if value in {"low_title_similarity"}:
            return "mb_injected_failed_title"
        if value in {"low_artist_similarity"}:
            return "mb_injected_failed_artist"
        return f"mb_injected_failed_{value}"

    def _apply_album_coherence_tiebreak(self, ranked, *, coherence_key, pass_name, query_label):
        if not ranked:
            return ranked, 0
        with self._album_coherence_lock:
            family_counts = dict(self._album_coherence_families.get(coherence_key) or {})
        if not family_counts:
            return ranked, 0

        top_non_rejected = [
            float(item.get("final_score") or 0.0)
            for item in ranked
            if not item.get("rejection_reason")
        ]
        if not top_non_rejected:
            return ranked, 0
        top_score = max(top_non_rejected)
        max_family_count = max(family_counts.values()) if family_counts else 0
        if max_family_count <= 0:
            return ranked, 0

        boosted = []
        applied = 0
        for item in ranked:
            candidate = dict(item)
            candidate["coherence_delta"] = float(candidate.get("coherence_delta") or 0.0)
            base_score = float(candidate.get("final_score") or 0.0)
            candidate["base_final_score"] = base_score
            if candidate.get("rejection_reason"):
                boosted.append(candidate)
                continue
            if (top_score - base_score) > _ALBUM_COHERENCE_TIE_WINDOW:
                boosted.append(candidate)
                continue
            family = self._candidate_authority_family(candidate)
            if not family:
                boosted.append(candidate)
                continue
            count = int(family_counts.get(family) or 0)
            if count <= 0:
                boosted.append(candidate)
                continue
            family_strength = count / float(max_family_count)
            delta = min(_ALBUM_COHERENCE_MAX_BOOST, _ALBUM_COHERENCE_MAX_BOOST * family_strength)
            if delta <= 0:
                boosted.append(candidate)
                continue
            candidate["coherence_family"] = family
            candidate["coherence_delta"] = delta
            candidate["final_score"] = min(1.0, base_score + delta)
            applied += 1
            _log_event(
                logging.DEBUG,
                "music_album_coherence_boost_applied",
                coherence_key=coherence_key,
                query_label=query_label,
                selected_pass=pass_name,
                candidate_id=candidate.get("candidate_id"),
                coherence_family=family,
                coherence_delta=delta,
                base_final_score=base_score,
                boosted_final_score=candidate.get("final_score"),
            )
            boosted.append(candidate)
        if applied <= 0:
            return ranked, 0
        return rank_candidates(boosted, source_priority=list(MUSIC_TRACK_SOURCE_PRIORITY_WITH_MB)), applied

    def _record_album_coherence_family(self, coherence_key, selected_candidate):
        if not coherence_key or not isinstance(selected_candidate, dict):
            return
        family = self._candidate_authority_family(selected_candidate)
        if not family:
            return
        with self._album_coherence_lock:
            bucket = self._album_coherence_families.setdefault(coherence_key, {})
            bucket[family] = int(bucket.get(family) or 0) + 1

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
            if self.debug_music_scoring:
                logging.debug(
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
                if self.debug_music_scoring:
                    logging.debug(
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

        for token in MUSIC_TRACK_PENALIZE_TOKENS:
            if allow_live and token == "live":
                continue
            if token in title_lower:
                adjustment -= 10.0
                reasons.append(f"penalty_{token}")
                if self.debug_music_scoring:
                    logging.debug(
                        f"[MUSIC] penalizing token={token} new_score={adjustment:.0f} "
                        f"for {candidate.get('url')}"
                    )
        return adjustment, reasons

    def _build_music_track_query(self, artist, track, album=None):
        ladder = self._build_music_track_query_ladder(artist, track, album)
        return ladder[0]["query"] if ladder else ""

    def _build_music_track_query_ladder(self, artist, track, album=None):
        artist_v = str(artist or "").strip()
        track_v = str(track or "").strip()
        album_v = str(album or "").strip()
        relaxed_track = relaxed_search_title(track_v) or track_v
        ladder = [
            {
                "rung": 0,
                "label": "canonical_full",
                "query": " ".join(
                    part
                    for part in [f'"{artist_v}"', f'"{track_v}"', f'"{album_v}"' if album_v else ""]
                    if part
                ).strip(),
            },
            {
                "rung": 1,
                "label": "canonical_no_album",
                "query": " ".join(part for part in [f'"{artist_v}"', f'"{track_v}"'] if part).strip(),
            },
            {
                "rung": 2,
                "label": "relaxed_no_album",
                "query": " ".join(part for part in [f'"{artist_v}"', f'"{relaxed_track}"'] if part).strip(),
            },
            {
                "rung": 3,
                "label": "official_audio_fallback",
                "query": " ".join(part for part in [artist_v, relaxed_track, "official audio"] if part).strip(),
            },
            {
                "rung": 4,
                "label": "legacy_topic_fallback",
                "query": " ".join(part for part in [artist_v, "-", track_v, "topic"] if part).strip(),
            },
            {
                "rung": 5,
                "label": "legacy_audio_fallback",
                "query": " ".join(part for part in [artist_v, "-", track_v, "audio"] if part).strip(),
            },
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

    def _music_track_is_live(self, artist, track, album):
        return has_live_intent(artist, track, album)

    def search_music_track_candidates(self, query: str, limit: int = 6, *, query_label: str | None = None) -> list[dict]:
        candidates = []
        for source in MUSIC_TRACK_SOURCE_PRIORITY:
            adapter = self.adapters.get(source)
            if not adapter:
                continue
            _log_event(
                logging.INFO,
                "adapter_search_started",
                source=source,
                mode="music_track",
                query=query,
                query_label=query_label,
            )
            try:
                adapter_candidates = adapter.search_music_track(query, limit)
            except Exception:
                logging.exception("Music track adapter failed source=%s query=%s", source, query)
                adapter_candidates = []
            adapter_candidates = [dict(c) for c in (adapter_candidates or []) if isinstance(c, dict)]
            _log_event(
                logging.INFO,
                "adapter_search_completed",
                source=source,
                mode="music_track",
                query=query,
                query_label=query_label,
                candidates=len(adapter_candidates),
            )
            for candidate in adapter_candidates:
                if not _is_http_url(candidate.get("url")):
                    continue
                candidate["source"] = candidate.get("source") or source
                candidates.append(candidate)
        _log_event(
            logging.INFO,
            "music_track_candidates_total",
            query=query,
            query_label=query_label,
            candidates_total=len(candidates),
        )
        return candidates

    def retrieve_candidates(self, ctx) -> list[dict]:
        query = str((ctx or {}).get("query") or "").strip()
        if not query:
            return []
        limit = int((ctx or {}).get("limit") or 6)
        query_label = (ctx or {}).get("query_label")
        rung = int((ctx or {}).get("rung") or 0)
        first_rung = int((ctx or {}).get("first_rung") or 0)
        injected = (ctx or {}).get("mb_injected_candidates")
        injected_candidates = [dict(c) for c in (injected or []) if isinstance(c, dict)]

        candidates = self.search_music_track_candidates(query, limit=limit, query_label=query_label)
        if rung == first_rung and injected_candidates:
            deduped = []
            seen_urls = set()
            for candidate in list(injected_candidates) + list(candidates):
                if not isinstance(candidate, dict):
                    continue
                url_key = str(candidate.get("url") or "").strip().lower()
                if url_key and url_key in seen_urls:
                    continue
                if url_key:
                    seen_urls.add(url_key)
                deduped.append(candidate)
            candidates = deduped
        return candidates

    def _music_similarity_thresholds(self, expected_base):
        has_album = bool(tokenize(expected_base.get("album")))
        if has_album:
            return {
                "title_similarity": (20.0 / 30.0),
                "artist_similarity": (15.0 / 24.0),
                "album_similarity": (8.0 / 18.0),
            }
        return {
            "title_similarity": (20.0 / 39.0),
            "artist_similarity": (15.0 / 33.0),
            "album_similarity": 0.0,
        }

    def _build_candidate_observation(self, candidate, *, reason, expected_base, pass_name):
        similarity_thresholds = self._music_similarity_thresholds(expected_base)
        max_delta_ms = (
            _MUSIC_DURATION_STRICT_MAX_DELTA_MS
            if pass_name == "strict"
            else _MUSIC_DURATION_EXPANDED_MAX_DELTA_MS
        )
        hard_cap_ms = int(expected_base.get("duration_hard_cap_ms") or _MUSIC_DURATION_HARD_CAP_MS)
        title_similarity = max(
            float(candidate.get("score_track") or 0.0),
            min(float(candidate.get("score_track_relaxed") or 0.0) * 0.90, 1.0),
        )
        artist_similarity = float(candidate.get("score_artist") or 0.0)
        album_similarity = float(candidate.get("score_album") or 0.0)
        duration_delta_ms = candidate.get("duration_delta_ms")
        try:
            duration_delta_ms = int(duration_delta_ms) if duration_delta_ms is not None else None
        except Exception:
            duration_delta_ms = None
        final_score = float(candidate.get("final_score") or 0.0)
        gate = "score_threshold"
        metric = {
            "name": "final_score",
            "value": final_score,
            "threshold": float(self.music_source_match_threshold),
            "margin_to_pass": float(self.music_source_match_threshold) - final_score,
            "direction": ">=",
        }

        reason_value = str(reason or "").strip().lower()
        if reason_value in {"duration_out_of_bounds", "pass_b_duration"}:
            gate = "duration_delta_ms"
            metric = {
                "name": "duration_delta_ms",
                "value": duration_delta_ms,
                "threshold": max_delta_ms,
                "margin_to_pass": (float(duration_delta_ms) - float(max_delta_ms)) if duration_delta_ms is not None else None,
                "direction": "<=",
            }
        elif reason_value == "duration_over_hard_cap":
            gate = "duration_hard_cap_ms"
            metric = {
                "name": "duration_delta_ms",
                "value": duration_delta_ms,
                "threshold": hard_cap_ms,
                "margin_to_pass": (float(duration_delta_ms) - float(hard_cap_ms)) if duration_delta_ms is not None else None,
                "direction": "<=",
            }
        elif reason_value == "preview_duration":
            expected_sec = expected_base.get("duration_hint_sec")
            try:
                expected_ms = int(expected_sec) * 1000 if expected_sec is not None else None
                candidate_sec = candidate.get("duration_sec")
                candidate_ms = int(candidate_sec) * 1000 if candidate_sec is not None else None
            except Exception:
                expected_ms, candidate_ms = None, None
            min_ms = max(45000, int(expected_ms * 0.45)) if expected_ms is not None else 45000
            gate = "preview_duration_min_ms"
            metric = {
                "name": "candidate_duration_ms",
                "value": candidate_ms,
                "threshold": min_ms,
                "margin_to_pass": (float(min_ms) - float(candidate_ms)) if candidate_ms is not None else None,
                "direction": ">=",
            }
        elif reason_value in {"low_title_similarity", "floor_check_failed", "pass_b_track_similarity"}:
            gate = "title_similarity"
            threshold = _MUSIC_PASS_B_MIN_TITLE_SIMILARITY if reason_value == "pass_b_track_similarity" else similarity_thresholds["title_similarity"]
            metric = {
                "name": "title_similarity",
                "value": title_similarity,
                "threshold": threshold,
                "margin_to_pass": threshold - title_similarity,
                "direction": ">=",
            }
        elif reason_value in {"low_artist_similarity", "cover_artist_mismatch", "pass_b_artist_similarity"}:
            gate = "artist_similarity"
            threshold = _MUSIC_PASS_B_MIN_ARTIST_SIMILARITY if reason_value == "pass_b_artist_similarity" else similarity_thresholds["artist_similarity"]
            metric = {
                "name": "artist_similarity",
                "value": artist_similarity,
                "threshold": threshold,
                "margin_to_pass": threshold - artist_similarity,
                "direction": ">=",
            }
        elif reason_value == "low_album_similarity":
            gate = "album_similarity"
            metric = {
                "name": "album_similarity",
                "value": album_similarity,
                "threshold": similarity_thresholds["album_similarity"],
                "margin_to_pass": similarity_thresholds["album_similarity"] - album_similarity,
                "direction": ">=",
            }
        elif reason_value in {"disallowed_variant", "preview_variant", "session_variant"}:
            gate = "variant_alignment"
            metric = {
                "name": "variant_alignment",
                "value": 0.0,
                "threshold": 1.0,
                "margin_to_pass": 1.0,
                "direction": "==",
            }
        elif reason_value == "pass_b_authority":
            gate = "authority_channel_match"
            authority_value = 1.0 if bool(candidate.get("authority_channel_match")) else 0.0
            metric = {
                "name": "authority_channel_match",
                "value": authority_value,
                "threshold": 1.0,
                "margin_to_pass": 1.0 - authority_value,
                "direction": "==",
            }

        return {
            "candidate_id": candidate.get("candidate_id"),
            "source": candidate.get("source"),
            "title": candidate.get("title"),
            "variant_tags": sorted(classify_music_title_variants(candidate.get("title"))),
            "rejection_reason": reason_value or "score_threshold",
            "top_failed_gate": gate,
            "nearest_pass_margin": metric,
            "final_score": final_score,
            "pass": pass_name,
        }

    def _nearest_rejection(self, rejected_candidates):
        if not rejected_candidates:
            return None

        def _margin_value(entry):
            metric = entry.get("nearest_pass_margin") if isinstance(entry.get("nearest_pass_margin"), dict) else {}
            value = metric.get("margin_to_pass")
            try:
                return abs(float(value))
            except Exception:
                return float("inf")

        ranked = sorted(
            [entry for entry in rejected_candidates if isinstance(entry, dict)],
            key=lambda entry: (
                _margin_value(entry),
                -float(entry.get("final_score") or 0.0),
                str(entry.get("candidate_id") or ""),
            ),
        )
        return ranked[0] if ranked else None

    def _top_rejected_variant_tags(self, rejected_candidates):
        tag_counts = {}
        for entry in rejected_candidates or []:
            if not isinstance(entry, dict):
                continue
            tags = entry.get("variant_tags") if isinstance(entry.get("variant_tags"), list) else []
            for tag in tags:
                key = str(tag or "").strip()
                if not key:
                    continue
                tag_counts[key] = int(tag_counts.get(key) or 0) + 1
        ranked = sorted(tag_counts.items(), key=lambda item: (-int(item[1]), item[0]))
        return [tag for tag, _count in ranked]

    def rank_and_gate(self, ctx, candidates) -> MusicTrackSelectionResult:
        expected_base = dict((ctx or {}).get("expected_base") or {})
        coherence_key = (ctx or {}).get("coherence_key")
        query_label = str((ctx or {}).get("query_label") or "")
        rung = int((ctx or {}).get("rung") or 0)
        recording_mbid = str((ctx or {}).get("recording_mbid") or "").strip() or None
        selected = None
        selected_pass = None
        ranked_selected: list[dict] = []
        failure_reason = "no_candidate_above_threshold"
        coherence_boost_applied = 0
        mb_injected_rejections: dict[str, int] = {}
        rejected_candidates: list[dict] = []
        accepted_selection = None
        final_rejection = None
        variant_distribution = {}
        for candidate in candidates:
            tags = classify_music_title_variants(candidate.get("title"))
            for tag in tags:
                variant_distribution[tag] = int(variant_distribution.get(tag) or 0) + 1

        def _score_for_pass(duration_max_delta_ms):
            scored = []
            expected = dict(expected_base)
            expected["duration_max_delta_ms"] = int(duration_max_delta_ms)
            for candidate in candidates:
                source = candidate.get("source")
                adapter = self.adapters.get(source)
                source_modifier = adapter.source_modifier(candidate) if adapter else 1.0
                item = dict(candidate)
                item.update(score_candidate(expected, item, source_modifier=source_modifier))
                breakdown = item.get("score_breakdown") if isinstance(item.get("score_breakdown"), dict) else {}
                item["base_score"] = breakdown.get("raw_score_100")
                item["final_score_100"] = breakdown.get("final_score_100")
                scored.append(item)
            return rank_candidates(scored, source_priority=list(MUSIC_TRACK_SOURCE_PRIORITY_WITH_MB))

        ranked_a = _score_for_pass(_MUSIC_DURATION_STRICT_MAX_DELTA_MS)
        if coherence_key:
            ranked_a, applied = self._apply_album_coherence_tiebreak(
                ranked_a,
                coherence_key=coherence_key,
                pass_name="strict",
                query_label=query_label,
            )
            coherence_boost_applied += applied
        for scored in ranked_a:
            rejection_reason = str(scored.get("rejection_reason") or "").strip()
            if rejection_reason:
                rejected_candidates.append(
                    self._build_candidate_observation(
                        scored,
                        reason=rejection_reason,
                        expected_base=expected_base,
                        pass_name="strict",
                    )
                )
            elif float(scored.get("final_score", 0.0)) < float(self.music_source_match_threshold):
                rejected_candidates.append(
                    self._build_candidate_observation(
                        scored,
                        reason="score_threshold",
                        expected_base=expected_base,
                        pass_name="strict",
                    )
                )
            if str(scored.get("source") or "").strip().lower() != "mb_relationship":
                continue
            if not rejection_reason:
                continue
            key = self._classify_mb_injected_rejection(rejection_reason)
            mb_injected_rejections[key] = int(mb_injected_rejections.get(key) or 0) + 1
            _log_event(
                logging.INFO,
                "mb_youtube_injected_rejected",
                recording_mbid=recording_mbid,
                candidate_id=scored.get("candidate_id"),
                url=scored.get("url"),
                rejection_reason=rejection_reason,
                classified_reason=key,
                selected_rung=rung,
                query_label=query_label,
            )
        eligible_a = [
            c for c in ranked_a
            if not c.get("rejection_reason")
            and float(c.get("final_score", 0.0)) >= float(self.music_source_match_threshold)
        ]
        selected_a = eligible_a[0] if eligible_a else None
        if selected_a is not None:
            selected = selected_a
            ranked_selected = ranked_a
            selected_pass = "strict"
            selected_score = float(selected_a.get("final_score") or 0.0)
            runner_up_score = None
            for item in ranked_a:
                if item.get("candidate_id") == selected_a.get("candidate_id"):
                    continue
                if item.get("rejection_reason"):
                    continue
                runner_up_score = float(item.get("final_score") or 0.0)
                break
            title_similarity = max(
                float(selected_a.get("score_track") or 0.0),
                min(float(selected_a.get("score_track_relaxed") or 0.0) * 0.90, 1.0),
            )
            accepted_selection = {
                "selected_candidate_id": selected_a.get("candidate_id"),
                "selected_score": selected_score,
                "runner_up_score": runner_up_score,
                "runner_up_gap": selected_score - float(runner_up_score or 0.0),
                    "top_supporting_features": {
                        "duration_delta_ms": selected_a.get("duration_delta_ms"),
                        "title_similarity": title_similarity,
                        "artist_similarity": float(selected_a.get("score_artist") or 0.0),
                        "variant_alignment": True,
                        "variant_tags": sorted(classify_music_title_variants(selected_a.get("title"))),
                    },
                }
            return MusicTrackSelectionResult(
                selected=selected,
                selected_pass=selected_pass,
                ranked=ranked_selected,
                failure_reason="",
                coherence_boost_applied=coherence_boost_applied,
                mb_injected_rejections=mb_injected_rejections,
                rejected_candidates=rejected_candidates,
                accepted_selection=accepted_selection,
                final_rejection=None,
                candidate_variant_distribution=dict(sorted(variant_distribution.items(), key=lambda item: item[0])),
                selected_candidate_variant_tags=sorted(classify_music_title_variants(selected_a.get("title"))),
                top_rejected_variant_tags=self._top_rejected_variant_tags(rejected_candidates),
            )

        ranked_b = _score_for_pass(_MUSIC_DURATION_EXPANDED_MAX_DELTA_MS)
        if coherence_key:
            ranked_b, applied = self._apply_album_coherence_tiebreak(
                ranked_b,
                coherence_key=coherence_key,
                pass_name="expanded",
                query_label=query_label,
            )
            coherence_boost_applied += applied
        eligible_b = []
        for candidate in ranked_b:
            candidate_reason = str(candidate.get("rejection_reason") or "").strip()
            if candidate_reason:
                rejected_candidates.append(
                    self._build_candidate_observation(
                        candidate,
                        reason=candidate_reason,
                        expected_base=expected_base,
                        pass_name="expanded",
                    )
                )
                continue
            if float(candidate.get("final_score", 0.0)) < float(self.music_source_match_threshold):
                rejected_candidates.append(
                    self._build_candidate_observation(
                        candidate,
                        reason="score_threshold",
                        expected_base=expected_base,
                        pass_name="expanded",
                    )
                )
                continue
            try:
                delta_ok = candidate.get("duration_delta_ms") is not None and int(candidate.get("duration_delta_ms")) <= _MUSIC_DURATION_EXPANDED_MAX_DELTA_MS
            except Exception:
                delta_ok = False
            if not delta_ok:
                rejected_candidates.append(
                    self._build_candidate_observation(
                        candidate,
                        reason="pass_b_duration",
                        expected_base=expected_base,
                        pass_name="expanded",
                    )
                )
                continue
            if float(candidate.get("score_track", 0.0)) < _MUSIC_PASS_B_MIN_TITLE_SIMILARITY:
                rejected_candidates.append(
                    self._build_candidate_observation(
                        candidate,
                        reason="pass_b_track_similarity",
                        expected_base=expected_base,
                        pass_name="expanded",
                    )
                )
                if str(candidate.get("source") or "").strip().lower() == "mb_relationship":
                    key = "mb_injected_failed_title"
                    mb_injected_rejections[key] = int(mb_injected_rejections.get(key) or 0) + 1
                continue
            if float(candidate.get("score_artist", 0.0)) < _MUSIC_PASS_B_MIN_ARTIST_SIMILARITY:
                rejected_candidates.append(
                    self._build_candidate_observation(
                        candidate,
                        reason="pass_b_artist_similarity",
                        expected_base=expected_base,
                        pass_name="expanded",
                    )
                )
                if str(candidate.get("source") or "").strip().lower() == "mb_relationship":
                    key = "mb_injected_failed_artist"
                    mb_injected_rejections[key] = int(mb_injected_rejections.get(key) or 0) + 1
                continue
            if not bool(candidate.get("authority_channel_match")):
                rejected_candidates.append(
                    self._build_candidate_observation(
                        candidate,
                        reason="pass_b_authority",
                        expected_base=expected_base,
                        pass_name="expanded",
                    )
                )
                if str(candidate.get("source") or "").strip().lower() == "mb_relationship":
                    key = "mb_injected_failed_authority"
                    mb_injected_rejections[key] = int(mb_injected_rejections.get(key) or 0) + 1
                continue
            eligible_b.append(candidate)
        selected_b = eligible_b[0] if eligible_b else None
        if selected_b is not None:
            selected = selected_b
            ranked_selected = ranked_b
            selected_pass = "expanded"
            selected_score = float(selected_b.get("final_score") or 0.0)
            runner_up_score = None
            for item in ranked_b:
                if item.get("candidate_id") == selected_b.get("candidate_id"):
                    continue
                if item.get("rejection_reason"):
                    continue
                if float(item.get("final_score", 0.0)) < float(self.music_source_match_threshold):
                    continue
                runner_up_score = float(item.get("final_score") or 0.0)
                break
            title_similarity = max(
                float(selected_b.get("score_track") or 0.0),
                min(float(selected_b.get("score_track_relaxed") or 0.0) * 0.90, 1.0),
            )
            accepted_selection = {
                "selected_candidate_id": selected_b.get("candidate_id"),
                "selected_score": selected_score,
                "runner_up_score": runner_up_score,
                "runner_up_gap": selected_score - float(runner_up_score or 0.0),
                    "top_supporting_features": {
                        "duration_delta_ms": selected_b.get("duration_delta_ms"),
                        "title_similarity": title_similarity,
                        "artist_similarity": float(selected_b.get("score_artist") or 0.0),
                        "variant_alignment": True,
                        "variant_tags": sorted(classify_music_title_variants(selected_b.get("title"))),
                    },
                }
            return MusicTrackSelectionResult(
                selected=selected,
                selected_pass=selected_pass,
                ranked=ranked_selected,
                failure_reason="",
                coherence_boost_applied=coherence_boost_applied,
                mb_injected_rejections=mb_injected_rejections,
                rejected_candidates=rejected_candidates,
                accepted_selection=accepted_selection,
                final_rejection=None,
                candidate_variant_distribution=dict(sorted(variant_distribution.items(), key=lambda item: item[0])),
                selected_candidate_variant_tags=sorted(classify_music_title_variants(selected_b.get("title"))),
                top_rejected_variant_tags=self._top_rejected_variant_tags(rejected_candidates),
            )

        pass_a_reasons = {str(c.get("rejection_reason") or "") for c in ranked_a if c.get("rejection_reason")}
        if "duration_out_of_bounds" in pass_a_reasons or "duration_over_hard_cap" in pass_a_reasons:
            failure_reason = "duration_filtered"
        final_rejection = self._nearest_rejection(rejected_candidates)
        if isinstance(final_rejection, dict):
            final_rejection = {
                "failure_reason": failure_reason,
                "top_failed_gate": final_rejection.get("top_failed_gate"),
                "nearest_pass_margin": final_rejection.get("nearest_pass_margin"),
                "candidate_id": final_rejection.get("candidate_id"),
            }
        return MusicTrackSelectionResult(
            selected=None,
            selected_pass=None,
            ranked=[],
            failure_reason=failure_reason,
            coherence_boost_applied=coherence_boost_applied,
            mb_injected_rejections=mb_injected_rejections,
            rejected_candidates=rejected_candidates,
            accepted_selection=None,
            final_rejection=final_rejection,
            candidate_variant_distribution=dict(sorted(variant_distribution.items(), key=lambda item: item[0])),
            selected_candidate_variant_tags=[],
            top_rejected_variant_tags=self._top_rejected_variant_tags(rejected_candidates),
        )

    def search_music_track_best_match(
        self,
        artist,
        track,
        album=None,
        duration_ms=None,
        limit=6,
        *,
        start_rung=0,
        coherence_context=None,
        track_aliases=None,
        track_disambiguation=None,
        mb_youtube_urls=None,
        recording_mbid=None,
    ):
        expected_duration_hint_sec = (int(duration_ms) // 1000) if duration_ms is not None else None
        ladder = self._build_music_track_query_ladder(artist, track, album)
        coherence_key = self._coherence_context_key(coherence_context)
        normalized_aliases = []
        if isinstance(track_aliases, (list, tuple, set)):
            seen_aliases = set()
            for value in track_aliases:
                text = str(value or "").strip()
                if not text:
                    continue
                lowered = text.lower()
                if lowered in seen_aliases:
                    continue
                seen_aliases.add(lowered)
                normalized_aliases.append(text)
        normalized_disambiguation = str(track_disambiguation or "").strip() or None
        if not ladder:
            self.last_music_track_search = {
                "attempted": [],
                "start_rung": 0,
                "selected_rung": None,
                "selected_pass": None,
                "failure_reason": "no_candidates",
                "coherence_key": coherence_key,
                "coherence_boost_applied": 0,
                "decision_edge": {
                    "accepted_selection": None,
                    "rejected_candidates": [],
                    "final_rejection": None,
                    "candidate_variant_distribution": {},
                    "selected_candidate_variant_tags": [],
                    "top_rejected_variant_tags": [],
                },
            }
            return None
        first_rung = max(0, min(int(start_rung or 0), len(ladder) - 1))
        attempted = []
        selected = None
        selected_ranked = []
        selected_query = None
        selected_rung = None
        selected_pass = None
        failure_reason = "no_candidates"
        coherence_boost_applied = 0
        resolved_injected = self._resolve_mb_relationship_candidates(
            mb_youtube_urls=mb_youtube_urls,
            artist=artist,
            track=track,
            album=album,
            recording_mbid=recording_mbid,
        )
        if isinstance(resolved_injected, tuple) and len(resolved_injected) == 2:
            mb_injected_candidates, mb_probe_rejections = resolved_injected
        else:
            mb_injected_candidates, mb_probe_rejections = resolved_injected, {}
        mb_injected_rejections = dict(mb_probe_rejections or {})
        mb_injected_selected = False
        decision_rejected_candidates = []
        decision_accepted = None
        decision_final_rejection = None
        decision_variant_distribution = {}
        decision_selected_variant_tags = []
        decision_top_rejected_variant_tags = []

        for ladder_entry in ladder[first_rung:]:
            query = str(ladder_entry.get("query") or "").strip()
            rung = int(ladder_entry.get("rung") or 0)
            query_label = str(ladder_entry.get("label") or f"rung_{rung}")
            expected_base = {
                "artist": artist,
                "track": track,
                "album": album,
                "query": query,
                "media_intent": "music_track",
                "duration_hint_sec": expected_duration_hint_sec,
                "duration_hard_cap_ms": _MUSIC_DURATION_HARD_CAP_MS,
                "variant_allow_tokens": {"live"} if self._music_track_is_live(artist, track, album) else set(),
                "track_aliases": normalized_aliases,
                "track_disambiguation": normalized_disambiguation,
            }
            candidates = self.retrieve_candidates(
                {
                    "query": query,
                    "limit": limit,
                    "query_label": query_label,
                    "rung": rung,
                    "first_rung": first_rung,
                    "mb_injected_candidates": mb_injected_candidates,
                }
            )
            rung_meta = {
                "rung": rung,
                "query_label": query_label,
                "query": query,
                "candidates": len(candidates),
                "selected_pass": None,
            }
            if not candidates:
                rung_meta["failure_reason"] = "no_candidates"
                attempted.append(rung_meta)
                _log_event(
                    logging.INFO,
                    "music_query_rung_evaluated",
                    rung=rung,
                    query_label=query_label,
                    query=query,
                    candidates=0,
                    selected_pass=None,
                    failure_reason="no_candidates",
                )
                continue

            rank_result = self.rank_and_gate(
                {
                    "expected_base": expected_base,
                    "coherence_key": coherence_key,
                    "query_label": query_label,
                    "rung": rung,
                    "recording_mbid": recording_mbid,
                },
                candidates,
            )
            coherence_boost_applied += int(rank_result.coherence_boost_applied or 0)
            for key, value in (rank_result.mb_injected_rejections or {}).items():
                mb_injected_rejections[key] = int(mb_injected_rejections.get(key) or 0) + int(value or 0)
            for tag, count in (rank_result.candidate_variant_distribution or {}).items():
                tag_key = str(tag or "").strip()
                if not tag_key:
                    continue
                decision_variant_distribution[tag_key] = int(decision_variant_distribution.get(tag_key) or 0) + int(count or 0)
            for entry in (rank_result.rejected_candidates or []):
                if isinstance(entry, dict):
                    decision_rejected_candidates.append(entry)
            if rank_result.selected is not None:
                selected = rank_result.selected
                selected_ranked = rank_result.ranked
                selected_query = query
                selected_rung = rung
                selected_pass = rank_result.selected_pass
                mb_injected_selected = str(selected.get("source") or "").strip().lower() == "mb_relationship"
                decision_accepted = rank_result.accepted_selection if isinstance(rank_result.accepted_selection, dict) else None
                decision_selected_variant_tags = list(rank_result.selected_candidate_variant_tags or [])
                decision_top_rejected_variant_tags = list(rank_result.top_rejected_variant_tags or [])
                decision_final_rejection = None
                rung_meta["selected_pass"] = selected_pass
                attempted.append(rung_meta)
                _log_event(
                    logging.INFO,
                    "music_query_rung_evaluated",
                    rung=rung,
                    query_label=query_label,
                    query=query,
                    candidates=len(candidates),
                    selected_pass=selected_pass,
                    failure_reason=None,
                )
                if coherence_key:
                    self._record_album_coherence_family(coherence_key, selected)
                break

            failure_reason = str(rank_result.failure_reason or "no_candidate_above_threshold")
            if isinstance(rank_result.final_rejection, dict):
                decision_final_rejection = rank_result.final_rejection
            decision_top_rejected_variant_tags = list(rank_result.top_rejected_variant_tags or decision_top_rejected_variant_tags)
            rung_meta["failure_reason"] = failure_reason
            attempted.append(rung_meta)
            _log_event(
                logging.INFO,
                "music_query_rung_evaluated",
                rung=rung,
                query_label=query_label,
                query=query,
                candidates=len(candidates),
                selected_pass=None,
                failure_reason=failure_reason,
            )
        if selected is None and not isinstance(decision_final_rejection, dict):
            nearest = self._nearest_rejection(decision_rejected_candidates)
            if isinstance(nearest, dict):
                decision_final_rejection = {
                    "failure_reason": failure_reason,
                    "top_failed_gate": nearest.get("top_failed_gate"),
                    "nearest_pass_margin": nearest.get("nearest_pass_margin"),
                    "candidate_id": nearest.get("candidate_id"),
                }
        if not decision_top_rejected_variant_tags:
            decision_top_rejected_variant_tags = self._top_rejected_variant_tags(decision_rejected_candidates)
        mb_injected_album_success_count = 0
        if mb_injected_selected and coherence_key:
            with self._album_coherence_lock:
                current = int(self._album_mb_injection_success.get(coherence_key) or 0) + 1
                self._album_mb_injection_success[coherence_key] = current
            mb_injected_album_success_count = current
            _log_event(
                logging.INFO,
                "mb_youtube_injected_selected",
                coherence_key=coherence_key,
                selected_rung=selected_rung,
                selected_pass=selected_pass,
                count_for_album_run=current,
            )
        self.last_music_track_search = {
            "attempted": attempted,
            "start_rung": first_rung,
            "selected_rung": selected_rung,
            "selected_pass": selected_pass,
            "failure_reason": None if selected is not None else failure_reason,
            "coherence_key": coherence_key,
            "coherence_boost_applied": coherence_boost_applied,
            "mb_injected_candidates": len(mb_injected_candidates),
            "mb_injected_selected": mb_injected_selected,
            "mb_injected_rejections": mb_injected_rejections,
            "mb_injected_album_success_count": mb_injected_album_success_count,
            "decision_edge": {
                "accepted_selection": decision_accepted,
                "rejected_candidates": decision_rejected_candidates,
                "final_rejection": None if selected is not None else decision_final_rejection,
                "candidate_variant_distribution": dict(sorted(decision_variant_distribution.items(), key=lambda item: item[0])),
                "selected_candidate_variant_tags": sorted({str(tag) for tag in (decision_selected_variant_tags or []) if str(tag or "").strip()}),
                "top_rejected_variant_tags": list(decision_top_rejected_variant_tags or []),
            },
        }
        ranked = selected_ranked
        if self.debug_music_scoring:
            selected_id = selected.get("candidate_id") if isinstance(selected, dict) else None
            for candidate in ranked:
                candidate_score = float(candidate.get("final_score") or 0.0)
                breakdown = candidate.get("score_breakdown") if isinstance(candidate.get("score_breakdown"), dict) else {}
                rejection_reason = candidate.get("rejection_reason")
                decision = "below_threshold"
                if rejection_reason:
                    decision = f"rejected:{rejection_reason}"
                elif selected_id and candidate.get("candidate_id") == selected_id:
                    decision = "selected"
                _log_event(
                    logging.DEBUG,
                    "music_track_candidate_scored",
                    query=selected_query,
                    selected_rung=selected_rung,
                    selected_pass=selected_pass,
                    source=candidate.get("source"),
                    title=candidate.get("title"),
                    duration=candidate.get("duration_sec"),
                    duration_delta=candidate.get("duration_delta_ms"),
                    score_artist=candidate.get("score_artist"),
                    score_track=candidate.get("score_track"),
                    score_album=candidate.get("score_album"),
                    score_duration=candidate.get("score_duration"),
                    penalties_applied=breakdown.get("penalty_reasons"),
                    final_score=candidate_score,
                    threshold=self.music_source_match_threshold,
                    acceptance_decision=decision,
                )
        return selected

    def _resolve_request_destination(self, destination):
        if not self.paths:
            return destination
        try:
            template = build_output_template(
                self.config,
                destination=destination,
                base_dir=self.paths.single_downloads_dir,
            )
            return template.get("output_dir")
        except ValueError:
            return destination

    def _get_request_override(self, request_id):
        overrides = getattr(self, "request_overrides", None)
        if isinstance(overrides, dict):
            return overrides.get(request_id)
        return None


    def _ensure_queue_schema(self):
        conn = sqlite3.connect(self.queue_db_path)
        try:
            ensure_download_jobs_table(conn)
        finally:
            conn.close()

    def create_search_request(self, payload):
        # Invariant: URLs must never enter the search pipeline
        if _payload_contains_url(payload):
            logging.info(
                safe_json_dumps(
                    {
                        "message": "SEARCH_BYPASS_URL",
                        "reason": "direct_or_playlist_url",
                        "payload": payload,
                    }
                )
            )
            return None
        return self.store.create_request(payload)

    def get_search_request(self, request_id):
        result = self.store.get_request(request_id)
        if not result:
            return None
        request = result.get("request") or {}
        request["resolved_destination"] = self._resolve_request_destination(request.get("destination_dir"))
        return result

    def list_item_candidates(self, item_id):
        return self.store.list_candidates(item_id)

    def list_search_requests(self, status=None, limit=None):
        return self.store.list_requests(status=status, limit=limit)

    def cancel_search_request(self, request_id):
        return self.store.cancel_request(request_id)

    def run_search_resolution_once(self, *, stop_event=None, request_id=None):
        request_row = self.store.claim_request(request_id)
        # Absolute guardrail: never resolve URL-based requests
        if request_row and _payload_contains_url(request_row):
            self.store.update_request_status(
                request_row["id"], "failed", error="url_must_bypass_search"
            )
            logging.info(
                safe_json_dumps(
                    {
                        "message": "SEARCH_ABORT_URL_LEAK",
                        "request_id": request_row["id"],
                    }
                )
            )
            return request_row["id"]
        if not request_row:
            return None

        request_id = request_row["id"]
        intent = request_row["intent"]

        auto_enqueue_value = request_row.get("auto_enqueue")
        auto_enqueue = True if auto_enqueue_value is None else bool(auto_enqueue_value)
        destination_dir = request_row.get("destination_dir") or None
        created_by = (request_row.get("created_by") or "").strip()
        playlist_id = None
        if created_by.startswith("spotify_playlist:"):
            playlist_id = created_by.split(":", 1)[1]
        job_origin = "spotify_playlist" if playlist_id else "search"
        if intent in {"artist", "artist_collection"}:
            self.store.update_request_status(request_id, "failed", error="not_implemented")
            _log_event(logging.WARNING, "request_failed", request_id=request_id, error="not_implemented")
            return request_id

        self.store.create_items_for_request(request_row)
        items = self.store.list_items(request_id)

        for item in items:
            if stop_event and stop_event.is_set():
                return request_id
            if item.get("status") in {"enqueued", "failed", "skipped"}:
                continue
            self.store.update_item_status(item["id"], "searching")
            _log_event(logging.INFO, "item_searching", request_id=request_id, item_id=item["id"])

            # Step #1: Progressive search results - adapter progress state
            adapters_total = len(_parse_source_priority(request_row.get("source_priority_json")))
            adapters_completed = 0
            self.store.update_request_progress(
                request_id,
                adapters_total=adapters_total,
                adapters_completed=0,
            )

            canonical_payload = None
            if self.canonical_resolver:
                if item["item_type"] == "album":
                    canonical_payload = self.canonical_resolver.resolve_album(item["artist"], item.get("album"))
                else:
                    canonical_payload = self.canonical_resolver.resolve_track(
                        item["artist"],
                        item.get("track"),
                        album=item.get("album"),
                    )

            source_priority = _parse_source_priority(request_row.get("source_priority_json"))
            max_candidates = int(request_row.get("max_candidates_per_source") or 5)
            scored = []

            # --- Parallel adapter execution (bounded) ---
            futures = {}
            with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_ADAPTERS, len(source_priority))) as pool:
                for source in source_priority:
                    adapter = self.adapters.get(source)
                    if not adapter:
                        _log_event(
                            logging.ERROR,
                            "adapter_missing",
                            request_id=request_id,
                            item_id=item["id"],
                            source=source,
                        )
                        adapters_completed += 1
                        self.store.update_request_progress(
                            request_id,
                            adapters_completed=adapters_completed,
                        )
                        continue

                    self.store.update_item_status(item["id"], "searching_source")
                    _log_event(
                        logging.INFO,
                        "adapter_search_started",
                        request_id=request_id,
                        item_id=item["id"],
                        source=source,
                        adapters_completed=adapters_completed,
                        adapters_total=adapters_total,
                    )

                    futures[
                        pool.submit(
                            _run_adapter_search,
                            adapter,
                            item,
                            max_candidates,
                            canonical_payload,
                        )
                    ] = source

                for fut in as_completed(futures):
                    source = futures[fut]
                    try:
                        candidates = fut.result()
                    except Exception as exc:
                        _log_event(
                            logging.ERROR,
                            "adapter_search_failed",
                            request_id=request_id,
                            item_id=item["id"],
                            source=source,
                            error=str(exc),
                        )
                        adapters_completed += 1
                        self.store.update_request_progress(
                            request_id,
                            adapters_completed=adapters_completed,
                        )
                        continue

                    for cand in candidates:
                        if not _is_http_url(cand.get("url")):
                            _log_event(
                                logging.WARNING,
                                "adapter_candidate_invalid_url",
                                request_id=request_id,
                                item_id=item["id"],
                                source=source,
                                url=cand.get("url"),
                            )
                            continue
                        cand["source"] = source
                        cand["candidate_id"] = str(
                            cand.get("candidate_id")
                            or cand.get("external_id")
                            or cand.get("url")
                            or ""
                        )
                        modifier = self.adapters[source].source_modifier(cand)
                        scores = score_candidate(item, cand, source_modifier=modifier)
                        cand.update(scores)
                        cand["canonical_json"] = safe_json_dumps(canonical_payload) if canonical_payload else None
                        cand["id"] = uuid4().hex
                        scored.append(cand)

                    if candidates:
                        # Insert partial candidates immediately for progressive UI updates
                        partial_ranked = rank_candidates(
                            scored,
                            source_priority=source_priority,
                        )
                        self.store.reset_candidates_for_item(item["id"])
                        self.store.insert_candidates(item["id"], partial_ranked)
                        _log_event(
                            logging.INFO,
                            "adapter_candidates_emitted",
                            request_id=request_id,
                            item_id=item["id"],
                            source=source,
                            candidates_total=len(partial_ranked),
                            adapters_completed=adapters_completed,
                            adapters_total=adapters_total,
                        )

                    adapters_completed += 1
                    _log_event(
                        logging.INFO,
                        "adapter_search_completed",
                        request_id=request_id,
                        item_id=item["id"],
                        source=source,
                        adapters_completed=adapters_completed,
                        adapters_total=adapters_total,
                    )
                    self.store.update_request_progress(
                        request_id,
                        adapters_completed=adapters_completed,
                    )

            # --- Final selection logic below: do not change ---
            if not scored:
                self.store.update_item_status(item["id"], "failed", error="no_candidates")
                _log_event(logging.WARNING, "item_failed", request_id=request_id, item_id=item["id"], error="no_candidates")
                continue

            ranked = rank_candidates(scored, source_priority=source_priority)
            self.store.reset_candidates_for_item(item["id"])
            self.store.insert_candidates(item["id"], ranked)
            self.store.update_item_status(item["id"], "candidate_found")
            _log_event(logging.INFO, "item_candidate_found", request_id=request_id, item_id=item["id"])

            min_score = float(request_row.get("min_match_score") or 0.92)
            request_media_type = request_row.get("media_type") or "generic"
            is_music_request = request_media_type == "music"
            selection_threshold = min_score if is_music_request else 0.0
            chosen = select_best_candidate(ranked, selection_threshold, source_priority=source_priority)
            if not chosen:
                self.store.update_item_status(item["id"], "failed", error="no_candidate_above_threshold")
                _log_event(
                    logging.WARNING,
                    "item_failed",
                    request_id=request_id,
                    item_id=item["id"],
                    error="no_candidate_above_threshold",
                )
                continue

            self.store.update_item_status(item["id"], "selected", chosen=chosen)
            _log_event(
                logging.INFO,
                "item_selected",
                request_id=request_id,
                item_id=item["id"],
                source=chosen.get("source"),
                score=chosen.get("final_score"),
            )

            if not auto_enqueue:
                # Invariant B: search-only requests never auto-enqueue download jobs.
                # Manual enqueue via API is always allowed.
                continue

            # Do not enqueue downloads from search for playlists (must bypass search)
            if chosen.get("url") and "list=" in chosen.get("url", ""):
                self.store.update_item_status(
                    item["id"], "skipped", error="playlist_url_bypass"
                )
                continue

            resolved_destination = None
            request_override = self._get_request_override(request_id)
            final_format_override = request_override.get("final_format") if request_override else None

            canonical_for_job = chosen.get("canonical_metadata") if isinstance(chosen, dict) else None
            if not canonical_for_job and isinstance(chosen, dict) and chosen.get("canonical_json"):
                try:
                    canonical_for_job = json.loads(chosen.get("canonical_json"))
                except json.JSONDecodeError:
                    canonical_for_job = None
            expected_music_metadata = dict(canonical_for_job) if isinstance(canonical_for_job, dict) else {}

            canonical_id = _extract_canonical_id(canonical_for_job or chosen)
            trace_id = uuid4().hex
            media_intent = "album" if item["item_type"] == "album" else "track"
            target_media_type = ("music" if _is_audio_final_format(final_format_override) else item["media_type"])
            if target_media_type == "music" and media_intent == "track":
                if not expected_music_metadata.get("artist"):
                    expected_music_metadata["artist"] = item.get("artist")
                if not expected_music_metadata.get("track"):
                    expected_music_metadata["track"] = item.get("track")
                if not expected_music_metadata.get("album"):
                    expected_music_metadata["album"] = item.get("album")
                if not expected_music_metadata.get("duration_ms"):
                    try:
                        hint_sec = int(request_row.get("duration_hint_sec")) if request_row.get("duration_hint_sec") is not None else None
                    except (TypeError, ValueError):
                        hint_sec = None
                    if hint_sec:
                        expected_music_metadata["duration_ms"] = hint_sec * 1000
            external_id = chosen.get("external_id") if isinstance(chosen, dict) else None
            canonical_url = canonicalize_url(chosen.get("source"), chosen.get("url"), external_id)
            try:
                enqueue_payload = build_download_job_payload(
                    config=self.config,
                    origin=job_origin,
                    origin_id=request_id,
                    media_type=target_media_type,
                    media_intent=media_intent,
                    source=chosen["source"],
                    url=chosen["url"],
                    input_url=chosen.get("url"),
                    destination=destination_dir,
                    base_dir=(self.paths.single_downloads_dir if self.paths is not None else "."),
                    final_format_override=final_format_override,
                    resolved_metadata=(expected_music_metadata if expected_music_metadata else canonical_for_job),
                    trace_id=trace_id,
                    canonical_id=canonical_id,
                    canonical_url=canonical_url,
                    external_id=external_id,
                )
                resolved_destination = enqueue_payload.get("resolved_destination")
            except ValueError as exc:
                self.store.update_item_status(item["id"], "failed", error="invalid_destination")
                _log_event(
                    logging.ERROR,
                    "item_failed",
                    request_id=request_id,
                    item_id=item["id"],
                    error=f"invalid_destination: {exc}",
                )
                continue
            job_id, created, dedupe_reason = self.queue_store.enqueue_job(**enqueue_payload)

            if not created and dedupe_reason == "duplicate":
                self.store.update_item_status(
                    item["id"],
                    "skipped",
                    chosen=chosen,
                    error="duplicate",
                )
                _log_event(
                    logging.INFO,
                    "job_skipped_duplicate",
                    request_id=request_id,
                    item_id=item["id"],
                    job_id=job_id,
                    source=chosen.get("source"),
                    origin=job_origin,
                    destination=resolved_destination,
                    media_type=item["media_type"],
                    dedupe_result=dedupe_reason,
                )
                continue

            if created:
                self.store.update_item_status(item["id"], "enqueued", chosen=chosen)
                _log_event(
                    logging.INFO,
                    "job_enqueued",
                    request_id=request_id,
                    item_id=item["id"],
                    job_id=job_id,
                    trace_id=trace_id,
                    source=chosen.get("source"),
                    origin=job_origin,
                    destination=resolved_destination,
                    media_type=item["media_type"],
                )
                if job_origin == "spotify_playlist" and playlist_id:
                    _log_event(
                        logging.INFO,
                        "search_enqueued",
                        playlist_id=playlist_id,
                        request_id=request_id,
                        job_id=job_id,
                        source=chosen.get("source"),
                        destination=resolved_destination,
                    )
            else:
                self.store.update_item_status(item["id"], "enqueued", chosen=chosen)
            _log_event(
                logging.INFO,
                "job_exists",
                request_id=request_id,
                item_id=item["id"],
                job_id=job_id,
                source=chosen.get("source"),
                destination=resolved_destination,
                media_type=item["media_type"],
            )

        items = self.store.list_items(request_id)
        if not auto_enqueue:
            has_candidates = any(
                item.get("status") in {"candidate_found", "selected", "enqueued", "skipped"}
                or item.get("error") == "no_candidate_above_threshold"
                for item in items
            )
            if has_candidates:
                self.store.update_request_status(request_id, "completed")
            else:
                self.store.update_request_status(request_id, "failed", error="no_candidates")
            return request_id

        has_enqueued = any(item.get("status") == "enqueued" for item in items)
        has_skipped = any(item.get("status") == "skipped" for item in items)
        if has_enqueued:
            status_value = "completed_with_skips" if has_skipped else "completed"
            self.store.update_request_status(request_id, status_value)
        elif has_skipped:
            self.store.update_request_status(request_id, "completed_with_skips")
        else:
            self.store.update_request_status(request_id, "failed", error="no_items_enqueued")
            self.store.update_request_progress(
                request_id,
                adapters_completed=adapters_total,
            )
        return request_id

    def enqueue_item_candidate(self, item_id, candidate_id, *, final_format_override=None):
        item = self.store.get_item(item_id)
        if not item:
            return None
        candidate = self.store.get_candidate(candidate_id)
        if not candidate or candidate.get("item_id") != item_id:
            return None
        candidate_url = candidate.get("url")
        if not _is_http_url(candidate_url):
            return None
        request = self.store.get_request_row(item.get("request_id"))
        if not request:
            return None
        if final_format_override is None:
            request_override = self._get_request_override(request.get("id"))
            if request_override:
                final_format_override = request_override.get("final_format")

        destination_dir = request.get("destination_dir") or None

        canonical_payload = None
        canonical_raw = candidate.get("canonical_json")
        if canonical_raw:
            try:
                canonical_payload = json.loads(canonical_raw)
            except json.JSONDecodeError:
                canonical_payload = None
        expected_music_metadata = dict(canonical_payload) if isinstance(canonical_payload, dict) else {}

        canonical_id = _extract_canonical_id(canonical_payload or candidate)
        trace_id = uuid4().hex
        media_intent = "album" if item.get("item_type") == "album" else "track"
        target_media_type = ("music" if _is_audio_final_format(final_format_override) else (item.get("media_type") or "generic"))
        if target_media_type == "music" and media_intent == "track":
            candidate_title = candidate.get("title")
            candidate_uploader = candidate.get("uploader") or candidate.get("channel")
            query_hint = " ".join(
                str(part).strip()
                for part in (
                    request.get("artist"),
                    request.get("track"),
                    request.get("album"),
                )
                if str(part or "").strip()
            ) or None
            parsed_artist, parsed_track = _parse_artist_track_from_candidate(
                str(candidate_title or ""),
                str(candidate_uploader or "") if candidate_uploader is not None else None,
                query_hint,
            )
            # Lookup-only normalized track used for deterministic MB binding input diagnostics.
            track_lookup = _normalize_title_for_mb_lookup(parsed_track or "")

            expected_music_metadata["artist"] = (
                parsed_artist
                or expected_music_metadata.get("artist")
                or item.get("artist")
            )
            expected_music_metadata["track"] = (
                parsed_track
                or expected_music_metadata.get("track")
                or item.get("track")
            )
            if track_lookup:
                expected_music_metadata["track_lookup"] = track_lookup

            candidate_album = candidate.get("album_detected") or candidate.get("album")
            if candidate_album and not expected_music_metadata.get("album"):
                expected_music_metadata["album"] = candidate_album

            if not expected_music_metadata.get("duration_ms"):
                duration_sec = candidate.get("duration_sec")
                if duration_sec is None:
                    duration_sec = candidate.get("duration")
                try:
                    duration_int = int(duration_sec) if duration_sec is not None else None
                except (TypeError, ValueError):
                    duration_int = None
                if duration_int and duration_int > 0:
                    expected_music_metadata["duration_ms"] = duration_int * 1000
                else:
                    try:
                        hint_sec = int(request.get("duration_hint_sec")) if request.get("duration_hint_sec") is not None else None
                    except (TypeError, ValueError):
                        hint_sec = None
                    if hint_sec:
                        expected_music_metadata["duration_ms"] = hint_sec * 1000
        external_id = candidate.get("external_id") if isinstance(candidate, dict) else None
        canonical_url = canonicalize_url(candidate.get("source"), candidate_url, external_id)
        output_template_overrides = {}
        candidate_title = str(candidate.get("title") or "").strip()
        candidate_uploader = str(
            candidate.get("uploader") or candidate.get("channel") or candidate.get("artist_detected") or ""
        ).strip()
        if candidate_title:
            output_template_overrides["title"] = candidate_title
        if candidate_uploader:
            output_template_overrides["channel"] = candidate_uploader
            output_template_overrides["artist"] = candidate_uploader
        try:
            enqueue_payload = build_download_job_payload(
                config=self.config,
                origin="search",
                origin_id=request["id"],
                media_type=target_media_type,
                media_intent=media_intent,
                source=candidate.get("source"),
                url=candidate_url,
                input_url=candidate_url,
                destination=destination_dir,
                base_dir=(self.paths.single_downloads_dir if self.paths is not None else "."),
                final_format_override=final_format_override,
                resolved_metadata=(expected_music_metadata if expected_music_metadata else canonical_payload),
                output_template_overrides=(output_template_overrides or None),
                trace_id=trace_id,
                canonical_id=canonical_id,
                canonical_url=canonical_url,
                external_id=external_id,
            )
        except ValueError as exc:
            if str(exc.args[0] if exc.args else exc).strip() == "music_track_requires_mb_bound_metadata":
                reasons = []
                if len(exc.args) > 1 and isinstance(exc.args[1], (list, tuple)):
                    reasons = [str(item) for item in exc.args[1] if str(item or "").strip()]
                raise ValueError(
                    {
                        "error": "music_mode_mb_binding_failed",
                        "reason": reasons,
                    }
                )
            raise ValueError(f"invalid_destination: {exc}")
        resolved_destination = enqueue_payload.get("resolved_destination")
        job_id, created, dedupe_reason = self.queue_store.enqueue_job(**enqueue_payload)

        if not created and dedupe_reason == "duplicate":
            self.store.update_item_status(
                item_id,
                "skipped",
                chosen=candidate,
                error="duplicate",
            )
            _log_event(
                logging.INFO,
                "job_skipped_duplicate",
                request_id=request["id"],
                item_id=item_id,
                job_id=job_id,
                source=candidate.get("source"),
                origin="search",
                destination=resolved_destination,
                media_type=item.get("media_type"),
                dedupe_result=dedupe_reason,
            )
            return {"job_id": job_id, "created": False}

        self.store.update_item_status(item_id, "enqueued", chosen=candidate)
        if created:
            _log_event(
                logging.INFO,
                "job_enqueued",
                request_id=request["id"],
                item_id=item_id,
                job_id=job_id,
                trace_id=trace_id,
                source=candidate.get("source"),
                origin="search",
                destination=resolved_destination,
                media_type=item.get("media_type"),
            )
        else:
            _log_event(
                logging.INFO,
                "job_exists",
                request_id=request["id"],
                item_id=item_id,
                job_id=job_id,
                source=candidate.get("source"),
                destination=resolved_destination,
                media_type=item.get("media_type"),
            )

        if request.get("status") not in {"completed", "completed_with_skips", "failed"}:
            self.store.update_request_status(request["id"], "completed")

        return {"job_id": job_id, "created": created}

    def run_search_resolution_loop(self, *, poll_seconds=5, stop_event=None):
        while True:
            if stop_event and stop_event.is_set():
                return
            request_id = self.run_search_resolution_once(stop_event=stop_event)
            if request_id is None:
                time.sleep(poll_seconds)


def _parse_source_priority(raw_value):
    if not raw_value:
        return list(DEFAULT_SOURCE_PRIORITY)
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value if item]
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if item]
        except json.JSONDecodeError:
            parts = [part.strip() for part in raw_value.split(",")]
            return [part for part in parts if part]
    return list(DEFAULT_SOURCE_PRIORITY)


def _normalize_source_priority(raw_value):
    if raw_value is None:
        return list(DEFAULT_SOURCE_PRIORITY)
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value if item]
    if isinstance(raw_value, str):
        parts = [part.strip() for part in raw_value.split(",")]
        return [part for part in parts if part]
    return list(DEFAULT_SOURCE_PRIORITY)
