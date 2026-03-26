"""
Retreivr Community Cache Lookup

Provides a lightweight client for retrieving optional community transport
hints from the Retreivr community index hosted on GitHub.

The community cache is an **accelerator only**. It never overrides the
canonical deterministic resolver pipeline.

Lookup order (handled by caller):

MusicBrainz resolve
    ↓
Local acquisition cache
    ↓
Community cache (this module)
    ↓
Transport search ladder

If a community entry fails validation or download later, it must be
invalidated locally and the resolver should fall back to normal search.
"""

import json
import logging
import os
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor

import requests
import sqlite3


logger = logging.getLogger(__name__)
_RESOLUTION_INDEX_DB_PATH: str | None = None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GITHUB_RAW_BASE = (
    "https://raw.githubusercontent.com/"
    "sudostacks/retreivr-community-cache/main/youtube/recording"
)

REQUEST_TIMEOUT = 0.8  # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _prefix_from_mbid(recording_mbid: str) -> str:
    """Return the prefix shard for a recording MBID."""
    return recording_mbid[0:2]


def _build_url(recording_mbid: str) -> str:
    """Build GitHub raw URL for a recording entry."""
    prefix = _prefix_from_mbid(recording_mbid)
    return f"{GITHUB_RAW_BASE}/{prefix}/{recording_mbid}.json"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_community_record(recording_mbid: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a community cache entry from GitHub.

    Returns parsed JSON dict if present, otherwise None.
    """

    url = _build_url(recording_mbid)

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)

        if response.status_code == 404:
            logger.debug(
                "community_cache_miss recording_mbid=%s", recording_mbid
            )
            return None

        response.raise_for_status()

        data = response.json()

        logger.info(
            "community_cache_hit recording_mbid=%s", recording_mbid
        )

        return data

    except requests.RequestException as e:
        logger.debug(
            "community_cache_error recording_mbid=%s error=%s",
            recording_mbid,
            str(e),
        )

    except json.JSONDecodeError:
        logger.warning(
            "community_cache_invalid_json recording_mbid=%s",
            recording_mbid,
        )

    return None


def _record_path(dataset_root: str | Path, recording_mbid: str) -> Path:
    normalized = str(recording_mbid or "").strip().lower()
    root = _dataset_record_root(dataset_root)
    return root / normalized[:2] / f"{normalized}.json"


def configure_resolution_index_db_path(db_path: str | None) -> None:
    global _RESOLUTION_INDEX_DB_PATH
    _RESOLUTION_INDEX_DB_PATH = str(db_path or "").strip() or None


def load_local_community_record(recording_mbid: str, *, dataset_root: str | Path | None) -> Optional[Dict[str, Any]]:
    normalized = str(recording_mbid or "").strip().lower()
    if not normalized or not dataset_root:
        return None
    path = _record_path(dataset_root, normalized)
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def persist_local_community_record(
    recording_mbid: str,
    record: Dict[str, Any],
    *,
    dataset_root: str | Path | None,
) -> bool:
    normalized = str(recording_mbid or "").strip().lower()
    if not normalized or not dataset_root or not isinstance(record, dict):
        return False
    path = _record_path(dataset_root, normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(record, indent=2, sort_keys=True) + "\n"
    existing = None
    try:
        if path.exists():
            existing = path.read_text(encoding="utf-8")
    except Exception:
        existing = None
    if existing == content:
        return False
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, path)
    if _RESOLUTION_INDEX_DB_PATH:
        try:
            from engine.resolution_api import _connect, upsert_resolution_record_from_dataset_record

            conn = _connect(_RESOLUTION_INDEX_DB_PATH)
            try:
                upsert_resolution_record_from_dataset_record(
                    conn,
                    recording_mbid=normalized,
                    record=record,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.debug("community_cache_resolution_index_update_failed recording_mbid=%s", normalized, exc_info=True)
    return True


# ---------------------------------------------------------------------------
# Candidate Extraction
# ---------------------------------------------------------------------------


def extract_best_candidate(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract the highest confidence candidate from a community record.

    Community entries may contain multiple sources.
    This function returns the highest confidence one.
    """

    sources = record.get("sources", [])

    if not sources:
        return None

    sources_sorted = sorted(
        sources,
        key=lambda s: s.get("confidence", 0),
        reverse=True,
    )

    best = sources_sorted[0]

    # Basic sanity checks
    if "video_id" not in best:
        return None

    return best


def _candidate_score(candidate: Dict[str, Any]) -> tuple[float, str]:
    try:
        confidence = float(candidate.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    verified = str(candidate.get("last_verified_at") or candidate.get("updated_at") or "")
    return confidence, verified


def _normalize_candidate(candidate: Dict[str, Any], *, provider: str) -> Dict[str, Any]:
    normalized = dict(candidate or {})
    normalized["provider"] = provider
    return normalized


def _pick_better_candidate(left: Dict[str, Any] | None, right: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not isinstance(left, dict):
        return dict(right) if isinstance(right, dict) else None
    if not isinstance(right, dict):
        return dict(left)
    left_key = _candidate_score(left)
    right_key = _candidate_score(right)
    if right_key > left_key:
        return dict(right)
    return dict(left)


# ---------------------------------------------------------------------------
# High Level Helper
# ---------------------------------------------------------------------------


def lookup_recording(recording_mbid: str) -> Optional[Dict[str, Any]]:
    """
    High-level lookup helper.

    Returns candidate source metadata or None.

    Returned structure example:

    {
        "video_id": "abc123",
        "duration_ms": 242000,
        "confidence": 0.97
    }
    """

    record = fetch_community_record(recording_mbid)

    if not record:
        return None

    candidate = extract_best_candidate(record)

    if not candidate:
        logger.debug(
            "community_cache_no_candidate recording_mbid=%s",
            recording_mbid,
        )
        return None

    return candidate


def lookup_recording_local(recording_mbid: str, *, dataset_root: str | Path | None) -> Optional[Dict[str, Any]]:
    record = load_local_community_record(recording_mbid, dataset_root=dataset_root)
    if not isinstance(record, dict):
        return None
    candidate = extract_best_candidate(record)
    if not isinstance(candidate, dict):
        return None
    return _normalize_candidate(candidate, provider="local_dataset")


# ---------------------------------------------------------------------------
# Optional: simple in-memory TTL cache
# ---------------------------------------------------------------------------


_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_CACHE_TTL = 3600
_CACHE_MAX_SIZE = 2048
_CACHE_LOCK = threading.RLock()
_IN_FLIGHT: Dict[str, threading.Event] = {}
_REVERSE_LOOKUP_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_REVERSE_LOOKUP_CACHE_TTL = 300
_REVERSE_LOOKUP_CACHE_MAX_SIZE = 4096
_REVERSE_LOOKUP_CACHE_LOCK = threading.RLock()


def cached_lookup(
    recording_mbid: str,
    *,
    dataset_root: str | Path | None = None,
    allow_remote: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Lookup with simple in-memory caching to avoid repeated GitHub hits.
    """

    mbid = str(recording_mbid or "").strip().lower()
    if not mbid:
        return None
    dataset_root_key = str(Path(dataset_root).resolve()) if dataset_root else ""
    cache_key = f"{dataset_root_key}|{int(bool(allow_remote))}|{mbid}"

    fetch_event: threading.Event | None = None
    should_fetch = False

    while True:
        now = time.time()
        with _CACHE_LOCK:
            cached = _CACHE.get(cache_key)
            if cached:
                if now - float(cached.get("ts") or 0.0) < float(_CACHE_TTL):
                    _CACHE.move_to_end(cache_key)
                    return cached.get("data")
                _CACHE.pop(cache_key, None)

            in_flight = _IN_FLIGHT.get(cache_key)
            if in_flight is None:
                fetch_event = threading.Event()
                _IN_FLIGHT[cache_key] = fetch_event
                should_fetch = True
                break

            fetch_event = in_flight

        # Another thread is already fetching this MBID; wait for completion
        # and then re-check the cache state.
        fetch_event.wait(timeout=REQUEST_TIMEOUT + 0.5)

    if not should_fetch:
        return None

    result: Optional[Dict[str, Any]] = None
    try:
        local_candidate = None
        remote_candidate = None
        if allow_remote:
            with ThreadPoolExecutor(max_workers=2) as pool:
                local_future = pool.submit(lookup_recording_local, mbid, dataset_root=dataset_root)
                remote_future = pool.submit(fetch_community_record, mbid)
                try:
                    local_candidate = local_future.result(timeout=REQUEST_TIMEOUT + 0.2)
                except Exception:
                    local_candidate = None
                try:
                    remote_raw = remote_future.result(timeout=REQUEST_TIMEOUT + 0.5)
                except Exception:
                    remote_raw = None
            if isinstance(remote_raw, dict):
                remote_best = extract_best_candidate(remote_raw)
                if isinstance(remote_best, dict):
                    remote_candidate = _normalize_candidate(remote_best, provider="remote_github")
                try:
                    persist_local_community_record(mbid, remote_raw, dataset_root=dataset_root)
                except Exception:
                    logger.debug("community_cache_local_persist_failed recording_mbid=%s", mbid, exc_info=True)
        else:
            local_candidate = lookup_recording_local(mbid, dataset_root=dataset_root)
        result = _pick_better_candidate(local_candidate, remote_candidate)
    finally:
        with _CACHE_LOCK:
            _CACHE[cache_key] = {
                "ts": time.time(),
                "data": result,
            }
            _CACHE.move_to_end(cache_key)
            while len(_CACHE) > int(_CACHE_MAX_SIZE):
                _CACHE.popitem(last=False)
            done_event = _IN_FLIGHT.pop(cache_key, None)
            if done_event is not None:
                done_event.set()

    return result


def sync_recording_to_local_dataset(
    recording_mbid: str,
    *,
    dataset_root: str | Path | None,
) -> dict[str, Any]:
    normalized = str(recording_mbid or "").strip().lower()
    if not normalized:
        return {"status": "invalid", "recording_mbid": None, "updated": False}
    remote = fetch_community_record(normalized)
    if not isinstance(remote, dict):
        return {"status": "miss", "recording_mbid": normalized, "updated": False}
    updated = persist_local_community_record(normalized, remote, dataset_root=dataset_root)
    return {"status": "synced", "recording_mbid": normalized, "updated": bool(updated)}


def lookup_video_id(video_id: str, *, db_path: str | None = None) -> Optional[Dict[str, Any]]:
    """
    Resolve a local reverse index entry by YouTube video_id.

    This uses local SQLite state only and never performs network calls.
    Returns None when no reverse index exists or no match is present.
    """
    normalized = str(video_id or "").strip().lower()
    if not normalized:
        return None
    now = time.time()
    with _REVERSE_LOOKUP_CACHE_LOCK:
        cached = _REVERSE_LOOKUP_CACHE.get(normalized)
        if cached:
            cached_ts = float(cached.get("ts") or 0.0)
            if now - cached_ts < float(_REVERSE_LOOKUP_CACHE_TTL):
                _REVERSE_LOOKUP_CACHE.move_to_end(normalized)
                payload = cached.get("payload")
                return dict(payload) if isinstance(payload, dict) else None
            _REVERSE_LOOKUP_CACHE.pop(normalized, None)

    if not db_path:
        return None

    payload = None
    conn = None
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT recording_mbid, confidence, updated_at
            FROM community_video_index
            WHERE video_id=?
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if row:
            payload = {
                "recording_mbid": row["recording_mbid"],
                "confidence": row["confidence"],
                "updated_at": row["updated_at"],
            }
    except sqlite3.OperationalError:
        payload = None
    except Exception:
        payload = None
    finally:
        if conn is not None:
            conn.close()

    with _REVERSE_LOOKUP_CACHE_LOCK:
        _REVERSE_LOOKUP_CACHE[normalized] = {
            "ts": time.time(),
            "payload": dict(payload) if isinstance(payload, dict) else None,
        }
        _REVERSE_LOOKUP_CACHE.move_to_end(normalized)
        while len(_REVERSE_LOOKUP_CACHE) > int(_REVERSE_LOOKUP_CACHE_MAX_SIZE):
            _REVERSE_LOOKUP_CACHE.popitem(last=False)

    return dict(payload) if isinstance(payload, dict) else None


def _as_sources(record: Dict[str, Any]) -> list[dict]:
    sources = record.get("sources")
    normalized = [dict(item) for item in (sources or []) if isinstance(item, dict)]
    if isinstance(record.get("video_id"), str):
        normalized.append(
            {
                "video_id": record.get("video_id"),
                "confidence": record.get("confidence"),
            }
        )
    return normalized


def _dataset_record_root(dataset_root: str | Path) -> Path:
    root = Path(dataset_root)
    if (root / "youtube" / "recording").is_dir():
        return root / "youtube" / "recording"
    if (root / "recording").is_dir():
        return root / "recording"
    return root / "youtube" / "recording"


def rebuild_reverse_index_from_dataset(*, db_path: str, dataset_root: str | Path) -> dict[str, int]:
    """
    Rebuild local community_video_index deterministically from local dataset files.

    No network calls are performed here.
    """
    root = _dataset_record_root(dataset_root)
    if not root.exists() or not root.is_dir():
        conn = sqlite3.connect(db_path, check_same_thread=False)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS community_video_index (
                    video_id TEXT PRIMARY KEY,
                    recording_mbid TEXT,
                    confidence REAL,
                    updated_at TEXT
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_community_video_index_recording ON community_video_index (recording_mbid)")
            cur.execute("BEGIN IMMEDIATE")
            cur.execute("DELETE FROM community_video_index")
            conn.commit()
        finally:
            conn.close()
        with _REVERSE_LOOKUP_CACHE_LOCK:
            _REVERSE_LOOKUP_CACHE.clear()
        return {"files_scanned": 0, "video_ids_indexed": 0}

    mapping: dict[str, tuple[float, str]] = {}
    files_scanned = 0
    for record_path in sorted(root.glob("*/*.json")):
        files_scanned += 1
        recording_mbid = record_path.stem.strip().lower()
        if not recording_mbid:
            continue
        try:
            payload = json.loads(record_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        for source in _as_sources(payload):
            video_id = str(source.get("video_id") or "").strip().lower()
            if not video_id:
                continue
            try:
                confidence = float(source.get("confidence") or 0.0)
            except Exception:
                confidence = 0.0
            current = mapping.get(video_id)
            if current is None:
                mapping[video_id] = (confidence, recording_mbid)
                continue
            current_confidence, current_mbid = current
            if confidence > current_confidence:
                mapping[video_id] = (confidence, recording_mbid)
            elif confidence == current_confidence and recording_mbid < current_mbid:
                mapping[video_id] = (confidence, recording_mbid)

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS community_video_index (
                video_id TEXT PRIMARY KEY,
                recording_mbid TEXT,
                confidence REAL,
                updated_at TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_community_video_index_recording ON community_video_index (recording_mbid)")
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM community_video_index")
        rows = [
            (video_id, recording_mbid, confidence, now_iso)
            for video_id, (confidence, recording_mbid) in sorted(mapping.items(), key=lambda item: item[0])
        ]
        cur.executemany(
            """
            INSERT INTO community_video_index (video_id, recording_mbid, confidence, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    with _REVERSE_LOOKUP_CACHE_LOCK:
        _REVERSE_LOOKUP_CACHE.clear()

    return {"files_scanned": files_scanned, "video_ids_indexed": len(mapping)}
