from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


RESOLUTION_API_SCHEMA_VERSION = 1
RESOLUTION_VERIFY_THRESHOLD = 2
RESOLUTION_SYNC_TIMEOUT = 5.0
RESOLUTION_SYNC_META_KEY = "local_cache_sync"
RESOLUTION_DATASET_SYNC_META_KEY = "dataset_sync"
RESOLUTION_UNRESOLVED_STATUS_PENDING = "pending"
RESOLUTION_UNRESOLVED_STATUS_RESOLVED = "resolved"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_resolution_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS resolution_sources (
            recording_mbid TEXT NOT NULL,
            source TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_id TEXT,
            duration_seconds INTEGER,
            media_format TEXT,
            bitrate_kbps INTEGER,
            file_hash TEXT,
            resolution_method TEXT,
            added_at TEXT NOT NULL,
            added_by TEXT,
            verification_status TEXT NOT NULL,
            verification_count INTEGER NOT NULL DEFAULT 0,
            verified_by_json TEXT NOT NULL DEFAULT '[]',
            last_verified_at TEXT,
            source_payload_json TEXT NOT NULL DEFAULT '{}',
            origin TEXT NOT NULL DEFAULT 'dataset',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (recording_mbid, source, source_url)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS resolution_meta (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS resolution_contributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id TEXT NOT NULL,
            action TEXT NOT NULL,
            recording_mbid TEXT NOT NULL,
            source_url TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS resolution_unresolved_queue (
            recording_mbid TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 1,
            last_reason TEXT,
            last_source TEXT,
            resolved_at TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_resolution_sources_recording ON resolution_sources (recording_mbid, verification_status, verification_count DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_resolution_sources_added_by ON resolution_sources (added_by)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_resolution_contributions_node ON resolution_contributions (node_id, action, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_resolution_unresolved_status ON resolution_unresolved_queue (status, updated_at DESC)"
    )
    conn.commit()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    ensure_resolution_tables(conn)
    return conn


def _normalize_verified_by(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            parsed = [value]
    else:
        parsed = value
    items: list[str] = []
    for item in parsed or []:
        normalized = str(item or "").strip()
        if normalized and normalized not in items:
            items.append(normalized)
    return items


def _verification_payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    verified_by = _normalize_verified_by(data.get("verified_by_json"))
    return {
        "status": str(data.get("verification_status") or "pending_verification"),
        "verification_count": int(data.get("verification_count") or 0),
        "verified_by": verified_by,
        "last_verified_at": str(data.get("last_verified_at") or "").strip() or None,
    }


def _source_response(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    payload = {}
    try:
        payload = json.loads(str(data.get("source_payload_json") or "{}"))
    except Exception:
        payload = {}
    verification = _verification_payload(data)
    verification_status = str(verification.get("status") or "pending_verification").strip()
    origin = str(data.get("origin") or "").strip().lower()
    status = "local_only" if origin != "dataset" else "verified" if verification_status == "verified" else "pending"
    instant_available = status in {"verified", "local_only"}
    return {
        "url": str(data.get("source_url") or "").strip(),
        "source": str(data.get("source") or "").strip() or None,
        "source_id": str(data.get("source_id") or "").strip() or None,
        "duration": int(data.get("duration_seconds")) if data.get("duration_seconds") is not None else None,
        "format": str(data.get("media_format") or "").strip() or None,
        "bitrate": int(data.get("bitrate_kbps")) if data.get("bitrate_kbps") is not None else None,
        "file_hash": str(data.get("file_hash") or "").strip() or None,
        "resolution_method": str(data.get("resolution_method") or "").strip() or None,
        "added_at": str(data.get("added_at") or "").strip() or None,
        "added_by": str(data.get("added_by") or "").strip() or None,
        "origin": origin or None,
        "verification": verification,
        "availability": {
            "status": status,
            "instant_streamable": instant_available,
            "instant_downloadable": instant_available,
        },
        "metadata": payload if isinstance(payload, dict) else {},
    }


def _best_source_score(item: dict[str, Any]) -> tuple[int, int, int, int, str]:
    availability = item.get("availability") if isinstance(item.get("availability"), dict) else {}
    status = str(availability.get("status") or "").strip()
    verified_weight = 3 if status == "verified" else 2 if status == "local_only" else 1 if status == "pending" else 0
    verification = item.get("verification") if isinstance(item.get("verification"), dict) else {}
    verification_count = int(verification.get("verification_count") or 0)
    bitrate = int(item.get("bitrate") or 0)
    duration = int(item.get("duration") or 0)
    return (
        verified_weight,
        verification_count,
        bitrate,
        duration,
        str(item.get("url") or ""),
    )


def _availability_payload(sources: list[dict[str, Any]], best_source: dict[str, Any] | None) -> dict[str, Any]:
    if not sources:
        return {
            "status": "not_found",
            "instant_available": False,
            "network_available": False,
            "best_source_status": None,
        }
    source_statuses = {
        str((((item.get("availability") or {}).get("status")) or "")).strip()
        for item in sources
    }
    best_status = str((((best_source or {}).get("availability") or {}).get("status") or "")).strip() or None
    if "verified" in source_statuses:
        overall = "verified"
    elif "pending" in source_statuses:
        overall = "pending"
    elif "local_only" in source_statuses:
        overall = "local_only"
    else:
        overall = "not_found"
    return {
        "status": overall,
        "instant_available": overall in {"verified", "local_only"},
        "network_available": overall in {"verified", "pending"},
        "best_source_status": best_status,
    }


def _record_response(mbid: str, rows: list[sqlite3.Row | dict[str, Any]]) -> dict[str, Any]:
    sources = [_source_response(row) for row in rows]
    sources.sort(
        key=lambda item: (
            0 if str((((item.get("availability") or {}).get("status")) or "")).strip() == "verified" else 1,
            0 if str((((item.get("availability") or {}).get("status")) or "")).strip() == "local_only" else 1,
            -int(((item.get("verification") or {}).get("verification_count") or 0)),
            -int(item.get("bitrate") or 0),
            -(int(item.get("duration") or 0) or 0),
            str(item.get("url") or ""),
        )
    )
    verified = sum(1 for item in sources if str((((item.get("availability") or {}).get("status")) or "")).strip() == "verified")
    pending = sum(1 for item in sources if str((((item.get("availability") or {}).get("status")) or "")).strip() == "pending")
    local_only = sum(1 for item in sources if str((((item.get("availability") or {}).get("status")) or "")).strip() == "local_only")
    best_source = max(sources, key=_best_source_score) if sources else None
    return {
        "schema_version": RESOLUTION_API_SCHEMA_VERSION,
        "entity_type": "recording",
        "mbid": str(mbid or "").strip().lower(),
        "availability": _availability_payload(sources, best_source),
        "best_source": dict(best_source) if isinstance(best_source, dict) else None,
        "selection": {
            "strategy": "verified_then_verification_count_then_quality",
            "selected_url": str((best_source or {}).get("url") or "").strip() or None,
        },
        "sources": sources,
        "stats": {
            "sources_total": len(sources),
            "verified_sources": verified,
            "pending_sources": pending,
            "local_only_sources": local_only,
        },
    }


def resolve_recording(db_path: str, mbid: str) -> dict[str, Any]:
    normalized = str(mbid or "").strip().lower()
    if not normalized:
        return _record_response("", [])
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM resolution_sources
            WHERE recording_mbid=?
            ORDER BY verification_status='verified' DESC, verification_count DESC, updated_at DESC, source_url ASC
            """,
            (normalized,),
        )
        rows = list(cur.fetchall())
    finally:
        conn.close()
    return _record_response(normalized, rows)


def resolve_bulk(db_path: str, mbids: list[str]) -> dict[str, Any]:
    normalized = [str(item or "").strip().lower() for item in mbids if str(item or "").strip()]
    normalized = list(dict.fromkeys(normalized))
    if not normalized:
        return {
            "schema_version": RESOLUTION_API_SCHEMA_VERSION,
            "entity_type": "recording_bulk",
            "results": [],
        }
    conn = _connect(db_path)
    grouped: dict[str, list[sqlite3.Row]] = {mbid: [] for mbid in normalized}
    try:
        placeholders = ",".join("?" for _ in normalized)
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT *
            FROM resolution_sources
            WHERE recording_mbid IN ({placeholders})
            ORDER BY verification_status='verified' DESC, verification_count DESC, updated_at DESC, source_url ASC
            """,
            normalized,
        )
        for row in cur.fetchall():
            grouped.setdefault(str(row["recording_mbid"] or "").strip().lower(), []).append(row)
    finally:
        conn.close()
    return {
        "schema_version": RESOLUTION_API_SCHEMA_VERSION,
        "entity_type": "recording_bulk",
        "results": [_record_response(mbid, grouped.get(mbid, [])) for mbid in normalized],
    }


def _coerce_duration_seconds(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _coerce_bitrate(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def submit_mapping(
    db_path: str,
    *,
    mbid: str,
    source_url: str,
    source: str,
    node_id: str,
    duration_seconds: int | None = None,
    media_format: str | None = None,
    bitrate_kbps: int | None = None,
    file_hash: str | None = None,
    resolution_method: str | None = None,
    source_id: str | None = None,
    raw_payload: dict[str, Any] | None = None,
    origin: str | None = None,
) -> dict[str, Any]:
    normalized_mbid = str(mbid or "").strip().lower()
    normalized_url = str(source_url or "").strip()
    normalized_source = str(source or "").strip().lower()
    normalized_node = str(node_id or "").strip()
    normalized_origin = str(origin or "submit").strip().lower() or "submit"
    if not normalized_mbid or not normalized_url or not normalized_source or not normalized_node:
        raise ValueError("mbid, source_url, source, and node_id are required")
    now_iso = utc_now()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM resolution_sources
            WHERE recording_mbid=? AND source=? AND source_url=?
            LIMIT 1
            """,
            (normalized_mbid, normalized_source, normalized_url),
        )
        existing = cur.fetchone()
        if existing is None:
            verified_by = [normalized_node]
            cur.execute(
                """
                INSERT INTO resolution_sources (
                    recording_mbid, source, source_url, source_id, duration_seconds, media_format,
                    bitrate_kbps, file_hash, resolution_method, added_at, added_by,
                    verification_status, verification_count, verified_by_json, last_verified_at,
                    source_payload_json, origin, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_mbid,
                    normalized_source,
                    normalized_url,
                    str(source_id or "").strip() or None,
                    _coerce_duration_seconds(duration_seconds),
                    str(media_format or "").strip() or None,
                    _coerce_bitrate(bitrate_kbps),
                    str(file_hash or "").strip() or None,
                    str(resolution_method or "").strip() or None,
                    now_iso,
                    normalized_node,
                    "pending_verification",
                    1,
                    json.dumps(verified_by, sort_keys=True),
                    now_iso,
                    json.dumps(raw_payload or {}, sort_keys=True),
                    normalized_origin,
                    now_iso,
                ),
            )
            conn.commit()
            cur.execute(
                """
                INSERT INTO resolution_contributions (node_id, action, recording_mbid, source_url, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (normalized_node, "submit", normalized_mbid, normalized_url, now_iso),
            )
            conn.commit()
            status = "created"
        else:
            verified_by = _normalize_verified_by(existing["verified_by_json"])
            if normalized_node not in verified_by:
                verified_by.append(normalized_node)
            cur.execute(
                """
                UPDATE resolution_sources
                SET source_id=COALESCE(?, source_id),
                    duration_seconds=COALESCE(?, duration_seconds),
                    media_format=COALESCE(?, media_format),
                    bitrate_kbps=COALESCE(?, bitrate_kbps),
                    file_hash=COALESCE(?, file_hash),
                    resolution_method=COALESCE(?, resolution_method),
                    verified_by_json=?,
                    verification_count=?,
                    last_verified_at=?,
                    source_payload_json=?,
                    updated_at=?
                WHERE recording_mbid=? AND source=? AND source_url=?
                """,
                (
                    str(source_id or "").strip() or None,
                    _coerce_duration_seconds(duration_seconds),
                    str(media_format or "").strip() or None,
                    _coerce_bitrate(bitrate_kbps),
                    str(file_hash or "").strip() or None,
                    str(resolution_method or "").strip() or None,
                    json.dumps(verified_by, sort_keys=True),
                    max(int(existing["verification_count"] or 0), len(verified_by)),
                    now_iso,
                    json.dumps(raw_payload or {}, sort_keys=True),
                    now_iso,
                    normalized_mbid,
                    normalized_source,
                    normalized_url,
                ),
            )
            conn.commit()
            cur.execute(
                """
                INSERT INTO resolution_contributions (node_id, action, recording_mbid, source_url, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (normalized_node, "submit_update", normalized_mbid, normalized_url, now_iso),
            )
            conn.commit()
            status = "updated"
    finally:
        conn.close()
    return {
        "status": status,
        "mbid": normalized_mbid,
        "source": normalized_source,
        "url": normalized_url,
        "origin": normalized_origin,
    }


def verify_mapping(
    db_path: str,
    *,
    mbid: str,
    source_url: str,
    verifier_id: str,
    duration_seconds: int | None = None,
    media_format: str | None = None,
    bitrate_kbps: int | None = None,
    file_hash: str | None = None,
    threshold: int = RESOLUTION_VERIFY_THRESHOLD,
) -> dict[str, Any]:
    normalized_mbid = str(mbid or "").strip().lower()
    normalized_url = str(source_url or "").strip()
    normalized_verifier = str(verifier_id or "").strip()
    if not normalized_mbid or not normalized_url or not normalized_verifier:
        raise ValueError("mbid, source_url, and verifier_id are required")
    now_iso = utc_now()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM resolution_sources
            WHERE recording_mbid=? AND source_url=?
            LIMIT 1
            """,
            (normalized_mbid, normalized_url),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError("mapping_not_found")
        verified_by = _normalize_verified_by(row["verified_by_json"])
        if normalized_verifier not in verified_by:
            verified_by.append(normalized_verifier)
        verification_count = max(int(row["verification_count"] or 0), len(verified_by))
        status = "verified" if verification_count >= max(1, int(threshold)) else "pending_verification"
        cur.execute(
            """
            UPDATE resolution_sources
            SET duration_seconds=COALESCE(?, duration_seconds),
                media_format=COALESCE(?, media_format),
                bitrate_kbps=COALESCE(?, bitrate_kbps),
                file_hash=COALESCE(?, file_hash),
                verification_status=?,
                verification_count=?,
                verified_by_json=?,
                last_verified_at=?,
                updated_at=?
            WHERE recording_mbid=? AND source_url=?
            """,
            (
                _coerce_duration_seconds(duration_seconds),
                str(media_format or "").strip() or None,
                _coerce_bitrate(bitrate_kbps),
                str(file_hash or "").strip() or None,
                status,
                verification_count,
                json.dumps(verified_by, sort_keys=True),
                now_iso,
                now_iso,
                normalized_mbid,
                normalized_url,
            ),
        )
        cur.execute(
            """
            INSERT INTO resolution_contributions (node_id, action, recording_mbid, source_url, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (normalized_verifier, "verify", normalized_mbid, normalized_url, now_iso),
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "status": status,
        "mbid": normalized_mbid,
        "url": normalized_url,
        "verification_count": verification_count,
        "verified_by": verified_by,
    }


def enqueue_unresolved_mbid(
    db_path: str,
    *,
    mbid: str,
    reason: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    normalized = str(mbid or "").strip().lower()
    if not normalized:
        raise ValueError("mbid is required")
    now_iso = utc_now()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT recording_mbid, status, attempt_count FROM resolution_unresolved_queue WHERE recording_mbid=? LIMIT 1",
            (normalized,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                """
                INSERT INTO resolution_unresolved_queue (
                    recording_mbid, status, first_seen_at, last_seen_at, attempt_count,
                    last_reason, last_source, resolved_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized,
                    RESOLUTION_UNRESOLVED_STATUS_PENDING,
                    now_iso,
                    now_iso,
                    1,
                    str(reason or "").strip() or None,
                    str(source or "").strip() or None,
                    None,
                    now_iso,
                ),
            )
            status = "created"
            attempts = 1
        else:
            attempts = int(row["attempt_count"] or 0) + 1
            cur.execute(
                """
                UPDATE resolution_unresolved_queue
                SET status=?,
                    last_seen_at=?,
                    attempt_count=?,
                    last_reason=?,
                    last_source=?,
                    resolved_at=NULL,
                    updated_at=?
                WHERE recording_mbid=?
                """,
                (
                    RESOLUTION_UNRESOLVED_STATUS_PENDING,
                    now_iso,
                    attempts,
                    str(reason or "").strip() or None,
                    str(source or "").strip() or None,
                    now_iso,
                    normalized,
                ),
            )
            status = "updated"
        conn.commit()
    finally:
        conn.close()
    return {"status": status, "mbid": normalized, "attempt_count": attempts}


def mark_unresolved_mbid_resolved(conn: sqlite3.Connection, mbid: str) -> None:
    normalized = str(mbid or "").strip().lower()
    if not normalized:
        return
    now_iso = utc_now()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE resolution_unresolved_queue
        SET status=?, resolved_at=?, updated_at=?
        WHERE recording_mbid=?
        """,
        (RESOLUTION_UNRESOLVED_STATUS_RESOLVED, now_iso, now_iso, normalized),
    )


def upsert_local_acquired_mapping(
    db_path: str,
    *,
    mbid: str,
    source_url: str,
    source: str,
    node_id: str,
    duration_seconds: int | None = None,
    media_format: str | None = None,
    bitrate_kbps: int | None = None,
    file_hash: str | None = None,
    resolution_method: str | None = None,
    source_id: str | None = None,
    raw_payload: dict[str, Any] | None = None,
    threshold: int = RESOLUTION_VERIFY_THRESHOLD,
) -> dict[str, Any]:
    normalized_mbid = str(mbid or "").strip().lower()
    normalized_url = str(source_url or "").strip()
    normalized_source = str(source or "").strip().lower()
    normalized_node = str(node_id or "").strip()
    if not normalized_mbid or not normalized_url or not normalized_source or not normalized_node:
        raise ValueError("mbid, source_url, source, and node_id are required")
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM resolution_sources
            WHERE recording_mbid=? AND source=? AND source_url=?
            LIMIT 1
            """,
            (normalized_mbid, normalized_source, normalized_url),
        )
        exists = cur.fetchone() is not None
    finally:
        conn.close()
    if exists:
        result = verify_mapping(
            db_path,
            mbid=normalized_mbid,
            source_url=normalized_url,
            verifier_id=normalized_node,
            duration_seconds=duration_seconds,
            media_format=media_format,
            bitrate_kbps=bitrate_kbps,
            file_hash=file_hash,
            threshold=threshold,
        )
        result["action"] = "verified_existing"
    else:
        result = submit_mapping(
            db_path,
            mbid=normalized_mbid,
            source_url=normalized_url,
            source=normalized_source,
            node_id=normalized_node,
            duration_seconds=duration_seconds,
            media_format=media_format,
            bitrate_kbps=bitrate_kbps,
            file_hash=file_hash,
            resolution_method=resolution_method,
            source_id=source_id,
            raw_payload=raw_payload,
            origin="local_acquisition",
        )
        result["action"] = "submitted_local_only"
    conn = _connect(db_path)
    try:
        mark_unresolved_mbid_resolved(conn, normalized_mbid)
        conn.commit()
    finally:
        conn.close()
    return result


def build_stats(db_path: str) -> dict[str, Any]:
    conn = _connect(db_path)
    contributor_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT recording_mbid) FROM resolution_sources")
        total_mbids = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("SELECT COUNT(*) FROM resolution_sources WHERE verification_status='verified'")
        total_verified = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("SELECT COUNT(*) FROM resolution_sources WHERE verification_status!='verified'")
        total_pending = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("SELECT COUNT(*) FROM resolution_sources WHERE origin!='dataset'")
        total_local_only = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("SELECT COUNT(*) FROM resolution_unresolved_queue WHERE status=?", (RESOLUTION_UNRESOLVED_STATUS_PENDING,))
        unresolved_pending = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("SELECT node_id, action FROM resolution_contributions")
        contribution_rows = cur.fetchall()
        cur.execute("SELECT value_json FROM resolution_meta WHERE key=? LIMIT 1", (RESOLUTION_DATASET_SYNC_META_KEY,))
        dataset_sync_row = cur.fetchone()
        cur.execute("SELECT value_json FROM resolution_meta WHERE key=? LIMIT 1", (RESOLUTION_SYNC_META_KEY,))
        local_sync_row = cur.fetchone()
    finally:
        conn.close()
    for row in contribution_rows:
        node_id = str(row["node_id"] or "").strip()
        action = str(row["action"] or "").strip()
        if node_id:
            contributor_counts[node_id] += 1
        if action:
            action_counts[action] += 1
    top_contributors = [
        {"node_id": node_id, "contributions": count}
        for node_id, count in contributor_counts.most_common(20)
    ]
    dataset_sync = None
    if dataset_sync_row is not None:
        try:
            dataset_sync = json.loads(str(dataset_sync_row[0] or "{}"))
        except Exception:
            dataset_sync = None
    local_sync = None
    if local_sync_row is not None:
        try:
            local_sync = json.loads(str(local_sync_row[0] or "{}"))
        except Exception:
            local_sync = None
    return {
        "schema_version": RESOLUTION_API_SCHEMA_VERSION,
        "total_resolved_mbids": total_mbids,
        "total_verified_mappings": total_verified,
        "total_pending_mappings": total_pending,
        "total_local_only_mappings": total_local_only,
        "unresolved_queue_pending": unresolved_pending,
        "top_contributors": top_contributors,
        "total_nodes": len(contributor_counts),
        "contributions": dict(sorted(action_counts.items())),
        "dataset_sync": dataset_sync if isinstance(dataset_sync, dict) else None,
        "local_cache_sync": local_sync if isinstance(local_sync, dict) else None,
    }


def build_health(db_path: str) -> dict[str, Any]:
    stats = build_stats(db_path)
    overall = "ok"
    if int(stats.get("total_resolved_mbids") or 0) == 0:
        overall = "degraded"
    return {
        "schema_version": RESOLUTION_API_SCHEMA_VERSION,
        "status": overall,
        "checked_at": utc_now(),
        "stats": stats,
    }


def _distinct_recording_updates(
    conn: sqlite3.Connection,
    *,
    updated_after: str | None = None,
    limit: int = 500,
) -> list[sqlite3.Row]:
    cur = conn.cursor()
    if updated_after:
        cur.execute(
            """
            SELECT recording_mbid, MAX(updated_at) AS updated_at
            FROM resolution_sources
            GROUP BY recording_mbid
            HAVING MAX(updated_at) >= ?
            ORDER BY MAX(updated_at) ASC, recording_mbid ASC
            LIMIT ?
            """,
            (str(updated_after).strip(), int(limit)),
        )
    else:
        cur.execute(
            """
            SELECT recording_mbid, MAX(updated_at) AS updated_at
            FROM resolution_sources
            GROUP BY recording_mbid
            ORDER BY MAX(updated_at) ASC, recording_mbid ASC
            LIMIT ?
            """,
            (int(limit),),
        )
    return list(cur.fetchall())


def build_snapshot(db_path: str, *, limit: int = 500) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        rows = _distinct_recording_updates(conn, updated_after=None, limit=limit)
    finally:
        conn.close()
    results = [resolve_recording(db_path, str(row["recording_mbid"] or "")) for row in rows]
    cursor = str(rows[-1]["updated_at"] or "").strip() if rows else None
    return {
        "schema_version": RESOLUTION_API_SCHEMA_VERSION,
        "type": "snapshot",
        "cursor": cursor,
        "results": results,
    }


def build_diff(db_path: str, *, since: str, limit: int = 500) -> dict[str, Any]:
    conn = _connect(db_path)
    try:
        rows = _distinct_recording_updates(conn, updated_after=since, limit=limit)
    finally:
        conn.close()
    results = [resolve_recording(db_path, str(row["recording_mbid"] or "")) for row in rows]
    cursor = str(rows[-1]["updated_at"] or "").strip() if rows else str(since or "").strip() or None
    return {
        "schema_version": RESOLUTION_API_SCHEMA_VERSION,
        "type": "diff",
        "since": str(since or "").strip() or None,
        "cursor": cursor,
        "results": results,
    }


def upsert_resolution_record_from_dataset_record(
    conn: sqlite3.Connection,
    *,
    recording_mbid: str,
    record: dict[str, Any],
) -> None:
    ensure_resolution_tables(conn)
    normalized_mbid = str(recording_mbid or "").strip().lower()
    if not normalized_mbid or not isinstance(record, dict):
        return
    now_iso = utc_now()
    cur = conn.cursor()
    sources = record.get("sources") if isinstance(record.get("sources"), list) else []
    for item in sources:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source") or "youtube").strip().lower() or "youtube"
        source_id = str(item.get("candidate_id") or item.get("video_id") or "").strip() or None
        source_url = str(item.get("candidate_url") or "").strip()
        if not source_url and source == "youtube" and source_id:
            source_url = f"https://www.youtube.com/watch?v={source_id}"
        if not source_url:
            continue
        added_by = str(item.get("verified_by") or "community_cache").strip() or "community_cache"
        last_verified_at = str(item.get("last_verified_at") or record.get("updated_at") or now_iso).strip() or now_iso
        duration_seconds = None
        try:
            duration_ms = item.get("duration_ms")
            if duration_ms is not None:
                duration_seconds = max(1, int(int(duration_ms) / 1000))
        except Exception:
            duration_seconds = None
        bitrate = None
        try:
            bitrate = _coerce_bitrate(item.get("bitrate") or item.get("bitrate_kbps"))
        except Exception:
            bitrate = None
        verified_by = [added_by] if added_by else []
        cur.execute(
            """
            INSERT INTO resolution_sources (
                recording_mbid, source, source_url, source_id, duration_seconds, media_format,
                bitrate_kbps, file_hash, resolution_method, added_at, added_by,
                verification_status, verification_count, verified_by_json, last_verified_at,
                source_payload_json, origin, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(recording_mbid, source, source_url) DO UPDATE SET
                source_id=excluded.source_id,
                duration_seconds=COALESCE(excluded.duration_seconds, resolution_sources.duration_seconds),
                media_format=COALESCE(excluded.media_format, resolution_sources.media_format),
                bitrate_kbps=COALESCE(excluded.bitrate_kbps, resolution_sources.bitrate_kbps),
                file_hash=COALESCE(excluded.file_hash, resolution_sources.file_hash),
                resolution_method=COALESCE(excluded.resolution_method, resolution_sources.resolution_method),
                added_at=CASE
                    WHEN excluded.added_at < resolution_sources.added_at THEN excluded.added_at
                    ELSE resolution_sources.added_at
                END,
                added_by=COALESCE(resolution_sources.added_by, excluded.added_by),
                verification_status='verified',
                verification_count=CASE
                    WHEN resolution_sources.verification_count > 0 THEN resolution_sources.verification_count
                    ELSE excluded.verification_count
                END,
                verified_by_json=CASE
                    WHEN length(COALESCE(resolution_sources.verified_by_json, '')) > 2 THEN resolution_sources.verified_by_json
                    ELSE excluded.verified_by_json
                END,
                last_verified_at=CASE
                    WHEN excluded.last_verified_at > COALESCE(resolution_sources.last_verified_at, '') THEN excluded.last_verified_at
                    ELSE resolution_sources.last_verified_at
                END,
                source_payload_json=excluded.source_payload_json,
                updated_at=excluded.updated_at
            """,
            (
                normalized_mbid,
                source,
                source_url,
                source_id,
                duration_seconds,
                str(item.get("format") or "").strip() or None,
                bitrate,
                str(item.get("file_hash") or "").strip() or None,
                str(item.get("retreivr_version") or item.get("resolution_method") or "").strip() or None,
                str(record.get("updated_at") or last_verified_at).strip() or last_verified_at,
                added_by,
                "verified",
                max(1, len(verified_by)),
                json.dumps(verified_by, sort_keys=True),
                last_verified_at,
                json.dumps(item, sort_keys=True),
                "dataset",
                str(record.get("updated_at") or last_verified_at).strip() or now_iso,
            ),
        )
    mark_unresolved_mbid_resolved(conn, normalized_mbid)


def _dataset_record_root(dataset_root: str | Path) -> Path:
    root = Path(dataset_root)
    if (root / "youtube" / "recording").is_dir():
        return root / "youtube" / "recording"
    if (root / "recording").is_dir():
        return root / "recording"
    return root / "youtube" / "recording"


def _dataset_record_path(dataset_root: str | Path, recording_mbid: str) -> Path:
    normalized = str(recording_mbid or "").strip().lower()
    root = _dataset_record_root(dataset_root)
    return root / normalized[:2] / f"{normalized}.json"


def _record_to_dataset_payload(record: dict[str, Any]) -> dict[str, Any]:
    mbid = str(record.get("mbid") or record.get("recording_mbid") or "").strip().lower()
    sources_out: list[dict[str, Any]] = []
    updated_at = utc_now()
    for source in record.get("sources") if isinstance(record.get("sources"), list) else []:
        if not isinstance(source, dict):
            continue
        metadata = source.get("metadata") if isinstance(source.get("metadata"), dict) else {}
        verification = source.get("verification") if isinstance(source.get("verification"), dict) else {}
        source_id = str(source.get("source_id") or metadata.get("candidate_id") or metadata.get("video_id") or "").strip()
        source_url = str(source.get("url") or metadata.get("candidate_url") or "").strip()
        source_name = str(source.get("source") or metadata.get("source") or "youtube").strip().lower() or "youtube"
        last_verified_at = str(
            verification.get("last_verified_at")
            or metadata.get("last_verified_at")
            or record.get("updated_at")
            or updated_at
        ).strip() or updated_at
        updated_at = max(updated_at, last_verified_at)
        video_id = str(metadata.get("video_id") or source_id or "").strip() or None
        item = {
            "source": source_name,
            "candidate_url": source_url or None,
            "candidate_id": source_id or None,
            "video_id": video_id,
            "duration_ms": int(source.get("duration") or 0) * 1000 if source.get("duration") is not None else None,
            "format": source.get("format"),
            "bitrate": source.get("bitrate"),
            "file_hash": source.get("file_hash"),
            "resolution_method": source.get("resolution_method"),
            "last_verified_at": last_verified_at,
            "verified_by": (verification.get("verified_by") or [source.get("added_by") or "resolution_api"])[0],
            "confidence": metadata.get("confidence"),
            "retreivr_version": metadata.get("retreivr_version") or source.get("resolution_method"),
        }
        sources_out.append({key: value for key, value in item.items() if value is not None})
    return {
        "schema_version": RESOLUTION_API_SCHEMA_VERSION,
        "recording_mbid": mbid,
        "updated_at": updated_at,
        "sources": sources_out,
    }


def get_local_sync_status(db_path: str) -> dict[str, Any] | None:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT value_json FROM resolution_meta WHERE key=? LIMIT 1", (RESOLUTION_SYNC_META_KEY,))
        row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    try:
        payload = json.loads(str(row[0] or "{}"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def sync_local_cache_from_api(
    *,
    db_path: str,
    dataset_root: str | Path,
    api_base_url: str,
    limit: int = 500,
    timeout: float = RESOLUTION_SYNC_TIMEOUT,
) -> dict[str, Any]:
    base_url = str(api_base_url or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("api_base_url is required")
    if int(limit) <= 0:
        raise ValueError("limit must be >= 1")
    previous = get_local_sync_status(db_path) or {}
    since = str(previous.get("cursor") or "").strip() or None
    mode = "diff" if since else "snapshot"
    endpoint = f"{base_url}/resolve/diff" if since else f"{base_url}/resolve/snapshot"
    params = {"limit": int(limit)}
    if since:
        params["since"] = since
    response = requests.get(endpoint, params=params, timeout=float(timeout))
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results") if isinstance(payload, dict) else []
    if not isinstance(results, list):
        results = []
    files_written = 0
    records_synced = 0
    root = _dataset_record_root(dataset_root)
    root.mkdir(parents=True, exist_ok=True)
    for item in results:
        if not isinstance(item, dict):
            continue
        mbid = str(item.get("mbid") or "").strip().lower()
        if not mbid:
            continue
        local_record = _record_to_dataset_payload(item)
        path = _dataset_record_path(dataset_root, mbid)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(local_record, indent=2, sort_keys=True) + "\n"
        existing = None
        try:
            if path.exists():
                existing = path.read_text(encoding="utf-8")
        except Exception:
            existing = None
        if existing != content:
            tmp_path = path.with_suffix(f"{path.suffix}.tmp")
            tmp_path.write_text(content, encoding="utf-8")
            tmp_path.replace(path)
            files_written += 1
        records_synced += 1
    resolution_rebuild = rebuild_resolution_index_from_dataset(db_path=db_path, dataset_root=dataset_root)
    try:
        from engine import community_cache

        reverse_index = community_cache.rebuild_reverse_index_from_dataset(db_path=db_path, dataset_root=dataset_root)
    except Exception:
        reverse_index = None
    summary = {
        "status": "completed",
        "mode": mode,
        "api_base_url": base_url,
        "since": since,
        "cursor": str(payload.get("cursor") or "").strip() or since,
        "results_count": records_synced,
        "files_written": files_written,
        "updated_at": utc_now(),
        "resolution_rebuild": resolution_rebuild,
        "reverse_index": reverse_index if isinstance(reverse_index, dict) else None,
    }
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO resolution_meta (key, value_json) VALUES (?, ?)",
            (RESOLUTION_SYNC_META_KEY, json.dumps(summary, sort_keys=True)),
        )
        conn.commit()
    finally:
        conn.close()
    return summary


def rebuild_resolution_index_from_dataset(*, db_path: str, dataset_root: str | Path) -> dict[str, int]:
    root = Path(dataset_root)
    if (root / "youtube" / "recording").is_dir():
        root = root / "youtube" / "recording"
    elif (root / "recording").is_dir():
        root = root / "recording"
    else:
        root = root / "youtube" / "recording"
    files_scanned = 0
    records_indexed = 0
    sources_indexed = 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM resolution_sources WHERE origin='dataset'")
        if root.exists() and root.is_dir():
            for path in sorted(root.glob("*/*.json")):
                files_scanned += 1
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                recording_mbid = str(payload.get("recording_mbid") or path.stem).strip().lower()
                before = conn.total_changes
                upsert_resolution_record_from_dataset_record(
                    conn,
                    recording_mbid=recording_mbid,
                    record=payload,
                )
                if conn.total_changes > before:
                    records_indexed += 1
                    sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
                    sources_indexed += sum(1 for item in sources if isinstance(item, dict))
        cur.execute(
            "INSERT OR REPLACE INTO resolution_meta (key, value_json) VALUES (?, ?)",
            (RESOLUTION_DATASET_SYNC_META_KEY, json.dumps({"updated_at": utc_now(), "files_scanned": files_scanned}, sort_keys=True)),
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "files_scanned": files_scanned,
        "records_indexed": records_indexed,
        "sources_indexed": sources_indexed,
    }
