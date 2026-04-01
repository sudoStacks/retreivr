from __future__ import annotations

import json
import mimetypes
import os
import re
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from typing import Any

from db.downloaded_tracks import record_downloaded_track
from engine.paths import ensure_dir


REVIEW_STATUS_PENDING = "pending"
REVIEW_STATUS_ACCEPTED = "accepted"
REVIEW_STATUS_REJECTED = "rejected"
_REVIEW_STATUSES = {
    REVIEW_STATUS_PENDING,
    REVIEW_STATUS_ACCEPTED,
    REVIEW_STATUS_REJECTED,
}
_REVIEW_SENTINEL_PLAYLIST_ID = "__review_accept__"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_review_queue_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS review_queue_items (
            id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            parent_job_id TEXT,
            status TEXT NOT NULL,
            media_type TEXT NOT NULL,
            media_intent TEXT NOT NULL,
            source TEXT,
            candidate_url TEXT,
            candidate_id TEXT,
            failure_reason TEXT,
            top_failed_gate TEXT,
            nearest_pass_margin_json TEXT,
            candidate_details_json TEXT,
            canonical_metadata_json TEXT,
            target_destination TEXT,
            quarantine_root TEXT,
            file_path TEXT,
            filename TEXT,
            mime_type TEXT,
            file_size_bytes INTEGER,
            duration_ms INTEGER,
            bitrate_kbps INTEGER,
            artist TEXT,
            album TEXT,
            track TEXT,
            album_artist TEXT,
            recording_mbid TEXT,
            mb_release_id TEXT,
            trace_id TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_at TEXT,
            accepted_at TEXT,
            rejected_at TEXT,
            resolution_note TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_review_queue_status_created ON review_queue_items (status, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_review_queue_artist_status ON review_queue_items (artist, status, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_review_queue_album_status ON review_queue_items (album, status, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_review_queue_parent_job ON review_queue_items (parent_job_id)")
    conn.commit()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    ensure_review_queue_table(conn)
    return conn


def _json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, sort_keys=True)
    except Exception:
        return None


def _json_loads(value: Any, default: Any) -> Any:
    if not isinstance(value, str) or not value.strip():
        return default
    try:
        parsed = json.loads(value)
    except Exception:
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except Exception:
        return None
    return parsed if parsed >= 0 else None


def _cleanup_empty_review_dirs(file_path: str | None, quarantine_root: str | None) -> None:
    candidate = str(file_path or "").strip()
    root = str(quarantine_root or "").strip()
    if not candidate or not root:
        return
    try:
        current = os.path.dirname(os.path.abspath(candidate))
        stop_at = os.path.abspath(root)
    except Exception:
        return
    if not current or not stop_at:
        return
    while current.startswith(stop_at):
        if current == stop_at:
            break
        try:
            os.rmdir(current)
        except OSError:
            break
        current = os.path.dirname(current)


def _normalize_status(value: Any) -> str:
    normalized = str(value or REVIEW_STATUS_PENDING).strip().lower()
    return normalized if normalized in _REVIEW_STATUSES else REVIEW_STATUS_PENDING


def _row_to_item(row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    raw = dict(row)
    raw["status"] = _normalize_status(raw.get("status"))
    raw["nearest_pass_margin"] = _json_loads(raw.pop("nearest_pass_margin_json", None), {})
    raw["candidate_details"] = _json_loads(raw.pop("candidate_details_json", None), {})
    raw["canonical_metadata"] = _json_loads(raw.pop("canonical_metadata_json", None), {})
    return raw


def _candidate_detail_value(candidate: dict[str, Any], key: str) -> Any:
    if not isinstance(candidate, dict):
        return None
    return candidate.get(key)


def record_completed_review_item(db_path: str, job, file_path: str, *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    output_template = getattr(job, "output_template", None)
    if not isinstance(output_template, dict):
        output_template = {}
    canonical = output_template.get("canonical_metadata") if isinstance(output_template.get("canonical_metadata"), dict) else {}
    candidate_details = output_template.get("review_candidate_details")
    if not isinstance(candidate_details, dict):
        candidate_details = {}
    nearest_pass_margin = output_template.get("review_nearest_pass_margin")
    if not isinstance(nearest_pass_margin, dict):
        nearest_pass_margin = {}

    now = utc_now_iso()
    file_size_bytes = None
    try:
        file_size_bytes = int(os.path.getsize(file_path))
    except OSError:
        file_size_bytes = None
    mime_type, _ = mimetypes.guess_type(file_path)
    duration_ms = _safe_int((meta or {}).get("duration_ms"))
    if duration_ms is None:
        duration_sec = _safe_int((meta or {}).get("duration_sec"))
        if duration_sec is not None:
            duration_ms = duration_sec * 1000
    bitrate_kbps = None
    for key in ("abr", "audio_bitrate_kbps", "bitrate_kbps"):
        parsed = _safe_int((meta or {}).get(key))
        if parsed is not None:
            bitrate_kbps = parsed
            break

    payload = {
        "id": str(getattr(job, "canonical_id", "") or getattr(job, "id", "")).strip() or str(getattr(job, "id")),
        "job_id": str(getattr(job, "id", "")).strip(),
        "parent_job_id": str(output_template.get("review_parent_job_id") or "").strip() or None,
        "status": REVIEW_STATUS_PENDING,
        "media_type": str(getattr(job, "media_type", "") or "music").strip() or "music",
        "media_intent": str(getattr(job, "media_intent", "") or "music_track_review").strip() or "music_track_review",
        "source": str(getattr(job, "source", "") or "").strip() or None,
        "candidate_url": str(output_template.get("review_candidate_url") or getattr(job, "url", "") or "").strip() or None,
        "candidate_id": str(output_template.get("review_candidate_id") or "").strip() or None,
        "failure_reason": str(output_template.get("review_failure_reason") or "").strip() or None,
        "top_failed_gate": str(output_template.get("review_top_failed_gate") or "").strip() or None,
        "nearest_pass_margin_json": _json_dumps(nearest_pass_margin) or "{}",
        "candidate_details_json": _json_dumps(candidate_details) or "{}",
        "canonical_metadata_json": _json_dumps(canonical) or "{}",
        "target_destination": str(output_template.get("review_target_destination") or "").strip() or None,
        "quarantine_root": str(getattr(job, "resolved_destination", "") or "").strip() or None,
        "file_path": file_path,
        "filename": os.path.basename(file_path),
        "mime_type": mime_type or "application/octet-stream",
        "file_size_bytes": file_size_bytes,
        "duration_ms": duration_ms,
        "bitrate_kbps": bitrate_kbps,
        "artist": str(canonical.get("artist") or output_template.get("artist") or "").strip() or None,
        "album": str(canonical.get("album") or output_template.get("album") or "").strip() or None,
        "track": str(canonical.get("track") or output_template.get("track") or "").strip() or None,
        "album_artist": str(canonical.get("album_artist") or output_template.get("album_artist") or "").strip() or None,
        "recording_mbid": str(canonical.get("recording_mbid") or output_template.get("recording_mbid") or "").strip() or None,
        "mb_release_id": str(canonical.get("mb_release_id") or output_template.get("mb_release_id") or "").strip() or None,
        "trace_id": str(getattr(job, "trace_id", "") or "").strip() or None,
        "created_at": now,
        "updated_at": now,
        "resolved_at": None,
        "accepted_at": None,
        "rejected_at": None,
        "resolution_note": None,
    }

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO review_queue_items (
                id, job_id, parent_job_id, status, media_type, media_intent,
                source, candidate_url, candidate_id, failure_reason, top_failed_gate,
                nearest_pass_margin_json, candidate_details_json, canonical_metadata_json,
                target_destination, quarantine_root, file_path, filename, mime_type,
                file_size_bytes, duration_ms, bitrate_kbps, artist, album, track,
                album_artist, recording_mbid, mb_release_id, trace_id,
                created_at, updated_at, resolved_at, accepted_at, rejected_at, resolution_note
            ) VALUES (
                :id, :job_id, :parent_job_id, :status, :media_type, :media_intent,
                :source, :candidate_url, :candidate_id, :failure_reason, :top_failed_gate,
                :nearest_pass_margin_json, :candidate_details_json, :canonical_metadata_json,
                :target_destination, :quarantine_root, :file_path, :filename, :mime_type,
                :file_size_bytes, :duration_ms, :bitrate_kbps, :artist, :album, :track,
                :album_artist, :recording_mbid, :mb_release_id, :trace_id,
                :created_at, :updated_at, :resolved_at, :accepted_at, :rejected_at, :resolution_note
            )
            ON CONFLICT(id) DO UPDATE SET
                job_id=excluded.job_id,
                parent_job_id=excluded.parent_job_id,
                status=excluded.status,
                media_type=excluded.media_type,
                media_intent=excluded.media_intent,
                source=excluded.source,
                candidate_url=excluded.candidate_url,
                candidate_id=excluded.candidate_id,
                failure_reason=excluded.failure_reason,
                top_failed_gate=excluded.top_failed_gate,
                nearest_pass_margin_json=excluded.nearest_pass_margin_json,
                candidate_details_json=excluded.candidate_details_json,
                canonical_metadata_json=excluded.canonical_metadata_json,
                target_destination=excluded.target_destination,
                quarantine_root=excluded.quarantine_root,
                file_path=excluded.file_path,
                filename=excluded.filename,
                mime_type=excluded.mime_type,
                file_size_bytes=excluded.file_size_bytes,
                duration_ms=excluded.duration_ms,
                bitrate_kbps=excluded.bitrate_kbps,
                artist=excluded.artist,
                album=excluded.album,
                track=excluded.track,
                album_artist=excluded.album_artist,
                recording_mbid=excluded.recording_mbid,
                mb_release_id=excluded.mb_release_id,
                trace_id=excluded.trace_id,
                updated_at=excluded.updated_at
            """,
            payload,
        )
        conn.commit()
        cur.execute("SELECT * FROM review_queue_items WHERE id=?", (payload["id"],))
        row = cur.fetchone()
        return _row_to_item(row) or payload
    finally:
        conn.close()


def list_review_queue_items(db_path: str, *, status: str = REVIEW_STATUS_PENDING, limit: int = 200) -> dict[str, Any]:
    normalized_status = _normalize_status(status)
    max_rows = max(1, min(int(limit or 200), 2000))
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM review_queue_items
            WHERE status=?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (normalized_status, max_rows),
        )
        items = [_row_to_item(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM review_queue_items
            GROUP BY status
            """
        )
        summary = {REVIEW_STATUS_PENDING: 0, REVIEW_STATUS_ACCEPTED: 0, REVIEW_STATUS_REJECTED: 0}
        for row in cur.fetchall():
            summary[_normalize_status(row["status"])] = int(row["count"] or 0)
        return {
            "items": [item for item in items if isinstance(item, dict)],
            "summary": summary,
        }
    finally:
        conn.close()


def get_review_queue_item(db_path: str, item_id: str) -> dict[str, Any] | None:
    normalized_id = str(item_id or "").strip()
    if not normalized_id:
        return None
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM review_queue_items WHERE id=? LIMIT 1", (normalized_id,))
        return _row_to_item(cur.fetchone())
    finally:
        conn.close()


def _update_download_job_path(
    cur: sqlite3.Cursor,
    *,
    job_id: str,
    status: str | None = None,
    file_path: str | None = None,
    resolved_destination: str | None = None,
    last_error: str | None = None,
    source: str | None = None,
    url: str | None = None,
    input_url: str | None = None,
    canonical_url: str | None = None,
    external_id: str | None = None,
) -> None:
    updates: list[str] = ["updated_at=?"]
    params: list[Any] = [utc_now_iso()]
    if status is not None:
        updates.append("status=?")
        params.append(status)
        if status == "completed":
            updates.append("completed=?")
            params.append(utc_now_iso())
            updates.append("failed=NULL")
            updates.append("last_error=NULL")
    if file_path is not None:
        updates.append("file_path=?")
        params.append(file_path)
    if resolved_destination is not None:
        updates.append("resolved_destination=?")
        params.append(resolved_destination)
    if last_error is not None:
        updates.append("last_error=?")
        params.append(last_error)
    if source is not None:
        updates.append("source=?")
        params.append(source)
    if url is not None:
        updates.append("url=?")
        params.append(url)
    if input_url is not None:
        updates.append("input_url=?")
        params.append(input_url)
    if canonical_url is not None:
        updates.append("canonical_url=?")
        params.append(canonical_url)
    if external_id is not None:
        updates.append("external_id=?")
        params.append(external_id)
    params.append(job_id)
    cur.execute(f"UPDATE download_jobs SET {', '.join(updates)} WHERE id=?", tuple(params))


def _move_review_file_to_library(item: dict[str, Any]) -> tuple[str, str]:
    from engine.job_queue import atomic_move, resolve_collision_path

    file_path = str(item.get("file_path") or "").strip()
    quarantine_root = str(item.get("quarantine_root") or "").strip()
    target_destination = str(item.get("target_destination") or "").strip()
    if not file_path or not quarantine_root or not target_destination:
        raise RuntimeError("review_item_missing_paths")
    if not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)
    rel_path = os.path.relpath(file_path, quarantine_root)
    if rel_path.startswith(".."):
        raise RuntimeError("review_item_path_outside_quarantine")
    final_path = resolve_collision_path(os.path.join(target_destination, rel_path))
    ensure_dir(os.path.dirname(final_path))
    atomic_move(file_path, final_path)
    return final_path, target_destination


def _record_history_for_accepted_review(db_path: str, item: dict[str, Any], final_path: str) -> None:
    from engine.job_queue import record_download_history

    meta = {
        "title": item.get("track") or item.get("filename"),
        "channel_id": None,
        "recording_mbid": item.get("recording_mbid"),
        "mb_recording_id": item.get("recording_mbid"),
    }
    job = type(
        "AcceptedReviewJob",
        (),
        {
            "id": str(item.get("parent_job_id") or item.get("job_id") or item.get("id")),
            "origin": "music_review",
            "origin_id": str(item.get("recording_mbid") or item.get("job_id") or item.get("id")),
            "url": item.get("candidate_url"),
            "input_url": item.get("candidate_url"),
            "external_id": None,
            "source": item.get("source") or "unknown",
            "canonical_url": item.get("candidate_url"),
        },
    )()
    record_download_history(db_path, job, final_path, meta=meta)


def _record_review_isrc(item: dict[str, Any], final_path: str) -> None:
    canonical = item.get("canonical_metadata")
    if not isinstance(canonical, dict):
        return
    isrc = str(canonical.get("isrc") or canonical.get("isrc_code") or "").strip()
    if not isrc:
        return
    try:
        record_downloaded_track(_REVIEW_SENTINEL_PLAYLIST_ID, isrc, final_path)
    except Exception:
        return


def _extract_video_id(url: str | None) -> str | None:
    normalized = str(url or "").strip()
    if not normalized:
        return None
    if "youtube.com" in normalized:
        match = re.search(r"v=([a-zA-Z0-9_-]{6,})", normalized)
        if match:
            return match.group(1)
    if "youtu.be" in normalized:
        parsed = urllib.parse.urlparse(normalized)
        if parsed.path:
            return parsed.path.lstrip("/").split("/")[0]
    return None


def _backfill_resolution_for_accepted_review(db_path: str, item: dict[str, Any], final_path: str) -> dict[str, Any]:
    from engine.resolution_api import upsert_local_acquired_mapping
    from engine.search_engine import resolve_search_db_path

    recording_mbid = str(item.get("recording_mbid") or "").strip()
    candidate_url = str(item.get("candidate_url") or "").strip()
    source = str(item.get("source") or "").strip().lower()
    if not recording_mbid or not candidate_url or not source:
        return {"status": "skipped", "reason": "missing_mapping_fields"}

    duration_seconds = None
    duration_ms = _safe_int(item.get("duration_ms"))
    if duration_ms is not None and duration_ms > 0:
        duration_seconds = max(1, duration_ms // 1000)
    bitrate_kbps = _safe_int(item.get("bitrate_kbps"))
    media_format = str(os.path.splitext(str(final_path or "").strip())[1] or "").lstrip(".").lower() or None
    source_id = str(item.get("candidate_id") or "").strip() or _extract_video_id(candidate_url)
    search_db_path = resolve_search_db_path(db_path, config=None)

    result = upsert_local_acquired_mapping(
        search_db_path,
        mbid=recording_mbid,
        source_url=candidate_url,
        source=source,
        node_id="review_accept",
        duration_seconds=duration_seconds,
        media_format=media_format,
        bitrate_kbps=bitrate_kbps,
        file_hash=None,
        resolution_method="review_accept",
        source_id=source_id or None,
        raw_payload={
            "accepted_from_review": True,
            "review_parent_job_id": str(item.get("parent_job_id") or "").strip() or None,
            "review_job_id": str(item.get("job_id") or "").strip() or None,
            "candidate_id": str(item.get("candidate_id") or "").strip() or None,
            "final_path": str(final_path or "").strip() or None,
        },
    )
    return result if isinstance(result, dict) else {"status": "updated"}


def accept_review_queue_items(db_path: str, item_ids: list[str]) -> dict[str, Any]:
    requested = [str(item_id or "").strip() for item_id in (item_ids or []) if str(item_id or "").strip()]
    if not requested:
        return {"accepted": 0, "errors": ["no_items_selected"], "items": []}
    now = utc_now_iso()
    accepted_items: list[dict[str, Any]] = []
    errors: list[str] = []
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for item_id in requested:
            cur.execute("SELECT * FROM review_queue_items WHERE id=? LIMIT 1", (item_id,))
            row = cur.fetchone()
            item = _row_to_item(row)
            if not item:
                errors.append(f"{item_id}:not_found")
                continue
            if item.get("status") != REVIEW_STATUS_PENDING:
                errors.append(f"{item_id}:not_pending")
                continue
            try:
                original_file_path = str(item.get("file_path") or "").strip() or None
                quarantine_root = str(item.get("quarantine_root") or "").strip() or None
                final_path, final_destination = _move_review_file_to_library(item)
                _cleanup_empty_review_dirs(original_file_path, quarantine_root)
                _record_history_for_accepted_review(db_path, item, final_path)
                _record_review_isrc(item, final_path)
                _backfill_resolution_for_accepted_review(db_path, item, final_path)
                review_job_id = str(item.get("job_id") or "").strip()
                parent_job_id = str(item.get("parent_job_id") or "").strip()
                candidate_url = str(item.get("candidate_url") or "").strip() or None
                if review_job_id:
                    _update_download_job_path(
                        cur,
                        job_id=review_job_id,
                        file_path=final_path,
                        resolved_destination=final_destination,
                    )
                if parent_job_id:
                    _update_download_job_path(
                        cur,
                        job_id=parent_job_id,
                        status="completed",
                        file_path=final_path,
                        resolved_destination=final_destination,
                        source=str(item.get("source") or "").strip() or None,
                        url=candidate_url,
                        input_url=candidate_url,
                        canonical_url=candidate_url,
                    )
                cur.execute(
                    """
                    UPDATE review_queue_items
                    SET status=?,
                        file_path=?,
                        updated_at=?,
                        resolved_at=?,
                        accepted_at=?,
                        resolution_note=?
                    WHERE id=?
                    """,
                    (
                        REVIEW_STATUS_ACCEPTED,
                        final_path,
                        now,
                        now,
                        now,
                        "accepted_by_operator",
                        item_id,
                    ),
                )
                accepted_items.append({"id": item_id, "file_path": final_path})
            except Exception as exc:
                errors.append(f"{item_id}:{exc}")
        conn.commit()
        return {"accepted": len(accepted_items), "errors": errors, "items": accepted_items}
    finally:
        conn.close()


def reject_review_queue_items(db_path: str, item_ids: list[str]) -> dict[str, Any]:
    requested = [str(item_id or "").strip() for item_id in (item_ids or []) if str(item_id or "").strip()]
    if not requested:
        return {"rejected": 0, "errors": ["no_items_selected"], "items": []}
    now = utc_now_iso()
    rejected_items: list[dict[str, Any]] = []
    errors: list[str] = []
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for item_id in requested:
            cur.execute("SELECT * FROM review_queue_items WHERE id=? LIMIT 1", (item_id,))
            row = cur.fetchone()
            item = _row_to_item(row)
            if not item:
                errors.append(f"{item_id}:not_found")
                continue
            if item.get("status") != REVIEW_STATUS_PENDING:
                errors.append(f"{item_id}:not_pending")
                continue
            file_path = str(item.get("file_path") or "").strip()
            try:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                _cleanup_empty_review_dirs(file_path, item.get("quarantine_root"))
            except Exception as exc:
                errors.append(f"{item_id}:delete_failed:{exc}")
                continue
            review_job_id = str(item.get("job_id") or "").strip()
            if review_job_id:
                _update_download_job_path(cur, job_id=review_job_id, file_path=None, last_error="review_rejected")
            cur.execute(
                """
                UPDATE review_queue_items
                SET status=?,
                    file_path=NULL,
                    updated_at=?,
                    resolved_at=?,
                    rejected_at=?,
                    resolution_note=?
                WHERE id=?
                """,
                (
                    REVIEW_STATUS_REJECTED,
                    now,
                    now,
                    now,
                    "rejected_by_operator",
                    item_id,
                ),
            )
            rejected_items.append({"id": item_id})
        conn.commit()
        return {"rejected": len(rejected_items), "errors": errors, "items": rejected_items}
    finally:
        conn.close()
