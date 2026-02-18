from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from typing import Any

from metadata.importers.base import TrackIntent


@dataclass(frozen=True)
class ImportResult:
    total_tracks: int
    resolved_count: int
    unresolved_count: int
    enqueued_count: int
    failed_count: int
    resolved_track_paths: tuple[str, ...] = ()


def _build_query(intent: TrackIntent) -> str:
    if intent.artist and intent.title:
        return f"{intent.artist} - {intent.title}".strip()
    return str(intent.raw_line or "").strip()


def _get_search_service(config: Any):
    if isinstance(config, dict):
        service = config.get("search_service")
        if service is not None:
            return service

        search_db_path = config.get("search_db_path")
        queue_db_path = config.get("queue_db_path")
        if search_db_path and queue_db_path:
            from engine.search_engine import SearchResolutionService
            return SearchResolutionService(
                search_db_path=search_db_path,
                queue_db_path=queue_db_path,
                adapters=config.get("adapters"),
                config=config.get("app_config") or config,
                paths=config.get("paths"),
                canonical_resolver=config.get("canonical_resolver"),
            )

    raise ValueError("search_service or search_db_path/queue_db_path required")


_INVALID_FS_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
_MULTISPACE_RE = re.compile(r"\s+")


def _sanitize_component(value: Any, fallback: str) -> str:
    text = _INVALID_FS_CHARS_RE.sub("", str(value or ""))
    text = _MULTISPACE_RE.sub(" ", text).strip().rstrip(" .")
    return text or fallback


def _normalize_track_number(value: Any) -> int:
    if isinstance(value, int):
        return value
    text = str(value or "").strip()
    return int(text) if text.isdigit() else 0


def _build_predicted_music_relative_path(canonical: dict[str, Any], ext: str) -> str:
    album_artist = _sanitize_component(canonical.get("album_artist") or canonical.get("artist"), "Unknown Artist")
    album = _sanitize_component(canonical.get("album"), "Unknown Album")
    release_date = str(canonical.get("release_date") or canonical.get("date") or "").strip()
    year = release_date[:4] if len(release_date) >= 4 and release_date[:4].isdigit() else ""
    album_folder = f"{album} ({year})" if year else album
    disc_number = _normalize_track_number(canonical.get("disc_number") or canonical.get("disc"))
    disc_folder = f"Disc {disc_number or 1}"
    track_number = _normalize_track_number(canonical.get("track_number") or canonical.get("track_num"))
    title = _sanitize_component(canonical.get("track") or canonical.get("title"), "media")
    return f"Music/{album_artist}/{album_folder}/{disc_folder}/{track_number:02d} - {title}.{ext}"


def _predict_job_final_path(service: Any, request_id: str) -> str | None:
    queue_store = getattr(service, "queue_store", None)
    db_path = getattr(queue_store, "db_path", None)
    if not db_path:
        return None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT file_path, output_template, resolved_destination
            FROM download_jobs
            WHERE origin_id=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (request_id,),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return None
    file_path = str(row["file_path"] or "").strip()
    if file_path:
        return file_path

    output_template_raw = row["output_template"]
    if not output_template_raw:
        return None
    try:
        output_template = json.loads(output_template_raw)
    except Exception:
        return None
    if not isinstance(output_template, dict):
        return None
    canonical = output_template.get("canonical_metadata")
    if not isinstance(canonical, dict):
        return None

    final_format = str(output_template.get("final_format") or "mp3").strip().lstrip(".") or "mp3"
    relative_path = _build_predicted_music_relative_path(canonical, final_format)
    destination = (
        str(row["resolved_destination"] or "").strip()
        or str(output_template.get("output_dir") or "").strip()
        or "/downloads"
    )
    return os.path.join(destination, relative_path)


def process_imported_tracks(track_intents: list[TrackIntent], config) -> ImportResult:
    service = _get_search_service(config)

    total_tracks = len(track_intents)
    resolved_count = 0
    unresolved_count = 0
    enqueued_count = 0
    failed_count = 0
    resolved_track_paths: list[str] = []

    for intent in track_intents:
        query = _build_query(intent)
        if not query:
            failed_count += 1
            continue

        artist = (intent.artist or "").strip() or query
        track = (intent.title or "").strip() or query
        album = (intent.album or "").strip() or None

        payload = {
            "created_by": "import_pipeline",
            "intent": "track",
            "media_type": "music",
            "artist": artist,
            "track": track,
            "album": album,
            "auto_enqueue": True,
        }

        if isinstance(config, dict):
            if config.get("source_priority"):
                payload["source_priority"] = config.get("source_priority")
            if config.get("destination_dir"):
                payload["destination_dir"] = config.get("destination_dir")
            if config.get("min_match_score") is not None:
                payload["min_match_score"] = config.get("min_match_score")
            if config.get("max_candidates_per_source") is not None:
                payload["max_candidates_per_source"] = config.get("max_candidates_per_source")

        try:
            request_id = service.create_search_request(payload)
            if not request_id:
                failed_count += 1
                continue
            service.run_search_resolution_once(request_id=request_id)
            result = service.get_search_request(request_id)
        except Exception:
            failed_count += 1
            continue

        items = (result or {}).get("items") or []
        item = items[0] if items else {}
        status = str(item.get("status") or "").strip().lower()

        if status == "enqueued":
            resolved_count += 1
            enqueued_count += 1
            predicted_path = _predict_job_final_path(service, request_id)
            if predicted_path:
                resolved_track_paths.append(predicted_path)
        elif status in {"selected", "candidate_found", "skipped"}:
            resolved_count += 1
        elif status == "failed":
            unresolved_count += 1
        else:
            unresolved_count += 1

    return ImportResult(
        total_tracks=total_tracks,
        resolved_count=resolved_count,
        unresolved_count=unresolved_count,
        enqueued_count=enqueued_count,
        failed_count=failed_count,
        resolved_track_paths=tuple(resolved_track_paths),
    )
