from __future__ import annotations

import importlib.util
import logging
import sqlite3
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from metadata.importers.base import TrackIntent
from engine.job_queue import DownloadJobStore, build_download_job_payload

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
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportResult:
    total_tracks: int
    resolved_count: int
    unresolved_count: int
    enqueued_count: int
    failed_count: int
    resolved_mbids: list[str] = field(default_factory=list)
    import_batch_id: str = ""


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


def _record_music_failure(
    *,
    db_path: str | None,
    origin_batch_id: str,
    artist: str | None,
    track: str | None,
    reasons: list[str] | None = None,
    recording_mbid_attempted: str | None = None,
    last_query: str | None = None,
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
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("music_failure_record_persist_failed")


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
    duration_ms: int | None = None,
    country_preference: str = "US",
    threshold: float = _DEFAULT_CONFIDENCE_THRESHOLD,
) -> dict[str, Any] | None:
    return resolve_best_mb_pair(
        mb_service,
        artist=artist,
        track=track,
        album=album,
        duration_ms=duration_ms,
        country_preference=country_preference,
        min_recording_score=float(threshold or 0.0),
        threshold=float(threshold or _DEFAULT_CONFIDENCE_THRESHOLD),
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
    duration_ms: int | None,
) -> bool:
    canonical_id = f"music_track:{recording_mbid}"
    canonical_metadata = {
        "artist": artist,
        "track": title,
        "album": album,
        "release_date": release_date,
        "track_number": track_number,
        "disc_number": disc_number,
        "duration_ms": duration_ms,
        "recording_mbid": recording_mbid,
        "mb_recording_id": recording_mbid,
        "mb_release_id": release_mbid,
        "mb_release_group_id": release_group_mbid,
    }
    placeholder_url = f"musicbrainz://recording/{recording_mbid}"
    enqueue_payload = job_payload_builder(
        config=runtime_config,
        origin="import",
        origin_id=import_batch_id,
        media_type="music",
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
            "source_index": source_index,
            "track_number": track_number,
            "disc_number": disc_number,
            "release_date": release_date,
            "duration_ms": duration_ms,
            "recording_mbid": recording_mbid,
            "mb_recording_id": recording_mbid,
            "mb_release_id": release_mbid,
            "mb_release_group_id": release_group_mbid,
        },
        canonical_id=canonical_id,
    )
    _job_id, created, _reason = queue_store.enqueue_job(**enqueue_payload)
    return bool(created)


def process_imported_tracks(track_intents: list[TrackIntent], config) -> ImportResult:
    mb_service = _get_musicbrainz_service(config)
    queue_store = _get_queue_store(config)
    job_payload_builder = _get_job_payload_builder(config)
    import_batch_id = uuid4().hex
    failure_db_path = _resolve_failure_db_path(config, queue_store)
    confidence_threshold = _DEFAULT_CONFIDENCE_THRESHOLD
    runtime_config = config.get("app_config") if isinstance(config, dict) and isinstance(config.get("app_config"), dict) else (config if isinstance(config, dict) else {})
    base_dir = "/downloads"
    destination = None
    final_format_override = None
    if isinstance(config, dict):
        try:
            if config.get("music_mb_binding_threshold") is not None:
                confidence_threshold = float(config.get("music_mb_binding_threshold"))
            elif config.get("min_confidence") is not None:
                confidence_threshold = float(config.get("min_confidence"))
        except (TypeError, ValueError):
            confidence_threshold = _DEFAULT_CONFIDENCE_THRESHOLD
        base_dir = config.get("base_dir")
        destination = config.get("destination_dir")
        final_format_override = config.get("final_format")

    total_tracks = len(track_intents)
    resolved_count = 0
    unresolved_count = 0
    enqueued_count = 0
    failed_count = 0
    resolved_mbids: list[str] = []

    for idx, intent in enumerate(track_intents):
        query = _build_query(intent)
        if not query:
            failed_count += 1
            continue
        artist = str(intent.artist or "").strip() or None
        title = str(intent.title or "").strip() or query
        album = str(intent.album or "").strip() or None
        duration_ms = getattr(intent, "duration_ms", None)
        try:
            duration_ms = int(duration_ms) if duration_ms is not None else None
        except (TypeError, ValueError):
            duration_ms = None

        try:
            selected_pair = _resolve_bound_mb_pair(
                mb_service,
                artist=artist,
                track=title,
                album=album,
                duration_ms=duration_ms,
                country_preference="US",
                threshold=confidence_threshold,
            )
            if not selected_pair:
                unresolved_count += 1
                reasons = []
                try:
                    last = getattr(resolve_best_mb_pair, "last_failure_reasons", [])
                    if isinstance(last, list):
                        reasons = [str(item) for item in last if str(item or "").strip()]
                except Exception:
                    reasons = []
                _record_music_failure(
                    db_path=failure_db_path,
                    origin_batch_id=import_batch_id,
                    artist=artist,
                    track=title,
                    reasons=reasons or ["mb_pair_not_found"],
                    recording_mbid_attempted=None,
                    last_query=query,
                )
                continue
            recording_mbid = str(selected_pair.get("recording_mbid") or "").strip()
            if not recording_mbid:
                unresolved_count += 1
                _record_music_failure(
                    db_path=failure_db_path,
                    origin_batch_id=import_batch_id,
                    artist=artist,
                    track=title,
                    reasons=["missing_recording_mbid"],
                    recording_mbid_attempted=None,
                    last_query=query,
                )
                continue
            release_mbid = str(selected_pair.get("mb_release_id") or "").strip() or None
            release_group_mbid = str(selected_pair.get("mb_release_group_id") or "").strip() or None
            canonical_artist = artist or ""
            canonical_title = title
            canonical_album = str(selected_pair.get("album") or album or "").strip() or None
            release_date_raw = str(selected_pair.get("release_date") or "").strip() or None
            release_date = _extract_release_year(release_date_raw) or release_date_raw
            track_number = _safe_int(selected_pair.get("track_number"))
            disc_number = _safe_int(selected_pair.get("disc_number")) or 1
            resolved_duration_ms = _safe_int(selected_pair.get("duration_ms"))
            created = _enqueue_music_track_job(
                queue_store,
                job_payload_builder,
                runtime_config=runtime_config,
                base_dir=base_dir,
                destination=destination,
                final_format_override=final_format_override,
                import_batch_id=import_batch_id,
                source_index=idx,
                recording_mbid=recording_mbid,
                release_mbid=release_mbid,
                release_group_mbid=release_group_mbid,
                artist=canonical_artist,
                title=canonical_title,
                album=canonical_album,
                release_date=release_date,
                track_number=track_number,
                disc_number=disc_number,
                duration_ms=resolved_duration_ms,
            )
            resolved_count += 1
            resolved_mbids.append(recording_mbid)
            if created:
                enqueued_count += 1
        except Exception as exc:
            failed_count += 1
            _record_music_failure(
                db_path=failure_db_path,
                origin_batch_id=import_batch_id,
                artist=artist,
                track=title,
                reasons=["import_exception", str(exc)],
                recording_mbid_attempted=None,
                last_query=query,
            )

    unresolved_count = max(unresolved_count, total_tracks - resolved_count - failed_count)
    return ImportResult(
        total_tracks=total_tracks,
        resolved_count=resolved_count,
        unresolved_count=unresolved_count,
        enqueued_count=enqueued_count,
        failed_count=failed_count,
        resolved_mbids=resolved_mbids,
        import_batch_id=import_batch_id,
    )
