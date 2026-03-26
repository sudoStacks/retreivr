from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

from engine.community_publish_worker import append_publish_proposal_to_outbox, utc_now
from engine.job_queue import _fetch_release_enrichment
from engine.musicbrainz_binding import resolve_best_mb_pair
from library.reconcile import (
    _SUPPORTED_AUDIO_EXTENSIONS,
    _coerce_tag_value,
    _lookup_tag,
    _normalize_index,
    _should_skip_reconcile_path,
)
from metadata.services.musicbrainz_service import get_musicbrainz_service
from metadata.tagger import apply_tags
from media.ffprobe import get_media_duration


logger = logging.getLogger(__name__)


def _first_tag(tags: Any, *keys: str) -> str | None:
    for key in keys:
        value = _lookup_tag(tags, key)
        if value:
            return value
    return None


def _read_identity(path: Path) -> dict[str, Any] | None:
    try:
        from mutagen import File as MutagenFile
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("mutagen is required for community publish backfill") from exc

    audio = MutagenFile(str(path), easy=False)
    if audio is None:
        return None
    tags = getattr(audio, "tags", None)
    if not tags:
        return None

    return {
        "path": str(path),
        "title": _first_tag(tags, "TIT2", "\xa9nam", "title"),
        "artist": _first_tag(tags, "TPE1", "\xa9ART", "artist"),
        "album": _first_tag(tags, "TALB", "\xa9alb", "album"),
        "album_artist": _first_tag(tags, "TPE2", "aART", "albumartist", "album_artist"),
        "track_number": _normalize_index(_first_tag(tags, "TRCK", "trkn", "tracknumber")),
        "disc_number": _normalize_index(_first_tag(tags, "TPOS", "disk", "discnumber")),
        "release_date": _first_tag(tags, "TDRC", "\xa9day", "date", "year"),
        "genre": _first_tag(tags, "TCON", "\xa9gen", "genre"),
        "recording_mbid": _first_tag(
            tags,
            "TXXX:MBID",
            "----:com.apple.iTunes:MBID",
            "musicbrainz_trackid",
            "musicbrainz_recordingid",
            "mbid",
        ),
        "mb_release_id": _first_tag(
            tags,
            "TXXX:MUSICBRAINZ_RELEASEID",
            "----:com.apple.iTunes:MUSICBRAINZ_RELEASEID",
            "musicbrainz_releaseid",
        ),
        "mb_release_group_id": _first_tag(
            tags,
            "TXXX:MUSICBRAINZ_RELEASEGROUPID",
            "----:com.apple.iTunes:MUSICBRAINZ_RELEASEGROUPID",
            "musicbrainz_releasegroupid",
        ),
        "retreivr_source": _first_tag(
            tags,
            "TXXX:RETREIVR_SOURCE",
            "----:com.apple.iTunes:RETREIVR_SOURCE",
            "retreivr_source",
        ),
        "retreivr_source_id": _first_tag(
            tags,
            "TXXX:RETREIVR_SOURCE_ID",
            "----:com.apple.iTunes:RETREIVR_SOURCE_ID",
            "retreivr_source_id",
        ),
        "isrc": _first_tag(tags, "TSRC", "----:com.apple.iTunes:ISRC", "isrc"),
    }


def _probe_duration_ms(path: Path) -> int | None:
    try:
        duration_sec = get_media_duration(str(path))
    except Exception:
        return None
    if duration_sec is None:
        return None
    try:
        duration_ms = int(float(duration_sec) * 1000)
    except Exception:
        return None
    return duration_ms if duration_ms > 0 else None


def _resolve_history_hint(conn: sqlite3.Connection, identity: dict[str, Any]) -> dict[str, Any]:
    source_id = str(identity.get("retreivr_source_id") or "").strip()
    recording_mbid = str(identity.get("recording_mbid") or "").strip()
    file_path = Path(str(identity.get("path") or ""))
    cur = conn.cursor()
    try:
        if source_id:
            cur.execute(
                """
                SELECT source, external_id, input_url, canonical_url
                FROM download_history
                WHERE external_id=? OR video_id=?
                ORDER BY COALESCE(completed_at, created_at, id) DESC
                LIMIT 1
                """,
                (source_id, source_id),
            )
            row = cur.fetchone()
            if row is not None:
                return {
                    "source": str(row[0] or "").strip() or None,
                    "external_id": str(row[1] or "").strip() or None,
                    "input_url": str(row[2] or "").strip() or None,
                    "canonical_url": str(row[3] or "").strip() or None,
                }
        if file_path:
            cur.execute(
                """
                SELECT source, external_id, input_url, canonical_url
                FROM download_history
                WHERE destination=? AND filename=?
                ORDER BY COALESCE(completed_at, created_at, id) DESC
                LIMIT 1
                """,
                (str(file_path.parent), file_path.name),
            )
            row = cur.fetchone()
            if row is not None:
                return {
                    "source": str(row[0] or "").strip() or None,
                    "external_id": str(row[1] or "").strip() or None,
                    "input_url": str(row[2] or "").strip() or None,
                    "canonical_url": str(row[3] or "").strip() or None,
                }
        if recording_mbid:
            cur.execute(
                """
                SELECT source, external_id, input_url, canonical_url
                FROM download_jobs
                WHERE file_path=? OR origin_id=? OR canonical_id LIKE ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (str(file_path), recording_mbid, f"%{recording_mbid}%"),
            )
            row = cur.fetchone()
            if row is not None:
                return {
                    "source": str(row[0] or "").strip() or None,
                    "external_id": str(row[1] or "").strip() or None,
                    "input_url": str(row[2] or "").strip() or None,
                    "canonical_url": str(row[3] or "").strip() or None,
                }
    except sqlite3.OperationalError:
        return {}
    return {}


def _repair_identity(identity: dict[str, Any], *, threshold: float) -> tuple[dict[str, Any], bool]:
    repaired = False
    recording_mbid = str(identity.get("recording_mbid") or "").strip()
    release_id = str(identity.get("mb_release_id") or "").strip() or None

    if recording_mbid:
        enriched = _fetch_release_enrichment(recording_mbid, release_id)
        for key, value in enriched.items():
            if value not in (None, "") and not identity.get(key):
                identity[key] = value
                repaired = True
    elif identity.get("artist") and identity.get("title"):
        duration_ms = _probe_duration_ms(Path(str(identity.get("path") or "")))
        resolved = resolve_best_mb_pair(
            get_musicbrainz_service(),
            artist=str(identity.get("artist") or "").strip() or None,
            track=str(identity.get("title") or "").strip() or None,
            album=str(identity.get("album") or "").strip() or None,
            duration_ms=duration_ms,
            threshold=threshold,
            resolution_profile="library_import",
        )
        if not resolved:
            raise RuntimeError("mb_pair_not_resolved")
        identity["recording_mbid"] = str(resolved.get("recording_mbid") or "").strip() or None
        identity["mb_release_id"] = str(resolved.get("mb_release_id") or "").strip() or None
        identity["mb_release_group_id"] = str(resolved.get("mb_release_group_id") or "").strip() or None
        identity["album"] = str(identity.get("album") or resolved.get("album") or "").strip() or None
        identity["release_date"] = str(identity.get("release_date") or resolved.get("release_date") or "").strip() or None
        identity["track_number"] = identity.get("track_number") or resolved.get("track_number")
        identity["disc_number"] = identity.get("disc_number") or resolved.get("disc_number")
        repaired = True

    return identity, repaired


def _classify_backfill_error(exc: Exception) -> str:
    text = str(exc or "").strip().lower()
    if text in {"no_valid_release_for_recording", "mb_pair_not_resolved"}:
        return "skip_unresolved_release_enrichment"
    return "backfill_failed"


def _retag_identity_file(path: Path, identity: dict[str, Any]) -> None:
    tags = {
        "artist": identity.get("artist"),
        "album": identity.get("album"),
        "title": identity.get("title"),
        "album_artist": identity.get("album_artist") or identity.get("artist"),
        "track_number": identity.get("track_number"),
        "disc_number": identity.get("disc_number"),
        "date": identity.get("release_date"),
        "genre": identity.get("genre"),
        "recording_id": identity.get("recording_mbid"),
        "mb_release_id": identity.get("mb_release_id"),
        "mb_release_group_id": identity.get("mb_release_group_id"),
        "retreivr_source": identity.get("retreivr_source"),
        "retreivr_source_id": identity.get("retreivr_source_id"),
        "isrc": identity.get("isrc"),
    }
    apply_tags(str(path), tags, None, allow_overwrite=True, dry_run=False)


def _build_backfill_proposal(identity: dict[str, Any], history_hint: dict[str, Any]) -> dict[str, Any]:
    video_id = str(identity.get("retreivr_source_id") or history_hint.get("external_id") or "").strip()
    candidate_url = str(history_hint.get("canonical_url") or history_hint.get("input_url") or "").strip()
    source = str(identity.get("retreivr_source") or history_hint.get("source") or "youtube").strip() or "youtube"
    if not candidate_url and video_id and source in {"youtube", "youtube_music"}:
        candidate_url = f"https://www.youtube.com/watch?v={video_id}"
    if source == "youtube_music":
        source = "youtube"
    final_path = Path(str(identity.get("path") or ""))
    return {
        "schema_version": 1,
        "proposal_type": "community_cache_publish_proposal",
        "proposal_id": f"backfill-{uuid4().hex}",
        "emitted_at": utc_now(),
        "recording_mbid": str(identity.get("recording_mbid") or "").strip().lower(),
        "release_mbid": str(identity.get("mb_release_id") or "").strip() or None,
        "release_group_mbid": str(identity.get("mb_release_group_id") or "").strip() or None,
        "video_id": video_id,
        "source": source,
        "candidate_url": candidate_url,
        "candidate_id": video_id or None,
        "duration_ms": _probe_duration_ms(final_path),
        "selected_score": 1.0,
        "duration_delta_ms": 0,
        "final_path": str(final_path).strip() or None,
        "retreivr_version": "backfill",
        "verified_by": "retreivr_backfill",
    }


def run_publish_backfill(
    *,
    db_path: str,
    config: dict[str, Any] | None,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    music_cfg = cfg.get("music") if isinstance(cfg.get("music"), dict) else {}
    library_path = Path(str(music_cfg.get("library_path") or "").strip())
    if not str(library_path).strip():
        raise RuntimeError("music.library_path not configured")
    if not library_path.exists() or not library_path.is_dir():
        raise RuntimeError("music.library_path not found")

    threshold_raw = cfg.get("music_mb_binding_threshold", 0.78)
    try:
        threshold = float(threshold_raw)
    except Exception:
        threshold = 0.78
    if threshold > 1.0:
        threshold = threshold / 100.0

    summary = {
        "status": "ok",
        "library_path": str(library_path),
        "files_seen": 0,
        "audio_files_seen": 0,
        "eligible": 0,
        "repaired_tags": 0,
        "proposals_written": 0,
        "skipped_missing_identity": 0,
        "skipped_missing_source_id": 0,
        "skipped_not_youtube": 0,
        "errors": 0,
        "dry_run": bool(dry_run),
        "recent": [],
    }

    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        for path in sorted(library_path.rglob("*")):
            if limit is not None and summary["audio_files_seen"] >= int(limit):
                break
            if not path.is_file() or _should_skip_reconcile_path(path):
                continue
            summary["files_seen"] += 1
            if path.suffix.lower() not in _SUPPORTED_AUDIO_EXTENSIONS:
                continue
            summary["audio_files_seen"] += 1
            try:
                identity = _read_identity(path)
                if identity is None:
                    summary["skipped_missing_identity"] += 1
                    continue
                history_hint = _resolve_history_hint(conn, identity)
                if history_hint.get("source") and str(history_hint.get("source")).strip().lower() not in {"youtube", "youtube_music"}:
                    summary["skipped_not_youtube"] += 1
                    continue
                identity.setdefault("retreivr_source", history_hint.get("source") or "youtube")
                if not identity.get("retreivr_source_id"):
                    identity["retreivr_source_id"] = history_hint.get("external_id")
                identity, repaired = _repair_identity(identity, threshold=threshold)
                if repaired:
                    _retag_identity_file(path, identity)
                    summary["repaired_tags"] += 1
                proposal = _build_backfill_proposal(identity, history_hint)
                if not proposal.get("recording_mbid"):
                    summary["skipped_missing_identity"] += 1
                    continue
                if not proposal.get("video_id") or not proposal.get("candidate_url"):
                    summary["skipped_missing_source_id"] += 1
                    continue
                summary["eligible"] += 1
                if not dry_run:
                    result = append_publish_proposal_to_outbox(config=cfg, db_path=db_path, proposal=proposal)
                    if str(result.get("status") or "") != "written":
                        raise RuntimeError(str(result.get("reason") or "outbox_write_failed"))
                    summary["proposals_written"] += 1
                if len(summary["recent"]) < 20:
                    summary["recent"].append(
                        {
                            "path": str(path),
                            "recording_mbid": proposal.get("recording_mbid"),
                            "video_id": proposal.get("video_id"),
                            "repaired": repaired,
                        }
                    )
            except Exception as exc:
                classification = _classify_backfill_error(exc)
                if classification == "skip_unresolved_release_enrichment":
                    logger.warning("community_publish_backfill_skipped path=%s reason=%s", path, exc)
                else:
                    logger.exception("community_publish_backfill_failed path=%s", path)
                    summary["errors"] += 1
                if len(summary["recent"]) < 20:
                    summary["recent"].append(
                        {
                            "path": str(path),
                            "error": str(exc) or classification,
                            "status": classification,
                        }
                    )
    finally:
        conn.close()
    if dry_run:
        summary["status"] = "dry_run"
    return summary
