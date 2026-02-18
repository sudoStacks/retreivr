from __future__ import annotations

import importlib.util
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from metadata.importers.base import TrackIntent

_DEFAULT_CONFIDENCE_THRESHOLD = 0.90


@dataclass(frozen=True)
class ImportResult:
    total_tracks: int
    resolved_count: int
    unresolved_count: int
    enqueued_count: int
    failed_count: int
    resolved_mbids: list[str] = field(default_factory=list)
    import_batch_id: str = ""


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


def _build_queue_store_from_path(queue_db_path: str):
    module_path = Path(__file__).resolve().parent / "job_queue.py"
    spec = importlib.util.spec_from_file_location("engine_job_queue_for_import_pipeline", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module.DownloadJobStore(queue_db_path)


def _get_queue_store(config: Any):
    if isinstance(config, dict):
        queue_store = config.get("queue_store")
        if queue_store is not None:
            return queue_store
        queue_db_path = config.get("queue_db_path")
        if queue_db_path:
            return _build_queue_store_from_path(str(queue_db_path))
    raise ValueError("queue_store (or queue_db_path) required")


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
    *,
    import_batch_id: str,
    source_index: int,
    recording_mbid: str,
    release_mbid: str | None,
    artist: str,
    title: str,
    album: str | None,
    track_number: int | None,
) -> bool:
    canonical_id = f"music_track:{recording_mbid}"
    output_template = {
        "kind": "music_track",
        "recording_mbid": recording_mbid,
        "mb_recording_id": recording_mbid,
        "mb_release_id": release_mbid,
        "source": "import",
        "import_batch": import_batch_id,
        "import_batch_id": import_batch_id,
        "source_index": source_index,
        "artist": artist,
        "track": title,
        "album": album,
        "track_number": track_number,
        "canonical_metadata": {
            "artist": artist,
            "track": title,
            "album": album,
            "track_number": track_number,
            "recording_mbid": recording_mbid,
            "mb_recording_id": recording_mbid,
            "mb_release_id": release_mbid,
        },
    }
    placeholder_url = f"musicbrainz://recording/{recording_mbid}"
    _job_id, created, _reason = queue_store.enqueue_job(
        origin="import",
        origin_id=import_batch_id,
        media_type="music",
        media_intent="music_track",
        source="music_import",
        url=placeholder_url,
        input_url=placeholder_url,
        output_template=output_template,
        canonical_id=canonical_id,
    )
    return bool(created)


def process_imported_tracks(track_intents: list[TrackIntent], config) -> ImportResult:
    mb_service = _get_musicbrainz_service(config)
    queue_store = _get_queue_store(config)
    import_batch_id = uuid4().hex
    confidence_threshold = _DEFAULT_CONFIDENCE_THRESHOLD
    if isinstance(config, dict):
        try:
            if config.get("min_confidence") is not None:
                confidence_threshold = float(config.get("min_confidence"))
        except (TypeError, ValueError):
            confidence_threshold = _DEFAULT_CONFIDENCE_THRESHOLD

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

        try:
            recordings_payload = None
            isrc = str(getattr(intent, "isrc", "") or "").strip()
            if isrc and hasattr(mb_service, "get_recording_by_isrc"):
                recordings_payload = mb_service.get_recording_by_isrc(isrc)
            if recordings_payload is None:
                recordings_payload = mb_service.search_recordings(artist, title, album=album, limit=5)
            recordings = _extract_recordings(recordings_payload)
            selected = _select_recording_candidate(recordings, threshold=confidence_threshold)
            if not selected:
                unresolved_count += 1
                continue
            recording_mbid = str(selected.get("id") or "").strip()
            if not recording_mbid:
                unresolved_count += 1
                continue
            release_mbid = _extract_release_mbid(selected)
            canonical_artist = _canonical_artist(selected, fallback_artist=artist)
            canonical_title = str(selected.get("title") or title).strip() or title
            canonical_album = album
            track_number = None
            try:
                track_number = int(selected.get("position"))
            except (TypeError, ValueError):
                track_number = None
            created = _enqueue_music_track_job(
                queue_store,
                import_batch_id=import_batch_id,
                source_index=idx,
                recording_mbid=recording_mbid,
                release_mbid=release_mbid,
                artist=canonical_artist,
                title=canonical_title,
                album=canonical_album,
                track_number=track_number,
            )
            resolved_count += 1
            resolved_mbids.append(recording_mbid)
            if created:
                enqueued_count += 1
        except Exception:
            failed_count += 1

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
