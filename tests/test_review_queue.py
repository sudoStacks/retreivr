from __future__ import annotations

import sqlite3
import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_review_modules():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    if "db" not in sys.modules:
        db_pkg = types.ModuleType("db")
        db_pkg.__path__ = [str(_ROOT / "db")]  # type: ignore[attr-defined]
        sys.modules["db"] = db_pkg
    if "library" not in sys.modules:
        library_pkg = types.ModuleType("library")
        library_pkg.__path__ = [str(_ROOT / "library")]  # type: ignore[attr-defined]
        sys.modules["library"] = library_pkg
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("db.migrations", _ROOT / "db" / "migrations.py")
    _load_module("db.downloaded_tracks", _ROOT / "db" / "downloaded_tracks.py")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    if "metadata.services" not in sys.modules:
        services_pkg = types.ModuleType("metadata.services")
        services_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["metadata.services"] = services_pkg
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_module = types.ModuleType("metadata.services.musicbrainz_service")
        mb_module.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_module
    if "engine.musicbrainz_binding" not in sys.modules:
        binding_module = types.ModuleType("engine.musicbrainz_binding")
        binding_module.resolve_best_mb_pair = lambda *args, **kwargs: None
        binding_module._normalize_title_for_mb_lookup = lambda value, **kwargs: str(value or "")
        sys.modules["engine.musicbrainz_binding"] = binding_module
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    job_queue = _load_module("engine_job_queue_review_tests", _ROOT / "engine" / "job_queue.py")
    review_queue = _load_module("library_review_queue_tests", _ROOT / "library" / "review_queue.py")
    return job_queue, review_queue


def _insert_job(
    db_path: str,
    *,
    job_id: str,
    origin: str,
    media_intent: str,
    url: str,
    canonical_id: str,
    destination: str,
) -> None:
    jq, _ = _load_review_modules()
    conn = sqlite3.connect(db_path)
    try:
        jq.ensure_download_jobs_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO download_jobs (
                id, origin, origin_id, media_type, media_intent, source, url,
                input_url, canonical_url, external_id, status, queued, claimed,
                downloading, postprocessing, completed, failed, canceled, attempts,
                max_attempts, created_at, updated_at, last_error, trace_id, output_template,
                resolved_destination, canonical_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                origin,
                "origin-1",
                "music",
                media_intent,
                "youtube",
                url,
                url,
                url,
                None,
                "failed" if media_intent == "music_track" else "completed",
                "2026-03-19T00:00:00+00:00",
                None,
                None,
                None,
                None,
                "2026-03-19T00:00:00+00:00" if media_intent == "music_track" else None,
                None,
                1,
                3,
                "2026-03-19T00:00:00+00:00",
                "2026-03-19T00:00:00+00:00",
                "failed_to_resolve" if media_intent == "music_track" else None,
                f"trace-{job_id}",
                "{}",
                destination,
                canonical_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_accept_review_queue_item_moves_file_and_promotes_parent_job(tmp_path: Path) -> None:
    _, review_queue = _load_review_modules()
    db_path = str(tmp_path / "db.sqlite")
    downloads_root = tmp_path / "downloads" / "Music"
    quarantine_root = tmp_path / "data" / "review_queue" / "files" / "review-item"
    quarantine_root.mkdir(parents=True)
    file_path = quarantine_root / "Artist" / "Album (2024)" / "01 - Song.m4a"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"audio-data")

    _insert_job(
        db_path,
        job_id="parent-job",
        origin="import",
        media_intent="music_track",
        url="musicbrainz://recording/rec-1",
        canonical_id="music_track:rec-1:rel-1:disc-1",
        destination=str(downloads_root),
    )
    _insert_job(
        db_path,
        job_id="review-job",
        origin="music_review",
        media_intent="music_track_review",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        canonical_id="review:rec-1:cand-1",
        destination=str(quarantine_root),
    )

    review_job = SimpleNamespace(
        id="review-job",
        canonical_id="review:rec-1:cand-1",
        media_type="music",
        media_intent="music_track_review",
        source="youtube",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        trace_id="trace-review-job",
        resolved_destination=str(quarantine_root),
        output_template={
            "artist": "Artist",
            "album": "Album",
            "track": "Song",
            "review_parent_job_id": "parent-job",
            "review_target_destination": str(downloads_root),
            "review_candidate_id": "cand-1",
            "review_candidate_url": "https://www.youtube.com/watch?v=abc123xyz00",
            "review_failure_reason": "all_filtered_by_gate",
            "review_top_failed_gate": "score_threshold",
            "review_candidate_details": {"final_score": 0.76, "duration_delta_ms": 1500},
            "canonical_metadata": {
                "artist": "Artist",
                "album": "Album",
                "track": "Song",
                "album_artist": "Artist",
                "recording_mbid": "rec-1",
                "mb_release_id": "rel-1",
            },
        },
    )
    review_queue.record_completed_review_item(db_path, review_job, str(file_path), meta={"title": "Song", "duration_sec": 201})

    recorded_backfill: list[tuple[str, dict, str]] = []
    original_backfill = review_queue._backfill_resolution_for_accepted_review
    review_queue._backfill_resolution_for_accepted_review = lambda path, item, final_path: recorded_backfill.append((path, dict(item), str(final_path))) or {"status": "updated"}

    try:
        result = review_queue.accept_review_queue_items(db_path, ["review:rec-1:cand-1"])
    finally:
        review_queue._backfill_resolution_for_accepted_review = original_backfill
    assert result["accepted"] == 1
    accepted_item = review_queue.get_review_queue_item(db_path, "review:rec-1:cand-1")
    assert accepted_item is not None
    assert accepted_item["status"] == review_queue.REVIEW_STATUS_ACCEPTED
    final_path = Path(accepted_item["file_path"])
    assert final_path.exists()
    assert str(final_path).startswith(str(downloads_root))
    assert not file_path.exists()
    assert not (quarantine_root / "Artist" / "Album (2024)").exists()
    assert not (quarantine_root / "Artist").exists()
    assert len(recorded_backfill) == 1
    assert recorded_backfill[0][0] == db_path
    assert recorded_backfill[0][1]["recording_mbid"] == "rec-1"
    assert recorded_backfill[0][2] == str(final_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
      cur = conn.cursor()
      cur.execute("SELECT status, file_path FROM download_jobs WHERE id='parent-job'")
      parent = cur.fetchone()
      assert parent["status"] == "completed"
      assert parent["file_path"] == str(final_path)
    finally:
      conn.close()


def test_reject_review_queue_item_deletes_quarantine_file(tmp_path: Path) -> None:
    _, review_queue = _load_review_modules()
    db_path = str(tmp_path / "db.sqlite")
    quarantine_root = tmp_path / "data" / "review_queue" / "files" / "review-item"
    quarantine_root.mkdir(parents=True)
    file_path = quarantine_root / "Artist" / "Album" / "01 - Song.m4a"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"audio-data")

    _insert_job(
        db_path,
        job_id="review-job",
        origin="music_review",
        media_intent="music_track_review",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        canonical_id="review:rec-2:cand-2",
        destination=str(quarantine_root),
    )

    review_job = SimpleNamespace(
        id="review-job",
        canonical_id="review:rec-2:cand-2",
        media_type="music",
        media_intent="music_track_review",
        source="youtube",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        trace_id="trace-review-job",
        resolved_destination=str(quarantine_root),
        output_template={
            "artist": "Artist",
            "album": "Album",
            "track": "Song",
            "review_target_destination": str(tmp_path / "downloads" / "Music"),
            "review_candidate_url": "https://www.youtube.com/watch?v=abc123xyz00",
            "canonical_metadata": {"artist": "Artist", "album": "Album", "track": "Song"},
        },
    )
    item = review_queue.record_completed_review_item(db_path, review_job, str(file_path), meta={"title": "Song"})
    assert item["status"] == review_queue.REVIEW_STATUS_PENDING

    result = review_queue.reject_review_queue_items(db_path, ["review:rec-2:cand-2"])
    assert result["rejected"] == 1
    assert not file_path.exists()
    assert not (quarantine_root / "Artist" / "Album").exists()
    assert not (quarantine_root / "Artist").exists()
    rejected_item = review_queue.get_review_queue_item(db_path, "review:rec-2:cand-2")
    assert rejected_item is not None
    assert rejected_item["status"] == review_queue.REVIEW_STATUS_REJECTED
    assert rejected_item["file_path"] is None
