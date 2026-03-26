from __future__ import annotations

import json
import sqlite3
import sys
import types
from pathlib import Path

# Optional dependency shims needed by engine package import path in test env.
if "google" not in sys.modules:
    sys.modules["google"] = types.ModuleType("google")
if "google.auth" not in sys.modules:
    sys.modules["google.auth"] = types.ModuleType("google.auth")
if "google.auth.exceptions" not in sys.modules:
    google_auth_exc_mod = types.ModuleType("google.auth.exceptions")
    google_auth_exc_mod.RefreshError = Exception
    sys.modules["google.auth.exceptions"] = google_auth_exc_mod
if "google.auth.transport" not in sys.modules:
    sys.modules["google.auth.transport"] = types.ModuleType("google.auth.transport")
if "google.auth.transport.requests" not in sys.modules:
    google_auth_transport_requests = types.ModuleType("google.auth.transport.requests")
    google_auth_transport_requests.Request = object
    sys.modules["google.auth.transport.requests"] = google_auth_transport_requests
if "google.oauth2" not in sys.modules:
    sys.modules["google.oauth2"] = types.ModuleType("google.oauth2")
if "google.oauth2.credentials" not in sys.modules:
    google_oauth2_credentials = types.ModuleType("google.oauth2.credentials")
    google_oauth2_credentials.Credentials = object
    sys.modules["google.oauth2.credentials"] = google_oauth2_credentials
if "googleapiclient" not in sys.modules:
    sys.modules["googleapiclient"] = types.ModuleType("googleapiclient")
if "googleapiclient.discovery" not in sys.modules:
    googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
    googleapiclient_discovery.build = lambda *args, **kwargs: None
    sys.modules["googleapiclient.discovery"] = googleapiclient_discovery
if "googleapiclient.errors" not in sys.modules:
    googleapiclient_errors = types.ModuleType("googleapiclient.errors")
    googleapiclient_errors.HttpError = Exception
    sys.modules["googleapiclient.errors"] = googleapiclient_errors
if "rapidfuzz" not in sys.modules:
    rapidfuzz_mod = types.ModuleType("rapidfuzz")
    rapidfuzz_mod.fuzz = types.SimpleNamespace(ratio=lambda *_args, **_kwargs: 0)
    sys.modules["rapidfuzz"] = rapidfuzz_mod
if "metadata.queue" not in sys.modules:
    metadata_queue_mod = types.ModuleType("metadata.queue")
    metadata_queue_mod.enqueue_metadata = lambda *_args, **_kwargs: None
    sys.modules["metadata.queue"] = metadata_queue_mod
if "musicbrainzngs" not in sys.modules:
    sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")

from engine.job_queue import (
    DownloadJob,
    DownloadWorkerEngine,
    JOB_STATUS_COMPLETED,
    _validate_community_publish_proposal,
)
from engine.paths import EnginePaths


def _build_engine(tmp_path: Path, *, config: dict) -> DownloadWorkerEngine:
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS download_jobs (
                id TEXT PRIMARY KEY,
                origin TEXT NOT NULL,
                origin_id TEXT NOT NULL,
                media_type TEXT NOT NULL,
                media_intent TEXT NOT NULL,
                source TEXT NOT NULL,
                url TEXT NOT NULL,
                input_url TEXT,
                canonical_url TEXT,
                external_id TEXT,
                status TEXT NOT NULL,
                queued TEXT,
                claimed TEXT,
                downloading TEXT,
                postprocessing TEXT,
                completed TEXT,
                failed TEXT,
                canceled TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT,
                trace_id TEXT NOT NULL UNIQUE,
                output_template TEXT,
                resolved_destination TEXT,
                canonical_id TEXT,
                file_path TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    paths = EnginePaths(
        log_dir=str(tmp_path / "logs"),
        db_path=str(db_path),
        temp_downloads_dir=str(tmp_path / "tmp"),
        single_downloads_dir=str(tmp_path / "downloads"),
        lock_file=str(tmp_path / "retreivr.lock"),
        ytdlp_temp_dir=str(tmp_path / "yt"),
        thumbs_dir=str(tmp_path / "yt" / "thumbs"),
    )
    return DownloadWorkerEngine(
        db_path=str(db_path),
        config=dict(config or {}),
        paths=paths,
        adapters={},
        search_service=None,
    )


def _build_music_job(job_id: str, *, output_template: dict) -> DownloadJob:
    return DownloadJob(
        id=job_id,
        origin="music_album",
        origin_id="album-run-1",
        media_type="music",
        media_intent="music_track",
        source="youtube",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        input_url="https://www.youtube.com/watch?v=abc123xyz00",
        canonical_url="https://www.youtube.com/watch?v=abc123xyz00",
        external_id="abc123xyz00",
        status=JOB_STATUS_COMPLETED,
        queued=None,
        claimed=None,
        downloading=None,
        postprocessing=None,
        completed=None,
        failed=None,
        canceled=None,
        attempts=0,
        max_attempts=3,
        created_at=None,
        updated_at=None,
        last_error=None,
        trace_id=f"trace-{job_id}",
        output_template=output_template,
        resolved_destination=None,
        canonical_id=None,
        file_path=None,
    )


def _base_output_template(*, score=0.95) -> dict:
    return {
        "canonical_metadata": {
            "recording_mbid": "rec-1",
            "mb_release_id": "rel-1",
            "mb_release_group_id": "rg-1",
            "duration_ms": 200000,
        },
        "runtime_search_meta": {
            "failure_reason": None,
            "selected_score": score,
            "selected_candidate_id": "cand-1",
            "selected_candidate_url": "https://www.youtube.com/watch?v=abc123xyz00",
            "selected_candidate_source": "youtube",
            "selected_duration_delta_ms": 0,
        },
    }


def test_publish_schema_validation_rejects_missing_required_fields() -> None:
    valid, reason = _validate_community_publish_proposal({"schema_version": 1})
    assert valid is False
    assert reason == "missing_proposal_id"


def test_publish_skips_when_ineligible_score_below_min(tmp_path: Path) -> None:
    engine = _build_engine(
        tmp_path,
        config={
            "community_cache_publish_enabled": True,
            "community_cache_publish_mode": "write_outbox",
            "community_cache_publish_min_score": 0.99,
            "community_cache_publish_outbox_dir": str(tmp_path / "outbox"),
        },
    )
    job = _build_music_job("job-low-score", output_template=_base_output_template(score=0.90))
    outcome = engine._emit_community_publish_proposal(job, final_path=str(tmp_path / "a.mp3"), meta={"duration_sec": 200})
    assert outcome["status"] == "skipped_ineligible"
    assert outcome["reason"] == "selected_score_below_min"
    outbox_dir = tmp_path / "outbox"
    assert not outbox_dir.exists() or not list(outbox_dir.glob("*.jsonl"))


def test_publish_dry_run_does_not_write_files(tmp_path: Path) -> None:
    engine = _build_engine(
        tmp_path,
        config={
            "community_cache_publish_enabled": True,
            "community_cache_publish_mode": "dry_run",
            "community_cache_publish_min_score": 0.78,
            "community_cache_publish_outbox_dir": str(tmp_path / "outbox"),
        },
    )
    job = _build_music_job("job-dry-run", output_template=_base_output_template(score=0.96))
    outcome = engine._emit_community_publish_proposal(job, final_path=str(tmp_path / "a.mp3"), meta={"duration_sec": 200})
    assert outcome["status"] == "dry_run"
    assert outcome["emitted"] is True
    outbox_dir = tmp_path / "outbox"
    assert not outbox_dir.exists() or not list(outbox_dir.glob("*.jsonl"))


def test_publish_write_outbox_dedupes_recent_recording_video_pair(tmp_path: Path) -> None:
    outbox_dir = tmp_path / "outbox"
    engine = _build_engine(
        tmp_path,
        config={
            "community_cache_publish_enabled": True,
            "community_cache_publish_mode": "write_outbox",
            "community_cache_publish_min_score": 0.78,
            "community_cache_publish_outbox_dir": str(outbox_dir),
        },
    )
    job = _build_music_job("job-write-1", output_template=_base_output_template(score=0.97))
    first = engine._emit_community_publish_proposal(job, final_path=str(tmp_path / "a.mp3"), meta={"duration_sec": 200})
    second = engine._emit_community_publish_proposal(job, final_path=str(tmp_path / "a.mp3"), meta={"duration_sec": 200})

    assert first["status"] == "written"
    assert second["status"] == "deduped"
    files = list(outbox_dir.glob("*.jsonl"))
    assert len(files) == 1
    lines = [line for line in files[0].read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["recording_mbid"] == "rec-1"
    assert payload["video_id"] == "abc123xyz00"
