from __future__ import annotations

import json
import sqlite3
import sys
import types
from pathlib import Path

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

from engine.job_queue import build_music_album_run_summary


def test_build_music_album_run_summary_aggregates_export_results(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE download_jobs (
                id TEXT PRIMARY KEY,
                origin TEXT NOT NULL,
                origin_id TEXT NOT NULL,
                media_intent TEXT NOT NULL,
                status TEXT NOT NULL,
                last_error TEXT,
                output_template TEXT,
                file_path TEXT,
                created_at TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO download_jobs
            (id, origin, origin_id, media_intent, status, last_error, output_template, file_path, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "job-1",
                    "music_album",
                    "album-run-exports",
                    "music_track",
                    "completed",
                    None,
                    json.dumps({"export_results": {"apple_music": {"status": "copied"}, "portable_aac": {"status": "transcoded"}}}),
                    str(tmp_path / "Music" / "a.mp3"),
                    "2026-03-24T00:00:00+00:00",
                ),
                (
                    "job-2",
                    "music_album",
                    "album-run-exports",
                    "music_track",
                    "completed",
                    None,
                    json.dumps({"export_results": {"apple_music": {"status": "failed"}}}),
                    str(tmp_path / "Music" / "b.mp3"),
                    "2026-03-24T00:00:01+00:00",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    summary, _ = build_music_album_run_summary(str(db_path), "album-run-exports")

    assert summary["exports"]["apple_music"] == {"copied": 1, "transcoded": 0, "failed": 1}
    assert summary["exports"]["portable_aac"] == {"copied": 0, "transcoded": 1, "failed": 0}
