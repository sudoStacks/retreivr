from __future__ import annotations

import json
import sqlite3
import sys
import types

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

from engine.job_queue import write_music_album_run_summary


def test_write_music_album_run_summary_emits_expected_schema(tmp_path) -> None:
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
        rows = [
            (
                "job-ok",
                "music_album",
                "album-run-1",
                "music_track",
                "completed",
                None,
                json.dumps(
                    {
                        "canonical_metadata": {
                            "recording_mbid": "rec-ok",
                            "mb_release_group_id": "rg-1",
                        },
                        "runtime_search_meta": {
                            "decision_edge": {
                                "accepted_selection": {
                                    "selected_candidate_id": "ok-cand",
                                    "selected_score": 0.95,
                                    "runner_up_score": 0.90,
                                    "runner_up_gap": 0.05,
                                    "top_supporting_features": {
                                        "duration_delta_ms": 0,
                                        "title_similarity": 1.0,
                                        "artist_similarity": 1.0,
                                        "variant_alignment": True,
                                    },
                                },
                                "rejected_candidates": [],
                                "final_rejection": None,
                            }
                        },
                    }
                ),
                str(tmp_path / "Music" / "Artist" / "Album (2024)" / "01 - Song.mp3"),
                "2026-02-26T00:00:01+00:00",
            ),
            (
                "job-fail",
                "music_album",
                "album-run-1",
                "music_track",
                "failed",
                "source_unavailable:region_restricted",
                json.dumps(
                    {
                        "canonical_metadata": {
                            "recording_mbid": "rec-fail",
                            "mb_release_group_id": "rg-1",
                        },
                        "runtime_search_meta": {
                            "decision_edge": {
                                "accepted_selection": None,
                                "rejected_candidates": [
                                    {
                                        "candidate_id": "bad-cand",
                                        "top_failed_gate": "duration_delta_ms",
                                        "nearest_pass_margin": {
                                            "name": "duration_delta_ms",
                                            "value": 4200,
                                            "threshold": 3000,
                                            "margin_to_pass": 1200,
                                            "direction": "<=",
                                        },
                                    }
                                ],
                                "final_rejection": {
                                    "failure_reason": "duration_filtered",
                                    "top_failed_gate": "duration_delta_ms",
                                    "nearest_pass_margin": {
                                        "name": "duration_delta_ms",
                                        "value": 4200,
                                        "threshold": 3000,
                                        "margin_to_pass": 1200,
                                        "direction": "<=",
                                    },
                                    "candidate_id": "bad-cand",
                                },
                            }
                        },
                    }
                ),
                None,
                "2026-02-26T00:00:02+00:00",
            ),
        ]
        conn.executemany(
            """
            INSERT INTO download_jobs
            (id, origin, origin_id, media_intent, status, last_error, output_template, file_path, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    album_dir = tmp_path / "Music" / "Artist" / "Album (2024)"
    album_dir.mkdir(parents=True, exist_ok=True)

    output_path = write_music_album_run_summary(str(db_path), "album-run-1", output_dir=str(album_dir))
    assert output_path is not None

    summary_file = album_dir / "run_summary.json"
    assert summary_file.exists()
    payload = json.loads(summary_file.read_text(encoding="utf-8"))

    assert payload["run_type"] == "music_album"
    assert payload["album_run_id"] == "album-run-1"
    assert payload["tracks_total"] == 2
    assert payload["tracks_resolved"] == 1
    assert "completion_percent" in payload
    assert "wrong_variant_flags" in payload
    assert "rejection_mix" in payload
    assert "unresolved_classification" in payload
    assert "why_missing" in payload
    assert "hint_counts" in payload["why_missing"]
    assert isinstance(payload["why_missing"]["tracks"], list)
    assert "per_track" in payload
    assert isinstance(payload["per_track"], list)
    assert all("decision_edge" in item for item in payload["per_track"])
    edge = payload["per_track"][0]["decision_edge"]
    assert "candidate_variant_distribution" in edge
    assert "selected_candidate_variant_tags" in edge
    assert "top_rejected_variant_tags" in edge
