from __future__ import annotations

import json
import sqlite3
import sys
import types
from pathlib import Path


if "requests" not in sys.modules:
    sys.modules["requests"] = types.ModuleType("requests")
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
if "yt_dlp" not in sys.modules:
    yt_dlp_mod = types.ModuleType("yt_dlp")
    yt_dlp_mod.YoutubeDL = object
    sys.modules["yt_dlp"] = yt_dlp_mod
if "yt_dlp.version" not in sys.modules:
    yt_dlp_version_mod = types.ModuleType("yt_dlp.version")
    yt_dlp_version_mod.__version__ = "0.0-test"
    sys.modules["yt_dlp.version"] = yt_dlp_version_mod
if "yt_dlp.utils" not in sys.modules:
    yt_dlp_utils = types.ModuleType("yt_dlp.utils")
    yt_dlp_utils.DownloadError = Exception
    yt_dlp_utils.ExtractorError = Exception
    sys.modules["yt_dlp.utils"] = yt_dlp_utils
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

from engine.community_publish_worker import (
    append_publish_proposal_to_outbox,
    normalize_community_publish_source,
    summarize_publish_runtime,
)


def _write_proposal() -> dict:
    return {
        "proposal_id": "proposal-1",
        "recording_mbid": "12345678-1234-1234-1234-1234567890ab",
        "video_id": "abc123DEF45",
        "source": "youtube",
        "candidate_url": "https://www.youtube.com/watch?v=abc123DEF45",
        "selected_score": 0.93,
        "emitted_at": "2026-03-24T12:00:00+00:00",
    }


def test_append_publish_proposal_to_outbox_and_runtime_summary(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "db.sqlite"
    sqlite3.connect(db_path).close()
    monkeypatch.setenv("RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN", "token-value")

    result = append_publish_proposal_to_outbox(
        config={
            "community_cache_publish_enabled": True,
            "community_cache_publish_mode": "write_outbox",
            "community_cache_publish_repo": "sudostacks/retreivr-community-cache",
        },
        db_path=str(db_path),
        proposal=_write_proposal(),
    )

    assert result["status"] == "written"
    outbox_path = Path(str(result["outbox_path"]))
    assert outbox_path.exists()
    payload = json.loads(outbox_path.read_text(encoding="utf-8").strip())
    assert payload["proposal_id"] == "proposal-1"

    status = summarize_publish_runtime(
        config={
            "community_cache_publish_enabled": True,
            "community_cache_publish_mode": "write_outbox",
            "community_cache_publish_repo": "sudostacks/retreivr-community-cache",
            "community_cache_publish_target_branch": "main",
            "community_cache_publish_branch": "retreivr-community-publish/tester",
        },
        db_path=str(db_path),
        last_summary={"status": "ok", "published_proposals": 1},
        active_task={"kind": "publish", "running": False, "status": "completed"},
    )

    assert status["enabled"] is True
    assert status["worker_enabled"] is True
    assert status["token_present"] is True
    assert status["outbox"]["file_count"] == 1
    assert status["outbox"]["proposal_lines"] == 1
    assert status["queue"]["counts"]["total"] == 0
    assert status["last_summary"]["published_proposals"] == 1
    assert status["active_task"]["kind"] == "publish"


def test_append_publish_proposal_rejects_invalid_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    sqlite3.connect(db_path).close()

    result = append_publish_proposal_to_outbox(
        config={},
        db_path=str(db_path),
        proposal={"proposal_id": "bad"},
    )

    assert result["status"] == "validation_failed"
    assert result["outbox_path"] is None


def test_normalize_community_publish_source_maps_youtube_music_to_youtube() -> None:
    assert normalize_community_publish_source("youtube_music") == "youtube"
    assert normalize_community_publish_source("youtube") == "youtube"
