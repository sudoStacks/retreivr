from __future__ import annotations

import sys
import types
from pathlib import Path


if "requests" not in sys.modules:
    sys.modules["requests"] = types.ModuleType("requests")
if "musicbrainzngs" not in sys.modules:
    sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
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

from engine.community_publish_backfill import _build_backfill_proposal


def test_build_backfill_proposal_normalizes_youtube_music(monkeypatch, tmp_path: Path) -> None:
    media = tmp_path / "Track 01.m4a"
    media.write_bytes(b"audio")
    monkeypatch.setattr("engine.community_publish_backfill.get_media_duration", lambda _path: 212.4)

    proposal = _build_backfill_proposal(
        {
            "path": str(media),
            "recording_mbid": "12345678-1234-1234-1234-1234567890ab",
            "mb_release_id": "aaaaaaaa-1234-1234-1234-1234567890ab",
            "mb_release_group_id": "bbbbbbbb-1234-1234-1234-1234567890ab",
            "retreivr_source": "youtube_music",
            "retreivr_source_id": "abc123DEF45",
        },
        {},
    )

    assert proposal["source"] == "youtube"
    assert proposal["candidate_url"] == "https://www.youtube.com/watch?v=abc123DEF45"
    assert proposal["candidate_id"] == "abc123DEF45"
    assert proposal["duration_ms"] == 212400
    assert proposal["verified_by"] == "retreivr_backfill"
    assert proposal["proposal_id"].startswith("backfill-")
