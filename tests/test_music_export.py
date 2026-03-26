from __future__ import annotations

import subprocess
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

from engine.music_export import run_music_exports
from engine.job_queue import apply_media_metadata_now


def test_run_music_exports_copy_and_transcode(monkeypatch, tmp_path: Path) -> None:
    canonical = tmp_path / "canonical" / "Track 01.mp3"
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(b"audio")

    def _fake_run(cmd, check, capture_output, text):
        destination = Path(cmd[-1])
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"transcoded")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr("engine.music_export.subprocess.run", _fake_run)

    results = run_music_exports(
        str(canonical),
        {
            "title": "Track 01",
            "artist": "Artist",
            "album": "Album",
            "album_artist": "Artist",
            "track_number": 1,
        },
        {
            "music": {
                "exports": [
                    {
                        "name": "apple_music",
                        "enabled": True,
                        "type": "copy",
                        "path": str(tmp_path / "exports" / "apple"),
                    },
                    {
                        "name": "portable_aac",
                        "enabled": True,
                        "type": "transcode",
                        "path": str(tmp_path / "exports" / "portable"),
                        "codec": "aac",
                        "bitrate": "256k",
                    },
                ]
            }
        },
    )

    assert (tmp_path / "exports" / "apple" / canonical.name).exists()
    assert results["apple_music"]["status"] == "copied"
    assert results["portable_aac"]["status"] == "transcoded"
    assert results["portable_aac"]["path"].endswith(".m4a")


def test_run_music_exports_is_non_fatal_per_target(monkeypatch, tmp_path: Path) -> None:
    canonical = tmp_path / "canonical.mp3"
    canonical.write_bytes(b"audio")

    def _fake_run(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, ["ffmpeg"], stderr="boom")

    monkeypatch.setattr("engine.music_export.subprocess.run", _fake_run)

    results = run_music_exports(
        str(canonical),
        {"title": "Track"},
        {
            "music": {
                "exports": [
                    {"name": "copy_ok", "enabled": True, "type": "copy", "path": str(tmp_path / "copy")},
                    {"name": "transcode_bad", "enabled": True, "type": "transcode", "path": str(tmp_path / "x")},
                ]
            }
        },
    )

    assert results["copy_ok"]["status"] == "copied"
    assert results["transcode_bad"]["status"] == "failed"


def test_apply_media_metadata_now_prefers_synchronous_processor(monkeypatch) -> None:
    calls = []

    monkeypatch.setattr(
        "engine.job_queue.process_metadata_now",
        lambda file_path, meta, config: calls.append(("sync", file_path, meta.get("title"))) or True,
    )
    monkeypatch.setattr(
        "engine.job_queue.enqueue_metadata",
        lambda *_args, **_kwargs: calls.append(("enqueue", None, None)),
    )

    result = apply_media_metadata_now("/tmp/test.m4a", {"title": "Tagged Track"}, {"music_metadata": {"enabled": True}})

    assert result is True
    assert calls == [("sync", "/tmp/test.m4a", "Tagged Track")]
