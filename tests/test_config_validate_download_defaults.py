from __future__ import annotations

import importlib
import sys
import types


def _install_core_import_stubs() -> None:
    for name in (
        "requests",
        "google",
        "google.auth",
        "google.auth.exceptions",
        "google.auth.transport",
        "google.auth.transport.requests",
        "google.oauth2",
        "google.oauth2.credentials",
        "googleapiclient",
        "googleapiclient.discovery",
        "googleapiclient.errors",
        "yt_dlp",
        "yt_dlp.version",
    ):
        sys.modules.setdefault(name, types.ModuleType(name))

    sys.modules["google.auth.exceptions"].RefreshError = Exception
    sys.modules["google.auth.transport.requests"].Request = object
    sys.modules["google.oauth2.credentials"].Credentials = object
    sys.modules["googleapiclient.discovery"].build = lambda *_a, **_k: None
    sys.modules["googleapiclient.errors"].HttpError = Exception
    sys.modules["yt_dlp"].YoutubeDL = object
    sys.modules["yt_dlp.version"].__version__ = "0.0-test"

    jq = types.ModuleType("engine.job_queue")

    class _Dummy:
        def __init__(self, *_a, **_k):
            pass

    jq.DownloadWorkerEngine = _Dummy
    jq.DownloadJobStore = _Dummy
    jq.build_download_job_payload = lambda *_a, **_k: {}
    jq.build_output_template = lambda *_a, **_k: {}
    jq.ensure_download_jobs_table = lambda *_a, **_k: None
    jq.preview_direct_url = lambda *_a, **_k: {}
    jq.resolve_cookie_file = lambda *_a, **_k: None
    jq.resolve_media_intent = lambda *_a, **_k: None
    jq.resolve_media_type = lambda *_a, **_k: None
    jq.resolve_source = lambda *_a, **_k: None
    sys.modules["engine.job_queue"] = jq


def _load_engine_core():
    _install_core_import_stubs()
    sys.modules.pop("engine.core", None)
    return importlib.import_module("engine.core")


def test_validate_config_rejects_non_string_download_defaults() -> None:
    core = _load_engine_core()

    errors = core.validate_config(
        {
            "single_download_folder": 123,
            "home_music_download_folder": [],
            "home_music_video_download_folder": {},
        }
    )

    assert "single_download_folder must be a string" in errors
    assert "home_music_download_folder must be a string" in errors
    assert "home_music_video_download_folder must be a string" in errors


def test_validate_config_rejects_invalid_music_export_targets() -> None:
    core = _load_engine_core()

    errors = core.validate_config(
        {
            "music": {
                "library_path": 123,
                "exports": [
                    {"name": "bad-copy", "enabled": "yes", "type": "copy", "path": "/tmp/out"},
                    {"name": "bad-type", "enabled": True, "type": "move", "path": "/tmp/out"},
                ],
            }
        }
    )

    assert "music.library_path must be a string" in errors
    assert "music.exports[1].enabled must be true/false" in errors
    assert "music.exports[2].type must be 'copy' or 'transcode'" in errors


def test_validate_config_requires_existing_music_export_paths_when_enabled(tmp_path) -> None:
    core = _load_engine_core()

    errors = core.validate_config(
        {
            "music": {
                "library_path": str(tmp_path / "missing-library"),
                "exports": [
                    {"name": "apple_music", "enabled": True, "type": "copy", "path": str(tmp_path / "missing-export")},
                ],
            }
        }
    )

    assert "music.library_path must exist" in errors
    assert "music.exports[1].path must exist when enabled" in errors
