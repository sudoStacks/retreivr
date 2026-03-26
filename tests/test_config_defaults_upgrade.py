from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path


def _install_core_import_stubs() -> None:
    # Optional runtime deps used by engine.core.
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

    # Prevent deep import chain through real engine.job_queue for these tests.
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


def _collect_dict_key_paths(payload: dict, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else key
        paths.add(path)
        if isinstance(value, dict):
            paths.update(_collect_dict_key_paths(value, path))
    return paths


def test_apply_config_defaults_backfills_missing_keys_from_sample() -> None:
    core = _load_engine_core()
    sample = core._load_default_config_template()  # noqa: SLF001

    normalized = core.apply_config_defaults({"accounts": {}})

    expected_paths = _collect_dict_key_paths(sample)
    actual_paths = _collect_dict_key_paths(normalized)
    missing = sorted(expected_paths - actual_paths)
    assert not missing


def test_apply_config_defaults_does_not_inject_sample_entities() -> None:
    core = _load_engine_core()
    normalized = core.apply_config_defaults({})

    assert normalized.get("accounts") == {}
    assert normalized.get("playlists") == []
    assert normalized.get("spotify_playlists") == []


def test_apply_config_defaults_preserves_user_values() -> None:
    core = _load_engine_core()
    user = {
        "schedule": {"enabled": True, "interval_hours": 2},
        "music_metadata": {"enabled": False, "use_acoustid": True},
        "music_skip_metadata_probe": False,
        "accounts": {"my_account": {"client_secret": "a.json", "token": "b.json"}},
    }

    normalized = core.apply_config_defaults(user)

    assert normalized["schedule"]["enabled"] is True
    assert normalized["schedule"]["interval_hours"] == 2
    assert normalized["music_metadata"]["enabled"] is False
    assert normalized["music_metadata"]["use_acoustid"] is True
    assert normalized["music_skip_metadata_probe"] is False
    assert normalized["accounts"] == {"my_account": {"client_secret": "a.json", "token": "b.json"}}


def test_load_config_write_back_defaults_persists_missing_entries(tmp_path: Path) -> None:
    core = _load_engine_core()
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"accounts": {}}), encoding="utf-8")

    loaded = core.load_config(str(config_path), write_back_defaults=True)
    persisted = json.loads(config_path.read_text(encoding="utf-8"))

    assert "watch_policy" in loaded
    assert "watch_policy" in persisted
    assert "community_cache_lookup_enabled" in persisted
    assert "music_skip_metadata_probe" in persisted
    assert "music_candidate_cooldown_enabled" in persisted
    assert persisted.get("accounts") == {}
    assert persisted.get("playlists") == []
    assert persisted.get("spotify_playlists") == []
