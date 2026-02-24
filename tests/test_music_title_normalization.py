from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_binding():
    return _load_module("engine_musicbrainz_binding_normalization_tests", _ROOT / "engine" / "musicbrainz_binding.py")


def _load_job_queue():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_module = types.ModuleType("metadata.services.musicbrainz_service")
        mb_module.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_module
    return _load_module("engine_job_queue_title_normalization_tests", _ROOT / "engine" / "job_queue.py")


def test_music_video_suffix_is_stripped():
    binding = _load_binding()
    raw = "John Rich - Shuttin’ Detroit Down [Music Video]"
    normalized = binding._normalize_title_for_mb_lookup(raw)
    assert normalized == "john rich - shuttin’ detroit down"


def test_official_video_parenthetical_removed():
    binding = _load_binding()
    raw = "Shuttin’ Detroit Down (Official Video)"
    normalized = binding._normalize_title_for_mb_lookup(raw)
    assert normalized == "shuttin’ detroit down"


def test_audio_tag_removed():
    binding = _load_binding()
    raw = "Shuttin’ Detroit Down (Official Audio)"
    normalized = binding._normalize_title_for_mb_lookup(raw)
    assert normalized == "shuttin’ detroit down"


def test_live_not_removed():
    binding = _load_binding()
    raw = "Shuttin’ Detroit Down (Live)"
    normalized = binding._normalize_title_for_mb_lookup(raw)
    assert "live" in normalized


def test_standard_mode_not_affected(monkeypatch):
    jq = _load_job_queue()

    def _boom(_value, *, query_flags=None):
        _ = query_flags
        raise AssertionError("normalization should not be called for non-music paths")

    monkeypatch.setattr(jq, "_normalize_title_for_mb_lookup", _boom)
    payload = jq.build_download_job_payload(
        config={"final_format": "mkv"},
        origin="manual",
        origin_id="video-1",
        media_type="video",
        media_intent="episode",
        source="youtube",
        url="https://www.youtube.com/watch?v=abc123xyz00",
        base_dir="/downloads",
    )
    assert payload["media_type"] == "video"

