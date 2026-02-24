from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_job_queue_module():
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
    return _load_module("engine_job_queue_music_opts_regression", _ROOT / "engine" / "job_queue.py")


@pytest.fixture(scope="module")
def jq():
    return _load_job_queue_module()


def test_music_job_builds_audio_ytdlp_opts(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mkv",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }
    opts = jq.build_ytdlp_opts(context)

    assert context["audio_mode"] is True
    assert str(opts.get("format", "")).startswith("bestaudio")
    assert "merge_output_format" not in opts
    postprocessors = opts.get("postprocessors") or []
    extract_pp = next((pp for pp in postprocessors if pp.get("key") == "FFmpegExtractAudio"), None)
    assert extract_pp is not None
    assert extract_pp.get("preferredcodec") == "mp3"


def test_video_job_builds_video_ytdlp_opts(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "video",
        "media_intent": "episode",
        "final_format": "webm",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "webm",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "webm",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }
    opts = jq.build_ytdlp_opts(context)

    assert context["audio_mode"] is False
    assert opts.get("format") == "bestvideo[ext=webm]+bestaudio/bestvideo[ext=mp4]+bestaudio/best"
    assert "merge_output_format" not in opts
    postprocessors = opts.get("postprocessors") or []
    assert not any(pp.get("key") == "FFmpegExtractAudio" for pp in postprocessors if isinstance(pp, dict))


def test_music_invariant_raises_if_video_fields_present(jq, monkeypatch) -> None:
    original_merge_overrides = jq._merge_overrides

    def _regressing_merge_overrides(opts, overrides, *, operation, lock_format=False):
        merged = original_merge_overrides(opts, overrides, operation=operation, lock_format=lock_format)
        merged["merge_output_format"] = "mkv"
        return merged

    monkeypatch.setattr(jq, "_merge_overrides", _regressing_merge_overrides)

    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mkv",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }

    with pytest.raises(RuntimeError, match="music_job_built_video_opts"):
        jq.build_ytdlp_opts(context)


def test_music_job_uses_resolved_music_codec_for_extract_audio(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mkv",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mkv",
            "music_final_format": "m4a",
        },
        "config": {
            "final_format": "mkv",
            "music_final_format": "m4a",
        },
        "overrides": {},
    }
    opts = jq.build_ytdlp_opts(context)

    postprocessors = opts.get("postprocessors") or []
    extract_pp = next((pp for pp in postprocessors if pp.get("key") == "FFmpegExtractAudio"), None)
    assert extract_pp is not None
    assert extract_pp.get("preferredcodec") == "m4a"


def test_video_mp4_job_enforces_quicktime_compatible_selector(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "video",
        "media_intent": "episode",
        "final_format": "mp4",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mp4",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "mp4",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }

    opts = jq.build_ytdlp_opts(context)

    assert context["audio_mode"] is False
    assert opts.get("format") == (
        "bestvideo[vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a][ext=m4a]/"
        "bestvideo[vcodec^=avc1]+bestaudio[acodec^=mp4a]/"
        "best[ext=mp4][vcodec^=avc1][acodec^=mp4a]"
    )
    assert opts.get("merge_output_format") == "mp4"
