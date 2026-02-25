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


def _load_job_queue():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_service = types.ModuleType("metadata.services.musicbrainz_service")
        mb_service.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_service
    if "metadata.services" not in sys.modules:
        metadata_services = types.ModuleType("metadata.services")
        metadata_services.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services"] = metadata_services
    return _load_module("engine_job_queue_cli_parity", _ROOT / "engine" / "job_queue.py")


def test_music_cli_translation_parity_includes_runtime_cookie_and_audio_flags() -> None:
    jq = _load_job_queue()
    tokens_dir = Path(jq.TOKENS_DIR)
    tokens_dir.mkdir(parents=True, exist_ok=True)
    cookies_path = tokens_dir / "cookies.txt"
    cookies_path.write_text("# Netscape HTTP Cookie File\n", encoding="utf-8")

    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": None,
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {"music_final_format": "mp3"},
        "config": {
            "music_final_format": "mp3",
            "js_runtime": "node:/usr/bin/node",
            "yt_dlp_cookies": "cookies.txt",
        },
        "overrides": {},
    }

    opts = jq.build_ytdlp_opts(context)
    argv = jq._render_ytdlp_cli_argv(opts, context["url"])

    assert "-f" in argv and argv[argv.index("-f") + 1] == "bestaudio/best"
    assert "--cookies" not in argv
    assert "--js-runtimes" not in argv
    assert "--extractor-args" not in argv
    assert "-x" in argv
    assert "--audio-format" in argv and argv[argv.index("--audio-format") + 1] == "mp3"


def test_video_mp4_cli_translation_includes_recode_video() -> None:
    jq = _load_job_queue()
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "video",
        "media_intent": "episode",
        "final_format": "mp4",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {"final_format": "mp4"},
        "config": {
            "final_format": "mp4",
        },
        "overrides": {},
    }

    opts = jq.build_ytdlp_opts(context)
    argv = jq._render_ytdlp_cli_argv(opts, context["url"])

    assert "--merge-output-format" in argv and argv[argv.index("--merge-output-format") + 1] == "mp4"
    assert "--recode-video" in argv and argv[argv.index("--recode-video") + 1] == "mp4"
