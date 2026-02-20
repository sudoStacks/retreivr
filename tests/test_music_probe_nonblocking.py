from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace


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
    if "engine.musicbrainz_binding" not in sys.modules:
        binding_module = types.ModuleType("engine.musicbrainz_binding")
        binding_module.resolve_best_mb_pair = lambda *args, **kwargs: None
        binding_module._normalize_title_for_mb_lookup = lambda value, **kwargs: str(value or "")
        sys.modules["engine.musicbrainz_binding"] = binding_module
    return _load_module("engine_job_queue_music_probe_nonblocking", _ROOT / "engine" / "job_queue.py")


def test_music_probe_failure_does_not_block_download(tmp_path, monkeypatch) -> None:
    jq = _load_job_queue()
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    fake_output = temp_dir / "download.mp3"
    fake_output.write_bytes(b"ok")

    class _FailProbeYDL:
        def __init__(self, opts):
            self.opts = dict(opts or {})

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download=False):
            _ = url, download, self.opts
            raise RuntimeError("probe_failed")

    calls = {"download": 0}

    def _fake_subprocess_run(*args, **kwargs):
        _ = args, kwargs
        calls["download"] += 1
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(jq, "YoutubeDL", _FailProbeYDL)
    monkeypatch.setattr(jq.subprocess, "run", _fake_subprocess_run)
    monkeypatch.setattr(jq, "_select_download_output", lambda *_args, **_kwargs: str(fake_output))

    info, local_file = jq.download_with_ytdlp(
        "https://www.youtube.com/watch?v=abc123xyz00",
        str(temp_dir),
        {"music_final_format": "mp3"},
        audio_mode=True,
        final_format="mp3",
        media_type="music",
        media_intent="music_track",
        job_id="job-probe-nonblocking",
        origin="test",
    )

    assert info is not None
    assert local_file == str(fake_output)
    assert calls["download"] >= 1

