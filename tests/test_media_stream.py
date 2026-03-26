from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module():
    if "fastapi" not in sys.modules:
        fastapi_mod = types.ModuleType("fastapi")
        fastapi_mod.Request = object
        sys.modules["fastapi"] = fastapi_mod
    if "fastapi.responses" not in sys.modules:
        responses_mod = types.ModuleType("fastapi.responses")

        class _Response:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        class _StreamingResponse(_Response):
            pass

        responses_mod.Response = _Response
        responses_mod.StreamingResponse = _StreamingResponse
        sys.modules["fastapi.responses"] = responses_mod
    spec = importlib.util.spec_from_file_location(
        "api.media_stream",
        _ROOT / "api" / "media_stream.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["api.media_stream"] = module
    spec.loader.exec_module(module)
    return module


media_stream = _load_module()


def test_resolve_byte_range_handles_standard_and_suffix_ranges():
    assert media_stream.resolve_byte_range("bytes=0-9", 100) == (0, 9)
    assert media_stream.resolve_byte_range("bytes=10-", 100) == (10, 99)
    assert media_stream.resolve_byte_range("bytes=-25", 100) == (75, 99)


def test_iter_file_range_reads_only_requested_slice(tmp_path: Path):
    path = tmp_path / "sample.bin"
    path.write_bytes(b"abcdefghijklmnopqrstuvwxyz")

    payload = b"".join(media_stream.iter_file_range(str(path), 5, 9, chunk_size=2))

    assert payload == b"fghij"


def test_guess_browser_media_type_normalizes_browser_playable_audio_types():
    assert media_stream.guess_browser_media_type("sample.m4a") == "audio/mp4"
    assert media_stream.guess_browser_media_type("sample.mp3") == "audio/mpeg"
    assert media_stream.guess_browser_media_type("sample.aac") == "audio/aac"
