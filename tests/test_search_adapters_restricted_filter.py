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


def _load_search_adapters_module():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    return _load_module("engine_search_adapters_restricted_filter_test", _ROOT / "engine" / "search_adapters.py")


def test_youtube_adapter_filters_age_restricted_results(monkeypatch):
    mod = _load_search_adapters_module()

    class _FakeYDL:
        def __init__(self, opts):
            self.opts = opts

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, _search_term, download=False):
            assert download is False
            return {
                "entries": [
                    {
                        "webpage_url": "https://www.youtube.com/watch?v=adult000001",
                        "title": "Restricted Entry",
                        "uploader": "Uploader A",
                        "age_limit": 18,
                    },
                    {
                        "webpage_url": "https://www.youtube.com/watch?v=safe0000002",
                        "title": "Safe Entry",
                        "uploader": "Uploader B",
                        "age_limit": 0,
                    },
                ]
            }

    monkeypatch.setattr(mod, "YoutubeDL", _FakeYDL)
    adapter = mod.YouTubeAdapter()
    rows = adapter.search_music_track("example", limit=5)
    urls = [str(row.get("url") or "") for row in rows]
    assert urls == ["https://www.youtube.com/watch?v=safe0000002"]

