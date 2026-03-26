from __future__ import annotations

import importlib.util
import json
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
    return _load_module("engine_search_adapters_site_registry_test", _ROOT / "engine" / "search_adapters.py")


def test_default_adapters_include_new_site_sources():
    mod = _load_search_adapters_module()
    adapters = mod.default_adapters(config={})
    assert "archive_org" in adapters
    assert "rumble" in adapters


def test_archive_org_adapter_uses_advancedsearch_api(monkeypatch):
    mod = _load_search_adapters_module()

    class _Resp:
        ok = True

        def json(self):
            return {
                "response": {
                    "docs": [
                        {
                            "identifier": "example-item",
                            "title": "Example Archive Video",
                            "creator": "Archive Author",
                            "mediatype": ["movies"],
                        }
                    ]
                }
            }

    def _fake_get(*_args, **_kwargs):
        return _Resp()

    monkeypatch.setattr(mod.requests, "get", _fake_get)
    adapter = mod.ArchiveOrgAdapter()
    rows = adapter.search_track("fox", "news", limit=3, lightweight=True)
    assert rows
    assert rows[0]["url"] == "https://archive.org/details/example-item"
    assert rows[0]["title"] == "Example Archive Video"
    assert rows[0]["thumbnail_url"] == "https://archive.org/services/img/example-item"


def test_rumble_adapter_enriches_title_and_thumbnail_from_oembed(monkeypatch):
    mod = _load_search_adapters_module()

    class _RespSearch:
        ok = True
        text = '<a href="/v5abcd-sample-video.html">placeholder</a>'

    class _RespOEmbed:
        ok = True

        def json(self):
            return {
                "title": "Sample Rumble Title",
                "thumbnail_url": "https://sp.rmbl.ws/s8/1/ab/cd/ef/Sample.jpg",
                "author_name": "Sample Channel",
                "html": '<iframe src="https://rumble.com/embed/v5abcd/?pub=4"></iframe>',
            }

    def _fake_get(url, *args, **kwargs):
        if "oembed" in str(url):
            return _RespOEmbed()
        return _RespSearch()

    monkeypatch.setattr(mod.requests, "get", _fake_get)
    adapter = mod.RumbleAdapter()
    rows = adapter.search_track("sample", "video", limit=3, lightweight=True)
    assert rows
    assert rows[0]["title"] == "Sample Rumble Title"
    assert rows[0]["thumbnail_url"] == "https://sp.rmbl.ws/s8/1/ab/cd/ef/Sample.jpg"
    raw_meta = json.loads(rows[0]["raw_meta_json"])
    assert raw_meta.get("embed_url") == "https://rumble.com/embed/v5abcd/?pub=4"


def test_default_adapters_load_custom_site_adapter_from_json(tmp_path):
    mod = _load_search_adapters_module()
    custom_file = tmp_path / "custom_search_adapters.json"
    custom_file.write_text(
        json.dumps(
            {
                "version": 1,
                "adapters": [
                    {
                        "source": "my_video_mirror",
                        "type": "site_search",
                        "enabled": True,
                        "domains": ["media.example.org"],
                        "source_modifier": 0.77,
                        "query_suffix": "video",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    adapters = mod.default_adapters(config={"custom_search_adapters_file": str(custom_file)})
    assert "my_video_mirror" in adapters
    adapter = adapters["my_video_mirror"]
    assert getattr(adapter, "source", "") == "my_video_mirror"
    assert "media.example.org" in tuple(getattr(adapter, "domains", ()))


def test_duckduckgo_site_search_parses_protocol_relative_redirect_links(monkeypatch):
    mod = _load_search_adapters_module()

    class _Resp:
        ok = True
        text = (
            '<html><body>'
            '<a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Frumble.com%2Fvabc123">'
            "Rumble Result"
            "</a>"
            "</body></html>"
        )

    def _fake_get(*_args, **_kwargs):
        return _Resp()

    monkeypatch.setattr(mod.requests, "get", _fake_get)
    rows = mod._duckduckgo_site_search(  # type: ignore[attr-defined]
        "example query",
        domains=("rumble.com",),
        limit=5,
        timeout_sec=2.0,
    )
    assert len(rows) == 1
    assert rows[0]["url"] == "https://rumble.com/vabc123"


def test_site_search_adapter_dedupes_repeated_terms(monkeypatch):
    mod = _load_search_adapters_module()

    captured = {}

    def _fake_search(query, *, domains, limit, timeout_sec):
        captured["query"] = query
        return []

    monkeypatch.setattr(mod, "_duckduckgo_site_search", _fake_search)
    adapter = mod.SiteSearchAdapter(source="test_site", domains=["example.com"], query_suffix="video")
    adapter.search_track("fox news", "fox news", limit=5, lightweight=True)
    assert captured["query"] == "fox news video"
