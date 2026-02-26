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


def test_search_music_metadata_album_mode_queries_album_or_ep() -> None:
    captured = {"query": None}

    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    mbngs = sys.modules["musicbrainzngs"]
    mbngs.search_release_groups = (
        lambda query, limit, offset=0: (
            captured.update({"query": str(query)}),
            {
                "release-group-list": [
                    {
                        "id": "rg-ep-1",
                        "title": "Live from the South",
                        "artist-credit": [{"name": "ERNEST"}],
                        "first-release-date": "2025-01-01",
                    }
                ]
            },
        )[1]
    )

    if "metadata.services" not in sys.modules:
        services_pkg = types.ModuleType("metadata.services")
        services_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["metadata.services"] = services_pkg
    mb_service_mod = types.ModuleType("metadata.services.musicbrainz_service")
    mb_service_mod.get_musicbrainz_service = lambda: types.SimpleNamespace(_call_with_retry=lambda func: func())
    sys.modules["metadata.services.musicbrainz_service"] = mb_service_mod

    binding = _load_module(
        "engine_musicbrainz_binding_ep_album_tests",
        _ROOT / "engine" / "musicbrainz_binding.py",
    )
    payload = binding.search_music_metadata(
        artist="ERNEST",
        album="Live from the South",
        mode="auto",
        limit=10,
    )
    assert isinstance(payload, dict)
    assert len(payload.get("albums") or []) == 1
    query_text = str(captured.get("query") or "")
    assert "primarytype:(album OR ep)" in query_text


def test_release_bucket_classifies_ep_as_album() -> None:
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    if "metadata.services" not in sys.modules:
        services_pkg = types.ModuleType("metadata.services")
        services_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["metadata.services"] = services_pkg
    mb_service_mod = types.ModuleType("metadata.services.musicbrainz_service")
    mb_service_mod.get_musicbrainz_service = lambda: None
    sys.modules["metadata.services.musicbrainz_service"] = mb_service_mod

    binding = _load_module(
        "engine_musicbrainz_binding_ep_bucket_tests",
        _ROOT / "engine" / "musicbrainz_binding.py",
    )
    bucket = binding._classify_release_bucket(
        {
            "release": {
                "release-group": {
                    "primary-type": "EP",
                    "secondary-type-list": [],
                }
            }
        }
    )
    assert bucket == "album"


def test_service_search_release_groups_queries_album_or_ep(monkeypatch) -> None:
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    mbngs = sys.modules["musicbrainzngs"]
    mbngs.set_useragent = lambda *args, **kwargs: None
    mbngs.set_rate_limit = lambda *args, **kwargs: None
    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")
        requests_stub.get = lambda *args, **kwargs: None
        sys.modules["requests"] = requests_stub

    service_mod = _load_module(
        "metadata_services_musicbrainz_ep_album_tests",
        _ROOT / "metadata" / "services" / "musicbrainz_service.py",
    )

    captured = {"query": None}

    def _fake_search_release_groups(*, query, limit):
        captured["query"] = str(query)
        return {"release-group-list": []}

    monkeypatch.setattr(service_mod.musicbrainzngs, "search_release_groups", _fake_search_release_groups)
    service = service_mod.MusicBrainzService(debug=True)
    monkeypatch.setattr(service, "_respect_rate_limit", lambda: None)
    service.search_release_groups("ERNEST Live from the South", limit=10)
    assert '(primarytype:"album" OR primarytype:"ep")' in str(captured.get("query") or "")
