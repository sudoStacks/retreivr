from __future__ import annotations

import importlib.util
import sys
import types
from types import SimpleNamespace
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _install_google_stubs():
    if "google" not in sys.modules:
        sys.modules["google"] = types.ModuleType("google")
    if "google.auth" not in sys.modules:
        sys.modules["google.auth"] = types.ModuleType("google.auth")
    if "google.auth.exceptions" not in sys.modules:
        m = types.ModuleType("google.auth.exceptions")
        m.RefreshError = RuntimeError
        sys.modules["google.auth.exceptions"] = m
    if "google.auth.transport" not in sys.modules:
        sys.modules["google.auth.transport"] = types.ModuleType("google.auth.transport")
    if "google.auth.transport.requests" not in sys.modules:
        m = types.ModuleType("google.auth.transport.requests")
        m.Request = object
        sys.modules["google.auth.transport.requests"] = m
    if "google.oauth2" not in sys.modules:
        sys.modules["google.oauth2"] = types.ModuleType("google.oauth2")
    if "google.oauth2.credentials" not in sys.modules:
        m = types.ModuleType("google.oauth2.credentials")
        m.Credentials = object
        sys.modules["google.oauth2.credentials"] = m
    if "googleapiclient" not in sys.modules:
        sys.modules["googleapiclient"] = types.ModuleType("googleapiclient")
    if "googleapiclient.discovery" not in sys.modules:
        m = types.ModuleType("googleapiclient.discovery")
        m.build = lambda *args, **kwargs: None
        sys.modules["googleapiclient.discovery"] = m
    if "googleapiclient.errors" not in sys.modules:
        m = types.ModuleType("googleapiclient.errors")
        m.HttpError = RuntimeError
        sys.modules["googleapiclient.errors"] = m


def _load_search_engine():
    _install_google_stubs()
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    _load_module("engine.search_adapters", _ROOT / "engine" / "search_adapters.py")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_module = types.ModuleType("metadata.services.musicbrainz_service")
        mb_module.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_module
    if "metadata.canonical" not in sys.modules:
        m = types.ModuleType("metadata.canonical")
        m.CanonicalMetadataResolver = lambda config=None: SimpleNamespace(resolve_track=lambda *a, **k: None)
        sys.modules["metadata.canonical"] = m
    return _load_module("engine_search_request_destination_test", _ROOT / "engine" / "search_engine.py")


class _StubCanonicalResolver:
    def resolve_track(self, artist, track, *, album=None):
        _ = artist, track, album
        return None

    def resolve_album(self, artist, album):
        _ = artist, album
        return None


def test_get_search_request_resolves_destination_without_name_error(tmp_path):
    se = _load_search_engine()
    service = se.SearchResolutionService(
        search_db_path=str(tmp_path / "search.sqlite"),
        queue_db_path=str(tmp_path / "queue.sqlite"),
        adapters={},
        config={},
        paths=SimpleNamespace(single_downloads_dir=str(tmp_path / "downloads")),
        canonical_resolver=_StubCanonicalResolver(),
    )

    request_id = service.store.create_request(
        {
            "created_by": "test",
            "intent": "track",
            "media_type": "music",
            "artist": "Artist",
            "track": "Track",
            "destination_dir": "CustomFolder",
            "include_albums": 1,
            "include_singles": 1,
            "min_match_score": 0.92,
            "duration_hint_sec": 200,
            "quality_min_bitrate_kbps": None,
            "lossless_only": 0,
            "auto_enqueue": 0,
            "source_priority_json": "[]",
            "max_candidates_per_source": 5,
        }
    )

    payload = service.get_search_request(request_id)
    assert payload is not None
    request = payload["request"]
    assert request.get("resolved_destination")
