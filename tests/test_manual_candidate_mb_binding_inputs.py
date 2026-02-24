from __future__ import annotations

import importlib.util
import json
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


def _install_engine_base_stubs():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    _load_module("engine.search_adapters", _ROOT / "engine" / "search_adapters.py")
    _load_module("engine.musicbrainz_binding", _ROOT / "engine" / "musicbrainz_binding.py")
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


def _load_job_queue():
    _install_engine_base_stubs()
    return _load_module("engine_job_queue_manual_mb_input_tests", _ROOT / "engine" / "job_queue.py")


def _load_search_engine(job_queue_module):
    _install_engine_base_stubs()
    sys.modules["engine.job_queue"] = job_queue_module
    return _load_module("engine_search_engine_manual_mb_input_tests", _ROOT / "engine" / "search_engine.py")


def test_parse_artist_track_from_candidate_uses_title_split_and_lookup_normalization():
    jq = _load_job_queue()
    se = _load_search_engine(jq)

    artist, track = se._parse_artist_track_from_candidate(
        "John Rich - Shuttin’ Detroit Down [Music Video]",
        "JohnRichVEVO",
        "John Rich",
    )
    assert artist == "John Rich"
    assert "Shuttin’ Detroit Down" in (track or "")

    lookup_track = se._normalize_title_for_mb_lookup(track or "")
    assert "music video" not in lookup_track


def test_enqueue_item_candidate_passes_parsed_music_fields_to_mb_binding(tmp_path: Path, monkeypatch):
    jq = _load_job_queue()
    captured: dict[str, object] = {}

    def _fake_resolve_best_mb_pair(
        _service,
        *,
        artist,
        track,
        album=None,
        duration_ms=None,
        **kwargs,
    ):
        _ = kwargs
        captured["artist"] = artist
        captured["track"] = track
        captured["album"] = album
        captured["duration_ms"] = duration_ms
        return {
            "recording_mbid": "mbid-1",
            "mb_release_id": "rel-1",
            "mb_release_group_id": "rg-1",
            "album": "Son of a Preacher Man",
            "release_date": "2009-01-01",
            "track_number": 1,
            "disc_number": 1,
            "duration_ms": 198000,
        }

    monkeypatch.setattr(jq, "resolve_best_mb_pair", _fake_resolve_best_mb_pair)
    monkeypatch.setattr(jq, "get_musicbrainz_service", lambda: object())
    se = _load_search_engine(jq)

    service = se.SearchResolutionService(
        search_db_path=str(tmp_path / "search.sqlite"),
        queue_db_path=str(tmp_path / "queue.sqlite"),
        adapters={},
        config={},
        paths=SimpleNamespace(single_downloads_dir=str(tmp_path / "downloads")),
        canonical_resolver=SimpleNamespace(resolve_track=lambda *a, **k: None),
    )

    request_id = service.store.create_request(
        {
            "created_by": "test",
            "intent": "track",
            "media_type": "music",
            "artist": "John Rich",
            "track": "John Rich",
            "destination_dir": None,
            "include_albums": 1,
            "include_singles": 1,
            "min_match_score": 0.92,
            "duration_hint_sec": None,
            "quality_min_bitrate_kbps": None,
            "lossless_only": 0,
            "auto_enqueue": 0,
            "source_priority_json": json.dumps(["youtube_music"]),
            "max_candidates_per_source": 5,
        }
    )
    req_row = service.store.get_request_row(request_id)
    service.store.create_items_for_request(req_row)
    item_id = service.store.list_items(request_id)[0]["id"]
    service.store.insert_candidates(
        item_id,
        [
            {
                "id": "cand-1",
                "source": "youtube",
                "url": "https://www.youtube.com/watch?v=abc123xyz00",
                "title": "John Rich - Shuttin’ Detroit Down [Music Video]",
                "uploader": "JohnRichVEVO",
                "duration_sec": 198,
                "score": 0.95,
                "final_score": 0.95,
                "canonical_json": None,
            }
        ],
    )
    candidate_id = service.store.list_candidates(item_id)[0]["id"]
    result = service.enqueue_item_candidate(item_id, candidate_id, final_format_override="mp3")
    assert result and result.get("job_id")

    assert captured.get("artist") == "John Rich"
    assert "shuttin" in str(captured.get("track") or "").lower()
    assert "music video" not in str(captured.get("track") or "").lower()
    assert captured.get("album") in (None, "")
    assert captured.get("duration_ms") == 198000

