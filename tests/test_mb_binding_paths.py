from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import types
from pathlib import Path
from types import SimpleNamespace

from metadata.importers.base import TrackIntent

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


def _install_engine_base_stubs():
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


class _FakeMBService:
    def __init__(self, *, recordings_payload, recording_payloads, release_payloads):
        self._recordings_payload = recordings_payload
        self._recording_payloads = recording_payloads
        self._release_payloads = release_payloads

    def search_recordings(self, artist, title, *, album=None, limit=5):
        _ = artist, title, album, limit
        return self._recordings_payload

    def get_recording(self, recording_id, *, includes=None):
        _ = includes
        return self._recording_payloads.get(recording_id, {"recording": {"id": recording_id, "release-list": []}})

    def get_release(self, release_id, *, includes=None):
        _ = includes
        return self._release_payloads.get(release_id, {"release": {"id": release_id}})


def _release_payload(
    release_id: str,
    *,
    title: str,
    country: str = "US",
    date: str = "2015-01-01",
    release_group_id: str = "rg-1",
    recording_mbid: str,
    status: str = "Official",
    primary_type: str = "Album",
    with_completeness: bool = True,
):
    release = {
        "id": release_id,
        "title": title,
        "country": country,
        "date": date if with_completeness else None,
        "status": status,
        "release-group": {
            "id": release_group_id if with_completeness else None,
            "primary-type": primary_type,
        },
        "medium-list": [
            {
                "position": "1",
                "track-list": [
                    {
                        "position": "2",
                        "recording": {"id": recording_mbid, "length": "210000"},
                    }
                ],
            }
        ],
    }
    if with_completeness:
        release["label-info-list"] = [{"label": {"name": "Label"}}]
        release["barcode"] = "1234567890123"
    return {"release": release}


def _load_binding_module():
    _install_engine_base_stubs()
    return _load_module("engine_musicbrainz_binding_tests", _ROOT / "engine" / "musicbrainz_binding.py")


def _load_job_queue_with_fake_mb(fake_mb):
    _install_engine_base_stubs()
    mb_module = types.ModuleType("metadata.services.musicbrainz_service")
    mb_module.get_musicbrainz_service = lambda: fake_mb
    sys.modules["metadata.services.musicbrainz_service"] = mb_module
    return _load_module("engine_job_queue_mb_binding_tests", _ROOT / "engine" / "job_queue.py")


def _load_import_pipeline():
    _install_engine_base_stubs()
    return _load_module("engine_import_pipeline_mb_binding_tests", _ROOT / "engine" / "import_pipeline.py")


def _load_search_engine(jq_module):
    _install_engine_base_stubs()
    sys.modules["engine.job_queue"] = jq_module
    _load_module("engine.search_adapters", _ROOT / "engine" / "search_adapters.py")
    if "metadata.canonical" not in sys.modules:
        m = types.ModuleType("metadata.canonical")
        m.CanonicalMetadataResolver = lambda config=None: SimpleNamespace(resolve_track=lambda *a, **k: None)
        sys.modules["metadata.canonical"] = m
    return _load_module("engine_search_engine_mb_binding_tests", _ROOT / "engine" / "search_engine.py")


def test_mb_binding_prefers_correct_over_complete():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {"id": "wrong-rich", "title": "Song", "ext:score": "99", "artist-credit": [{"name": "Wrong Artist"}]},
                {"id": "right-basic", "title": "Song", "ext:score": "95", "artist-credit": [{"name": "Right Artist"}]},
            ]
        },
        recording_payloads={
            "wrong-rich": {"recording": {"id": "wrong-rich", "release-list": [{"id": "rel-rich", "date": "2010-01-01"}], "isrcs": ["USAA10000001"]}},
            "right-basic": {"recording": {"id": "right-basic", "release-list": [{"id": "rel-basic", "date": "2010-01-01"}]}},
        },
        release_payloads={
            "rel-rich": _release_payload("rel-rich", title="Rich Album", recording_mbid="wrong-rich", with_completeness=True),
            "rel-basic": _release_payload("rel-basic", title="Basic Album", recording_mbid="right-basic", with_completeness=True),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Right Artist",
        track="Song",
        album=None,
        duration_ms=210000,
        country_preference="US",
    )
    assert selected is not None
    assert selected["recording_mbid"] == "right-basic"


def test_mb_binding_prefers_more_complete_when_correctness_tied():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {"id": "rec-a", "title": "Song", "ext:score": "98", "artist-credit": [{"name": "Artist"}], "isrcs": ["USAA10000002"]},
                {"id": "rec-b", "title": "Song", "ext:score": "98", "artist-credit": [{"name": "Artist"}]},
            ]
        },
        recording_payloads={
            "rec-a": {"recording": {"id": "rec-a", "release-list": [{"id": "rel-complete", "date": "2011-01-01"}], "isrcs": ["USAA10000002"]}},
            "rec-b": {"recording": {"id": "rec-b", "release-list": [{"id": "rel-thin", "date": "2011-01-01"}]}},
        },
        release_payloads={
            "rel-complete": _release_payload("rel-complete", title="Album", recording_mbid="rec-a", with_completeness=True),
            "rel-thin": _release_payload("rel-thin", title="Album", recording_mbid="rec-b", with_completeness=False),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album="Album",
        duration_ms=210000,
        country_preference="US",
    )
    assert selected is not None
    assert selected["mb_release_id"] == "rel-complete"


def test_import_mb_bound_before_enqueue():
    pipeline = _load_import_pipeline()

    mb = _FakeMBService(
        recordings_payload={"recording-list": [{"id": "mbid-1", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]}]},
        recording_payloads={"mbid-1": {"recording": {"id": "mbid-1", "length": "210000", "release-list": [{"id": "rel-1", "date": "2012-01-01"}]}}},
        release_payloads={"rel-1": _release_payload("rel-1", title="Album", recording_mbid="mbid-1")},
    )

    captured = {}

    class _Queue:
        def enqueue_job(self, **kwargs):
            captured["payload"] = kwargs
            return "job-1", True, None

    result = pipeline.process_imported_tracks(
        [TrackIntent(artist="Artist", title="Song", album="Album", raw_line="", source_format="m3u")],
        {
            "musicbrainz_service": mb,
            "queue_store": _Queue(),
            "job_payload_builder": _load_job_queue_with_fake_mb(mb).build_download_job_payload,
            "app_config": {},
        },
    )
    assert result.enqueued_count == 1
    canonical = captured["payload"]["output_template"]["canonical_metadata"]
    for key in ("recording_mbid", "mb_release_id", "mb_release_group_id", "album", "release_date", "track_number", "disc_number", "duration_ms"):
        assert canonical.get(key) not in (None, "")


def test_manual_enqueue_music_track_mb_bound_before_enqueue(tmp_path: Path):
    mb = _FakeMBService(
        recordings_payload={"recording-list": [{"id": "mbid-2", "title": "Track", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]}]},
        recording_payloads={"mbid-2": {"recording": {"id": "mbid-2", "length": "210000", "release-list": [{"id": "rel-2", "date": "2013-01-01"}]}}},
        release_payloads={"rel-2": _release_payload("rel-2", title="Album", recording_mbid="mbid-2")},
    )
    jq = _load_job_queue_with_fake_mb(mb)
    se = _load_search_engine(jq)
    queue_db = tmp_path / "queue.sqlite"
    search_db = tmp_path / "search.sqlite"
    service = se.SearchResolutionService(
        search_db_path=str(search_db),
        queue_db_path=str(queue_db),
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
            "artist": "Artist",
            "album": "Album",
            "track": "Track",
            "destination_dir": None,
            "include_albums": 1,
            "include_singles": 1,
            "min_match_score": 0.92,
            "duration_hint_sec": 210,
            "quality_min_bitrate_kbps": None,
            "lossless_only": 0,
            "auto_enqueue": 0,
            "source_priority_json": json.dumps(["youtube_music"]),
            "max_candidates_per_source": 5,
        }
    )
    req_row = service.store.get_request_row(request_id)
    service.store.create_items_for_request(req_row)
    items = service.store.list_items(request_id)
    item_id = items[0]["id"]
    service.store.insert_candidates(
        item_id,
        [
            {
                "id": "cand-1",
                "source": "youtube_music",
                "url": "https://music.youtube.com/watch?v=abc123xyz00",
                "title": "Artist - Track",
                "score": 0.95,
                "final_score": 0.95,
                "canonical_json": None,
            }
        ],
    )
    candidates = service.store.list_candidates(item_id)
    candidate_id = candidates[0]["id"]
    result = service.enqueue_item_candidate(item_id, candidate_id, final_format_override="mp3")
    assert result and result.get("job_id")

    conn = sqlite3.connect(str(queue_db))
    try:
        cur = conn.cursor()
        cur.execute("SELECT output_template FROM download_jobs WHERE id=?", (result["job_id"],))
        row = cur.fetchone()
        assert row is not None
        output_template = json.loads(row[0])
        canonical = output_template.get("canonical_metadata") or {}
        for key in ("recording_mbid", "mb_release_id", "mb_release_group_id", "album", "release_date", "track_number", "disc_number", "duration_ms"):
            assert canonical.get(key) not in (None, "")
    finally:
        conn.close()


def test_direct_url_music_mode_mb_bound_before_enqueue(tmp_path: Path, monkeypatch):
    _install_google_stubs()
    mb = _FakeMBService(
        recordings_payload={"recording-list": [{"id": "mbid-3", "title": "Track", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]}]},
        recording_payloads={"mbid-3": {"recording": {"id": "mbid-3", "length": "210000", "release-list": [{"id": "rel-3", "date": "2014-01-01"}]}}},
        release_payloads={"rel-3": _release_payload("rel-3", title="Album", recording_mbid="mbid-3")},
    )
    jq = _load_job_queue_with_fake_mb(mb)
    sys.modules["engine.job_queue"] = jq
    core = _load_module("engine_core_mb_binding_tests", _ROOT / "engine" / "core.py")

    captured = {}

    class _FakeStore:
        def __init__(self, db_path):
            _ = db_path

        def enqueue_job(self, **kwargs):
            captured["payload"] = kwargs
            return "job-1", True, None

    monkeypatch.setattr(core, "DownloadJobStore", _FakeStore)
    monkeypatch.setattr(
        core,
        "preview_direct_url",
        lambda url, config: {"title": "Artist - Track", "uploader": "Artist - Topic", "duration_sec": 210},
    )

    ok = core.run_single_download(
        {"final_format": "mp3"},
        "https://music.youtube.com/watch?v=abc123xyz00",
        paths=SimpleNamespace(
            db_path=str(tmp_path / "queue.sqlite"),
            single_downloads_dir=str(tmp_path / "downloads"),
        ),
        status=core.EngineStatus(),
        music_mode=True,
    )
    assert ok is True
    canonical = captured["payload"]["output_template"]["canonical_metadata"]
    for key in ("recording_mbid", "mb_release_id", "mb_release_group_id", "album", "release_date", "track_number", "disc_number", "duration_ms"):
        assert canonical.get(key) not in (None, "")


def test_determinism_same_inputs_same_mb_pair():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={"recording-list": [{"id": "rec-a", "title": "Song", "ext:score": "99", "artist-credit": [{"name": "Artist"}]}]},
        recording_payloads={"rec-a": {"recording": {"id": "rec-a", "release-list": [{"id": "rel-a", "date": "2014-01-01"}, {"id": "rel-b", "date": "2014-01-01"}]}}},
        release_payloads={
            "rel-a": _release_payload("rel-a", title="Album", recording_mbid="rec-a"),
            "rel-b": _release_payload("rel-b", title="Album", recording_mbid="rec-a"),
        },
    )
    first = binding.resolve_best_mb_pair(mb, artist="Artist", track="Song", album="Album", duration_ms=210000)
    second = binding.resolve_best_mb_pair(mb, artist="Artist", track="Song", album="Album", duration_ms=210000)
    assert first == second


def test_fail_fast_when_no_acceptable_mb_pair():
    mb = _FakeMBService(
        recordings_payload={"recording-list": []},
        recording_payloads={},
        release_payloads={},
    )
    jq = _load_job_queue_with_fake_mb(mb)
    try:
        jq.build_download_job_payload(
            config={},
            origin="manual",
            origin_id="x",
            media_type="music",
            media_intent="track",
            source="youtube_music",
            url="https://music.youtube.com/watch?v=abc123xyz00",
            base_dir="/downloads",
            resolved_metadata={"artist": "Artist", "track": "Song", "album": "Album", "duration_ms": 210000},
        )
    except ValueError as exc:
        assert "music_track_requires_mb_bound_metadata" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing acceptable MB pair")


def test_job_queue_binding_uses_normalized_lookup_track_without_mutating_canonical_track(monkeypatch):
    mb = _FakeMBService(recordings_payload={"recording-list": []}, recording_payloads={}, release_payloads={})
    jq = _load_job_queue_with_fake_mb(mb)
    captured = {}

    def _fake_resolve_best_mb_pair(*args, **kwargs):
        captured["track"] = kwargs.get("track")
        return {
            "recording_mbid": "mbid-x",
            "mb_release_id": "rel-x",
            "mb_release_group_id": "rg-x",
            "album": "Album",
            "release_date": "2015-01-01",
            "track_number": 1,
            "disc_number": 1,
            "duration_ms": 210000,
        }

    monkeypatch.setattr(jq, "resolve_best_mb_pair", _fake_resolve_best_mb_pair)
    payload = {
        "output_template": {
            "canonical_metadata": {
                "artist": "Artist",
                "track": "Song - Official Video [HD] (Visualizer)",
            }
        }
    }
    jq.ensure_mb_bound_music_track(payload, config={})
    assert "music video" not in (captured.get("track") or "")
    assert "official video" not in (captured.get("track") or "")
    assert "visualizer" not in (captured.get("track") or "")
    assert payload["output_template"]["canonical_metadata"]["track"] == "Song - Official Video [HD] (Visualizer)"


def test_mb_binding_rejects_30s_preview_candidate():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "preview-rec",
                    "title": "Song (Preview)",
                    "ext:score": "99",
                    "length": "30000",
                    "artist-credit": [{"name": "Artist"}],
                }
            ]
        },
        recording_payloads={
            "preview-rec": {
                "recording": {
                    "id": "preview-rec",
                    "length": "30000",
                    "release-list": [{"id": "rel-preview", "date": "2014-01-01"}],
                }
            }
        },
        release_payloads={
            "rel-preview": _release_payload("rel-preview", title="Album", recording_mbid="preview-rec"),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album="Album",
        duration_ms=210000,
        country_preference="US",
    )
    assert selected is None


def test_music_video_query_normalizes_and_binds_to_official_album_release():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "jr-song",
                    "title": "Shuttin’ Detroit Down",
                    "ext:score": "99",
                    "length": "211000",
                    "artist-credit": [{"name": "John Rich"}],
                }
            ]
        },
        recording_payloads={
            "jr-song": {
                "recording": {
                    "id": "jr-song",
                    "length": "211000",
                    "release-list": [{"id": "jr-rel", "date": "2009-01-01"}],
                }
            }
        },
        release_payloads={
            "jr-rel": _release_payload(
                "jr-rel",
                title="Son of a Preacher Man",
                recording_mbid="jr-song",
                release_group_id="jr-rg",
                date="2009-01-01",
                country="US",
                status="Official",
                primary_type="Album",
            ),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="John Rich",
        track="John Rich - Shuttin’ Detroit Down [Music Video]",
        album="Son of a Preacher Man",
        duration_ms=211000,
        country_preference="US",
    )
    assert selected is not None
    assert selected["album"] == "Son of a Preacher Man"
    assert selected["mb_release_id"] == "jr-rel"
    assert selected["mb_release_group_id"] == "jr-rg"
    assert int(selected["track_number"]) > 0
    assert int(selected["disc_number"]) > 0


def test_live_variant_without_intent_fails_binding():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "live-rec",
                    "title": "Shuttin’ Detroit Down (Live)",
                    "ext:score": "99",
                    "length": "211000",
                    "artist-credit": [{"name": "John Rich"}],
                }
            ]
        },
        recording_payloads={
            "live-rec": {
                "recording": {
                    "id": "live-rec",
                    "length": "211000",
                    "release-list": [{"id": "live-rel", "date": "2010-01-01"}],
                }
            }
        },
        release_payloads={
            "live-rel": _release_payload(
                "live-rel",
                title="Live Album",
                recording_mbid="live-rec",
                release_group_id="live-rg",
                date="2010-01-01",
                country="US",
                status="Official",
                primary_type="Album",
            ),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="John Rich",
        track="Shuttin’ Detroit Down",
        album="Son of a Preacher Man",
        duration_ms=211000,
        country_preference="US",
    )
    assert selected is None


def test_official_video_suffix_is_neutral_for_mb_binding():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "official-rec",
                    "title": "Young (Official Music Video)",
                    "ext:score": "99",
                    "length": "236000",
                    "artist-credit": [{"name": "Kenny Chesney"}],
                }
            ]
        },
        recording_payloads={
            "official-rec": {
                "recording": {
                    "id": "official-rec",
                    "length": "236000",
                    "release-list": [{"id": "official-rel", "date": "2002-01-01"}],
                }
            }
        },
        release_payloads={
            "official-rel": _release_payload(
                "official-rel",
                title="No Shoes, No Shirt, No Problems",
                recording_mbid="official-rec",
                release_group_id="official-rg",
                date="2002-01-01",
                country="US",
                status="Official",
                primary_type="Album",
            ),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Kenny Chesney",
        track="Young",
        album="No Shoes, No Shirt, No Problems",
        duration_ms=235000,
        country_preference="US",
    )
    assert selected is not None
    assert selected["recording_mbid"] == "official-rec"


def test_extended_mix_variant_is_rejected_without_intent():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "mix-rec",
                    "title": "Song (Extended Mix)",
                    "ext:score": "99",
                    "length": "210000",
                    "artist-credit": [{"name": "Artist"}],
                }
            ]
        },
        recording_payloads={
            "mix-rec": {
                "recording": {
                    "id": "mix-rec",
                    "length": "210000",
                    "release-list": [{"id": "mix-rel", "date": "2015-01-01"}],
                }
            }
        },
        release_payloads={
            "mix-rel": _release_payload(
                "mix-rel",
                title="Album",
                recording_mbid="mix-rec",
                release_group_id="mix-rg",
                date="2015-01-01",
                country="US",
                status="Official",
                primary_type="Album",
            ),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album="Album",
        duration_ms=210000,
        country_preference="US",
    )
    assert selected is None
    reasons = getattr(binding.resolve_best_mb_pair, "last_failure_reasons", [])
    assert "disallowed_variant" in reasons


def test_album_beats_compilation_when_both_pass():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "rec-album",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "210000",
                    "artist-credit": [{"name": "Artist"}],
                },
                {
                    "id": "rec-comp",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "210000",
                    "artist-credit": [{"name": "Artist"}],
                },
            ]
        },
        recording_payloads={
            "rec-album": {"recording": {"id": "rec-album", "length": "210000", "release-list": [{"id": "rel-album", "date": "2012-01-01"}]}},
            "rec-comp": {"recording": {"id": "rec-comp", "length": "210000", "release-list": [{"id": "rel-comp", "date": "2012-01-01"}]}},
        },
        release_payloads={
            "rel-album": _release_payload(
                "rel-album",
                title="Studio Album",
                recording_mbid="rec-album",
                release_group_id="rg-album",
                primary_type="Album",
            ),
            "rel-comp": {
                "release": {
                    **_release_payload(
                        "rel-comp",
                        title="Best Of",
                        recording_mbid="rec-comp",
                        release_group_id="rg-comp",
                        primary_type="Album",
                    )["release"],
                    "release-group": {
                        "id": "rg-comp",
                        "primary-type": "Album",
                        "secondary-type-list": ["Compilation"],
                    },
                }
            },
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album=None,
        duration_ms=210000,
        country_preference="US",
    )
    assert selected is not None
    assert selected["mb_release_id"] == "rel-album"
    assert selected["recording_mbid"] == "rec-album"


def test_compilation_beats_single_when_no_album_passes():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "rec-comp",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "210000",
                    "artist-credit": [{"name": "Artist"}],
                },
                {
                    "id": "rec-single",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "210000",
                    "artist-credit": [{"name": "Artist"}],
                },
            ]
        },
        recording_payloads={
            "rec-comp": {"recording": {"id": "rec-comp", "length": "210000", "release-list": [{"id": "rel-comp", "date": "2012-01-01"}]}},
            "rec-single": {"recording": {"id": "rec-single", "length": "210000", "release-list": [{"id": "rel-single", "date": "2012-01-01"}]}},
        },
        release_payloads={
            "rel-comp": {
                "release": {
                    **_release_payload(
                        "rel-comp",
                        title="Best Of",
                        recording_mbid="rec-comp",
                        release_group_id="rg-comp",
                        primary_type="Album",
                    )["release"],
                    "release-group": {
                        "id": "rg-comp",
                        "primary-type": "Album",
                        "secondary-type-list": ["Compilation"],
                    },
                }
            },
            "rel-single": _release_payload(
                "rel-single",
                title="Song (Single)",
                recording_mbid="rec-single",
                release_group_id="rg-single",
                primary_type="Single",
            ),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album=None,
        duration_ms=210000,
        country_preference="US",
        allow_non_album_fallback=True,
    )
    assert selected is not None
    assert selected["mb_release_id"] == "rel-comp"
    assert selected["recording_mbid"] == "rec-comp"


def test_single_allowed_when_no_album_or_compilation_pass():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "rec-single",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "210000",
                    "artist-credit": [{"name": "Artist"}],
                }
            ]
        },
        recording_payloads={
            "rec-single": {"recording": {"id": "rec-single", "length": "210000", "release-list": [{"id": "rel-single", "date": "2012-01-01"}]}}
        },
        release_payloads={
            "rel-single": _release_payload(
                "rel-single",
                title="Song (Single)",
                recording_mbid="rec-single",
                release_group_id="rg-single",
                primary_type="Single",
            ),
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album=None,
        duration_ms=210000,
        country_preference="US",
        allow_non_album_fallback=True,
    )
    assert selected is not None
    assert selected["mb_release_id"] == "rel-single"
    assert selected["recording_mbid"] == "rec-single"


def test_compilation_rejected_when_album_hint_mismatch():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "rec-comp",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "210000",
                    "artist-credit": [{"name": "Artist"}],
                }
            ]
        },
        recording_payloads={
            "rec-comp": {"recording": {"id": "rec-comp", "length": "210000", "release-list": [{"id": "rel-comp", "date": "2012-01-01"}]}}
        },
        release_payloads={
            "rel-comp": {
                "release": {
                    **_release_payload(
                        "rel-comp",
                        title="Greatest Hits",
                        recording_mbid="rec-comp",
                        release_group_id="rg-comp",
                        primary_type="Album",
                    )["release"],
                    "release-group": {
                        "id": "rg-comp",
                        "primary-type": "Album",
                        "secondary-type-list": ["Compilation"],
                    },
                }
            },
        },
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album="Specific Studio Album",
        duration_ms=210000,
        country_preference="US",
        allow_non_album_fallback=True,
    )
    assert selected is None
    reasons = getattr(binding.resolve_best_mb_pair, "last_failure_reasons", [])
    assert "compilation_album_mismatch" in reasons


def test_bucket_selection_determinism_same_inputs_same_selected_pair():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {"id": "rec-album", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]},
                {"id": "rec-comp", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]},
            ]
        },
        recording_payloads={
            "rec-album": {"recording": {"id": "rec-album", "length": "210000", "release-list": [{"id": "rel-album", "date": "2012-01-01"}]}},
            "rec-comp": {"recording": {"id": "rec-comp", "length": "210000", "release-list": [{"id": "rel-comp", "date": "2012-01-01"}]}},
        },
        release_payloads={
            "rel-album": _release_payload(
                "rel-album",
                title="Studio Album",
                recording_mbid="rec-album",
                release_group_id="rg-album",
                primary_type="Album",
            ),
            "rel-comp": {
                "release": {
                    **_release_payload(
                        "rel-comp",
                        title="Best Of",
                        recording_mbid="rec-comp",
                        release_group_id="rg-comp",
                        primary_type="Album",
                    )["release"],
                    "release-group": {
                        "id": "rg-comp",
                        "primary-type": "Album",
                        "secondary-type-list": ["Compilation"],
                    },
                }
            },
        },
    )
    first = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album=None,
        duration_ms=210000,
        country_preference="US",
    )
    second = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album=None,
        duration_ms=210000,
        country_preference="US",
    )
    assert first is not None and second is not None
    assert (
        first["recording_mbid"],
        first["mb_release_id"],
        first["mb_release_group_id"],
    ) == (
        second["recording_mbid"],
        second["mb_release_id"],
        second["mb_release_group_id"],
    )


def test_manual_enqueue_logs_mb_pair_selected_before_job_enqueued(tmp_path: Path, caplog):
    import logging

    mb = _FakeMBService(
        recordings_payload={"recording-list": [{"id": "mbid-log-2", "title": "Track", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]}]},
        recording_payloads={"mbid-log-2": {"recording": {"id": "mbid-log-2", "length": "210000", "release-list": [{"id": "rel-log-2", "date": "2013-01-01"}]}}},
        release_payloads={"rel-log-2": _release_payload("rel-log-2", title="Album", recording_mbid="mbid-log-2")},
    )
    jq = _load_job_queue_with_fake_mb(mb)
    se = _load_search_engine(jq)
    queue_db = tmp_path / "queue.sqlite"
    search_db = tmp_path / "search.sqlite"
    service = se.SearchResolutionService(
        search_db_path=str(search_db),
        queue_db_path=str(queue_db),
        adapters={},
        config={},
        paths=SimpleNamespace(single_downloads_dir=str(tmp_path / "downloads")),
        canonical_resolver=SimpleNamespace(resolve_track=lambda *a, **k: None),
    )
    caplog.set_level(logging.INFO)

    request_id = service.store.create_request(
        {
            "created_by": "test",
            "intent": "track",
            "media_type": "music",
            "artist": "Artist",
            "album": "Album",
            "track": "Track",
            "destination_dir": None,
            "include_albums": 1,
            "include_singles": 1,
            "min_match_score": 0.92,
            "duration_hint_sec": 210,
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
                "id": "cand-log-1",
                "source": "youtube_music",
                "url": "https://music.youtube.com/watch?v=abc123xyz00",
                "title": "Artist - Track",
                "score": 0.95,
                "final_score": 0.95,
                "canonical_json": None,
            }
        ],
    )
    candidate_id = service.store.list_candidates(item_id)[0]["id"]
    result = service.enqueue_item_candidate(item_id, candidate_id, final_format_override="mp3")
    assert result and result.get("job_id")

    lines = [rec.getMessage() for rec in caplog.records]
    selected_idx = next(i for i, line in enumerate(lines) if "mb_pair_selected" in line)
    enqueued_idx = next(i for i, line in enumerate(lines) if "job_enqueued" in line)
    assert selected_idx < enqueued_idx


def test_direct_url_enqueue_logs_mb_pair_selected_before_job_enqueued(tmp_path: Path, monkeypatch, caplog):
    import logging

    _install_google_stubs()
    mb = _FakeMBService(
        recordings_payload={"recording-list": [{"id": "mbid-log-3", "title": "Track", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]}]},
        recording_payloads={"mbid-log-3": {"recording": {"id": "mbid-log-3", "length": "210000", "release-list": [{"id": "rel-log-3", "date": "2014-01-01"}]}}},
        release_payloads={"rel-log-3": _release_payload("rel-log-3", title="Album", recording_mbid="mbid-log-3")},
    )
    jq = _load_job_queue_with_fake_mb(mb)
    sys.modules["engine.job_queue"] = jq
    core = _load_module("engine_core_mb_binding_log_tests", _ROOT / "engine" / "core.py")

    class _FakeStore:
        def __init__(self, db_path):
            _ = db_path

        def enqueue_job(self, **kwargs):
            return "job-1", True, None

    monkeypatch.setattr(core, "DownloadJobStore", _FakeStore)
    monkeypatch.setattr(
        core,
        "preview_direct_url",
        lambda url, config: {"title": "Artist - Track", "uploader": "Artist - Topic", "duration_sec": 210},
    )
    caplog.set_level(logging.INFO)

    ok = core.run_single_download(
        {"final_format": "mp3"},
        "https://music.youtube.com/watch?v=abc123xyz00",
        paths=SimpleNamespace(
            db_path=str(tmp_path / "queue.sqlite"),
            single_downloads_dir=str(tmp_path / "downloads"),
        ),
        status=core.EngineStatus(),
        music_mode=True,
    )
    assert ok is True
    lines = [rec.getMessage() for rec in caplog.records]
    selected_idx = next(i for i, line in enumerate(lines) if "mb_pair_selected" in line)
    enqueued_idx = next(i for i, line in enumerate(lines) if "job_enqueued" in line)
    assert selected_idx < enqueued_idx
