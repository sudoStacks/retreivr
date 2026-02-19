from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


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
    primary_type: str = "Album",
    secondary_types: list[str] | None = None,
):
    return {
        "release": {
            "id": release_id,
            "title": title,
            "country": country,
            "date": date,
            "status": "Official",
            "release-group": {
                "id": release_group_id,
                "primary-type": primary_type,
                "secondary-type-list": list(secondary_types or []),
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
    }


def _load_binding_module():
    return _load_module("engine_musicbrainz_binding_bucket_fallback_tests", _ROOT / "engine" / "musicbrainz_binding.py")


def test_only_single_available_succeeds_when_correctness_passes():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={"recording-list": [{"id": "rec-single", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]}]},
        recording_payloads={"rec-single": {"recording": {"id": "rec-single", "length": "210000", "release-list": [{"id": "rel-single", "date": "2015-01-01"}]}}},
        release_payloads={"rel-single": _release_payload("rel-single", title="Song", recording_mbid="rec-single", release_group_id="rg-single", primary_type="Single")},
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
    assert selected["mb_release_id"] == "rel-single"
    assert selected["mb_release_group_id"] == "rg-single"


def test_album_wins_over_us_single_when_album_non_us():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {"id": "rec-album", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]},
                {"id": "rec-single", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]},
            ]
        },
        recording_payloads={
            "rec-album": {"recording": {"id": "rec-album", "length": "210000", "release-list": [{"id": "rel-album", "date": "2014-01-01"}]}},
            "rec-single": {"recording": {"id": "rec-single", "length": "210000", "release-list": [{"id": "rel-single", "date": "2014-01-01"}]}},
        },
        release_payloads={
            "rel-album": _release_payload("rel-album", title="Studio Album", country="GB", recording_mbid="rec-album", release_group_id="rg-album", primary_type="Album"),
            "rel-single": _release_payload("rel-single", title="Song", country="US", recording_mbid="rec-single", release_group_id="rg-single", primary_type="Single"),
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
    assert first["mb_release_id"] == "rel-album"
    assert (
        first["recording_mbid"],
        first["mb_release_id"],
        first["mb_release_group_id"],
    ) == (
        second["recording_mbid"],
        second["mb_release_id"],
        second["mb_release_group_id"],
    )


def test_compilation_wins_over_single_when_album_absent():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {"id": "rec-comp", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]},
                {"id": "rec-single", "title": "Song", "ext:score": "99", "length": "210000", "artist-credit": [{"name": "Artist"}]},
            ]
        },
        recording_payloads={
            "rec-comp": {"recording": {"id": "rec-comp", "length": "210000", "release-list": [{"id": "rel-comp", "date": "2013-01-01"}]}},
            "rec-single": {"recording": {"id": "rec-single", "length": "210000", "release-list": [{"id": "rel-single", "date": "2013-01-01"}]}},
        },
        release_payloads={
            "rel-comp": _release_payload(
                "rel-comp",
                title="Best Of",
                country="US",
                recording_mbid="rec-comp",
                release_group_id="rg-comp",
                primary_type="Album",
                secondary_types=["Compilation"],
            ),
            "rel-single": _release_payload(
                "rel-single",
                title="Song",
                country="US",
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
    )
    assert selected is not None
    assert selected["mb_release_id"] == "rel-comp"

