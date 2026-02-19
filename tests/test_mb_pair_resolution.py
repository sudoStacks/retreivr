from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


_MODULE_PATH = Path(__file__).resolve().parent.parent / "engine" / "import_pipeline.py"
_SPEC = importlib.util.spec_from_file_location("engine_import_pipeline_mb_pair_tests", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

_resolve_bound_mb_pair = _MODULE._resolve_bound_mb_pair


class _FakeMB:
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
    country: str,
    date: str,
    release_group_id: str,
    recording_mbid: str,
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
                "primary-type": "Album",
            },
            "medium-list": [
                {
                    "position": "1",
                    "track-list": [
                        {
                            "position": "3",
                            "recording": {"id": recording_mbid, "length": "210000"},
                        }
                    ],
                }
            ],
        }
    }


def test_bound_mb_pair_produces_album_and_track_position():
    recording_mbid = "rec-1"
    mb = _FakeMB(
        recordings_payload={
            "recording-list": [
                {
                    "id": recording_mbid,
                    "title": "Track A",
                    "ext:score": "97",
                    "release-list": [{"id": "rel-us"}],
                }
            ]
        },
        recording_payloads={
            recording_mbid: {
                "recording": {
                    "id": recording_mbid,
                    "release-list": [{"id": "rel-us", "date": "2010-01-01"}],
                }
            }
        },
        release_payloads={
            "rel-us": _release_payload(
                "rel-us",
                title="Album A",
                country="US",
                date="2010-01-01",
                release_group_id="rg-1",
                recording_mbid=recording_mbid,
            )
        },
    )

    pair = _resolve_bound_mb_pair(
        mb,
        artist="Artist A",
        track="Track A",
        album=None,
        duration_ms=210000,
        country_preference="US",
        threshold=0.90,
    )
    assert pair is not None
    assert pair["mb_release_id"] is not None
    assert pair["mb_release_group_id"] is not None
    assert pair["album"] is not None
    assert int(pair["track_number"]) > 0


def test_bound_mb_pair_prefers_us_release_when_available():
    recording_mbid = "rec-2"
    mb = _FakeMB(
        recordings_payload={
            "recording-list": [
                {
                    "id": recording_mbid,
                    "title": "Track B",
                    "ext:score": "99",
                }
            ]
        },
        recording_payloads={
            recording_mbid: {
                "recording": {
                    "id": recording_mbid,
                    "release-list": [
                        {"id": "rel-gb", "date": "2009-01-01"},
                        {"id": "rel-us", "date": "2012-01-01"},
                    ],
                }
            }
        },
        release_payloads={
            "rel-gb": _release_payload(
                "rel-gb",
                title="Album GB",
                country="GB",
                date="2009-01-01",
                release_group_id="rg-gb",
                recording_mbid=recording_mbid,
            ),
            "rel-us": _release_payload(
                "rel-us",
                title="Album US",
                country="US",
                date="2012-01-01",
                release_group_id="rg-us",
                recording_mbid=recording_mbid,
            ),
        },
    )

    pair = _resolve_bound_mb_pair(
        mb,
        artist="Artist B",
        track="Track B",
        album=None,
        duration_ms=210000,
        country_preference="US",
        threshold=0.90,
    )
    assert pair is not None
    assert pair["mb_release_id"] == "rel-us"


def test_bound_mb_pair_prefers_matching_album_when_album_hint_present():
    recording_mbid = "rec-3"
    mb = _FakeMB(
        recordings_payload={
            "recording-list": [
                {
                    "id": recording_mbid,
                    "title": "Track C",
                    "ext:score": "98",
                    "release-list": [{"id": "rel-z"}, {"id": "rel-a"}],
                }
            ]
        },
        recording_payloads={
            recording_mbid: {
                "recording": {
                    "id": recording_mbid,
                    "release-list": [
                        {"id": "rel-z", "date": "2015-01-01"},
                        {"id": "rel-a", "date": "2015-01-01"},
                    ],
                }
            }
        },
        release_payloads={
            "rel-z": _release_payload(
                "rel-z",
                title="Expected Album",
                country="US",
                date="2015-01-01",
                release_group_id="rg-z",
                recording_mbid=recording_mbid,
            ),
            "rel-a": _release_payload(
                "rel-a",
                title="Other Album",
                country="US",
                date="2015-01-01",
                release_group_id="rg-a",
                recording_mbid=recording_mbid,
            ),
        },
    )

    pair = _resolve_bound_mb_pair(
        mb,
        artist="Artist C",
        track="Track C",
        album="Expected Album",
        duration_ms=210000,
        country_preference="US",
        threshold=0.90,
    )
    assert pair is not None
    assert pair["mb_release_id"] == "rel-z"


def test_bound_mb_pair_tie_break_is_deterministic_by_release_id():
    recording_mbid = "rec-4"
    mb = _FakeMB(
        recordings_payload={
            "recording-list": [
                {
                    "id": recording_mbid,
                    "title": "Track D",
                    "ext:score": "99",
                }
            ]
        },
        recording_payloads={
            recording_mbid: {
                "recording": {
                    "id": recording_mbid,
                    "release-list": [
                        {"id": "rel-b", "date": "2014-01-01"},
                        {"id": "rel-a", "date": "2014-01-01"},
                    ],
                }
            }
        },
        release_payloads={
            "rel-a": _release_payload(
                "rel-a",
                title="Album D",
                country="US",
                date="2014-01-01",
                release_group_id="rg-a",
                recording_mbid=recording_mbid,
            ),
            "rel-b": _release_payload(
                "rel-b",
                title="Album D",
                country="US",
                date="2014-01-01",
                release_group_id="rg-b",
                recording_mbid=recording_mbid,
            ),
        },
    )

    pair = _resolve_bound_mb_pair(
        mb,
        artist="Artist D",
        track="Track D",
        album=None,
        duration_ms=210000,
        country_preference="US",
        threshold=0.90,
    )
    assert pair is not None
    assert pair["mb_release_id"] == "rel-a"
