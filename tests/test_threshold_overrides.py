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


def _release_payload(release_id: str, *, recording_mbid: str):
    return {
        "release": {
            "id": release_id,
            "title": "Album",
            "country": "GB",
            "date": "2012-01-01",
            "status": "Official",
            "release-group": {
                "id": "rg-1",
                "primary-type": "Album",
                "secondary-type-list": [],
            },
            "medium-list": [
                {
                    "position": "1",
                    "track-list": [
                        {
                            "position": "1",
                            "recording": {"id": recording_mbid, "length": "198000"},
                        }
                    ],
                }
            ],
        }
    }


def _load_binding_module():
    return _load_module("engine_musicbrainz_binding_threshold_tests", _ROOT / "engine" / "musicbrainz_binding.py")


def test_mb_binding_threshold_high_rejects_borderline_candidate():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "rec-1",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "198000",
                    "artist-credit": [{"name": "Artist"}],
                }
            ]
        },
        recording_payloads={
            "rec-1": {
                "recording": {
                    "id": "rec-1",
                    "length": "198000",
                    "release-list": [{"id": "rel-1", "date": "2012-01-01"}],
                }
            }
        },
        release_payloads={"rel-1": _release_payload("rel-1", recording_mbid="rec-1")},
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album=None,
        duration_ms=210000,  # delta=12s => lower duration similarity but still valid
        country_preference="US",
        threshold=0.95,
    )
    assert selected is None
    reasons = getattr(binding.resolve_best_mb_pair, "last_failure_reasons", [])
    assert "mb_binding_below_threshold" in reasons


def test_mb_binding_threshold_low_accepts_same_borderline_candidate():
    binding = _load_binding_module()
    mb = _FakeMBService(
        recordings_payload={
            "recording-list": [
                {
                    "id": "rec-1",
                    "title": "Song",
                    "ext:score": "99",
                    "length": "198000",
                    "artist-credit": [{"name": "Artist"}],
                }
            ]
        },
        recording_payloads={
            "rec-1": {
                "recording": {
                    "id": "rec-1",
                    "length": "198000",
                    "release-list": [{"id": "rel-1", "date": "2012-01-01"}],
                }
            }
        },
        release_payloads={"rel-1": _release_payload("rel-1", recording_mbid="rec-1")},
    )
    selected = binding.resolve_best_mb_pair(
        mb,
        artist="Artist",
        track="Song",
        album=None,
        duration_ms=210000,
        country_preference="US",
        threshold=0.70,
    )
    assert selected is not None
    assert selected["recording_mbid"] == "rec-1"
    assert selected["mb_release_id"] == "rel-1"

