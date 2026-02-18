from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest

engine_pkg = ModuleType("engine")
engine_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "engine")]
sys.modules.setdefault("engine", engine_pkg)

google_mod = ModuleType("google")
google_auth_mod = ModuleType("google.auth")
google_auth_ex_mod = ModuleType("google.auth.exceptions")
setattr(google_auth_ex_mod, "RefreshError", Exception)
sys.modules.setdefault("google", google_mod)
sys.modules.setdefault("google.auth", google_auth_mod)
sys.modules.setdefault("google.auth.exceptions", google_auth_ex_mod)

pytest.importorskip("musicbrainzngs")

from metadata.providers.musicbrainz import MusicBrainzMetadataProvider


def test_resolve_track_defers_release_lookup_until_best_candidate(monkeypatch) -> None:
    calls = {"search_recordings": 0, "get_release": 0}

    class _FakeService:
        def search_recordings(self, artist, title, *, album=None, limit=5):
            calls["search_recordings"] += 1
            # Two candidates from different releases; only best candidate should trigger get_release.
            return {
                "recording-list": [
                    {
                        "id": "rec-1",
                        "title": "Target Track",
                        "artist-credit": [{"artist": {"name": "Target Artist"}}],
                        "release-list": [{"id": "rel-1", "title": "Target Album", "date": "2020-01-01"}],
                    },
                    {
                        "id": "rec-2",
                        "title": "Target Track Live",
                        "artist-credit": [{"artist": {"name": "Other Artist"}}],
                        "release-list": [{"id": "rel-2", "title": "Live Album", "date": "2021-01-01"}],
                    },
                ]
            }

        def get_release(self, release_id, *, includes=None):
            calls["get_release"] += 1
            return {
                "release": {
                    "medium-list": [
                        {
                            "track-list": [
                                {
                                    "position": "3",
                                    "recording": {"id": "rec-1"},
                                }
                            ]
                        }
                    ]
                }
            }

    fake = _FakeService()
    monkeypatch.setattr("metadata.providers.musicbrainz.get_musicbrainz_service", lambda: fake)

    provider = MusicBrainzMetadataProvider(min_confidence=0.1)
    resolved = provider.resolve_track("Target Artist", "Target Track", album="Target Album")

    assert resolved is not None
    assert resolved["provider"] == "musicbrainz"
    assert resolved["track_number"] == "3"
    assert calls["search_recordings"] == 1
    assert calls["get_release"] == 1
