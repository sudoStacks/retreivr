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


class _FakeMBService:
    def search_recordings(self, artist, title, *, album=None, limit=5):
        _ = artist, title, album, limit
        return {
            "recording-list": [
                {
                    "id": "rec-1",
                    "title": "Song Part II",
                    "ext:score": "99",
                    "length": "200000",
                    "artist-credit": [{"name": "Artist"}],
                }
            ]
        }

    def get_recording(self, recording_id, *, includes=None):
        _ = recording_id, includes
        return {
            "recording": {
                "id": "rec-1",
                "title": "Song Part II",
                "disambiguation": "Album Version",
                "alias-list": [{"name": "Song Pt 2"}],
                "url-relation-list": [
                    {"type": "official music video", "target": "https://www.youtube.com/watch?v=abc123xyz00&t=10"},
                    {"type": "streaming", "target": "https://example.com/not-youtube"},
                ],
                "length": "200000",
                "release-list": [{"id": "rel-1", "date": "2010-01-01"}],
            }
        }

    def get_release(self, release_id, *, includes=None):
        _ = release_id, includes
        return {
            "release": {
                "id": "rel-1",
                "title": "Album",
                "status": "Official",
                "date": "2010-01-01",
                "country": "US",
                "release-group": {
                    "id": "rg-1",
                    "primary-type": "Album",
                    "secondary-type-list": [],
                },
                "url-relation-list": [
                    {"type": "streaming", "target": "https://youtu.be/def456uvw00?si=abc"},
                ],
                "medium-list": [
                    {
                        "position": "1",
                        "track-list": [
                            {
                                "position": "3",
                                "title": "Song Pt. 2",
                                "recording": {"id": "rec-1", "title": "Song Part II"},
                            }
                        ],
                    }
                ],
            }
        }


def test_resolve_best_mb_pair_includes_title_aliases_and_disambiguation() -> None:
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    if "metadata.services" not in sys.modules:
        services_pkg = types.ModuleType("metadata.services")
        services_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["metadata.services"] = services_pkg
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_service_module = types.ModuleType("metadata.services.musicbrainz_service")
        mb_service_module.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_service_module
    binding = _load_module("engine_musicbrainz_binding_alias_tests", _ROOT / "engine" / "musicbrainz_binding.py")
    selected = binding.resolve_best_mb_pair(
        _FakeMBService(),
        artist="Artist",
        track="Song Part II",
        album="Album",
        duration_ms=200000,
        country_preference="US",
    )
    assert selected is not None
    aliases = selected.get("track_aliases") or []
    lowered = {str(value).lower() for value in aliases}
    assert "song pt 2" in lowered
    assert any("album version" in str(value).lower() for value in aliases)
    assert selected.get("mb_youtube_urls") == [
        "https://www.youtube.com/watch?v=abc123xyz00",
        "https://www.youtube.com/watch?v=def456uvw00",
    ]
