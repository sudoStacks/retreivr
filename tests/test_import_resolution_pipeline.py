from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

from metadata.importers.base import TrackIntent

_MODULE_PATH = Path(__file__).resolve().parent.parent / "engine" / "import_pipeline.py"
_SPEC = importlib.util.spec_from_file_location("engine_import_pipeline", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
process_imported_tracks = _MODULE.process_imported_tracks


class FakeMusicBrainzService:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def search_recordings(self, artist, title, *, album=None, limit=5):
        self.calls.append(
            {
                "artist": artist,
                "title": title,
                "album": album,
                "limit": limit,
            }
        )
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeQueueStore:
    def __init__(self):
        self.enqueued = []

    def enqueue_job(self, **kwargs):
        self.enqueued.append(kwargs)
        return f"job-{len(self.enqueued)}", True, None


def test_import_pipeline_resolves_musicbrainz_and_enqueues_music_track() -> None:
    mb = FakeMusicBrainzService(
        [
            {
                "recording-list": [
                    {
                        "id": "mbid-1",
                        "title": "Harder Better Faster Stronger",
                        "ext:score": "95",
                        "artist-credit": [{"name": "Daft Punk"}],
                        "release-list": [{"id": "release-1"}],
                    }
                ]
            }
        ]
    )
    queue_store = FakeQueueStore()
    intents = [
        TrackIntent(
            artist="Daft Punk",
            title="Harder Better Faster Stronger",
            album="Discovery",
            raw_line="",
            source_format="m3u",
        )
    ]

    result = process_imported_tracks(
        intents,
        {
            "musicbrainz_service": mb,
            "queue_store": queue_store,
        },
    )

    assert result.total_tracks == 1
    assert result.resolved_count == 1
    assert result.unresolved_count == 0
    assert result.enqueued_count == 1
    assert result.failed_count == 0
    assert result.resolved_mbids == ["mbid-1"]
    assert result.import_batch_id
    assert not hasattr(result, "resolved_track_paths")

    assert len(queue_store.enqueued) == 1
    payload = queue_store.enqueued[0]
    assert payload["media_intent"] == "music_track"
    assert payload["media_type"] == "music"
    assert payload["canonical_id"] == "music_track:mbid-1"
    assert payload["url"] == "musicbrainz://recording/mbid-1"
    output = payload["output_template"]
    assert output["recording_mbid"] == "mbid-1"
    assert output["mb_release_id"] == "release-1"
    assert output["kind"] == "music_track"


def test_import_pipeline_unresolved_when_no_acceptable_musicbrainz_match() -> None:
    mb = FakeMusicBrainzService(
        [
            {
                "recording-list": [
                    {"id": "mbid-low", "title": "Hello", "ext:score": "65"},
                ]
            }
        ]
    )
    queue_store = FakeQueueStore()
    intents = [TrackIntent(artist="Adele", title="Hello", album=None, raw_line="", source_format="csv")]

    result = process_imported_tracks(
        intents,
        {
            "musicbrainz_service": mb,
            "queue_store": queue_store,
        },
    )

    assert result.total_tracks == 1
    assert result.resolved_count == 0
    assert result.unresolved_count == 1
    assert result.enqueued_count == 0
    assert result.failed_count == 0
    assert result.resolved_mbids == []
    assert len(queue_store.enqueued) == 0
    assert not hasattr(result, "resolved_track_paths")


def test_import_pipeline_uses_single_batch_id_for_all_enqueued_items() -> None:
    mb = FakeMusicBrainzService(
        [
            {"recording-list": [{"id": "mbid-1", "title": "One", "ext:score": "96"}]},
            {"recording-list": [{"id": "mbid-2", "title": "Two", "ext:score": "94"}]},
        ]
    )
    queue_store = FakeQueueStore()
    intents = [
        TrackIntent(artist="Artist", title="One", album=None, raw_line="", source_format="m3u"),
        TrackIntent(artist="Artist", title="Two", album=None, raw_line="", source_format="m3u"),
    ]

    result = process_imported_tracks(
        intents,
        {
            "musicbrainz_service": mb,
            "queue_store": queue_store,
        },
    )

    assert result.import_batch_id
    assert len(queue_store.enqueued) == 2
    first = queue_store.enqueued[0]["output_template"]["import_batch_id"]
    second = queue_store.enqueued[1]["output_template"]["import_batch_id"]
    assert first == result.import_batch_id
    assert second == result.import_batch_id
