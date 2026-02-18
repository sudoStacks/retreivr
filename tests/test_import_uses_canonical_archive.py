from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

from metadata.importers.base import TrackIntent

_MODULE_PATH = Path(__file__).resolve().parent.parent / "engine" / "import_pipeline.py"
_SPEC = importlib.util.spec_from_file_location("engine_import_pipeline_for_canonical_archive_test", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
process_imported_tracks = _MODULE.process_imported_tracks


class _FakeMusicBrainzService:
    def search_recordings(self, artist, title, *, album=None, limit=5):
        return {
            "recording-list": [
                {
                    "id": "mbid-canonical-1",
                    "title": title,
                    "ext:score": "96",
                    "artist-credit": [{"name": artist}],
                    "release-list": [{"id": "release-canonical-1"}],
                }
            ]
        }


class _FakeQueueStore:
    def __init__(self):
        self.enqueued = []

    def enqueue_job(self, **kwargs):
        self.enqueued.append(kwargs)
        return "job-1", True, None


def test_import_enqueue_does_not_override_archive_paths() -> None:
    queue_store = _FakeQueueStore()
    result = process_imported_tracks(
        [
            TrackIntent(
                artist="Artist",
                title="Track",
                album="Album",
                raw_line="",
                source_format="m3u",
            )
        ],
        {
            "musicbrainz_service": _FakeMusicBrainzService(),
            "queue_store": queue_store,
        },
    )

    assert result.enqueued_count == 1
    assert len(queue_store.enqueued) == 1
    enqueued = queue_store.enqueued[0]

    # Import flow must rely on runtime canonical archive naming, not import-specific path overrides.
    assert enqueued["origin"] == "import"
    assert enqueued["media_type"] == "music"
    assert enqueued["media_intent"] == "music_track"
    assert "resolved_destination" not in enqueued
    output_template = enqueued["output_template"]
    assert "output_dir" not in output_template
    assert "filename_template" not in output_template
    assert "audio_filename_template" not in output_template
    assert "playlist_item_id" not in output_template
    assert "source_account" not in output_template
