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


def _spy_job_payload_builder(*, config, **kwargs):
    output_template = {
        "output_dir": kwargs.get("base_dir") or "/downloads",
        "final_format": kwargs.get("final_format_override") or (config or {}).get("final_format"),
    }
    if isinstance(kwargs.get("resolved_metadata"), dict):
        output_template["canonical_metadata"] = kwargs["resolved_metadata"]
    if isinstance(kwargs.get("output_template_overrides"), dict):
        output_template.update(kwargs["output_template_overrides"])
    return {
        "origin": kwargs["origin"],
        "origin_id": kwargs["origin_id"],
        "media_type": kwargs["media_type"],
        "media_intent": kwargs["media_intent"],
        "source": kwargs["source"],
        "url": kwargs["url"],
        "input_url": kwargs.get("input_url") or kwargs["url"],
        "output_template": output_template,
        "resolved_destination": output_template.get("output_dir"),
        "canonical_id": kwargs.get("canonical_id"),
    }


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
            "app_config": {"final_format": "mkv"},
            "base_dir": "/downloads",
            "job_payload_builder": _spy_job_payload_builder,
        },
    )

    assert result.enqueued_count == 1
    assert len(queue_store.enqueued) == 1
    enqueued = queue_store.enqueued[0]

    # Import flow must rely on canonical builder output template defaults.
    assert enqueued["origin"] == "import"
    assert enqueued["media_type"] == "music"
    assert enqueued["media_intent"] == "music_track"
    assert enqueued["resolved_destination"] == "/downloads"
    output_template = enqueued["output_template"]
    assert output_template.get("output_dir") == "/downloads"
    assert output_template.get("final_format") == "mkv"
    assert output_template.get("playlist_item_id") is None
    assert output_template.get("source_account") is None
