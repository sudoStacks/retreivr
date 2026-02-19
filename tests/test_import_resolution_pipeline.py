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
        self._recording_release_ids = {}

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
        if isinstance(response, dict):
            for rec in response.get("recording-list", []) or []:
                if not isinstance(rec, dict):
                    continue
                rid = str(rec.get("id") or "").strip()
                release_list = rec.get("release-list") if isinstance(rec.get("release-list"), list) else []
                release_ids = [
                    str(item.get("id") or "").strip()
                    for item in release_list
                    if isinstance(item, dict) and str(item.get("id") or "").strip()
                ]
                if rid and release_ids:
                    self._recording_release_ids[rid] = release_ids
        return response

    def get_recording(self, recording_id, *, includes=None):
        _ = includes
        rid = str(recording_id or "").strip()
        release_ids = self._recording_release_ids.get(rid, [])
        return {
            "recording": {
                "id": rid,
                "release-list": [{"id": release_id, "date": "2011-01-01"} for release_id in release_ids],
            }
        }

    def get_release(self, release_id, *, includes=None):
        _ = includes
        release_id = str(release_id or "").strip()
        matched_recording = None
        for rec_id, release_ids in self._recording_release_ids.items():
            if release_id in release_ids:
                matched_recording = rec_id
                break
        return {
            "release": {
                "id": release_id,
                "title": "Album",
                "date": "2011-01-01",
                "status": "Official",
                "country": "US",
                "release-group": {
                    "id": f"rg-{release_id}",
                    "primary-type": "Album",
                },
                "medium-list": [
                    {
                        "position": "1",
                        "track-list": [
                            {
                                "position": "1",
                                "recording": {"id": matched_recording or "unknown-recording"},
                            }
                        ],
                    }
                ],
            }
        }


class FakeQueueStore:
    def __init__(self):
        self.enqueued = []

    def enqueue_job(self, **kwargs):
        self.enqueued.append(kwargs)
        return f"job-{len(self.enqueued)}", True, None


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
            "job_payload_builder": _spy_job_payload_builder,
            "app_config": {},
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
    assert output["mb_release_group_id"] == "rg-release-1"
    assert output["disc_number"] == 1
    assert output["track_number"] == 1
    assert output["release_date"] == "2011"
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
            "job_payload_builder": _spy_job_payload_builder,
            "app_config": {},
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
            {"recording-list": [{"id": "mbid-1", "title": "One", "ext:score": "96", "release-list": [{"id": "release-1"}]}]},
            {"recording-list": [{"id": "mbid-2", "title": "Two", "ext:score": "94", "release-list": [{"id": "release-2"}]}]},
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
            "job_payload_builder": _spy_job_payload_builder,
            "app_config": {},
        },
    )

    assert result.import_batch_id
    assert len(queue_store.enqueued) == 2
    first = queue_store.enqueued[0]["output_template"]["import_batch_id"]
    second = queue_store.enqueued[1]["output_template"]["import_batch_id"]
    assert first == result.import_batch_id
    assert second == result.import_batch_id
