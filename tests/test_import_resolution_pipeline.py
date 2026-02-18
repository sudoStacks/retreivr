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


class FakeSearchResolutionService:
    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self._requests = {}
        self.request_payloads = []
        self._next_id = 1

    def create_search_request(self, payload):
        request_id = f"req-{self._next_id}"
        self._next_id += 1
        self.request_payloads.append(payload)
        self._requests[request_id] = {"items": [{"status": "queued"}]}
        return request_id

    def run_search_resolution_once(self, *, request_id=None, stop_event=None):
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        self._requests[request_id] = {"items": [{"status": outcome}]}
        return request_id

    def get_search_request(self, request_id):
        return self._requests.get(request_id)


def test_import_pipeline_resolved_and_unresolved_counts() -> None:
    service = FakeSearchResolutionService(["enqueued", "failed", "skipped"])
    intents = [
        TrackIntent(artist="Daft Punk", title="Harder Better Faster Stronger", album=None, raw_line="", source_format="m3u"),
        TrackIntent(artist="", title="", album=None, raw_line="Unknown Song", source_format="csv"),
        TrackIntent(artist="Massive Attack", title="Teardrop", album=None, raw_line="", source_format="json"),
    ]

    result = process_imported_tracks(intents, {"search_service": service})

    assert result.total_tracks == 3
    assert result.resolved_count == 2
    assert result.unresolved_count == 1
    assert result.enqueued_count == 1
    assert result.failed_count == 0

    # Query mapping check:
    # artist+title path keeps structured fields, while raw_line fallback maps query into artist/track.
    assert service.request_payloads[0]["artist"] == "Daft Punk"
    assert service.request_payloads[0]["track"] == "Harder Better Faster Stronger"
    assert service.request_payloads[1]["artist"] == "Unknown Song"
    assert service.request_payloads[1]["track"] == "Unknown Song"


def test_import_pipeline_resolution_exception_counts_as_failed() -> None:
    service = FakeSearchResolutionService([RuntimeError("resolver down")])
    intents = [
        TrackIntent(artist="Adele", title="Hello", album=None, raw_line="", source_format="m3u"),
    ]

    result = process_imported_tracks(intents, {"search_service": service})

    assert result.total_tracks == 1
    assert result.resolved_count == 0
    assert result.unresolved_count == 0
    assert result.enqueued_count == 0
    assert result.failed_count == 1
