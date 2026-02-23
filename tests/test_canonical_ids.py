from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


_CANONICAL_IDS_PATH = Path(__file__).resolve().parent.parent / "engine" / "canonical_ids.py"
_SPEC = importlib.util.spec_from_file_location("engine_canonical_ids_test", _CANONICAL_IDS_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

extract_external_track_canonical_id = _MODULE.extract_external_track_canonical_id


def test_extract_external_track_canonical_id_prefers_spotify_id_then_isrc() -> None:
    assert (
        extract_external_track_canonical_id(
            {
                "spotify_id": "sp-1",
                "isrc": "USABC1234567",
            }
        )
        == "sp-1"
    )


def test_extract_external_track_canonical_id_uses_fallback_when_missing() -> None:
    assert (
        extract_external_track_canonical_id({}, fallback_spotify_id="sp-fallback")
        == "sp-fallback"
    )


def test_extract_external_track_canonical_id_returns_none_without_ids() -> None:
    assert extract_external_track_canonical_id(None) is None
