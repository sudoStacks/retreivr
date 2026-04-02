from __future__ import annotations

from pathlib import Path


def test_api_main_does_not_make_raw_musicbrainzngs_request_calls() -> None:
    api_main = (Path(__file__).resolve().parents[1] / "api" / "main.py").read_text(encoding="utf-8")

    forbidden = [
        "musicbrainzngs.search_release_groups(",
        "musicbrainzngs.get_release_by_id(",
        "musicbrainzngs.get_release_group_by_id(",
        "musicbrainzngs.get_recording_by_id(",
        "musicbrainzngs.search_recordings(",
        "musicbrainzngs.search_releases(",
        "musicbrainzngs.search_artists(",
    ]
    allowed_wrapper_defs = [
        "def _mb_search_release_groups_raw(",
        "def _mb_get_release_by_id_raw(",
        "def _mb_get_release_group_by_id_raw(",
    ]

    scrubbed = api_main
    for marker in allowed_wrapper_defs:
        start = scrubbed.find(marker)
        assert start >= 0, f"expected wrapper marker missing: {marker}"
        next_def = scrubbed.find("\ndef ", start + 1)
        if next_def < 0:
            next_def = len(scrubbed)
        scrubbed = scrubbed[:start] + scrubbed[next_def:]

    offenders = [entry for entry in forbidden if entry in scrubbed]
    assert offenders == [], f"raw musicbrainzngs calls found in api/main.py: {offenders}"
