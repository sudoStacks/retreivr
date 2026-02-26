from __future__ import annotations

import importlib.util
from pathlib import Path


_MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "music_hard_negative_miner.py"
_SPEC = importlib.util.spec_from_file_location("music_hard_negative_miner", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MINER = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MINER)


def test_build_fixture_stub_payload_from_run_summary_track() -> None:
    summary = {
        "run_type": "music_album",
        "per_track": [
            {
                "track_id": "rec-1",
                "wrong_variant_flag": True,
                "failure_reason": "duration_filtered",
                "decision_edge": {
                    "selected_candidate_variant_tags": ["lyric_video"],
                    "top_rejected_variant_tags": ["remaster", "live"],
                    "rejected_candidates": [
                        {"candidate_id": "cand-1", "source": "youtube", "title": "Song (Remaster)"},
                        {"candidate_id": "cand-2", "source": "youtube_music", "title": "Song (Live)"},
                    ],
                    "accepted_selection": {
                        "selected_candidate_id": "cand-accepted",
                        "top_supporting_features": {"artist_similarity": 0.25},
                    },
                },
            }
        ],
    }

    output = _MINER.build_fixture_stub_payload(summary, fixture_prefix="hn", max_candidates=3)
    assert "hn_001" in output["generated_fixtures"]
    fixture = output["generated_fixtures"]["hn_001"]
    assert fixture["expect_match"] is False
    assert len(fixture["rungs"][0]) == 3
    motifs = output["top_recurring_failure_motifs"]
    assert motifs["wrong_variant_accept"] == 1
    assert motifs["wrong_artist_accept"] == 1
    assert motifs["duration_drift"] == 1
    assert motifs["remaster_rejection"] == 1
    assert motifs["live_rejection"] == 1


def test_build_fixture_stub_payload_accepts_wrapped_summary_and_failure_motifs() -> None:
    wrapped = {
        "summary": {
            "dataset_name": "music-bench",
            "failure_motifs": {"wrong_artist_accept": 3, "duration_drift": 2},
            "per_track": [],
        }
    }

    output = _MINER.build_fixture_stub_payload(wrapped)
    assert output["source_dataset_name"] == "music-bench"
    assert output["generated_fixtures"] == {}
    assert output["top_recurring_failure_motifs"] == {
        "wrong_artist_accept": 3,
        "duration_drift": 2,
    }
