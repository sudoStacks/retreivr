from __future__ import annotations

import copy
import json
import random
from pathlib import Path

import pytest
import benchmarks.music_search_benchmark_runner as benchmark_runner

from benchmarks.music_search_benchmark_runner import (
    TrackRunResult,
    _classify_missing_hint,
    evaluate_gate,
    run_benchmark,
)


def _dataset() -> dict:
    return {
        "dataset_name": "unit",
        "fixtures": {
            "ok": {
                "expect_match": True,
                "expect_selected_candidate_id": "ok-main",
                "rungs": [
                    [
                        {
                            "candidate_id": "ok-main",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}",
                        }
                    ],
                    [],
                    [],
                    [],
                ],
            },
            "fail": {
                "expect_match": False,
                "rungs": [
                    [
                        {
                            "candidate_id": "bad-remix",
                            "source": "youtube",
                            "title": "{{track}} (Remix)",
                            "uploader": "Mixes",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://youtube.com/watch?v={{recording_mbid}}bad",
                        }
                    ],
                    [],
                    [],
                    [],
                ],
            },
        },
        "albums": [
            {
                "album_id": "a1",
                "artist": "Artist One",
                "title": "Album One",
                "release_group_mbid": "00000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "00000000-0000-4000-8000-000000000101",
                        "track": "Track One",
                        "duration_ms": 210000,
                        "fixture": "ok",
                    }
                ],
            },
            {
                "album_id": "a2",
                "artist": "Artist Two",
                "title": "Album Two",
                "release_group_mbid": "00000000-0000-4000-8000-000000000002",
                "tracks": [
                    {
                        "recording_mbid": "00000000-0000-4000-8000-000000000202",
                        "track": "Track Two",
                        "duration_ms": 210000,
                        "fixture": "fail",
                    }
                ],
            },
        ],
    }


def test_run_benchmark_returns_expected_summary_fields() -> None:
    summary = run_benchmark(_dataset())
    assert summary["albums_total"] == 2
    assert summary["tracks_total"] == 2
    assert summary["tracks_resolved"] == 1
    assert summary["completion_percent"] == 50.0
    assert "rejection_mix" in summary
    assert "per_track" in summary
    assert "retrieval_metrics" in summary
    assert summary["unresolved_classification"]["no_viable_match"] >= 1


def test_retrieval_metrics_recall_at_k_are_deterministic() -> None:
    dataset = {
        "dataset_name": "retrieval-metrics",
        "fixtures": {
            "fixture-top1": {
                "expect_match": True,
                "expect_selected_candidate_id": "cand-1",
                "rungs": [[
                    {
                        "candidate_id": "cand-1",
                        "source": "youtube_music",
                        "title": "{{track}}",
                        "uploader": "{{artist}} - Topic",
                        "artist_detected": "{{artist}}",
                        "track_detected": "{{track}}",
                        "album_detected": "{{album}}",
                        "duration_sec": 210,
                        "url": "https://music.youtube.com/watch?v={{recording_mbid}}1",
                    },
                    {
                        "candidate_id": "cand-2",
                        "source": "youtube",
                        "title": "{{track}} live",
                        "uploader": "{{artist}} Archive",
                        "artist_detected": "{{artist}}",
                        "track_detected": "{{track}}",
                        "album_detected": "{{album}}",
                        "duration_sec": 210,
                        "url": "https://youtube.com/watch?v={{recording_mbid}}2",
                    },
                ]],
            },
            "fixture-top4": {
                "expect_match": False,
                "expect_selected_candidate_id": "cand-4",
                "rungs": [[
                    {"candidate_id": "cand-a", "source": "youtube", "title": "{{track}} remix", "uploader": "Mixes", "artist_detected": "{{artist}}", "track_detected": "{{track}}", "album_detected": "{{album}}", "duration_sec": 210, "url": "https://youtube.com/watch?v={{recording_mbid}}a"},
                    {"candidate_id": "cand-b", "source": "youtube", "title": "{{track}} cover", "uploader": "Covers", "artist_detected": "Other", "track_detected": "{{track}}", "album_detected": "{{album}}", "duration_sec": 210, "url": "https://youtube.com/watch?v={{recording_mbid}}b"},
                    {"candidate_id": "cand-c", "source": "youtube", "title": "{{track}} live", "uploader": "Live", "artist_detected": "{{artist}}", "track_detected": "{{track}}", "album_detected": "{{album}}", "duration_sec": 210, "url": "https://youtube.com/watch?v={{recording_mbid}}c"},
                    {"candidate_id": "cand-4", "source": "youtube", "title": "{{track}} acoustic", "uploader": "{{artist}}", "artist_detected": "{{artist}}", "track_detected": "{{track}}", "album_detected": "{{album}}", "duration_sec": 210, "url": "https://youtube.com/watch?v={{recording_mbid}}4"},
                ]],
            },
            "fixture-miss": {
                "expect_match": False,
                "expect_selected_candidate_id": "missing-id",
                "rungs": [[
                    {"candidate_id": "cand-x", "source": "youtube", "title": "{{track}} remix", "uploader": "Mixes", "artist_detected": "{{artist}}", "track_detected": "{{track}}", "album_detected": "{{album}}", "duration_sec": 210, "url": "https://youtube.com/watch?v={{recording_mbid}}x"},
                ]],
            },
        },
        "albums": [
            {
                "album_id": "retrieval-a1",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "51000000-0000-4000-8000-000000000001",
                "tracks": [
                    {"recording_mbid": "51000000-0000-4000-8000-000000000101", "track": "Track 1", "duration_ms": 210000, "fixture": "fixture-top1"},
                    {"recording_mbid": "51000000-0000-4000-8000-000000000102", "track": "Track 2", "duration_ms": 210000, "fixture": "fixture-top4"},
                    {"recording_mbid": "51000000-0000-4000-8000-000000000103", "track": "Track 3", "duration_ms": 210000, "fixture": "fixture-miss"},
                ],
            }
        ],
    }
    first = run_benchmark(dataset)
    second = run_benchmark(dataset)
    assert first["retrieval_metrics"] == second["retrieval_metrics"]

    recall = first["retrieval_metrics"]["recall_at_k"]
    assert recall["1"]["hits"] == 1
    assert recall["3"]["hits"] == 1
    assert recall["5"]["hits"] == 2
    assert recall["10"]["hits"] == 2
    assert recall["10"]["evaluated"] == 3
    assert recall["1"]["recall_percent"] == pytest.approx(100.0 / 3.0)
    assert recall["5"]["recall_percent"] == pytest.approx(200.0 / 3.0)


def test_benchmark_decision_edge_emits_gate_margins() -> None:
    dataset = {
        "dataset_name": "decision-edge-margins",
        "fixtures": {
            "edge": {
                "expect_match": False,
                "rungs": [
                    [
                        {
                            "candidate_id": "c-variant",
                            "source": "youtube_music",
                            "title": "{{track}} (Live)",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}v",
                        },
                        {
                            "candidate_id": "c-artist",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "Unrelated Performer - Topic",
                            "artist_detected": "Unrelated Performer",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}a",
                        },
                        {
                            "candidate_id": "c-title",
                            "source": "youtube_music",
                            "title": "Different Song",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "Totally Different",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}t",
                        },
                        {
                            "candidate_id": "c-duration",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 280,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}d",
                        },
                    ]
                ],
            }
        },
        "albums": [
            {
                "album_id": "edge-a1",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "52000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "52000000-0000-4000-8000-000000000101",
                        "track": "Song",
                        "duration_ms": 210000,
                        "fixture": "edge",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    per_track = summary["per_track"][0]
    decision_edge = per_track["decision_edge"]
    rejected = decision_edge["rejected_candidates"]
    gates = {str(item.get("top_failed_gate") or "") for item in rejected}
    assert "variant_alignment" in gates
    assert "artist_similarity" in gates
    assert "title_similarity" in gates
    assert ("duration_delta_ms" in gates) or ("duration_hard_cap_ms" in gates)
    final_rejection = decision_edge["final_rejection"]
    assert isinstance(final_rejection, dict)
    margin = final_rejection.get("nearest_pass_margin") or {}
    assert "margin_to_pass" in margin
    variant_dist = decision_edge.get("candidate_variant_distribution") or {}
    assert int(variant_dist.get("live") or 0) >= 1
    assert int(variant_dist.get("remaster") or 0) == 0
    assert isinstance(decision_edge.get("selected_candidate_variant_tags"), list)
    assert isinstance(decision_edge.get("top_rejected_variant_tags"), list)
    assert "live" in set(decision_edge.get("top_rejected_variant_tags") or [])


def test_benchmark_decision_edge_margins_stable_across_candidate_shuffles() -> None:
    base_candidates = [
        {
            "candidate_id": "s-variant",
            "source": "youtube_music",
            "title": "{{track}} (Live)",
            "uploader": "{{artist}} - Topic",
            "artist_detected": "{{artist}}",
            "track_detected": "{{track}}",
            "album_detected": "{{album}}",
            "duration_sec": 210,
            "url": "https://music.youtube.com/watch?v={{recording_mbid}}sv",
        },
        {
            "candidate_id": "s-artist",
            "source": "youtube_music",
            "title": "{{track}}",
            "uploader": "Unrelated Performer - Topic",
            "artist_detected": "Unrelated Performer",
            "track_detected": "{{track}}",
            "album_detected": "{{album}}",
            "duration_sec": 210,
            "url": "https://music.youtube.com/watch?v={{recording_mbid}}sa",
        },
        {
            "candidate_id": "s-duration",
            "source": "youtube_music",
            "title": "{{track}}",
            "uploader": "{{artist}} - Topic",
            "artist_detected": "{{artist}}",
            "track_detected": "{{track}}",
            "album_detected": "{{album}}",
            "duration_sec": 260,
            "url": "https://music.youtube.com/watch?v={{recording_mbid}}sd",
        },
    ]
    reference = None
    for seed in range(10):
        shuffled = copy.deepcopy(base_candidates)
        random.Random(seed).shuffle(shuffled)
        dataset = {
            "dataset_name": f"decision-edge-stable-{seed}",
            "fixtures": {"edge": {"expect_match": False, "rungs": [shuffled]}},
            "albums": [
                {
                    "album_id": "edge-a2",
                    "artist": "Artist",
                    "title": "Album",
                    "release_group_mbid": "52000000-0000-4000-8000-000000000002",
                    "tracks": [
                        {
                            "recording_mbid": "52000000-0000-4000-8000-000000000202",
                            "track": "Song",
                            "duration_ms": 210000,
                            "fixture": "edge",
                        }
                    ],
                }
            ],
        }
        summary = run_benchmark(dataset)
        edge = summary["per_track"][0]["decision_edge"]
        normalized = {
            "final_rejection": edge.get("final_rejection"),
            "variant_distribution": edge.get("candidate_variant_distribution"),
            "top_rejected_variant_tags": edge.get("top_rejected_variant_tags"),
            "rejected": sorted(
                [
                    (
                        str(item.get("candidate_id") or ""),
                        str(item.get("top_failed_gate") or ""),
                        float(((item.get("nearest_pass_margin") or {}).get("margin_to_pass")) or 0.0),
                    )
                    for item in (edge.get("rejected_candidates") or [])
                ]
            ),
        }
        if reference is None:
            reference = normalized
            continue
        assert normalized == reference


def test_gate_passes_and_fails_as_expected() -> None:
    summary = run_benchmark(_dataset())
    passing_gate = {
        "baseline": {"completion_percent": 50.0, "wrong_variant_flags": 0},
        "tolerance": {"max_completion_drop_pct_points": 0.5, "max_wrong_variant_increase": 0},
    }
    ok, failures = evaluate_gate(summary, passing_gate)
    assert ok is True
    assert failures == []

    failing_gate = {
        "baseline": {"completion_percent": 90.0, "wrong_variant_flags": 0},
        "tolerance": {"max_completion_drop_pct_points": 0.0, "max_wrong_variant_increase": 0},
    }
    ok, failures = evaluate_gate(summary, failing_gate)
    assert ok is False
    assert failures


def test_album_coherence_can_recover_near_threshold_track() -> None:
    dataset = {
        "dataset_name": "coherence-unit",
        "fixtures": {
            "seed": {
                "expect_match": True,
                "expect_selected_candidate_id": "seed-a",
                "rungs": [
                    [
                        {
                            "candidate_id": "seed-a",
                            "source": "youtube",
                            "title": "{{track}}",
                            "uploader": "{{artist}} Official",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 200,
                            "channel_id": "chan-a",
                            "url": "https://youtube.com/watch?v={{recording_mbid}}seed",
                        }
                    ],
                    [],
                    [],
                    [],
                ],
            },
            "near-threshold": {
                "expect_match": True,
                "expect_selected_candidate_id": "family-a",
                "rungs": [
                    [
                        {
                            "candidate_id": "family-a",
                            "source": "youtube",
                            "title": "{{track}} (Official Video)",
                            "uploader": "Archive Channel",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 208,
                            "official": False,
                            "channel_id": "chan-a",
                            "url": "https://youtube.com/watch?v={{recording_mbid}}a",
                        },
                        {
                            "candidate_id": "family-b",
                            "source": "youtube",
                            "title": "{{track}} (Official Video)",
                            "uploader": "Mirror Channel",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 208,
                            "official": False,
                            "channel_id": "chan-b",
                            "url": "https://youtube.com/watch?v={{recording_mbid}}b",
                        },
                    ],
                    [],
                    [],
                    [],
                ],
            },
        },
        "albums": [
            {
                "album_id": "coh-a1",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "10000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "10000000-0000-4000-8000-000000000101",
                        "track": "Seed Song",
                        "duration_ms": 200000,
                        "fixture": "seed",
                    },
                    {
                        "recording_mbid": "10000000-0000-4000-8000-000000000102",
                        "track": "Deep Cut",
                        "duration_ms": 200000,
                        "fixture": "near-threshold",
                    },
                ],
            }
        ],
    }
    without_coherence = run_benchmark(dataset, enable_album_coherence=False)
    with_coherence = run_benchmark(dataset, enable_album_coherence=True)

    assert without_coherence["tracks_resolved"] == 1
    assert with_coherence["tracks_resolved"] == 2
    assert with_coherence["wrong_variant_flags"] <= without_coherence["wrong_variant_flags"]


def test_alias_matching_reduces_low_title_similarity_failures_mb_case() -> None:
    dataset = {
        "dataset_name": "alias-unit",
        "fixtures": {
            "alias": {
                "expect_match": True,
                "expect_selected_candidate_id": "alias-ok",
                "rungs": [
                    [
                        {
                            "candidate_id": "alias-ok",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "Song Pt 2",
                            "album_detected": "{{album}}",
                            "duration_sec": 200,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}",
                        }
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "alias-a1",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "20000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "20000000-0000-4000-8000-000000000101",
                        "track": "Song Part II",
                        "track_aliases": ["Song Pt 2"],
                        "track_disambiguation": "Album Version",
                        "duration_ms": 200000,
                        "fixture": "alias",
                    }
                ],
            }
        ],
    }
    without_alias = run_benchmark(dataset, enable_alias_matching=False)
    with_alias = run_benchmark(dataset, enable_alias_matching=True)

    assert without_alias["tracks_resolved"] == 0
    assert with_alias["tracks_resolved"] == 1
    assert int((with_alias.get("rejection_mix") or {}).get("low_title_similarity") or 0) <= int(
        (without_alias.get("rejection_mix") or {}).get("low_title_similarity") or 0
    )


def _hint_labels(summary: dict) -> dict[str, str]:
    why_missing = summary.get("why_missing") or {}
    tracks = why_missing.get("tracks") or []
    out = {}
    for item in tracks:
        if not isinstance(item, dict):
            continue
        out[str(item.get("track_id") or "")] = str(item.get("hint_label") or "")
    return out


def test_why_missing_hint_recoverable_by_ladder_extension_no_candidates() -> None:
    dataset = {
        "dataset_name": "hint-ladder",
        "fixtures": {"none": {"expect_match": False, "rungs": [[], [], [], [], [], []]}},
        "albums": [
            {
                "album_id": "hint-a1",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "30000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "30000000-0000-4000-8000-000000000101",
                        "track": "Missing Song",
                        "duration_ms": 210000,
                        "fixture": "none",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    labels = _hint_labels(summary)
    assert labels["30000000-0000-4000-8000-000000000101"] == "Recoverable by ladder extension (no candidates)"


def test_why_missing_hint_recoverable_by_alias_matching_low_title_similarity() -> None:
    dataset = {
        "dataset_name": "hint-alias",
        "fixtures": {
            "alias-case": {
                "expect_match": False,
                "rungs": [
                    [
                        {
                            "candidate_id": "alias-hit",
                            "source": "youtube_music",
                            "title": "Great Gig",
                            "uploader": "Pink Floyd - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "Great Gig",
                            "album_detected": "{{album}}",
                            "duration_sec": 276,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}alias",
                        }
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "hint-a2",
                "artist": "Pink Floyd",
                "title": "The Dark Side of the Moon",
                "release_group_mbid": "30000000-0000-4000-8000-000000000002",
                "tracks": [
                    {
                        "recording_mbid": "30000000-0000-4000-8000-000000000201",
                        "track": "The Great Gig in the Sky",
                        "duration_ms": 276000,
                        "fixture": "alias-case",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset, enable_alias_matching=False)
    labels = _hint_labels(summary)
    assert labels["30000000-0000-4000-8000-000000000201"] == "Recoverable by alias matching (low title sim)"


def test_why_missing_hint_recoverable_by_coherence_near_miss() -> None:
    dataset = {
        "dataset_name": "hint-coherence",
        "fixtures": {
            "seed": {
                "expect_match": True,
                "expect_selected_candidate_id": "seed-a",
                "rungs": [
                    [
                        {
                            "candidate_id": "seed-a",
                            "source": "youtube",
                            "title": "{{track}}",
                            "uploader": "{{artist}} Official",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 200,
                            "channel_id": "chan-a",
                            "url": "https://youtube.com/watch?v={{recording_mbid}}seed",
                        }
                    ],
                    [],
                    [],
                    [],
                ],
            },
            "near-threshold": {
                "expect_match": False,
                "rungs": [
                    [
                        {
                            "candidate_id": "family-a",
                            "source": "youtube",
                            "title": "{{track}} (Official Video)",
                            "uploader": "Archive Channel",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 208,
                            "official": False,
                            "channel_id": "chan-a",
                            "url": "https://youtube.com/watch?v={{recording_mbid}}a",
                        },
                        {
                            "candidate_id": "family-b",
                            "source": "youtube",
                            "title": "{{track}} (Official Video)",
                            "uploader": "Mirror Channel",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 208,
                            "official": False,
                            "channel_id": "chan-b",
                            "url": "https://youtube.com/watch?v={{recording_mbid}}b",
                        },
                    ],
                    [],
                    [],
                    [],
                ],
            },
        },
        "albums": [
            {
                "album_id": "hint-a3",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "30000000-0000-4000-8000-000000000003",
                "tracks": [
                    {
                        "recording_mbid": "30000000-0000-4000-8000-000000000301",
                        "track": "Seed Song",
                        "duration_ms": 200000,
                        "fixture": "seed",
                    },
                    {
                        "recording_mbid": "30000000-0000-4000-8000-000000000302",
                        "track": "Deep Cut",
                        "duration_ms": 200000,
                        "fixture": "near-threshold",
                    },
                ],
            }
        ],
    }
    summary = run_benchmark(dataset, enable_album_coherence=False)
    labels = _hint_labels(summary)
    assert labels["30000000-0000-4000-8000-000000000302"] == "Recoverable by coherence (near-miss tie-break)"


def test_why_missing_hint_unavailable_and_wrong_length() -> None:
    dataset = {
        "dataset_name": "hint-unavailable-duration",
        "fixtures": {
            "unavailable": {
                "expect_match": False,
                "failure_reason_override": "source_unavailable:removed_or_deleted",
                "rungs": [[], [], [], [], [], []],
            },
            "duration-mismatch": {
                "expect_match": False,
                "rungs": [
                    [
                        {
                            "candidate_id": "dur-1",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 320,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}dur1",
                        },
                        {
                            "candidate_id": "dur-2",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 330,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}dur2",
                        },
                        {
                            "candidate_id": "dur-3",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 340,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}dur3",
                        },
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            },
        },
        "albums": [
            {
                "album_id": "hint-a4",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "30000000-0000-4000-8000-000000000004",
                "tracks": [
                    {
                        "recording_mbid": "30000000-0000-4000-8000-000000000401",
                        "track": "Blocked Song",
                        "duration_ms": 210000,
                        "fixture": "unavailable",
                    },
                    {
                        "recording_mbid": "30000000-0000-4000-8000-000000000402",
                        "track": "Wrong Length Song",
                        "duration_ms": 210000,
                        "fixture": "duration-mismatch",
                    },
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    labels = _hint_labels(summary)
    assert labels["30000000-0000-4000-8000-000000000401"] == "Unavailable (blocked/removed)"
    assert labels["30000000-0000-4000-8000-000000000402"] == (
        "Likely wrong MB recording length (duration mismatch persistent across many candidates)"
    )


def test_mb_relationship_injection_improves_legacy_album_completion_without_precision_regression() -> None:
    dataset = {
        "dataset_name": "legacy-mb-injection",
        "fixtures": {
            "legacy-track": {
                "expect_match": True,
                "expect_selected_candidate_id": "mb-rel-hit",
                "mb_injected_candidates": [
                    {
                        "candidate_id": "mb-rel-hit",
                        "source": "mb_relationship",
                        "title": "{{track}}",
                        "uploader": "{{artist}} - Topic",
                        "artist_detected": "{{artist}}",
                        "track_detected": "{{track}}",
                        "album_detected": "{{album}}",
                        "duration_sec": 210,
                        "url": "https://www.youtube.com/watch?v={{recording_mbid}}mb",
                    }
                ],
                "rungs": [[], [], [], [], [], []],
            }
        },
        "albums": [
            {
                "album_id": "brooks-dunn-like-case",
                "artist": "Brooks & Dunn",
                "title": "Legacy Hits",
                "release_group_mbid": "40000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "40000000-0000-4000-8000-000000000101",
                        "track": "Red Dirt Road",
                        "duration_ms": 210000,
                        "fixture": "legacy-track",
                    }
                ],
            }
        ],
    }

    without_injection = run_benchmark(dataset, enable_mb_relationship_injection=False)
    with_injection = run_benchmark(dataset, enable_mb_relationship_injection=True)

    assert without_injection["tracks_resolved"] == 0
    assert with_injection["tracks_resolved"] == 1
    assert with_injection["wrong_variant_flags"] <= without_injection["wrong_variant_flags"]
    assert int(with_injection.get("mb_injected_success_total") or 0) == 1


def test_why_missing_hint_no_candidates_maps_to_ladder_extension() -> None:
    summary = run_benchmark(
        {
            "dataset_name": "hints-ladder",
            "fixtures": {"empty": {"expect_match": False, "rungs": [[], [], [], [], [], []]}},
            "albums": [
                {
                    "album_id": "a1",
                    "artist": "Artist",
                    "title": "Album",
                    "release_group_mbid": "30000000-0000-4000-8000-000000000001",
                    "tracks": [
                        {
                            "recording_mbid": "30000000-0000-4000-8000-000000000101",
                            "track": "Missing",
                            "duration_ms": 200000,
                            "fixture": "empty",
                        }
                    ],
                }
            ],
        }
    )
    hint_counts = (summary.get("why_missing") or {}).get("hint_counts") or {}
    assert int(hint_counts.get("Recoverable by ladder extension (no candidates)") or 0) == 1


def test_why_missing_hint_alias_maps_from_low_title_similarity() -> None:
    dataset = {
        "dataset_name": "hints-alias",
        "fixtures": {
            "alias-case": {
                "expect_match": False,
                "rungs": [
                    [
                        {
                            "candidate_id": "alias-hit",
                            "source": "youtube_music",
                            "title": "Great Gig",
                            "uploader": "Pink Floyd - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "Great Gig",
                            "album_detected": "{{album}}",
                            "duration_sec": 276,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}alias",
                        }
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "alias-a1",
                "artist": "Pink Floyd",
                "title": "The Dark Side of the Moon",
                "release_group_mbid": "30000000-0000-4000-8000-000000000002",
                "tracks": [
                    {
                        "recording_mbid": "30000000-0000-4000-8000-000000000102",
                        "track": "The Great Gig in the Sky",
                        "track_aliases": ["Great Gig"],
                        "duration_ms": 276000,
                        "fixture": "alias-case",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset, enable_alias_matching=False)
    hint_counts = (summary.get("why_missing") or {}).get("hint_counts") or {}
    assert int(hint_counts.get("Recoverable by alias matching (low title sim)") or 0) == 1


def test_why_missing_hint_unavailable_maps_from_failure_reason() -> None:
    code, label, evidence = _classify_missing_hint(
        TrackRunResult(
            album_id="a1",
            track_id="t1",
            resolved=False,
            selected_pass=None,
            selected_rung=None,
            selected_candidate_id=None,
            selected_score=None,
            wrong_variant_flag=False,
            failure_reason="source_unavailable:removed_or_deleted",
            rejection_counts={},
            avg_candidate_score=None,
            candidate_count=1,
            expected_selected_candidate_id=None,
            expected_match=False,
            retrieved_candidate_ids=[],
            decision_accepted_selection=None,
            decision_rejected_candidates=[],
            decision_final_rejection=None,
            decision_candidate_variant_distribution={},
            decision_selected_candidate_variant_tags=[],
            decision_top_rejected_variant_tags=[],
            coherence_boost_applied=0,
            coherence_selected_delta=0.0,
            coherence_near_miss=False,
            mb_injected_selected=False,
            mb_injected_rejection_counts={},
        )
    )
    assert code == "unavailable"
    assert label == "Unavailable (blocked/removed)"
    assert evidence.get("unavailable_class") == "removed_or_deleted"


def test_why_missing_hint_duration_mismatch_persistent() -> None:
    code, label, evidence = _classify_missing_hint(
        TrackRunResult(
            album_id="a1",
            track_id="t1",
            resolved=False,
            selected_pass=None,
            selected_rung=None,
            selected_candidate_id=None,
            selected_score=None,
            wrong_variant_flag=False,
            failure_reason="duration_filtered",
            rejection_counts={"duration_out_of_bounds": 3, "duration_over_hard_cap": 1},
            avg_candidate_score=0.2,
            candidate_count=4,
            expected_selected_candidate_id=None,
            expected_match=False,
            retrieved_candidate_ids=[],
            decision_accepted_selection=None,
            decision_rejected_candidates=[],
            decision_final_rejection=None,
            decision_candidate_variant_distribution={},
            decision_selected_candidate_variant_tags=[],
            decision_top_rejected_variant_tags=[],
            coherence_boost_applied=0,
            coherence_selected_delta=0.0,
            coherence_near_miss=False,
            mb_injected_selected=False,
            mb_injected_rejection_counts={},
        )
    )
    assert code == "likely_wrong_mb_recording_length"
    assert evidence.get("duration_filtered") == 4


def test_why_missing_hint_coherence_near_miss() -> None:
    code, label, _ = _classify_missing_hint(
        TrackRunResult(
            album_id="a1",
            track_id="t1",
            resolved=False,
            selected_pass=None,
            selected_rung=None,
            selected_candidate_id=None,
            selected_score=None,
            wrong_variant_flag=False,
            failure_reason="no_candidate_above_threshold",
            rejection_counts={},
            avg_candidate_score=0.77,
            candidate_count=3,
            expected_selected_candidate_id=None,
            expected_match=False,
            retrieved_candidate_ids=[],
            decision_accepted_selection=None,
            decision_rejected_candidates=[],
            decision_final_rejection=None,
            decision_candidate_variant_distribution={},
            decision_selected_candidate_variant_tags=[],
            decision_top_rejected_variant_tags=[],
            coherence_boost_applied=0,
            coherence_selected_delta=0.0,
            coherence_near_miss=True,
            mb_injected_selected=False,
            mb_injected_rejection_counts={},
        )
    )
    assert code == "recoverable_coherence"
    assert label == "Recoverable by coherence (near-miss tie-break)"


def test_alias_matching_reduces_low_title_similarity_failures() -> None:
    dataset = {
        "dataset_name": "alias-unit",
        "fixtures": {
            "alias-case": {
                "expect_match": True,
                "expect_selected_candidate_id": "alias-hit",
                "rungs": [
                    [
                        {
                            "candidate_id": "alias-hit",
                            "source": "youtube_music",
                            "title": "Great Gig",
                            "uploader": "Pink Floyd - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "Great Gig",
                            "album_detected": "{{album}}",
                            "duration_sec": 276,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}alias",
                        }
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "alias-a1",
                "artist": "Pink Floyd",
                "title": "The Dark Side of the Moon",
                "release_group_mbid": "20000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "20000000-0000-4000-8000-000000000101",
                        "track": "The Great Gig in the Sky",
                        "track_aliases": ["Great Gig"],
                        "duration_ms": 276000,
                        "fixture": "alias-case",
                    }
                ],
            }
        ],
    }
    without_alias = run_benchmark(dataset, enable_alias_matching=False)
    with_alias = run_benchmark(dataset, enable_alias_matching=True)

    assert without_alias["tracks_resolved"] == 0
    assert with_alias["tracks_resolved"] == 1
    assert int((with_alias.get("rejection_mix") or {}).get("low_title_similarity") or 0) <= int(
        (without_alias.get("rejection_mix") or {}).get("low_title_similarity") or 0
    )


def test_mb_relationship_injection_improves_completion_without_variant_regression() -> None:
    dataset = {
        "dataset_name": "mb-injected-unit",
        "fixtures": {
            "mb-rel": {
                "expect_match": True,
                "expect_selected_candidate_id": "mb-rel-ok",
                "mb_injected_candidates": [
                    {
                        "candidate_id": "mb-rel-ok",
                        "source": "mb_relationship",
                        "title": "{{track}}",
                        "uploader": "{{artist}} - Official",
                        "artist_detected": "{{artist}}",
                        "track_detected": "{{track}}",
                        "album_detected": "{{album}}",
                        "duration_sec": 210,
                        "url": "https://www.youtube.com/watch?v={{recording_mbid}}mb",
                    }
                ],
                "rungs": [[], [], [], [], [], []],
            }
        },
        "albums": [
            {
                "album_id": "legacy-a1",
                "artist": "Brooks & Dunn",
                "title": "The Greatest Hits Collection",
                "release_group_mbid": "50000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "50000000-0000-4000-8000-000000000101",
                        "track": "Red Dirt Road",
                        "duration_ms": 210000,
                        "fixture": "mb-rel",
                    }
                ],
            }
        ],
    }
    without_injection = run_benchmark(dataset, enable_mb_relationship_injection=False)
    with_injection = run_benchmark(dataset, enable_mb_relationship_injection=True)
    assert without_injection["tracks_resolved"] == 0
    assert with_injection["tracks_resolved"] == 1
    assert int(with_injection.get("wrong_variant_flags") or 0) <= int(without_injection.get("wrong_variant_flags") or 0)
    assert int(with_injection.get("mb_injected_success_total") or 0) == 1


def _load_album_benchmark_dataset() -> dict:
    dataset_path = Path(__file__).resolve().parents[1] / "benchmarks" / "music_search_album_dataset.json"
    return json.loads(dataset_path.read_text(encoding="utf-8"))


def _shuffle_fixture_candidates(dataset: dict, seed: int) -> dict:
    shuffled = copy.deepcopy(dataset)
    rng = random.Random(seed)
    fixtures = shuffled.get("fixtures") if isinstance(shuffled.get("fixtures"), dict) else {}
    for fixture in fixtures.values():
        if not isinstance(fixture, dict):
            continue
        rungs = fixture.get("rungs")
        if not isinstance(rungs, list):
            continue
        for rung in rungs:
            if isinstance(rung, list):
                rng.shuffle(rung)
    return shuffled


def _selected_map(summary: dict) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for item in summary.get("per_track") or []:
        if not isinstance(item, dict):
            continue
        track_id = str(item.get("track_id") or "")
        out[track_id] = str(item.get("selected_candidate_id") or "") or None
    return out


def test_fixture_candidate_order_shuffle_does_not_change_benchmark_outcome() -> None:
    dataset = _load_album_benchmark_dataset()
    baseline = run_benchmark(dataset)
    baseline_selected = _selected_map(baseline)

    for seed in (3, 7, 13, 23, 42):
        shuffled_summary = run_benchmark(_shuffle_fixture_candidates(dataset, seed))
        assert shuffled_summary["tracks_resolved"] == baseline["tracks_resolved"]
        assert shuffled_summary["completion_percent"] == baseline["completion_percent"]
        assert shuffled_summary["wrong_variant_flags"] == baseline["wrong_variant_flags"]
        assert _selected_map(shuffled_summary) == baseline_selected


def test_fixture_candidates_do_not_embed_precomputed_score_fields() -> None:
    dataset = _load_album_benchmark_dataset()
    fixtures = dataset.get("fixtures") if isinstance(dataset.get("fixtures"), dict) else {}
    forbidden = {
        "final_score",
        "score_track",
        "score_artist",
        "score_album",
        "rejection_reason",
        "coherence_delta",
        "base_final_score",
    }
    for fixture_name, fixture in fixtures.items():
        if not isinstance(fixture, dict):
            continue
        rungs = fixture.get("rungs")
        if not isinstance(rungs, list):
            continue
        for rung in rungs:
            if not isinstance(rung, list):
                continue
            for candidate in rung:
                if not isinstance(candidate, dict):
                    continue
                leaked = forbidden.intersection(candidate.keys())
                assert not leaked, f"fixture {fixture_name} candidate leaks precomputed fields: {sorted(leaked)}"


def test_variant_collision_fixture_selects_only_canonical_audio() -> None:
    dataset = {
        "dataset_name": "variant-collision-unit",
        "fixtures": {
            "variant-collision": {
                "expect_match": True,
                "expect_selected_candidate_id": "vc-official-audio",
                "rungs": [
                    [
                        {
                            "candidate_id": "vc-official-audio",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}vc0",
                        },
                        {
                            "candidate_id": "vc-lyric-video",
                            "source": "youtube",
                            "title": "{{track}} (Official Lyric Video)",
                            "uploader": "{{artist}} Official",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "Lyric Singles",
                            "duration_sec": 210,
                            "url": "https://www.youtube.com/watch?v={{recording_mbid}}vc1",
                        },
                        {
                            "candidate_id": "vc-remastered",
                            "source": "youtube_music",
                            "title": "{{track}} (Remastered 2021)",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}} Remastered 2021",
                            "album_detected": "Greatest Hits Remastered",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}vc2",
                        },
                        {
                            "candidate_id": "vc-live",
                            "source": "youtube",
                            "title": "{{track}} (Live at Wembley)",
                            "uploader": "{{artist}} Live",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://www.youtube.com/watch?v={{recording_mbid}}vc3",
                        },
                        {
                            "candidate_id": "vc-duration-intro",
                            "source": "youtube",
                            "title": "{{track}} (Official Audio)",
                            "uploader": "{{artist}} Archive",
                            "artist_detected": "Drive Collective",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 217,
                            "url": "https://www.youtube.com/watch?v={{recording_mbid}}vc4",
                        },
                        {
                            "candidate_id": "vc-wrong-artist",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "Nightshift Echo - Topic",
                            "artist_detected": "Nightshift Echo",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}vc5",
                        },
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "variant-a1",
                "artist": "Night Drive",
                "title": "Midnight Static",
                "release_group_mbid": "92000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "92000000-0000-4000-8000-000000000101",
                        "track": "City Lights",
                        "duration_ms": 210000,
                        "fixture": "variant-collision",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    assert summary["tracks_resolved"] == 1
    per_track = summary["per_track"][0]
    assert per_track["selected_candidate_id"] == "vc-official-audio"
    rejection_mix = summary.get("rejection_mix") or {}
    assert int(rejection_mix.get("disallowed_variant") or 0) >= 1
    assert int(rejection_mix.get("low_album_similarity") or 0) >= 1
    assert int(rejection_mix.get("low_artist_similarity") or 0) >= 1


def test_wrong_artist_authority_collision_prefers_correct_artist_candidate() -> None:
    dataset = {
        "dataset_name": "wrong-artist-authority-unit",
        "fixtures": {
            "collision": {
                "expect_match": True,
                "expect_selected_candidate_id": "waac-correct-lower-authority",
                "rungs": [
                    [
                        {
                            "candidate_id": "waac-wrong-high-authority",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "The Metro - Topic",
                            "artist_detected": "The Metro",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}waac0",
                        },
                        {
                            "candidate_id": "waac-correct-lower-authority",
                            "source": "youtube",
                            "title": "{{track}}",
                            "uploader": "Aurora Avenue Archive",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://www.youtube.com/watch?v={{recording_mbid}}waac1",
                        },
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "collision-a1",
                "artist": "Aurora Avenue",
                "title": "Neon Skyline",
                "release_group_mbid": "92000000-0000-4000-8000-000000000002",
                "tracks": [
                    {
                        "recording_mbid": "92000000-0000-4000-8000-000000000201",
                        "track": "Afterglow",
                        "duration_ms": 210000,
                        "fixture": "collision",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    assert summary["tracks_resolved"] == 1
    per_track = summary["per_track"][0]
    assert per_track["selected_candidate_id"] == "waac-correct-lower-authority"
    rejection_mix = summary.get("rejection_mix") or {}
    assert int(rejection_mix.get("low_artist_similarity") or 0) >= 1


def test_mb_injected_duration_failure_falls_back_to_ladder_and_reports_bucket() -> None:
    dataset = {
        "dataset_name": "mb-injected-duration-fallback",
        "fixtures": {
            "case": {
                "expect_match": True,
                "expect_selected_candidate_id": "ladder-ok",
                "mb_injected_candidates": [
                    {
                        "candidate_id": "mb-bad-dur",
                        "source": "mb_relationship",
                        "title": "{{track}}",
                        "uploader": "{{artist}} - Topic",
                        "artist_detected": "{{artist}}",
                        "track_detected": "{{track}}",
                        "album_detected": "{{album}}",
                        "duration_sec": 320,
                        "url": "https://www.youtube.com/watch?v={{recording_mbid}}mbdur",
                    }
                ],
                "rungs": [
                    [
                        {
                            "candidate_id": "ladder-ok",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}ok",
                        }
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "inj-a1",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "93000000-0000-4000-8000-000000000001",
                "tracks": [
                    {
                        "recording_mbid": "93000000-0000-4000-8000-000000000101",
                        "track": "Song",
                        "duration_ms": 210000,
                        "fixture": "case",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    assert summary["tracks_resolved"] == 1
    assert summary["per_track"][0]["selected_candidate_id"] == "ladder-ok"
    assert int((summary.get("mb_injected_rejection_mix") or {}).get("duration_fail") or 0) >= 1
    per_album = summary.get("per_album") or {}
    assert int(((per_album.get("inj-a1") or {}).get("injected_rejection_mix") or {}).get("duration_fail") or 0) >= 1


def test_mb_injected_variant_failure_falls_back_to_ladder_and_reports_bucket() -> None:
    dataset = {
        "dataset_name": "mb-injected-variant-fallback",
        "fixtures": {
            "case": {
                "expect_match": True,
                "expect_selected_candidate_id": "ladder-ok",
                "mb_injected_candidates": [
                    {
                        "candidate_id": "mb-live",
                        "source": "mb_relationship",
                        "title": "{{track}} (Live)",
                        "uploader": "{{artist}} - Topic",
                        "artist_detected": "{{artist}}",
                        "track_detected": "{{track}}",
                        "album_detected": "{{album}}",
                        "duration_sec": 210,
                        "url": "https://www.youtube.com/watch?v={{recording_mbid}}mblive",
                    }
                ],
                "rungs": [
                    [
                        {
                            "candidate_id": "ladder-ok",
                            "source": "youtube_music",
                            "title": "{{track}}",
                            "uploader": "{{artist}} - Topic",
                            "artist_detected": "{{artist}}",
                            "track_detected": "{{track}}",
                            "album_detected": "{{album}}",
                            "duration_sec": 210,
                            "url": "https://music.youtube.com/watch?v={{recording_mbid}}ok",
                        }
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                ],
            }
        },
        "albums": [
            {
                "album_id": "inj-a2",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "93000000-0000-4000-8000-000000000002",
                "tracks": [
                    {
                        "recording_mbid": "93000000-0000-4000-8000-000000000201",
                        "track": "Song",
                        "duration_ms": 210000,
                        "fixture": "case",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    assert summary["tracks_resolved"] == 1
    assert summary["per_track"][0]["selected_candidate_id"] == "ladder-ok"
    assert int((summary.get("mb_injected_rejection_mix") or {}).get("variant_blocked") or 0) >= 1
    per_album = summary.get("per_album") or {}
    assert int(((per_album.get("inj-a2") or {}).get("injected_rejection_mix") or {}).get("variant_blocked") or 0) >= 1


def test_mb_injected_unavailable_is_classified_as_unavailable_bucket() -> None:
    dataset = {
        "dataset_name": "mb-injected-unavailable",
        "fixtures": {
            "case": {
                "expect_match": False,
                "failure_reason_override": "source_unavailable:region_restricted",
                "mb_injected_candidates": [
                    {
                        "candidate_id": "mb-region-blocked",
                        "source": "mb_relationship",
                        "title": "{{track}}",
                        "uploader": "{{artist}} - Topic",
                        "artist_detected": "{{artist}}",
                        "track_detected": "{{track}}",
                        "album_detected": "{{album}}",
                        "duration_sec": 320,
                        "url": "https://www.youtube.com/watch?v={{recording_mbid}}region",
                    }
                ],
                "rungs": [[], [], [], [], [], []],
            }
        },
        "albums": [
            {
                "album_id": "inj-a3",
                "artist": "Artist",
                "title": "Album",
                "release_group_mbid": "93000000-0000-4000-8000-000000000003",
                "tracks": [
                    {
                        "recording_mbid": "93000000-0000-4000-8000-000000000301",
                        "track": "Song",
                        "duration_ms": 210000,
                        "fixture": "case",
                    }
                ],
            }
        ],
    }
    summary = run_benchmark(dataset)
    assert summary["tracks_resolved"] == 0
    assert int((summary.get("mb_injected_rejection_mix") or {}).get("unavailable") or 0) >= 1
    per_album = summary.get("per_album") or {}
    assert int(((per_album.get("inj-a3") or {}).get("injected_rejection_mix") or {}).get("unavailable") or 0) >= 1


def _assert_threshold_contract() -> None:
    assert float(benchmark_runner.PASS_B_MIN_TITLE) == 0.92, (
        "PASS_B_MIN_TITLE changed. Explicitly update benchmark gate/config contract before merging."
    )


def test_relaxing_title_similarity_threshold_trips_contract_guard(monkeypatch) -> None:
    _assert_threshold_contract()
    monkeypatch.setattr(
        benchmark_runner,
        "PASS_B_MIN_TITLE",
        float(benchmark_runner.PASS_B_MIN_TITLE) - 0.01,
    )
    with pytest.raises(AssertionError, match="PASS_B_MIN_TITLE changed"):
        _assert_threshold_contract()
