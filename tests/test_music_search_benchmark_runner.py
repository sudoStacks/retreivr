from __future__ import annotations

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
    assert summary["unresolved_classification"]["no_viable_match"] >= 1


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
