import os
import random
import tempfile
import unittest
import importlib.util
from pathlib import Path
import sys
import types

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_search_modules():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    _load_module("engine.search_adapters", _ROOT / "engine" / "search_adapters.py")
    if "engine.musicbrainz_binding" not in sys.modules:
        binding_module = types.ModuleType("engine.musicbrainz_binding")
        binding_module.resolve_best_mb_pair = lambda *args, **kwargs: None
        binding_module._normalize_title_for_mb_lookup = lambda value, **kwargs: str(value or "")
        sys.modules["engine.musicbrainz_binding"] = binding_module
    if "metadata.services" not in sys.modules:
        metadata_services_pkg = types.ModuleType("metadata.services")
        metadata_services_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["metadata.services"] = metadata_services_pkg
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_module = types.ModuleType("metadata.services.musicbrainz_service")
        mb_module.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_module
    if "metadata.canonical" not in sys.modules:
        metadata_canonical = types.ModuleType("metadata.canonical")
        metadata_canonical.CanonicalMetadataResolver = lambda config=None: _StubCanonicalResolver()
        sys.modules["metadata.canonical"] = metadata_canonical
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    jq = _load_module("engine.job_queue", _ROOT / "engine" / "job_queue.py")
    se = _load_module("engine.search_engine", _ROOT / "engine" / "search_engine.py")
    return se, sys.modules["engine.search_scoring"]


class _StubAdapter:
    def __init__(self, source, candidates):
        self.source = source
        self._candidates = [dict(c) for c in candidates]

    def search_music_track(self, query, limit=6):
        return [dict(c) for c in self._candidates[:limit]]

    def source_modifier(self, candidate):
        return 1.0


class _QueryAwareAdapter:
    def __init__(self, source, query_map):
        self.source = source
        self.query_map = {str(k): [dict(c) for c in (v or [])] for k, v in (query_map or {}).items()}
        self.calls = []

    def search_music_track(self, query, limit=6):
        q = str(query or "")
        self.calls.append(q)
        return [dict(c) for c in self.query_map.get(q, [])[:limit]]

    def source_modifier(self, candidate):
        return 1.0


class _StubCanonicalResolver:
    def resolve_track(self, artist, track, *, album=None):
        return None

    def resolve_album(self, artist, album):
        return None


def _candidate(*, source, candidate_id, title, uploader, artist, track, album, duration_sec, **extra):
    candidate = {
        "source": source,
        "candidate_id": candidate_id,
        "url": f"https://example.test/{source}/{candidate_id}",
        "title": title,
        "uploader": uploader,
        "artist_detected": artist,
        "track_detected": track,
        "album_detected": album,
        "duration_sec": duration_sec,
        "official": True,
    }
    candidate.update(extra)
    return candidate


class MusicTrackHardenedScoringTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_db = os.path.join(self.tmpdir.name, "queue.sqlite")
        self.search_db = os.path.join(self.tmpdir.name, "search.sqlite")
        self.se, self.scoring = _load_search_modules()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _service(self, adapter_candidates_by_source):
        adapters = {
            source: _StubAdapter(source, candidates)
            for source, candidates in adapter_candidates_by_source.items()
        }
        return self.se.SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters=adapters,
            config={},
            paths=None,
            canonical_resolver=_StubCanonicalResolver(),
        )

    def test_correct_match_is_accepted(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="good",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        best = service.search_music_track_best_match(
            "Artist", "Song", album="Album", duration_ms=200000, limit=6
        )
        self.assertIsNotNone(best)
        self.assertGreaterEqual(float(best.get("final_score") or 0.0), 0.78)
        self.assertFalse(best.get("rejection_reason"))

    def test_preview_30s_is_rejected(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="preview",
                        title="Artist - Song (Preview)",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=30,
                    )
                ]
            }
        )
        best = service.search_music_track_best_match(
            "Artist", "Song", album="Album", duration_ms=200000, limit=6
        )
        self.assertIsNone(best)

    def test_live_version_is_rejected(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="live",
                        title="Artist - Song (Live)",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        best = service.search_music_track_best_match(
            "Artist", "Song", album="Album", duration_ms=200000, limit=6
        )
        self.assertIsNone(best)

    def test_acoustic_version_is_rejected(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="acoustic",
                        title="Artist - Song (Acoustic)",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        best = service.search_music_track_best_match(
            "Artist", "Song", album="Album", duration_ms=200000, limit=6
        )
        self.assertIsNone(best)

    def test_remaster_tag_penalty_reduces_score(self):
        expected = {
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song" "Album" audio official topic',
        }
        plain = _candidate(
            source="youtube_music",
            candidate_id="plain",
            title="Artist - Song",
            uploader="Artist - Topic",
            artist="Artist",
            track="Song",
            album="Album",
            duration_sec=200,
        )
        remaster = _candidate(
            source="youtube_music",
            candidate_id="remaster",
            title="Artist - Song (Remastered 2020)",
            uploader="Artist - Topic",
            artist="Artist",
            track="Song",
            album="Album",
            duration_sec=200,
        )
        plain_score = self.scoring.score_candidate(expected, plain, source_modifier=1.0)
        remaster_score = self.scoring.score_candidate(expected, remaster, source_modifier=1.0)
        plain_noise = float((plain_score.get("score_breakdown") or {}).get("noise_penalty") or 0.0)
        remaster_noise = float((remaster_score.get("score_breakdown") or {}).get("noise_penalty") or 0.0)
        self.assertGreater(remaster_noise, plain_noise)

    def test_cover_artist_rejected(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="cover",
                        title="Song (Cover)",
                        uploader="Other Artist",
                        artist="Other Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        best = service.search_music_track_best_match(
            "Artist", "Song", album="Album", duration_ms=200000, limit=6
        )
        self.assertIsNone(best)

    def test_album_mismatch_penalty_reduces_score(self):
        expected = {
            "artist": "Artist",
            "track": "Song",
            "album": "Correct Album",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song" "Correct Album" audio official topic',
        }
        matched = _candidate(
            source="youtube_music",
            candidate_id="album-match",
            title="Artist - Song",
            uploader="Artist - Topic",
            artist="Artist",
            track="Song",
            album="Correct Album",
            duration_sec=200,
        )
        mismatch = _candidate(
            source="youtube_music",
            candidate_id="album-mismatch",
            title="Artist - Song",
            uploader="Artist - Topic",
            artist="Artist",
            track="Song",
            album="Different Album",
            duration_sec=200,
        )
        matched_score = self.scoring.score_candidate(expected, matched, source_modifier=1.0)
        mismatch_score = self.scoring.score_candidate(expected, mismatch, source_modifier=1.0)
        self.assertGreater(float(matched_score["final_score"]), float(mismatch_score["final_score"]))

    def test_youtube_music_preferred_when_scores_close(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="ytm",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ],
                "youtube": [
                    _candidate(
                        source="youtube",
                        candidate_id="yt",
                        title="Artist - Song",
                        uploader="Artist - Channel",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ],
            }
        )
        best = service.search_music_track_best_match(
            "Artist", "Song", album="Album", duration_ms=200000, limit=6
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("source"), "youtube_music")
        self.assertGreaterEqual(float(best.get("final_score") or 0.0), 0.78)

    def test_select_best_candidate_tie_break_order(self):
        candidates = [
            {
                "candidate_id": "c1",
                "source": "youtube",
                "final_score": 0.90,
                "duration_delta_ms": 500,
                "title_noise_score": 0.0,
            },
            {
                "candidate_id": "c2",
                "source": "youtube_music",
                "final_score": 0.90,
                "duration_delta_ms": 900,
                "title_noise_score": 10.0,
            },
            {
                "candidate_id": "c3",
                "source": "youtube_music",
                "final_score": 0.90,
                "duration_delta_ms": 300,
                "title_noise_score": 4.0,
            },
            {
                "candidate_id": "c4",
                "source": "youtube_music",
                "final_score": 0.90,
                "duration_delta_ms": 300,
                "title_noise_score": 1.0,
            },
        ]
        best = self.scoring.select_best_candidate(
            candidates,
            0.78,
            source_priority=["youtube_music", "youtube", "soundcloud", "bandcamp"],
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "c4")

    def test_search_engine_candidate_order_invariance_across_shuffles(self):
        base_candidates = [
            _candidate(
                source="youtube_music",
                candidate_id="best-canonical",
                title="Artist - Song",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube_music",
                candidate_id="variant-live",
                title="Artist - Song (Live)",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube",
                candidate_id="wrong-artist",
                title="Artist - Song",
                uploader="Other Artist Official",
                artist="Other Artist",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube",
                candidate_id="bad-duration",
                title="Artist - Song",
                uploader="Artist Archive",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=320,
            ),
        ]

        expected_selected_id = None
        expected_accepted = None
        expected_failure_reason = None
        expected_score = None

        for seed in range(10):
            shuffled = [dict(c) for c in base_candidates]
            random.Random(seed).shuffle(shuffled)
            service = self._service({"youtube_music": shuffled})
            best = service.search_music_track_best_match(
                "Artist",
                "Song",
                album="Album",
                duration_ms=200000,
                limit=6,
            )
            meta = getattr(service, "last_music_track_search", {}) or {}
            selected_id = best.get("candidate_id") if isinstance(best, dict) else None
            accepted = best is not None
            failure_reason = str(meta.get("failure_reason") or "") or None
            final_score = float(best.get("final_score") or 0.0) if isinstance(best, dict) else None

            if seed == 0:
                expected_selected_id = selected_id
                expected_accepted = accepted
                expected_failure_reason = failure_reason
                expected_score = final_score
                continue

            self.assertEqual(selected_id, expected_selected_id)
            self.assertEqual(accepted, expected_accepted)
            self.assertEqual(failure_reason, expected_failure_reason)
            if expected_score is None:
                self.assertIsNone(final_score)
            else:
                self.assertIsNotNone(final_score)
                self.assertAlmostEqual(float(final_score), float(expected_score), places=12)

    def test_rank_and_gate_matches_end_to_end_selection_for_same_candidate_list(self):
        adapter_candidates = [
            _candidate(
                source="youtube_music",
                candidate_id="best-canonical",
                title="Artist - Song",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube_music",
                candidate_id="variant-live",
                title="Artist - Song (Live)",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube",
                candidate_id="bad-duration",
                title="Artist - Song",
                uploader="Artist Archive",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=320,
            ),
        ]
        service = self._service({"youtube_music": adapter_candidates, "youtube": []})
        ladder = service._build_music_track_query_ladder("Artist", "Song", "Album")
        first = dict(ladder[0])
        expected_base = {
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
            "query": first.get("query"),
            "media_intent": "music_track",
            "duration_hint_sec": 200,
            "duration_hard_cap_ms": 35000,
            "variant_allow_tokens": set(),
            "track_aliases": [],
            "track_disambiguation": None,
        }
        retrieved = service.retrieve_candidates(
            {
                "query": first.get("query"),
                "limit": 6,
                "query_label": first.get("label"),
                "rung": int(first.get("rung") or 0),
                "first_rung": int(first.get("rung") or 0),
                "mb_injected_candidates": [],
            }
        )
        direct = service.rank_and_gate(
            {
                "expected_base": expected_base,
                "coherence_key": None,
                "query_label": first.get("label"),
                "rung": int(first.get("rung") or 0),
                "recording_mbid": "rec-direct",
            },
            retrieved,
        )

        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
        )
        meta = getattr(service, "last_music_track_search", {}) or {}
        direct_selected_id = direct.selected.get("candidate_id") if isinstance(direct.selected, dict) else None
        best_selected_id = best.get("candidate_id") if isinstance(best, dict) else None
        self.assertEqual(direct_selected_id, best_selected_id)
        self.assertEqual(direct.selected_pass, meta.get("selected_pass"))
        self.assertEqual(str(direct.failure_reason or "") or None, str(meta.get("failure_reason") or "") or None)
        if isinstance(best, dict) and isinstance(direct.selected, dict):
            self.assertAlmostEqual(
                float(best.get("final_score") or 0.0),
                float(direct.selected.get("final_score") or 0.0),
                places=12,
            )

    def test_rank_and_gate_emits_decision_edge_margins_for_core_gates(self):
        candidates = [
            _candidate(
                source="youtube_music",
                candidate_id="gate-variant",
                title="Artist - Song (Live)",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube_music",
                candidate_id="gate-artist",
                title="Artist - Song",
                uploader="Unrelated Performer Official",
                artist="Unrelated Performer",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube_music",
                candidate_id="gate-title",
                title="Artist - Different Song",
                uploader="Artist - Topic",
                artist="Artist",
                track="Totally Different",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube_music",
                candidate_id="gate-duration",
                title="Artist - Song",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=260,
            ),
        ]
        service = self._service({"youtube_music": [], "youtube": []})
        result = service.rank_and_gate(
            {
                "expected_base": {
                    "artist": "Artist",
                    "track": "Song",
                    "album": "Album",
                    "query": '"Artist" "Song" "Album"',
                    "media_intent": "music_track",
                    "duration_hint_sec": 200,
                    "duration_hard_cap_ms": 35000,
                },
                "coherence_key": None,
                "query_label": "unit",
                "rung": 0,
                "recording_mbid": "edge-1",
            },
            candidates,
        )
        self.assertIsNone(result.selected)
        gates = {str(item.get("top_failed_gate") or "") for item in result.rejected_candidates}
        self.assertIn("variant_alignment", gates)
        self.assertIn("artist_similarity", gates)
        self.assertIn("title_similarity", gates)
        self.assertTrue("duration_delta_ms" in gates or "duration_hard_cap_ms" in gates)
        self.assertIsInstance(result.final_rejection, dict)
        self.assertIn("nearest_pass_margin", result.final_rejection)

    def test_rank_and_gate_margins_stable_across_candidate_shuffles(self):
        base_candidates = [
            _candidate(
                source="youtube_music",
                candidate_id="stable-variant",
                title="Artist - Song (Live)",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube_music",
                candidate_id="stable-artist",
                title="Artist - Song",
                uploader="Unrelated Performer Official",
                artist="Unrelated Performer",
                track="Song",
                album="Album",
                duration_sec=200,
            ),
            _candidate(
                source="youtube_music",
                candidate_id="stable-duration",
                title="Artist - Song",
                uploader="Artist - Topic",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=260,
            ),
        ]
        reference = None
        for seed in range(10):
            shuffled = [dict(c) for c in base_candidates]
            random.Random(seed).shuffle(shuffled)
            service = self._service({"youtube_music": [], "youtube": []})
            result = service.rank_and_gate(
                {
                    "expected_base": {
                        "artist": "Artist",
                        "track": "Song",
                        "album": "Album",
                        "query": '"Artist" "Song" "Album"',
                        "media_intent": "music_track",
                        "duration_hint_sec": 200,
                        "duration_hard_cap_ms": 35000,
                    },
                    "coherence_key": None,
                    "query_label": "unit",
                    "rung": 0,
                    "recording_mbid": "edge-2",
                },
                shuffled,
            )
            normalized = {
                "final_rejection": result.final_rejection,
                "rejected": sorted(
                    [
                        (
                            str(item.get("candidate_id") or ""),
                            str(item.get("top_failed_gate") or ""),
                            float(((item.get("nearest_pass_margin") or {}).get("margin_to_pass")) or 0.0),
                        )
                        for item in (result.rejected_candidates or [])
                    ]
                ),
            }
            if reference is None:
                reference = normalized
                continue
            self.assertEqual(normalized, reference)

    def test_topic_channel_with_strong_artist_overlap_gets_authority_bonus(self):
        expected = {
            "artist": "Kenny Chesney",
            "track": "Young",
            "album": "No Shoes No Shirt No Problems",
            "duration_hint_sec": 235,
            "media_intent": "music_track",
            "query": '"Kenny Chesney" "Young" "No Shoes No Shirt No Problems" audio official topic',
        }
        strong_topic = _candidate(
            source="youtube",
            candidate_id="topic-strong",
            title="Kenny Chesney - Young (Official Video)",
            uploader="Kenny Chesney - Topic",
            artist="Kenny Chesney",
            track="Young",
            album="No Shoes No Shirt No Problems",
            duration_sec=235,
        )
        weak_topic = _candidate(
            source="youtube",
            candidate_id="topic-weak",
            title="Kenny Chesney - Young (Official Video)",
            uploader="Random Uploads - Topic",
            artist="Kenny Chesney",
            track="Young",
            album="No Shoes No Shirt No Problems",
            duration_sec=235,
        )

        strong_score = self.scoring.score_candidate(expected, strong_topic, source_modifier=1.0)
        weak_score = self.scoring.score_candidate(expected, weak_topic, source_modifier=1.0)
        self.assertGreater(float(strong_score["final_score"]), float(weak_score["final_score"]))

    def test_query_ladder_progresses_until_viable_rung(self):
        rung_1 = '"Artist" "Song (Live)" "Album"'
        rung_2 = '"Artist" "Song (Live)"'
        rung_3 = '"Artist" "Song"'
        rung_4 = "Artist Song official audio"
        rung_5 = "Artist - Song (Live) topic"
        rung_6 = "Artist - Song (Live) audio"
        adapter = _QueryAwareAdapter(
            "youtube_music",
            {
                rung_3: [
                    _candidate(
                        source="youtube_music",
                        candidate_id="r3-match",
                        title="Artist - Song (Live)",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song Live",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            },
        )
        service = self.se.SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"youtube_music": adapter},
            config={},
            paths=None,
            canonical_resolver=_StubCanonicalResolver(),
        )
        best = service.search_music_track_best_match("Artist", "Song (Live)", album="Album", duration_ms=200000, limit=6)
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "r3-match")
        self.assertEqual(adapter.calls, [rung_1, rung_2, rung_3])
        self.assertNotIn(rung_4, adapter.calls)
        self.assertNotIn(rung_5, adapter.calls)
        self.assertNotIn(rung_6, adapter.calls)

    def test_legacy_topic_rung_activates_only_after_prior_rungs_fail(self):
        rung_1 = '"Artist" "Song" "Album"'
        rung_2 = '"Artist" "Song"'
        rung_3 = '"Artist" "Song"'
        rung_4 = "Artist Song official audio"
        rung_5 = "Artist - Song topic"
        rung_6 = "Artist - Song audio"
        adapter = _QueryAwareAdapter(
            "youtube_music",
            {
                rung_5: [
                    _candidate(
                        source="youtube_music",
                        candidate_id="legacy-topic-hit",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            },
        )
        service = self.se.SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"youtube_music": adapter},
            config={},
            paths=None,
            canonical_resolver=_StubCanonicalResolver(),
        )
        best = service.search_music_track_best_match("Artist", "Song", album="Album", duration_ms=200000, limit=6)
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "legacy-topic-hit")
        self.assertEqual(adapter.calls, [rung_1, rung_2, rung_4, rung_5])
        self.assertNotIn(rung_6, adapter.calls)

    def test_legacy_audio_rung_activates_when_topic_rung_fails_gates(self):
        rung_1 = '"Artist" "Song" "Album"'
        rung_2 = '"Artist" "Song"'
        rung_4 = "Artist Song official audio"
        rung_5 = "Artist - Song topic"
        rung_6 = "Artist - Song audio"
        adapter = _QueryAwareAdapter(
            "youtube_music",
            {
                rung_5: [
                    _candidate(
                        source="youtube_music",
                        candidate_id="legacy-topic-variant",
                        title="Artist - Song (Live)",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song Live",
                        album="Album",
                        duration_sec=200,
                    )
                ],
                rung_6: [
                    _candidate(
                        source="youtube_music",
                        candidate_id="legacy-audio-hit",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ],
            },
        )
        service = self.se.SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"youtube_music": adapter},
            config={},
            paths=None,
            canonical_resolver=_StubCanonicalResolver(),
        )
        best = service.search_music_track_best_match("Artist", "Song", album="Album", duration_ms=200000, limit=6)
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "legacy-audio-hit")
        self.assertEqual(adapter.calls, [rung_1, rung_2, rung_4, rung_5, rung_6])

    def test_pass_b_accepts_high_similarity_authority_match_with_expanded_duration(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="expanded-ok",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=230,  # +30s (fails strict pass A; eligible for pass B)
                    )
                ]
            }
        )
        best = service.search_music_track_best_match("Artist", "Song", album="Album", duration_ms=200000, limit=6)
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "expanded-ok")
        self.assertGreater(int(best.get("duration_delta_ms") or 0), 12000)
        self.assertLessEqual(int(best.get("duration_delta_ms") or 0), 35000)
        self.assertTrue(bool(best.get("authority_channel_match")))
        self.assertGreaterEqual(float(best.get("score_track") or 0.0), 0.92)
        self.assertGreaterEqual(float(best.get("score_artist") or 0.0), 0.92)
        search_meta = getattr(service, "last_music_track_search", {}) or {}
        self.assertEqual(search_meta.get("selected_pass"), "expanded")

    def test_live_canonical_track_can_pass_when_expected_track_is_live(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="live-ok",
                        title="Artist - Song (Live)",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song Live",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        best = service.search_music_track_best_match(
            "Artist", "Song (Live)", album="Album", duration_ms=200000, limit=6
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "live-ok")

    def test_mb_alias_variant_recovers_title_match(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="alias-ok",
                        title="Artist - Song Pt. 2",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song Pt 2",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        without_alias = service.search_music_track_best_match(
            "Artist",
            "Song Part II",
            album="Album",
            duration_ms=200000,
            limit=6,
        )
        self.assertIsNone(without_alias)
        with_alias = service.search_music_track_best_match(
            "Artist",
            "Song Part II",
            album="Album",
            duration_ms=200000,
            limit=6,
            track_aliases=["Song Pt 2"],
        )
        self.assertIsNotNone(with_alias)
        self.assertEqual(with_alias.get("candidate_id"), "alias-ok")

    def test_mb_relationship_injected_candidate_can_pass_when_gates_match(self):
        service = self._service({"youtube_music": []})
        service._resolve_mb_relationship_candidates = lambda **kwargs: [
            _candidate(
                source="mb_relationship",
                candidate_id="mb-rel-ok",
                title="Artist - Song",
                uploader="Artist Official",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
                url="https://www.youtube.com/watch?v=mbrelok",
            )
        ]
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=["https://www.youtube.com/watch?v=mbrelok"],
            recording_mbid="rec-1",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "mb-rel-ok")
        self.assertEqual(best.get("source"), "mb_relationship")

    def test_mb_relationship_injected_candidate_failing_duration_is_rejected(self):
        service = self._service({"youtube_music": []})
        service._resolve_mb_relationship_candidates = lambda **kwargs: [
            _candidate(
                source="mb_relationship",
                candidate_id="mb-rel-bad-duration",
                title="Artist - Song",
                uploader="Artist Official",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=320,
                url="https://www.youtube.com/watch?v=mbreldur",
            )
        ]
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=["https://www.youtube.com/watch?v=mbreldur"],
            recording_mbid="rec-2",
        )
        self.assertIsNone(best)
        meta = getattr(service, "last_music_track_search", {}) or {}
        rejection_mix = meta.get("mb_injected_rejections") or {}
        self.assertGreaterEqual(int(rejection_mix.get("mb_injected_failed_duration") or 0), 1)

    def test_mb_relationship_injection_absent_uses_normal_ladder(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="normal-ok",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        service._resolve_mb_relationship_candidates = lambda **kwargs: []
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=[],
            recording_mbid="rec-3",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "normal-ok")

    def test_mb_relationship_injection_failure_does_not_block_ladder_recovery(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="ladder-ok",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        service._resolve_mb_relationship_candidates = lambda **kwargs: [
            _candidate(
                source="mb_relationship",
                candidate_id="mb-rel-variant",
                title="Artist - Song (Live)",
                uploader="Artist Official",
                artist="Artist",
                track="Song",
                album="Album",
                duration_sec=200,
                url="https://www.youtube.com/watch?v=mbrelbad",
            )
        ]
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=["https://www.youtube.com/watch?v=mbrelbad"],
            recording_mbid="rec-4",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "ladder-ok")

    def test_album_coherence_boost_recovers_near_threshold_track(self):
        rung_1_seed = '"Artist" "Seed Song" "Album"'
        rung_1_target = '"Artist" "Deep Cut" "Album"'
        adapter = _QueryAwareAdapter(
            "youtube",
            {
                rung_1_seed: [
                    _candidate(
                        source="youtube",
                        candidate_id="seed-a",
                        title="Artist - Seed Song",
                        uploader="Artist Official",
                        artist="Artist",
                        track="Seed Song",
                        album="Album",
                        duration_sec=200,
                        channel_id="chan-a",
                    )
                ],
                rung_1_target: [
                    _candidate(
                        source="youtube",
                        candidate_id="candidate-a",
                        title="Artist - Deep Cut (Official Video)",
                        uploader="Archive Channel",
                        artist="Artist",
                        track="Deep Cut",
                        album="Album",
                        duration_sec=208,
                        channel_id="chan-a",
                        official=False,
                    ),
                    _candidate(
                        source="youtube",
                        candidate_id="candidate-b",
                        title="Artist - Deep Cut (Official Video)",
                        uploader="Mirror Channel",
                        artist="Artist",
                        track="Deep Cut",
                        album="Album",
                        duration_sec=208,
                        channel_id="chan-b",
                        official=False,
                    ),
                ],
            },
        )
        service = self.se.SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"youtube": adapter},
            config={"debug_music_scoring": True},
            paths=None,
            canonical_resolver=_StubCanonicalResolver(),
        )
        coherence_ctx = {"mb_release_id": "rel-coherence-1", "track_total": 10}

        baseline = service.search_music_track_best_match(
            "Artist",
            "Deep Cut",
            album="Album",
            duration_ms=200000,
            limit=6,
        )
        self.assertIsNone(baseline)

        seed = service.search_music_track_best_match(
            "Artist",
            "Seed Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            coherence_context=coherence_ctx,
        )
        self.assertIsNotNone(seed)
        self.assertEqual(seed.get("candidate_id"), "seed-a")

        boosted = service.search_music_track_best_match(
            "Artist",
            "Deep Cut",
            album="Album",
            duration_ms=200000,
            limit=6,
            coherence_context=coherence_ctx,
        )
        self.assertIsNotNone(boosted)
        self.assertEqual(boosted.get("candidate_id"), "candidate-a")
        self.assertGreater(float(boosted.get("coherence_delta") or 0.0), 0.0)
        self.assertGreater(float(boosted.get("final_score") or 0.0), float(boosted.get("base_final_score") or 0.0))

    def test_album_coherence_boost_does_not_override_variant_rejection(self):
        rung_1_seed = '"Artist" "Seed Song" "Album"'
        rung_1_target = '"Artist" "Blocked Song" "Album"'
        adapter = _QueryAwareAdapter(
            "youtube",
            {
                rung_1_seed: [
                    _candidate(
                        source="youtube",
                        candidate_id="seed-a",
                        title="Artist - Seed Song",
                        uploader="Artist Official",
                        artist="Artist",
                        track="Seed Song",
                        album="Album",
                        duration_sec=200,
                        channel_id="chan-a",
                    )
                ],
                rung_1_target: [
                    _candidate(
                        source="youtube",
                        candidate_id="blocked-live",
                        title="Artist - Blocked Song (Live)",
                        uploader="Archive Channel",
                        artist="Artist",
                        track="Blocked Song",
                        album="Album",
                        duration_sec=208,
                        channel_id="chan-a",
                        official=False,
                    ),
                    _candidate(
                        source="youtube",
                        candidate_id="below-threshold",
                        title="Artist - Blocked Song (Official Video)",
                        uploader="Mirror Channel",
                        artist="Artist",
                        track="Blocked Song",
                        album="Album",
                        duration_sec=208,
                        channel_id="chan-b",
                        official=False,
                    ),
                ],
            },
        )
        service = self.se.SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"youtube": adapter},
            config={},
            paths=None,
            canonical_resolver=_StubCanonicalResolver(),
        )
        coherence_ctx = {"mb_release_id": "rel-coherence-2", "track_total": 10}
        seed = service.search_music_track_best_match(
            "Artist",
            "Seed Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            coherence_context=coherence_ctx,
        )
        self.assertIsNotNone(seed)
        blocked = service.search_music_track_best_match(
            "Artist",
            "Blocked Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            coherence_context=coherence_ctx,
        )
        self.assertIsNone(blocked)

    def test_mb_relationship_injection_candidate_can_win_when_gates_pass(self):
        service = self._service({"youtube_music": []})
        service._resolve_mb_relationship_candidates = (  # type: ignore[attr-defined]
            lambda **kwargs: [
                {
                    "source": "mb_relationship",
                    "candidate_id": "mbrel-ok",
                    "url": "https://www.youtube.com/watch?v=abc123xyz00",
                    "title": "Artist - Song",
                    "uploader": "Artist - Topic",
                    "artist_detected": "Artist",
                    "track_detected": "Song",
                    "album_detected": "Album",
                    "duration_sec": 200,
                    "official": True,
                }
            ]
        )
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=["https://www.youtube.com/watch?v=abc123xyz00"],
            recording_mbid="rec-1",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("source"), "mb_relationship")
        self.assertEqual(best.get("candidate_id"), "mbrel-ok")
        meta = service.last_music_track_search or {}
        self.assertTrue(meta.get("mb_injected_selected"))

    def test_mb_relationship_injection_duration_failure_does_not_bypass_ladder(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="ladder-ok",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        service._resolve_mb_relationship_candidates = (  # type: ignore[attr-defined]
            lambda **kwargs: [
                {
                    "source": "mb_relationship",
                    "candidate_id": "mbrel-bad-dur",
                    "url": "https://www.youtube.com/watch?v=def456uvw00",
                    "title": "Artist - Song",
                    "uploader": "Artist - Topic",
                    "artist_detected": "Artist",
                    "track_detected": "Song",
                    "album_detected": "Album",
                    "duration_sec": 320,
                    "official": True,
                }
            ]
        )
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=["https://www.youtube.com/watch?v=def456uvw00"],
            recording_mbid="rec-2",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "ladder-ok")
        meta = service.last_music_track_search or {}
        rejections = meta.get("mb_injected_rejections") or {}
        self.assertGreaterEqual(int(rejections.get("mb_injected_failed_duration") or 0), 1)

    def test_mb_relationship_injection_absent_keeps_normal_ladder_behavior(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="ladder-only",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=[],
            recording_mbid="rec-3",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "ladder-only")
        meta = service.last_music_track_search or {}
        self.assertEqual(int(meta.get("mb_injected_candidates") or 0), 0)

    def test_mb_relationship_variant_rejection_keeps_ladder_search(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="ladder-ok-2",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        service._resolve_mb_relationship_candidates = (  # type: ignore[attr-defined]
            lambda **kwargs: [
                {
                    "source": "mb_relationship",
                    "candidate_id": "mbrel-live",
                    "url": "https://www.youtube.com/watch?v=ghi789rst00",
                    "title": "Artist - Song (Live)",
                    "uploader": "Artist - Topic",
                    "artist_detected": "Artist",
                    "track_detected": "Song",
                    "album_detected": "Album",
                    "duration_sec": 200,
                    "official": True,
                }
            ]
        )
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=["https://www.youtube.com/watch?v=ghi789rst00"],
            recording_mbid="rec-4",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "ladder-ok-2")
        meta = service.last_music_track_search or {}
        rejections = meta.get("mb_injected_rejections") or {}
        self.assertGreaterEqual(int(rejections.get("mb_injected_failed_variant") or 0), 1)

    def test_mb_relationship_region_blocked_probe_is_classified_unavailable(self):
        service = self._service(
            {
                "youtube_music": [
                    _candidate(
                        source="youtube_music",
                        candidate_id="ladder-ok-region",
                        title="Artist - Song",
                        uploader="Artist - Topic",
                        artist="Artist",
                        track="Song",
                        album="Album",
                        duration_sec=200,
                    )
                ]
            }
        )
        service._resolve_mb_relationship_candidates = (  # type: ignore[attr-defined]
            lambda **kwargs: ([], {"mb_injected_failed_unavailable": 1})
        )
        best = service.search_music_track_best_match(
            "Artist",
            "Song",
            album="Album",
            duration_ms=200000,
            limit=6,
            mb_youtube_urls=["https://www.youtube.com/watch?v=region00block"],
            recording_mbid="rec-region-1",
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("candidate_id"), "ladder-ok-region")
        meta = service.last_music_track_search or {}
        rejections = meta.get("mb_injected_rejections") or {}
        self.assertGreaterEqual(int(rejections.get("mb_injected_failed_unavailable") or 0), 1)


if __name__ == "__main__":
    unittest.main()
