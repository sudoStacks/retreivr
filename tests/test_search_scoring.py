import unittest
import importlib.util
import sys
import types
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


if "engine" not in sys.modules:
    engine_pkg = types.ModuleType("engine")
    engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
    sys.modules["engine"] = engine_pkg
_load_module("engine.music_title_normalization", _ROOT / "engine" / "music_title_normalization.py")
scoring = _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
duration_score = scoring.duration_score
normalize_text = scoring.normalize_text
rank_candidates = scoring.rank_candidates
score_candidate = scoring.score_candidate
select_best_candidate = scoring.select_best_candidate
tokenize = scoring.tokenize
classify_music_title_variants = scoring.classify_music_title_variants


class SearchScoringTests(unittest.TestCase):
    def test_normalize_text_removes_bracket_junk(self):
        text = "Song Title (Official Video) [Lyrics] feat. Guest"
        normalized = normalize_text(text)
        self.assertIn("feat", normalized)
        self.assertNotIn("official", normalized)
        self.assertNotIn("lyrics", normalized)

    def test_tokenize_preserves_slash_and_ampersand(self):
        tokens = tokenize("AC/DC & Friends")
        self.assertIn("ac/dc", tokens)
        self.assertIn("&", tokens)

    def test_duration_curve(self):
        self.assertEqual(duration_score(100, 102), 1.00)
        self.assertEqual(duration_score(100, 104), 0.90)
        self.assertEqual(duration_score(100, 108), 0.75)
        self.assertEqual(duration_score(100, 115), 0.50)
        self.assertEqual(duration_score(100, 130), 0.20)

    def test_penalties_cover(self):
        expected = {"artist": "Artist", "track": "Song"}
        candidate = {"title": "Song (Cover)", "artist_detected": "Artist"}
        scores = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertAlmostEqual(scores["penalty_multiplier"], 0.10)

    def test_rank_and_select(self):
        candidates = [
            {"source": "soundcloud", "final_score": 0.80},
            {"source": "bandcamp", "final_score": 0.85},
            {"source": "youtube_music", "final_score": 0.83},
        ]
        ranked = rank_candidates(candidates, source_priority=["bandcamp", "youtube_music", "soundcloud"])
        self.assertEqual(ranked[0]["source"], "bandcamp")
        best = select_best_candidate(ranked, 0.90)
        self.assertIsNone(best)


    def test_canonical_bonus(self):
        expected = {"artist": "Example", "track": "Song", "album": "Album", "duration_hint_sec": 200}
        candidate = {
            "title": "Song",
            "artist_detected": "Example",
            "track_detected": "Song",
            "album_detected": "Album",
            "duration_sec": 200,
        }
        base = score_candidate(expected, candidate, source_modifier=1.0)
        candidate["canonical_metadata"] = {
            "artist": "Example",
            "track": "Song",
            "album": "Album",
            "duration_sec": 200,
            "external_ids": {"isrc": "ABC123"},
        }
        candidate["isrc"] = "ABC123"
        boosted = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertGreater(boosted["final_score"], base["final_score"])

    def test_music_track_scoring_uses_relaxed_parenthetical_overlap(self):
        expected = {
            "artist": "Artist",
            "track": "Song (Deluxe Edition)",
            "album": "Album",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song (Deluxe Edition)" "Album"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "Artist - Song",
            "uploader": "Artist - Topic",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "album_detected": "Album",
            "duration_sec": 200,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertGreater(float(scored.get("score_track_relaxed") or 0.0), float(scored.get("score_track") or 0.0))

    def test_music_track_scoring_uses_mb_alias_variants(self):
        expected = {
            "artist": "Anna Nalick",
            "track": "Breathe (2 AM)",
            "album": "Wreck of the Day",
            "duration_hint_sec": 220,
            "media_intent": "music_track",
            "query": '"Anna Nalick" "Breathe (2 AM)" "Wreck of the Day"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "2 AM",
            "uploader": "Anna Nalick - Topic",
            "artist_detected": "Anna Nalick",
            "track_detected": "2 AM",
            "album_detected": "Wreck of the Day",
            "duration_sec": 220,
            "official": True,
        }
        baseline = score_candidate(expected, candidate, source_modifier=1.0)
        expected_with_alias = dict(expected)
        expected_with_alias["track_aliases"] = ["2 AM"]
        alias_scored = score_candidate(expected_with_alias, candidate, source_modifier=1.0)
        self.assertGreater(float(alias_scored.get("score_track") or 0.0), float(baseline.get("score_track") or 0.0))
        self.assertEqual(alias_scored.get("score_track_variant_used"), "2 AM")

    def test_music_track_alias_does_not_bypass_variant_rejection(self):
        expected = {
            "artist": "Artist",
            "track": "Song (Radio Version)",
            "track_aliases": ["Song"],
            "album": "Album",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song (Radio Version)" "Album"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "Song (Live)",
            "uploader": "Artist - Topic",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "album_detected": "Album",
            "duration_sec": 200,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertEqual(scored.get("rejection_reason"), "disallowed_variant")

    def test_music_track_scoring_is_case_insensitive_for_artist_track_album(self):
        expected_upper = {
            "artist": "HIXTAPE",
            "track": "TO HANK",
            "album": "HIXTAPE VOL. 2",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"HIXTAPE" "TO HANK" "HIXTAPE VOL. 2"',
        }
        expected_mixed = {
            "artist": "HiXtaPe",
            "track": "To Hank",
            "album": "Hixtape Vol. 2",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"HiXtaPe" "To Hank" "Hixtape Vol. 2"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "HiXTAPE - To Hank (Official Audio)",
            "uploader": "HiXTAPE - Topic",
            "artist_detected": "hixtape",
            "track_detected": "to hank",
            "album_detected": "HIXTAPE vol 2",
            "duration_sec": 200,
            "official": True,
        }

        scored_upper = score_candidate(expected_upper, candidate, source_modifier=1.0)
        scored_mixed = score_candidate(expected_mixed, candidate, source_modifier=1.0)
        self.assertAlmostEqual(float(scored_upper.get("score_artist") or 0.0), float(scored_mixed.get("score_artist") or 0.0))
        self.assertAlmostEqual(float(scored_upper.get("score_track") or 0.0), float(scored_mixed.get("score_track") or 0.0))
        self.assertAlmostEqual(float(scored_upper.get("score_album") or 0.0), float(scored_mixed.get("score_album") or 0.0))
        self.assertAlmostEqual(float(scored_upper.get("final_score") or 0.0), float(scored_mixed.get("final_score") or 0.0))

    def test_music_track_scoring_accepts_primary_artist_when_expected_has_feat_credit(self):
        expected = {
            "artist": "HARDY feat. Morgan Wallen",
            "track": "red",
            "duration_hint_sec": 205,
            "media_intent": "music_track",
            "query": '"HARDY feat. Morgan Wallen" "red"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "HARDY - red (feat. Morgan Wallen)",
            "uploader": "HARDY",
            "artist_detected": "HARDY",
            "track_detected": "red",
            "duration_sec": 205,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertGreaterEqual(float(scored.get("score_artist") or 0.0), 0.99)
        self.assertNotEqual(scored.get("rejection_reason"), "low_artist_similarity")

    def test_music_track_scoring_uses_album_artist_variant_for_artist_overlap(self):
        expected = {
            "artist": "HARDY feat. Morgan Wallen",
            "album_artist": "HARDY",
            "track": "red",
            "duration_hint_sec": 205,
            "media_intent": "music_track",
            "query": '"HARDY feat. Morgan Wallen" "red"',
        }
        candidate = {
            "source": "soundcloud",
            "title": "red (feat. Morgan Wallen)",
            "uploader": "HARDY",
            "artist_detected": "HARDY",
            "track_detected": "red",
            "duration_sec": 205,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertGreaterEqual(float(scored.get("score_artist") or 0.0), 0.99)

    def test_music_track_scoring_splits_collaboration_artists(self):
        expected = {
            "artist": "HiXTAPE, HARDY & Morgan Wallen",
            "track": "He Went To Jared",
            "duration_hint_sec": 210,
            "media_intent": "music_track",
            "query": '"HiXTAPE, HARDY & Morgan Wallen" "He Went To Jared"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "HARDY - He Went To Jared",
            "uploader": "HARDY",
            "artist_detected": "HARDY",
            "track_detected": "He Went To Jared",
            "duration_sec": 210,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertGreaterEqual(float(scored.get("score_artist") or 0.0), 0.99)

    def test_view_count_bonus_prefers_higher_views_for_generic_scores(self):
        expected = {"artist": "Artist", "track": "Song"}
        low_views = {
            "title": "Artist - Song",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "duration_sec": 200,
            "view_count": 250,
        }
        high_views = dict(low_views)
        high_views["view_count"] = 2_500_000
        low_scored = score_candidate(expected, low_views, source_modifier=1.0)
        high_scored = score_candidate(expected, high_views, source_modifier=1.0)
        self.assertGreater(float(high_scored.get("final_score") or 0.0), float(low_scored.get("final_score") or 0.0))

    def test_view_count_bonus_reads_raw_meta_json_when_field_missing(self):
        expected = {"artist": "Artist", "track": "Song"}
        low_views = {
            "title": "Artist - Song",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "duration_sec": 200,
            "raw_meta_json": '{"view_count": 100}',
        }
        high_views = {
            "title": "Artist - Song",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "duration_sec": 200,
            "raw_meta_json": '{"view_count": 9000000}',
        }
        low_scored = score_candidate(expected, low_views, source_modifier=1.0)
        high_scored = score_candidate(expected, high_views, source_modifier=1.0)
        self.assertGreater(float(high_scored.get("final_score") or 0.0), float(low_scored.get("final_score") or 0.0))

    def test_music_track_youtube_missing_album_does_not_trigger_album_gate_rejection(self):
        expected = {
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song" "Album"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "Artist - Song",
            "uploader": "Artist - Topic",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "album_detected": "",
            "duration_sec": 200,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertNotEqual(scored.get("rejection_reason"), "low_album_similarity")
        self.assertNotIn("album_mismatch_penalty", (scored.get("score_breakdown") or {}).get("penalty_reasons") or [])

    def test_music_track_bandcamp_still_requires_album_similarity_floor(self):
        expected = {
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song" "Album"',
        }
        candidate = {
            "source": "bandcamp",
            "title": "Artist - Song",
            "uploader": "Artist",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "album_detected": "",
            "duration_sec": 200,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertEqual(scored.get("rejection_reason"), "low_album_similarity")

    def test_music_track_applies_gate_pass_bonus_when_hard_gates_pass(self):
        expected = {
            "artist": "Artist",
            "track": "Song",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "Artist - Song",
            "uploader": "Artist - Topic",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "duration_sec": 200,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertIsNone(scored.get("rejection_reason"))
        self.assertAlmostEqual(float((scored.get("score_breakdown") or {}).get("gate_pass_bonus") or 0.0), 0.03)

    def test_music_track_does_not_apply_gate_pass_bonus_when_hard_gate_fails(self):
        expected = {
            "artist": "Artist",
            "track": "Song",
            "duration_hint_sec": 200,
            "media_intent": "music_track",
            "query": '"Artist" "Song"',
        }
        candidate = {
            "source": "youtube_music",
            "title": "Artist - Song (Live)",
            "uploader": "Artist - Topic",
            "artist_detected": "Artist",
            "track_detected": "Song",
            "duration_sec": 200,
            "official": True,
        }
        scored = score_candidate(expected, candidate, source_modifier=1.0)
        self.assertEqual(scored.get("rejection_reason"), "disallowed_variant")
        self.assertAlmostEqual(float((scored.get("score_breakdown") or {}).get("gate_pass_bonus") or 0.0), 0.0)

    def test_classify_music_title_variants_representative_tokens(self):
        tags = classify_music_title_variants(
            "Artist - Song (Official Audio) [Lyric Video] (Remastered 2020) (Radio Edit) (Extended Cut) [Sped Up] [Nightcore] [8D]"
        )
        self.assertIn("official_audio", tags)
        self.assertIn("lyric_video", tags)
        self.assertIn("remaster", tags)
        self.assertIn("radio_edit", tags)
        self.assertIn("extended", tags)
        self.assertIn("cut", tags)
        self.assertIn("sped_up", tags)
        self.assertIn("nightcore", tags)
        self.assertIn("8d", tags)

    def test_classify_music_title_variants_stable_under_normalization(self):
        a = classify_music_title_variants("  ARTIST — Song (Lyric Video) [Slowed Down]  ")
        b = classify_music_title_variants("artist song lyric-video slowed_down")
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
