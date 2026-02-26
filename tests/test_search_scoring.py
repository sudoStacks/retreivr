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


if __name__ == "__main__":
    unittest.main()
