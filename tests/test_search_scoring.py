import unittest

from engine.search_scoring import (
    duration_score,
    normalize_text,
    rank_candidates,
    score_candidate,
    select_best_candidate,
    tokenize,
)


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


if __name__ == "__main__":
    unittest.main()
