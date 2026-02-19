import os
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


class _StubCanonicalResolver:
    def resolve_track(self, artist, track, *, album=None):
        return None

    def resolve_album(self, artist, album):
        return None


def _candidate(*, source, candidate_id, title, uploader, artist, track, album, duration_sec):
    return {
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
        self.assertGreater(float(plain_score["final_score"]), float(remaster_score["final_score"]))

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


if __name__ == "__main__":
    unittest.main()
