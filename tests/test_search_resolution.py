import os
import sqlite3
import tempfile
import types
import unittest

from engine.search_engine import SearchResolutionService


class StubAdapter:
    source = "stub"

    def search_track(self, artist, track, album=None, limit=5):
        return [
            {
                "source": self.source,
                "url": "https://example.test/media",
                "title": f"{artist} - {track}",
                "uploader": artist,
                "artist_detected": artist,
                "album_detected": album,
                "track_detected": track,
                "duration_sec": 200,
                "artwork_url": None,
                "raw_meta_json": "{}",
                "official": True,
            }
        ]

    def search_album(self, artist, album, limit=5):
        return []

    def source_modifier(self, candidate):
        return 1.0



class StubCanonicalResolver:
    def resolve_track(self, artist, track, *, album=None):
        return None

    def resolve_album(self, artist, album):
        return None


class SearchResolutionTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_db = os.path.join(self.tmpdir.name, "queue.sqlite")
        self.search_db = os.path.join(self.tmpdir.name, "search.sqlite")
        self.paths = types.SimpleNamespace(single_downloads_dir=self.tmpdir.name)
        self.service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def _count_jobs(self):
        conn = sqlite3.connect(self.queue_db)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM download_jobs")
            return cur.fetchone()[0]
        finally:
            conn.close()

    def test_enqueue_idempotency(self):
        request_id = self.service.create_search_request(
            {
                "intent": "track",
                "artist": "Example Artist",
                "track": "Example Track",
                "source_priority": ["stub"],
            }
        )
        self.assertIsNotNone(request_id)
        self.service.run_search_resolution_once()
        self.assertEqual(self._count_jobs(), 1)
        self.service.run_search_resolution_once()
        self.assertEqual(self._count_jobs(), 1)


if __name__ == "__main__":
    unittest.main()
