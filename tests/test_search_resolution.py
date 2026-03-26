import os
import sqlite3
import tempfile
import types
import unittest
from unittest import mock

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

    def search_music_track(self, query, limit=5):
        return [
            {
                "source": "youtube",
                "url": "https://www.youtube.com/watch?v=stubvideo001",
                "title": query,
                "uploader": "Example Artist",
                "artist_detected": "Example Artist",
                "track_detected": query,
                "duration_sec": 200,
                "raw_meta_json": "{}",
            }
        ]

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

    def test_create_request_seeds_cached_candidates_when_enabled(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        payload = {
            "intent": "track",
            "artist": "Example Artist",
            "track": "Example Track",
            "source_priority": ["stub"],
            "auto_enqueue": False,
        }
        request_id_a = service.create_search_request(payload)
        row_a = service.store.get_request_row(request_id_a)
        service.store.create_items_for_request(row_a)
        item_a = service.store.list_items(request_id_a)[0]
        cache_key, query = service._search_cache_key_for_item(item_a, row_a)  # type: ignore[attr-defined]
        service.store.replace_search_cache(
            cache_key=cache_key,
            query_text=query,
            media_type="generic",
            candidates=[
                {
                    "source": "stub",
                    "url": "https://example.test/cached-media",
                    "title": "Cached Candidate",
                    "uploader": "Cached Uploader",
                    "duration_sec": 201,
                    "candidate_id": "cached-1",
                }
            ],
        )

        request_id_b = service.create_search_request(payload)
        response = service.get_search_request(request_id_b)
        self.assertIsNotNone(response)
        items = (response or {}).get("items") or []
        self.assertEqual(len(items), 1)
        self.assertGreaterEqual(int(items[0].get("candidate_count") or 0), 1)
        self.assertEqual(items[0].get("status"), "candidate_found")

    def test_seeded_candidates_remain_visible_after_adapter_batches(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        payload = {
            "intent": "track",
            "artist": "Example Artist",
            "track": "Example Track",
            "source_priority": ["stub"],
            "auto_enqueue": False,
        }
        request_id_a = service.create_search_request(payload)
        row_a = service.store.get_request_row(request_id_a)
        service.store.create_items_for_request(row_a)
        item_a = service.store.list_items(request_id_a)[0]
        cache_key, query = service._search_cache_key_for_item(item_a, row_a)  # type: ignore[attr-defined]
        service.store.replace_search_cache(
            cache_key=cache_key,
            query_text=query,
            media_type="generic",
            candidates=[
                {
                    "source": "stub",
                    "url": "https://example.test/cached-media",
                    "title": "Cached Candidate",
                    "uploader": "Cached Uploader",
                    "duration_sec": 201,
                    "candidate_id": "cached-1",
                }
            ],
        )

        request_id_b = service.create_search_request(payload)
        response_b = service.get_search_request(request_id_b) or {}
        item_b = (response_b.get("items") or [None])[0]
        self.assertIsNotNone(item_b)
        item_id = item_b["id"]
        seeded_urls = {row.get("url") for row in service.list_item_candidates(item_id)}
        self.assertIn("https://example.test/cached-media", seeded_urls)

        service.run_search_resolution_once(request_id=request_id_b)
        final_urls = {row.get("url") for row in service.list_item_candidates(item_id)}
        self.assertIn("https://example.test/cached-media", final_urls)
        self.assertIn("https://example.test/media", final_urls)

    def test_adapter_results_dedupe_against_cache_seeded_identity(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        payload = {
            "intent": "track",
            "artist": "Example Artist",
            "track": "Example Track",
            "source_priority": ["stub"],
            "auto_enqueue": False,
        }
        request_id_a = service.create_search_request(payload)
        row_a = service.store.get_request_row(request_id_a)
        service.store.create_items_for_request(row_a)
        item_a = service.store.list_items(request_id_a)[0]
        cache_key, query = service._search_cache_key_for_item(item_a, row_a)  # type: ignore[attr-defined]
        service.store.replace_search_cache(
            cache_key=cache_key,
            query_text=query,
            media_type="generic",
            candidates=[
                {
                    "source": "stub",
                    "url": "https://example.test/media",
                    "title": "Cached Candidate",
                    "uploader": "Cached Uploader",
                    "duration_sec": 201,
                    "candidate_id": "cached-dup-1",
                    "search_cache_seeded": True,
                }
            ],
        )

        request_id_b = service.create_search_request(payload)
        service.run_search_resolution_once(request_id=request_id_b)
        response_b = service.get_search_request(request_id_b) or {}
        item_b = (response_b.get("items") or [None])[0]
        self.assertIsNotNone(item_b)
        item_id = item_b["id"]
        rows = service.list_item_candidates(item_id)
        matching = [row for row in rows if row.get("url") == "https://example.test/media"]
        self.assertEqual(len(matching), 1)
        self.assertTrue(bool(matching[0].get("official")))
        self.assertTrue(bool(matching[0].get("search_cache_seeded")))

    def test_reverse_lookup_annotation_in_search_candidates(self):
        dataset_root = os.path.join(self.tmpdir.name, "community_cache_dataset")
        os.makedirs(os.path.join(dataset_root, "youtube", "recording", "aa"), exist_ok=True)
        with open(
            os.path.join(dataset_root, "youtube", "recording", "aa", "aa-rec-1.json"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write(
                '{"sources":[{"video_id":"stubvideo001","confidence":0.99}]}'
            )
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"youtube": StubAdapter()},
            config={
                "community_cache_lookup_enabled": True,
                "community_cache_dataset_dir": dataset_root,
            },
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )

        candidates = service.search_music_track_candidates("Example Artist Example Track", limit=1)
        self.assertGreaterEqual(len(candidates), 1)
        first = candidates[0]
        self.assertTrue(bool(first.get("community_verified_transport")))
        self.assertEqual(first.get("community_verified_recording_mbid"), "aa-rec-1")

    def test_invalidate_search_cache_entry_prunes_on_failure(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={
                "search_cache_enabled": True,
                "search_cache_prune_on_failure": True,
            },
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        service.store.replace_search_cache(
            cache_key="cache-key-1",
            query_text="example",
            media_type="generic",
            candidates=[
                {
                    "source": "youtube",
                    "url": "https://www.youtube.com/watch?v=deadbeef001",
                    "title": "Old Candidate",
                    "uploader": "Old Uploader",
                    "duration_sec": 180,
                    "candidate_id": "old-1",
                }
            ],
        )
        before = service.store.list_search_cache(cache_key="cache-key-1", max_age_seconds=None, limit=10)
        self.assertEqual(len(before), 1)
        removed = service.invalidate_search_cache_entry(
            url="https://www.youtube.com/watch?v=deadbeef001",
            reason="yt_dlp_source_unavailable:removed_or_deleted",
        )
        self.assertGreaterEqual(int(removed), 1)
        after = service.store.list_search_cache(cache_key="cache-key-1", max_age_seconds=None, limit=10)
        self.assertEqual(after, [])

    def test_create_request_ignores_search_cache_seed_failure(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        with mock.patch.object(
            service,
            "_seed_request_from_search_cache",
            side_effect=RuntimeError("cache-seed-fail"),
        ):
            request_id = service.create_search_request(
                {
                    "intent": "track",
                    "artist": "Example Artist",
                    "track": "Example Track",
                    "source_priority": ["stub"],
                }
            )
        self.assertIsNotNone(request_id)
        self.assertIsNotNone(service.store.get_request_row(request_id))

    def test_refresh_search_cache_for_item_is_fail_open(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        with mock.patch.object(
            service.store,
            "replace_search_cache",
            side_effect=RuntimeError("cache-write-fail"),
        ):
            service._refresh_search_cache_for_item(  # type: ignore[attr-defined]
                {"id": "req-1", "media_type": "generic", "max_candidates_per_source": 5},
                {"id": "item-1", "item_type": "track", "artist": "A", "track": "T"},
                [{"url": "https://example.test/x", "source": "stub"}],
            )

    def test_search_cache_read_failure_returns_empty_candidates(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        with mock.patch.object(
            service.store,
            "list_search_cache",
            side_effect=RuntimeError("cache-read-fail"),
        ):
            rows = service._search_cache_candidates_for_item(  # type: ignore[attr-defined]
                {"id": "item-1", "item_type": "track", "artist": "A", "track": "T"},
                {"media_type": "generic", "source_priority_json": '["stub"]'},
                limit=5,
            )
        self.assertEqual(rows, [])

    def test_search_cache_seed_filters_restricted_entries(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        item = {"id": "item-1", "item_type": "track", "artist": "A", "track": "T"}
        request_row = {"media_type": "generic", "source_priority_json": '["stub"]'}
        cache_key, query = service._search_cache_key_for_item(item, request_row)  # type: ignore[attr-defined]
        service.store.replace_search_cache(
            cache_key=cache_key,
            query_text=query,
            media_type="generic",
            candidates=[
                {
                    "source": "youtube",
                    "url": "https://www.youtube.com/watch?v=adult000001",
                    "title": "Restricted Candidate",
                    "uploader": "Uploader",
                    "duration_sec": 180,
                    "candidate_id": "adult-1",
                    "raw_meta_json": '{"age_limit": 18, "availability": "needs_auth"}',
                }
            ],
        )
        rows = service.store.list_search_cache(
            cache_key=cache_key,
            max_age_seconds=None,
            limit=10,
        )
        self.assertEqual(len(rows), 1)
        filtered = service._search_cache_candidates_for_item(  # type: ignore[attr-defined]
            item,
            request_row,
            limit=10,
        )
        self.assertEqual(filtered, [])

    def test_refresh_search_cache_seeds_up_to_top_30_by_default(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        ranked = []
        for idx in range(40):
            ranked.append(
                {
                    "candidate_id": f"cand-{idx}",
                    "source": "stub",
                    "url": f"https://example.test/media/{idx}",
                    "title": f"Candidate {idx}",
                    "uploader": "Uploader",
                    "duration_sec": 200,
                }
            )
        captured = {"count": 0}

        def _capture(**kwargs):
            candidates = kwargs.get("candidates") or []
            captured["count"] = len(candidates)

        with mock.patch.object(service.store, "replace_search_cache", side_effect=_capture):
            service._refresh_search_cache_for_item(  # type: ignore[attr-defined]
                {"id": "req-1", "media_type": "generic", "max_candidates_per_source": 5},
                {"id": "item-1", "item_type": "track", "artist": "A", "track": "T"},
                ranked,
            )
        self.assertEqual(captured["count"], 30)

    def test_search_cache_title_query_reuses_alias_seed(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        request_row = {
            "id": "req-1",
            "media_type": "generic",
            "source_priority_json": '["stub"]',
            "max_candidates_per_source": 5,
        }
        broad_item = {"id": "item-1", "item_type": "track", "artist": "libra", "track": "libra"}
        ranked = [
            {
                "candidate_id": "cand-1",
                "source": "stub",
                "url": "https://example.test/media/exact-song",
                "title": "Exact Song Title",
                "uploader": "Example Artist",
                "duration_sec": 200,
            }
        ]
        service._refresh_search_cache_for_item(  # type: ignore[attr-defined]
            request_row,
            broad_item,
            ranked,
        )
        title_item = {"id": "item-2", "item_type": "track", "artist": "Exact Song Title", "track": "Exact Song Title"}
        seeded = service._search_cache_candidates_for_item(  # type: ignore[attr-defined]
            title_item,
            request_row,
            limit=5,
        )
        self.assertEqual(len(seeded), 1)
        self.assertEqual(seeded[0].get("url"), "https://example.test/media/exact-song")
        self.assertTrue(bool(seeded[0].get("search_cache_seeded")))

    def test_search_cache_query_normalization_reuses_similar_video_text(self):
        service = SearchResolutionService(
            search_db_path=self.search_db,
            queue_db_path=self.queue_db,
            adapters={"stub": StubAdapter()},
            config={"search_cache_enabled": True},
            paths=self.paths,
            canonical_resolver=StubCanonicalResolver(),
        )
        request_row = {
            "id": "req-1",
            "media_type": "generic",
            "source_priority_json": '["stub"]',
            "max_candidates_per_source": 5,
        }
        broad_item = {"id": "item-1", "item_type": "track", "artist": "hello", "track": "hello"}
        ranked = [
            {
                "candidate_id": "cand-1",
                "source": "stub",
                "url": "https://example.test/media/hello",
                "title": "Hello Official Video",
                "uploader": "Artist",
                "duration_sec": 200,
            }
        ]
        service._refresh_search_cache_for_item(  # type: ignore[attr-defined]
            request_row,
            broad_item,
            ranked,
        )
        similar_item = {
            "id": "item-2",
            "item_type": "track",
            "artist": "hello official video",
            "track": "hello official video",
        }
        seeded = service._search_cache_candidates_for_item(  # type: ignore[attr-defined]
            similar_item,
            request_row,
            limit=5,
        )
        self.assertEqual(len(seeded), 1)
        self.assertEqual(seeded[0].get("url"), "https://example.test/media/hello")


if __name__ == "__main__":
    unittest.main()
