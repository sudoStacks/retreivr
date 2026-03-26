import importlib.util
import sqlite3
import sys
import threading
import time
from collections import OrderedDict
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_community_cache_module():
    spec = importlib.util.spec_from_file_location("engine.community_cache", _ROOT / "engine" / "community_cache.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["engine.community_cache"] = module
    spec.loader.exec_module(module)
    return module


community_cache = _load_community_cache_module()


def _reset_cache_state(monkeypatch, *, ttl=3600, max_size=2048):
    monkeypatch.setattr(community_cache, "_CACHE", OrderedDict())
    monkeypatch.setattr(community_cache, "_IN_FLIGHT", {})
    monkeypatch.setattr(community_cache, "_CACHE_TTL", ttl)
    monkeypatch.setattr(community_cache, "_CACHE_MAX_SIZE", max_size)


def test_cached_lookup_coalesces_concurrent_misses(monkeypatch):
    _reset_cache_state(monkeypatch, max_size=64)
    calls = {"count": 0}

    def _lookup(_mbid):
        calls["count"] += 1
        time.sleep(0.05)
        return {"video_id": "vid-1", "duration_ms": 123000, "confidence": 0.9}

    monkeypatch.setattr(community_cache, "lookup_recording", _lookup)

    results = []
    errors = []

    def _worker():
        try:
            results.append(community_cache.cached_lookup("rec-1"))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(results) == 8
    assert all(isinstance(item, dict) and item.get("video_id") == "vid-1" for item in results)
    assert calls["count"] == 1


def test_cached_lookup_preserves_negative_caching(monkeypatch):
    _reset_cache_state(monkeypatch, max_size=64)
    calls = {"count": 0}

    def _lookup(_mbid):
        calls["count"] += 1
        return None

    monkeypatch.setattr(community_cache, "lookup_recording", _lookup)

    first = community_cache.cached_lookup("missing-rec")
    second = community_cache.cached_lookup("missing-rec")

    assert first is None
    assert second is None
    assert calls["count"] == 1


def test_cached_lookup_enforces_cache_max_size(monkeypatch):
    _reset_cache_state(monkeypatch, max_size=3)

    def _lookup(mbid):
        return {"video_id": f"vid-{mbid}"}

    monkeypatch.setattr(community_cache, "lookup_recording", _lookup)

    for idx in range(5):
        community_cache.cached_lookup(f"rec-{idx}")

    keys = list(community_cache._CACHE.keys())
    assert len(keys) == 3
    assert keys == ["rec-2", "rec-3", "rec-4"]


def test_rebuild_reverse_index_from_dataset_is_deterministic(tmp_path):
    dataset_root = tmp_path / "dataset"
    shard = dataset_root / "youtube" / "recording" / "aa"
    shard.mkdir(parents=True, exist_ok=True)
    (shard / "aa111.json").write_text(
        '{"sources":[{"video_id":"vid-1","confidence":0.9},{"video_id":"vid-2","confidence":0.5}]}',
        encoding="utf-8",
    )
    (shard / "aa222.json").write_text(
        '{"sources":[{"video_id":"vid-1","confidence":0.95},{"video_id":"vid-3","confidence":0.7}]}',
        encoding="utf-8",
    )
    db_path = tmp_path / "search.sqlite"

    first = community_cache.rebuild_reverse_index_from_dataset(
        db_path=str(db_path),
        dataset_root=str(dataset_root),
    )
    second = community_cache.rebuild_reverse_index_from_dataset(
        db_path=str(db_path),
        dataset_root=str(dataset_root),
    )

    assert first["files_scanned"] == 2
    assert second["video_ids_indexed"] == 3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT video_id, recording_mbid, confidence FROM community_video_index ORDER BY video_id"
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    assert rows == [
        ("vid-1", "aa222", 0.95),
        ("vid-2", "aa111", 0.5),
        ("vid-3", "aa222", 0.7),
    ]


def test_rebuild_reverse_index_clears_existing_rows_when_dataset_missing(tmp_path):
    db_path = tmp_path / "search.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS community_video_index (
                video_id TEXT PRIMARY KEY,
                recording_mbid TEXT,
                confidence REAL,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT OR REPLACE INTO community_video_index
            (video_id, recording_mbid, confidence, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            ("stale-vid", "stale-rec", 0.1, "2026-03-01T00:00:00+00:00"),
        )
        conn.commit()
    finally:
        conn.close()

    stats = community_cache.rebuild_reverse_index_from_dataset(
        db_path=str(db_path),
        dataset_root=str(tmp_path / "missing_dataset_root"),
    )
    assert stats == {"files_scanned": 0, "video_ids_indexed": 0}

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM community_video_index")
        count = int(cur.fetchone()[0] or 0)
    finally:
        conn.close()
    assert count == 0
