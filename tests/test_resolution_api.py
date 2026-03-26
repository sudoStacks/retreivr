from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "engine" / "resolution_api.py"
    spec = importlib.util.spec_from_file_location("resolution_api_test_module", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_rebuild_resolution_index_from_dataset_and_resolve(tmp_path: Path) -> None:
    resolution_api = _load_module()
    db_path = tmp_path / "resolution.sqlite"
    dataset_root = tmp_path / "community_cache_dataset" / "youtube" / "recording" / "12"
    dataset_root.mkdir(parents=True, exist_ok=True)
    mbid = "12345678-1234-1234-1234-1234567890ab"
    record_path = dataset_root / f"{mbid}.json"
    record_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "recording_mbid": mbid,
                "updated_at": "2026-03-25T00:00:00+00:00",
                "sources": [
                    {
                        "source": "youtube",
                        "video_id": "abc123DEF45",
                        "candidate_url": "https://www.youtube.com/watch?v=abc123DEF45",
                        "duration_ms": 210000,
                        "retreivr_version": "0.9.16",
                        "last_verified_at": "2026-03-25T00:00:00+00:00",
                        "verified_by": "loganbuilt",
                        "confidence": 0.97,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    stats = resolution_api.rebuild_resolution_index_from_dataset(
        db_path=str(db_path),
        dataset_root=str(tmp_path / "community_cache_dataset"),
    )
    assert stats["records_indexed"] == 1

    resolved = resolution_api.resolve_recording(str(db_path), mbid)
    assert resolved["mbid"] == mbid
    assert resolved["stats"]["verified_sources"] == 1
    assert resolved["sources"][0]["url"] == "https://www.youtube.com/watch?v=abc123DEF45"
    assert resolved["sources"][0]["verification"]["status"] == "verified"
    assert resolved["sources"][0]["availability"]["status"] == "verified"
    assert resolved["availability"]["status"] == "verified"
    assert resolved["best_source"]["url"] == "https://www.youtube.com/watch?v=abc123DEF45"


def test_submit_verify_and_stats(tmp_path: Path) -> None:
    resolution_api = _load_module()
    db_path = tmp_path / "resolution.sqlite"
    sqlite3.connect(db_path).close()
    mbid = "12345678-1234-1234-1234-1234567890ab"
    url = "https://www.youtube.com/watch?v=abc123DEF45"

    submit = resolution_api.submit_mapping(
        str(db_path),
        mbid=mbid,
        source_url=url,
        source="youtube",
        node_id="nodeA",
        duration_seconds=210,
        media_format="opus",
        bitrate_kbps=160,
        file_hash="sha256:test",
        resolution_method="retreivr_v0.9.16",
        source_id="abc123DEF45",
        raw_payload={"note": "first seen"},
    )
    assert submit["status"] == "created"

    before_verify = resolution_api.resolve_recording(str(db_path), mbid)
    assert before_verify["sources"][0]["verification"]["status"] == "pending_verification"
    assert before_verify["availability"]["status"] == "local_only"

    verify = resolution_api.verify_mapping(
        str(db_path),
        mbid=mbid,
        source_url=url,
        verifier_id="nodeB",
        duration_seconds=210,
        media_format="opus",
        bitrate_kbps=160,
        file_hash="sha256:test",
        threshold=2,
    )
    assert verify["status"] == "verified"
    assert verify["verification_count"] >= 2

    stats = resolution_api.build_stats(str(db_path))
    assert stats["total_resolved_mbids"] == 1
    assert stats["total_verified_mappings"] == 1
    assert stats["total_nodes"] >= 2
    assert stats["contributions"]["submit"] == 1
    assert stats["contributions"]["verify"] == 1
    assert stats["total_local_only_mappings"] == 1


def test_snapshot_and_diff_return_updated_records(tmp_path: Path) -> None:
    resolution_api = _load_module()
    db_path = tmp_path / "resolution.sqlite"
    sqlite3.connect(db_path).close()
    mbid = "12345678-1234-1234-1234-1234567890ab"
    url = "https://www.youtube.com/watch?v=abc123DEF45"

    resolution_api.submit_mapping(
        str(db_path),
        mbid=mbid,
        source_url=url,
        source="youtube",
        node_id="nodeA",
        duration_seconds=210,
        resolution_method="retreivr_v0.9.16",
        source_id="abc123DEF45",
        raw_payload={},
    )

    snapshot = resolution_api.build_snapshot(str(db_path), limit=100)
    assert snapshot["type"] == "snapshot"
    assert len(snapshot["results"]) == 1
    cursor = snapshot["cursor"]
    assert cursor

    resolution_api.verify_mapping(
        str(db_path),
        mbid=mbid,
        source_url=url,
        verifier_id="nodeB",
        threshold=2,
    )

    diff = resolution_api.build_diff(str(db_path), since=cursor, limit=100)
    assert diff["type"] == "diff"
    assert len(diff["results"]) == 1
    assert diff["results"][0]["mbid"] == mbid


def test_enqueue_unresolved_and_mark_resolved(tmp_path: Path) -> None:
    resolution_api = _load_module()
    db_path = tmp_path / "resolution.sqlite"
    sqlite3.connect(db_path).close()
    mbid = "12345678-1234-1234-1234-1234567890ab"
    url = "https://www.youtube.com/watch?v=abc123DEF45"

    queued = resolution_api.enqueue_unresolved_mbid(
        str(db_path),
        mbid=mbid,
        reason="not_found",
        source="resolve_recording",
    )
    assert queued["status"] == "created"

    stats = resolution_api.build_stats(str(db_path))
    assert stats["unresolved_queue_pending"] == 1

    result = resolution_api.upsert_local_acquired_mapping(
        str(db_path),
        mbid=mbid,
        source_url=url,
        source="youtube",
        node_id="local_node",
        duration_seconds=210,
        source_id="abc123DEF45",
    )
    assert result["action"] == "submitted_local_only"

    stats = resolution_api.build_stats(str(db_path))
    assert stats["unresolved_queue_pending"] == 0


def test_sync_local_cache_from_api_writes_dataset_and_status(tmp_path: Path, monkeypatch) -> None:
    resolution_api = _load_module()
    db_path = tmp_path / "resolution.sqlite"
    sqlite3.connect(db_path).close()
    dataset_root = tmp_path / "community_cache_dataset"
    mbid = "12345678-1234-1234-1234-1234567890ab"

    class _Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "type": "snapshot",
                "cursor": "2026-03-25T00:00:00+00:00",
                "results": [
                    {
                        "schema_version": 1,
                        "entity_type": "recording",
                        "mbid": mbid,
                        "availability": {"status": "verified"},
                        "sources": [
                            {
                                "url": "https://www.youtube.com/watch?v=abc123DEF45",
                                "source": "youtube",
                                "source_id": "abc123DEF45",
                                "duration": 210,
                                "format": "opus",
                                "bitrate": 160,
                                "resolution_method": "retreivr_v0.9.16",
                                "added_at": "2026-03-25T00:00:00+00:00",
                                "added_by": "nodeA",
                                "verification": {
                                    "status": "verified",
                                    "verification_count": 2,
                                    "verified_by": ["nodeA", "nodeB"],
                                    "last_verified_at": "2026-03-25T00:00:00+00:00",
                                },
                                "availability": {"status": "verified"},
                                "metadata": {"confidence": 0.99, "video_id": "abc123DEF45"},
                            }
                        ],
                    }
                ],
            }

    monkeypatch.setattr(resolution_api.requests, "get", lambda *args, **kwargs: _Response())
    summary = resolution_api.sync_local_cache_from_api(
        db_path=str(db_path),
        dataset_root=str(dataset_root),
        api_base_url="https://resolution.example.com",
        limit=100,
    )
    assert summary["status"] == "completed"
    assert summary["results_count"] == 1
    assert (dataset_root / "youtube" / "recording" / mbid[:2] / f"{mbid}.json").exists()
    stored = resolution_api.get_local_sync_status(str(db_path))
    assert stored is not None
    assert stored["cursor"] == "2026-03-25T00:00:00+00:00"
