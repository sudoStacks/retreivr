from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "engine" / "community_cache.py"
    spec = importlib.util.spec_from_file_location("community_cache_test_module", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cached_lookup_prefers_higher_confidence_and_persists_remote(tmp_path: Path, monkeypatch) -> None:
    community_cache = _load_module()
    mbid = "12345678-1234-1234-1234-1234567890ab"
    local_root = tmp_path / "dataset"
    local_path = local_root / "youtube" / "recording" / mbid[:2] / f"{mbid}.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "recording_mbid": mbid,
                "updated_at": "2026-03-24T00:00:00+00:00",
                "sources": [{"source": "youtube", "video_id": "abc123DEF45", "confidence": 0.81}],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        community_cache,
        "fetch_community_record",
        lambda recording_mbid: {
            "schema_version": 1,
            "recording_mbid": recording_mbid,
            "updated_at": "2026-03-25T00:00:00+00:00",
            "sources": [{"source": "youtube", "video_id": "xyz987LMN54", "confidence": 0.97}],
        },
    )

    community_cache._CACHE.clear()
    community_cache._IN_FLIGHT.clear()
    result = community_cache.cached_lookup(mbid, dataset_root=local_root, allow_remote=True)

    assert result["video_id"] == "xyz987LMN54"
    assert result["provider"] == "remote_github"

    persisted = json.loads(local_path.read_text(encoding="utf-8"))
    assert persisted["sources"][0]["video_id"] == "xyz987LMN54"


def test_sync_recording_to_local_dataset_writes_shard(tmp_path: Path, monkeypatch) -> None:
    community_cache = _load_module()
    mbid = "12345678-1234-1234-1234-1234567890ab"
    local_root = tmp_path / "dataset"

    monkeypatch.setattr(
        community_cache,
        "fetch_community_record",
        lambda _recording_mbid: {
            "schema_version": 1,
            "recording_mbid": mbid,
            "updated_at": "2026-03-25T00:00:00+00:00",
            "sources": [{"source": "youtube", "video_id": "xyz987LMN54", "confidence": 0.97}],
        },
    )

    result = community_cache.sync_recording_to_local_dataset(mbid, dataset_root=local_root)

    assert result["status"] == "synced"
    path = local_root / "youtube" / "recording" / mbid[:2] / f"{mbid}.json"
    assert path.exists()
