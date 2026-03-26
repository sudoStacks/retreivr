from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "engine.community_publish_worker",
        _ROOT / "engine" / "community_publish_worker.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["engine.community_publish_worker"] = module
    spec.loader.exec_module(module)
    return module


community_publish_worker = _load_module()


def test_merge_proposals_into_record_updates_existing_source_and_keeps_schema() -> None:
    existing = {
        "schema_version": 1,
        "recording_mbid": "aa11",
        "sources": [
            {"video_id": "vid-1", "source": "youtube", "confidence": 0.80},
            {"video_id": "vid-2", "source": "youtube", "confidence": 0.60},
        ],
    }
    proposals = [
        {
            "recording_mbid": "aa11",
            "video_id": "vid-1",
            "source": "youtube",
            "selected_score": 0.95,
            "candidate_url": "https://www.youtube.com/watch?v=vid-1",
            "candidate_id": "cand-1",
            "duration_ms": 200000,
            "duration_delta_ms": 0,
            "retreivr_version": "0.9.15",
            "emitted_at": "2026-03-23T00:00:00+00:00",
        }
    ]

    merged, changed = community_publish_worker.merge_proposals_into_record(existing, proposals)

    assert changed is True
    assert merged["schema_version"] == 1
    assert merged["recording_mbid"] == "aa11"
    assert len(merged["sources"]) == 2
    assert merged["sources"][0]["video_id"] == "vid-1"
    assert float(merged["sources"][0]["confidence"]) == 0.95


def test_merge_proposals_into_record_normalizes_youtube_music_sources() -> None:
    existing = {
        "schema_version": 1,
        "recording_mbid": "aa11",
        "sources": [
            {"video_id": "vid-1", "source": "youtube_music", "confidence": 0.80},
        ],
    }
    proposals = [
        {
            "recording_mbid": "aa11",
            "video_id": "vid-1",
            "source": "youtube_music",
            "selected_score": 0.95,
            "candidate_url": "https://www.youtube.com/watch?v=vid-1",
            "candidate_id": "cand-1",
            "duration_ms": 200000,
            "duration_delta_ms": 0,
            "retreivr_version": "0.9.16",
            "emitted_at": "2026-03-25T00:00:00+00:00",
        }
    ]

    merged, changed = community_publish_worker.merge_proposals_into_record(existing, proposals)

    assert changed is True
    assert merged["sources"][0]["source"] == "youtube"


def test_community_publish_worker_ingests_outbox_and_marks_rows_published(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    outbox_dir = tmp_path / "outbox"
    outbox_dir.mkdir(parents=True, exist_ok=True)
    proposal = {
        "schema_version": 1,
        "proposal_type": "community_cache_publish_proposal",
        "proposal_id": "proposal-1",
        "emitted_at": "2026-03-23T00:00:00+00:00",
        "recording_mbid": "aa11",
        "release_mbid": "rel-1",
        "release_group_mbid": "rg-1",
        "video_id": "vid-1",
        "source": "youtube",
        "candidate_url": "https://www.youtube.com/watch?v=vid-1",
        "candidate_id": "cand-1",
        "duration_ms": 200000,
        "selected_score": 0.97,
        "duration_delta_ms": 0,
        "final_path": "/downloads/Music/example.m4a",
        "retreivr_version": "0.9.15",
    }
    (outbox_dir / "community_publish_20260323.jsonl").write_text(
        json.dumps(proposal) + "\n",
        encoding="utf-8",
    )

    captured = {"puts": [], "pr_calls": 0}

    class FakePublisher:
        def __init__(self, *, repo, token, branch, target_branch, open_pr=True):
            self.repo = repo
            self.token = token
            self.branch = branch
            self.target_branch = target_branch
            self.open_pr = open_pr

        def get_open_pull_request(self):
            return None

        def ensure_branch(self, *, reset_existing=False):
            assert reset_existing is True
            return None

        def get_file(self, path):
            assert path == "youtube/recording/aa/aa11.json"
            return None, None

        def put_file(self, path, *, content, sha, message):
            captured["puts"].append({"path": path, "content": content, "sha": sha, "message": message})
            return "commit-sha-1"

        def ensure_pull_request(self):
            captured["pr_calls"] += 1
            return 42

    monkeypatch.setenv("RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN", "token-1")
    monkeypatch.setattr(community_publish_worker, "GitHubCommunityCachePublisher", FakePublisher)

    config = {
        "community_cache_publish_enabled": True,
        "community_cache_publish_mode": "write_outbox",
        "community_cache_publish_outbox_dir": str(outbox_dir),
        "community_cache_publish_repo": "sudoStacks/retreivr-community-cache",
        "community_cache_publish_target_branch": "main",
        "community_cache_publish_branch": "retreivr-community-publish/tester",
        "community_cache_publish_open_pr": True,
        "community_cache_publish_token_env": "RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN",
        "community_cache_publish_poll_minutes": 15,
        "community_cache_publish_batch_size": 25,
    }
    worker = community_publish_worker.CommunityPublishWorker(db_path=str(db_path), config_getter=lambda: dict(config))
    summary = worker.run_once()

    assert summary["status"] == "ok"
    assert summary["published_groups"] == 1
    assert summary["published_proposals"] == 1
    assert summary["pr_number"] == 42
    assert len(captured["puts"]) == 1
    sources = captured["puts"][0]["content"]["sources"]
    assert len(sources) == 1
    assert sources[0]["video_id"] == "vid-1"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT status, branch_name, pr_number, commit_sha FROM community_publish_queue WHERE proposal_id=?", ("proposal-1",))
        row = cur.fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == community_publish_worker.COMMUNITY_PUBLISH_STATUS_PUBLISHED
    assert row[1] == "retreivr-community-publish/tester"
    assert row[2] == 42
    assert row[3] == "commit-sha-1"


def test_community_publish_worker_resets_branch_when_no_open_pr(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    outbox_dir = tmp_path / "outbox"
    outbox_dir.mkdir(parents=True, exist_ok=True)
    proposal = {
        "schema_version": 1,
        "proposal_type": "community_cache_publish_proposal",
        "proposal_id": "proposal-reset-1",
        "emitted_at": "2026-03-26T00:00:00+00:00",
        "recording_mbid": "bb22",
        "release_mbid": "rel-2",
        "release_group_mbid": "rg-2",
        "video_id": "vid-2",
        "source": "youtube",
        "candidate_url": "https://www.youtube.com/watch?v=vid-2",
        "candidate_id": "cand-2",
        "duration_ms": 210000,
        "selected_score": 0.93,
        "duration_delta_ms": 0,
        "final_path": "/downloads/Music/example2.m4a",
        "retreivr_version": "0.9.17",
    }
    (outbox_dir / "community_publish_20260326.jsonl").write_text(json.dumps(proposal) + "\n", encoding="utf-8")

    captured = {"reset_existing": None}

    class FakePublisher:
        def __init__(self, *, repo, token, branch, target_branch, open_pr=True):
            self.branch = branch

        def get_open_pull_request(self):
            return None

        def ensure_branch(self, *, reset_existing=False):
            captured["reset_existing"] = reset_existing

        def get_file(self, path):
            return None, None

        def put_file(self, path, *, content, sha, message):
            return "commit-sha-2"

        def ensure_pull_request(self):
            return 43

    monkeypatch.setenv("RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN", "token-1")
    monkeypatch.setattr(community_publish_worker, "GitHubCommunityCachePublisher", FakePublisher)

    config = {
        "community_cache_publish_enabled": True,
        "community_cache_publish_mode": "write_outbox",
        "community_cache_publish_outbox_dir": str(outbox_dir),
        "community_cache_publish_repo": "sudoStacks/retreivr-community-cache",
        "community_cache_publish_target_branch": "main",
        "community_cache_publish_branch": "retreivr-community-publish/tester",
        "community_cache_publish_open_pr": True,
        "community_cache_publish_token_env": "RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN",
        "community_cache_publish_poll_minutes": 15,
        "community_cache_publish_batch_size": 25,
    }
    worker = community_publish_worker.CommunityPublishWorker(db_path=str(db_path), config_getter=lambda: dict(config))
    summary = worker.run_once()

    assert summary["status"] == "ok"
    assert captured["reset_existing"] is True
