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
        "recording_mbid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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
        "recording_mbid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
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


def test_merge_proposals_omits_unknown_or_zero_duration() -> None:
    proposal = {
        "recording_mbid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "video_id": "abc123DEF45",
        "source": "youtube",
        "selected_score": 0.95,
        "candidate_url": "https://www.youtube.com/watch?v=abc123DEF45",
        "duration_ms": 0,
        "emitted_at": "2026-08-03T00:00:00+00:00",
    }

    merged, changed = community_publish_worker.merge_proposals_into_record(None, [proposal])

    assert changed is True
    assert "duration_ms" not in merged["sources"][0]


def test_publish_proposal_validation_matches_public_dataset_contract() -> None:
    valid = {
        "proposal_id": "proposal-1",
        "recording_mbid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "video_id": "abc123DEF45",
        "source": "youtube",
        "candidate_url": "https://www.youtube.com/watch?v=abc123DEF45",
        "selected_score": 0.95,
        "emitted_at": "2026-08-03T00:00:00+00:00",
    }
    assert community_publish_worker._validate_publish_proposal(dict(valid)) == (True, None)
    assert community_publish_worker._validate_publish_proposal({**valid, "duration_ms": 0}) == (
        False,
        "invalid_duration_ms",
    )
    assert community_publish_worker._validate_publish_proposal({**valid, "video_id": "not-a-youtube-id"}) == (
        False,
        "invalid_video_id",
    )
    assert community_publish_worker._validate_publish_proposal({**valid, "source": "unknown"}) == (
        False,
        "unsupported_source",
    )
    assert community_publish_worker._validate_publish_proposal({**valid, "selected_score": 0.73}) == (
        False,
        "invalid_selected_score",
    )


def test_complete_dataset_record_is_validated_before_publish() -> None:
    record = {
        "schema_version": 1,
        "recording_mbid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "updated_at": "2026-08-03T00:00:00+00:00",
        "sources": [
            {
                "video_id": "abc123DEF45",
                "source": "youtube",
                "confidence": 0.95,
                "last_verified_at": "2026-08-03T00:00:00+00:00",
            }
        ],
    }
    assert community_publish_worker.validate_dataset_record(record) == (True, None)
    poisoned = json.loads(json.dumps(record))
    poisoned["sources"][0]["duration_ms"] = 0
    assert community_publish_worker.validate_dataset_record(poisoned) == (False, "invalid_duration_ms")


def test_community_publish_worker_ingests_outbox_and_marks_rows_published(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    outbox_dir = tmp_path / "outbox"
    outbox_dir.mkdir(parents=True, exist_ok=True)
    proposal = {
        "schema_version": 1,
        "proposal_type": "community_cache_publish_proposal",
        "proposal_id": "proposal-1",
        "emitted_at": "2026-03-23T00:00:00+00:00",
        "recording_mbid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "release_mbid": "rel-1",
        "release_group_mbid": "rg-1",
        "video_id": "abc123DEF45",
        "source": "youtube",
        "candidate_url": "https://www.youtube.com/watch?v=abc123DEF45",
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

    captured = {"puts": [], "pr_calls": 0, "pr_kwargs": None}

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
            assert path == "youtube/recording/aa/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json"
            return None, None

        def put_files(self, files, *, message):
            captured["puts"].append({"files": files, "message": message})
            return "commit-sha-1"

        def ensure_pull_request(self, **kwargs):
            captured["pr_calls"] += 1
            captured["pr_kwargs"] = kwargs
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
    published_files = captured["puts"][0]["files"]
    sources = published_files["youtube/recording/aa/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.json"]["sources"]
    assert len(sources) == 1
    assert sources[0]["video_id"] == "abc123DEF45"

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
    assert captured["pr_kwargs"] == {"recording_count": 1, "source_count": 1}


def test_community_publish_worker_resets_branch_when_no_open_pr(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    outbox_dir = tmp_path / "outbox"
    outbox_dir.mkdir(parents=True, exist_ok=True)
    proposal = {
        "schema_version": 1,
        "proposal_type": "community_cache_publish_proposal",
        "proposal_id": "proposal-reset-1",
        "emitted_at": "2026-03-26T00:00:00+00:00",
        "recording_mbid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "release_mbid": "rel-2",
        "release_group_mbid": "rg-2",
        "video_id": "xyz987ABC65",
        "source": "youtube",
        "candidate_url": "https://www.youtube.com/watch?v=xyz987ABC65",
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

        def put_files(self, files, *, message):
            return "commit-sha-2"

        def ensure_pull_request(self, **_kwargs):
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


def test_format_publish_pr_summary_includes_recording_and_source_counts() -> None:
    title, body = community_publish_worker.format_publish_pr_summary(
        branch="retreivr-community-publish/tester",
        recording_count=3,
        source_count=5,
    )

    assert title == "Retreivr community cache publish: 3 recordings, 5 sources"
    assert "Recordings updated: 3" in body
    assert "Source mappings included: 5" in body


def test_github_publisher_resets_existing_branch_via_git_refs_endpoint(monkeypatch) -> None:
    calls: list[tuple[str, str, dict | None]] = []

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
            self.status_code = status_code
            self._payload = payload or {}
            self.text = text

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self) -> None:
            self.headers = {}

        def request(self, method, url, params=None, json=None, timeout=None):
            path = url.replace(community_publish_worker.GITHUB_API_BASE, "")
            calls.append((method.upper(), path, json))
            if method.upper() == "GET" and path == "/repos/sudoStacks/retreivr-community-cache/git/ref/heads/main":
                return FakeResponse(200, {"object": {"sha": "target-sha"}})
            if method.upper() == "PATCH" and path == "/repos/sudoStacks/retreivr-community-cache/git/refs/heads/retreivr-community-publish/tester":
                return FakeResponse(200, {"object": {"sha": "target-sha"}})
            raise AssertionError(f"unexpected request {method} {path} {json}")

        def get(self, url, timeout=None, headers=None):
            path = url.replace(community_publish_worker.GITHUB_API_BASE, "")
            calls.append(("GET", path, None))
            if path == "/repos/sudoStacks/retreivr-community-cache/git/ref/heads/retreivr-community-publish/tester":
                return FakeResponse(200, {"object": {"sha": "old-sha"}})
            raise AssertionError(f"unexpected get {path}")

    monkeypatch.setattr(community_publish_worker.requests, "Session", FakeSession)
    publisher = community_publish_worker.GitHubCommunityCachePublisher(
        repo="sudoStacks/retreivr-community-cache",
        token="token",
        branch="retreivr-community-publish/tester",
        target_branch="main",
    )

    publisher.ensure_branch(reset_existing=True)

    assert ("PATCH", "/repos/sudoStacks/retreivr-community-cache/git/refs/heads/retreivr-community-publish/tester", {"sha": "target-sha", "force": True}) in calls


def test_github_publisher_writes_multiple_files_in_one_commit(monkeypatch) -> None:
    calls: list[tuple[str, str, dict | None]] = []

    class FakeResponse:
        def __init__(self, payload: dict) -> None:
            self.status_code = 200
            self._payload = payload
            self.text = ""

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self) -> None:
            self.headers = {}
            self.blob_count = 0

        def request(self, method, url, params=None, json=None, timeout=None):
            path = url.replace(community_publish_worker.GITHUB_API_BASE, "")
            calls.append((method.upper(), path, json))
            if method.upper() == "GET" and path.endswith("/git/ref/heads/retreivr-community-publish/tester"):
                return FakeResponse({"object": {"sha": "head-sha"}})
            if method.upper() == "GET" and path.endswith("/git/commits/head-sha"):
                return FakeResponse({"tree": {"sha": "base-tree-sha"}})
            if method.upper() == "POST" and path.endswith("/git/blobs"):
                self.blob_count += 1
                return FakeResponse({"sha": f"blob-{self.blob_count}"})
            if method.upper() == "POST" and path.endswith("/git/trees"):
                return FakeResponse({"sha": "new-tree-sha"})
            if method.upper() == "POST" and path.endswith("/git/commits"):
                return FakeResponse({"sha": "batch-commit-sha"})
            if method.upper() == "PATCH" and path.endswith("/git/refs/heads/retreivr-community-publish/tester"):
                return FakeResponse({"object": {"sha": "batch-commit-sha"}})
            raise AssertionError(f"unexpected request {method} {path} {json}")

    monkeypatch.setattr(community_publish_worker.requests, "Session", FakeSession)
    publisher = community_publish_worker.GitHubCommunityCachePublisher(
        repo="sudoStacks/retreivr-community-cache",
        token="token",
        branch="retreivr-community-publish/tester",
        target_branch="main",
    )

    commit_sha = publisher.put_files(
        {
            "youtube/recording/aa/a.json": {"recording_mbid": "a"},
            "youtube/recording/bb/b.json": {"recording_mbid": "b"},
        },
        message="batch",
    )

    assert commit_sha == "batch-commit-sha"
    assert len([call for call in calls if call[1].endswith("/git/blobs")]) == 2
    assert len([call for call in calls if call[1].endswith("/git/commits") and call[0] == "POST"]) == 1
    assert len([call for call in calls if call[1].endswith("/git/refs/heads/retreivr-community-publish/tester") and call[0] == "PATCH"]) == 1
