from __future__ import annotations

import base64
import json
import logging
import os
import re
import socket
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


logger = logging.getLogger(__name__)
_PUBLISH_IDENTITY_CACHE: dict[str, tuple[float, str]] = {}

DEFAULT_COMMUNITY_CACHE_PUBLISH_REPO = "sudostacks/retreivr-community-cache"
DEFAULT_COMMUNITY_CACHE_PUBLISH_TARGET_BRANCH = "main"
DEFAULT_COMMUNITY_CACHE_PUBLISH_POLL_MINUTES = 15
DEFAULT_COMMUNITY_CACHE_PUBLISH_OPEN_PR = True
DEFAULT_COMMUNITY_CACHE_PUBLISH_TOKEN_ENV = "RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN"
DEFAULT_COMMUNITY_CACHE_PUBLISH_BATCH_SIZE = 25
DEFAULT_COMMUNITY_CACHE_PUBLISH_BRANCH_PREFIX = "retreivr-community-publish"
DEFAULT_COMMUNITY_CACHE_PUBLISH_PUBLISHER = ""
GITHUB_API_BASE = "https://api.github.com"
COMMUNITY_PUBLISH_STATUS_PENDING = "pending"
COMMUNITY_PUBLISH_STATUS_PUBLISHED = "published"
COMMUNITY_PUBLISH_STATUS_SKIPPED = "skipped"
COMMUNITY_PUBLISH_STATUS_ERROR = "error"


def normalize_community_publish_source(value: Any) -> str:
    source = str(value or "").strip().lower()
    if source == "youtube_music":
        return "youtube"
    return source


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def apply_community_publish_defaults(config: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(config or {})
    cfg.setdefault("community_cache_publish_repo", DEFAULT_COMMUNITY_CACHE_PUBLISH_REPO)
    cfg.setdefault("community_cache_publish_target_branch", DEFAULT_COMMUNITY_CACHE_PUBLISH_TARGET_BRANCH)
    cfg.setdefault("community_cache_publish_branch", "")
    cfg.setdefault("community_cache_publish_open_pr", DEFAULT_COMMUNITY_CACHE_PUBLISH_OPEN_PR)
    cfg.setdefault("community_cache_publish_poll_minutes", DEFAULT_COMMUNITY_CACHE_PUBLISH_POLL_MINUTES)
    cfg.setdefault("community_cache_publish_token_env", DEFAULT_COMMUNITY_CACHE_PUBLISH_TOKEN_ENV)
    cfg.setdefault("community_cache_publish_batch_size", DEFAULT_COMMUNITY_CACHE_PUBLISH_BATCH_SIZE)
    cfg.setdefault("community_cache_publish_publisher", DEFAULT_COMMUNITY_CACHE_PUBLISH_PUBLISHER)
    return cfg


def _sanitize_publish_identity(value: Any) -> str:
    text = re.sub(r"[^a-z0-9._-]+", "-", str(value or "").strip().lower()).strip("./-")
    return text or ""


def resolve_publish_identity(config: dict[str, Any] | None) -> str:
    cfg = apply_community_publish_defaults(config)
    explicit = _sanitize_publish_identity(cfg.get("community_cache_publish_publisher"))
    if explicit:
        return explicit
    token = resolve_publish_token(cfg)
    if token:
        cached = _PUBLISH_IDENTITY_CACHE.get(token)
        now_ts = datetime.now(timezone.utc).timestamp()
        if cached and (now_ts - float(cached[0])) < 300:
            return cached[1]
        try:
            response = requests.get(
                f"{GITHUB_API_BASE}/user",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": "retreivr-community-publisher",
                },
                timeout=4,
            )
            response.raise_for_status()
            payload = response.json() if response.content else {}
            login = _sanitize_publish_identity((payload or {}).get("login"))
            if login:
                _PUBLISH_IDENTITY_CACHE[token] = (now_ts, login)
                return login
        except Exception:
            logger.debug("community_publish_identity_lookup_failed", exc_info=True)
    host = _sanitize_publish_identity(socket.gethostname())
    return host or "instance"


def normalize_publish_branch_name(config: dict[str, Any] | None) -> str:
    cfg = apply_community_publish_defaults(config)
    configured = str(cfg.get("community_cache_publish_branch") or "").strip()
    if configured:
        base = configured
    else:
        base = f"{DEFAULT_COMMUNITY_CACHE_PUBLISH_BRANCH_PREFIX}/{resolve_publish_identity(cfg)}"
    base = re.sub(r"[^A-Za-z0-9._/-]+", "-", base).strip("./-")
    return base or f"{DEFAULT_COMMUNITY_CACHE_PUBLISH_BRANCH_PREFIX}/instance"


def resolve_publish_token(config: dict[str, Any] | None) -> str:
    cfg = apply_community_publish_defaults(config)
    token_env = str(cfg.get("community_cache_publish_token_env") or "").strip()
    candidates = []
    if token_env:
        candidates.append(token_env)
    candidates.extend(["RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN", "GITHUB_TOKEN"])
    seen = set()
    for name in candidates:
        if not name or name in seen:
            continue
        seen.add(name)
        value = str(os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def community_publish_worker_enabled(config: dict[str, Any] | None) -> bool:
    cfg = apply_community_publish_defaults(config)
    enabled = bool(cfg.get("community_cache_publish_enabled", False))
    mode = str(cfg.get("community_cache_publish_mode") or "off").strip().lower()
    return enabled and mode == "write_outbox"


def resolve_publish_outbox_dir(config: dict[str, Any] | None, *, db_path: str) -> str:
    cfg = apply_community_publish_defaults(config)
    configured = str(cfg.get("community_cache_publish_outbox_dir") or "").strip()
    base_dir = os.path.join(os.path.dirname(db_path), "run_summaries")
    if not configured:
        return os.path.join(base_dir, "community_publish_outbox")
    if os.path.isabs(configured):
        return configured
    return str((Path(base_dir) / configured).resolve())


def _ensure_publish_queue_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS community_publish_queue (
            proposal_id TEXT PRIMARY KEY,
            recording_mbid TEXT NOT NULL,
            video_id TEXT NOT NULL,
            status TEXT NOT NULL,
            proposal_json TEXT NOT NULL,
            source_file TEXT,
            source_line INTEGER,
            ingested_at TEXT NOT NULL,
            published_at TEXT,
            branch_name TEXT,
            pr_number INTEGER,
            commit_sha TEXT,
            last_error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_community_publish_queue_status ON community_publish_queue (status, ingested_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_community_publish_queue_recording ON community_publish_queue (recording_mbid, status)"
    )
    conn.commit()


def _validate_publish_proposal(payload: dict[str, Any]) -> tuple[bool, str | None]:
    required = [
        "proposal_id",
        "recording_mbid",
        "video_id",
        "source",
        "candidate_url",
        "selected_score",
        "emitted_at",
    ]
    for key in required:
        value = payload.get(key)
        if value is None or str(value).strip() == "":
            return False, f"missing_{key}"
    try:
        score = float(payload.get("selected_score"))
    except (TypeError, ValueError):
        return False, "invalid_selected_score"
    if score < 0 or score > 1:
        return False, "invalid_selected_score"
    normalized_source = normalize_community_publish_source(payload.get("source"))
    if not normalized_source:
        return False, "missing_source"
    payload["source"] = normalized_source
    return True, None


def append_publish_proposal_to_outbox(
    *,
    config: dict[str, Any] | None,
    db_path: str,
    proposal: dict[str, Any],
) -> dict[str, Any]:
    valid, reason = _validate_publish_proposal(proposal if isinstance(proposal, dict) else {})
    if not valid:
        return {
            "status": "validation_failed",
            "reason": str(reason or "invalid_proposal"),
            "outbox_path": None,
        }
    outbox_dir = resolve_publish_outbox_dir(config, db_path=db_path)
    os.makedirs(outbox_dir, exist_ok=True)
    filename = f"community_publish_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"
    outbox_path = os.path.join(outbox_dir, filename)
    with open(outbox_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(proposal, sort_keys=True))
        handle.write("\n")
    return {
        "status": "written",
        "reason": "outbox_append_ok",
        "outbox_path": outbox_path,
    }


def summarize_publish_outbox(*, config: dict[str, Any] | None, db_path: str) -> dict[str, Any]:
    outbox_dir = resolve_publish_outbox_dir(config, db_path=db_path)
    root = Path(outbox_dir)
    files = sorted(root.glob("*.jsonl")) if root.exists() else []
    total_lines = 0
    recent_files: list[dict[str, Any]] = []
    for path in files:
        line_count = 0
        try:
            with path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    if raw_line.strip():
                        line_count += 1
        except Exception:
            line_count = 0
        total_lines += line_count
        recent_files.append(
            {
                "name": path.name,
                "path": str(path),
                "lines": line_count,
                "size_bytes": path.stat().st_size if path.exists() else 0,
            }
        )
    return {
        "outbox_dir": outbox_dir,
        "exists": root.exists(),
        "file_count": len(files),
        "proposal_lines": total_lines,
        "recent_files": recent_files[-5:],
    }


def summarize_publish_queue(*, db_path: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_publish_queue_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM community_publish_queue
            GROUP BY status
            """
        )
        counts = {str(row["status"] or ""): int(row["count"] or 0) for row in cur.fetchall()}
        cur.execute(
            """
            SELECT proposal_id, recording_mbid, video_id, status, ingested_at, published_at,
                   branch_name, pr_number, commit_sha, last_error, attempts
            FROM community_publish_queue
            ORDER BY COALESCE(ingested_at, '') DESC, proposal_id DESC
            LIMIT 20
            """
        )
        recent = [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()
    return {
        "counts": {
            "pending": int(counts.get(COMMUNITY_PUBLISH_STATUS_PENDING, 0)),
            "published": int(counts.get(COMMUNITY_PUBLISH_STATUS_PUBLISHED, 0)),
            "skipped": int(counts.get(COMMUNITY_PUBLISH_STATUS_SKIPPED, 0)),
            "error": int(counts.get(COMMUNITY_PUBLISH_STATUS_ERROR, 0)),
            "total": sum(int(value or 0) for value in counts.values()),
        },
        "recent": recent,
    }


def summarize_publish_runtime(
    *,
    config: dict[str, Any] | None,
    db_path: str,
    last_summary: dict[str, Any] | None = None,
    active_task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = apply_community_publish_defaults(config)
    token_env = str(cfg.get("community_cache_publish_token_env") or DEFAULT_COMMUNITY_CACHE_PUBLISH_TOKEN_ENV).strip()
    token_present = bool(resolve_publish_token(cfg))
    outbox = summarize_publish_outbox(config=cfg, db_path=db_path)
    queue = summarize_publish_queue(db_path=db_path)
    return {
        "enabled": bool(cfg.get("community_cache_publish_enabled", False)),
        "mode": str(cfg.get("community_cache_publish_mode") or "off").strip().lower(),
        "worker_enabled": community_publish_worker_enabled(cfg),
        "repo": str(cfg.get("community_cache_publish_repo") or DEFAULT_COMMUNITY_CACHE_PUBLISH_REPO).strip(),
        "target_branch": str(cfg.get("community_cache_publish_target_branch") or DEFAULT_COMMUNITY_CACHE_PUBLISH_TARGET_BRANCH).strip(),
        "branch": normalize_publish_branch_name(cfg),
        "poll_minutes": int(cfg.get("community_cache_publish_poll_minutes") or DEFAULT_COMMUNITY_CACHE_PUBLISH_POLL_MINUTES),
        "batch_size": int(cfg.get("community_cache_publish_batch_size") or DEFAULT_COMMUNITY_CACHE_PUBLISH_BATCH_SIZE),
        "publisher": resolve_publish_identity(cfg),
        "token_env": token_env,
        "token_present": token_present,
        "outbox": outbox,
        "queue": queue,
        "last_summary": dict(last_summary or {}) if isinstance(last_summary, dict) else None,
        "active_task": dict(active_task or {}) if isinstance(active_task, dict) else None,
    }


def _proposal_file_path(recording_mbid: str) -> str:
    normalized = str(recording_mbid or "").strip().lower()
    prefix = normalized[:2] if len(normalized) >= 2 else "xx"
    return f"youtube/recording/{prefix}/{normalized}.json"


def _source_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    confidence = item.get("confidence")
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError):
        confidence_value = 0.0
    return (-confidence_value, str(item.get("source") or ""), str(item.get("video_id") or ""))


def merge_proposals_into_record(
    existing: dict[str, Any] | None,
    proposals: list[dict[str, Any]],
) -> tuple[dict[str, Any], bool]:
    record: dict[str, Any] = {}
    if isinstance(existing, dict):
        record = json.loads(json.dumps(existing))
    sources = record.get("sources")
    if not isinstance(sources, list):
        sources = []
    normalized_sources: list[dict[str, Any]] = []
    for item in sources:
        if not isinstance(item, dict):
            continue
        current = dict(item)
        normalized_source = normalize_community_publish_source(current.get("source"))
        if normalized_source and current.get("source") != normalized_source:
            current["source"] = normalized_source
            changed = True
        normalized_sources.append(current)

    changed = False
    recording_mbid = None
    for proposal in proposals:
        recording_mbid = str(proposal.get("recording_mbid") or recording_mbid or "").strip().lower()
        entry = {
            "video_id": str(proposal.get("video_id") or "").strip(),
            "source": normalize_community_publish_source(proposal.get("source") or "youtube") or "youtube",
            "confidence": float(proposal.get("selected_score")),
            "duration_ms": proposal.get("duration_ms"),
            "candidate_url": str(proposal.get("candidate_url") or "").strip() or None,
            "candidate_id": str(proposal.get("candidate_id") or "").strip() or None,
            "duration_delta_ms": proposal.get("duration_delta_ms"),
            "retreivr_version": str(proposal.get("retreivr_version") or "").strip() or None,
            "last_verified_at": str(proposal.get("emitted_at") or "").strip() or utc_now(),
            "verified_by": str(proposal.get("verified_by") or "").strip() or "retreivr",
        }
        existing_idx = None
        for idx, current in enumerate(normalized_sources):
            if str(current.get("video_id") or "").strip() == entry["video_id"]:
                existing_idx = idx
                break
        if existing_idx is None:
            normalized_sources.append(entry)
            changed = True
            continue
        current = normalized_sources[existing_idx]
        if current != entry:
            normalized_sources[existing_idx] = entry
            changed = True

    normalized_sources.sort(key=_source_sort_key)
    if record.get("sources") != normalized_sources:
        changed = True
    record["schema_version"] = 1
    if recording_mbid:
        record["recording_mbid"] = recording_mbid
    record["updated_at"] = utc_now()
    record["sources"] = normalized_sources
    return record, changed


class GitHubCommunityCachePublisher:
    def __init__(self, *, repo: str, token: str, branch: str, target_branch: str, open_pr: bool = True):
        self.repo = str(repo or "").strip()
        self.token = str(token or "").strip()
        self.branch = str(branch or "").strip()
        self.target_branch = str(target_branch or "main").strip() or "main"
        self.open_pr = bool(open_pr)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "retreivr-community-publisher",
            }
        )

    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None, ok_statuses: tuple[int, ...] = (200, 201)) -> requests.Response:
        response = self.session.request(
            method.upper(),
            f"{GITHUB_API_BASE}{path}",
            params=params,
            json=json_body,
            timeout=20,
        )
        if response.status_code not in ok_statuses:
            raise RuntimeError(f"github_api_error status={response.status_code} path={path} body={response.text[:400]}")
        return response

    def ensure_branch(self, *, reset_existing: bool = False) -> None:
        target_ref = self._request("GET", f"/repos/{self.repo}/git/ref/heads/{self.target_branch}")
        target_sha = (((target_ref.json() or {}).get("object") or {}).get("sha") or "").strip()
        if not target_sha:
            raise RuntimeError("missing_target_branch_sha")
        branch_path = f"/repos/{self.repo}/git/ref/heads/{self.branch}"
        response = self.session.get(f"{GITHUB_API_BASE}{branch_path}", timeout=20, headers=self.session.headers)
        if response.status_code == 200:
            if reset_existing:
                self._request(
                    "PATCH",
                    branch_path.replace("/git/ref/", "/git/refs/"),
                    json_body={"sha": target_sha, "force": True},
                )
            return
        if response.status_code != 404:
            raise RuntimeError(f"github_api_error status={response.status_code} path={branch_path} body={response.text[:400]}")
        self._request(
            "POST",
            f"/repos/{self.repo}/git/refs",
            json_body={"ref": f"refs/heads/{self.branch}", "sha": target_sha},
        )

    def get_file(self, path: str) -> tuple[dict[str, Any] | None, str | None]:
        response = self.session.get(
            f"{GITHUB_API_BASE}/repos/{self.repo}/contents/{path}",
            params={"ref": self.branch},
            timeout=20,
            headers=self.session.headers,
        )
        if response.status_code == 404:
            return None, None
        if response.status_code != 200:
            raise RuntimeError(f"github_api_error status={response.status_code} path=/repos/{self.repo}/contents/{path} body={response.text[:400]}")
        data = response.json() or {}
        content = str(data.get("content") or "").strip()
        decoded = base64.b64decode(content).decode("utf-8") if content else "{}"
        return json.loads(decoded), str(data.get("sha") or "").strip() or None

    def put_file(self, path: str, *, content: dict[str, Any], sha: str | None, message: str) -> str:
        encoded = base64.b64encode(
            (json.dumps(content, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
        ).decode("ascii")
        body = {
            "message": message,
            "content": encoded,
            "branch": self.branch,
        }
        if sha:
            body["sha"] = sha
        response = self._request(
            "PUT",
            f"/repos/{self.repo}/contents/{path}",
            json_body=body,
        )
        payload = response.json() or {}
        commit = payload.get("commit") if isinstance(payload.get("commit"), dict) else {}
        commit_sha = str(commit.get("sha") or "").strip()
        if not commit_sha:
            raise RuntimeError("missing_commit_sha")
        return commit_sha

    def get_open_pull_request(self) -> int | None:
        if not self.open_pr:
            return None
        owner = self.repo.split("/", 1)[0]
        response = self._request(
            "GET",
            f"/repos/{self.repo}/pulls",
            params={"state": "open", "head": f"{owner}:{self.branch}", "base": self.target_branch},
            ok_statuses=(200,),
        )
        pulls = response.json() or []
        if isinstance(pulls, list) and pulls:
            try:
                return int(pulls[0].get("number"))
            except Exception:
                return None
        return None

    def ensure_pull_request(self) -> int | None:
        existing = self.get_open_pull_request()
        if existing is not None:
            return existing
        if not self.open_pr:
            return None
        title = f"Retreivr community cache publish: {self.branch}"
        body = (
            "Automated community cache publish proposals generated by Retreivr.\n\n"
            "This PR batches verified transport-resolution matches from the local outbox."
        )
        created = self._request(
            "POST",
            f"/repos/{self.repo}/pulls",
            json_body={
                "title": title,
                "head": self.branch,
                "base": self.target_branch,
                "body": body,
            },
        )
        try:
            return int((created.json() or {}).get("number"))
        except Exception:
            return None


class CommunityPublishWorker:
    def __init__(self, *, db_path: str, config_getter):
        self.db_path = str(db_path)
        self.config_getter = config_getter

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _ensure_publish_queue_table(conn)
        return conn

    def ingest_outbox(self, *, outbox_dir: str) -> dict[str, Any]:
        ingested = 0
        skipped = 0
        invalid = 0
        root = Path(outbox_dir)
        if not root.exists():
            return {"files_scanned": 0, "ingested": 0, "skipped_existing": 0, "invalid": 0}
        files = sorted(root.glob("*.jsonl"))
        conn = self._connect()
        try:
            for path in files:
                with path.open("r", encoding="utf-8") as handle:
                    for line_no, raw_line in enumerate(handle, start=1):
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            payload = json.loads(line)
                        except json.JSONDecodeError:
                            invalid += 1
                            continue
                        valid, reason = _validate_publish_proposal(payload if isinstance(payload, dict) else {})
                        if not valid:
                            invalid += 1
                            logger.warning("community_publish_outbox_invalid file=%s line=%s reason=%s", path, line_no, reason)
                            continue
                        proposal_id = str(payload.get("proposal_id") or "").strip()
                        try:
                            conn.execute(
                                """
                                INSERT INTO community_publish_queue (
                                    proposal_id,
                                    recording_mbid,
                                    video_id,
                                    status,
                                    proposal_json,
                                    source_file,
                                    source_line,
                                    ingested_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    proposal_id,
                                    str(payload.get("recording_mbid") or "").strip().lower(),
                                    str(payload.get("video_id") or "").strip(),
                                    COMMUNITY_PUBLISH_STATUS_PENDING,
                                    json.dumps(payload, sort_keys=True),
                                    str(path),
                                    int(line_no),
                                    utc_now(),
                                ),
                            )
                            ingested += 1
                        except sqlite3.IntegrityError:
                            skipped += 1
            conn.commit()
        finally:
            conn.close()
        return {
            "files_scanned": len(files),
            "ingested": ingested,
            "skipped_existing": skipped,
            "invalid": invalid,
        }

    def _pending_rows(self, conn: sqlite3.Connection, *, limit: int) -> list[sqlite3.Row]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM community_publish_queue
            WHERE status=?
            ORDER BY ingested_at ASC, proposal_id ASC
            LIMIT ?
            """,
            (COMMUNITY_PUBLISH_STATUS_PENDING, int(limit)),
        )
        return list(cur.fetchall())

    def run_once(self) -> dict[str, Any]:
        config = apply_community_publish_defaults(self.config_getter() or {})
        outbox_dir = resolve_publish_outbox_dir(config, db_path=self.db_path)
        ingest = self.ingest_outbox(outbox_dir=outbox_dir)
        summary = {
            "status": "idle",
            "outbox_dir": outbox_dir,
            "ingest": ingest,
            "published_groups": 0,
            "published_proposals": 0,
            "skipped_proposals": 0,
            "errors": 0,
            "branch": normalize_publish_branch_name(config),
            "pr_number": None,
        }
        if not community_publish_worker_enabled(config):
            summary["status"] = "disabled"
            return summary
        token = resolve_publish_token(config)
        if not token:
            summary["status"] = "waiting_for_token"
            return summary

        repo = str(config.get("community_cache_publish_repo") or DEFAULT_COMMUNITY_CACHE_PUBLISH_REPO).strip()
        target_branch = str(
            config.get("community_cache_publish_target_branch") or DEFAULT_COMMUNITY_CACHE_PUBLISH_TARGET_BRANCH
        ).strip()
        branch = normalize_publish_branch_name(config)
        batch_size = int(config.get("community_cache_publish_batch_size") or DEFAULT_COMMUNITY_CACHE_PUBLISH_BATCH_SIZE)
        publisher = GitHubCommunityCachePublisher(
            repo=repo,
            token=token,
            branch=branch,
            target_branch=target_branch,
            open_pr=bool(config.get("community_cache_publish_open_pr", True)),
        )

        conn = self._connect()
        try:
            pending = self._pending_rows(conn, limit=max(1, batch_size))
            if not pending:
                summary["status"] = "no_pending"
                return summary
            open_pr_number = publisher.get_open_pull_request()
            publisher.ensure_branch(reset_existing=open_pr_number is None)
            grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
            for row in pending:
                grouped[str(row["recording_mbid"] or "").strip().lower()].append(row)

            for recording_mbid, rows in grouped.items():
                path = _proposal_file_path(recording_mbid)
                proposals = []
                for row in rows:
                    try:
                        proposals.append(json.loads(str(row["proposal_json"] or "{}")))
                    except json.JSONDecodeError:
                        proposals.append({})
                try:
                    existing, sha = publisher.get_file(path)
                    merged, changed = merge_proposals_into_record(existing, proposals)
                    commit_sha = None
                    if changed or sha is None:
                        commit_sha = publisher.put_file(
                            path,
                            content=merged,
                            sha=sha,
                            message=f"retreivr: publish community cache for {recording_mbid}",
                        )
                    status = COMMUNITY_PUBLISH_STATUS_PUBLISHED
                    published_at = utc_now()
                    for row in rows:
                        conn.execute(
                            """
                            UPDATE community_publish_queue
                            SET status=?, published_at=?, branch_name=?, commit_sha=?, attempts=attempts+1, last_error=NULL
                            WHERE proposal_id=?
                            """,
                            (
                                status,
                                published_at,
                                branch,
                                commit_sha,
                                str(row["proposal_id"] or ""),
                            ),
                        )
                    conn.commit()
                    summary["published_groups"] += 1
                    summary["published_proposals"] += len(rows)
                except Exception as exc:
                    summary["errors"] += len(rows)
                    logger.exception("community_publish_group_failed recording_mbid=%s", recording_mbid)
                    for row in rows:
                        conn.execute(
                            """
                            UPDATE community_publish_queue
                            SET status=?, last_error=?, attempts=attempts+1
                            WHERE proposal_id=?
                            """,
                            (
                                COMMUNITY_PUBLISH_STATUS_ERROR,
                                str(exc),
                                str(row["proposal_id"] or ""),
                            ),
                        )
                    conn.commit()

            try:
                pr_number = open_pr_number if open_pr_number is not None else publisher.ensure_pull_request()
                summary["pr_number"] = pr_number
                if pr_number is not None:
                    conn.execute(
                        """
                        UPDATE community_publish_queue
                        SET pr_number=?
                        WHERE branch_name=? AND pr_number IS NULL
                        """,
                        (int(pr_number), branch),
                    )
                    conn.commit()
            except Exception:
                logger.exception("community_publish_pr_ensure_failed branch=%s", branch)
                summary["errors"] += 1

            summary["status"] = "ok" if summary["published_proposals"] > 0 else "no_changes"
            return summary
        finally:
            conn.close()
