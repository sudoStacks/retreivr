from __future__ import annotations

import asyncio
import importlib
import json
import sqlite3
import sys
import tempfile
import threading
import types
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from engine import core as engine_core


def _load_api_main():
    sys.version_info = (3, 11, 0, "final", 0)  # type: ignore[assignment]
    sys.version = "3.11.9"
    sys.modules.pop("api.main", None)
    monkey_modules = {
        "google_auth_oauthlib": types.ModuleType("google_auth_oauthlib"),
        "google_auth_oauthlib.flow": types.ModuleType("google_auth_oauthlib.flow"),
        "googleapiclient": types.ModuleType("googleapiclient"),
        "googleapiclient.errors": types.ModuleType("googleapiclient.errors"),
        "google": types.ModuleType("google"),
        "google.auth": types.ModuleType("google.auth"),
        "google.auth.exceptions": types.ModuleType("google.auth.exceptions"),
        "musicbrainzngs": types.ModuleType("musicbrainzngs"),
    }
    monkey_modules["google_auth_oauthlib.flow"].InstalledAppFlow = object
    monkey_modules["googleapiclient.errors"].HttpError = Exception
    monkey_modules["google.auth.exceptions"].RefreshError = Exception
    for key, mod in monkey_modules.items():
        if key not in sys.modules:
            sys.modules[key] = mod
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return module


@pytest.fixture()
def api_module(monkeypatch, tmp_path: Path):
    module = _load_api_main()
    db_path = tmp_path / "regression_099.sqlite"
    config_path = tmp_path / "config.json"

    engine_core.init_db(str(db_path))
    module._ensure_watch_tables(str(db_path))

    module.app.state.paths = SimpleNamespace(db_path=str(db_path))
    module.app.state.config_path = str(config_path)
    module.app.state.loaded_config = {}
    module.app.state.config = {}
    module.app.state.status = module.get_status()
    module.app.state.schedule_lock = threading.Lock()
    module.app.state.schedule_last_run = None
    module.app.state.schedule_next_run = None
    module.app.state.schedule_config = module._default_schedule_config()
    module.app.state.single_worker_enforced = True
    module.app.state.watcher_lock = None
    module.app.state.watcher_task = None
    module.app.state.watcher_status = {
        "state": "disabled",
        "pending_playlists_count": 0,
        "batch_active": False,
    }

    monkeypatch.setattr(module, "validate_config", lambda _payload: [])
    monkeypatch.setattr(module, "_strip_deprecated_fields", lambda payload: payload)
    monkeypatch.setattr(module, "_apply_spotify_schedule", lambda _payload: None)
    monkeypatch.setattr(module, "_apply_schedule_config", lambda _schedule: None)

    async def _noop_disable(_reason=None):
        return None

    monkeypatch.setattr(module, "_disable_watcher_runtime", _noop_disable)
    monkeypatch.setattr(module, "_enable_watcher_runtime", lambda: None)

    return module


@pytest.fixture()
def api_client(api_module):
    return TestClient(api_module.app)


def test_scheduler_run_executes_and_updates_schedule_state(api_module, monkeypatch) -> None:
    module = api_module
    called = {"count": 0, "run_source": None}

    async def _fake_start(config, **kwargs):
        called["count"] += 1
        called["run_source"] = kwargs.get("run_source")
        return "started", None

    monkeypatch.setattr(module, "_playlist_imports_active", lambda: False)
    monkeypatch.setattr(module, "_read_config_for_scheduler", lambda: {"enable_watcher": False})
    monkeypatch.setattr(module, "_start_run_with_config", _fake_start)
    monkeypatch.setattr(module, "_get_next_run_iso", lambda: "2026-03-11T01:00:00+00:00")

    asyncio.run(module._handle_scheduled_run())

    assert called["count"] == 1
    assert called["run_source"] == "scheduled"
    assert module.app.state.schedule_last_run is not None
    assert module.app.state.schedule_next_run == "2026-03-11T01:00:00+00:00"


def test_watcher_detection_adds_pending_playlist_for_batch(api_module, monkeypatch) -> None:
    module = api_module
    now = datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc)
    policy = {
        "min_interval_minutes": 5,
        "max_interval_minutes": 60,
        "idle_backoff_factor": 2,
        "active_reset_minutes": 5,
        "downtime": {"enabled": False, "start": "23:00", "end": "09:00", "timezone": "UTC"},
    }
    playlist = {"playlist_id": "PL_DETECT_1", "account": "acct", "mode": "full"}
    watch = {}
    batch_state = {"pending_playlists": set(), "last_detection_ts": None, "batch_active": False}

    monkeypatch.setattr(module, "get_playlist_videos", lambda _yt, _playlist_id: [{"videoId": "vid001"}])

    asyncio.run(
        module._poll_single_playlist(
            {},
            now,
            policy,
            playlist,
            watch,
            {"acct": object()},
            batch_state,
        )
    )

    assert "PL_DETECT_1" in batch_state["pending_playlists"]
    assert batch_state["last_detection_ts"] is not None


def test_downtime_skip_deferred_run_sets_next_allowed_timestamp(api_module, monkeypatch) -> None:
    module = api_module
    next_allowed = datetime(2026, 3, 11, 8, 0, tzinfo=timezone.utc)

    async def _fake_start(_config, **_kwargs):
        return "deferred", next_allowed

    monkeypatch.setattr(module, "_playlist_imports_active", lambda: False)
    monkeypatch.setattr(module, "_read_config_for_scheduler", lambda: {"schedule": {"enabled": True}})
    monkeypatch.setattr(module, "_start_run_with_config", _fake_start)

    asyncio.run(module._handle_scheduled_run())

    assert module.app.state.schedule_next_run == next_allowed.isoformat()


def test_scheduled_tick_skips_before_run_when_downtime_active(api_module, monkeypatch) -> None:
    module = api_module
    next_allowed = datetime(2026, 3, 11, 8, 0, tzinfo=timezone.utc)
    called = {"start": 0}

    async def _fake_start(*_args, **_kwargs):
        called["start"] += 1
        return "started", None

    monkeypatch.setattr(module, "_playlist_imports_active", lambda: False)
    monkeypatch.setattr(module, "_read_config_for_scheduler", lambda: {"schedule": {"enabled": True}})
    monkeypatch.setattr(module, "_check_downtime", lambda _config, now=None: (True, next_allowed))
    monkeypatch.setattr(module, "_start_run_with_config", _fake_start)

    asyncio.run(module._handle_scheduled_run())

    assert called["start"] == 0
    assert module.app.state.schedule_next_run == next_allowed.isoformat()


def test_start_run_defers_watcher_source_during_downtime(api_module, monkeypatch) -> None:
    module = api_module
    next_allowed = datetime(2026, 3, 11, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(module, "_check_downtime", lambda _config, now=None: (True, next_allowed))

    result, deferred_until = asyncio.run(
        module._start_run_with_config(
            {"enable_watcher": True},
            run_source="watcher",
            now=datetime(2026, 3, 11, 7, 0, tzinfo=timezone.utc),
        )
    )

    assert result == "deferred"
    assert deferred_until == next_allowed
    assert module.app.state.running is False


def test_telegram_summary_content_includes_titles_for_scheduler_and_watcher(api_module, monkeypatch) -> None:
    module = api_module
    db_path = Path(module.app.state.paths.db_path)
    job_id = "0123456789abcdef0123456789abcdef"

    captured: list[str] = []

    def _fake_send(_config, message):
        captured.append(str(message))
        return {"ok": True, "message_id": 42}

    monkeypatch.setattr(module, "telegram_notify_result", _fake_send)

    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS download_jobs (
                id TEXT PRIMARY KEY,
                status TEXT,
                file_path TEXT,
                output_template TEXT,
                url TEXT
            )
            """
        )
        cur.execute(
            "INSERT OR REPLACE INTO download_jobs (id, status, file_path, output_template, url) VALUES (?, ?, ?, ?, ?)",
            (
                job_id,
                "completed",
                None,
                json.dumps({"track": "Regression Title"}),
                "https://www.youtube.com/watch?v=abc123xyz00",
            ),
        )
        conn.commit()

    status = SimpleNamespace(run_successes=[job_id], run_failures=[])
    scheduler_result = module.notify_run_summary(
        {},
        run_type="scheduled",
        status=status,
        started_at="2026-03-11T00:00:00+00:00",
        finished_at="2026-03-11T00:00:04+00:00",
    )
    watcher_result = module.notify_run_summary(
        {},
        run_type="watcher",
        status=status,
        started_at="2026-03-11T00:00:05+00:00",
        finished_at="2026-03-11T00:00:09+00:00",
    )

    assert scheduler_result["sent"] is True
    assert watcher_result["sent"] is True
    assert len(captured) == 2
    assert "Retreivr Scheduler Run Summary" in captured[0]
    assert "Retreivr Watcher Run Summary" in captured[1]
    assert "Regression Title" in captured[0]
    assert "Regression Title" in captured[1]
    assert job_id not in captured[0]
    assert job_id not in captured[1]


def test_playlist_add_persists_via_config_api(api_client: TestClient) -> None:
    payload = {
        "enable_watcher": True,
        "schedule": {"enabled": False, "mode": "interval", "interval_hours": 6, "run_on_startup": False},
        "playlists": [
            {
                "name": "Test Playlist",
                "playlist_id": "PL_ADD_001",
                "folder": "Videos/Test",
                "account": "",
                "final_format": "mp4",
                "media_mode": "video",
            }
        ],
    }

    put_resp = api_client.put("/api/config", json=payload)
    assert put_resp.status_code == 200

    get_resp = api_client.get("/api/config")
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert isinstance(body.get("playlists"), list)
    assert body["playlists"][0]["playlist_id"] == "PL_ADD_001"


def test_playlist_remove_persists_via_config_api(api_client: TestClient) -> None:
    initial = {
        "enable_watcher": False,
        "schedule": {"enabled": False, "mode": "interval", "interval_hours": 6, "run_on_startup": False},
        "playlists": [{"name": "Remove Me", "playlist_id": "PL_REMOVE_001"}],
    }
    cleared = {
        "enable_watcher": False,
        "schedule": {"enabled": False, "mode": "interval", "interval_hours": 6, "run_on_startup": False},
        "playlists": [],
    }

    assert api_client.put("/api/config", json=initial).status_code == 200
    assert api_client.put("/api/config", json=cleared).status_code == 200

    body = api_client.get("/api/config").json()
    assert body.get("playlists") == []


def test_config_save_load_roundtrip_preserves_critical_keys(api_client: TestClient) -> None:
    payload = {
        "enable_watcher": True,
        "watch_policy": {
            "min_interval_minutes": 5,
            "max_interval_minutes": 180,
            "idle_backoff_factor": 2,
            "active_reset_minutes": 5,
            "downtime": {"enabled": True, "start": "23:00", "end": "08:00", "timezone": "UTC"},
        },
        "schedule": {
            "enabled": True,
            "mode": "interval",
            "interval_hours": 4,
            "run_on_startup": True,
        },
        "telegram": {
            "enabled": True,
            "bot_token": "bot-token",
            "chat_id": "12345",
            "notify_on_success": True,
            "notify_on_failure": True,
        },
        "accounts": {
            "default": {
                "credentials": "tokens/default.json",
            }
        },
        "playlists": [
            {
                "name": "Roundtrip",
                "playlist_id": "PL_RT_001",
                "folder": "Videos/Roundtrip",
                "account": "default",
                "mode": "full",
                "media_mode": "video",
                "final_format": "mp4",
            }
        ],
    }

    put_resp = api_client.put("/api/config", json=payload)
    assert put_resp.status_code == 200

    get_resp = api_client.get("/api/config")
    assert get_resp.status_code == 200
    body = get_resp.json()

    assert body.get("enable_watcher") is True
    assert body.get("watch_policy", {}).get("downtime", {}).get("enabled") is True
    assert body.get("schedule", {}).get("interval_hours") == 4
    assert body.get("telegram", {}).get("enabled") is True
    assert body.get("accounts", {}).get("default", {}).get("credentials") == "tokens/default.json"
    assert body.get("playlists", [{}])[0].get("playlist_id") == "PL_RT_001"
