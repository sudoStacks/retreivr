from __future__ import annotations

import asyncio
import importlib
import os
import sqlite3
import sys
import tempfile
import types
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
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


def test_watcher_supervisor_task_done_schedules_restart_on_crash(monkeypatch) -> None:
    module = _load_api_main()

    scheduled = {"count": 0}

    class _Loop:
        def is_closed(self):
            return False

        def create_task(self, coro):
            scheduled["count"] += 1
            # Prevent un-awaited coroutine warnings in unit tests.
            coro.close()
            return SimpleNamespace()

    class _Task:
        def cancelled(self):
            return False

        def exception(self):
            return RuntimeError("watcher exploded")

    module.app.state.loop = _Loop()
    module.app.state.watcher_lock = 1
    module.app.state.watcher_task = object()
    module.app.state.watcher_status = {"state": "running_batch", "batch_active": True, "pending_playlists_count": 3}
    monkeypatch.setattr(module, "_ensure_watcher_lock_runtime", lambda: True)

    module._watcher_supervisor_task_done(_Task())

    assert scheduled["count"] == 1
    assert module.app.state.watcher_task is None
    assert module.app.state.watcher_status.get("state") == "recovering"
    assert module.app.state.watcher_status.get("batch_active") is False


def test_watcher_supervisor_task_done_ignores_cancelled_task() -> None:
    module = _load_api_main()

    class _Task:
        def cancelled(self):
            return True

        def exception(self):
            raise AssertionError("exception() should not be called when task is cancelled")

    module.app.state.watcher_task = "sentinel"
    module._watcher_supervisor_task_done(_Task())
    assert module.app.state.watcher_task == "sentinel"


def test_ensure_watcher_lock_runtime_recovers_invalid_handle(monkeypatch) -> None:
    module = _load_api_main()
    module.app.state.watcher_lock = "invalid-fd"
    monkeypatch.setattr(module, "_acquire_watcher_lock", lambda _lock_dir: 99)

    recovered = module._ensure_watcher_lock_runtime()

    assert recovered is True
    assert module.app.state.watcher_lock == 99


def test_ensure_watcher_lock_runtime_disables_when_recovery_fails(monkeypatch) -> None:
    module = _load_api_main()
    module.app.state.watcher_lock = "invalid-fd"
    monkeypatch.setattr(module, "_acquire_watcher_lock", lambda _lock_dir: None)

    recovered = module._ensure_watcher_lock_runtime()

    assert recovered is False
    assert module.app.state.watcher_lock is None


def test_api_put_config_hot_applies_watcher_enable_toggle(monkeypatch) -> None:
    module = _load_api_main()

    with tempfile.TemporaryDirectory() as tmpdir:
        module.app.state.config_path = os.path.join(tmpdir, "config.json")

        monkeypatch.setattr(module, "validate_config", lambda _payload: [])
        monkeypatch.setattr(module, "_strip_deprecated_fields", lambda payload: payload)
        monkeypatch.setattr(module, "_apply_spotify_schedule", lambda _payload: None)
        monkeypatch.setattr(module, "_merge_schedule_config", lambda value: value or {})
        monkeypatch.setattr(module, "_apply_schedule_config", lambda _schedule: None)

        policy = module._default_watch_policy()
        monkeypatch.setattr(module, "normalize_watch_policy", lambda _payload: policy)
        module.normalize_watch_policy.valid = True

        calls = {"enable": 0, "disable": 0}

        def _fake_enable():
            calls["enable"] += 1

        async def _fake_disable(_reason=None):
            calls["disable"] += 1

        monkeypatch.setattr(module, "_enable_watcher_runtime", _fake_enable)
        monkeypatch.setattr(module, "_disable_watcher_runtime", _fake_disable)

        asyncio.run(module.api_put_config({"enable_watcher": True, "watch_policy": policy}))
        asyncio.run(module.api_put_config({"enable_watcher": False, "watch_policy": policy}))

    assert calls["enable"] == 1
    assert calls["disable"] == 1


def test_send_watcher_batch_telegram_returns_message_id(monkeypatch) -> None:
    module = _load_api_main()

    def _fake_send(_config, _message):
        return {"ok": True, "message_id": 777}

    monkeypatch.setattr(module, "telegram_notify_result", _fake_send)

    result = module._send_watcher_batch_telegram({}, "test")
    assert result["sent"] is True
    assert result["message_id"] == 777


def test_restart_watcher_supervisor_respects_lock_recovery(monkeypatch) -> None:
    module = _load_api_main()
    starts = {"count": 0}

    monkeypatch.setattr(module, "_start_watcher_supervisor_task", lambda: starts.__setitem__("count", starts["count"] + 1))
    monkeypatch.setattr(module, "_ensure_watcher_lock_runtime", lambda: False)
    module.app.state.watcher_task = None
    module.app.state.watcher_status = {"state": "running_batch", "batch_active": True}

    asyncio.run(module._restart_watcher_supervisor(delay_seconds=0))

    assert starts["count"] == 0
    assert module.app.state.watcher_status.get("state") == "disabled"
    assert module.app.state.watcher_status.get("batch_active") is False


def test_watcher_skew_limit_allows_max_backoff_interval() -> None:
    module = _load_api_main()
    policy = {
        "min_interval_minutes": 5,
        "max_interval_minutes": 360,
        "idle_backoff_factor": 2,
        "active_reset_minutes": 5,
        "downtime": {"enabled": False, "start": "23:00", "end": "09:00", "timezone": "UTC"},
    }
    watch = {"current_interval_min": 360}
    skew_limit = module._watcher_next_poll_skew_limit_seconds(policy, watch)
    assert skew_limit >= 21600


def test_watcher_skew_limit_has_guardrail_without_watch_state() -> None:
    module = _load_api_main()
    policy = {
        "min_interval_minutes": 5,
        "max_interval_minutes": 360,
        "idle_backoff_factor": 2,
        "active_reset_minutes": 5,
        "downtime": {"enabled": False, "start": "23:00", "end": "09:00", "timezone": "UTC"},
    }
    skew_limit = module._watcher_next_poll_skew_limit_seconds(policy, {})
    assert skew_limit >= 900


def test_poll_single_playlist_uses_fallback_when_oauth_missing(monkeypatch) -> None:
    module = _load_api_main()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "watcher.sqlite")
        engine_core.init_db(db_path)
        module._ensure_watch_tables(db_path)
        module.app.state.paths = SimpleNamespace(db_path=db_path)

        calls = {"fallback": 0}

        def _fake_fallback(playlist_id, *, cookie_file=None):
            calls["fallback"] += 1
            assert playlist_id == "PL_PUBLIC_1"
            return ([{"videoId": "vid_new_1"}], False)

        monkeypatch.setattr(module, "get_playlist_videos_fallback", _fake_fallback)
        monkeypatch.setattr(module, "resolve_cookie_file", lambda _cfg: "/tokens/cookies.txt")

        batch_state = {"pending_playlists": set(), "last_detection_ts": None, "batch_active": False}
        policy = {
            "min_interval_minutes": 5,
            "max_interval_minutes": 60,
            "idle_backoff_factor": 2,
            "active_reset_minutes": 5,
            "downtime": {"enabled": False, "start": "23:00", "end": "09:00", "timezone": "UTC"},
        }
        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        pl = {"playlist_id": "PL_PUBLIC_1", "mode": "full"}
        watch = {}

        import asyncio

        asyncio.run(module._poll_single_playlist({}, now, policy, pl, watch, {}, batch_state))

        assert calls["fallback"] == 1
        assert "PL_PUBLIC_1" in batch_state["pending_playlists"]
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT next_poll_at, skip_reason, last_error FROM playlist_watch WHERE playlist_id=?", ("PL_PUBLIC_1",))
            row = cur.fetchone()
            assert row is not None


def test_poll_single_playlist_subscribe_detects_unseen_when_seen_appears_first(monkeypatch) -> None:
    module = _load_api_main()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "watcher.sqlite")
        engine_core.init_db(db_path)
        module._ensure_watch_tables(db_path)
        module.app.state.paths = SimpleNamespace(db_path=db_path)

        with sqlite3.connect(db_path) as conn:
            module.mark_video_seen(conn, "PL_SUB_1", "vid_seen_1", downloaded=False)
            conn.commit()

        monkeypatch.setattr(
            module,
            "get_playlist_videos",
            lambda _yt, _playlist_id: [
                {"videoId": "vid_seen_1"},
                {"videoId": "vid_new_2"},
            ],
        )

        batch_state = {"pending_playlists": set(), "last_detection_ts": None, "batch_active": False}
        policy = {
            "min_interval_minutes": 5,
            "max_interval_minutes": 60,
            "idle_backoff_factor": 2,
            "active_reset_minutes": 5,
            "downtime": {"enabled": False, "start": "23:00", "end": "09:00", "timezone": "UTC"},
        }
        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        pl = {"playlist_id": "PL_SUB_1", "mode": "subscribe", "account": "acc1"}
        watch = {}
        yt_clients = {"acc1": object()}

        asyncio.run(module._poll_single_playlist({}, now, policy, pl, watch, yt_clients, batch_state))

        assert "PL_SUB_1" in batch_state["pending_playlists"]


def test_poll_single_playlist_subscribe_marks_new_ids_seen_on_detection(monkeypatch) -> None:
    module = _load_api_main()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "watcher.sqlite")
        engine_core.init_db(db_path)
        module._ensure_watch_tables(db_path)
        module.app.state.paths = SimpleNamespace(db_path=db_path)

        monkeypatch.setattr(
            module,
            "get_playlist_videos",
            lambda _yt, _playlist_id: [
                {"videoId": "vid_new_1"},
                {"videoId": "vid_new_2"},
            ],
        )

        batch_state = {"pending_playlists": set(), "last_detection_ts": None, "batch_active": False}
        policy = {
            "min_interval_minutes": 5,
            "max_interval_minutes": 60,
            "idle_backoff_factor": 2,
            "active_reset_minutes": 5,
            "downtime": {"enabled": False, "start": "23:00", "end": "09:00", "timezone": "UTC"},
        }
        now = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        pl = {"playlist_id": "PL_SUB_2", "mode": "subscribe", "account": "acc1"}
        watch = {"last_checked_at": "2026-03-11T11:58:00+00:00", "next_poll_at": "2026-03-11T12:00:00+00:00"}
        yt_clients = {"acc1": object()}

        asyncio.run(module._poll_single_playlist({}, now, policy, pl, watch, yt_clients, batch_state))

        assert "PL_SUB_2" in batch_state["pending_playlists"]
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT video_id, downloaded FROM playlist_videos WHERE playlist_id=? ORDER BY video_id",
                ("PL_SUB_2",),
            )
            rows = cur.fetchall()
        assert rows == [("vid_new_1", 0), ("vid_new_2", 0)]


def test_reset_watch_state_for_startup_sets_immediate_poll_and_min_interval() -> None:
    module = _load_api_main()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "watcher.sqlite")
        engine_core.init_db(db_path)
        module._ensure_watch_tables(db_path)

        module._write_watch_state(
            db_path,
            "PL_STARTUP_1",
            last_checked_at="2026-03-11T10:00:00+00:00",
            next_poll_at="2026-03-11T23:00:00+00:00",
            idle_count=12,
            current_interval_min=360,
            consecutive_no_change=12,
            last_change_at="2026-03-10T20:00:00+00:00",
            skip_reason="poll error",
            last_error="oauth missing",
            last_error_at="2026-03-11T09:00:00+00:00",
        )

        policy = {
            "min_interval_minutes": 7,
            "max_interval_minutes": 360,
            "idle_backoff_factor": 2,
            "active_reset_minutes": 5,
            "downtime": {"enabled": False, "start": "23:00", "end": "09:00", "timezone": "UTC"},
        }
        playlists = [{"playlist_id": "PL_STARTUP_1"}]

        reset_count = module._reset_watch_state_for_startup(db_path, playlists, policy)
        assert reset_count == 1

        rows = module._read_watch_state(db_path)
        entry = rows["PL_STARTUP_1"]
        assert entry["current_interval_min"] == 7
        assert entry["consecutive_no_change"] == 0
        assert entry["idle_count"] == 0
        assert entry["skip_reason"] is None
        assert entry["last_error"] is None
        assert entry["next_poll_at"] is not None
