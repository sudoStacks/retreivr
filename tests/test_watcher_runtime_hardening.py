from __future__ import annotations

import asyncio
import importlib
import os
import sys
import tempfile
import types
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")


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

    module._watcher_supervisor_task_done(_Task())

    assert scheduled["count"] == 1
    assert module.app.state.watcher_task is None


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
