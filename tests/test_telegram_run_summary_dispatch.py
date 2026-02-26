from __future__ import annotations

import importlib
import sys
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
    module.app.state.run_summary_dispatch = {}
    return module


def test_successful_run_dispatches_exactly_one_summary(monkeypatch) -> None:
    module = _load_api_main()
    calls: list[dict] = []

    def _fake_notify(*_args, **kwargs):
        calls.append(dict(kwargs))
        return {"attempted": 1, "sent": True, "telegram_message_id": 101}

    monkeypatch.setattr(module, "notify_run_summary", _fake_notify)
    status = SimpleNamespace(run_successes=["a"], run_failures=[], summary_sent=False, telegram_message_id=None)

    first = module.dispatch_run_summary_once(
        {},
        run_type="api",
        run_id="run-1",
        status=status,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:10+00:00",
        last_error=None,
    )
    second = module.dispatch_run_summary_once(
        {},
        run_type="api",
        run_id="run-1",
        status=status,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:10+00:00",
        last_error=None,
    )

    assert len(calls) == 1
    assert first["summary_sent"] is True
    assert first["telegram_message_id"] == 101
    assert second["summary_sent"] is True
    assert getattr(status, "summary_sent") is True
    assert getattr(status, "telegram_message_id") == 101


def test_partial_failure_dispatches_exactly_one_summary(monkeypatch) -> None:
    module = _load_api_main()
    calls = {"count": 0}

    def _fake_notify(*_args, **_kwargs):
        calls["count"] += 1
        return {"attempted": 2, "sent": True, "telegram_message_id": 202}

    monkeypatch.setattr(module, "notify_run_summary", _fake_notify)
    status = SimpleNamespace(run_successes=["a"], run_failures=["b"], summary_sent=False, telegram_message_id=None)

    module.dispatch_run_summary_once(
        {},
        run_type="scheduled",
        run_id="run-2",
        status=status,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:10+00:00",
        last_error="partial_failure",
    )
    module.dispatch_run_summary_once(
        {},
        run_type="scheduled",
        run_id="run-2",
        status=status,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:10+00:00",
        last_error="partial_failure",
    )
    assert calls["count"] == 1


def test_crash_reentry_does_not_duplicate_for_same_run_id(monkeypatch) -> None:
    module = _load_api_main()
    calls = {"count": 0}

    def _fake_notify(*_args, **_kwargs):
        calls["count"] += 1
        return {"attempted": 1, "sent": True, "telegram_message_id": 303}

    monkeypatch.setattr(module, "notify_run_summary", _fake_notify)
    status_first = SimpleNamespace(run_successes=[], run_failures=["x"], summary_sent=False, telegram_message_id=None)
    status_after_restart = SimpleNamespace(run_successes=[], run_failures=["x"], summary_sent=False, telegram_message_id=None)

    module.dispatch_run_summary_once(
        {},
        run_type="watcher",
        run_id="run-3",
        status=status_first,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:10+00:00",
        last_error="crash",
    )
    module.dispatch_run_summary_once(
        {},
        run_type="watcher",
        run_id="run-3",
        status=status_after_restart,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:15+00:00",
        last_error="crash",
    )

    assert calls["count"] == 1


def test_retry_flow_still_sends_single_summary(monkeypatch) -> None:
    module = _load_api_main()
    calls = {"count": 0}

    def _fake_notify(*_args, **_kwargs):
        calls["count"] += 1
        return {"attempted": 1, "sent": True, "telegram_message_id": 404}

    monkeypatch.setattr(module, "notify_run_summary", _fake_notify)
    status = SimpleNamespace(run_successes=["ok"], run_failures=[], summary_sent=False, telegram_message_id=None)

    module.dispatch_run_summary_once(
        {},
        run_type="api",
        run_id="run-4",
        status=status,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:10+00:00",
        last_error=None,
    )
    module.dispatch_run_summary_once(
        {},
        run_type="api",
        run_id="run-4",
        status=status,
        started_at="2026-02-26T00:00:00+00:00",
        finished_at="2026-02-26T00:00:11+00:00",
        last_error=None,
    )

    assert calls["count"] == 1
