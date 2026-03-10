from __future__ import annotations

import importlib
import json
import sqlite3
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


def test_notify_run_summary_uses_run_type_header_and_resolves_track_labels(monkeypatch) -> None:
    module = _load_api_main()
    captured = {"message": None}

    def _fake_send(_config, message):
        captured["message"] = str(message)
        return {"ok": True, "message_id": 909}

    monkeypatch.setattr(module, "telegram_notify_result", _fake_send)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/db.sqlite"
        conn = sqlite3.connect(db_path)
        try:
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
                "INSERT INTO download_jobs (id, status, file_path, output_template, url) VALUES (?, ?, ?, ?, ?)",
                (
                    "0123456789abcdef0123456789abcdef",
                    "completed",
                    None,
                    json.dumps({"track": "Alcohol"}),
                    "https://www.youtube.com/watch?v=abc123xyz00",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        module.app.state.paths = SimpleNamespace(db_path=db_path)
        status = SimpleNamespace(run_successes=["0123456789abcdef0123456789abcdef"], run_failures=[])
        result = module.notify_run_summary(
            {},
            run_type="scheduled",
            status=status,
            started_at="2026-02-26T00:00:00+00:00",
            finished_at="2026-02-26T00:00:10+00:00",
        )

    msg = captured["message"] or ""
    assert result["sent"] is True
    assert "Retreivr Scheduler Run Summary" in msg
    assert "YouTube Playlist Download Attempts" in msg
    assert "Attempted successes: 1" in msg
    assert "Attempted failures: 0" in msg
    assert "Attempted:" in msg
    assert "Alcohol" in msg
    assert "0123456789abcdef0123456789abcdef" not in msg


def test_notify_run_summary_resolves_video_titles_from_download_history_for_scheduler_and_watcher(monkeypatch) -> None:
    module = _load_api_main()
    captured_messages: list[str] = []

    def _fake_send(_config, message):
        captured_messages.append(str(message))
        return {"ok": True, "message_id": 910}

    monkeypatch.setattr(module, "telegram_notify_result", _fake_send)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/db.sqlite"
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS download_jobs (
                    id TEXT PRIMARY KEY,
                    status TEXT,
                    file_path TEXT,
                    output_template TEXT,
                    url TEXT,
                    external_id TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS download_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT,
                    title TEXT,
                    completed_at TEXT
                )
                """
            )
            cur.execute(
                "INSERT INTO download_jobs (id, status, file_path, output_template, url, external_id) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "completed",
                    None,
                    "{}",
                    "https://www.youtube.com/watch?v=KhIjNfUCKUQ",
                    "KhIjNfUCKUQ",
                ),
            )
            cur.execute(
                "INSERT INTO download_history (video_id, title, completed_at) VALUES (?, ?, ?)",
                ("KhIjNfUCKUQ", "My Real Video Title", "2026-02-26T00:00:01+00:00"),
            )
            conn.commit()
        finally:
            conn.close()

        module.app.state.paths = SimpleNamespace(db_path=db_path)
        for run_type in ("scheduled", "watcher"):
            status = SimpleNamespace(run_successes=["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"], run_failures=[])
            result = module.notify_run_summary(
                {},
                run_type=run_type,
                status=status,
                started_at="2026-02-26T00:00:00+00:00",
                finished_at="2026-02-26T00:00:10+00:00",
            )
            assert result["sent"] is True

    assert len(captured_messages) == 2
    assert all("My Real Video Title" in msg for msg in captured_messages)
    assert all("YouTube Video (KhIjNfUCKUQ)" not in msg for msg in captured_messages)


def test_notify_run_summary_skips_when_only_enqueued_not_attempted(monkeypatch) -> None:
    module = _load_api_main()
    captured = {"called": 0}

    def _fake_send(_config, _message):
        captured["called"] += 1
        return {"ok": True, "message_id": 919}

    monkeypatch.setattr(module, "telegram_notify_result", _fake_send)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/db.sqlite"
        conn = sqlite3.connect(db_path)
        try:
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
                "INSERT INTO download_jobs (id, status, file_path, output_template, url) VALUES (?, ?, ?, ?, ?)",
                (
                    "fedcba9876543210fedcba9876543210",
                    "queued",
                    None,
                    json.dumps({"track": "No Attempt Yet"}),
                    "https://www.youtube.com/watch?v=queued12345",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        module.app.state.paths = SimpleNamespace(db_path=db_path)
        status = SimpleNamespace(run_successes=["fedcba9876543210fedcba9876543210"], run_failures=[])
        result = module.notify_run_summary(
            {},
            run_type="scheduled",
            status=status,
            started_at="2026-02-26T00:00:00+00:00",
            finished_at="2026-02-26T00:00:10+00:00",
        )

    assert result["sent"] is False
    assert result["attempted"] == 0
    assert captured["called"] == 0


def test_should_dispatch_run_summary_skips_watcher_only() -> None:
    module = _load_api_main()
    assert module._should_dispatch_run_summary("watcher") is False
    assert module._should_dispatch_run_summary("scheduled") is True
    assert module._should_dispatch_run_summary("api") is True


def test_dispatch_run_summary_dedupes_across_registry_reset_via_db(monkeypatch) -> None:
    module = _load_api_main()
    calls = {"count": 0}

    def _fake_notify(*_args, **_kwargs):
        calls["count"] += 1
        return {"attempted": 1, "sent": True, "telegram_message_id": 111}

    monkeypatch.setattr(module, "notify_run_summary", _fake_notify)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/db.sqlite"
        conn = sqlite3.connect(db_path)
        conn.close()
        module.app.state.paths = SimpleNamespace(db_path=db_path)

        status_first = SimpleNamespace(run_successes=["ok"], run_failures=[], summary_sent=False, telegram_message_id=None)
        status_second = SimpleNamespace(run_successes=["ok"], run_failures=[], summary_sent=False, telegram_message_id=None)

        first = module.dispatch_run_summary_once(
            {},
            run_type="scheduled",
            run_id="persisted-run-1",
            status=status_first,
            started_at="2026-02-26T00:00:00+00:00",
            finished_at="2026-02-26T00:00:10+00:00",
            last_error=None,
        )
        assert first["summary_sent"] is True
        assert calls["count"] == 1

        module.app.state.run_summary_dispatch = {}

        second = module.dispatch_run_summary_once(
            {},
            run_type="scheduled",
            run_id="persisted-run-1",
            status=status_second,
            started_at="2026-02-26T00:00:00+00:00",
            finished_at="2026-02-26T00:00:10+00:00",
            last_error=None,
        )

    assert calls["count"] == 1
    assert second["summary_sent"] is True
