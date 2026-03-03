from __future__ import annotations

import importlib
import sqlite3
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    db_path = tmp_path / "music_failures.sqlite3"
    module.app.state.paths = SimpleNamespace(db_path=str(db_path))
    return TestClient(module.app), module, db_path


def _insert_failure(db_path, *, created_at: str, artist: str = "artist", track: str = "track"):
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS music_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                origin_batch_id TEXT,
                artist TEXT,
                track TEXT,
                reason_json TEXT,
                recording_mbid_attempted TEXT,
                last_query TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT INTO music_failures (
                created_at, origin_batch_id, artist, track, reason_json, recording_mbid_attempted, last_query
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (created_at, "batch-1", artist, track, '{"reasons":["test"]}', None, f"{artist} - {track}"),
        )
        conn.commit()
    finally:
        conn.close()


def _count_rows(db_path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM music_failures")
        return int((cur.fetchone() or [0])[0] or 0)
    finally:
        conn.close()


def test_delete_music_failures_clear_all(monkeypatch, tmp_path) -> None:
    client, _module, db_path = _build_client(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for i in range(3):
        _insert_failure(db_path, created_at=now, artist=f"artist-{i}", track=f"track-{i}")

    response = client.delete("/api/music/failures")
    payload = response.json()

    assert response.status_code == 200
    assert payload["deleted"] == 3
    assert payload["remaining"] == 0
    assert _count_rows(db_path) == 0


def test_delete_music_failures_keep_latest(monkeypatch, tmp_path) -> None:
    client, _module, db_path = _build_client(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for i in range(5):
        _insert_failure(db_path, created_at=now, artist=f"artist-{i}", track=f"track-{i}")

    response = client.delete("/api/music/failures?keep_latest=2")
    payload = response.json()

    assert response.status_code == 200
    assert payload["deleted"] == 3
    assert payload["remaining"] == 2
    assert _count_rows(db_path) == 2


def test_delete_music_failures_before(monkeypatch, tmp_path) -> None:
    client, _module, db_path = _build_client(monkeypatch, tmp_path)
    _insert_failure(db_path, created_at="2024-01-01T00:00:00+00:00", artist="old", track="old")
    _insert_failure(db_path, created_at="2026-01-01T00:00:00+00:00", artist="new", track="new")

    response = client.delete("/api/music/failures?before=2025-01-01T00:00:00Z")
    payload = response.json()

    assert response.status_code == 200
    assert payload["deleted"] == 1
    assert payload["remaining"] == 1
    assert _count_rows(db_path) == 1

