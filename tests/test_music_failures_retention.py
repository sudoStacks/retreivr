from __future__ import annotations

import importlib.util
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _load_import_pipeline():
    module_path = Path(__file__).resolve().parents[1] / "engine" / "import_pipeline.py"
    spec = importlib.util.spec_from_file_location("test_import_pipeline_module", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        pytest.skip(f"optional dependency missing: {exc}")
    return module


def _count_failures(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM music_failures")
        return int((cur.fetchone() or [0])[0] or 0)
    finally:
        conn.close()


def test_record_music_failure_prunes_to_max_rows(tmp_path) -> None:
    import_pipeline = _load_import_pipeline()
    db_path = str(tmp_path / "music_failures.sqlite3")

    for idx in range(4):
        import_pipeline._record_music_failure(
            db_path=db_path,
            origin_batch_id="batch-1",
            artist="artist",
            track=f"track-{idx}",
            reasons=["reason"],
            last_query="artist - track",
            retention_max_rows=2,
            retention_max_age_days=365,
        )

    assert _count_failures(db_path) == 2


def test_record_music_failure_prunes_old_rows_by_age(tmp_path) -> None:
    import_pipeline = _load_import_pipeline()
    db_path = str(tmp_path / "music_failures.sqlite3")
    conn = sqlite3.connect(db_path)
    try:
        import_pipeline._ensure_music_failures_table(conn)
        cutoff_old = (datetime.now(timezone.utc) - timedelta(days=10)).replace(microsecond=0).isoformat()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO music_failures (
                created_at, origin_batch_id, artist, track, reason_json, recording_mbid_attempted, last_query
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (cutoff_old, "batch-old", "old artist", "old track", '{"reasons":["old"]}', None, "old query"),
        )
        conn.commit()
    finally:
        conn.close()

    import_pipeline._record_music_failure(
        db_path=db_path,
        origin_batch_id="batch-new",
        artist="new artist",
        track="new track",
        reasons=["new"],
        last_query="new query",
        retention_max_rows=100,
        retention_max_age_days=1,
    )

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT artist FROM music_failures ORDER BY id ASC")
        artists = [str(row[0] or "") for row in cur.fetchall()]
    finally:
        conn.close()

    assert artists == ["new artist"]
