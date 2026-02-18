from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace


engine_pkg = ModuleType("engine")
engine_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "engine")]
sys.modules.setdefault("engine", engine_pkg)

metadata_pkg = ModuleType("metadata")
metadata_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "metadata")]
sys.modules.setdefault("metadata", metadata_pkg)
metadata_queue_mod = ModuleType("metadata.queue")
setattr(metadata_queue_mod, "enqueue_metadata", lambda *_args, **_kwargs: None)
sys.modules.setdefault("metadata.queue", metadata_queue_mod)

google_mod = ModuleType("google")
google_auth_mod = ModuleType("google.auth")
google_auth_ex_mod = ModuleType("google.auth.exceptions")
setattr(google_auth_ex_mod, "RefreshError", Exception)
sys.modules.setdefault("google", google_mod)
sys.modules.setdefault("google.auth", google_auth_mod)
sys.modules.setdefault("google.auth.exceptions", google_auth_ex_mod)

from engine.job_queue import ensure_download_history_table, record_download_history


def test_migration_adds_channel_id_and_preserves_history(tmp_path) -> None:
    db_path = tmp_path / "legacy_history.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        # Legacy schema without channel_id column.
        cur.execute(
            """
            CREATE TABLE download_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT,
                title TEXT,
                filename TEXT,
                destination TEXT,
                source TEXT,
                status TEXT,
                created_at TEXT,
                completed_at TEXT,
                file_size_bytes INTEGER,
                input_url TEXT,
                canonical_url TEXT,
                external_id TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT INTO download_history (
                video_id, title, filename, destination, source, status,
                created_at, completed_at, file_size_bytes,
                input_url, canonical_url, external_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy123",
                "Legacy Title",
                "Legacy File.mp3",
                str(tmp_path),
                "youtube",
                "completed",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
                1234,
                "https://www.youtube.com/watch?v=legacy123",
                "https://www.youtube.com/watch?v=legacy123",
                "legacy123",
            ),
        )
        conn.commit()

        ensure_download_history_table(conn)

        columns = [row[1] for row in conn.execute("PRAGMA table_info(download_history)").fetchall()]
        assert "channel_id" in columns

        # Legacy row remains and channel_id is NULL/default.
        legacy_row = conn.execute(
            "SELECT video_id, title, channel_id FROM download_history WHERE video_id = ?",
            ("legacy123",),
        ).fetchone()
        assert legacy_row == ("legacy123", "Legacy Title", None)

        preserved_count = conn.execute("SELECT COUNT(*) FROM download_history").fetchone()[0]
        assert preserved_count == 1
    finally:
        conn.close()

    # New writes should populate channel_id.
    output_file = tmp_path / "new-track.mp3"
    output_file.write_bytes(b"audio-bytes")
    job = SimpleNamespace(
        id="job-new-1",
        url="https://www.youtube.com/watch?v=new123",
        input_url="https://www.youtube.com/watch?v=new123",
        external_id="new123",
        source="youtube",
        canonical_url="https://www.youtube.com/watch?v=new123",
        origin="single",
        origin_id="",
    )
    meta = {"video_id": "new123", "title": "New Track", "channel_id": "UC_NEW_CHANNEL"}
    record_download_history(str(db_path), job, str(output_file), meta=meta)

    conn = sqlite3.connect(str(db_path))
    try:
        total_count = conn.execute("SELECT COUNT(*) FROM download_history").fetchone()[0]
        assert total_count == 2

        new_row = conn.execute(
            """
            SELECT video_id, external_id, channel_id
            FROM download_history
            WHERE video_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            ("new123",),
        ).fetchone()
        assert new_row == ("new123", "new123", "UC_NEW_CHANNEL")
    finally:
        conn.close()
