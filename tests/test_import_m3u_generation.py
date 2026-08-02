from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path
import sys

_MODULE_PATH = Path(__file__).resolve().parent.parent / "engine" / "import_m3u_builder.py"
_SPEC = importlib.util.spec_from_file_location("engine_import_m3u_builder", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
write_import_m3u_from_batch = _MODULE.write_import_m3u_from_batch


def _create_history_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE download_history (
                output_path TEXT,
                import_batch_id TEXT,
                status TEXT,
                source_index INTEGER
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_write_import_m3u_from_batch_orders_by_source_index_and_deduplicates(tmp_path) -> None:
    db_path = tmp_path / "history.sqlite"
    playlist_root = tmp_path / "Playlists"
    _create_history_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO download_history (output_path, import_batch_id, status, source_index)
            VALUES (?, ?, ?, ?)
            """,
            [
                ("/downloads/Music/A/Album/Disc 1/02 - B.mp3", "batch-1", "completed", 2),
                ("/downloads/Music/A/Album/Disc 1/01 - A.mp3", "batch-1", "completed", 1),
                ("/downloads/Music/A/Album/Disc 1/01 - A.mp3", "batch-1", "completed", 1),
                ("/downloads/Music/A/Album/Disc 1/03 - C.mp3", "batch-1", "failed", 3),
                ("/downloads/Music/A/Album/Disc 1/99 - Z.mp3", "batch-2", "completed", 1),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    count = write_import_m3u_from_batch(
        import_batch_id="batch-1",
        playlist_name="My Import",
        db_path=str(db_path),
        playlist_root=playlist_root,
    )

    assert count == 2
    m3u_path = playlist_root / "My Import.m3u"
    assert m3u_path.exists() is True
    assert m3u_path.read_text(encoding="utf-8").splitlines() == [
        "#EXTM3U",
        "/downloads/Music/A/Album/Disc 1/01 - A.mp3",
        "/downloads/Music/A/Album/Disc 1/02 - B.mp3",
    ]


def test_write_import_m3u_from_batch_returns_zero_without_completed_rows(tmp_path) -> None:
    db_path = tmp_path / "history.sqlite"
    playlist_root = tmp_path / "Playlists"
    _create_history_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO download_history (output_path, import_batch_id, status, source_index)
            VALUES (?, ?, ?, ?)
            """,
            ("/downloads/Music/A/Album/Disc 1/01 - A.mp3", "batch-x", "failed", 1),
        )
        conn.commit()
    finally:
        conn.close()

    count = write_import_m3u_from_batch(
        import_batch_id="batch-x",
        playlist_name="Batch X",
        db_path=str(db_path),
        playlist_root=playlist_root,
    )
    assert count == 0
    assert (playlist_root / "Batch X.m3u").exists() is False


def test_write_import_m3u_uses_linked_jobs_and_keeps_source_order(tmp_path) -> None:
    db_path = tmp_path / "jobs.sqlite"
    playlist_root = tmp_path / "Playlists"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE import_batch_items (
                batch_id TEXT,
                source_index INTEGER,
                linked_job_id TEXT
            );
            CREATE TABLE download_jobs (
                id TEXT PRIMARY KEY,
                status TEXT,
                file_path TEXT
            );
            """
        )
        conn.executemany(
            "INSERT INTO import_batch_items (batch_id, source_index, linked_job_id) VALUES (?, ?, ?)",
            [("batch-linked", 2, "job-b"), ("batch-linked", 1, "job-a"), ("batch-linked", 3, "job-c")],
        )
        conn.executemany(
            "INSERT INTO download_jobs (id, status, file_path) VALUES (?, ?, ?)",
            [
                ("job-a", "completed", "/music/A/01 - First.mp3"),
                ("job-b", "completed", "/music/A/02 - Second.mp3"),
                ("job-c", "queued", None),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    count = write_import_m3u_from_batch(
        import_batch_id="batch-linked",
        playlist_name="Linked Import",
        db_path=str(db_path),
        playlist_root=playlist_root,
    )

    assert count == 2
    assert (playlist_root / "Linked Import.m3u").read_text(encoding="utf-8").splitlines() == [
        "#EXTM3U",
        "/music/A/01 - First.mp3",
        "/music/A/02 - Second.mp3",
    ]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        playlist = conn.execute(
            "SELECT id, name, import_batch_id FROM music_player_playlists WHERE import_batch_id=?",
            ("batch-linked",),
        ).fetchone()
        assert playlist is not None
        assert playlist["name"] == "Linked Import"
        items = conn.execute(
            "SELECT title, artist, album, local_path, position FROM music_player_playlist_items "
            "WHERE playlist_id=? ORDER BY position",
            (playlist["id"],),
        ).fetchall()
    finally:
        conn.close()
    assert [item["local_path"] for item in items] == [
        "/music/A/01 - First.mp3",
        "/music/A/02 - Second.mp3",
    ]
    assert [item["position"] for item in items] == [1, 2]

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO music_player_playlist_items (
                playlist_id, item_id, title, source_kind, position
            ) VALUES (?, 'manual-item', 'Manual Track', 'local', 99)
            """,
            (playlist["id"],),
        )
        conn.commit()
    finally:
        conn.close()

    write_import_m3u_from_batch(
        import_batch_id="batch-linked",
        playlist_name="Linked Import",
        db_path=str(db_path),
        playlist_root=playlist_root,
    )
    conn = sqlite3.connect(db_path)
    try:
        item_ids = [
            row[0]
            for row in conn.execute(
                "SELECT item_id FROM music_player_playlist_items WHERE playlist_id=? ORDER BY position",
                (playlist["id"],),
            ).fetchall()
        ]
    finally:
        conn.close()
    assert item_ids == ["job-a", "job-b", "manual-item"]
