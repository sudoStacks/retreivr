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
