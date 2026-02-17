from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import ModuleType


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

from engine.job_queue import ensure_download_jobs_table


def test_download_job_duplicate_detection_indexes_exist(tmp_path) -> None:
    db_path = tmp_path / "jobs.sqlite"
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_download_jobs_table(conn)
        rows = conn.execute("PRAGMA index_list(download_jobs)").fetchall()
    finally:
        conn.close()

    index_names = {row[1] for row in rows}
    assert "idx_download_jobs_canonical_dest_status_created" in index_names
    assert "idx_download_jobs_url_dest_status_created" in index_names
