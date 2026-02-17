from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
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

from engine.job_queue import (
    build_output_filename,
    record_download_history,
    resolve_collision_path,
)


def test_video_filename_omits_id_and_upload_date() -> None:
    name = build_output_filename(
        {
            "title": "Example Track",
            "channel": "Artist Channel",
            "upload_date": "20240131",
        },
        "abc12345",
        "mp4",
        None,
        False,
    )
    assert name == "Example Track - Artist Channel.mp4"
    assert "abc12345" not in name
    assert "20240131" not in name


def test_template_id_and_date_tokens_are_blank() -> None:
    name = build_output_filename(
        {"title": "Song", "channel": "Artist", "upload_date": "20240131"},
        "vid123",
        "webm",
        "%(title)s__%(id)s__%(upload_date)s.%(ext)s",
        False,
    )
    assert name == "Song____.webm"
    assert "vid123" not in name
    assert "20240131" not in name


def test_collision_path_appends_counter(tmp_path) -> None:
    first = tmp_path / "Track.mp3"
    first.write_bytes(b"a")
    second = tmp_path / "Track (2).mp3"
    second.write_bytes(b"b")

    resolved = resolve_collision_path(str(first))
    assert resolved == str(tmp_path / "Track (3).mp3")


def test_record_download_history_persists_channel_id(tmp_path) -> None:
    db_path = str(tmp_path / "downloads.db")
    file_path = tmp_path / "out.mp3"
    file_path.write_bytes(b"audio")

    job = SimpleNamespace(
        id="job1",
        url="https://www.youtube.com/watch?v=xyz987",
        input_url="https://www.youtube.com/watch?v=xyz987",
        external_id="xyz987",
        source="youtube",
        canonical_url="https://www.youtube.com/watch?v=xyz987",
        origin="single",
        origin_id="",
    )
    meta = {"video_id": "xyz987", "title": "Song", "channel_id": "UC123456"}

    record_download_history(db_path, job, str(file_path), meta=meta)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT video_id, external_id, channel_id FROM download_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert row == ("xyz987", "xyz987", "UC123456")
