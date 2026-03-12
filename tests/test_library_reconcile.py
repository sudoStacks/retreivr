from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path

google = types.ModuleType("google")
google_auth = types.ModuleType("google.auth")
google_auth_exceptions = types.ModuleType("google.auth.exceptions")
google_auth_transport = types.ModuleType("google.auth.transport")
google_auth_transport_requests = types.ModuleType("google.auth.transport.requests")
google_oauth2 = types.ModuleType("google.oauth2")
google_oauth2_credentials = types.ModuleType("google.oauth2.credentials")
googleapiclient = types.ModuleType("googleapiclient")
googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
googleapiclient_errors = types.ModuleType("googleapiclient.errors")


class _RefreshError(Exception):
    pass


google_auth_exceptions.RefreshError = _RefreshError
google_auth_transport_requests.Request = object
google_oauth2_credentials.Credentials = object
googleapiclient_discovery.build = object
googleapiclient_errors.HttpError = Exception
google_auth_transport.requests = google_auth_transport_requests
google_auth.exceptions = google_auth_exceptions
google_auth.transport = google_auth_transport
google.auth = google_auth
sys.modules.setdefault("google", google)
sys.modules.setdefault("google.auth", google_auth)
sys.modules.setdefault("google.auth.exceptions", google_auth_exceptions)
sys.modules.setdefault("google.auth.transport", google_auth_transport)
sys.modules.setdefault("google.auth.transport.requests", google_auth_transport_requests)
sys.modules.setdefault("google.oauth2", google_oauth2)
sys.modules.setdefault("google.oauth2.credentials", google_oauth2_credentials)
sys.modules.setdefault("googleapiclient", googleapiclient)
sys.modules.setdefault("googleapiclient.discovery", googleapiclient_discovery)
sys.modules.setdefault("googleapiclient.errors", googleapiclient_errors)

from engine.job_queue import ensure_download_history_table, ensure_download_jobs_table
from db.migrations import ensure_downloaded_music_tracks_table
from library import reconcile as reconcile_module


def _prepare_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_download_jobs_table(conn)
        ensure_download_history_table(conn)
        ensure_downloaded_music_tracks_table(conn)
    finally:
        conn.close()


def test_reconcile_music_library_backfills_jobs_history_and_isrc(tmp_path, monkeypatch) -> None:
    downloads_root = tmp_path / "downloads"
    music_root = downloads_root / "Music"
    track_path = music_root / "Artist" / "Album (2024)" / "01 - Song.mp3"
    track_path.parent.mkdir(parents=True, exist_ok=True)
    track_path.write_bytes(b"audio")

    db_path = tmp_path / "db.sqlite"
    _prepare_db(db_path)

    monkeypatch.setattr(reconcile_module, "DOWNLOADS_DIR", downloads_root)
    monkeypatch.setattr(
        reconcile_module,
        "_read_music_identity",
        lambda path: {
            "title": "Song",
            "artist": "Artist",
            "album": "Album",
            "album_artist": "Artist",
            "track_number": 1,
            "disc_number": 1,
            "recording_mbid": "rec-1",
            "mb_release_id": "rel-1",
            "isrc": "USABC1234567",
            "canonical_id": "music_track:rec-1:rel-1:d1:t1",
        },
    )

    first = reconcile_module.reconcile_music_library(
        db_path=str(db_path),
        config={"music_download_folder": "Music"},
    )
    second = reconcile_module.reconcile_music_library(
        db_path=str(db_path),
        config={"music_download_folder": "Music"},
    )

    assert first["jobs_inserted"] == 1
    assert first["history_inserted"] == 1
    assert first["isrc_records_inserted"] == 1
    assert second["jobs_inserted"] == 0
    assert second["history_inserted"] == 0
    assert second["isrc_records_inserted"] == 0
    assert second["skipped_existing_jobs"] == 1

    conn = sqlite3.connect(db_path)
    try:
        job_count = conn.execute("SELECT COUNT(*) FROM download_jobs").fetchone()[0]
        history_count = conn.execute("SELECT COUNT(*) FROM download_history").fetchone()[0]
        isrc_count = conn.execute("SELECT COUNT(*) FROM downloaded_music_tracks WHERE isrc=?", ("USABC1234567",)).fetchone()[0]
    finally:
        conn.close()

    assert job_count == 1
    assert history_count == 1
    assert isrc_count == 1


def test_reconcile_library_backfills_video_jobs_and_history(tmp_path, monkeypatch) -> None:
    downloads_root = tmp_path / "downloads"
    video_root = downloads_root / "Videos"
    video_path = video_root / "Channel" / "Clip.mp4"
    video_path.parent.mkdir(parents=True, exist_ok=True)
    video_path.write_bytes(b"video")

    db_path = tmp_path / "db.sqlite"
    _prepare_db(db_path)

    monkeypatch.setattr(reconcile_module, "DOWNLOADS_DIR", downloads_root)
    monkeypatch.setattr(
        reconcile_module,
        "_read_video_identity",
        lambda path: {
            "title": "Clip",
            "media_type": "video",
            "media_intent": "episode",
            "source": "youtube",
            "external_id": "abc123xyz89",
            "input_url": "https://www.youtube.com/watch?v=abc123xyz89",
            "canonical_url": "https://www.youtube.com/watch?v=abc123xyz89",
            "channel_id": "channel-1",
        },
    )

    first = reconcile_module.reconcile_library(
        db_path=str(db_path),
        config={"single_download_folder": "Videos"},
    )
    second = reconcile_module.reconcile_library(
        db_path=str(db_path),
        config={"single_download_folder": "Videos"},
    )

    assert first["jobs_inserted"] == 1
    assert first["history_inserted"] == 1
    assert first["video_files_seen"] == 1
    assert second["jobs_inserted"] == 0
    assert second["history_inserted"] == 0
    assert second["skipped_existing_jobs"] == 1

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT source, external_id, canonical_url, status FROM download_jobs LIMIT 1"
        ).fetchone()
        history_row = conn.execute(
            "SELECT source, input_url, canonical_url, channel_id, status FROM download_history LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert row == ("youtube", "abc123xyz89", "https://www.youtube.com/watch?v=abc123xyz89", "completed")
    assert history_row == (
        "youtube",
        "https://www.youtube.com/watch?v=abc123xyz89",
        "https://www.youtube.com/watch?v=abc123xyz89",
        "channel-1",
        "completed",
    )


def test_reconcile_library_skips_macos_appledouble_sidecars(tmp_path, monkeypatch) -> None:
    downloads_root = tmp_path / "downloads"
    video_root = downloads_root / "Videos"
    sidecar_path = video_root / "._Clip.webm"
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_bytes(b"\x00\x01\x02")

    db_path = tmp_path / "db.sqlite"
    _prepare_db(db_path)

    monkeypatch.setattr(reconcile_module, "DOWNLOADS_DIR", downloads_root)

    first = reconcile_module.reconcile_library(
        db_path=str(db_path),
        config={"single_download_folder": "Videos"},
    )

    assert first["files_seen"] == 0
    assert first["video_files_seen"] == 0
    assert first["errors"] == 0
    assert first["skipped_unsupported"] == 1
