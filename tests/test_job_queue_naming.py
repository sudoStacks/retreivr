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

musicbrainzngs_mod = ModuleType("musicbrainzngs")
sys.modules.setdefault("musicbrainzngs", musicbrainzngs_mod)

metadata_services_pkg = ModuleType("metadata.services")
setattr(metadata_services_pkg, "get_musicbrainz_service", lambda: None)
sys.modules.setdefault("metadata.services", metadata_services_pkg)

metadata_mb_service_mod = ModuleType("metadata.services.musicbrainz_service")
setattr(metadata_mb_service_mod, "get_musicbrainz_service", lambda: None)
sys.modules.setdefault("metadata.services.musicbrainz_service", metadata_mb_service_mod)

google_mod = ModuleType("google")
google_auth_mod = ModuleType("google.auth")
google_auth_ex_mod = ModuleType("google.auth.exceptions")
setattr(google_auth_ex_mod, "RefreshError", Exception)
sys.modules.setdefault("google", google_mod)
sys.modules.setdefault("google.auth", google_auth_mod)
sys.modules.setdefault("google.auth.exceptions", google_auth_ex_mod)

from engine.job_queue import (
    _enrich_info_from_sidecar,
    _hydrate_meta_from_local_filename,
    _hydrate_meta_from_output_template,
    _select_download_output,
    build_output_filename,
    record_download_history,
    resolve_collision_path,
    finalize_download_artifact,
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


def test_template_with_blank_tokens_falls_back_to_pretty_filename() -> None:
    name = build_output_filename(
        {"title": "Example Track", "channel": "Artist Channel", "upload_date": ""},
        "abc12345",
        "mp4",
        "%(title)s - %(uploader)s - %(upload_date)s.%(ext)s",
        False,
    )
    assert name == "Example Track - Artist Channel.mp4"


def test_hydrate_meta_from_output_template_and_local_filename() -> None:
    meta = {"video_id": "abc123xyz00"}
    hydrated = _hydrate_meta_from_output_template(
        meta,
        {"title": "Track Title", "channel": "Channel Name"},
    )
    hydrated = _hydrate_meta_from_local_filename(
        hydrated,
        local_file="Track Title - Channel Name - abc123xyz00.webm",
        fallback_id="abc123xyz00",
    )
    assert hydrated.get("title") == "Track Title"
    assert hydrated.get("channel") == "Channel Name"


def test_select_download_output_ignores_sidecar_files(tmp_path) -> None:
    media = tmp_path / "video.webm"
    media.write_bytes(b"media")
    sidecar = tmp_path / "video.info.json"
    sidecar.write_text('{"title":"x"}')
    thumbnail = tmp_path / "video.jpg"
    thumbnail.write_bytes(b"thumb")

    selected = _select_download_output(str(tmp_path), {"id": "abc"}, audio_mode=False)
    assert selected == str(media)


def test_enrich_info_from_sidecar_prefers_rich_sidecar(tmp_path) -> None:
    sidecar = tmp_path / "abc123.info.json"
    sidecar.write_text('{"id":"abc123","title":"Real Title","channel":"Real Channel","uploader":"Real Channel"}')

    enriched = _enrich_info_from_sidecar(
        {"id": "abc123", "title": "", "channel": ""},
        temp_dir=str(tmp_path),
        url="https://youtu.be/abc123",
        job_id="job-1",
    )

    assert enriched.get("title") == "Real Title"
    assert enriched.get("channel") == "Real Channel"


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


def test_finalize_audio_uses_actual_output_extension_over_config(tmp_path) -> None:
    local_file = tmp_path / "downloaded.mp3"
    local_file.write_bytes(b"audio-bytes")

    final_path, _meta = finalize_download_artifact(
        local_file=str(local_file),
        meta={
            "artist": "Artist",
            "album": "Album",
            "track": "Track",
            "track_number": 1,
            "disc_number": 1,
            "release_date": "2011",
            "mb_release_group_id": "rg-1",
        },
        fallback_id="abc123",
        destination_dir=str(tmp_path / "out"),
        audio_mode=True,
        final_format="m4a",
        template=None,
        paths=None,
        config={},
        enforce_music_contract=False,
        enqueue_audio_metadata=False,
    )

    assert final_path.endswith(".mp3")
    assert Path(final_path).exists()
