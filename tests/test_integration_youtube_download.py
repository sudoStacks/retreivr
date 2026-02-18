from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@pytest.fixture()
def api_module(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "youtube_integration.sqlite"
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.paths = SimpleNamespace(
        db_path=str(db_path),
        temp_downloads_dir=str(tmp_path / "temp"),
        thumbs_dir=str(tmp_path / "thumbs"),
    )
    module.app.state.run_id = None
    return module, db_path, downloads_dir, tmp_path


@pytest.fixture()
def api_client(api_module) -> TestClient:
    module, _db, _downloads, _tmp = api_module
    return TestClient(module.app)


def test_direct_youtube_download_naming_collision_and_history_persist(
    api_module,
    api_client: TestClient,
    monkeypatch,
) -> None:
    module, db_path, downloads_dir, tmp_path = api_module
    source_url = "https://www.youtube.com/watch?v=abc123xyz99"
    video_id = "abc123xyz99"
    channel_id = "UC_FAKE_CHANNEL_001"

    monkeypatch.setattr(module, "_read_config_or_404", lambda: {"final_format": "mp3"})

    sanitize_calls = {"count": 0}
    from engine import job_queue as jq

    original_sanitize = jq.sanitize_for_filesystem

    def _sanitize_spy(value, maxlen=180):
        sanitize_calls["count"] += 1
        return original_sanitize(value, maxlen=maxlen)

    monkeypatch.setattr("engine.job_queue.sanitize_for_filesystem", _sanitize_spy)

    def _fake_download_with_ytdlp(*args, **kwargs):
        raw_temp_output = tmp_path / "raw-ytdlp-output.mp3"
        raw_temp_output.write_bytes(b"fake-mp3")
        meta = {
            "title": "Great Song (Official Video)",
            "channel": "Artist/Channel:Name",
            "upload_date": "20240201",
            "video_id": video_id,
            "channel_id": channel_id,
        }
        return str(raw_temp_output), meta

    monkeypatch.setattr(module, "download_with_ytdlp", _fake_download_with_ytdlp)

    async def _fake_start_run_with_config(
        config,
        *,
        single_url=None,
        playlist_id=None,
        playlist_account=None,
        playlist_mode=None,
        destination=None,
        final_format_override=None,
        js_runtime=None,
        music_mode=None,
        run_source="api",
        skip_downtime=False,
        run_id_override=None,
        now=None,
        delivery_mode=None,
    ):
        assert single_url == source_url
        assert delivery_mode == "server"
        raw_temp_output_str, meta = module.download_with_ytdlp(single_url)
        raw_temp_output = Path(raw_temp_output_str)

        # This naming path must strip source IDs and upload date by design.
        clean_name = module.build_output_filename(
            meta,
            fallback_id=video_id,
            ext="mp3",
            template=None,
            audio_mode=False,
        )
        base_target = downloads_dir / clean_name
        base_target.parent.mkdir(parents=True, exist_ok=True)
        base_target.write_bytes(b"existing-file")
        final_target = Path(module.resolve_collision_path(str(base_target)))

        module.atomic_move(str(raw_temp_output), str(final_target))

        job = SimpleNamespace(
            id="job-youtube-integration-1",
            url=single_url,
            input_url=single_url,
            source="youtube",
            external_id=video_id,
            canonical_url=module.canonicalize_url("youtube", single_url, video_id),
            origin="manual",
            origin_id="manual",
        )
        module.record_download_history(
            str(db_path),
            job,
            str(final_target),
            meta=meta,
        )

        module.app.state.run_id = "run-youtube-integration-1"
        return "started", None

    monkeypatch.setattr(module, "_start_run_with_config", _fake_start_run_with_config)

    response = api_client.post(
        "/api/run",
        json={
            "single_url": source_url,
            "delivery_mode": "server",
            "final_format_override": "mp3",
        },
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "started"
    assert body["run_id"] == "run-youtube-integration-1"

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            """
            SELECT video_id, channel_id, input_url, canonical_url, filename
            FROM download_history
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == video_id
    assert row[1] == channel_id
    assert row[2] == source_url
    assert row[3] == module.canonicalize_url("youtube", source_url, video_id)
    assert row[4].endswith(" (2).mp3")
    assert video_id not in row[4]
    assert "20240201" not in row[4]

    assert sanitize_calls["count"] > 0
