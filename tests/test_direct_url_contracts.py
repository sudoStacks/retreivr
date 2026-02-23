from __future__ import annotations

import importlib
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")


@pytest.fixture()
def api_module(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "direct_url_contracts.sqlite"
    temp_dir = tmp_path / "temp"
    thumbs_dir = tmp_path / "thumbs"
    temp_dir.mkdir(parents=True, exist_ok=True)
    thumbs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.paths = SimpleNamespace(
        db_path=str(db_path),
        temp_downloads_dir=str(temp_dir),
        thumbs_dir=str(thumbs_dir),
    )
    module.app.state.state = "idle"
    module.app.state.current_download_proc = None
    module.app.state.current_download_job_id = None
    return module


def test_client_direct_url_video_mode_does_not_coerce_audio_on_audio_override(
    api_module, monkeypatch, tmp_path: Path
) -> None:
    module = api_module
    config = {
        "final_format": "mkv",
        "filename_template": "VID-%(title)s__%(uploader)s.%(ext)s",
    }
    captured: dict = {}

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = config_arg
        captured.update(kwargs)
        output = Path(temp_dir) / "payload.mkv"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"video")
        return {
            "id": "abc123xyz99",
            "title": "Video Title",
            "uploader": "Channel Name",
            "webpage_url": url,
        }, str(output)

    monkeypatch.setattr(module, "get_loaded_config", lambda: config)
    monkeypatch.setattr(module, "download_with_ytdlp", _fake_download_with_ytdlp)
    monkeypatch.setattr(module, "embed_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_register_client_delivery",
        lambda path, filename: ("delivery-id", datetime.now(timezone.utc), object()),
    )

    result = module._run_immediate_download_to_client(
        url="https://youtu.be/-LI8X-GhFA8",
        config=config,
        paths=module.app.state.paths,
        media_type="video",
        media_intent="episode",
        final_format_override="mp3",
        stop_event=threading.Event(),
        status=None,
        origin="api",
    )

    assert captured.get("audio_mode") is False
    assert captured.get("media_type") == "video"
    assert captured.get("media_intent") == "episode"
    assert captured.get("final_format") == "mkv"
    assert result["filename"].startswith("VID-Video Title__Channel Name.")
    assert result["filename"].endswith(".mkv")


def test_server_direct_url_video_mode_uses_video_template_and_container_policy(
    api_module, monkeypatch, tmp_path: Path
) -> None:
    module = api_module
    destination = tmp_path / "downloads"
    destination.mkdir(parents=True, exist_ok=True)
    config = {
        "final_format": "mkv",
        "filename_template": "VID-%(title)s__%(uploader)s.%(ext)s",
    }
    captured: dict = {}

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = config_arg
        captured.update(kwargs)
        output = Path(temp_dir) / "payload.mkv"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"video")
        return {
            "id": "abc123xyz99",
            "title": "Video Title",
            "uploader": "Channel Name",
            "webpage_url": url,
        }, str(output)

    monkeypatch.setattr(module, "get_loaded_config", lambda: config)
    monkeypatch.setattr(module, "download_with_ytdlp", _fake_download_with_ytdlp)
    monkeypatch.setattr(module, "embed_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_record_direct_url_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "ensure_mb_bound_music_track", lambda *args, **kwargs: None)

    module._run_direct_url_with_cli(
        url="https://youtu.be/-LI8X-GhFA8",
        paths=module.app.state.paths,
        config=config,
        destination=str(destination),
        final_format_override="mp3",
        media_type="video",
        media_intent="episode",
        music_mode=False,
        stop_event=threading.Event(),
        status=None,
    )

    assert captured.get("audio_mode") is False
    assert captured.get("media_type") == "video"
    assert captured.get("media_intent") == "episode"
    assert captured.get("final_format") == "mkv"

    files = [p.name for p in destination.glob("*") if p.is_file()]
    assert len(files) == 1
    assert files[0].startswith("VID-Video Title__Channel Name.")
    assert files[0].endswith(".mkv")


def test_client_direct_url_music_mode_is_rejected(api_module) -> None:
    module = api_module
    config = {"music_final_format": "mp3"}
    with pytest.raises(RuntimeError, match="music_client_delivery_unsupported"):
        module._run_immediate_download_to_client(
            url="https://youtu.be/-LI8X-GhFA8",
            config=config,
            paths=module.app.state.paths,
            media_type="music",
            media_intent="music_track",
            final_format_override="mp3",
            stop_event=threading.Event(),
            status=None,
            origin="api",
        )


def test_server_direct_url_music_mode_enforces_mb_metadata_and_music_path(
    api_module, monkeypatch, tmp_path: Path
) -> None:
    module = api_module
    destination = tmp_path / "downloads_music"
    destination.mkdir(parents=True, exist_ok=True)
    config = {
        "final_format": "mkv",
        "music_final_format": "mp3",
    }

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = config_arg, kwargs
        output = Path(temp_dir) / "payload.mp3"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"audio")
        return {
            "id": "abc123xyz99",
            "title": "Transport Title",
            "uploader": "Uploader",
            "webpage_url": url,
        }, str(output)

    def _bind_stub(payload, *, config, country_preference="US"):
        _ = config, country_preference
        canonical = payload.setdefault("output_template", {}).setdefault("canonical_metadata", {})
        canonical.update(
            {
                "recording_mbid": "rec-1",
                "mb_release_id": "rel-1",
                "mb_release_group_id": "rg-1",
                "artist": "Canonical Artist",
                "track": "Canonical Track",
                "album": "Canonical Album",
                "release_date": "2010",
                "track_number": 1,
                "disc_number": 1,
            }
        )
        return canonical

    monkeypatch.setattr(module, "get_loaded_config", lambda: config)
    monkeypatch.setattr(module, "download_with_ytdlp", _fake_download_with_ytdlp)
    monkeypatch.setattr(module, "embed_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_record_direct_url_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "ensure_mb_bound_music_track", _bind_stub)
    monkeypatch.setattr(module, "enqueue_media_metadata", lambda *args, **kwargs: None)

    module._run_direct_url_with_cli(
        url="https://youtu.be/-LI8X-GhFA8",
        paths=module.app.state.paths,
        config=config,
        destination=str(destination),
        final_format_override="mp3",
        media_type="music",
        media_intent="music_track",
        music_mode=True,
        stop_event=threading.Event(),
        status=None,
    )

    files = [str(p.relative_to(destination)).replace("\\", "/") for p in destination.rglob("*") if p.is_file()]
    assert len(files) == 1
    assert files[0].startswith("Music/Canonical Artist/Canonical Album (2010)/Disc 1/")
    assert files[0].endswith("01 - Canonical Track.mp3")


def test_server_direct_url_music_mode_fails_when_mb_binding_incomplete(
    api_module, monkeypatch, tmp_path: Path
) -> None:
    module = api_module
    destination = tmp_path / "downloads_music_fail"
    destination.mkdir(parents=True, exist_ok=True)
    config = {
        "final_format": "mkv",
        "music_final_format": "mp3",
    }

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = config_arg, kwargs
        output = Path(temp_dir) / "payload.mp3"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"audio")
        return {
            "id": "abc123xyz99",
            "title": "Transport Title",
            "uploader": "Uploader",
            "webpage_url": url,
        }, str(output)

    monkeypatch.setattr(module, "get_loaded_config", lambda: config)
    monkeypatch.setattr(module, "download_with_ytdlp", _fake_download_with_ytdlp)
    monkeypatch.setattr(module, "_record_direct_url_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "ensure_mb_bound_music_track",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("music_track_requires_mb_bound_metadata")),
    )

    with pytest.raises(ValueError, match="music_track_requires_mb_bound_metadata"):
        module._run_direct_url_with_cli(
            url="https://youtu.be/-LI8X-GhFA8",
            paths=module.app.state.paths,
            config=config,
            destination=str(destination),
            final_format_override="mp3",
            media_type="music",
            media_intent="music_track",
            music_mode=True,
            stop_event=threading.Event(),
            status=None,
        )

    assert not any(destination.rglob("*"))
