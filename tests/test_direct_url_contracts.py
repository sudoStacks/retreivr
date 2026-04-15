from __future__ import annotations

import importlib
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@pytest.fixture()
def api_module(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "direct_url_contracts.sqlite"
    temp_dir = tmp_path / "temp"
    thumbs_dir = tmp_path / "thumbs"
    downloads_dir = tmp_path / "downloads"
    temp_dir.mkdir(parents=True, exist_ok=True)
    thumbs_dir.mkdir(parents=True, exist_ok=True)
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
        temp_downloads_dir=str(temp_dir),
        single_downloads_dir=str(downloads_dir),
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


def test_client_direct_url_music_mode_returns_client_delivery(api_module, monkeypatch) -> None:
    module = api_module
    config = {"music_final_format": "mp3"}

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = url, config_arg, kwargs
        output = Path(temp_dir) / "payload.mp3"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"audio")
        return {
            "id": "abc123xyz99",
            "title": "Transport Title",
            "uploader": "Uploader",
            "webpage_url": "https://youtu.be/-LI8X-GhFA8",
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
    monkeypatch.setattr(module, "ensure_mb_bound_music_track", _bind_stub)
    monkeypatch.setattr(module, "enqueue_media_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_register_client_delivery",
        lambda path, filename, cleanup_dir=None: ("delivery-id", datetime.now(timezone.utc), object()),
    )

    result = module._run_immediate_download_to_client(
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

    assert result["delivery_id"] == "delivery-id"
    assert result["filename"].endswith(".mp3")


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
    assert files[0].startswith("Canonical Artist/Canonical Album (2010)/")
    assert "/Disc 1/" not in files[0]
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


def test_server_direct_url_relative_destination_resolves_within_downloads_root(
    api_module, monkeypatch
) -> None:
    module = api_module
    config = {
        "final_format": "mkv",
        "filename_template": "VID-%(title)s__%(uploader)s.%(ext)s",
    }

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = url, config_arg, kwargs
        output = Path(temp_dir) / "payload.mkv"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"video")
        return {
            "id": "abc123xyz99",
            "title": "Video Title",
            "uploader": "Channel Name",
            "webpage_url": "https://youtu.be/-LI8X-GhFA8",
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
        destination="Singles",
        final_format_override=None,
        media_type="video",
        media_intent="episode",
        music_mode=False,
        stop_event=threading.Event(),
        status=None,
    )

    target_dir = Path(module.app.state.paths.single_downloads_dir) / "Singles"
    files = [p.name for p in target_dir.glob("*") if p.is_file()]
    assert len(files) == 1
    assert files[0].startswith("VID-Video Title__Channel Name.")
    assert files[0].endswith(".mkv")


def test_server_direct_url_default_destination_uses_downloads_root(
    api_module, monkeypatch
) -> None:
    module = api_module
    config = {
        "final_format": "mkv",
        "filename_template": "VID-%(title)s__%(uploader)s.%(ext)s",
    }

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = url, config_arg, kwargs
        output = Path(temp_dir) / "payload.mkv"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"video")
        return {
            "id": "abc123xyz99",
            "title": "Video Title",
            "uploader": "Channel Name",
            "webpage_url": "https://youtu.be/-LI8X-GhFA8",
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
        destination=None,
        final_format_override=None,
        media_type="video",
        media_intent="episode",
        music_mode=False,
        stop_event=threading.Event(),
        status=None,
    )

    target_dir = Path(module.app.state.paths.single_downloads_dir)
    files = [p.name for p in target_dir.glob("*") if p.is_file()]
    assert len(files) == 1
    assert files[0].startswith("VID-Video Title__Channel Name.")
    assert files[0].endswith(".mkv")


def test_server_direct_url_absolute_destination_within_downloads_root_is_allowed(
    api_module, monkeypatch
) -> None:
    module = api_module
    config = {
        "final_format": "mkv",
        "filename_template": "VID-%(title)s__%(uploader)s.%(ext)s",
    }
    absolute_target = Path(module.app.state.paths.single_downloads_dir) / "Singles"

    def _fake_download_with_ytdlp(url, temp_dir, config_arg, **kwargs):
        _ = url, config_arg, kwargs
        output = Path(temp_dir) / "payload.mkv"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"video")
        return {
            "id": "abc123xyz99",
            "title": "Video Title",
            "uploader": "Channel Name",
            "webpage_url": "https://youtu.be/-LI8X-GhFA8",
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
        destination=str(absolute_target),
        final_format_override=None,
        media_type="video",
        media_intent="episode",
        music_mode=False,
        stop_event=threading.Event(),
        status=None,
    )

    files = [p.name for p in absolute_target.glob("*") if p.is_file()]
    assert len(files) == 1
    assert files[0].startswith("VID-Video Title__Channel Name.")
    assert files[0].endswith(".mkv")


def test_server_direct_url_destination_escape_is_rejected(
    api_module, monkeypatch
) -> None:
    module = api_module
    config = {"final_format": "mkv"}

    monkeypatch.setattr(module, "get_loaded_config", lambda: config)

    with pytest.raises(ValueError, match="Path must be within base directory"):
        module._run_direct_url_with_cli(
            url="https://youtu.be/-LI8X-GhFA8",
            paths=module.app.state.paths,
            config=config,
            destination="../outside",
            final_format_override=None,
            media_type="video",
            media_intent="episode",
            music_mode=False,
            stop_event=threading.Event(),
            status=None,
        )


def test_direct_url_resolve_returns_home_result_for_single_video(api_module, monkeypatch) -> None:
    module = api_module
    monkeypatch.setattr(module, "get_loaded_config", lambda: {"final_format": "mkv"})
    monkeypatch.setattr(
        module,
        "preview_direct_url",
        lambda url, _config: {
            "title": "Resolved Video",
            "uploader": "Resolved Channel",
            "thumbnail_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
            "url": url,
            "source": "youtube",
            "duration_sec": 95,
        },
    )
    client = TestClient(module.app)

    response = client.post("/api/direct-url/resolve", json={"url": "https://www.youtube.com/watch?v=stub123", "media_mode": "video"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["result_type"] == "home_result"
    assert payload["home_item"]["track"] == "Resolved Video"
    assert payload["home_candidates"][0]["title"] == "Resolved Video"


def test_direct_url_resolve_returns_music_album_for_playlist(api_module, monkeypatch) -> None:
    module = api_module
    monkeypatch.setattr(module, "get_loaded_config", lambda: {"final_format": "mkv"})
    monkeypatch.setattr(
        module,
        "get_playlist_preview_fallback",
        lambda playlist_id, cookie_file=None: (
            {
                "playlist_title": f"Playlist {playlist_id}",
                "thumbnail_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
                "first_video_id": "stub123",
            },
            None,
        ),
    )
    client = TestClient(module.app)

    response = client.post(
        "/api/direct-url/resolve",
        json={"url": "https://www.youtube.com/playlist?list=PLstub", "media_mode": "music"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["result_type"] == "music_album"
    assert payload["music_album"]["playlist_id"] == "PLstub"
    assert payload["music_album"]["title"] == "Playlist PLstub"
