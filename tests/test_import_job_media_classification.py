from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys
import types

from metadata.importers.base import TrackIntent

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_import_pipeline():
    return _load_module("engine_import_pipeline_media_classification", _ROOT / "engine" / "import_pipeline.py")


def _load_job_queue():
    # Avoid importing engine.__init__ (which pulls optional Google deps) for this focused regression test.
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    return _load_module("engine_job_queue_media_classification", _ROOT / "engine" / "job_queue.py")


def test_import_job_media_type_and_ytdlp_audio_flags(monkeypatch, tmp_path: Path) -> None:
    pipeline = _load_import_pipeline()
    jq = _load_job_queue()
    monkeypatch.setattr(
        pipeline,
        "resolve_best_mb_pair",
        lambda *_args, **_kwargs: {
            "recording_mbid": "mbid-abc-123",
            "mb_release_id": "release-xyz",
            "mb_release_group_id": "release-group-xyz",
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
            "release_date": "2024",
            "track_number": 1,
            "disc_number": 1,
            "duration_ms": 210000,
        },
    )

    class _FakeMB:
        def search_recordings(self, artist, title, *, album=None, limit=5):
            return {
                "recording-list": [
                    {
                        "id": "mbid-abc-123",
                        "title": title,
                        "ext:score": "99",
                        "artist-credit": [{"name": artist}],
                        "release-list": [{"id": "release-xyz"}],
                    }
                ]
            }

    class _Queue:
        def __init__(self):
            self.payload = None

        def enqueue_job(self, **kwargs):
            self.payload = kwargs
            return "job-1", True, None

    queue = _Queue()
    result = pipeline.process_imported_tracks(
        [TrackIntent(artist="Artist", title="Song", album="Album", raw_line="", source_format="m3u")],
        {
            "musicbrainz_service": _FakeMB(),
            "queue_store": queue,
            "job_payload_builder": jq.build_download_job_payload,
            "app_config": {"final_format": "mp3"},
            "base_dir": str(tmp_path / "downloads"),
            "final_format": "mp3",
        },
    )

    assert result.enqueued_count == 1
    assert queue.payload is not None
    assert queue.payload["media_type"] == "music"
    assert queue.payload["media_intent"] == "music_track"
    assert jq.is_music_media_type(queue.payload["media_type"]) is True

    captured = {}

    def _fake_download_with_ytdlp(
        url,
        temp_dir,
        config,
        *,
        audio_mode,
        final_format,
        cookie_file=None,
        stop_event=None,
        media_type=None,
        media_intent=None,
        job_id=None,
        origin=None,
        resolved_destination=None,
        cancel_check=None,
        cancel_reason=None,
        output_template_meta=None,
    ):
        _ = output_template_meta
        context = {
            "operation": "download",
            "url": url,
            "audio_mode": audio_mode,
            "final_format": final_format,
            "output_template": str(Path(temp_dir) / "%(id)s.%(ext)s"),
            "media_type": media_type,
            "media_intent": media_intent,
            "origin": origin,
            "overrides": {},
        }
        opts = jq.build_ytdlp_opts(context)
        captured["audio_mode"] = audio_mode
        captured["final_format"] = final_format
        captured["media_type"] = media_type
        captured["opts"] = opts

        local_file = Path(temp_dir) / "test.mp3"
        local_file.write_bytes(b"abc")
        return {"title": "Song", "id": "vid123", "ext": "mp3"}, str(local_file)

    monkeypatch.setattr(jq, "download_with_ytdlp", _fake_download_with_ytdlp)
    monkeypatch.setattr(
        jq,
        "extract_meta",
        lambda info, fallback_url=None: {
            "title": "Song",
            "video_id": info.get("id"),
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
        },
    )
    monkeypatch.setattr(jq, "enqueue_media_metadata", lambda file_path, meta, config: None)

    paths = SimpleNamespace(
        single_downloads_dir=str(tmp_path / "downloads"),
        temp_downloads_dir=str(tmp_path / "temp"),
        thumbs_dir=str(tmp_path / "thumbs"),
    )

    job = SimpleNamespace(
        id="job-1",
        url=queue.payload["url"],
        origin=queue.payload["origin"],
        output_template=queue.payload["output_template"],
        media_type=queue.payload["media_type"],
        media_intent=queue.payload["media_intent"],
        resolved_destination=queue.payload["resolved_destination"],
    )

    adapter = jq.YouTubeAdapter()
    output = adapter.execute(
        job,
        {"final_format": "mp3"},
        paths,
        media_type=job.media_type,
        media_intent=job.media_intent,
    )

    assert output is not None
    assert captured["media_type"] == "music"
    assert captured["audio_mode"] is True
    assert captured["final_format"] == "mp3"
    assert captured["opts"]["addmetadata"] is True
    assert captured["opts"]["embedthumbnail"] is True
    assert captured["opts"]["writethumbnail"] is True
    assert captured["opts"]["format"] == "bestaudio/best"
    assert any(pp.get("key") == "FFmpegExtractAudio" for pp in (captured["opts"].get("postprocessors") or []))
