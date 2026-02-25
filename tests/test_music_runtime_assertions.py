from __future__ import annotations

import importlib.util
import tempfile
import sys
import types
from pathlib import Path
from types import SimpleNamespace


_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_job_queue():
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
    if "metadata.services" not in sys.modules:
        services_pkg = types.ModuleType("metadata.services")
        services_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["metadata.services"] = services_pkg
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_module = types.ModuleType("metadata.services.musicbrainz_service")
        mb_module.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_module
    if "engine.musicbrainz_binding" not in sys.modules:
        binding_module = types.ModuleType("engine.musicbrainz_binding")
        binding_module.resolve_best_mb_pair = lambda *args, **kwargs: None
        binding_module._normalize_title_for_mb_lookup = lambda value, **kwargs: str(value or "")
        sys.modules["engine.musicbrainz_binding"] = binding_module
    return _load_module("engine_job_queue_runtime_assertions", _ROOT / "engine" / "job_queue.py")


def test_music_mode_track_download_format_selection() -> None:
    jq = _load_job_queue()
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mp3",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {"music_final_format": "mp3", "final_format": "mkv"},
        "config": {"music_final_format": "mp3", "final_format": "mkv"},
        "overrides": {},
    }
    opts = jq.build_ytdlp_opts(context)
    assert context.get("audio_mode") is True
    assert opts.get("format") == "bestaudio/best"
    assert "merge_output_format" not in opts
    postprocessors = opts.get("postprocessors") or []
    extract_pp = next((pp for pp in postprocessors if pp.get("key") == "FFmpegExtractAudio"), None)
    assert extract_pp is not None
    assert extract_pp.get("preferredcodec") == "mp3"
    assert extract_pp.get("preferredquality") == "0"


def test_metadata_probe_fallback_retries_with_best(tmp_path, monkeypatch) -> None:
    jq = _load_job_queue()
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    fake_output = temp_dir / "download.mp3"
    fake_output.write_bytes(b"ok")

    seen_probe_formats: list[str] = []

    class _FakeYDL:
        def __init__(self, opts):
            self.opts = dict(opts or {})

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download=False):
            _ = url, download
            probe_format = str(self.opts.get("format") or "")
            seen_probe_formats.append(probe_format)
            if probe_format == "bestaudio/best":
                raise RuntimeError("first_probe_failed")
            return {"id": "vid-1", "title": "Song", "requested_downloads": [{"filepath": str(fake_output)}]}

    monkeypatch.setattr(jq, "YoutubeDL", _FakeYDL)
    monkeypatch.setattr(jq.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr=""))
    monkeypatch.setattr(jq, "_select_download_output", lambda *_args, **_kwargs: str(fake_output))

    info, local_file = jq.download_with_ytdlp(
        "https://www.youtube.com/watch?v=abc123xyz00",
        str(temp_dir),
        {},
        audio_mode=True,
        final_format="mp3",
        media_type="music",
        media_intent="music_track",
        job_id="job-1",
        origin="test",
    )

    assert info is not None
    assert local_file == str(fake_output)
    assert seen_probe_formats[:2] == ["bestaudio/best", "best"]


def test_metadata_probe_fallback_fails_after_second_probe(tmp_path, monkeypatch) -> None:
    jq = _load_job_queue()
    temp_dir = tmp_path / "tmp2"
    temp_dir.mkdir(parents=True, exist_ok=True)

    class _AlwaysFailYDL:
        def __init__(self, opts):
            self.opts = dict(opts or {})

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download=False):
            _ = url, download, self.opts
            raise RuntimeError("probe_failed")

    monkeypatch.setattr(jq, "YoutubeDL", _AlwaysFailYDL)
    monkeypatch.setattr(jq.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr=""))

    try:
        jq.download_with_ytdlp(
            "https://www.youtube.com/watch?v=abc123xyz00",
            str(temp_dir),
            {},
            audio_mode=True,
            final_format="mp3",
            media_type="music",
            media_intent="music_track",
            job_id="job-2",
            origin="test",
        )
        assert False, "expected RuntimeError from probe failure after retry"
    except RuntimeError as exc:
        assert "yt_dlp_metadata_probe_failed" in str(exc)


def test_music_retry_escalates_query_rung_and_records_duration_filtered(monkeypatch) -> None:
    jq = _load_job_queue()
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "db.sqlite")
        paths = jq.EnginePaths(
            log_dir=tmp,
            db_path=db_path,
            temp_downloads_dir=tmp,
            single_downloads_dir=tmp,
            lock_file=str(Path(tmp) / "retreivr.lock"),
            ytdlp_temp_dir=tmp,
            thumbs_dir=tmp,
        )

        class _FakeSearchService:
            def __init__(self):
                self.calls = []
                self.last_music_track_search = {"failure_reason": "duration_filtered"}

            def search_music_track_best_match(self, artist, track, album=None, duration_ms=None, limit=6, *, start_rung=0):
                self.calls.append(
                    {
                        "artist": artist,
                        "track": track,
                        "album": album,
                        "duration_ms": duration_ms,
                        "limit": limit,
                        "start_rung": start_rung,
                    }
                )
                return None

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        captured = {}

        def _capture_record_failure(job, *, error_message, retryable, retry_delay_seconds):
            captured["error_message"] = error_message
            captured["retryable"] = retryable
            captured["retry_delay_seconds"] = retry_delay_seconds
            captured["job_id"] = job.id
            return jq.JOB_STATUS_QUEUED if retryable else jq.JOB_STATUS_FAILED

        monkeypatch.setattr(engine.store, "record_failure", _capture_record_failure)

        job = jq.DownloadJob(
            id="job-escalate-1",
            origin="intent",
            origin_id="manual",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url="musicbrainz://recording/rec-1",
            input_url="musicbrainz://recording/rec-1",
            canonical_url="musicbrainz://recording/rec-1",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=2,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error="no_candidate_above_threshold",
            trace_id="trace-escalate-1",
            output_template={
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "recording_mbid": "rec-1",
                "mb_release_id": "rel-1",
                "duration_ms": 200000,
                "canonical_metadata": {
                    "artist": "Artist",
                    "track": "Song",
                    "album": "Album",
                    "recording_mbid": "rec-1",
                    "mb_release_id": "rel-1",
                    "duration_ms": 200000,
                },
            },
            resolved_destination=tmp,
            canonical_id="music_track:rec-1:rel-1:disc-1",
            file_path=None,
        )

        resolved = engine._resolve_music_track_job(job)
        assert resolved is None
        assert len(fake_search.calls) == 1
        assert fake_search.calls[0]["start_rung"] == 2
        assert captured["job_id"] == "job-escalate-1"
        assert captured["error_message"] == "duration_filtered"
        assert captured["retryable"] is True
