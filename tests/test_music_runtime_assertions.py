from __future__ import annotations

import importlib.util
import json
import tempfile
import sys
import threading
import time
import types
from pathlib import Path
from subprocess import CalledProcessError


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
    monkeypatch.setattr(jq, "_run_ytdlp_cli", lambda *args, **kwargs: None)
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
    assert seen_probe_formats in ([], [""])


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

    def _fail_download(*args, **kwargs):
        _ = args, kwargs
        raise CalledProcessError(1, ["yt-dlp"], stderr="ERROR: download failed")

    monkeypatch.setattr(jq, "_run_ytdlp_cli", _fail_download)

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
        assert "yt_dlp_download_failed" in str(exc)


def test_ytdlp_unavailability_classifier_maps_known_signals() -> None:
    jq = _load_job_queue()
    assert jq._classify_ytdlp_unavailability("Video unavailable. This video has been removed by the uploader") == "removed_or_deleted"
    assert jq._classify_ytdlp_unavailability("ERROR: Requested format is not available") == "format_unavailable"
    assert jq._classify_ytdlp_unavailability("Sign in to confirm your age") == "age_restricted"
    assert jq._classify_ytdlp_unavailability("The uploader has not made this video available in your country") == "region_restricted"
    assert jq._classify_ytdlp_unavailability("network error: timed out") is None


def test_download_with_ytdlp_marks_known_unavailability(tmp_path, monkeypatch) -> None:
    jq = _load_job_queue()
    temp_dir = tmp_path / "tmp-unavailable"
    temp_dir.mkdir(parents=True, exist_ok=True)

    class _ProbeOkYDL:
        def __init__(self, opts):
            self.opts = dict(opts or {})

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download=False):
            _ = url, download, self.opts
            return {"id": "vid-unavail", "webpage_url": "https://www.youtube.com/watch?v=vid-unavail"}

    def _raise_unavailable(*args, **kwargs):
        _ = args, kwargs
        raise CalledProcessError(1, ["yt-dlp"], stderr="ERROR: [youtube] abc: Video unavailable. This video has been removed by the uploader")

    monkeypatch.setattr(jq, "YoutubeDL", _ProbeOkYDL)
    monkeypatch.setattr(jq, "_run_ytdlp_cli", _raise_unavailable)

    try:
        jq.download_with_ytdlp(
            "https://www.youtube.com/watch?v=abc123xyz00",
            str(temp_dir),
            {},
            audio_mode=False,
            final_format="mkv",
            media_type="video",
            media_intent="playlist",
            job_id="job-unavail-1",
            origin="test",
        )
        assert False, "expected unavailable runtime error"
    except RuntimeError as exc:
        assert "yt_dlp_source_unavailable:removed_or_deleted" in str(exc)


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

            def search_music_track_best_match(
                self,
                artist,
                track,
                album=None,
                duration_ms=None,
                limit=6,
                *,
                start_rung=0,
                coherence_context=None,
                track_aliases=None,
                track_disambiguation=None,
                mb_youtube_urls=None,
                recording_mbid=None,
                is_ep_release=False,
                **kwargs,
            ):
                _ = kwargs
                self.calls.append(
                    {
                        "artist": artist,
                        "track": track,
                        "album": album,
                        "duration_ms": duration_ms,
                        "limit": limit,
                        "start_rung": start_rung,
                        "coherence_context": coherence_context,
                        "track_aliases": track_aliases,
                        "track_disambiguation": track_disambiguation,
                        "mb_youtube_urls": mb_youtube_urls,
                        "recording_mbid": recording_mbid,
                        "is_ep_release": bool(is_ep_release),
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
        assert fake_search.calls[0]["is_ep_release"] is False


def test_music_retry_marks_ep_release_context_when_release_type_ep(monkeypatch) -> None:
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
                self.last_music_track_search = {"failure_reason": "all_filtered_by_gate"}

            def search_music_track_best_match(
                self,
                artist,
                track,
                album=None,
                duration_ms=None,
                limit=6,
                *,
                start_rung=0,
                coherence_context=None,
                track_aliases=None,
                track_disambiguation=None,
                mb_youtube_urls=None,
                recording_mbid=None,
                is_ep_release=False,
                **kwargs,
            ):
                _ = kwargs, duration_ms, limit, coherence_context, track_aliases, track_disambiguation, mb_youtube_urls
                self.calls.append(
                    {
                        "artist": artist,
                        "track": track,
                        "album": album,
                        "start_rung": start_rung,
                        "recording_mbid": recording_mbid,
                        "is_ep_release": bool(is_ep_release),
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

        monkeypatch.setattr(
            engine.store,
            "record_failure",
            lambda job, *, error_message, retryable, retry_delay_seconds: jq.JOB_STATUS_QUEUED,
        )

        job = jq.DownloadJob(
            id="job-ep-1",
            origin="music_album",
            origin_id="album-run-ep",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url="musicbrainz://recording/rec-ep",
            input_url="musicbrainz://recording/rec-ep",
            canonical_url="musicbrainz://recording/rec-ep",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=1,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error="all_filtered_by_gate",
            trace_id="trace-ep-1",
            output_template={
                "artist": "Artist",
                "track": "Song",
                "album": "EP Title",
                "recording_mbid": "rec-ep",
                "mb_release_id": "rel-ep",
                "duration_ms": 200000,
                "canonical_metadata": {
                    "artist": "Artist",
                    "track": "Song",
                    "album": "EP Title",
                    "recording_mbid": "rec-ep",
                    "mb_release_id": "rel-ep",
                    "duration_ms": 200000,
                    "release_primary_type": "EP",
                    "release_secondary_types": [],
                },
            },
            resolved_destination=tmp,
            canonical_id="music_track:rec-ep:rel-ep:disc-1",
            file_path=None,
        )

        resolved = engine._resolve_music_track_job(job)
        assert resolved is None
        assert len(fake_search.calls) == 1
        assert fake_search.calls[0]["is_ep_release"] is True


def test_import_failure_enqueues_review_job_for_eligible_near_miss(monkeypatch) -> None:
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
                self.last_music_track_search = {
                    "failure_reason": "all_filtered_by_gate",
                    "decision_edge": {
                        "rejected_candidates": [
                            {
                                "candidate_id": "cand-near",
                                "source": "youtube",
                                "url": "https://www.youtube.com/watch?v=abc123xyz00",
                                "title": "Artist - Song",
                                "rejection_reason": "score_threshold",
                                "top_failed_gate": "score_threshold",
                                "nearest_pass_margin": {
                                    "name": "final_score",
                                    "value": 0.74,
                                    "threshold": 0.78,
                                    "margin_to_pass": 0.04,
                                    "direction": ">=",
                                },
                                "final_score": 0.74,
                                "title_similarity": 0.96,
                                "artist_similarity": 0.95,
                                "duration_delta_ms": 2000,
                            }
                        ]
                    },
                }

            def search_music_track_best_match(self, *args, **kwargs):
                _ = args, kwargs
                return None

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={"music_low_confidence_review_enabled": True},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        captured_failure = {}
        captured_enqueue = {}

        def _capture_record_failure(job, *, error_message, retryable, retry_delay_seconds):
            captured_failure["error_message"] = error_message
            captured_failure["retryable"] = retryable
            _ = job, retry_delay_seconds
            return jq.JOB_STATUS_FAILED

        def _capture_enqueue_job(**kwargs):
            captured_enqueue.update(kwargs)
            return ("review-job-1", True, None)

        monkeypatch.setattr(engine.store, "record_failure", _capture_record_failure)
        monkeypatch.setattr(engine.store, "enqueue_job", _capture_enqueue_job)

        job = jq.DownloadJob(
            id="job-import-1",
            origin="import",
            origin_id="batch-1",
            media_type="music",
            media_intent="music_track",
            source="music_import",
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
            attempts=1,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-import-1",
            output_template={
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "recording_mbid": "rec-1",
                "mb_release_id": "rel-1",
                "mb_release_group_id": "rg-1",
                "duration_ms": 200000,
                "import_batch_id": "batch-1",
                "canonical_metadata": {
                    "artist": "Artist",
                    "track": "Song",
                    "album": "Album",
                    "release_date": "2024",
                    "track_number": 1,
                    "disc_number": 1,
                    "recording_mbid": "rec-1",
                    "mb_release_id": "rel-1",
                    "mb_release_group_id": "rg-1",
                    "duration_ms": 200000,
                },
            },
            resolved_destination=tmp,
            canonical_id="music_track:rec-1:rel-1:disc-1",
            file_path=None,
        )

        resolved = engine._resolve_music_track_job(job)
        assert resolved is None
        assert captured_failure["error_message"] == "all_filtered_by_gate"
        assert captured_failure["retryable"] is False
        assert captured_enqueue["origin"] == "music_review"
        assert captured_enqueue["media_intent"] == "music_track_review"
        assert "review_queue/files" in str(captured_enqueue["resolved_destination"]).replace("\\", "/")
        assert str(captured_enqueue["resolved_destination"]).startswith(tmp)
        assert str(captured_enqueue["canonical_id"]).startswith("review:rec-1:")
        captured_template = captured_enqueue.get("output_template") if isinstance(captured_enqueue.get("output_template"), dict) else {}
        captured_canonical = captured_template.get("canonical_metadata") if isinstance(captured_template.get("canonical_metadata"), dict) else {}
        assert captured_template.get("album") == "Album"
        assert captured_canonical.get("album") == "Album"
        assert captured_template.get("review_parent_job_id") == "job-import-1"
        assert captured_template.get("review_target_destination") == tmp


def test_non_import_music_failure_enqueues_review_job_for_eligible_near_miss(monkeypatch) -> None:
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
                self.last_music_track_search = {
                    "failure_reason": "all_filtered_by_gate",
                    "decision_edge": {
                        "rejected_candidates": [
                            {
                                "candidate_id": "cand-near-non-import",
                                "source": "youtube_music",
                                "url": "https://www.youtube.com/watch?v=abc123xyz00",
                                "title": "Artist - Song",
                                "rejection_reason": "score_threshold",
                                "top_failed_gate": "score_threshold",
                                "nearest_pass_margin": {
                                    "name": "final_score",
                                    "value": 0.75,
                                    "threshold": 0.78,
                                    "margin_to_pass": 0.03,
                                    "direction": ">=",
                                },
                                "final_score": 0.75,
                                "title_similarity": 1.0,
                                "artist_similarity": 0.95,
                                "duration_delta_ms": 1000,
                            }
                        ]
                    },
                }

            def search_music_track_best_match(self, *args, **kwargs):
                _ = args, kwargs
                return None

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={"music_low_confidence_review_enabled": True},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        captured_enqueue = {}
        monkeypatch.setattr(
            engine.store,
            "enqueue_job",
            lambda **kwargs: (captured_enqueue.update(kwargs), ("review-job-non-import", True, None))[1],
        )
        monkeypatch.setattr(
            engine.store,
            "record_failure",
            lambda job, *, error_message, retryable, retry_delay_seconds: jq.JOB_STATUS_FAILED,
        )

        job = jq.DownloadJob(
            id="job-non-import-1",
            origin="music_album",
            origin_id="album-run-1",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url="https://music.youtube.com/search?q=Artist%20Song",
            input_url="https://music.youtube.com/search?q=Artist%20Song",
            canonical_url="https://music.youtube.com/search?q=Artist%20Song",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=1,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-non-import-1",
            output_template={
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "recording_mbid": "rec-ni-1",
                "mb_release_id": "rel-ni-1",
                "mb_release_group_id": "rg-ni-1",
                "duration_ms": 200000,
                "canonical_metadata": {
                    "artist": "Artist",
                    "track": "Song",
                    "album": "Album",
                    "release_date": "2024",
                    "track_number": 1,
                    "disc_number": 1,
                    "recording_mbid": "rec-ni-1",
                    "mb_release_id": "rel-ni-1",
                    "mb_release_group_id": "rg-ni-1",
                    "duration_ms": 200000,
                },
            },
            resolved_destination=tmp,
            canonical_id="music_track:rec-ni-1:rel-ni-1:disc-1",
            file_path=None,
        )

        resolved = engine._resolve_music_track_job(job)
        assert resolved is None
        assert captured_enqueue["origin"] == "music_review"
        assert captured_enqueue["media_intent"] == "music_track_review"
        assert "review_queue/files" in str(captured_enqueue["resolved_destination"]).replace("\\", "/")


def test_audio_filename_falls_back_when_contract_not_enforced() -> None:
    jq = _load_job_queue()
    filename = jq.build_output_filename(
        {"title": "Song", "artist": "Artist"},
        "fallback-id",
        "mp3",
        None,
        True,
        enforce_music_contract=False,
    )
    assert filename.endswith(".mp3")
    assert "Music/" not in filename


def test_import_failure_does_not_enqueue_review_for_variant_rejection(monkeypatch) -> None:
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
                self.last_music_track_search = {
                    "failure_reason": "all_filtered_by_gate",
                    "decision_edge": {
                        "rejected_candidates": [
                            {
                                "candidate_id": "cand-live",
                                "source": "youtube",
                                "url": "https://www.youtube.com/watch?v=abc123xyz00",
                                "title": "Artist - Song (Live)",
                                "rejection_reason": "disallowed_variant",
                                "top_failed_gate": "variant_alignment",
                                "nearest_pass_margin": {"margin_to_pass": 1.0},
                            }
                        ]
                    },
                }

            def search_music_track_best_match(self, *args, **kwargs):
                _ = args, kwargs
                return None

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={"music_low_confidence_review_enabled": True},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        captured_enqueue = {"count": 0}
        monkeypatch.setattr(
            engine.store,
            "enqueue_job",
            lambda **kwargs: (captured_enqueue.update({"count": captured_enqueue["count"] + 1}), ("review", True, None))[1],
        )
        monkeypatch.setattr(
            engine.store,
            "record_failure",
            lambda job, *, error_message, retryable, retry_delay_seconds: jq.JOB_STATUS_FAILED,
        )

        job = jq.DownloadJob(
            id="job-import-variant-1",
            origin="import",
            origin_id="batch-1",
            media_type="music",
            media_intent="music_track",
            source="music_import",
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
            attempts=1,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-import-variant-1",
            output_template={
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "recording_mbid": "rec-1",
                "mb_release_id": "rel-1",
                "import_batch_id": "batch-1",
                "canonical_metadata": {
                    "artist": "Artist",
                    "track": "Song",
                    "album": "Album",
                    "recording_mbid": "rec-1",
                    "mb_release_id": "rel-1",
                },
            },
            resolved_destination=tmp,
            canonical_id="music_track:rec-1:rel-1:disc-1",
            file_path=None,
        )

        resolved = engine._resolve_music_track_job(job)
        assert resolved is None
        assert captured_enqueue["count"] == 0


def test_import_failure_enqueues_review_for_likely_artist_metadata_mismatch(monkeypatch) -> None:
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
                self.last_music_track_search = {
                    "failure_reason": "all_filtered_by_gate",
                    "decision_edge": {
                        "rejected_candidates": [
                            {
                                "candidate_id": "cand-artist-mismatch",
                                "source": "youtube_music",
                                "url": "https://www.youtube.com/watch?v=abc123xyz00",
                                "title": "HiXTAPE - To Hank (feat. HARDY, Brantley Gilbert & Colt Ford)",
                                "rejection_reason": "low_artist_similarity",
                                "top_failed_gate": "artist_similarity",
                                "nearest_pass_margin": {
                                    "name": "artist_similarity",
                                    "value": 0.0,
                                    "threshold": 0.625,
                                    "margin_to_pass": 0.625,
                                    "direction": ">=",
                                },
                                "final_score": 0.40,
                                "title_similarity": 1.0,
                                "artist_similarity": 0.0,
                                "duration_delta_ms": 2000,
                            }
                        ]
                    },
                }

            def search_music_track_best_match(self, *args, **kwargs):
                _ = args, kwargs
                return None

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={"music_low_confidence_review_enabled": True},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        captured_enqueue = {}
        monkeypatch.setattr(
            engine.store,
            "enqueue_job",
            lambda **kwargs: (captured_enqueue.update(kwargs), ("review-job-2", True, None))[1],
        )
        monkeypatch.setattr(
            engine.store,
            "record_failure",
            lambda job, *, error_message, retryable, retry_delay_seconds: jq.JOB_STATUS_FAILED,
        )

        job = jq.DownloadJob(
            id="job-import-artist-mismatch-1",
            origin="import",
            origin_id="batch-artist-mismatch",
            media_type="music",
            media_intent="music_track",
            source="music_import",
            url="musicbrainz://recording/rec-artist-mismatch",
            input_url="musicbrainz://recording/rec-artist-mismatch",
            canonical_url="musicbrainz://recording/rec-artist-mismatch",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=1,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-import-artist-mismatch-1",
            output_template={
                "artist": "HARDY",
                "track": "To Hank",
                "album": "Hixtape Vol. 2",
                "recording_mbid": "rec-artist-mismatch",
                "mb_release_id": "rel-artist-mismatch",
                "import_batch_id": "batch-artist-mismatch",
                "canonical_metadata": {
                    "artist": "HARDY",
                    "track": "To Hank",
                    "album": "Hixtape Vol. 2",
                    "recording_mbid": "rec-artist-mismatch",
                    "mb_release_id": "rel-artist-mismatch",
                },
            },
            resolved_destination=tmp,
            canonical_id="music_track:rec-artist-mismatch:rel-artist-mismatch:disc-1",
            file_path=None,
        )

        resolved = engine._resolve_music_track_job(job)
        assert resolved is None
        assert captured_enqueue["origin"] == "music_review"
        assert captured_enqueue["media_intent"] == "music_track_review"
        assert captured_enqueue.get("output_template", {}).get("review_parent_job_id") == "job-import-artist-mismatch-1"


def test_review_job_output_dir_allows_internal_review_root() -> None:
    jq = _load_job_queue()
    with tempfile.TemporaryDirectory() as tmp:
        paths = jq.EnginePaths(
            log_dir=tmp,
            db_path=str(Path(tmp) / "db.sqlite"),
            temp_downloads_dir=tmp,
            single_downloads_dir=str(Path(tmp) / "downloads"),
            review_queue_dir=str(Path(tmp) / "data" / "review_queue"),
            review_queue_files_dir=str(Path(tmp) / "data" / "review_queue" / "files"),
            lock_file=str(Path(tmp) / "retreivr.lock"),
            ytdlp_temp_dir=tmp,
            thumbs_dir=tmp,
        )
        review_target = str(Path(paths.review_queue_files_dir) / "review-1")
        resolved = jq._resolve_job_output_dir(review_target, paths, media_intent="music_track_review")
        assert resolved == review_target


def test_worker_binds_store_into_adapters() -> None:
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
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={},
            paths=paths,
            adapters=jq.default_adapters(),
            search_service=None,
        )
        for source in ("youtube", "youtube_music", "soundcloud", "bandcamp", "direct", "unknown"):
            adapter = engine.adapters.get(source)
            assert adapter is not None
            assert getattr(adapter, "store", None) is engine.store


def test_music_resolve_uses_runtime_preresolved_candidate_without_search() -> None:
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
                self.calls = 0
                self.last_music_track_search = {}

            def search_music_track_best_match(self, *args, **kwargs):
                _ = args, kwargs
                self.calls += 1
                return None

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        job = jq.DownloadJob(
            id="job-preresolved-1",
            origin="music_album",
            origin_id="album-run-preresolved",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url="musicbrainz://recording/rec-preresolved",
            input_url="musicbrainz://recording/rec-preresolved",
            canonical_url="musicbrainz://recording/rec-preresolved",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=0,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-preresolved-1",
            output_template={
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "recording_mbid": "rec-preresolved",
                "mb_release_id": "rel-preresolved",
                "canonical_metadata": {
                    "artist": "Artist",
                    "track": "Song",
                    "album": "Album",
                    "recording_mbid": "rec-preresolved",
                    "mb_release_id": "rel-preresolved",
                },
                "runtime_pre_resolved": {
                    "recording_mbid": "rec-preresolved",
                    "mb_release_id": "rel-preresolved",
                    "resolved_url": "https://www.youtube.com/watch?v=abc123xyz00",
                    "resolved_source": "youtube_music",
                },
            },
            resolved_destination=tmp,
            canonical_id="music_track:rec-preresolved:rel-preresolved:disc-1",
            file_path=None,
        )

        resolved = engine._resolve_music_track_job(job)
        assert resolved is not None
        assert resolved.url == "https://www.youtube.com/watch?v=abc123xyz00"
        assert resolved.source in {"youtube", "youtube_music"}
        assert fake_search.calls == 0


def test_music_resolve_uses_in_memory_resolution_cache_for_same_mb_pair() -> None:
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
                self.calls = 0
                self.last_music_track_search = {}

            def search_music_track_best_match(self, *args, **kwargs):
                _ = args, kwargs
                self.calls += 1
                return {
                    "url": "https://www.youtube.com/watch?v=abc123xyz00",
                    "source": "youtube_music",
                    "candidate_id": "cand-1",
                    "final_score": 0.95,
                    "duration_ms": 200000,
                }

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        template = {
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
            "recording_mbid": "rec-cache-1",
            "mb_release_id": "rel-cache-1",
            "canonical_metadata": {
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "recording_mbid": "rec-cache-1",
                "mb_release_id": "rel-cache-1",
            },
        }

        job_a = jq.DownloadJob(
            id="job-cache-a",
            origin="music_album",
            origin_id="album-run-cache",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url="musicbrainz://recording/rec-cache-1",
            input_url="musicbrainz://recording/rec-cache-1",
            canonical_url="musicbrainz://recording/rec-cache-1",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=0,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-cache-a",
            output_template=dict(template),
            resolved_destination=tmp,
            canonical_id="music_track:rec-cache-1:rel-cache-1:disc-1:a",
            file_path=None,
        )

        job_b = jq.DownloadJob(
            id="job-cache-b",
            origin="music_album",
            origin_id="album-run-cache",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url="musicbrainz://recording/rec-cache-1",
            input_url="musicbrainz://recording/rec-cache-1",
            canonical_url="musicbrainz://recording/rec-cache-1",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=0,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-cache-b",
            output_template=dict(template),
            resolved_destination=tmp,
            canonical_id="music_track:rec-cache-1:rel-cache-1:disc-1:b",
            file_path=None,
        )

        resolved_a = engine._resolve_music_track_job(job_a)
        resolved_b = engine._resolve_music_track_job(job_b)

        assert resolved_a is not None
        assert resolved_b is not None
        assert resolved_a.url == "https://www.youtube.com/watch?v=abc123xyz00"
        assert resolved_b.url == "https://www.youtube.com/watch?v=abc123xyz00"
        assert fake_search.calls == 1


def test_worker_runtime_metrics_capture_cache_hit_rate_and_resolve_latency() -> None:
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
                self.calls = 0
                self.last_music_track_search = {}

            def search_music_track_best_match(self, *args, **kwargs):
                _ = args, kwargs
                self.calls += 1
                return {
                    "url": "https://www.youtube.com/watch?v=abc123xyz00",
                    "source": "youtube_music",
                    "candidate_id": "cand-metrics-1",
                    "final_score": 0.95,
                    "duration_ms": 200000,
                }

        fake_search = _FakeSearchService()
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={},
            paths=paths,
            adapters={},
            search_service=fake_search,
        )

        template = {
            "artist": "Artist",
            "track": "Song",
            "album": "Album",
            "recording_mbid": "rec-metrics-1",
            "mb_release_id": "rel-metrics-1",
            "canonical_metadata": {
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "recording_mbid": "rec-metrics-1",
                "mb_release_id": "rel-metrics-1",
            },
        }
        job_a = jq.DownloadJob(
            id="job-metrics-a",
            origin="music_album",
            origin_id="album-run-metrics",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url="musicbrainz://recording/rec-metrics-1",
            input_url="musicbrainz://recording/rec-metrics-1",
            canonical_url="musicbrainz://recording/rec-metrics-1",
            external_id=None,
            status=jq.JOB_STATUS_QUEUED,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=0,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-metrics-a",
            output_template=dict(template),
            resolved_destination=tmp,
            canonical_id="music_track:rec-metrics-1:rel-metrics-1:disc-1:a",
            file_path=None,
        )
        job_b = jq.replace(
            job_a,
            id="job-metrics-b",
            trace_id="trace-metrics-b",
            canonical_id="music_track:rec-metrics-1:rel-metrics-1:disc-1:b",
        )

        assert engine._resolve_music_track_job(job_a) is not None
        assert engine._resolve_music_track_job(job_b) is not None

        metrics = engine.get_runtime_metrics()
        cache = metrics.get("resolution_cache") if isinstance(metrics, dict) else {}
        latency = metrics.get("resolve_latency_ms") if isinstance(metrics, dict) else {}
        assert fake_search.calls == 1
        assert int(cache.get("hits") or 0) >= 1
        assert int(cache.get("misses") or 0) >= 1
        assert float(cache.get("hit_rate") or 0.0) > 0.0
        assert int(latency.get("count") or 0) >= 1
        assert latency.get("avg") is not None


def test_worker_runtime_metrics_track_source_active_slots(monkeypatch) -> None:
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
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={"max_concurrent_jobs_per_source": 1, "max_concurrent_jobs_total": 2},
            paths=paths,
            adapters={},
            search_service=None,
        )

        engine.store.enqueue_job(
            origin="test",
            origin_id="runtime-metrics-active",
            media_type="video",
            media_intent="episode",
            source="youtube_music",
            url="https://www.youtube.com/watch?v=metricsactive1",
            output_template={"output_dir": tmp, "final_format": "mkv"},
        )

        entered = threading.Event()
        release = threading.Event()

        def _execute_stub(job, *, stop_event=None):
            _ = job, stop_event
            entered.set()
            release.wait(timeout=2.0)

        monkeypatch.setattr(engine, "_execute_job", _execute_stub)

        lock = engine._get_source_lock("youtube_music")
        assert lock.acquire(blocking=False) is True
        runner = threading.Thread(
            target=engine._run_source_once,
            args=("youtube_music", lock, None),
            daemon=False,
        )
        runner.start()
        assert entered.wait(timeout=2.0)

        live_metrics = engine.get_runtime_metrics()
        active_slots = live_metrics.get("source_active_slots") if isinstance(live_metrics, dict) else {}
        assert int(active_slots.get("youtube_music") or 0) >= 1

        release.set()
        runner.join(timeout=3.0)
        assert runner.is_alive() is False

        final_metrics = engine.get_runtime_metrics()
        final_active_slots = final_metrics.get("source_active_slots") if isinstance(final_metrics, dict) else {}
        max_slots = final_metrics.get("source_max_active_slots") if isinstance(final_metrics, dict) else {}
        assert int(final_active_slots.get("youtube_music") or 0) == 0
        assert int(max_slots.get("youtube_music") or 0) >= 1


def test_same_source_parallel_execute_respects_source_concurrency(monkeypatch) -> None:
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
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={"max_concurrent_jobs_per_source": 2, "max_concurrent_jobs_total": 4},
            paths=paths,
            adapters={},
            search_service=None,
        )

        for idx in range(2):
            engine.store.enqueue_job(
                origin="test",
                origin_id="same-source-parallel",
                media_type="video",
                media_intent="episode",
                source="youtube_music",
                url=f"https://www.youtube.com/watch?v=samesource{idx}",
                output_template={"output_dir": tmp, "final_format": "mkv"},
            )

        counter_lock = threading.Lock()
        release_event = threading.Event()
        both_started = threading.Event()
        state = {"active": 0, "max_active": 0}

        def _execute_stub(job, *, stop_event=None):
            _ = stop_event
            with counter_lock:
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
                if state["active"] >= 2:
                    both_started.set()
            release_event.wait(timeout=2.0)
            engine.store.mark_completed(job.id, file_path=str(Path(tmp) / f"{job.id}.bin"))
            with counter_lock:
                state["active"] -= 1

        monkeypatch.setattr(engine, "_execute_job", _execute_stub)

        source_lock = engine._get_source_lock("youtube_music")
        assert source_lock.acquire(blocking=False) is True
        runner = threading.Thread(
            target=engine._run_source_once,
            args=("youtube_music", source_lock, None),
            daemon=False,
        )
        runner.start()
        assert both_started.wait(timeout=2.0), "expected two same-source jobs to execute in parallel"
        release_event.set()
        runner.join(timeout=3.0)
        assert runner.is_alive() is False

        assert state["max_active"] >= 2


def test_cross_source_parallel_execute(monkeypatch) -> None:
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
        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={"max_concurrent_jobs_per_source": 1, "max_concurrent_jobs_total": 4},
            paths=paths,
            adapters={},
            search_service=None,
        )

        engine.store.enqueue_job(
            origin="test",
            origin_id="cross-source-parallel",
            media_type="video",
            media_intent="episode",
            source="youtube_music",
            url="https://www.youtube.com/watch?v=crosssource1",
            output_template={"output_dir": tmp, "final_format": "mkv"},
        )
        engine.store.enqueue_job(
            origin="test",
            origin_id="cross-source-parallel",
            media_type="video",
            media_intent="episode",
            source="soundcloud",
            url="https://soundcloud.com/test/crosssource2",
            output_template={"output_dir": tmp, "final_format": "mkv"},
        )

        counter_lock = threading.Lock()
        release_event = threading.Event()
        both_started = threading.Event()
        state = {"active": 0, "max_active": 0}

        def _execute_stub(job, *, stop_event=None):
            _ = stop_event
            with counter_lock:
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
                if state["active"] >= 2:
                    both_started.set()
            release_event.wait(timeout=2.0)
            engine.store.mark_completed(job.id, file_path=str(Path(tmp) / f"{job.id}.bin"))
            with counter_lock:
                state["active"] -= 1

        monkeypatch.setattr(engine, "_execute_job", _execute_stub)

        lock_yt = engine._get_source_lock("youtube_music")
        lock_sc = engine._get_source_lock("soundcloud")
        assert lock_yt.acquire(blocking=False) is True
        assert lock_sc.acquire(blocking=False) is True
        runner_yt = threading.Thread(
            target=engine._run_source_once,
            args=("youtube_music", lock_yt, None),
            daemon=False,
        )
        runner_sc = threading.Thread(
            target=engine._run_source_once,
            args=("soundcloud", lock_sc, None),
            daemon=False,
        )
        runner_yt.start()
        runner_sc.start()
        assert both_started.wait(timeout=2.0), "expected cross-source jobs to execute in parallel"
        release_event.set()
        runner_yt.join(timeout=3.0)
        runner_sc.join(timeout=3.0)
        assert runner_yt.is_alive() is False
        assert runner_sc.is_alive() is False
        assert state["max_active"] >= 2


def test_preresolve_lookahead_handles_source_reassignment_race(monkeypatch) -> None:
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
            last_music_track_search = {}

        engine = jq.DownloadWorkerEngine(
            db_path=db_path,
            config={
                "music_preresolve_enabled": True,
                "music_preresolve_lookahead": 3,
                "music_preresolve_pool_workers": 2,
            },
            paths=paths,
            adapters={},
            search_service=_FakeSearchService(),
        )

        queued_ids: list[str] = []
        for idx in range(3):
            job_id, created, _reason = engine.store.enqueue_job(
                origin="test",
                origin_id="preresolve-race",
                media_type="music",
                media_intent="music_track",
                source="music_import",
                url=f"musicbrainz://recording/rec-race-{idx}",
                output_template={
                    "artist": "Artist",
                    "track": f"Song {idx}",
                    "album": "Album",
                    "recording_mbid": f"rec-race-{idx}",
                    "mb_release_id": f"rel-race-{idx}",
                    "canonical_metadata": {
                        "artist": "Artist",
                        "track": f"Song {idx}",
                        "album": "Album",
                        "recording_mbid": f"rec-race-{idx}",
                        "mb_release_id": f"rel-race-{idx}",
                    },
                },
            )
            assert created is True
            queued_ids.append(job_id)

        race_job_id = queued_ids[1]
        status_calls: dict[str, int] = {}
        original_get_status = engine.store.get_job_status

        def _get_status_racy(job_id):
            current = status_calls.get(job_id, 0) + 1
            status_calls[job_id] = current
            if job_id == race_job_id and current >= 2:
                return jq.JOB_STATUS_CLAIMED
            return original_get_status(job_id)

        monkeypatch.setattr(engine.store, "get_job_status", _get_status_racy)

        def _resolve_stub(job, *, persist_failures=True):
            _ = persist_failures
            return jq.replace(
                job,
                source="youtube_music",
                url=f"https://www.youtube.com/watch?v={job.id[:11]}",
                input_url=f"https://www.youtube.com/watch?v={job.id[:11]}",
                canonical_url=f"https://www.youtube.com/watch?v={job.id[:11]}",
            )

        monkeypatch.setattr(engine, "_resolve_music_track_job", _resolve_stub)

        current_job = jq.DownloadJob(
            id="current-preresolve-job",
            origin="test",
            origin_id="preresolve-race",
            media_type="music",
            media_intent="music_track",
            source="music_import",
            url="musicbrainz://recording/current",
            input_url="musicbrainz://recording/current",
            canonical_url="musicbrainz://recording/current",
            external_id=None,
            status=jq.JOB_STATUS_DOWNLOADING,
            queued=None,
            claimed=None,
            downloading=None,
            postprocessing=None,
            completed=None,
            failed=None,
            canceled=None,
            attempts=0,
            max_attempts=3,
            created_at=None,
            updated_at=None,
            last_error=None,
            trace_id="trace-current-preresolve",
            output_template={
                "recording_mbid": "current",
                "mb_release_id": "rel-current",
                "canonical_metadata": {"recording_mbid": "current", "mb_release_id": "rel-current"},
            },
            resolved_destination=tmp,
            canonical_id="music_track:current:rel-current:disc-1",
            file_path=None,
        )

        engine._maybe_preresolve_next_music_job(current_job)

        deadline = time.time() + 3.0
        while time.time() < deadline:
            with engine._pre_resolve_lock:
                if not engine._pre_resolve_inflight:
                    break
            time.sleep(0.05)

        with engine._pre_resolve_lock:
            assert not engine._pre_resolve_inflight

        conn = engine.store._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, source, output_template FROM download_jobs WHERE id IN (?, ?, ?)", tuple(queued_ids))
            rows = {row["id"]: row for row in cur.fetchall()}
        finally:
            conn.close()

        assert rows[race_job_id]["source"] == "music_import"
        updated_ids = [job_id for job_id in queued_ids if job_id != race_job_id]
        for job_id in updated_ids:
            assert rows[job_id]["source"] == "youtube_music"
            payload = json.loads(rows[job_id]["output_template"] or "{}")
            runtime = payload.get("runtime_pre_resolved") if isinstance(payload, dict) else None
            assert isinstance(runtime, dict)
            assert str(runtime.get("resolved_source") or "") == "youtube_music"
