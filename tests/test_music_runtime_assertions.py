from __future__ import annotations

import importlib.util
import tempfile
import sys
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
    assert seen_probe_formats == [""]


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
            ):
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
            ):
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
        assert "Needs Review" in str(captured_enqueue["resolved_destination"])
        assert str(captured_enqueue["canonical_id"]).startswith("review:rec-1:")


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
