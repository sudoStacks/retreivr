from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_job_queue_module():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    _load_module("engine.json_utils", _ROOT / "engine" / "json_utils.py")
    _load_module("engine.paths", _ROOT / "engine" / "paths.py")
    _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_service = types.ModuleType("metadata.services.musicbrainz_service")
        mb_service.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_service
    if "metadata.services" not in sys.modules:
        metadata_services = types.ModuleType("metadata.services")
        metadata_services.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services"] = metadata_services
    return _load_module("engine_job_queue_music_opts_regression", _ROOT / "engine" / "job_queue.py")


@pytest.fixture(scope="module")
def jq():
    return _load_job_queue_module()


def test_music_job_builds_audio_ytdlp_opts(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mkv",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }
    opts = jq.build_ytdlp_opts(context)

    assert context["audio_mode"] is True
    assert str(opts.get("format", "")).startswith("bestaudio")
    assert "merge_output_format" not in opts
    postprocessors = opts.get("postprocessors") or []
    extract_pp = next((pp for pp in postprocessors if pp.get("key") == "FFmpegExtractAudio"), None)
    assert extract_pp is not None
    assert extract_pp.get("preferredcodec") == "mp3"
    assert opts.get("concurrent_fragment_downloads") == 2


def test_music_job_allows_override_of_fragment_concurrency(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mkv",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "overrides": {
            "concurrent_fragment_downloads": 3,
        },
    }
    opts = jq.build_ytdlp_opts(context)
    assert opts.get("concurrent_fragment_downloads") == 3


def test_video_job_builds_video_ytdlp_opts(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "video",
        "media_intent": "episode",
        "final_format": "webm",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "webm",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "webm",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }
    opts = jq.build_ytdlp_opts(context)

    assert context["audio_mode"] is False
    assert opts.get("format") == (
        "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
        "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
        "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/"
        "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/"
        "bestvideo*+bestaudio/best"
    )
    assert "merge_output_format" not in opts
    postprocessors = opts.get("postprocessors") or []
    assert not any(pp.get("key") == "FFmpegExtractAudio" for pp in postprocessors if isinstance(pp, dict))


def test_music_invariant_raises_if_video_fields_present(jq, monkeypatch) -> None:
    original_merge_overrides = jq._merge_overrides

    def _regressing_merge_overrides(opts, overrides, *, operation, lock_format=False):
        merged = original_merge_overrides(opts, overrides, operation=operation, lock_format=lock_format)
        merged["merge_output_format"] = "mkv"
        return merged

    monkeypatch.setattr(jq, "_merge_overrides", _regressing_merge_overrides)

    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mkv",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "mkv",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }

    with pytest.raises(RuntimeError, match="music_job_built_video_opts"):
        jq.build_ytdlp_opts(context)


def test_music_job_uses_resolved_music_codec_for_extract_audio(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "music",
        "media_intent": "music_track",
        "final_format": "mkv",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mkv",
            "music_final_format": "m4a",
        },
        "config": {
            "final_format": "mkv",
            "music_final_format": "m4a",
        },
        "overrides": {},
    }
    opts = jq.build_ytdlp_opts(context)

    postprocessors = opts.get("postprocessors") or []
    extract_pp = next((pp for pp in postprocessors if pp.get("key") == "FFmpegExtractAudio"), None)
    assert extract_pp is not None
    assert extract_pp.get("preferredcodec") == "m4a"


def test_claim_next_job_respects_global_active_cap(jq, tmp_path) -> None:
    db_path = tmp_path / "queue-global-cap.sqlite"
    conn = jq.sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        jq.ensure_download_jobs_table(conn)
    finally:
        conn.close()

    store = jq.DownloadJobStore(str(db_path))
    for idx in range(3):
        store.enqueue_job(
            origin="test",
            origin_id="global-cap",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url=f"https://music.youtube.com/watch?v=cap{idx}",
            output_template={"output_dir": "/tmp", "final_format": "mp3"},
        )

    first = store.claim_next_job("youtube_music", max_active_per_source=3, max_active_total=2)
    second = store.claim_next_job("youtube_music", max_active_per_source=3, max_active_total=2)
    third = store.claim_next_job("youtube_music", max_active_per_source=3, max_active_total=2)

    assert first is not None
    assert second is not None
    assert third is None


def test_claim_next_job_global_cap_applies_across_sources(jq, tmp_path) -> None:
    db_path = tmp_path / "queue-global-cap-sources.sqlite"
    conn = jq.sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        jq.ensure_download_jobs_table(conn)
    finally:
        conn.close()

    store = jq.DownloadJobStore(str(db_path))
    store.enqueue_job(
        origin="test",
        origin_id="global-cap-cross-source",
        media_type="music",
        media_intent="music_track",
        source="youtube_music",
        url="https://music.youtube.com/watch?v=capsource1",
        output_template={"output_dir": "/tmp", "final_format": "mp3"},
    )
    store.enqueue_job(
        origin="test",
        origin_id="global-cap-cross-source",
        media_type="music",
        media_intent="music_track",
        source="soundcloud",
        url="https://soundcloud.com/test/track-capsource2",
        output_template={"output_dir": "/tmp", "final_format": "mp3"},
    )

    first = store.claim_next_job("youtube_music", max_active_per_source=2, max_active_total=1)
    second = store.claim_next_job("soundcloud", max_active_per_source=2, max_active_total=1)

    assert first is not None
    assert second is None


def test_claim_next_job_respects_import_batch_download_cap(jq, tmp_path) -> None:
    db_path = tmp_path / "queue-import-batch-cap.sqlite"
    conn = jq.sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        jq.ensure_download_jobs_table(conn)
    finally:
        conn.close()

    store = jq.DownloadJobStore(str(db_path))
    for idx in range(2):
        store.enqueue_job(
            origin="import",
            origin_id="import-batch-1",
            media_type="music",
            media_intent="music_track",
            source="youtube_music",
            url=f"musicbrainz://recording/importcap{idx}",
            output_template={
                "output_dir": "/tmp",
                "final_format": "mp3",
                "import_batch_id": "import-batch-1",
                "import_max_concurrent_downloads": 1,
            },
        )

    first = store.claim_next_job("youtube_music", max_active_per_source=3, max_active_total=3)
    second = store.claim_next_job("youtube_music", max_active_per_source=3, max_active_total=3)

    assert first is not None
    assert second is None


def test_claim_next_job_skips_capped_import_batch_for_other_jobs(jq, tmp_path) -> None:
    db_path = tmp_path / "queue-import-batch-cap-skip.sqlite"
    conn = jq.sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        jq.ensure_download_jobs_table(conn)
    finally:
        conn.close()

    store = jq.DownloadJobStore(str(db_path))
    store.enqueue_job(
        origin="import",
        origin_id="import-batch-2",
        media_type="music",
        media_intent="music_track",
        source="youtube_music",
        url="musicbrainz://recording/importcap-active",
        output_template={
            "output_dir": "/tmp",
            "final_format": "mp3",
            "import_batch_id": "import-batch-2",
            "import_max_concurrent_downloads": 1,
        },
    )
    store.enqueue_job(
        origin="import",
        origin_id="import-batch-2",
        media_type="music",
        media_intent="music_track",
        source="youtube_music",
        url="musicbrainz://recording/importcap-waiting",
        output_template={
            "output_dir": "/tmp",
            "final_format": "mp3",
            "import_batch_id": "import-batch-2",
            "import_max_concurrent_downloads": 1,
        },
    )
    store.enqueue_job(
        origin="manual",
        origin_id="manual-1",
        media_type="music",
        media_intent="music_track",
        source="youtube_music",
        url="https://music.youtube.com/watch?v=notblocked",
        output_template={"output_dir": "/tmp", "final_format": "mp3"},
    )

    first = store.claim_next_job("youtube_music", max_active_per_source=3, max_active_total=3)
    second = store.claim_next_job("youtube_music", max_active_per_source=3, max_active_total=3)

    assert first is not None
    assert first.origin == "import"
    assert second is not None
    assert second.origin == "manual"


def test_video_mp4_job_uses_same_download_selector_with_mp4_merge_target(jq) -> None:
    context = {
        "operation": "download",
        "url": "https://www.youtube.com/watch?v=abc123xyz00",
        "media_type": "video",
        "media_intent": "episode",
        "final_format": "mp4",
        "output_template": "%(id)s.%(ext)s",
        "output_template_meta": {
            "final_format": "mp4",
            "music_final_format": "mp3",
        },
        "config": {
            "final_format": "mp4",
            "music_final_format": "mp3",
        },
        "overrides": {},
    }

    opts = jq.build_ytdlp_opts(context)

    assert context["audio_mode"] is False
    assert opts.get("format") == (
        "bestvideo[ext=mp4][vcodec~='^(avc1|h264)'][height<=1080]+bestaudio[ext=m4a]/"
        "bestvideo[ext=mp4][vcodec~='^(avc1|h264)']+bestaudio[ext=m4a]/"
        "best[ext=mp4]/"
        "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
        "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
        "bestvideo*+bestaudio/best"
    )
    assert opts.get("merge_output_format") == "mp4"
