from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
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
    return _load_module("engine_job_queue_path_guard_tests", _ROOT / "engine" / "job_queue.py")


@pytest.fixture()
def jq():
    return _load_job_queue()


def test_worker_does_not_build_unknown_album_when_mb_pair_present(jq, monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        base_downloads = Path(tmpdir) / "downloads"
        job = SimpleNamespace(
            id="job-1",
            url="https://example.test/v",
            origin="import",
            media_type="music",
            media_intent="music_track",
            resolved_destination=str(base_downloads),
            output_template={
                "output_dir": str(base_downloads),
                "music_final_format": "mp3",
                "recording_mbid": "rec-1",
                "canonical_metadata": {
                    "artist": "Artist Name",
                    "track": "Track Name",
                    "album": "Bound Album",
                    "release_date": "2012",
                    "track_number": 4,
                    "disc_number": 1,
                    "mb_release_id": "rel-1",
                    "mb_release_group_id": "rg-1",
                    "recording_mbid": "rec-1",
                    "mb_recording_id": "rec-1",
                },
            },
        )
        adapter = jq.YouTubeAdapter()

        def _fake_download_with_ytdlp(*args, **kwargs):
            temp_dir = Path(args[1])
            temp_dir.mkdir(parents=True, exist_ok=True)
            out = temp_dir / "tmp.mp3"
            out.write_bytes(b"ok")
            return {"id": "vid-1", "title": "Track Name", "ext": "mp3"}, str(out)

        monkeypatch.setattr(jq, "download_with_ytdlp", _fake_download_with_ytdlp)
        monkeypatch.setattr(
            jq,
            "extract_meta",
            lambda info, fallback_url=None: {"video_id": info.get("id"), "title": "Track Name", "artist": "Artist Name", "track": "Track Name"},
        )
        monkeypatch.setattr(jq, "enqueue_media_metadata", lambda file_path, meta, config: None)

        paths = SimpleNamespace(
            single_downloads_dir=str(base_downloads),
            temp_downloads_dir=str(Path(tmpdir) / "tmp"),
            thumbs_dir=str(Path(tmpdir) / "thumbs"),
        )

        final = adapter.execute(
            job,
            {"final_format": "mkv", "music_final_format": "mp3"},
            paths,
            media_type="music",
            media_intent="music_track",
        )
        assert final is not None
        final_path, _ = final
        assert "Unknown Album" not in final_path
        assert "Bound Album (2012)" in final_path
        assert "/Disc 1/" in final_path
        assert "/04 - Track Name.mp3" in final_path


def test_worker_fails_fast_if_release_metadata_missing_and_enrichment_fails(jq, monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        base_downloads = Path(tmpdir) / "downloads"
        job = SimpleNamespace(
            id="job-2",
            url="https://example.test/v",
            origin="import",
            media_type="music",
            media_intent="music_track",
            resolved_destination=str(base_downloads),
            output_template={
                "output_dir": str(base_downloads),
                "music_final_format": "mp3",
                "recording_mbid": "rec-missing",
                "canonical_metadata": {
                    "artist": "Artist Name",
                    "track": "Track Name",
                    "recording_mbid": "rec-missing",
                    "mb_recording_id": "rec-missing",
                },
            },
        )
        adapter = jq.YouTubeAdapter()

        def _fake_download_with_ytdlp(*args, **kwargs):
            temp_dir = Path(args[1])
            temp_dir.mkdir(parents=True, exist_ok=True)
            out = temp_dir / "tmp.mp3"
            out.write_bytes(b"ok")
            return {"id": "vid-2", "title": "Track Name", "ext": "mp3"}, str(out)

        monkeypatch.setattr(jq, "download_with_ytdlp", _fake_download_with_ytdlp)
        monkeypatch.setattr(
            jq,
            "extract_meta",
            lambda info, fallback_url=None: {"video_id": info.get("id"), "title": "Track Name", "artist": "Artist Name", "track": "Track Name"},
        )
        monkeypatch.setattr(jq, "_fetch_release_enrichment", lambda recording_mbid, release_id_hint: {})

        paths = SimpleNamespace(
            single_downloads_dir=str(base_downloads),
            temp_downloads_dir=str(Path(tmpdir) / "tmp"),
            thumbs_dir=str(Path(tmpdir) / "thumbs"),
        )

        with pytest.raises(RuntimeError, match="release_enrichment_incomplete"):
            adapter.execute(
                job,
                {"final_format": "mkv", "music_final_format": "mp3"},
                paths,
                media_type="music",
                media_intent="music_track",
            )
        assert not (base_downloads / "Music").exists()


def test_worker_rejects_non_canonical_music_filename_contract(jq, monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        base_downloads = Path(tmpdir) / "downloads"
        job = SimpleNamespace(
            id="job-3",
            url="https://example.test/v",
            origin="import",
            media_type="music",
            media_intent="music_track",
            resolved_destination=str(base_downloads),
            output_template={
                "output_dir": str(base_downloads),
                "music_final_format": "mp3",
                "recording_mbid": "rec-1",
                "canonical_metadata": {
                    "artist": "Artist Name",
                    "track": "Track Name",
                    "album": "Bound Album",
                    "release_date": "2012",
                    "track_number": 4,
                    "disc_number": 1,
                    "mb_release_id": "rel-1",
                    "mb_release_group_id": "rg-1",
                    "recording_mbid": "rec-1",
                    "mb_recording_id": "rec-1",
                },
            },
        )
        adapter = jq.YouTubeAdapter()

        def _fake_download_with_ytdlp(*args, **kwargs):
            temp_dir = Path(args[1])
            temp_dir.mkdir(parents=True, exist_ok=True)
            out = temp_dir / "tmp.mp3"
            out.write_bytes(b"ok")
            return {"id": "vid-3", "title": "Track Name", "ext": "mp3"}, str(out)

        monkeypatch.setattr(jq, "download_with_ytdlp", _fake_download_with_ytdlp)
        monkeypatch.setattr(
            jq,
            "extract_meta",
            lambda info, fallback_url=None: {
                "video_id": info.get("id"),
                "title": "Track Name",
                "artist": "Artist Name",
                "track": "Track Name",
            },
        )
        monkeypatch.setattr(jq, "build_output_filename", lambda *args, **kwargs: "Track Name.mp3")

        paths = SimpleNamespace(
            single_downloads_dir=str(base_downloads),
            temp_downloads_dir=str(Path(tmpdir) / "tmp"),
            thumbs_dir=str(Path(tmpdir) / "thumbs"),
        )

        with pytest.raises(RuntimeError, match="music_filename_contract_violation"):
            adapter.execute(
                job,
                {"final_format": "mkv", "music_final_format": "mp3"},
                paths,
                media_type="music",
                media_intent="music_track",
            )
        assert not any(base_downloads.rglob("*"))
