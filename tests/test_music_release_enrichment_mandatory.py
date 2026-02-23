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
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    if "metadata.queue" not in sys.modules:
        metadata_queue = types.ModuleType("metadata.queue")
        metadata_queue.enqueue_metadata = lambda file_path, meta, config: None
        sys.modules["metadata.queue"] = metadata_queue
    if "metadata.services.musicbrainz_service" not in sys.modules:
        mb_module = types.ModuleType("metadata.services.musicbrainz_service")
        mb_module.get_musicbrainz_service = lambda: None
        sys.modules["metadata.services.musicbrainz_service"] = mb_module
    return _load_module("engine_job_queue_release_enrichment_tests", _ROOT / "engine" / "job_queue.py")


class _FakeMBService:
    def __init__(self, *, recording_payload: dict, release_payloads: dict[str, dict]):
        self._recording_payload = recording_payload
        self._release_payloads = release_payloads

    def get_recording(self, recording_id, *, includes=None):
        _ = recording_id, includes
        return self._recording_payload

    def get_release(self, release_id, *, includes=None):
        _ = includes
        return self._release_payloads.get(release_id, {})


@pytest.fixture()
def jq():
    return _load_job_queue()


def _valid_release_payload(*, release_id: str, recording_mbid: str, title: str = "Album Name", date: str = "2015-04-03", release_group_id: str = "rg-1", status: str = "Official", primary_type: str = "Album"):
    return {
        "release": {
            "id": release_id,
            "title": title,
            "date": date,
            "status": status,
            "release-group": {
                "id": release_group_id,
                "primary-type": primary_type,
            },
            "medium-list": [
                {
                    "position": "1",
                    "track-list": [
                        {
                            "position": "7",
                            "recording": {"id": recording_mbid},
                        }
                    ],
                }
            ],
        }
    }


def test_missing_track_number_enrichment_fills_metadata_and_path_builds(jq, monkeypatch):
    recording_mbid = "rec-1"
    release_id = "rel-1"
    fake_service = _FakeMBService(
        recording_payload={
            "recording": {
                "id": recording_mbid,
                "release-list": [
                    {"id": release_id, "date": "2015-04-03"},
                ],
            }
        },
        release_payloads={
            release_id: _valid_release_payload(release_id=release_id, recording_mbid=recording_mbid),
        },
    )
    monkeypatch.setattr(jq, "get_musicbrainz_service", lambda: fake_service)

    job = SimpleNamespace(
        output_template={
            "recording_mbid": recording_mbid,
            "canonical_metadata": {
                "artist": "Artist Name",
                "track": "Track Name",
            },
        },
    )
    jq._ensure_release_enriched(job)
    metadata = job.output_template["canonical_metadata"]
    assert metadata["track_number"] == 7
    assert metadata["disc_number"] == 1
    assert metadata["mb_release_id"] == release_id
    assert metadata["mb_release_group_id"] == "rg-1"
    assert metadata["release_date"] == "2015"
    path = jq.build_audio_filename(
        {
            "album_artist": "Artist Name",
            "artist": "Artist Name",
            "track": "Track Name",
            **metadata,
        },
        "mp3",
    )
    assert "Unknown Album" not in path
    assert "/Disc 1/" in path
    assert "/07 - Track Name.mp3" in path


def test_no_valid_release_raises_and_no_folder_created(jq, monkeypatch):
    recording_mbid = "rec-missing"
    release_id = "rel-invalid"
    fake_service = _FakeMBService(
        recording_payload={
            "recording": {
                "id": recording_mbid,
                "release-list": [
                    {"id": release_id, "date": "2011-01-01"},
                ],
            }
        },
        release_payloads={
            release_id: _valid_release_payload(
                release_id=release_id,
                recording_mbid=recording_mbid,
                status="Bootleg",
                primary_type="EP",
            ),
        },
    )
    monkeypatch.setattr(jq, "get_musicbrainz_service", lambda: fake_service)

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
                "recording_mbid": recording_mbid,
                "canonical_metadata": {
                    "artist": "Artist Name",
                    "track": "Track Name",
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


def test_missing_release_group_is_enriched(jq, monkeypatch):
    recording_mbid = "rec-2"
    release_id = "rel-2"
    fake_service = _FakeMBService(
        recording_payload={"recording": {"id": recording_mbid, "release-list": [{"id": release_id, "date": "2018-06-01"}]}},
        release_payloads={release_id: _valid_release_payload(release_id=release_id, recording_mbid=recording_mbid, release_group_id="rg-2")},
    )
    monkeypatch.setattr(jq, "get_musicbrainz_service", lambda: fake_service)

    job = SimpleNamespace(
        output_template={
            "recording_mbid": recording_mbid,
            "canonical_metadata": {
                "album": "Album Name",
                "release_date": "2018",
                "track_number": 7,
                "disc_number": 1,
                "mb_release_id": release_id,
            },
        }
    )

    jq._ensure_release_enriched(job)
    metadata = job.output_template["canonical_metadata"]
    assert metadata.get("mb_release_group_id") == "rg-2"


def test_no_unknown_album_fallback_occurs(jq):
    with pytest.raises(RuntimeError, match="music_release_metadata_incomplete_before_path_build"):
        jq.build_audio_filename(
            {
                "artist": "Artist Name",
                "track": "Track Name",
                "track_number": 1,
                "disc_number": 1,
                "release_date": "2019",
                "mb_release_group_id": "rg-3",
                # album intentionally missing
            },
            "mp3",
        )


def test_music_worker_enforces_canonical_artist_and_track_for_finalize(jq, monkeypatch):
    recording_mbid = "rec-canon"
    release_id = "rel-canon"
    fake_service = _FakeMBService(
        recording_payload={
            "recording": {
                "id": recording_mbid,
                "release-list": [{"id": release_id, "date": "2014-09-01"}],
            }
        },
        release_payloads={
            release_id: _valid_release_payload(
                release_id=release_id,
                recording_mbid=recording_mbid,
                title="Canonical Album",
                date="2014-09-01",
                release_group_id="rg-canon",
            ),
        },
    )
    monkeypatch.setattr(jq, "get_musicbrainz_service", lambda: fake_service)

    captured = {}

    def _fake_finalize_download_artifact(**kwargs):
        captured["meta"] = dict(kwargs.get("meta") or {})
        final_path = Path("/tmp/final-canon.mp3")
        final_path.write_bytes(b"ok")
        return str(final_path), captured["meta"]

    def _fake_download_with_ytdlp(*args, **kwargs):
        temp_dir = Path(args[1])
        temp_dir.mkdir(parents=True, exist_ok=True)
        out = temp_dir / "tmp.mp3"
        out.write_bytes(b"ok")
        return {"id": "vid-canon", "title": "YouTube Messy Title", "uploader": "Uploader"}, str(out)

    monkeypatch.setattr(jq, "download_with_ytdlp", _fake_download_with_ytdlp)
    monkeypatch.setattr(
        jq,
        "extract_meta",
        lambda info, fallback_url=None: {
            "video_id": info.get("id"),
            "title": "YouTube Messy Title",
            "artist": "Uploader",
            "track": "YouTube Messy Title",
        },
    )
    monkeypatch.setattr(jq, "finalize_download_artifact", _fake_finalize_download_artifact)

    job = SimpleNamespace(
        id="job-canon",
        url="musicbrainz://recording/rec-canon",
        source="music_import",
        origin="import",
        media_type="music",
        media_intent="music_track",
        resolved_destination="/downloads",
        output_template={
            "output_dir": "/downloads",
            "music_final_format": "mp3",
            "recording_mbid": recording_mbid,
            "canonical_metadata": {
                "artist": "Canonical Artist",
                "track": "Canonical Track",
                "album": "Canonical Album",
                "release_date": "2014",
                "track_number": 1,
                "disc_number": 1,
                "mb_release_id": release_id,
                "mb_release_group_id": "rg-canon",
            },
        },
    )

    paths = SimpleNamespace(
        single_downloads_dir="/downloads",
        temp_downloads_dir=str(Path("/tmp") / "retreivr-test-canon"),
        thumbs_dir=str(Path("/tmp") / "retreivr-test-thumbs"),
    )

    adapter = jq.YouTubeAdapter()
    result = adapter.execute(
        job,
        {"music_final_format": "mp3", "final_format": "mkv"},
        paths,
        media_type="music",
        media_intent="music_track",
    )

    assert result is not None
    assert captured["meta"]["artist"] == "Canonical Artist"
    assert captured["meta"]["album_artist"] == "Canonical Artist"
    assert captured["meta"]["track"] == "Canonical Track"
    assert captured["meta"]["title"] == "Canonical Track"
