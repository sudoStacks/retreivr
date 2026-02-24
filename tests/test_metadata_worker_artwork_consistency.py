from __future__ import annotations

import importlib
from pathlib import Path
import sys
import types


def _load_worker():
    if "rapidfuzz" not in sys.modules:
        rapidfuzz = types.ModuleType("rapidfuzz")
        rapidfuzz.fuzz = types.SimpleNamespace(token_set_ratio=lambda *_args, **_kwargs: 0)
        sys.modules["rapidfuzz"] = rapidfuzz
    if "mutagen" not in sys.modules:
        mutagen = types.ModuleType("mutagen")
        mutagen.File = lambda *_args, **_kwargs: None
        sys.modules["mutagen"] = mutagen
    if "PIL" not in sys.modules:
        pil_pkg = types.ModuleType("PIL")
        image_mod = types.ModuleType("PIL.Image")
        image_mod.open = lambda *_args, **_kwargs: None
        pil_pkg.Image = image_mod
        sys.modules["PIL"] = pil_pkg
        sys.modules["PIL.Image"] = image_mod
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")
    if "metadata.providers.musicbrainz" not in sys.modules:
        mb_provider = types.ModuleType("metadata.providers.musicbrainz")
        mb_provider.search_recordings = lambda *_args, **_kwargs: []
        sys.modules["metadata.providers.musicbrainz"] = mb_provider
    if "metadata.providers.acoustid" not in sys.modules:
        acoustid_provider = types.ModuleType("metadata.providers.acoustid")
        acoustid_provider.match_recording = lambda *_args, **_kwargs: None
        sys.modules["metadata.providers.acoustid"] = acoustid_provider
    sys.modules.pop("metadata.worker", None)
    return importlib.import_module("metadata.worker")


def test_worker_prefers_album_run_artwork_url_over_per_track_release(monkeypatch, tmp_path: Path) -> None:
    worker = _load_worker()

    track_path = tmp_path / "track.mp3"
    track_path.write_bytes(b"audio")

    monkeypatch.setattr(worker.matcher, "parse_source", lambda meta, file_path: {"artist": "Artist", "title": "Song"})
    monkeypatch.setattr(worker.matcher, "get_duration_seconds", lambda file_path: 200)
    monkeypatch.setattr(worker.musicbrainz_provider, "search_recordings", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        worker.matcher,
        "select_best_match",
        lambda source, candidates, duration: ({"artist": "Artist", "title": "Song", "release_id": "release-per-track"}, 99),
    )

    calls = {"url": 0, "release": 0}
    monkeypatch.setattr(
        worker.artwork_provider,
        "fetch_artwork_from_url",
        lambda artwork_url, max_size_px=1500, timeout=10: (
            calls.__setitem__("url", calls["url"] + 1) or {"data": b"url-art", "mime": "image/jpeg"}
        ),
    )
    monkeypatch.setattr(
        worker.artwork_provider,
        "fetch_artwork",
        lambda release_id, max_size_px=1500: (calls.__setitem__("release", calls["release"] + 1) or {"data": b"rel-art", "mime": "image/jpeg"}),
    )

    captured = {}
    monkeypatch.setattr(
        worker,
        "apply_tags",
        lambda file_path, tags, artwork, **kwargs: captured.update({"tags": tags, "artwork": artwork}),
    )

    worker._process_item(
        {
            "file_path": str(track_path),
            "config": {"embed_artwork": True, "confidence_threshold": 70},
            "meta": {
                "artist": "Artist",
                "track": "Song",
                "album_artist": "Album Artist",
                "artwork_url": "https://img.test/album-cover.jpg",
                "mb_release_id": "album-release",
            },
        }
    )

    assert calls["url"] == 1
    assert calls["release"] == 0
    assert captured["artwork"] == {"data": b"url-art", "mime": "image/jpeg"}


def test_worker_applies_album_run_artwork_even_when_match_confidence_fails(monkeypatch, tmp_path: Path) -> None:
    worker = _load_worker()

    track_path = tmp_path / "track.mp3"
    track_path.write_bytes(b"audio")

    monkeypatch.setattr(worker.matcher, "parse_source", lambda meta, file_path: {"artist": "Artist", "title": "Song"})
    monkeypatch.setattr(worker.matcher, "get_duration_seconds", lambda file_path: 200)
    monkeypatch.setattr(worker.musicbrainz_provider, "search_recordings", lambda *args, **kwargs: [])
    monkeypatch.setattr(worker.matcher, "select_best_match", lambda source, candidates, duration: ({}, 12))
    monkeypatch.setattr(
        worker.artwork_provider,
        "fetch_artwork_from_url",
        lambda artwork_url, max_size_px=1500, timeout=10: {"data": b"url-art", "mime": "image/jpeg"},
    )
    monkeypatch.setattr(worker.artwork_provider, "fetch_artwork", lambda release_id, max_size_px=1500: None)

    captured = {}
    monkeypatch.setattr(
        worker,
        "apply_tags",
        lambda file_path, tags, artwork, **kwargs: captured.update({"tags": tags, "artwork": artwork}),
    )

    worker._process_item(
        {
            "file_path": str(track_path),
            "config": {"embed_artwork": True, "confidence_threshold": 70},
            "meta": {
                "artist": "Artist",
                "track": "Song",
                "album": "Album",
                "album_artist": "Album Artist",
                "track_number": 2,
                "disc_number": 1,
                "artwork_url": "https://img.test/album-cover.jpg",
                "mb_release_id": "album-release",
            },
        }
    )

    assert captured["artwork"] == {"data": b"url-art", "mime": "image/jpeg"}
    assert captured["tags"]["artist"] == "Artist"
    assert captured["tags"]["album_artist"] == "Album Artist"
