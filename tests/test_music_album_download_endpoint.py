from __future__ import annotations

import importlib
import json
import sqlite3
import sys
import types

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    # Optional runtime dependencies used by api.main imports.
    if "google_auth_oauthlib" not in sys.modules:
        google_auth_oauthlib = types.ModuleType("google_auth_oauthlib")
        sys.modules["google_auth_oauthlib"] = google_auth_oauthlib
    if "google_auth_oauthlib.flow" not in sys.modules:
        flow_mod = types.ModuleType("google_auth_oauthlib.flow")
        flow_mod.InstalledAppFlow = object
        sys.modules["google_auth_oauthlib.flow"] = flow_mod
    if "googleapiclient" not in sys.modules:
        googleapiclient = types.ModuleType("googleapiclient")
        sys.modules["googleapiclient"] = googleapiclient
    if "googleapiclient.errors" not in sys.modules:
        errors_mod = types.ModuleType("googleapiclient.errors")
        errors_mod.HttpError = Exception
        sys.modules["googleapiclient.errors"] = errors_mod
    if "google" not in sys.modules:
        google_mod = types.ModuleType("google")
        sys.modules["google"] = google_mod
    if "google.auth" not in sys.modules:
        google_auth_mod = types.ModuleType("google.auth")
        sys.modules["google.auth"] = google_auth_mod
    if "google.auth.exceptions" not in sys.modules:
        google_auth_exc_mod = types.ModuleType("google.auth.exceptions")
        google_auth_exc_mod.RefreshError = Exception
        sys.modules["google.auth.exceptions"] = google_auth_exc_mod
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")

    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return TestClient(module.app)


def test_album_download_returns_partial_success_instead_of_500(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    class _MB:
        def _call_with_retry(self, func):
            return func()

        def fetch_release_group_cover_art_url(self, _release_group_id, timeout=8):
            _ = timeout
            return "https://img.test/cover.jpg"

        def cover_art_url(self, release_id):
            return f"https://coverartarchive.org/release/{release_id}/front"

    monkeypatch.setattr("api.main._mb_service", lambda: _MB())
    monkeypatch.setattr("api.main._read_config_or_404", lambda: {"music_mb_binding_threshold": 0.78})

    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_group_by_id",
        lambda _rg, includes=None: {
            "release-group": {
                "release-list": [
                    {"id": "rel-1", "status": "Official", "country": "US"},
                ]
            }
        },
    )
    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_by_id",
        lambda _rid, includes=None: {
            "release": {
                "id": "rel-1",
                "title": "Album Name",
                "date": "2010-01-01",
                "artist-credit": [{"name": "Artist Name"}],
                "medium-list": [
                    {
                        "position": "1",
                        "track-list": [
                            {
                                "position": "1",
                                "title": "Track A",
                                "recording": {"id": "rec-1", "title": "Track A", "length": "210000"},
                            },
                            {
                                "position": "2",
                                "title": "Track B",
                                "recording": {"id": "rec-2", "title": "Track B", "length": "211000"},
                            },
                        ],
                    }
                ],
            }
        },
    )

    def _fake_resolve(_mb, *, artist=None, track=None, album=None, duration_ms=None, threshold=None, max_duration_delta_ms=None, **kwargs):
        _ = artist, album, duration_ms, threshold, max_duration_delta_ms, kwargs
        if track == "Track B":
            raise RuntimeError("forced_track_failure")
        return {
            "recording_mbid": "rec-1",
            "mb_release_id": "rel-1",
            "mb_release_group_id": "rg-1",
            "artist": "Artist Name",
            "album": "Album Name",
            "track_number": 1,
            "disc_number": 1,
            "release_date": "2010",
            "duration_ms": 210000,
        }

    monkeypatch.setattr("api.main.resolve_best_mb_pair", _fake_resolve)

    enqueued: list[dict] = []

    def _capture_enqueue(self, payload: dict) -> None:
        enqueued.append(dict(payload))

    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", _capture_enqueue)

    response = client.post(
        "/api/music/album/download",
        json={"release_group_mbid": "rg-1"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["tracks_enqueued"] == 1
    assert payload["failed_tracks_count"] == 1
    assert len(payload["failed_tracks"]) == 1
    assert payload["failed_tracks"][0]["track"] == "Track B"
    assert "forced_track_failure" in payload["failed_tracks"][0]["reason"]
    assert len(enqueued) == 1
    assert enqueued[0].get("genre") is None


def test_album_download_enforces_album_context_for_featured_tracks(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    class _MB:
        def _call_with_retry(self, func):
            return func()

        def fetch_release_group_cover_art_url(self, _release_group_id, timeout=8):
            _ = timeout
            return "https://img.test/cover.jpg"

        def cover_art_url(self, release_id):
            return f"https://coverartarchive.org/release/{release_id}/front"

    monkeypatch.setattr("api.main._mb_service", lambda: _MB())
    monkeypatch.setattr("api.main._read_config_or_404", lambda: {"music_mb_binding_threshold": 0.78})

    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_group_by_id",
        lambda _rg, includes=None: {
            "release-group": {
                "release-list": [
                    {"id": "rel-1", "status": "Official", "country": "US"},
                ]
            }
        },
    )
    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_by_id",
        lambda _rid, includes=None: {
            "release": {
                "id": "rel-1",
                "title": "Canonical Album",
                "date": "2010-01-01",
                "genre-list": [{"name": "Country", "count": "25"}],
                "artist-credit": [{"name": "Main Artist"}],
                "medium-list": [
                    {
                        "position": "1",
                        "track-list": [
                            {
                                "position": "1",
                                "title": "Track A",
                                "recording": {"id": "rec-1", "title": "Track A", "length": "210000"},
                            },
                            {
                                "position": "2",
                                "title": "Track B",
                                "recording": {
                                    "id": "rec-2",
                                    "title": "Track B",
                                    "length": "211000",
                                    "artist-credit": [{"name": "Main Artist"}, " feat. ", {"name": "Grace Potter"}],
                                },
                            },
                        ],
                    }
                ],
            }
        },
    )

    def _fake_resolve(_mb, *, track=None, **kwargs):
        _ = kwargs
        if track == "Track B":
            return {
                "recording_mbid": "rec-2",
                "mb_release_id": "rel-featured",
                "mb_release_group_id": "rg-featured",
                "artist": "Main Artist feat. Grace Potter",
                "album": "Featured Album Variant",
                "track_number": 99,
                "disc_number": 7,
                "release_date": "2001",
                "duration_ms": 211000,
            }
        return {
            "recording_mbid": "rec-1",
            "mb_release_id": "rel-1",
            "mb_release_group_id": "rg-1",
            "artist": "Main Artist",
            "album": "Canonical Album",
            "track_number": 1,
            "disc_number": 1,
            "release_date": "2010",
            "duration_ms": 210000,
        }

    monkeypatch.setattr("api.main.resolve_best_mb_pair", _fake_resolve)

    enqueued: list[dict] = []

    def _capture_enqueue(self, payload: dict) -> None:
        enqueued.append(dict(payload))

    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", _capture_enqueue)

    response = client.post(
        "/api/music/album/download",
        json={"release_group_mbid": "rg-1"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["tracks_enqueued"] == 2
    assert len(enqueued) == 2
    assert [item["track_number"] for item in enqueued] == [1, 2]
    assert all(item["disc_number"] == 1 for item in enqueued)
    assert all(item["track_total"] == 2 for item in enqueued)
    assert all(item["disc_total"] == 1 for item in enqueued)
    assert all(item["album"] == "Canonical Album" for item in enqueued)
    assert all(item["album_artist"] == "Main Artist" for item in enqueued)
    assert all(item["mb_release_id"] == "rel-1" for item in enqueued)
    assert all(item["mb_release_group_id"] == "rg-1" for item in enqueued)
    assert all(item["artwork_url"] == "https://img.test/cover.jpg" for item in enqueued)
    assert all(item["genre"] == "Country" for item in enqueued)


def test_album_download_falls_back_to_resolved_genre_when_release_has_no_tags(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    class _MB:
        def _call_with_retry(self, func):
            return func()

        def fetch_release_group_cover_art_url(self, _release_group_id, timeout=8):
            _ = timeout
            return "https://img.test/cover.jpg"

        def cover_art_url(self, release_id):
            return f"https://coverartarchive.org/release/{release_id}/front"

    monkeypatch.setattr("api.main._mb_service", lambda: _MB())
    monkeypatch.setattr("api.main._read_config_or_404", lambda: {"music_mb_binding_threshold": 0.78})

    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_group_by_id",
        lambda _rg, includes=None: {
            "release-group": {
                "release-list": [
                    {"id": "rel-1", "status": "Official", "country": "US"},
                ]
            }
        },
    )
    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_by_id",
        lambda _rid, includes=None: {
            "release": {
                "id": "rel-1",
                "title": "Canonical Album",
                "date": "2010-01-01",
                "artist-credit": [{"name": "Main Artist"}],
                "medium-list": [
                    {
                        "position": "1",
                        "track-list": [
                            {
                                "position": "1",
                                "title": "Track A",
                                "recording": {"id": "rec-1", "title": "Track A", "length": "210000"},
                            },
                        ],
                    }
                ],
            }
        },
    )

    monkeypatch.setattr(
        "api.main.resolve_best_mb_pair",
        lambda *_args, **_kwargs: {
            "recording_mbid": "rec-1",
            "mb_release_id": "rel-1",
            "mb_release_group_id": "rg-1",
            "artist": "Main Artist",
            "album": "Canonical Album",
            "track_number": 1,
            "disc_number": 1,
            "release_date": "2010",
            "genre": "Rock",
            "duration_ms": 210000,
        },
    )

    enqueued: list[dict] = []
    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", lambda self, payload: enqueued.append(dict(payload)))

    response = client.post(
        "/api/music/album/download",
        json={"release_group_mbid": "rg-1"},
    )

    assert response.status_code == 200
    assert len(enqueued) == 1
    assert enqueued[0]["genre"] == "Rock"


def test_album_download_uses_release_group_genre_before_resolved_fallback(monkeypatch) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    class _MB:
        def _call_with_retry(self, func):
            return func()

        def fetch_release_group_cover_art_url(self, _release_group_id, timeout=8):
            _ = timeout
            return "https://img.test/cover.jpg"

        def cover_art_url(self, release_id):
            return f"https://coverartarchive.org/release/{release_id}/front"

    monkeypatch.setattr("api.main._mb_service", lambda: _MB())
    monkeypatch.setattr("api.main._read_config_or_404", lambda: {"music_mb_binding_threshold": 0.78})

    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_group_by_id",
        lambda _rg, includes=None: {
            "release-group": {
                "genre-list": [{"name": "Americana", "count": "7"}],
                "release-list": [
                    {"id": "rel-1", "status": "Official", "country": "US"},
                ],
            }
        },
    )
    monkeypatch.setattr(
        module.musicbrainzngs,
        "get_release_by_id",
        lambda _rid, includes=None: {
            "release": {
                "id": "rel-1",
                "title": "Canonical Album",
                "date": "2010-01-01",
                "artist-credit": [{"name": "Main Artist"}],
                "medium-list": [
                    {
                        "position": "1",
                        "track-list": [
                            {
                                "position": "1",
                                "title": "Track A",
                                "recording": {"id": "rec-1", "title": "Track A", "length": "210000"},
                            },
                        ],
                    }
                ],
            }
        },
    )

    monkeypatch.setattr(
        "api.main.resolve_best_mb_pair",
        lambda *_args, **_kwargs: {
            "recording_mbid": "rec-1",
            "mb_release_id": "rel-1",
            "mb_release_group_id": "rg-1",
            "artist": "Main Artist",
            "album": "Canonical Album",
            "track_number": 1,
            "disc_number": 1,
            "release_date": "2010",
            "genre": "Rock",
            "duration_ms": 210000,
        },
    )

    enqueued: list[dict] = []
    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", lambda self, payload: enqueued.append(dict(payload)))

    response = client.post(
        "/api/music/album/download",
        json={"release_group_mbid": "rg-1"},
    )

    assert response.status_code == 200
    assert len(enqueued) == 1
    assert enqueued[0]["genre"] == "Americana"


def test_music_album_run_summary_endpoint_writes_artifact_and_classifies_failures(monkeypatch, tmp_path) -> None:
    client = _build_client(monkeypatch)
    module = importlib.import_module("api.main")

    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE download_jobs (
                id TEXT PRIMARY KEY,
                origin TEXT NOT NULL,
                origin_id TEXT NOT NULL,
                media_intent TEXT NOT NULL,
                status TEXT NOT NULL,
                last_error TEXT,
                output_template TEXT,
                created_at TEXT
            )
            """
        )
        rows = [
            (
                "j-ok",
                "music_album",
                "album-run-1",
                "music_track",
                "completed",
                None,
                json.dumps({"canonical_metadata": {"recording_mbid": "rec-ok", "mb_release_group_id": "rg-1"}}),
                "2026-02-26T00:00:01+00:00",
            ),
            (
                "j-unavailable",
                "music_album",
                "album-run-1",
                "music_track",
                "failed",
                "source_unavailable:removed_or_deleted",
                json.dumps({"canonical_metadata": {"recording_mbid": "rec-missing-1", "mb_release_group_id": "rg-1"}}),
                "2026-02-26T00:00:02+00:00",
            ),
            (
                "j-duration",
                "music_album",
                "album-run-1",
                "music_track",
                "failed",
                "duration_filtered",
                json.dumps({"canonical_metadata": {"recording_mbid": "rec-missing-2", "mb_release_group_id": "rg-1"}}),
                "2026-02-26T00:00:03+00:00",
            ),
        ]
        conn.executemany(
            "INSERT INTO download_jobs (id, origin, origin_id, media_intent, status, last_error, output_template, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(module, "DATA_DIR", tmp_path)
    module.app.state.paths = types.SimpleNamespace(db_path=str(db_path))

    response = client.get("/api/music/album/runs/album-run-1/summary", params={"release_group_mbid": "rg-1"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["tracks_total"] == 3
    assert payload["tracks_resolved"] == 1
    assert payload["unresolved_classification"]["source_unavailable"] == 1
    assert payload["unresolved_classification"]["no_viable_match"] == 1
    hint_counts = payload["why_missing"]["hint_counts"]
    assert hint_counts["Unavailable (blocked/removed)"] == 1
    assert hint_counts["Likely wrong MB recording length (duration mismatch persistent across many candidates)"] == 1

    summary_path = tmp_path / "run_summaries" / "music_album" / "album-run-1" / "run_summary.json"
    assert summary_path.exists()
    saved = json.loads(summary_path.read_text(encoding="utf-8"))
    assert saved["album_run_id"] == "album-run-1"
