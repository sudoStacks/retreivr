from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@pytest.fixture()
def api_module(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "spotify_intent.sqlite"
    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.paths = SimpleNamespace(db_path=str(db_path))
    module.app.state.worker_engine = SimpleNamespace(store=module.DownloadJobStore(str(db_path)))
    module.app.state.search_service = SimpleNamespace()
    module.app.state.search_request_overrides = {}
    module.app.state.music_cover_art_cache = {}
    return module


@pytest.fixture()
def api_client(api_module) -> TestClient:
    return TestClient(api_module.app)


def test_spotify_playlist_intent_ingestion_enqueues_music_track_jobs(
    api_module,
    api_client: TestClient,
    monkeypatch,
) -> None:
    class _FakeSpotifyClient:
        def get_playlist_items(self, playlist_id: str):
            assert playlist_id == "PL12345678"
            return "snapshot-1", [
                {
                    "spotify_track_id": "sp-track-1",
                    "position": 0,
                    "added_at": "2026-02-17T00:00:00Z",
                    "artist": "Intent Artist",
                    "title": "Intent Song",
                    "album": "Intent Album",
                    "duration_ms": 212000,
                    "isrc": "USINT1234567",
                }
            ]

    monkeypatch.setattr(api_module, "_read_config_or_404", lambda: {"spotify_playlists": []})
    monkeypatch.setattr(api_module, "_build_spotify_client_with_optional_oauth", lambda _cfg: _FakeSpotifyClient())

    response = api_client.post(
        "/api/intent/execute",
        json={"intent_type": "spotify_playlist", "identifier": "PL12345678"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert body["intent_type"] == "spotify_playlist"
    assert body["enqueued_count"] == 1

    conn = sqlite3.connect(api_module.app.state.paths.db_path)
    try:
        row = conn.execute(
            """
            SELECT origin, origin_id, media_intent, source, url, output_template
            FROM download_jobs
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "spotify_playlist"
    assert row[1] == "PL12345678"
    assert row[2] == "music_track"
    assert row[3] == "youtube_music"
    assert row[4].startswith("https://music.youtube.com/search?q=")

    output_template = json.loads(row[5]) if row[5] else {}
    assert output_template.get("artist") == "Intent Artist"
    assert output_template.get("track") == "Intent Song"
    assert output_template.get("album") == "Intent Album"
    assert output_template.get("duration_ms") == 212000
    assert "spotify" not in json.dumps(output_template).lower()


def test_spotify_oauth_premium_still_prefers_musicbrainz_first(monkeypatch) -> None:
    from metadata.canonical import CanonicalMetadataResolver

    calls = {"mb": 0, "spotify": 0}

    class _FakeMusicBrainzProvider:
        def __init__(self, *, min_confidence=0.70):
            _ = min_confidence

        def resolve_track(self, artist, track, *, album=None):
            calls["mb"] += 1
            return {
                "kind": "track",
                "provider": "musicbrainz",
                "artist": artist,
                "track": track,
                "album": album,
                "external_ids": {"musicbrainz_recording_id": "mbid-123"},
            }

        def resolve_album(self, artist, album):
            return None

    class _FakeSpotifyProvider:
        def __init__(self, **kwargs):
            _ = kwargs

        def resolve_track(self, artist, track, album=None):
            calls["spotify"] += 1
            return {
                "kind": "track",
                "provider": "spotify",
                "artist": artist,
                "track": track,
                "album": album,
                "external_ids": {"spotify_id": "sp-123"},
            }

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _FakeMusicBrainzProvider)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _FakeSpotifyProvider)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: True)

    resolver = CanonicalMetadataResolver(
        config={
            "spotify": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "oauth_access_token": "premium-oauth-token",
            }
        }
    )

    result = resolver.resolve_track("Intent Artist", "Intent Song", album="Intent Album")
    assert result is not None
    assert result.get("provider") == "musicbrainz"
    assert calls["mb"] == 1
    assert calls["spotify"] == 0
    assert "spotify" not in json.dumps(result).lower()
