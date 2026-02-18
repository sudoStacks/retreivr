from __future__ import annotations

from typing import Any

from spotify.client import SpotifyPlaylistClient


def test_get_playlist_items_empty_playlist(monkeypatch) -> None:
    client = SpotifyPlaylistClient(client_id="id", client_secret="secret")

    def fake_request_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"snapshot_id": "snap-empty", "tracks": {"items": [], "next": None}}

    monkeypatch.setattr(client, "_request_json", fake_request_json)

    snapshot_id, items = client.get_playlist_items("playlist-empty")

    assert snapshot_id == "snap-empty"
    assert items == []


def test_get_playlist_items_preserves_duplicates_and_order(monkeypatch) -> None:
    client = SpotifyPlaylistClient(client_id="id", client_secret="secret")

    page_one = {
        "snapshot_id": "snap-dup",
        "tracks": {
            "items": [
                {
                    "added_at": "2026-02-01T00:00:00Z",
                    "track": {
                        "id": "track-1",
                        "name": "Song A",
                        "duration_ms": 1000,
                        "external_ids": {"isrc": "ISRC_A"},
                        "album": {"name": "Album A"},
                        "artists": [{"name": "Artist A"}],
                    },
                },
                {
                    "added_at": "2026-02-01T00:01:00Z",
                    "track": {
                        "id": "track-1",
                        "name": "Song A",
                        "duration_ms": 1000,
                        "external_ids": {"isrc": "ISRC_A"},
                        "album": {"name": "Album A"},
                        "artists": [{"name": "Artist A"}],
                    },
                },
            ],
            "next": "https://api.spotify.com/v1/playlists/p/tracks?offset=2&limit=2",
        },
    }
    page_two = {
        "items": [
            {
                "added_at": "2026-02-01T00:02:00Z",
                "track": {
                    "id": "track-2",
                    "name": "Song B",
                    "duration_ms": 2000,
                    "external_ids": {"isrc": "ISRC_B"},
                    "album": {"name": "Album B"},
                    "artists": [{"name": "Artist B"}],
                },
            }
        ],
        "next": None,
    }

    calls: list[str] = []

    def fake_request_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        calls.append(url)
        if "playlists" in url and "offset=2" not in url:
            return page_one
        return page_two

    monkeypatch.setattr(client, "_request_json", fake_request_json)

    snapshot_id, items = client.get_playlist_items("playlist-dup")

    assert snapshot_id == "snap-dup"
    assert [item["spotify_track_id"] for item in items] == ["track-1", "track-1", "track-2"]
    assert [item["position"] for item in items] == [0, 1, 2]
    assert items[0]["artist"] == "Artist A"
    assert items[2]["title"] == "Song B"
    assert items[2]["album"] == "Album B"
    assert items[2]["duration_ms"] == 2000
    assert items[2]["isrc"] == "ISRC_B"
    assert len(calls) == 2


def test_get_playlist_items_drops_null_track_entries(monkeypatch) -> None:
    client = SpotifyPlaylistClient(client_id="id", client_secret="secret")

    def fake_request_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "snapshot_id": "snap-null",
            "tracks": {
                "items": [
                    {"added_at": "2026-02-01T00:00:00Z", "track": None},
                    {
                        "added_at": "2026-02-01T00:01:00Z",
                        "track": {
                            "id": "track-3",
                            "name": "Song C",
                            "duration_ms": 3000,
                            "external_ids": {"isrc": "ISRC_C"},
                            "album": {"name": "Album C"},
                            "artists": [{"name": "Artist C"}],
                        },
                    },
                ],
                "next": None,
            },
        }

    monkeypatch.setattr(client, "_request_json", fake_request_json)

    snapshot_id, items = client.get_playlist_items("playlist-null")

    assert snapshot_id == "snap-null"
    assert len(items) == 1
    assert items[0]["spotify_track_id"] == "track-3"
    assert items[0]["position"] == 1

