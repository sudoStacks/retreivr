from __future__ import annotations

from pathlib import Path
from typing import Any

from playlist.rebuild import rebuild_playlist_from_tracks as _real_rebuild
from scheduler.jobs.spotify_playlist_watch import playlist_watch_job


def _item(track_id: str, position: int) -> dict[str, Any]:
    return {
        "spotify_track_id": track_id,
        "position": position,
        "added_at": f"2026-02-16T00:0{position}:00Z",
        "artist": f"artist-{track_id}",
        "title": f"title-{track_id}",
        "album": f"album-{track_id}",
        "duration_ms": 1000 + position,
        "isrc": f"isrc-{track_id}",
    }


class _MockSpotifyClient:
    def __init__(self, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.snapshot_id = snapshot_id
        self.items = items

    def get_playlist_items(self, playlist_id: str) -> tuple[str, list[dict[str, Any]]]:
        return self.snapshot_id, list(self.items)


class _MockSnapshotStore:
    def __init__(self, latest_snapshot: dict[str, Any] | None) -> None:
        self.latest_snapshot = latest_snapshot
        self.store_calls: list[tuple[str, str, list[dict[str, Any]]]] = []

    def get_latest_snapshot(self, playlist_id: str) -> dict[str, Any] | None:
        return self.latest_snapshot

    def store_snapshot(self, playlist_id: str, snapshot_id: str, items: list[dict[str, Any]]) -> Any:
        self.store_calls.append((playlist_id, snapshot_id, list(items)))
        return type("WriteResult", (), {"snapshot_db_id": 42})()


def test_playlist_watch_job_rebuilds_m3u_after_successful_sync(tmp_path, monkeypatch) -> None:
    music_root = tmp_path / "Music"
    playlist_root = tmp_path / "Playlists"
    track_paths: list[str] = []
    for n in (1, 2, 3):
        track = music_root / "Artist A" / "Album A (2020)" / "Disc 1" / f"{n:02d} - Song {n}.mp3"
        track.parent.mkdir(parents=True, exist_ok=True)
        track.write_bytes(b"x")
        track_paths.append(str(track))

    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch._load_downloaded_track_paths",
        lambda playlist_id: list(track_paths),
    )
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch._resolve_playlist_dirs",
        lambda config: (playlist_root, music_root),
    )

    calls: list[dict[str, Any]] = []

    def _spy_rebuild(playlist_name, playlist_root, music_root, track_file_paths):
        calls.append(
            {
                "playlist_name": playlist_name,
                "playlist_root": Path(playlist_root),
                "music_root": Path(music_root),
                "track_file_paths": list(track_file_paths),
            }
        )
        return _real_rebuild(
            playlist_name=playlist_name,
            playlist_root=Path(playlist_root),
            music_root=Path(music_root),
            track_file_paths=track_file_paths,
        )

    monkeypatch.setattr("scheduler.jobs.spotify_playlist_watch.rebuild_playlist_from_tracks", _spy_rebuild)

    prev_items = [_item("a", 0)]
    curr_items = [_item("a", 0), _item("b", 1)]
    store = _MockSnapshotStore({"snapshot_id": "snap-1", "items": prev_items})
    client = _MockSpotifyClient("snap-2", curr_items)
    enqueued: list[str] = []

    result = playlist_watch_job(
        client,
        store,
        lambda item: enqueued.append(str(item["spotify_track_id"])),
        "playlist-1",
        playlist_name="Country Bangers",
    )

    assert result["status"] == "updated"
    assert len(calls) == 1
    assert calls[0]["playlist_name"] == "Country Bangers"
    assert calls[0]["track_file_paths"] == track_paths

    m3u_path = playlist_root / "Country Bangers.m3u"
    assert m3u_path.exists() is True
    content = m3u_path.read_text(encoding="utf-8")
    assert "Artist A/Album A (2020)/Disc 1/01 - Song 1.mp3" in content
    assert "Artist A/Album A (2020)/Disc 1/02 - Song 2.mp3" in content
    assert "Artist A/Album A (2020)/Disc 1/03 - Song 3.mp3" in content
