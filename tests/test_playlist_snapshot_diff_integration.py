from __future__ import annotations

from pathlib import Path
from typing import Any

from db.playlist_snapshots import PlaylistSnapshotStore
from scheduler.jobs.spotify_playlist_watch import playlist_watch_job


def _item(track_id: str, position: int) -> dict[str, Any]:
    return {
        "spotify_track_id": track_id,
        "position": position,
        "added_at": f"2026-02-17T00:0{position}:00Z",
        "artist": f"artist-{track_id}",
        "title": f"title-{track_id}",
        "album": f"album-{track_id}",
        "duration_ms": 1000 + position,
        "isrc": f"isrc-{track_id}",
    }


class _StaticClient:
    def __init__(self, snapshot_id: str, items: list[dict[str, Any]]) -> None:
        self.snapshot_id = snapshot_id
        self.items = list(items)

    def get_playlist_items(self, playlist_id: str) -> tuple[str, list[dict[str, Any]]]:
        return self.snapshot_id, list(self.items)


def test_reordered_playlist_produces_no_new_jobs(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))
    playlist_id = "playlist-1234"
    previous_items = [_item("a", 0), _item("b", 1), _item("c", 2)]
    store.store_snapshot(playlist_id, "snap-old", previous_items)

    current_items = [_item("b", 0), _item("a", 1), _item("c", 2)]
    client = _StaticClient("snap-new", current_items)
    enqueued: list[str] = []

    result = playlist_watch_job(
        client,
        store,
        lambda item: enqueued.append(item["spotify_track_id"]),
        playlist_id,
        config={
            "music_download_folder": str(tmp_path / "Music"),
            "playlists_folder": str(tmp_path / "Playlists"),
        },
    )

    assert result["status"] == "updated"
    assert result["added_count"] == 0
    assert result["moved_count"] == 2
    assert result["enqueued"] == 0
    assert enqueued == []
    assert result["run_summary"]["added"] == 0
    assert result["run_summary"]["completed"] == 0


def test_removed_track_does_not_delete_local_files_unless_explicitly_configured(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))
    playlist_id = "playlist-5678"
    store.store_snapshot(playlist_id, "snap-prev", [_item("a", 0), _item("b", 1)])
    client = _StaticClient("snap-next", [_item("a", 0)])

    delete_calls: list[Path] = []
    original_unlink = Path.unlink

    def _spy_unlink(path_self: Path, *args, **kwargs):
        delete_calls.append(path_self)
        return original_unlink(path_self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _spy_unlink)
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch._load_downloaded_track_paths",
        lambda _playlist_id: [],
    )
    monkeypatch.setattr(
        "scheduler.jobs.spotify_playlist_watch._resolve_playlist_dirs",
        lambda _config: (tmp_path / "Playlists", tmp_path / "Music"),
    )

    result = playlist_watch_job(client, store, lambda _item: None, playlist_id, playlist_name="NoDelete")

    assert result["status"] == "updated"
    assert result["removed_count"] == 1
    assert result["enqueued"] == 0
    assert delete_calls == []


def test_crash_restart_recovery_is_idempotent_after_snapshot_persist(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    playlist_id = "playlist-9012"
    first_store = PlaylistSnapshotStore(str(db_path))
    first_store.store_snapshot(playlist_id, "snap-prev", [_item("a", 0)])

    client = _StaticClient("snap-next", [_item("a", 0), _item("b", 1)])
    first_enqueued: list[str] = []
    first_result = playlist_watch_job(
        client,
        first_store,
        lambda item: first_enqueued.append(item["spotify_track_id"]),
        playlist_id,
        config={
            "music_download_folder": str(tmp_path / "Music"),
            "playlists_folder": str(tmp_path / "Playlists"),
        },
    )

    assert first_result["status"] == "updated"
    assert first_enqueued == ["b"]
    assert first_result["enqueued"] == 1

    # Simulate process restart by creating fresh store/client instances.
    second_store = PlaylistSnapshotStore(str(db_path))
    second_client = _StaticClient("snap-next", [_item("a", 0), _item("b", 1)])
    second_enqueued: list[str] = []
    second_result = playlist_watch_job(
        second_client,
        second_store,
        lambda item: second_enqueued.append(item["spotify_track_id"]),
        playlist_id,
        config={
            "music_download_folder": str(tmp_path / "Music"),
            "playlists_folder": str(tmp_path / "Playlists"),
        },
    )

    assert second_result["status"] == "unchanged"
    assert second_result["enqueued"] == 0
    assert second_enqueued == []
    assert second_result["run_summary"]["completed"] == 0
