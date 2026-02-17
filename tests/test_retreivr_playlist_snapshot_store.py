import sqlite3

from db.playlist_snapshots import PlaylistSnapshotStore


def _items() -> list[dict[str, object]]:
    return [
        {
            "spotify_track_id": "track-1",
            "position": 0,
            "added_at": "2026-02-16T00:00:00Z",
            "artist": "Artist 1",
            "title": "Title 1",
            "album": "Album 1",
            "duration_ms": 1000,
            "isrc": "ISRC1",
        },
        {
            "spotify_track_id": "track-2",
            "position": 1,
            "added_at": "2026-02-16T00:01:00Z",
            "artist": "Artist 2",
            "title": "Title 2",
            "album": "Album 2",
            "duration_ms": 2000,
            "isrc": "ISRC2",
        },
    ]


def test_store_snapshot_inserts_rows_and_preserves_positions(tmp_path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))

    result = store.store_snapshot("playlist-a", "snapshot-1", _items())

    assert result.inserted is True
    latest = store.get_latest_snapshot("playlist-a")
    assert latest is not None
    assert latest["snapshot_id"] == "snapshot-1"
    assert latest["track_count"] == 2
    assert [item["spotify_track_id"] for item in latest["items"]] == ["track-1", "track-2"]
    assert [item["position"] for item in latest["items"]] == [0, 1]


def test_store_snapshot_fast_path_when_snapshot_unchanged(tmp_path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))

    first = store.store_snapshot("playlist-a", "snapshot-1", _items())
    second = store.store_snapshot("playlist-a", "snapshot-1", _items())

    assert first.inserted is True
    assert second.inserted is False
    assert second.reason == "snapshot_unchanged"
    assert first.snapshot_db_id == second.snapshot_db_id

    with sqlite3.connect(db_path) as conn:
        snapshot_count = conn.execute("SELECT COUNT(*) FROM playlist_snapshots").fetchone()[0]
        item_count = conn.execute("SELECT COUNT(*) FROM playlist_snapshot_items").fetchone()[0]
    assert snapshot_count == 1
    assert item_count == 2


def test_store_snapshot_fast_path_when_hash_unchanged_even_if_snapshot_id_changes(tmp_path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))

    first = store.store_snapshot("playlist-a", "snapshot-1", _items())
    second = store.store_snapshot("playlist-a", "snapshot-2", _items())

    assert first.inserted is True
    assert second.inserted is False
    assert second.reason == "snapshot_hash_unchanged"
    assert first.snapshot_db_id == second.snapshot_db_id

    with sqlite3.connect(db_path) as conn:
        snapshot_count = conn.execute("SELECT COUNT(*) FROM playlist_snapshots").fetchone()[0]
        item_count = conn.execute("SELECT COUNT(*) FROM playlist_snapshot_items").fetchone()[0]
    assert snapshot_count == 1
    assert item_count == 2
