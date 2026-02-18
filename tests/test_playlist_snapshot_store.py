import sqlite3

from db.playlist_snapshots import PlaylistSnapshotStore


def _sample_items() -> list[dict[str, object]]:
    return [
        {
            "uri": "spotify:track:1",
            "track_id": "1",
            "added_at": "2026-02-09T00:00:00+00:00",
            "added_by": "user_a",
            "is_local": False,
            "name": "Track One",
        },
        {
            "uri": "spotify:track:2",
            "track_id": "2",
            "added_at": "2026-02-09T00:01:00+00:00",
            "added_by": "user_b",
            "is_local": False,
            "name": "Track Two",
        },
    ]


def test_snapshot_store_inserts_snapshot_and_items(tmp_path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))

    result = store.insert_snapshot(
        source="spotify",
        playlist_id="playlist-1",
        snapshot_id="snap-1",
        items=_sample_items(),
    )

    assert result.inserted is True
    latest = store.get_latest_snapshot("spotify", "playlist-1")
    assert latest is not None
    assert latest["snapshot_id"] == "snap-1"
    assert latest["track_count"] == 2
    assert [item["track_uri"] for item in latest["items"]] == [
        "spotify:track:1",
        "spotify:track:2",
    ]


def test_snapshot_store_fast_path_for_same_snapshot_id(tmp_path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))
    store.insert_snapshot(
        source="spotify",
        playlist_id="playlist-1",
        snapshot_id="snap-1",
        items=_sample_items(),
    )

    second = store.insert_snapshot(
        source="spotify",
        playlist_id="playlist-1",
        snapshot_id="snap-1",
        items=_sample_items(),
    )

    assert second.inserted is False
    assert second.reason == "snapshot_unchanged"

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM playlist_snapshots").fetchone()[0]
    assert count == 1


def test_snapshot_store_tracks_latest_snapshot_uris(tmp_path) -> None:
    db_path = tmp_path / "snapshots.sqlite"
    store = PlaylistSnapshotStore(str(db_path))
    store.insert_snapshot(
        source="spotify",
        playlist_id="playlist-2",
        snapshot_id="snap-1",
        items=_sample_items(),
    )
    updated_items = _sample_items() + [
        {
            "uri": "spotify:track:3",
            "track_id": "3",
            "added_at": "2026-02-09T00:02:00+00:00",
            "added_by": "user_c",
            "is_local": False,
            "name": "Track Three",
        }
    ]
    store.insert_snapshot(
        source="spotify",
        playlist_id="playlist-2",
        snapshot_id="snap-2",
        items=updated_items,
    )

    latest_uris = store.get_latest_track_uris("spotify", "playlist-2")
    assert latest_uris == ["spotify:track:1", "spotify:track:2", "spotify:track:3"]
