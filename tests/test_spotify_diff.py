from __future__ import annotations

from spotify.diff import diff_playlist


def _item(track_id: str, position: int, *, added_at: str = "2026-02-16T00:00:00Z") -> dict:
    return {
        "spotify_track_id": track_id,
        "position": position,
        "added_at": added_at,
        "artist": f"artist-{track_id}",
        "title": f"title-{track_id}",
        "album": f"album-{track_id}",
        "duration_ms": 1000,
        "isrc": f"isrc-{track_id}",
    }


def test_diff_playlist_no_change() -> None:
    prev = [_item("a", 0), _item("b", 1)]
    curr = [_item("a", 0), _item("b", 1)]

    diff = diff_playlist(prev, curr)

    assert diff["added"] == []
    assert diff["removed"] == []
    assert diff["moved"] == []


def test_diff_playlist_simple_add() -> None:
    prev = [_item("a", 0)]
    curr = [_item("a", 0), _item("b", 1)]

    diff = diff_playlist(prev, curr)

    assert [item["spotify_track_id"] for item in diff["added"]] == ["b"]
    assert diff["removed"] == []
    assert diff["moved"] == []


def test_diff_playlist_simple_remove() -> None:
    prev = [_item("a", 0), _item("b", 1)]
    curr = [_item("a", 0)]

    diff = diff_playlist(prev, curr)

    assert diff["added"] == []
    assert [item["spotify_track_id"] for item in diff["removed"]] == ["b"]
    assert diff["moved"] == []


def test_diff_playlist_moved_only() -> None:
    prev = [_item("a", 0), _item("b", 1), _item("c", 2)]
    curr = [_item("b", 0), _item("a", 1), _item("c", 2)]

    diff = diff_playlist(prev, curr)

    assert diff["added"] == []
    assert diff["removed"] == []
    moved = diff["moved"]
    assert [entry["spotify_track_id"] for entry in moved] == ["b", "a"]
    assert moved[0]["from_position"] == 1
    assert moved[0]["to_position"] == 0
    assert moved[1]["from_position"] == 0
    assert moved[1]["to_position"] == 1


def test_diff_playlist_combination_add_remove_move_with_duplicates() -> None:
    prev = [
        _item("a", 0, added_at="2026-02-16T00:00:00Z"),
        _item("x", 1, added_at="2026-02-16T00:01:00Z"),
        _item("a", 2, added_at="2026-02-16T00:02:00Z"),
        _item("b", 3, added_at="2026-02-16T00:03:00Z"),
    ]
    curr = [
        _item("a", 0, added_at="2026-02-16T00:10:00Z"),
        _item("a", 1, added_at="2026-02-16T00:11:00Z"),
        _item("c", 2, added_at="2026-02-16T00:12:00Z"),
        _item("x", 3, added_at="2026-02-16T00:13:00Z"),
    ]

    diff = diff_playlist(prev, curr)

    assert [item["spotify_track_id"] for item in diff["added"]] == ["c"]
    assert [item["spotify_track_id"] for item in diff["removed"]] == ["b"]
    moved = diff["moved"]
    assert [entry["spotify_track_id"] for entry in moved] == ["a", "x"]
    assert moved[0]["from_position"] == 2
    assert moved[0]["to_position"] == 1
    assert moved[1]["from_position"] == 1
    assert moved[1]["to_position"] == 3

