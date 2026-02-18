from spotify.diff import diff_playlist


def test_diff_playlist_added_removed_and_moved() -> None:
    prev = ["a", "b", "c"]
    curr = ["b", "a", "d"]

    changes = diff_playlist(prev, curr)

    assert changes["added"] == ["d"]
    assert changes["removed"] == ["c"]
    assert changes["moved"] == [
        {"uri": "a", "from": 0, "to": 1},
        {"uri": "b", "from": 1, "to": 0},
    ]


def test_diff_playlist_honors_duplicates() -> None:
    prev = ["x", "y", "x"]
    curr = ["x", "x", "y", "x"]

    changes = diff_playlist(prev, curr)

    assert changes["added"] == ["x"]
    assert changes["removed"] == []
    assert changes["moved"] == [
        {"uri": "x", "from": 2, "to": 1},
        {"uri": "y", "from": 1, "to": 2},
    ]


def test_diff_playlist_handles_empty_lists() -> None:
    changes = diff_playlist([], [])

    assert changes == {"added": [], "removed": [], "moved": []}
