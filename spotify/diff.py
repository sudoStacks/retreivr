"""Deterministic playlist diff utilities."""

from __future__ import annotations

from collections import Counter, defaultdict, deque


def diff_playlist(prev: list[str], curr: list[str]) -> dict[str, list]:
    """Return duplicate-aware added/removed/moved changes from prev to curr order."""
    prev_list = [value for value in prev if value]
    curr_list = [value for value in curr if value]

    prev_counts = Counter(prev_list)
    curr_counts = Counter(curr_list)

    added: list[str] = []
    running_curr = Counter()
    for uri in curr_list:
        running_curr[uri] += 1
        if running_curr[uri] > prev_counts.get(uri, 0):
            added.append(uri)

    removed: list[str] = []
    running_prev = Counter()
    for uri in prev_list:
        running_prev[uri] += 1
        if running_prev[uri] > curr_counts.get(uri, 0):
            removed.append(uri)

    prev_positions: dict[str, deque[int]] = defaultdict(deque)
    curr_positions: dict[str, deque[int]] = defaultdict(deque)
    for idx, uri in enumerate(prev_list):
        prev_positions[uri].append(idx)
    for idx, uri in enumerate(curr_list):
        curr_positions[uri].append(idx)

    moved: list[dict[str, int | str]] = []
    for uri in sorted(set(prev_positions).intersection(curr_positions)):
        retained = min(len(prev_positions[uri]), len(curr_positions[uri]))
        for _ in range(retained):
            old_pos = prev_positions[uri].popleft()
            new_pos = curr_positions[uri].popleft()
            if old_pos != new_pos:
                moved.append({"uri": uri, "from": old_pos, "to": new_pos})

    return {"added": added, "removed": removed, "moved": moved}

