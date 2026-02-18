"""Diff helpers for Spotify playlist snapshots."""

from __future__ import annotations

from collections import defaultdict, deque

def diff_playlist(prev: list[dict], curr: list[dict]) -> dict[str, list[dict]]:
    """Return duplicate-aware `added`, `removed`, and `moved` playlist items."""
    prev_occurrences: dict[str | None, deque[int]] = defaultdict(deque)
    for idx, item in enumerate(prev):
        prev_occurrences[item.get("spotify_track_id")].append(idx)

    matched_curr_to_prev_index: dict[int, int] = {}
    added: list[dict] = []
    for curr_idx, curr_item in enumerate(curr):
        item_id = curr_item.get("spotify_track_id")
        remaining = prev_occurrences.get(item_id)
        if remaining:
            matched_curr_to_prev_index[curr_idx] = remaining.popleft()
        else:
            added.append(curr_item)

    matched_prev_indices = set(matched_curr_to_prev_index.values())
    removed: list[dict] = [
        prev[prev_idx] for prev_idx in range(len(prev)) if prev_idx not in matched_prev_indices
    ]

    moved: list[dict] = []
    for curr_idx, curr_item in enumerate(curr):
        prev_idx = matched_curr_to_prev_index.get(curr_idx)
        if prev_idx is None:
            continue
        prev_item = prev[prev_idx]
        prev_pos = int(prev_item.get("position", prev_idx))
        curr_pos = int(curr_item.get("position", curr_idx))
        if prev_pos != curr_pos:
            moved.append(
                {
                    "spotify_track_id": curr_item.get("spotify_track_id"),
                    "from_position": prev_pos,
                    "to_position": curr_pos,
                    "item": curr_item,
                }
            )

    return {"added": added, "removed": removed, "moved": moved}
