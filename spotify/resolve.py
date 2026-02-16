"""Spotify resolution stubs."""

from __future__ import annotations

import logging
from typing import Any

_LOG = logging.getLogger(__name__)

_SOURCE_PRIORITY = ["youtube_music", "youtube", "soundcloud", "bandcamp"]


def log_resolution(spotify_id: str, best_candidate: dict, score: float, reason: str) -> None:
    """Log a structured Spotify resolver decision.

    Example logging configuration:
    ```python
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    ```
    """
    media_url = (best_candidate or {}).get("media_url")
    _LOG.info(
        "resolver track_id=%s best_match=%s score=%s reason=%s",
        spotify_id,
        media_url,
        score,
        reason,
    )


def score_search_candidates(candidates: list[dict], spotify_track: dict) -> dict:
    """Return the best candidate using deterministic title/artist/duration scoring.

    Scoring behavior:
    - Title match: candidates whose `title` matches the Spotify track title are
      preferred. Match is case-insensitive and whitespace-normalized.
    - Artist match: candidates whose `artist` (or `artist_detected`) matches the
      Spotify track artist are preferred with the same normalization rules.
    - Duration proximity: candidates with duration closest to the Spotify track
      are preferred. Duration tolerance is +/- 3 seconds (higher preference),
      then increasing absolute difference.

    Tie-breaking strategy:
    - If multiple candidates have the same score tuple, source order is used.
      Lower index in `_SOURCE_PRIORITY` wins.
    - If source priority is also equal, original list order is preserved.

    Expected candidate fields:
    - `title`
    - `artist` or `artist_detected`
    - `duration` (seconds) or `duration_sec` or `duration_ms`
    - `source`

    The returned value is the selected candidate dictionary. If `candidates` is
    empty, an empty dictionary is returned.
    """
    if not candidates:
        return {}

    expected_title = _normalize_text(spotify_track.get("title") or spotify_track.get("name"))
    expected_artist = _normalize_text(spotify_track.get("artist"))
    expected_duration_sec = _to_seconds(spotify_track)

    scored: list[tuple[tuple[int, int, int, int], int, dict]] = []
    for idx, candidate in enumerate(candidates):
        candidate_title = _normalize_text(candidate.get("title"))
        candidate_artist = _normalize_text(candidate.get("artist") or candidate.get("artist_detected"))
        candidate_duration_sec = _to_seconds(candidate)

        title_exact = int(bool(expected_title and candidate_title == expected_title))
        artist_exact = int(bool(expected_artist and candidate_artist == expected_artist))

        if expected_duration_sec is None or candidate_duration_sec is None:
            duration_delta = 10**9
        else:
            duration_delta = abs(candidate_duration_sec - expected_duration_sec)
        within_tolerance = int(duration_delta <= 3)

        source_rank = _source_rank(candidate.get("source"))
        score_tuple = (title_exact, artist_exact, within_tolerance, -duration_delta)
        scored.append((score_tuple, source_rank, candidate))

    # Stable sort ensures original order for identical score + source rank.
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return scored[0][2]


async def execute_search(search_service, query: str) -> list[dict]:
    """Run an async search and return normalized result dictionaries.

    This helper calls `search_service.search(query)`, catches/logs search
    failures, and returns a normalized `list[dict]` where every item contains:
    `media_url`, `title`, `duration`, `source_id`, and `extra`.
    """
    try:
        raw_results = await search_service.search(query)
    except Exception:
        _LOG.exception("Search execution failed for query=%r", query)
        return []

    if not isinstance(raw_results, list):
        return []

    normalized: list[dict] = []
    for item in raw_results:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "media_url": item.get("media_url"),
                "title": item.get("title"),
                "duration": item.get("duration"),
                "source_id": item.get("source_id"),
                "extra": item.get("extra"),
            }
        )
    return normalized


async def resolve_spotify_track(spotify_track: dict, search_service) -> dict:
    """Resolve a Spotify track dictionary into the best available media candidate.

    This function builds a deterministic query from Spotify artist/title, runs
    async search execution, scores candidates, and returns the best candidate.
    If no candidates are returned, it returns an empty dictionary.
    """
    artist = str(spotify_track.get("artist") or "").strip()
    title = str(spotify_track.get("title") or spotify_track.get("name") or "").strip()
    query = f"{artist} - {title} official audio".strip()
    _LOG.info("Resolving Spotify track using query=%r", query)

    results = await execute_search(search_service, query)
    if not results:
        _LOG.info("No search results for query=%r", query)
        return {}

    # `score_search_candidates` expects `source`, while execute_search output
    # uses `source_id`; map for deterministic tie-breaking compatibility.
    scoring_results = [
        {**candidate, "source": candidate.get("source_id")} for candidate in results
    ]
    best = score_search_candidates(scoring_results, spotify_track)
    if not best:
        _LOG.info("No candidate selected for query=%r", query)
        return {}

    # Preserve the execute_search output key shape.
    best_out = {
        "media_url": best.get("media_url"),
        "title": best.get("title"),
        "duration": best.get("duration"),
        "source_id": best.get("source_id"),
        "extra": best.get("extra"),
    }
    _LOG.info(
        "Resolved Spotify track query=%r source_id=%r media_url=%r",
        query,
        best_out.get("source_id"),
        best_out.get("media_url"),
    )
    return best_out


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).casefold().strip().split())


def _to_seconds(data: dict) -> int | None:
    if "duration_ms" in data and data.get("duration_ms") is not None:
        try:
            return int(round(float(data["duration_ms"]) / 1000.0))
        except (TypeError, ValueError):
            return None
    for key in ("duration", "duration_sec"):
        if data.get(key) is None:
            continue
        try:
            return int(round(float(data[key])))
        except (TypeError, ValueError):
            return None
    return None


def _source_rank(source: Any) -> int:
    src = _normalize_text(source)
    try:
        return len(_SOURCE_PRIORITY) - _SOURCE_PRIORITY.index(src)
    except ValueError:
        return 0
