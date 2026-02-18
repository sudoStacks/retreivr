import asyncio

from spotify.resolve import resolve_spotify_track, score_search_candidates


def test_score_search_candidates_exact_match() -> None:
    spotify_track = {"artist": "Artist A", "title": "Track A", "duration_ms": 200000}
    candidates = [
        {"title": "Track A", "artist": "Artist A", "duration": 200, "source": "youtube"},
        {"title": "Track A live", "artist": "Artist A", "duration": 200, "source": "youtube_music"},
    ]

    best = score_search_candidates(candidates, spotify_track)

    assert best["title"] == "Track A"
    assert best["artist"] == "Artist A"


def test_score_search_candidates_duration_mismatch() -> None:
    spotify_track = {"artist": "Artist B", "title": "Track B", "duration_ms": 180000}
    candidates = [
        {"title": "Track B", "artist": "Artist B", "duration": 181, "source": "youtube"},
        {"title": "Track B", "artist": "Artist B", "duration": 240, "source": "youtube_music"},
    ]

    best = score_search_candidates(candidates, spotify_track)

    assert best["duration"] == 181


def test_score_search_candidates_tie_behavior_source_priority() -> None:
    spotify_track = {"artist": "Artist C", "title": "Track C", "duration_ms": 210000}
    candidates = [
        {"title": "Track C", "artist": "Artist C", "duration": 210, "source": "soundcloud"},
        {"title": "Track C", "artist": "Artist C", "duration": 210, "source": "youtube_music"},
    ]

    best = score_search_candidates(candidates, spotify_track)

    assert best["source"] == "youtube_music"


class _MockSearchService:
    def __init__(self, results):
        self._results = results
        self.calls = []

    async def search(self, query: str):
        self.calls.append(query)
        return self._results


def test_resolve_spotify_track_no_results() -> None:
    search_service = _MockSearchService([])
    spotify_track = {"artist": "Artist D", "title": "Track D", "duration_ms": 180000}

    resolved = asyncio.run(resolve_spotify_track(spotify_track, search_service))

    assert resolved == {}
    assert search_service.calls == ["Artist D - Track D official audio"]


def test_resolve_spotify_track_single_result() -> None:
    results = [
        {
            "media_url": "https://example.com/one",
            "title": "Track E",
            "duration": 200,
            "source_id": "youtube",
            "extra": {"id": "1"},
        }
    ]
    search_service = _MockSearchService(results)
    spotify_track = {"artist": "Artist E", "title": "Track E", "duration_ms": 200000}

    resolved = asyncio.run(resolve_spotify_track(spotify_track, search_service))

    assert resolved["media_url"] == "https://example.com/one"
    assert resolved["title"] == "Track E"
    assert resolved["source_id"] == "youtube"


def test_resolve_spotify_track_multiple_results_best_match_chosen() -> None:
    results = [
        {
            "media_url": "https://example.com/bad",
            "title": "Track F (live)",
            "duration": 260,
            "source_id": "youtube_music",
            "extra": {"id": "bad"},
            "artist": "Artist F",
        },
        {
            "media_url": "https://example.com/best",
            "title": "Track F",
            "duration": 210,
            "source_id": "youtube",
            "extra": {"id": "best"},
            "artist": "Artist F",
        },
    ]
    search_service = _MockSearchService(results)
    spotify_track = {"artist": "Artist F", "title": "Track F", "duration_ms": 210000}

    resolved = asyncio.run(resolve_spotify_track(spotify_track, search_service))

    assert resolved["media_url"] == "https://example.com/best"
    assert resolved["title"] == "Track F"
    assert resolved["source_id"] == "youtube"
