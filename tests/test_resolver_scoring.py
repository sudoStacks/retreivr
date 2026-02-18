from spotify.resolve import score_search_candidates


def test_exact_match_favored() -> None:
    spotify_track = {"title": "Track One", "artist": "Artist A", "duration_ms": 200000}
    candidates = [
        {"title": "Track One", "artist": "Artist A", "duration": 200, "source": "youtube"},
        {"title": "Track One (live)", "artist": "Artist A", "duration": 200, "source": "youtube_music"},
    ]

    best = score_search_candidates(candidates, spotify_track)
    assert best["title"] == "Track One"
    assert best["artist"] == "Artist A"


def test_duration_mismatch_deprioritized() -> None:
    spotify_track = {"title": "Track Two", "artist": "Artist B", "duration_ms": 180000}
    candidates = [
        {"title": "Track Two", "artist": "Artist B", "duration": 181, "source": "youtube"},
        {"title": "Track Two", "artist": "Artist B", "duration": 220, "source": "youtube_music"},
    ]

    best = score_search_candidates(candidates, spotify_track)
    assert best["duration"] == 181


def test_tie_broken_in_source_order() -> None:
    spotify_track = {"title": "Track Three", "artist": "Artist C", "duration_ms": 210000}
    candidates = [
        {"title": "Track Three", "artist": "Artist C", "duration": 210, "source": "soundcloud"},
        {"title": "Track Three", "artist": "Artist C", "duration": 210, "source": "youtube_music"},
    ]

    best = score_search_candidates(candidates, spotify_track)
    assert best["source"] == "youtube_music"

