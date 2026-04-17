from __future__ import annotations

import importlib
import sys

import pytest

pytest.importorskip("musicbrainzngs")


def test_saved_title_store_round_trip(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    store = module.SavedTitleStore(str(tmp_path / "saved_titles.sqlite3"))

    saved = store.save_title(
        "movie",
        {
            "tmdb_id": 101,
            "title": "Example Movie",
            "year": "2026",
            "poster_url": "https://example.test/poster.jpg",
            "overview": "Overview",
            "tmdb_url": "https://www.themoviedb.org/movie/101",
            "language": "en",
            "popularity": 42.5,
            "rating": 7.3,
        },
    )

    assert saved["saved"] is True
    assert saved["tmdb_id"] == 101

    rows = store.list_saved_titles("movie", limit=10)
    assert len(rows) == 1
    assert rows[0]["title"] == "Example Movie"

    status_map = store.get_saved_status_map("movie", [101, 202])
    assert status_map == {"101": True}

    removed = store.remove_title("movie", 101)
    assert removed is True
    assert store.list_saved_titles("movie", limit=10) == []
