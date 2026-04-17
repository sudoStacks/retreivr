from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch, tmp_path) -> tuple[object, TestClient]:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.paths = SimpleNamespace(db_path=str(tmp_path / "retreivr.sqlite3"))
    return module, TestClient(module.app)


def test_arr_search_movies_marks_saved_titles(monkeypatch, tmp_path) -> None:
    module, client = _build_client(monkeypatch, tmp_path)
    store = module.SavedTitleStore(module.app.state.paths.db_path)
    store.save_title("movie", {"tmdb_id": 55, "title": "Saved Movie"})

    monkeypatch.setattr(
        module,
        "build_movie_search_response",
        lambda _config, _q, *, limit=20, year=None: {
            "results": [
                {
                    "tmdb_id": 55,
                    "title": "Saved Movie",
                    "year": "2025",
                    "overview": "Example",
                    "tmdb_url": "https://www.themoviedb.org/movie/55",
                },
                {
                    "tmdb_id": 77,
                    "title": "Unsaved Movie",
                    "year": "2024",
                    "overview": "Example",
                    "tmdb_url": "https://www.themoviedb.org/movie/77",
                },
            ],
            "connection": {"configured": False, "reachable": False, "message": "Not configured"},
        },
    )

    response = client.get("/api/arr/search/movies?q=test")

    assert response.status_code == 200
    payload = response.json()
    assert payload["results"][0]["saved"] is True
    assert payload["results"][1]["saved"] is False


def test_arr_saved_toggle_and_saved_shelf(monkeypatch, tmp_path) -> None:
    module, client = _build_client(monkeypatch, tmp_path)

    monkeypatch.setattr(
        module,
        "test_radarr_connection",
        lambda _config: {"configured": False, "reachable": False, "message": "Not configured"},
    )
    monkeypatch.setattr(module, "get_bulk_status", lambda _config, _kind, _ids: {})

    save_response = client.post(
        "/api/arr/saved/toggle",
        json={
            "kind": "movie",
            "saved": True,
            "item": {
                "tmdb_id": 9001,
                "title": "Save Me",
                "year": "2026",
                "overview": "Later",
                "tmdb_url": "https://www.themoviedb.org/movie/9001",
            },
        },
    )

    assert save_response.status_code == 200
    assert save_response.json()["saved"] is True

    shelf_response = client.get("/api/arr/editorial/shelf?kind=movie&shelf=saved_for_later&limit=10")

    assert shelf_response.status_code == 200
    shelf_payload = shelf_response.json()
    assert shelf_payload["shelf"] == "saved_for_later"
    assert shelf_payload["results"][0]["title"] == "Save Me"
    assert shelf_payload["results"][0]["saved"] is True
