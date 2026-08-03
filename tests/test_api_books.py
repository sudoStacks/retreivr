from __future__ import annotations

import importlib
import sys

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _client(monkeypatch, tmp_path, *, enabled=True):
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    config = {
        "books": {
            "enabled": enabled,
            "library_path": str(tmp_path / "books"),
            "allow_direct_urls": True,
            "allow_private_source_urls": False,
            "max_download_mb": 25,
        }
    }
    monkeypatch.setattr(module, "_current_loaded_config", lambda: config)
    return module, TestClient(module.app)


def test_books_api_is_feature_gated(monkeypatch, tmp_path):
    _module, client = _client(monkeypatch, tmp_path, enabled=False)
    response = client.get("/api/books/library")
    assert response.status_code == 404
    assert "disabled" in response.json()["detail"]["error"]


def test_books_search_route_returns_native_contract(monkeypatch, tmp_path):
    module, client = _client(monkeypatch, tmp_path)
    monkeypatch.setattr(
        module,
        "search_open_library",
        lambda query, *, limit, page: {
            "provider": "openlibrary",
            "query": query,
            "page": page,
            "total": 1,
            "results": [{"id": "OL1W", "title": "Native Books"}],
        },
    )

    response = client.get("/api/books/search?q=native&limit=12")
    assert response.status_code == 200
    assert response.json()["results"][0]["title"] == "Native Books"


def test_books_import_and_library_round_trip(monkeypatch, tmp_path):
    _module, client = _client(monkeypatch, tmp_path)
    imported = client.post(
        "/api/books/import",
        files={"file": ("example.txt", b"hello book", "text/plain")},
        data={"metadata_json": '{"title":"Example","authors":["Writer"]}'},
    )
    assert imported.status_code == 201

    library = client.get("/api/books/library")
    assert library.status_code == 200
    assert library.json()["count"] == 1
    assert library.json()["results"][0]["title"] == "Example"


def test_books_one_click_openlibrary_acquisition(monkeypatch, tmp_path):
    module, client = _client(monkeypatch, tmp_path)
    captured = {}

    def fake_acquire(config, identifiers, metadata, *, preferred_format):
        captured.update(
            identifiers=identifiers,
            metadata=metadata,
            preferred_format=preferred_format,
        )
        return {
            "path": str(tmp_path / "books" / "Writer" / "Example.epub"),
            "metadata": {"title": "Example", "file_format": "epub"},
        }

    monkeypatch.setattr(module, "acquire_open_library_book", fake_acquire)
    response = client.post(
        "/api/books/acquire/openlibrary",
        json={
            "archive_identifiers": ["example00writ"],
            "preferred_format": "epub",
            "metadata": {"title": "Example", "authors": ["Writer"]},
        },
    )

    assert response.status_code == 201
    assert response.json()["status"] == "completed"
    assert captured["identifiers"] == ["example00writ"]
    assert captured["preferred_format"] == "epub"
