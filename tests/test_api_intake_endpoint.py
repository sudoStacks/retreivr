from __future__ import annotations

import importlib
import sys
import types

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    if "google_auth_oauthlib" not in sys.modules:
        google_auth_oauthlib = types.ModuleType("google_auth_oauthlib")
        sys.modules["google_auth_oauthlib"] = google_auth_oauthlib
    if "google_auth_oauthlib.flow" not in sys.modules:
        flow_mod = types.ModuleType("google_auth_oauthlib.flow")
        flow_mod.InstalledAppFlow = object
        sys.modules["google_auth_oauthlib.flow"] = flow_mod
    if "googleapiclient" not in sys.modules:
        googleapiclient = types.ModuleType("googleapiclient")
        sys.modules["googleapiclient"] = googleapiclient
    if "googleapiclient.errors" not in sys.modules:
        errors_mod = types.ModuleType("googleapiclient.errors")
        errors_mod.HttpError = Exception
        sys.modules["googleapiclient.errors"] = errors_mod
    if "google" not in sys.modules:
        google_mod = types.ModuleType("google")
        sys.modules["google"] = google_mod
    if "google.auth" not in sys.modules:
        google_auth_mod = types.ModuleType("google.auth")
        sys.modules["google.auth"] = google_auth_mod
    if "google.auth.exceptions" not in sys.modules:
        google_auth_exc_mod = types.ModuleType("google.auth.exceptions")
        google_auth_exc_mod.RefreshError = Exception
        sys.modules["google.auth.exceptions"] = google_auth_exc_mod
    if "musicbrainzngs" not in sys.modules:
        sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")

    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return TestClient(module.app)


def test_api_intake_accepts_audiobook_package(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    captured: dict = {}

    def _fake_enqueue(self, payload: dict) -> dict[str, object]:
        captured["payload"] = dict(payload)
        return {"job_id": "job-audio-1", "created": True, "dedupe_reason": None}

    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", _fake_enqueue)

    response = client.post(
        "/api/intake",
        json={
            "source_url": "https://media.example.test/book-01.mp3",
            "media_class": "audiobook",
            "metadata": {
                "title": "Chapter 1",
                "author": "Ursula Example",
                "series": "Collected Stories",
                "duration_ms": 123000,
            },
            "delivery": {
                "destination": "Books/Audiobooks",
                "final_format": "mp3",
            },
            "provenance": {
                "origin": "jellyfin_plugin",
                "origin_id": "item-123",
                "external_id": "jf-item-123",
            },
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["effective_media_type"] == "music"
    assert body["effective_media_intent"] == "audiobook"
    assert captured["payload"]["media_type"] == "music"
    assert captured["payload"]["media_intent"] == "audiobook"
    assert captured["payload"]["destination"] == "Books/Audiobooks"
    assert captured["payload"]["origin"] == "jellyfin_plugin"
    assert captured["payload"]["origin_id"] == "item-123"
    assert captured["payload"]["music_metadata"]["artist"] == "Ursula Example"
    assert captured["payload"]["music_metadata"]["album"] == "Collected Stories"
    assert captured["payload"]["resolved_media"]["duration_ms"] == 123000


def test_api_intake_accepts_book_package_as_generic_download(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    captured: dict = {}

    def _fake_enqueue(self, payload: dict) -> dict[str, object]:
        captured["payload"] = dict(payload)
        return {"job_id": "job-book-1", "created": False, "dedupe_reason": "duplicate"}

    monkeypatch.setattr("api.main._IntentQueueAdapter.enqueue", _fake_enqueue)

    response = client.post(
        "/api/intake",
        json={
            "source_url": "https://files.example.test/library/example.pdf",
            "media_class": "book",
            "metadata": {
                "title": "Distributed Systems Notes",
                "authors": ["A. Writer"],
            },
            "delivery": {
                "destination": "Books/PDFs",
            },
            "provenance": {
                "origin": "browser_extension",
            },
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["created"] is False
    assert body["dedupe_reason"] == "duplicate"
    assert body["effective_media_type"] == "video"
    assert body["effective_media_intent"] == "book"
    assert captured["payload"]["media_type"] == "video"
    assert captured["payload"]["media_intent"] == "book"
    assert captured["payload"]["kind"] == "book"
    assert captured["payload"]["destination"] == "Books/PDFs"
    assert captured["payload"]["origin"] == "browser_extension"


def test_api_intake_requires_source_url(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/intake",
        json={
            "media_class": "book",
            "metadata": {"title": "Missing URL"},
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "source_url is required"
