from __future__ import annotations

import importlib
import sys

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return TestClient(module.app)


def test_intent_execute_accepts_valid_spotify_album_intent(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/intent/execute",
        json={
            "intent_type": "spotify_album",
            "identifier": "1A2B3C4D5E",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["intent_type"] == "spotify_album"
    assert payload["identifier"] == "1A2B3C4D5E"


def test_intent_execute_rejects_invalid_intent_type(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/intent/execute",
        json={
            "intent_type": "not_real_intent",
            "identifier": "abc",
        },
    )

    assert response.status_code == 400
