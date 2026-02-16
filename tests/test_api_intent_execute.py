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


def test_api_intent_execute_delegates_to_dispatcher(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    expected = {
        "status": "accepted",
        "intent_type": "spotify_album",
        "identifier": "album-123",
        "message": "album sync completed",
        "enqueued_count": 7,
    }

    async def _fake_dispatch_intent(*, intent_type, identifier, config, db, queue, spotify_client):
        assert intent_type == "spotify_album"
        assert identifier == "album-123"
        assert config is not None
        assert db is not None
        assert queue is not None
        assert spotify_client is not None
        return expected

    monkeypatch.setattr("api.main.dispatch_intent", _fake_dispatch_intent)

    response = client.post(
        "/api/intent/execute",
        json={
            "intent_type": "spotify_album",
            "identifier": "album-123",
        },
    )

    assert response.status_code == 200
    assert response.json() == expected


def test_api_intent_execute_invalid_intent_type_returns_400(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    response = client.post(
        "/api/intent/execute",
        json={
            "intent_type": "invalid_intent",
            "identifier": "abc",
        },
    )

    assert response.status_code == 400
