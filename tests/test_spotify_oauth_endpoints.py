from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from spotify.oauth_store import SpotifyOAuthStore


def _build_client(monkeypatch, tmp_path) -> tuple[TestClient, object]:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()

    db_path = tmp_path / "oauth_endpoints.sqlite"
    module.app.state.paths = SimpleNamespace(db_path=str(db_path))
    module.app.state.spotify_oauth_state = None
    monkeypatch.setattr(
        module,
        "_read_config_or_404",
        lambda: {
            "spotify": {
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "redirect_uri": "http://localhost/callback",
            }
        },
    )
    return TestClient(module.app), module


def test_oauth_connect_returns_auth_url_with_client_id(monkeypatch, tmp_path) -> None:
    client, _module = _build_client(monkeypatch, tmp_path)

    response = client.get("/api/spotify/oauth/connect")

    assert response.status_code == 200
    payload = response.json()
    auth_url = payload["auth_url"]
    assert "accounts.spotify.com/authorize" in auth_url
    assert "client_id=test-client-id" in auth_url


def test_oauth_callback_stores_token_and_returns_connected(monkeypatch, tmp_path) -> None:
    client, module = _build_client(monkeypatch, tmp_path)
    module.app.state.spotify_oauth_state = "state-123"

    class _FakeResponse:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "expires_in": 3600,
                "scope": "user-library-read",
            }

    monkeypatch.setattr("api.main.requests.post", lambda *args, **kwargs: _FakeResponse())

    response = client.get("/api/spotify/oauth/callback?code=abc&state=state-123")

    assert response.status_code == 200
    assert response.json() == {"status": "connected"}

    store = SpotifyOAuthStore(tmp_path / "oauth_endpoints.sqlite")
    token = store.load()
    assert token is not None
    assert token.access_token == "access-token"
    assert token.refresh_token == "refresh-token"
    assert token.scope == "user-library-read"
    assert token.expires_at > 0


def test_oauth_callback_invalid_state_returns_400(monkeypatch, tmp_path) -> None:
    client, module = _build_client(monkeypatch, tmp_path)
    module.app.state.spotify_oauth_state = "expected-state"

    response = client.get("/api/spotify/oauth/callback?code=abc&state=wrong-state")

    assert response.status_code == 400
