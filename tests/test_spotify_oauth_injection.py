from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")


class _FakeSpotifyClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _FakeStoreWithToken:
    def __init__(self, _db_path):
        self._token = SimpleNamespace(access_token="oauth-access-token")

    def load(self):
        return self._token

    def get_valid_token(self, _client_id, _client_secret, config=None):
        return self._token


class _FakeStoreNoToken:
    def __init__(self, _db_path):
        self._token = None

    def load(self):
        return self._token

    def get_valid_token(self, _client_id, _client_secret, config=None):
        return None


def _import_api_main(monkeypatch):
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return module


def test_build_spotify_client_uses_oauth_access_token_when_valid(monkeypatch, tmp_path) -> None:
    module = _import_api_main(monkeypatch)
    module.app.state.paths = SimpleNamespace(db_path=str(tmp_path / "oauth.sqlite"))

    monkeypatch.setattr(module, "SpotifyOAuthStore", _FakeStoreWithToken)
    monkeypatch.setattr(module, "SpotifyPlaylistClient", _FakeSpotifyClient)

    client = module._build_spotify_client_with_optional_oauth(
        {
            "spotify": {
                "client_id": "client-id",
                "client_secret": "client-secret",
            }
        }
    )

    assert isinstance(client, _FakeSpotifyClient)
    assert client.kwargs["client_id"] == "client-id"
    assert client.kwargs["client_secret"] == "client-secret"
    assert client.kwargs["access_token"] == "oauth-access-token"


def test_build_spotify_client_falls_back_to_public_mode_when_no_token(monkeypatch, tmp_path) -> None:
    module = _import_api_main(monkeypatch)
    module.app.state.paths = SimpleNamespace(db_path=str(tmp_path / "oauth.sqlite"))

    monkeypatch.setattr(module, "SpotifyOAuthStore", _FakeStoreNoToken)
    monkeypatch.setattr(module, "SpotifyPlaylistClient", _FakeSpotifyClient)

    client = module._build_spotify_client_with_optional_oauth(
        {
            "spotify": {
                "client_id": "client-id",
                "client_secret": "client-secret",
            }
        }
    )

    assert isinstance(client, _FakeSpotifyClient)
    assert client.kwargs["client_id"] == "client-id"
    assert client.kwargs["client_secret"] == "client-secret"
    assert "access_token" not in client.kwargs
