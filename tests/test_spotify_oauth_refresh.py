from __future__ import annotations

import time

from spotify.oauth_store import SpotifyOAuthStore, SpotifyOAuthToken


def test_get_valid_token_returns_original_when_not_expired(tmp_path) -> None:
    store = SpotifyOAuthStore(tmp_path / "oauth_refresh.sqlite")
    token = SpotifyOAuthToken(
        access_token="access-current",
        refresh_token="refresh-current",
        expires_at=int(time.time()) + 3600,
        scope="user-library-read",
    )
    store.save(token)

    result = store.get_valid_token(client_id="cid", client_secret="secret")

    assert result is not None
    assert result.access_token == "access-current"
    assert result.refresh_token == "refresh-current"
    assert result.scope == "user-library-read"


def test_get_valid_token_refreshes_and_updates_db_when_expired(tmp_path, monkeypatch) -> None:
    store = SpotifyOAuthStore(tmp_path / "oauth_refresh.sqlite")
    old = SpotifyOAuthToken(
        access_token="old-access",
        refresh_token="old-refresh",
        expires_at=int(time.time()) - 10,
        scope="user-library-read",
    )
    store.save(old)

    monkeypatch.setattr(
        "spotify.oauth_store.refresh_access_token",
        lambda client_id, client_secret, refresh_token: {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 7200,
            "scope": "user-library-read playlist-read-private",
        },
    )

    result = store.get_valid_token(client_id="cid", client_secret="secret")

    assert result is not None
    assert result.access_token == "new-access"
    assert result.refresh_token == "new-refresh"
    assert result.scope == "user-library-read playlist-read-private"
    assert result.expires_at > int(time.time())

    persisted = store.load()
    assert persisted is not None
    assert persisted.access_token == "new-access"
    assert persisted.refresh_token == "new-refresh"
    assert persisted.scope == "user-library-read playlist-read-private"


def test_get_valid_token_clears_token_when_refresh_fails(tmp_path, monkeypatch) -> None:
    store = SpotifyOAuthStore(tmp_path / "oauth_refresh.sqlite")
    token = SpotifyOAuthToken(
        access_token="expired-access",
        refresh_token="expired-refresh",
        expires_at=int(time.time()) - 10,
        scope="user-library-read",
    )
    store.save(token)

    def _raise_refresh_error(client_id, client_secret, refresh_token):
        raise RuntimeError("refresh failed")

    monkeypatch.setattr("spotify.oauth_store.refresh_access_token", _raise_refresh_error)

    result = store.get_valid_token(client_id="cid", client_secret="secret")

    assert result is None
    assert store.load() is None
