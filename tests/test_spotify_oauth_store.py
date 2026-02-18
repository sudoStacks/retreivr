from __future__ import annotations

from spotify.oauth_store import SpotifyOAuthStore, SpotifyOAuthToken


def test_spotify_oauth_store_lifecycle(tmp_path) -> None:
    db_path = tmp_path / "spotify_oauth.sqlite"
    store = SpotifyOAuthStore(db_path)

    first = SpotifyOAuthToken(
        access_token="access-1",
        refresh_token="refresh-1",
        expires_at=1_800_000_000,
        scope="user-library-read",
    )
    store.save(first)

    loaded_first = store.load()
    assert loaded_first is not None
    assert loaded_first.access_token == first.access_token
    assert loaded_first.refresh_token == first.refresh_token
    assert loaded_first.expires_at == first.expires_at
    assert loaded_first.scope == first.scope

    second = SpotifyOAuthToken(
        access_token="access-2",
        refresh_token="refresh-2",
        expires_at=1_900_000_000,
        scope="user-library-read playlist-read-private",
    )
    store.save(second)

    loaded_second = store.load()
    assert loaded_second is not None
    assert loaded_second.access_token == second.access_token
    assert loaded_second.refresh_token == second.refresh_token
    assert loaded_second.expires_at == second.expires_at
    assert loaded_second.scope == second.scope

    store.clear()
    assert store.load() is None
