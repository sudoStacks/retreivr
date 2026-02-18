from __future__ import annotations

from metadata.canonical import CanonicalMetadataResolver


def test_canonical_resolver_prefers_musicbrainz_first(monkeypatch) -> None:
    calls = {"mb": 0, "sp": 0}

    class _MB:
        def __init__(self, *, min_confidence=0.70):
            pass

        def resolve_track(self, artist, track, *, album=None):
            calls["mb"] += 1
            return {"provider": "musicbrainz", "artist": artist, "track": track}

        def resolve_album(self, artist, album):
            return None

    class _SP:
        def __init__(self, **kwargs):
            pass

        def resolve_track(self, artist, track, album=None):
            calls["sp"] += 1
            return {"provider": "spotify", "artist": artist, "track": track}

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _MB)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _SP)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: True)

    resolver = CanonicalMetadataResolver(
        config={
            "spotify": {
                "client_id": "id",
                "client_secret": "secret",
                "oauth_access_token": "oauth-token",
            }
        }
    )
    out = resolver.resolve_track("Artist", "Track")

    assert out["provider"] == "musicbrainz"
    assert calls["mb"] == 1
    assert calls["sp"] == 0


def test_canonical_resolver_does_not_use_spotify_without_premium_validated_oauth(monkeypatch) -> None:
    calls = {"sp": 0}

    class _MB:
        def __init__(self, *, min_confidence=0.70):
            pass

        def resolve_track(self, artist, track, *, album=None):
            return None

        def resolve_album(self, artist, album):
            return None

    class _SP:
        def __init__(self, **kwargs):
            pass

        def resolve_track(self, artist, track, album=None):
            calls["sp"] += 1
            return {"provider": "spotify", "artist": artist, "track": track}

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _MB)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _SP)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: False)

    resolver = CanonicalMetadataResolver(
        config={
            "spotify": {
                "client_id": "id",
                "client_secret": "secret",
                "oauth_access_token": "oauth-token",
            }
        }
    )
    out = resolver.resolve_track("Artist", "Track")

    assert out is None
    assert calls["sp"] == 0


def test_canonical_resolver_uses_spotify_fallback_when_oauth_and_premium_valid(monkeypatch) -> None:
    calls = {"sp": 0}

    class _MB:
        def __init__(self, *, min_confidence=0.70):
            pass

        def resolve_track(self, artist, track, *, album=None):
            return None

        def resolve_album(self, artist, album):
            return None

    class _SP:
        def __init__(self, **kwargs):
            pass

        def resolve_track(self, artist, track, album=None):
            calls["sp"] += 1
            return {"provider": "spotify", "artist": artist, "track": track}

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _MB)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _SP)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: True)

    resolver = CanonicalMetadataResolver(
        config={
            "spotify": {
                "client_id": "id",
                "client_secret": "secret",
                "oauth_access_token": "oauth-token",
            }
        }
    )
    out = resolver.resolve_track("Artist", "Track")

    assert out["provider"] == "spotify"
    assert calls["sp"] == 1
