from __future__ import annotations

import pytest

pytest.importorskip("google.auth")

from metadata.canonical import CanonicalMetadataResolver


def test_musicbrainz_only_resolution_without_spotify(monkeypatch) -> None:
    calls = {"mb": 0, "sp": 0}

    class _MockMusicBrainzService:
        def __init__(self, *, min_confidence=0.70):
            self.min_confidence = min_confidence

        def resolve_track(self, artist, track, *, album=None):
            calls["mb"] += 1
            return {
                "provider": "musicbrainz",
                "artist": artist,
                "title": track,
                "album": album,
                "musicbrainz_recording_id": "mb-rec-1",
            }

        def resolve_album(self, artist, album):
            return None

    class _MockSpotifyProvider:
        def __init__(self, **kwargs):
            pass

        def resolve_track(self, artist, track, album=None):
            calls["sp"] += 1
            return {"provider": "spotify", "artist": artist, "title": track}

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _MockMusicBrainzService)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _MockSpotifyProvider)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: True)

    resolver = CanonicalMetadataResolver(config={})
    resolved = resolver.resolve_track("Artist Name", "Song Name", album="Album Name")

    assert resolved is not None
    assert resolved["provider"] == "musicbrainz"
    assert resolved["musicbrainz_recording_id"] == "mb-rec-1"
    assert calls["mb"] == 1
    assert calls["sp"] == 0


def test_spotify_fallback_only_when_oauth_and_premium_present(monkeypatch) -> None:
    calls = {"mb": 0, "sp": 0}

    class _MockMusicBrainzService:
        def __init__(self, *, min_confidence=0.70):
            self.min_confidence = min_confidence

        def resolve_track(self, artist, track, *, album=None):
            calls["mb"] += 1
            return None

        def resolve_album(self, artist, album):
            return None

    class _MockSpotifyProvider:
        def __init__(self, **kwargs):
            pass

        def resolve_track(self, artist, track, album=None):
            calls["sp"] += 1
            return {
                "provider": "spotify",
                "artist": artist,
                "title": track,
                "album": album,
                "spotify_id": "sp-track-1",
            }

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _MockMusicBrainzService)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _MockSpotifyProvider)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: True)

    resolver = CanonicalMetadataResolver(
        config={
            "spotify": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "oauth_access_token": "oauth-token",
            }
        }
    )
    resolved = resolver.resolve_track("Artist Name", "Song Name", album="Album Name")

    assert resolved is not None
    assert resolved["provider"] == "spotify"
    assert resolved["spotify_id"] == "sp-track-1"
    assert calls["mb"] == 1
    assert calls["sp"] == 1


def test_spotify_fallback_rejected_when_oauth_missing(monkeypatch) -> None:
    calls = {"mb": 0, "sp": 0}

    class _MockMusicBrainzService:
        def __init__(self, *, min_confidence=0.70):
            self.min_confidence = min_confidence

        def resolve_track(self, artist, track, *, album=None):
            calls["mb"] += 1
            return None

        def resolve_album(self, artist, album):
            return None

    class _MockSpotifyProvider:
        def __init__(self, **kwargs):
            pass

        def resolve_track(self, artist, track, album=None):
            calls["sp"] += 1
            return {"provider": "spotify", "artist": artist, "title": track}

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _MockMusicBrainzService)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _MockSpotifyProvider)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: True)

    resolver = CanonicalMetadataResolver(
        config={
            "spotify": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "oauth_access_token": "",
            }
        }
    )
    resolved = resolver.resolve_track("Artist Name", "Song Name", album="Album Name")

    assert resolved is None
    assert calls["mb"] == 1
    assert calls["sp"] == 0


def test_spotify_fallback_rejected_when_non_premium(monkeypatch) -> None:
    calls = {"mb": 0, "sp": 0}

    class _MockMusicBrainzService:
        def __init__(self, *, min_confidence=0.70):
            self.min_confidence = min_confidence

        def resolve_track(self, artist, track, *, album=None):
            calls["mb"] += 1
            return None

        def resolve_album(self, artist, album):
            return None

    class _MockSpotifyProvider:
        def __init__(self, **kwargs):
            pass

        def resolve_track(self, artist, track, album=None):
            calls["sp"] += 1
            return {"provider": "spotify", "artist": artist, "title": track}

        def resolve_album(self, artist, album):
            return None

    monkeypatch.setattr("metadata.canonical.MusicBrainzMetadataProvider", _MockMusicBrainzService)
    monkeypatch.setattr("metadata.canonical.SpotifyMetadataProvider", _MockSpotifyProvider)
    monkeypatch.setattr("metadata.canonical._validate_spotify_premium", lambda _token: False)

    resolver = CanonicalMetadataResolver(
        config={
            "spotify": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "oauth_access_token": "oauth-token",
            }
        }
    )
    resolved = resolver.resolve_track("Artist Name", "Song Name", album="Album Name")

    assert resolved is None
    assert calls["mb"] == 1
    assert calls["sp"] == 0
