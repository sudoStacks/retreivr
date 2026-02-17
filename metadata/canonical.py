import os
import logging

import requests

from engine.paths import DATA_DIR
from metadata.canonical_cache import JsonCache
from metadata.providers.musicbrainz import MusicBrainzMetadataProvider
from metadata.providers.spotify import SpotifyMetadataProvider


def _cache_dir(config):
    env = os.environ.get("RETREIVR_METADATA_CACHE_DIR")
    if env:
        return os.path.abspath(env)
    value = None
    if isinstance(config, dict):
        value = (config.get("canonical_metadata") or {}).get("cache_dir")
    if value:
        return os.path.abspath(value)
    return os.path.join(DATA_DIR, "metadata_cache")


def _spotify_credentials(config):
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    if client_id and client_secret:
        return client_id, client_secret
    if not isinstance(config, dict):
        return None, None
    spotify_cfg = config.get("spotify") or {}
    canonical_cfg = config.get("canonical_metadata") or {}
    client_id = spotify_cfg.get("client_id") or canonical_cfg.get("spotify_client_id")
    client_secret = spotify_cfg.get("client_secret") or canonical_cfg.get("spotify_client_secret")
    return client_id, client_secret


def _spotify_oauth_token(config):
    token = os.environ.get("SPOTIFY_OAUTH_ACCESS_TOKEN")
    if token:
        return str(token).strip() or None
    if not isinstance(config, dict):
        return None
    spotify_cfg = config.get("spotify") or {}
    canonical_cfg = config.get("canonical_metadata") or {}
    token = spotify_cfg.get("oauth_access_token") or canonical_cfg.get("spotify_oauth_access_token")
    if token:
        return str(token).strip() or None
    return None


def _validate_spotify_premium(access_token):
    token = str(access_token or "").strip()
    if not token:
        return False
    try:
        response = requests.get(
            "https://api.spotify.com/v1/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
    except Exception:
        logging.exception("Spotify premium validation failed")
        return False
    if response.status_code != 200:
        return False
    payload = response.json() if response.content else {}
    return str(payload.get("product") or "").strip().lower() == "premium"


def _min_confidence(config, default):
    if not isinstance(config, dict):
        return default
    canonical_cfg = config.get("canonical_metadata") or {}
    value = canonical_cfg.get("min_confidence")
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _cache_ttl(config, default):
    if not isinstance(config, dict):
        return default
    canonical_cfg = config.get("canonical_metadata") or {}
    value = canonical_cfg.get("cache_ttl_seconds")
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


class CanonicalMetadataResolver:
    def __init__(self, *, config=None):
        config = config or {}
        cache_dir = _cache_dir(config)
        ttl_seconds = _cache_ttl(config, 86400)
        spotify_cache = JsonCache(os.path.join(cache_dir, "spotify.json"), ttl_seconds=ttl_seconds)

        spotify_id, spotify_secret = _spotify_credentials(config)
        spotify_oauth_token = _spotify_oauth_token(config)
        spotify_min = _min_confidence(config, 0.92)
        mb_min = _min_confidence(config, 0.70)

        self.musicbrainz = MusicBrainzMetadataProvider(min_confidence=mb_min)
        self.spotify_enabled = bool(
            spotify_id
            and spotify_secret
            and spotify_oauth_token
            and _validate_spotify_premium(spotify_oauth_token)
        )

        self.spotify = SpotifyMetadataProvider(
            client_id=spotify_id,
            client_secret=spotify_secret,
            access_token=spotify_oauth_token,
            cache=spotify_cache,
            min_confidence=spotify_min,
        )

    def resolve_track(self, artist, track, *, album=None):
        mb = self.musicbrainz.resolve_track(artist, track, album=album)
        if mb:
            return mb
        if self.spotify_enabled:
            return self.spotify.resolve_track(artist, track, album=album)
        return None

    def resolve_album(self, artist, album):
        mb = self.musicbrainz.resolve_album(artist, album)
        if mb:
            return mb
        if self.spotify_enabled:
            return self.spotify.resolve_album(artist, album)
        return None
