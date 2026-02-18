import base64
import logging
import time
import urllib.parse

import requests

from engine.search_scoring import token_overlap_score, tokenize
from metadata.providers.base import CanonicalMetadataProvider


_SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
_SPOTIFY_SEARCH_URL = "https://api.spotify.com/v1/search"
_SPOTIFY_ALBUM_URL = "https://api.spotify.com/v1/albums/{album_id}"


def _release_year(value):
    if not value:
        return None
    return str(value).split("-")[0]


def _score_track_match(artist, track, album, candidate):
    cand_artists = [entry.get("name") for entry in candidate.get("artists", []) if entry.get("name")]
    cand_artist = " ".join(cand_artists)
    artist_score = token_overlap_score(tokenize(artist), tokenize(cand_artist))
    track_score = token_overlap_score(tokenize(track), tokenize(candidate.get("name")))
    if album:
        album_score = token_overlap_score(tokenize(album), tokenize(candidate.get("album", {}).get("name")))
        score = 0.55 * track_score + 0.35 * artist_score + 0.10 * album_score
    else:
        score = 0.60 * track_score + 0.40 * artist_score
    return score


def _score_album_match(artist, album, candidate):
    cand_artists = [entry.get("name") for entry in candidate.get("artists", []) if entry.get("name")]
    cand_artist = " ".join(cand_artists)
    artist_score = token_overlap_score(tokenize(artist), tokenize(cand_artist))
    album_score = token_overlap_score(tokenize(album), tokenize(candidate.get("name")))
    return 0.6 * album_score + 0.4 * artist_score


class SpotifyMetadataProvider(CanonicalMetadataProvider):
    def __init__(self, *, client_id, client_secret, access_token=None, cache=None, min_confidence=0.92):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = (access_token or "").strip() or None
        self.cache = cache
        self.min_confidence = float(min_confidence or 0.92)
        self._token = None
        self._token_expires_at = 0

    def _has_credentials(self):
        return bool(self.access_token or (self.client_id and self.client_secret))

    def _get_token(self):
        if self.access_token:
            return self.access_token
        if not self._has_credentials():
            return None
        now = time.time()
        if self._token and now < self._token_expires_at:
            return self._token
        auth = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode("utf-8")).decode("ascii")
        headers = {"Authorization": f"Basic {auth}"}
        data = {"grant_type": "client_credentials"}
        try:
            response = requests.post(_SPOTIFY_TOKEN_URL, data=data, headers=headers, timeout=15)
        except Exception:
            logging.exception("Spotify token request failed")
            return None
        if response.status_code != 200:
            logging.warning("Spotify token request failed: %s", response.text)
            return None
        payload = response.json()
        token = payload.get("access_token")
        expires_in = payload.get("expires_in") or 0
        if not token:
            return None
        self._token = token
        self._token_expires_at = now + max(0, int(expires_in) - 30)
        return token

    def _request(self, url, params=None):
        token = self._get_token()
        if not token:
            return None
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.get(url, params=params, headers=headers, timeout=20)
        except Exception:
            logging.exception("Spotify request failed")
            return None
        if response.status_code == 401:
            self._token = None
            token = self._get_token()
            if not token:
                return None
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, params=params, headers=headers, timeout=20)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    time.sleep(float(retry_after))
                except Exception:
                    pass
            return None
        if response.status_code != 200:
            logging.debug("Spotify request failed: %s", response.text)
            return None
        return response.json()

    def _search(self, query, search_type, limit):
        params = {
            "q": query,
            "type": search_type,
            "limit": limit,
        }
        return self._request(_SPOTIFY_SEARCH_URL, params=params)

    def resolve_track(self, artist, track, album=None):
        if not self._has_credentials() or not artist or not track:
            return None
        cache_key = None
        if self.cache:
            cache_key = f"spotify:track:{artist}|{track}|{album or ''}"
            cached = self.cache.get(cache_key)
            if cached:
                return cached
        query_parts = [f"track:{track}", f"artist:{artist}"]
        if album:
            query_parts.append(f"album:{album}")
        query = " ".join(query_parts)
        payload = self._search(query, "track", 8)
        items = (payload or {}).get("tracks", {}).get("items", [])
        best_item = None
        best_score = 0.0
        for item in items:
            score = _score_track_match(artist, track, album, item)
            if score > best_score:
                best_score = score
                best_item = item
        if not best_item or best_score < self.min_confidence:
            return None
        album_info = best_item.get("album") or {}
        artwork = [
            {"url": img.get("url"), "width": img.get("width"), "height": img.get("height")}
            for img in album_info.get("images", [])
            if img.get("url")
        ]
        artist_names = [entry.get("name") for entry in best_item.get("artists", []) if entry.get("name")]
        canonical = {
            "kind": "track",
            "provider": "spotify",
            "artist": artist_names[0] if artist_names else artist,
            "album": album_info.get("name") or album,
            "track": best_item.get("name") or track,
            "release_year": _release_year(album_info.get("release_date")),
            "album_type": album_info.get("album_type"),
            "duration_sec": int(best_item.get("duration_ms", 0) / 1000) if best_item.get("duration_ms") else None,
            "artwork": artwork,
            "external_ids": {
                "spotify_id": best_item.get("id"),
                "spotify_album_id": album_info.get("id"),
                "isrc": (best_item.get("external_ids") or {}).get("isrc"),
            },
            "track_number": best_item.get("track_number"),
            "disc_number": best_item.get("disc_number"),
            "album_track_count": album_info.get("total_tracks"),
        }
        if self.cache and cache_key:
            self.cache.set(cache_key, canonical)
        return canonical

    def resolve_album(self, artist, album):
        if not self._has_credentials() or not artist or not album:
            return None
        cache_key = None
        if self.cache:
            cache_key = f"spotify:album:{artist}|{album}"
            cached = self.cache.get(cache_key)
            if cached:
                return cached
        query = f"album:{album} artist:{artist}"
        payload = self._search(query, "album", 5)
        items = (payload or {}).get("albums", {}).get("items", [])
        best_item = None
        best_score = 0.0
        for item in items:
            score = _score_album_match(artist, album, item)
            if score > best_score:
                best_score = score
                best_item = item
        if not best_item or best_score < self.min_confidence:
            return None
        album_id = best_item.get("id")
        album_payload = self._request(_SPOTIFY_ALBUM_URL.format(album_id=urllib.parse.quote(album_id))) if album_id else None
        tracks_payload = (album_payload or {}).get("tracks") or {}
        track_items = tracks_payload.get("items", [])
        tracks = []
        for item in track_items:
            tracks.append(
                {
                    "title": item.get("name"),
                    "duration_sec": int(item.get("duration_ms", 0) / 1000) if item.get("duration_ms") else None,
                    "track_number": item.get("track_number"),
                    "disc_number": item.get("disc_number"),
                }
            )
        artwork = [
            {"url": img.get("url"), "width": img.get("width"), "height": img.get("height")}
            for img in best_item.get("images", [])
            if img.get("url")
        ]
        artist_names = [entry.get("name") for entry in best_item.get("artists", []) if entry.get("name")]
        canonical = {
            "kind": "album",
            "provider": "spotify",
            "artist": artist_names[0] if artist_names else artist,
            "album": best_item.get("name") or album,
            "release_year": _release_year(best_item.get("release_date")),
            "album_type": best_item.get("album_type"),
            "artwork": artwork,
            "external_ids": {
                "spotify_album_id": album_id,
            },
            "track_count": best_item.get("total_tracks"),
            "tracks": tracks,
        }
        if self.cache and cache_key:
            self.cache.set(cache_key, canonical)
        return canonical
