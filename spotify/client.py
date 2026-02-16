"""Spotify API client for playlist snapshot reads."""

from __future__ import annotations

import base64
import os
import time
import urllib.parse
from typing import Any

import requests


class SpotifyPlaylistClient:
    """Client for reading playlist snapshots and playlist items from Spotify."""

    _TOKEN_URL = "https://accounts.spotify.com/api/token"
    _PLAYLIST_URL = "https://api.spotify.com/v1/playlists/{playlist_id}"
    _PLAYLIST_ITEMS_URL = "https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    def __init__(
        self,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        timeout_sec: int = 20,
    ) -> None:
        self.client_id = client_id or os.environ.get("SPOTIFY_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("SPOTIFY_CLIENT_SECRET")
        self.timeout_sec = timeout_sec
        self._access_token: str | None = None
        self._access_token_expire_at: float = 0.0

    def _get_access_token(self) -> str:
        if not self.client_id or not self.client_secret:
            raise RuntimeError("Spotify credentials are required")

        now = time.time()
        if self._access_token and now < self._access_token_expire_at:
            return self._access_token

        auth_payload = f"{self.client_id}:{self.client_secret}".encode("utf-8")
        auth_header = base64.b64encode(auth_payload).decode("ascii")
        response = requests.post(
            self._TOKEN_URL,
            data={"grant_type": "client_credentials"},
            headers={"Authorization": f"Basic {auth_header}"},
            timeout=self.timeout_sec,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Spotify token request failed ({response.status_code})")

        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("Spotify token response missing access_token")

        expires_in = int(payload.get("expires_in") or 0)
        self._access_token = token
        self._access_token_expire_at = now + max(0, expires_in - 30)
        return token

    def _request_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        token = self._get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, params=params, headers=headers, timeout=self.timeout_sec)
        if response.status_code == 401:
            self._access_token = None
            token = self._get_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, params=params, headers=headers, timeout=self.timeout_sec)
        if response.status_code != 200:
            raise RuntimeError(f"Spotify request failed ({response.status_code})")
        return response.json()

    def get_playlist_items(self, playlist_id: str) -> tuple[str, list[dict[str, Any]]]:
        """Fetch a playlist snapshot id and ordered normalized item records."""
        playlist_id = (playlist_id or "").strip()
        if not playlist_id:
            raise ValueError("playlist_id is required")

        encoded_id = urllib.parse.quote(playlist_id, safe="")
        metadata = self._request_json(
            self._PLAYLIST_URL.format(playlist_id=encoded_id),
            params={"fields": "snapshot_id"},
        )
        snapshot_id = metadata.get("snapshot_id")
        if not snapshot_id:
            raise RuntimeError("Spotify playlist response missing snapshot_id")

        items: list[dict[str, Any]] = []
        offset = 0
        limit = 100
        while True:
            payload = self._request_json(
                self._PLAYLIST_ITEMS_URL.format(playlist_id=encoded_id),
                params={
                    "offset": offset,
                    "limit": limit,
                    "fields": "items(added_at,added_by(id),is_local,track(id,uri,name)),total,next",
                },
            )
            raw_items = payload.get("items") or []
            for raw in raw_items:
                track = raw.get("track") or {}
                items.append(
                    {
                        "uri": track.get("uri"),
                        "track_id": track.get("id"),
                        "added_at": raw.get("added_at"),
                        "added_by": (raw.get("added_by") or {}).get("id"),
                        "is_local": bool(raw.get("is_local")),
                        "name": track.get("name"),
                    }
                )

            if not payload.get("next"):
                break
            offset += len(raw_items)

        return str(snapshot_id), items

