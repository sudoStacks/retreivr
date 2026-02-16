"""Spotify API client for playlist snapshots and normalized playlist items."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import time
import urllib.parse
from typing import Any, TypedDict

import requests


class NormalizedItem(TypedDict):
    """Normalized Spotify playlist item record."""

    spotify_track_id: str | None
    position: int
    added_at: str | None
    artist: str | None
    title: str | None
    album: str | None
    duration_ms: int | None
    isrc: str | None


class SpotifyPlaylistClient:
    """Client for reading playlist snapshots and playlist items from Spotify."""

    _TOKEN_URL = "https://accounts.spotify.com/api/token"
    _PLAYLIST_URL = "https://api.spotify.com/v1/playlists/{playlist_id}"

    def __init__(
        self,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        access_token: str | None = None,
        timeout_sec: int = 20,
    ) -> None:
        self.client_id = client_id or os.environ.get("SPOTIFY_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("SPOTIFY_CLIENT_SECRET")
        self.timeout_sec = timeout_sec
        self._provided_access_token = (access_token or "").strip() or None
        self._access_token: str | None = None
        self._access_token_expire_at: float = 0.0

    def _get_access_token(self) -> str:
        if self._provided_access_token:
            return self._provided_access_token

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

    def get_playlist_items(self, playlist_id: str) -> tuple[str, list[NormalizedItem]]:
        """Fetch playlist `snapshot_id` and normalized items in original playlist order."""
        playlist_id = (playlist_id or "").strip()
        if not playlist_id:
            raise ValueError("playlist_id is required")

        encoded_id = urllib.parse.quote(playlist_id, safe="")
        fields = (
            "snapshot_id,"
            "tracks(items(added_at,track(id,name,duration_ms,external_ids(isrc),album(name),artists(name))),next)"
        )
        payload = self._request_json(
            self._PLAYLIST_URL.format(playlist_id=encoded_id),
            params={"fields": fields, "limit": 100},
        )

        snapshot_id = payload.get("snapshot_id")
        if not snapshot_id:
            raise RuntimeError("Spotify playlist response missing snapshot_id")

        items: list[NormalizedItem] = []
        absolute_position = 0
        tracks_page = payload.get("tracks") or {}
        while True:
            raw_items = tracks_page.get("items") or []
            for raw in raw_items:
                track = raw.get("track")
                if track is None:
                    absolute_position += 1
                    continue
                artists = track.get("artists") or []
                first_artist = artists[0].get("name") if artists and isinstance(artists[0], dict) else None
                album = track.get("album") or {}
                external_ids = track.get("external_ids") or {}
                items.append(
                    {
                        "spotify_track_id": track.get("id"),
                        "position": absolute_position,
                        "added_at": raw.get("added_at"),
                        "artist": first_artist,
                        "title": track.get("name"),
                        "album": album.get("name"),
                        "duration_ms": track.get("duration_ms"),
                        "isrc": external_ids.get("isrc"),
                    }
                )
                absolute_position += 1

            next_url = tracks_page.get("next")
            if not next_url:
                break
            tracks_page = self._request_json(str(next_url))

        return str(snapshot_id), items

    async def get_liked_songs(self) -> tuple[str, list[dict[str, Any]]]:
        """Fetch the authenticated user's saved tracks from Spotify.

        Returns:
            A tuple of ``(snapshot_id, items)`` where ``snapshot_id`` is a deterministic
            SHA-256 hash of the ordered track-id sequence, and ``items`` is an ordered
            list of normalized track dicts matching playlist ingestion structure.
        """
        if not self._provided_access_token:
            raise RuntimeError("Spotify OAuth access_token is required for liked songs")

        fields = (
            "items(added_at,track(id,name,duration_ms,external_ids(isrc),"
            "artists(name),album(id,name,release_date))),next,total"
        )
        offset = 0
        limit = 50
        position = 0
        items: list[dict[str, Any]] = []
        ordered_track_ids: list[str] = []

        while True:
            payload = await _request_json_with_retry(
                self,
                "https://api.spotify.com/v1/me/tracks",
                params={"limit": limit, "offset": offset, "fields": fields},
            )
            raw_items = payload.get("items") or []

            for raw in raw_items:
                track = raw.get("track")
                if not isinstance(track, dict):
                    continue

                track_id = track.get("id")
                if not track_id:
                    continue

                artists = track.get("artists") or []
                artist_names = [
                    str(artist.get("name")).strip()
                    for artist in artists
                    if isinstance(artist, dict) and artist.get("name")
                ]
                first_artist = artist_names[0] if artist_names else None
                album = track.get("album") or {}
                external_ids = track.get("external_ids") or {}

                items.append(
                    {
                        "spotify_track_id": track_id,
                        "position": position,
                        "added_at": raw.get("added_at"),
                        "artist": first_artist,
                        "title": track.get("name"),
                        "album": album.get("name"),
                        "duration_ms": track.get("duration_ms"),
                        "isrc": external_ids.get("isrc"),
                        "artists": artist_names,
                        "album_id": album.get("id"),
                        "album_release_date": album.get("release_date"),
                    }
                )
                ordered_track_ids.append(str(track_id))
                position += 1

            next_url = payload.get("next")
            if not next_url:
                break
            offset += limit

        snapshot_source = "\n".join(ordered_track_ids).encode("utf-8")
        snapshot_id = hashlib.sha256(snapshot_source).hexdigest()
        return snapshot_id, items

    async def get_saved_albums(self) -> tuple[str, list[dict[str, Any]]]:
        """Fetch authenticated user's saved albums from Spotify.

        Returns:
            A tuple ``(snapshot_id, items)`` where:
            - ``snapshot_id`` is a deterministic SHA-256 hash of ordered album IDs.
            - ``items`` is an ordered list of album dicts containing album metadata
              and normalized ordered track lists suitable for album sync flows.
        """
        if not self._provided_access_token:
            raise RuntimeError("Spotify OAuth access_token is required for saved albums")

        offset = 0
        limit = 50
        saved_albums: list[dict[str, Any]] = []
        ordered_album_ids: list[str] = []

        while True:
            payload = await _request_json_with_retry(
                self,
                "https://api.spotify.com/v1/me/albums",
                params={
                    "limit": limit,
                    "offset": offset,
                    "fields": "items(added_at,album(id,name,artists(name),release_date,total_tracks)),next,total",
                },
            )

            for entry in payload.get("items") or []:
                album = entry.get("album")
                if not isinstance(album, dict):
                    continue
                album_id = str(album.get("id") or "").strip()
                if not album_id:
                    continue
                ordered_album_ids.append(album_id)
                saved_albums.append(
                    {
                        "album_id": album_id,
                        "added_at": entry.get("added_at"),
                        "name": album.get("name"),
                        "artists": [
                            str(artist.get("name")).strip()
                            for artist in (album.get("artists") or [])
                            if isinstance(artist, dict) and artist.get("name")
                        ],
                        "release_date": album.get("release_date"),
                        "total_tracks": album.get("total_tracks"),
                    }
                )

            next_url = payload.get("next")
            if not next_url:
                break
            offset += limit

        album_items: list[dict[str, Any]] = []
        for position, album_entry in enumerate(saved_albums):
            album_id = str(album_entry.get("album_id") or "").strip()
            encoded_album_id = urllib.parse.quote(album_id, safe="")
            album_payload = await _request_json_with_retry(
                self,
                f"https://api.spotify.com/v1/albums/{encoded_album_id}",
                params={
                    "fields": (
                        "id,name,artists(name),release_date,total_tracks,"
                        "tracks(items(id,name,duration_ms,track_number,disc_number,artists(name),external_ids(isrc)),next)"
                    )
                },
            )

            album_name = album_payload.get("name") or album_entry.get("name")
            album_artists = [
                str(artist.get("name")).strip()
                for artist in (album_payload.get("artists") or [])
                if isinstance(artist, dict) and artist.get("name")
            ]
            tracks_page = album_payload.get("tracks") or {}
            tracks: list[dict[str, Any]] = []
            track_position = 0
            while True:
                for raw_track in tracks_page.get("items") or []:
                    if not isinstance(raw_track, dict):
                        continue
                    track_id = raw_track.get("id")
                    if not track_id:
                        continue
                    artists = raw_track.get("artists") or []
                    first_artist = (
                        artists[0].get("name")
                        if artists and isinstance(artists[0], dict)
                        else (album_artists[0] if album_artists else None)
                    )
                    external_ids = raw_track.get("external_ids") or {}
                    tracks.append(
                        {
                            "spotify_track_id": track_id,
                            "position": track_position,
                            "artist": first_artist,
                            "title": raw_track.get("name"),
                            "album": album_name,
                            "duration_ms": raw_track.get("duration_ms"),
                            "isrc": external_ids.get("isrc"),
                            "track_num": raw_track.get("track_number"),
                            "disc_num": raw_track.get("disc_number"),
                        }
                    )
                    track_position += 1

                next_tracks_url = tracks_page.get("next")
                if not next_tracks_url:
                    break
                tracks_page = await _request_json_with_retry(self, str(next_tracks_url))

            album_items.append(
                {
                    "album_id": album_id,
                    "position": position,
                    "added_at": album_entry.get("added_at"),
                    "name": album_name,
                    "artist": album_artists[0] if album_artists else None,
                    "artists": album_artists,
                    "release_date": album_payload.get("release_date") or album_entry.get("release_date"),
                    "total_tracks": album_payload.get("total_tracks") or album_entry.get("total_tracks"),
                    "tracks": tracks,
                }
            )

        snapshot_source = "\n".join(ordered_album_ids).encode("utf-8")
        snapshot_id = hashlib.sha256(snapshot_source).hexdigest()
        return snapshot_id, album_items


async def _request_json_with_retry(
    spotify_client: SpotifyPlaylistClient,
    url: str,
    params: dict[str, Any] | None = None,
    *,
    max_rate_limit_retries: int = 3,
) -> dict[str, Any]:
    """Perform a Spotify GET request and retry on HTTP 429 responses."""
    unauthorized_retry_used = False
    attempts = 0
    while True:
        attempts += 1
        token = await asyncio.to_thread(spotify_client._get_access_token)
        headers = {"Authorization": f"Bearer {token}"}
        response = await asyncio.to_thread(
            requests.get,
            url,
            params=params,
            headers=headers,
            timeout=spotify_client.timeout_sec,
        )

        if response.status_code == 401 and not unauthorized_retry_used:
            unauthorized_retry_used = True
            spotify_client._access_token = None
            continue

        if response.status_code == 429:
            if attempts > max_rate_limit_retries + 1:
                raise RuntimeError("Spotify request failed (429: rate limit exceeded retries)")
            retry_after = response.headers.get("Retry-After", "1")
            try:
                sleep_sec = float(retry_after)
            except (TypeError, ValueError):
                sleep_sec = 1.0
            await asyncio.sleep(max(0.0, sleep_sec))
            continue

        if response.status_code != 200:
            raise RuntimeError(f"Spotify request failed ({response.status_code})")
        return response.json()


async def get_playlist_items(
    spotify_client: SpotifyPlaylistClient,
    playlist_id: str,
) -> tuple[str, list[dict[str, Any]]]:
    """Fetch all Spotify playlist tracks with pagination and return `(snapshot_id, ordered_items)`."""
    cleaned_playlist_id = (playlist_id or "").strip()
    if not cleaned_playlist_id:
        raise ValueError("playlist_id is required")

    encoded_id = urllib.parse.quote(cleaned_playlist_id, safe="")
    fields = (
        "snapshot_id,"
        "tracks(items(added_at,track(id,name,duration_ms,external_ids(isrc),album(name),artists(name))),next)"
    )
    payload = await _request_json_with_retry(
        spotify_client,
        spotify_client._PLAYLIST_URL.format(playlist_id=encoded_id),
        params={"fields": fields, "limit": 100},
    )

    snapshot_id = payload.get("snapshot_id")
    if not snapshot_id:
        raise RuntimeError("Spotify playlist response missing snapshot_id")

    ordered_items: list[dict[str, Any]] = []
    absolute_position = 0
    tracks_page = payload.get("tracks") or {}
    while True:
        raw_items = tracks_page.get("items") or []
        for raw in raw_items:
            track = raw.get("track")
            if track is None:
                absolute_position += 1
                continue

            artists = track.get("artists") or []
            first_artist = artists[0].get("name") if artists and isinstance(artists[0], dict) else None
            album = track.get("album") or {}
            external_ids = track.get("external_ids") or {}
            ordered_items.append(
                {
                    "spotify_track_id": track.get("id"),
                    "position": absolute_position,
                    "added_at": raw.get("added_at"),
                    "artist": first_artist,
                    "title": track.get("name"),
                    "album": album.get("name"),
                    "duration_ms": track.get("duration_ms"),
                    "isrc": external_ids.get("isrc"),
                }
            )
            absolute_position += 1

        next_url = tracks_page.get("next")
        if not next_url:
            break
        tracks_page = await _request_json_with_retry(spotify_client, str(next_url))

    return str(snapshot_id), ordered_items
