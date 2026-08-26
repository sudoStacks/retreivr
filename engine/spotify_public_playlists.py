from __future__ import annotations

import base64
import csv
import html
import io
import json
import re
import time
from dataclasses import dataclass
from typing import Any

import requests

from metadata.importers.base import TrackIntent
from scheduler.jobs.spotify_playlist_watch import normalize_spotify_playlist_identifier
from spotify.client import SpotifyPlaylistClient


class SpotifyPublicPlaylistError(Exception):
    pass


@dataclass(frozen=True)
class SpotifyPlaylistSeed:
    title: str
    playlist_url: str
    description: str = ""
    genre: str = "country"
    source: str = "spotify"

    @property
    def playlist_id(self) -> str:
        return normalize_spotify_playlist_identifier(self.playlist_url)


@dataclass(frozen=True)
class SpotifyResolvedPlaylist:
    playlist_id: str
    playlist_url: str
    title: str
    owner: str | None
    description: str | None
    image_url: str | None
    tracks: list[TrackIntent]
    total_tracks: int
    resolver: str
    complete: bool
    warning: str | None = None

    def to_summary(self, *, preview_limit: int = 10) -> dict[str, Any]:
        return {
            "playlist_id": self.playlist_id,
            "playlist_url": self.playlist_url,
            "title": self.title,
            "owner": self.owner,
            "description": self.description,
            "image_url": self.image_url,
            "track_count": len(self.tracks),
            "total_tracks": int(self.total_tracks),
            "complete": bool(self.complete),
            "resolver": self.resolver,
            "warning": self.warning,
            "tracks_preview": [
                {
                    "artist": track.artist,
                    "title": track.title,
                    "album": track.album,
                    "duration_ms": track.duration_ms,
                }
                for track in self.tracks[:preview_limit]
            ],
        }


GENERAL_SPOTIFY_PLAYLIST_SEEDS: list[SpotifyPlaylistSeed] = [
    SpotifyPlaylistSeed(
        "Today's Top Hits",
        "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M",
        "Current global pop and crossover tracks.",
        genre="pop",
    ),
    SpotifyPlaylistSeed(
        "RapCaviar",
        "https://open.spotify.com/playlist/37i9dQZF1DX0XUsuxWHRQd",
        "Current hip hop and rap tracks.",
        genre="hip hop",
    ),
    SpotifyPlaylistSeed(
        "All New Indie",
        "https://open.spotify.com/playlist/37i9dQZF1DXdbXrPNafg9d",
        "Recent indie releases and discoveries.",
        genre="indie",
    ),
    SpotifyPlaylistSeed(
        "Rock Classics",
        "https://open.spotify.com/playlist/37i9dQZF1DWXRqgorJj26U",
        "Familiar rock staples.",
        genre="rock",
    ),
    SpotifyPlaylistSeed(
        "mint",
        "https://open.spotify.com/playlist/37i9dQZF1DX4dyzvuaRJ0n",
        "Dance and electronic picks.",
        genre="electronic",
    ),
    SpotifyPlaylistSeed(
        "Jazz Classics",
        "https://open.spotify.com/playlist/37i9dQZF1DXbITWG1ZJKYt",
        "Classic jazz entry points.",
        genre="jazz",
    ),
]


GENRE_SPOTIFY_PLAYLIST_SEEDS: dict[str, list[SpotifyPlaylistSeed]] = {
    "country": [
        SpotifyPlaylistSeed(
            "Hot Country",
            "https://open.spotify.com/playlist/37i9dQZF1DX1lVhptIYRda",
            "Today's top country hits.",
            genre="country",
        ),
        SpotifyPlaylistSeed(
            "New Boots",
            "https://open.spotify.com/playlist/37i9dQZF1DX8S0uQvJ4gaa",
            "Country discoveries and new releases.",
            genre="country",
        ),
    ],
    "pop": [GENERAL_SPOTIFY_PLAYLIST_SEEDS[0]],
    "hip hop": [GENERAL_SPOTIFY_PLAYLIST_SEEDS[1]],
    "indie": [GENERAL_SPOTIFY_PLAYLIST_SEEDS[2]],
    "rock": [GENERAL_SPOTIFY_PLAYLIST_SEEDS[3]],
    "electronic": [GENERAL_SPOTIFY_PLAYLIST_SEEDS[4]],
    "jazz": [GENERAL_SPOTIFY_PLAYLIST_SEEDS[5]],
}


COUNTRY_SPOTIFY_PLAYLIST_SEEDS: list[SpotifyPlaylistSeed] = [
    SpotifyPlaylistSeed(
        "Hot Country",
        "https://open.spotify.com/playlist/37i9dQZF1DX1lVhptIYRda",
        "Today's top country hits.",
    ),
    SpotifyPlaylistSeed(
        "New Boots",
        "https://open.spotify.com/playlist/37i9dQZF1DX8S0uQvJ4gaa",
        "Break them in, wear them out.",
    ),
    SpotifyPlaylistSeed(
        "Country Party",
        "https://open.spotify.com/playlist/37i9dQZF1DWXi7h4mmmkzD",
        "Nighttime is the right time for country party hits.",
    ),
    SpotifyPlaylistSeed(
        "Hot Country Presents Best Country Songs of 2025",
        "https://open.spotify.com/playlist/37i9dQZF1DWXuiFJj5T7Ii",
        "Spotify editors' picks for country songs of the year.",
    ),
    SpotifyPlaylistSeed(
        "This Is Eric Church",
        "https://open.spotify.com/playlist/37i9dQZF1DZ06evO1yUGpa",
        "The essential Eric Church tracks.",
    ),
    SpotifyPlaylistSeed(
        "This Is Kenny Chesney",
        "https://open.spotify.com/playlist/37i9dQZF1DZ06evO1TkYCI",
        "The essential Kenny Chesney tracks.",
    ),
]


def spotify_tracks_to_csv_bytes(tracks: list[TrackIntent]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=["artist", "title", "album", "duration_ms"],
    )
    writer.writeheader()
    for track in tracks:
        writer.writerow(
            {
                "artist": track.artist or "",
                "title": track.title or "",
                "album": track.album or "",
                "duration_ms": track.duration_ms or "",
            }
        )
    return output.getvalue().encode("utf-8")


def spotify_tracks_to_m3u(tracks: list[TrackIntent]) -> str:
    lines = ["#EXTM3U"]
    for track in tracks:
        seconds = int((track.duration_ms or 0) / 1000) if track.duration_ms else -1
        artist = track.artist or ""
        title = track.title or ""
        lines.append(f"#EXTINF:{seconds},{artist} - {title}".strip())
        lines.append(f"{artist} - {title}".strip())
    return "\n".join(lines) + "\n"


class SpotifyPlaylistResolver:
    def __init__(
        self,
        *,
        spotify_client: SpotifyPlaylistClient | None = None,
        timeout_sec: int = 20,
        public_attempts: int = 3,
        retry_delay_sec: float = 0.4,
    ) -> None:
        self.spotify_client = spotify_client
        self.timeout_sec = timeout_sec
        self.public_attempts = max(1, min(3, int(public_attempts or 1)))
        self.retry_delay_sec = max(0.0, min(1.0, float(retry_delay_sec or 0.0)))

    def resolve(self, playlist_url_or_id: str, *, prefer_api: bool = True) -> SpotifyResolvedPlaylist:
        playlist_id = normalize_spotify_playlist_identifier(playlist_url_or_id)
        if not playlist_id:
            raise SpotifyPublicPlaylistError("Spotify playlist URL or ID is required")
        playlist_url = f"https://open.spotify.com/playlist/{playlist_id}"

        public_result: SpotifyResolvedPlaylist | None = None
        public_error: Exception | None = None
        for attempt in range(self.public_attempts):
            try:
                candidate = self._resolve_public_page(playlist_id, playlist_url)
                if public_result is None or len(candidate.tracks) > len(public_result.tracks):
                    public_result = candidate
                if candidate.complete:
                    return candidate
            except Exception as exc:
                public_error = exc
            if attempt < self.public_attempts - 1 and self.retry_delay_sec > 0:
                time.sleep(self.retry_delay_sec)

        if public_result is not None and (not prefer_api or self.spotify_client is None):
            return public_result

        if prefer_api and self.spotify_client is not None:
            try:
                api_result = self._resolve_api(playlist_id, playlist_url)
                if public_result is not None:
                    return SpotifyResolvedPlaylist(
                        playlist_id=api_result.playlist_id,
                        playlist_url=api_result.playlist_url,
                        title=public_result.title or api_result.title,
                        owner=public_result.owner or api_result.owner,
                        description=public_result.description or api_result.description,
                        image_url=public_result.image_url or api_result.image_url,
                        tracks=api_result.tracks,
                        total_tracks=max(api_result.total_tracks, public_result.total_tracks),
                        resolver="spotify_public_page+spotify_api",
                        complete=True,
                    )
                return api_result
            except Exception:
                if public_result is not None:
                    return public_result

        if public_error is not None:
            raise public_error
        raise SpotifyPublicPlaylistError("Spotify playlist metadata not found")

    def _resolve_api(self, playlist_id: str, playlist_url: str) -> SpotifyResolvedPlaylist:
        assert self.spotify_client is not None
        snapshot_id, items = self.spotify_client.get_playlist_items(playlist_id)
        tracks = [
            TrackIntent(
                artist=item.get("artist"),
                title=item.get("title"),
                album=item.get("album"),
                raw_line=json.dumps(item, ensure_ascii=False),
                source_format="spotify",
                duration_ms=item.get("duration_ms"),
            )
            for item in items
            if item.get("title")
        ]
        return SpotifyResolvedPlaylist(
            playlist_id=playlist_id,
            playlist_url=playlist_url,
            title=playlist_id,
            owner=None,
            description=f"Spotify API playlist snapshot {snapshot_id}",
            image_url=None,
            tracks=tracks,
            total_tracks=len(tracks),
            resolver="spotify_api",
            complete=True,
        )

    def _resolve_public_page(self, playlist_id: str, playlist_url: str) -> SpotifyResolvedPlaylist:
        response = requests.get(
            playlist_url,
            headers={
                "User-Agent": "Retreivr/1.0 (+https://github.com/sudostacks/retreivr)",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=self.timeout_sec,
        )
        if response.status_code != 200:
            raise SpotifyPublicPlaylistError(f"Spotify playlist unavailable ({response.status_code})")

        candidates: list[dict[str, Any]] = []
        for state in _iter_embedded_json(response.text):
            candidates.extend(_find_playlist_candidates(state, playlist_id))

        best: dict[str, Any] | None = None
        best_tracks: list[TrackIntent] = []
        best_total = 0
        for candidate in candidates:
            tracks, total = _extract_tracks(candidate)
            if len(tracks) > len(best_tracks):
                best = candidate
                best_tracks = tracks
                best_total = total

        if best is None:
            raise SpotifyPublicPlaylistError("Spotify playlist metadata not found in public page")
        if not best_tracks and best_total > 0:
            raise SpotifyPublicPlaylistError("Spotify public page did not expose track metadata")

        title = str(best.get("name") or best.get("title") or playlist_id).strip() or playlist_id
        owner = _extract_owner(best)
        image_url = _extract_image(best)
        description = str(best.get("description") or "").strip() or None
        complete = len(best_tracks) >= int(best_total or len(best_tracks))
        warning = None if complete else f"Public Spotify page exposed {len(best_tracks)} of {best_total} tracks."
        return SpotifyResolvedPlaylist(
            playlist_id=playlist_id,
            playlist_url=playlist_url,
            title=title,
            owner=owner,
            description=description,
            image_url=image_url,
            tracks=best_tracks,
            total_tracks=int(best_total or len(best_tracks)),
            resolver="spotify_public_page",
            complete=complete,
            warning=warning,
        )


def _iter_embedded_json(html_content: str):
    for match in re.finditer(r'<script[^>]+type="text/plain"[^>]*>([^<]+)</script>', html_content):
        raw = match.group(1).strip()
        try:
            yield json.loads(base64.b64decode(raw + "===").decode("utf-8", "replace"))
        except Exception:
            continue

    next_match = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_content, re.S)
    if next_match:
        try:
            yield json.loads(html.unescape(next_match.group(1)))
        except Exception:
            pass


def _find_playlist_candidates(obj: Any, playlist_id: str, depth: int = 0) -> list[dict[str, Any]]:
    if depth > 14:
        return []
    found: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        uri = str(obj.get("uri") or "")
        if uri == f"spotify:playlist:{playlist_id}" or str(obj.get("id") or "") == playlist_id:
            if obj.get("content") or obj.get("tracks") or obj.get("name"):
                found.append(obj)
        entity = (obj.get("entities") or {}).get("items", {}).get(f"spotify:playlist:{playlist_id}")
        if isinstance(entity, dict):
            found.append(entity)
        for value in obj.values():
            found.extend(_find_playlist_candidates(value, playlist_id, depth + 1))
    elif isinstance(obj, list):
        for value in obj:
            found.extend(_find_playlist_candidates(value, playlist_id, depth + 1))
    return found


def _extract_tracks(playlist: dict[str, Any]) -> tuple[list[TrackIntent], int]:
    if isinstance(playlist.get("content"), dict):
        items = playlist["content"].get("items") or []
        total = playlist["content"].get("totalCount")
    elif isinstance(playlist.get("tracks"), dict):
        items = playlist["tracks"].get("items") or []
        total = playlist["tracks"].get("total") or playlist["tracks"].get("totalCount")
    else:
        items = []
        total = None

    tracks: list[TrackIntent] = []
    for item in items:
        track = None
        if isinstance(item, dict):
            track = ((item.get("itemV2") or {}).get("data") if isinstance(item.get("itemV2"), dict) else None)
            if track is None:
                track = item.get("track") if isinstance(item.get("track"), dict) else item
        if not isinstance(track, dict) or track.get("is_local"):
            continue
        title = str(track.get("name") or "").strip()
        if not title:
            continue
        album = track.get("albumOfTrack") or track.get("album") or {}
        artist_items = track.get("artists", {}).get("items") if isinstance(track.get("artists"), dict) else track.get("artists") or []
        artist_names = []
        for artist in artist_items or []:
            if not isinstance(artist, dict):
                continue
            name = str((artist.get("profile") or {}).get("name") or artist.get("name") or "").strip()
            if name:
                artist_names.append(name)
        duration = track.get("duration", {}).get("totalMilliseconds") if isinstance(track.get("duration"), dict) else track.get("duration_ms")
        tracks.append(
            TrackIntent(
                artist=", ".join(artist_names) or None,
                title=title,
                album=str(album.get("name") or "").strip() if isinstance(album, dict) else None,
                raw_line=json.dumps(track, ensure_ascii=False),
                source_format="spotify_public",
                duration_ms=_safe_int(duration),
            )
        )
    return tracks, int(total or len(tracks))


def _extract_owner(playlist: dict[str, Any]) -> str | None:
    owner = playlist.get("ownerV2") or playlist.get("owner") or {}
    data = owner.get("data") if isinstance(owner, dict) else None
    if isinstance(data, dict):
        return str(data.get("name") or data.get("display_name") or "").strip() or None
    if isinstance(owner, dict):
        return str(owner.get("name") or owner.get("display_name") or "").strip() or None
    return None


def _extract_image(playlist: dict[str, Any]) -> str | None:
    images = playlist.get("images") or {}
    if isinstance(images, dict):
        items = images.get("items") or []
        for item in items:
            for source in (item.get("sources") or []) if isinstance(item, dict) else []:
                url = str(source.get("url") or "").strip()
                if url:
                    return url
    if isinstance(images, list):
        for item in images:
            url = str((item or {}).get("url") or "").strip()
            if url:
                return url
    return None


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(float(str(value or "").strip()))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
