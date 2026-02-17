import logging
import re
from typing import Any, Dict, Optional

import requests

MUSICBRAINZ_RELEASE_GROUP_SEARCH_URL = "https://musicbrainz.org/ws/2/release-group"
USER_AGENT = "retreivr/1.0 (self-hosted album resolver)"
logger = logging.getLogger(__name__)
_NOISE_WORDS = {
    "album",
    "full",
    "official",
    "audio",
    "music",
    "track",
    "single",
    "version",
    "deluxe",
    "remastered",
    "bonus",
}


def _extract_artist_album_fragments(query: str) -> tuple[str, str]:
    text = (query or "").strip()
    if not text:
        return "", ""
    raw_tokens = [tok for tok in re.split(r"\s+", text) if tok]
    filtered_tokens = [tok for tok in raw_tokens if tok.lower() not in _NOISE_WORDS]
    tokens_for_split = filtered_tokens or raw_tokens
    text = " ".join(tokens_for_split).strip()
    if not text:
        return "", ""
    lowered = text.lower()
    for sep in (" - ", " – ", " — ", " by ", " : "):
        idx = lowered.find(sep)
        if idx > 0:
            left = text[:idx].strip()
            right = text[idx + len(sep):].strip()
            if left and right:
                return left, right

    tokens = [tok for tok in re.split(r"\s+", text) if tok]
    if len(tokens) < 3:
        return text, text
    split_at = max(1, len(tokens) // 2)
    artist = " ".join(tokens[:split_at]).strip()
    album = " ".join(tokens[split_at:]).strip()
    return artist or text, album or text


def _lucene_escape(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace('"', '\\"')


def search_albums(query: str) -> list:
    artist_fragment, album_fragment = _extract_artist_album_fragments(query)
    parts = []
    if artist_fragment:
        parts.append(f'artist:"{_lucene_escape(artist_fragment)}"')
    parts.append('primarytype:"Album"')
    if album_fragment:
        parts.append(f'releasegroup:"{_lucene_escape(album_fragment)}"')
    lucene_query = " AND ".join(parts)

    params = {
        "query": lucene_query,
        "fmt": "json",
        "limit": 5,
    }
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(
            MUSICBRAINZ_RELEASE_GROUP_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=8,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        release_groups = data.get("release-groups", []) or []
        candidates = []
        for group in release_groups[:5]:
            artist_credit = group.get("artist-credit", []) or []
            artist_name = None
            if artist_credit:
                first_credit = artist_credit[0]
                if isinstance(first_credit, dict):
                    artist_name = first_credit.get("name")
            candidates.append(
                {
                    "album_id": group.get("id"),
                    "title": group.get("title"),
                    "artist": artist_name,
                    "first_released": group.get("first-release-date"),
                    "track_count": group.get("track-count") or 0,
                }
            )
        logger.info(f"[MUSIC] album candidates count={len(candidates)} query={query}")
        return candidates
    except Exception:
        return []


def resolve_album(query: str) -> Optional[Dict[str, Any]]:
    """
    Attempt to resolve a query into a MusicBrainz album (release).
    Returns structured album metadata or None.
    """

    candidates = search_albums(query)
    if not candidates:
        return None
    album = candidates[0]
    return {
        "type": "album",
        "album_id": album.get("album_id"),
        "title": album.get("title"),
        "artist": album.get("artist"),
        "date": album.get("first_released"),
        "track_count": album.get("track_count"),
    }


def fetch_album_tracks(album_id: str) -> Optional[list]:
    """
    Fetch full track list for a MusicBrainz release.
    """

    url = f"https://musicbrainz.org/ws/2/release/{album_id}"
    params = {
        "inc": "recordings",
        "fmt": "json"
    }

    headers = {
        "User-Agent": USER_AGENT
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=8)
        if resp.status_code != 200:
            return None

        data = resp.json()
        media = data.get("media", [])
        if not media:
            return None

        tracks = []

        for disc in media:
            for t in disc.get("tracks", []):
                tracks.append({
                    "title": t.get("title"),
                    "track_number": t.get("position"),
                    "artist": data.get("artist-credit", [{}])[0].get("name"),
                    "album": data.get("title"),
                    "release_date": data.get("date")
                })

        return tracks

    except Exception:
        return None
