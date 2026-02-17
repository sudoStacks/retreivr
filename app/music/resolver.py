import requests
from typing import Optional, Dict, Any

MUSICBRAINZ_SEARCH_URL = "https://musicbrainz.org/ws/2/release/"
USER_AGENT = "retreivr/1.0 (self-hosted album resolver)"


def resolve_album(query: str) -> Optional[Dict[str, Any]]:
    """
    Attempt to resolve a query into a MusicBrainz album (release).
    Returns structured album metadata or None.
    """

    params = {
        "query": query,
        "fmt": "json",
        "limit": 5
    }

    headers = {
        "User-Agent": USER_AGENT
    }

    try:
        resp = requests.get(MUSICBRAINZ_SEARCH_URL, params=params, headers=headers, timeout=8)
        if resp.status_code != 200:
            return None

        data = resp.json()
        releases = data.get("releases", [])
        if not releases:
            return None

        # Pick best candidate (first result for now)
        release = releases[0]

        return {
            "type": "album",
            "album_id": release.get("id"),
            "title": release.get("title"),
            "artist": release.get("artist-credit", [{}])[0].get("name"),
            "date": release.get("date"),
            "track_count": release.get("track-count")
        }

    except Exception:
        return None


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
