import logging
import re
from datetime import datetime
from typing import Any

from app.musicbrainz.client import (
    RELEASE_GROUP_TTL_SECONDS,
    RELEASE_TRACKS_TTL_SECONDS,
    SEARCH_TTL_SECONDS,
    MUSICBRAINZ_USER_AGENT,
    get_musicbrainz_client,
)

_RELEASE_GROUP_SEARCH_ENDPOINT = "/ws/2/release-group"
_RELEASE_SEARCH_ENDPOINT = "/ws/2/release"
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
_DOWNRANK_SECONDARY = {"live", "compilation", "soundtrack", "remix"}
logger = logging.getLogger(__name__)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _tokenize(text: str) -> list[str]:
    return [tok for tok in re.split(r"[^a-z0-9]+", (text or "").lower()) if tok]


def _remove_noise_tokens(tokens: list[str]) -> list[str]:
    return [tok for tok in tokens if tok not in _NOISE_WORDS]


def _split_artist_album(query: str) -> tuple[str, str]:
    text = (query or "").strip()
    if not text:
        return "", ""
    lowered = text.lower()
    for sep in (" - ", " – ", " — ", " by ", " : "):
        idx = lowered.find(sep)
        if idx > 0:
            left = text[:idx].strip()
            right = text[idx + len(sep) :].strip()
            if left and right:
                return left, right

    raw_tokens = [tok for tok in re.split(r"\s+", text) if tok]
    if len(raw_tokens) < 3:
        return text, text
    split_at = max(1, len(raw_tokens) // 2)
    artist = " ".join(raw_tokens[:split_at]).strip()
    album = " ".join(raw_tokens[split_at:]).strip()
    return artist or text, album or text


def _lucene_escape(text: str) -> str:
    return (text or "").replace("\\", "\\\\").replace('"', '\\"')


def _artist_credit_text(artist_credit: Any) -> str:
    if not isinstance(artist_credit, list):
        return ""
    parts: list[str] = []
    for part in artist_credit:
        if isinstance(part, str):
            parts.append(part)
            continue
        if isinstance(part, dict):
            name = part.get("name")
            if isinstance(name, str) and name.strip():
                parts.append(name.strip())
            join = part.get("joinphrase")
            if isinstance(join, str) and join:
                parts.append(join)
    return "".join(parts).strip()


def _token_overlap(query_tokens: list[str], text: str) -> float:
    if not query_tokens or not text:
        return 0.0
    a = set(query_tokens)
    b = set(_tokenize(text))
    if not a:
        return 0.0
    return len(a & b) / len(a)


def _parse_date(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def search_release_groups(query: str, limit: int = 10) -> list[dict[str, Any]]:
    cleaned_query = str(query or "").strip()
    if not cleaned_query:
        return []

    query_tokens = _tokenize(cleaned_query)
    clean_tokens = _remove_noise_tokens(query_tokens) or query_tokens
    normalized_query = " ".join(clean_tokens).strip() or cleaned_query
    artist_fragment, album_fragment = _split_artist_album(normalized_query)

    lucene_parts = ['primarytype:"album"']
    if artist_fragment:
        lucene_parts.append(f'artist:"{_lucene_escape(artist_fragment)}"')
    if album_fragment:
        lucene_parts.append(f'releasegroup:"{_lucene_escape(album_fragment)}"')
    else:
        lucene_parts.append(f'"{_lucene_escape(normalized_query)}"')

    params = {
        "query": " AND ".join(lucene_parts),
        "fmt": "json",
        "limit": max(10, min(int(limit or 10), 100)),
    }
    client = get_musicbrainz_client()
    payload = client.get_json(
        _RELEASE_GROUP_SEARCH_ENDPOINT,
        params=params,
        cache_key=f"album_search:{cleaned_query}",
        ttl_seconds=SEARCH_TTL_SECONDS,
    )
    if not payload:
        return []

    allow_live = "live" in clean_tokens
    allow_compilation = "compilation" in clean_tokens
    groups = payload.get("release-groups", []) if isinstance(payload, dict) else []
    candidates: list[dict[str, Any]] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        secondary_types_raw = group.get("secondary-types", [])
        secondary_types = [str(value) for value in secondary_types_raw if isinstance(value, str)]
        secondary_lower = {value.lower() for value in secondary_types}
        artist_credit = _artist_credit_text(group.get("artist-credit"))

        base_score = _safe_int(group.get("score"), default=0)
        overlap = _token_overlap(clean_tokens, artist_credit)
        adjusted = base_score + int(overlap * 30)
        if overlap >= 0.5:
            adjusted += 10

        if "live" in secondary_lower and not allow_live:
            adjusted -= 25
        if "compilation" in secondary_lower and not allow_compilation:
            adjusted -= 25
        if "soundtrack" in secondary_lower:
            adjusted -= 20
        if "remix" in secondary_lower:
            adjusted -= 20

        primary_type = str(group.get("primary-type") or "")
        if primary_type and primary_type.lower() != "album":
            adjusted -= 15

        adjusted = max(0, min(100, adjusted))
        candidates.append(
            {
                "release_group_id": group.get("id"),
                "title": group.get("title"),
                "artist_credit": artist_credit,
                "first_release_date": group.get("first-release-date"),
                "primary_type": group.get("primary-type"),
                "secondary_types": secondary_types,
                "score": int(adjusted),
                "track_count": None,
            }
        )

    candidates.sort(key=lambda c: c.get("score", 0), reverse=True)
    candidates = candidates[: max(1, min(int(limit or 10), 50))]
    top_title = candidates[0]["title"] if candidates else "-"
    top_score = candidates[0]["score"] if candidates else 0
    logger.info(f"[MUSIC] candidates_count={len(candidates)} top={top_title} ({top_score}) query={query}")
    return candidates


def pick_best_release_with_reason(
    release_group_id: str,
    *,
    prefer_country: str | None = None,
) -> dict[str, Any]:
    rgid = str(release_group_id or "").strip()
    if not rgid:
        return {"release_id": None, "reason": "missing_release_group_id"}

    params = {
        "release-group": rgid,
        "fmt": "json",
        "limit": 100,
    }
    client = get_musicbrainz_client()
    payload = client.get_json(
        _RELEASE_SEARCH_ENDPOINT,
        params=params,
        cache_key=f"release_group:{rgid}",
        ttl_seconds=RELEASE_GROUP_TTL_SECONDS,
    )
    if not payload:
        return {"release_id": None, "reason": "request_error"}

    releases = payload.get("releases", []) if isinstance(payload, dict) else []
    if not releases:
        return {"release_id": None, "reason": "no_releases"}

    parsed_dates = [(_parse_date(r.get("date")), r) for r in releases if isinstance(r, dict)]
    date_values = [d for d, _ in parsed_dates if d is not None]
    earliest = min(date_values) if date_values else None
    preferred_country = (prefer_country or "").strip().upper() or None

    ranked: list[tuple[float, dict[str, Any], str]] = []
    for release in releases:
        if not isinstance(release, dict):
            continue
        score = 0.0
        reasons: list[str] = []
        status = str(release.get("status") or "").strip().lower()
        if status == "official":
            score += 40
            reasons.append("official")
        elif status:
            reasons.append(f"status:{status}")

        release_date = _parse_date(release.get("date"))
        if release_date and earliest:
            delta_days = max(0, (release_date - earliest).days)
            score += max(0.0, 25.0 - (delta_days / 365.0))
            if delta_days == 0:
                reasons.append("earliest")

        country = str(release.get("country") or "").strip().upper()
        if preferred_country and country == preferred_country:
            score += 10
            reasons.append(f"country:{country}")

        track_count = _safe_int(release.get("track-count"), default=0)
        if track_count > 0:
            score += 1

        ranked.append((score, release, ",".join(reasons) or "fallback"))

    if not ranked:
        return {"release_id": None, "reason": "no_ranked_release"}
    ranked.sort(key=lambda item: item[0], reverse=True)
    best_score, best_release, reason = ranked[0]
    return {
        "release_id": best_release.get("id"),
        "reason": f"{reason},score={best_score:.2f}",
        "release": best_release,
    }


def pick_best_release(release_group_id: str) -> str | None:
    return pick_best_release_with_reason(release_group_id).get("release_id")


def fetch_release_tracks(release_id: str) -> list[dict[str, Any]]:
    rid = str(release_id or "").strip()
    if not rid:
        return []
    params = {
        "inc": "recordings+artist-credits",
        "fmt": "json",
    }
    client = get_musicbrainz_client()
    payload = client.get_json(
        f"{_RELEASE_SEARCH_ENDPOINT}/{rid}",
        params=params,
        cache_key=f"release_tracks:{rid}",
        ttl_seconds=RELEASE_TRACKS_TTL_SECONDS,
    )
    if not payload:
        return []

    media = payload.get("media", []) if isinstance(payload, dict) else []
    artist_credit = _artist_credit_text(payload.get("artist-credit"))
    album_title = payload.get("title")
    release_date = payload.get("date")
    tracks: list[dict[str, Any]] = []
    for disc in media:
        if not isinstance(disc, dict):
            continue
        disc_number = _safe_int(disc.get("position"), default=0) or None
        for track in disc.get("tracks", []) or []:
            if not isinstance(track, dict):
                continue
            track_artist = _artist_credit_text(track.get("artist-credit")) or artist_credit
            tracks.append(
                {
                    "title": track.get("title"),
                    "track_number": _safe_int(track.get("position"), default=0) or None,
                    "disc_number": disc_number,
                    "artist": track_artist,
                    "album": album_title,
                    "release_date": release_date,
                    "duration_ms": _safe_int(track.get("length"), default=0) or None,
                    "artwork_url": None,
                }
            )
    return tracks
