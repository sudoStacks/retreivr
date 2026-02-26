from __future__ import annotations

import logging
import re
import urllib.parse
import unicodedata
from typing import Any

import musicbrainzngs

from metadata.services.musicbrainz_service import get_musicbrainz_service

logger = logging.getLogger(__name__)

CORRECTNESS_WEIGHT = 0.8
COMPLETENESS_WEIGHT = 0.2
CORRECTNESS_FLOOR = 62.0
MAX_DURATION_DELTA_MS = 12000
PREVIEW_REJECT_MS = 45000
BUCKET_MULTIPLIERS: dict[str, float] = {
    "album": 1.00,
    "compilation": 0.96,
    "single": 0.92,
}

_WORD_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PREVIEW_RE = re.compile(r"\b(preview|snippet|teaser)\b", re.IGNORECASE)
_BRACKETED_SEGMENT_RE = re.compile(r"\([^)]*\)|\[[^\]]*\]")
_TRANSPORT_TOKEN_RE = re.compile(
    r"\b("
    r"music video|official video|official music video|lyric video|visualizer|"
    r"official audio|audio|hd|4k|cmt sessions|topic"
    r")\b",
    re.IGNORECASE,
)
_TRAILING_HYPHEN_SUFFIX_RE = re.compile(
    r"\s*-\s*(official video|topic)\s*$",
    re.IGNORECASE,
)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,}$")

_DISALLOWED_VARIANT_TOKENS = {
    "live",
    "acoustic",
    "instrumental",
    "karaoke",
    "cover",
    "tribute",
    "remix",
    "nightcore",
    "stripped",
}
_DISALLOWED_VARIANT_PHRASES = (
    "radio edit",
    "extended mix",
    "sped up",
    "slowed",
)
_NEUTRAL_TITLE_PHRASES = (
    "official video",
    "official music video",
    "official audio",
    "lyric video",
    "music video",
    "visualizer",
    "hd",
    "4k",
    "remastered",
    "remaster",
)
_NEUTRAL_SEGMENT_TOKEN_ALLOWLIST = {
    "official",
    "video",
    "music",
    "audio",
    "lyric",
    "visualizer",
    "hd",
    "4k",
    "remastered",
    "remaster",
}
_NEUTRAL_TITLE_PHRASE_RES = tuple(
    re.compile(rf"\b{re.escape(phrase)}\b", re.IGNORECASE)
    for phrase in _NEUTRAL_TITLE_PHRASES
)
_MB_YOUTUBE_REL_ALLOWED_HINTS = (
    "streaming music",
    "official music video",
    "music video",
    "video",
    "streaming",
)


def _tokens(value: Any) -> set[str]:
    return {m.group(0) for m in _WORD_TOKEN_RE.finditer(str(value or "").lower())}


def _token_similarity(left: Any, right: Any) -> float:
    l_tokens = _tokens(left)
    r_tokens = _tokens(right)
    if not l_tokens or not r_tokens:
        return 0.0
    return len(l_tokens & r_tokens) / max(len(l_tokens), 1)


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _normalize_for_matching(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower()
    text = _NON_ALNUM_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_variant_triggers(value: Any) -> list[str]:
    normalized = _normalize_for_matching(value)
    if not normalized:
        return []
    padded = f" {normalized} "
    tokens = set(normalized.split())
    triggers: set[str] = set()
    triggers.update(token for token in tokens if token in _DISALLOWED_VARIANT_TOKENS)
    for phrase in _DISALLOWED_VARIANT_PHRASES:
        if f" {phrase} " in padded:
            triggers.add(phrase)
    return sorted(triggers)


def _strip_neutral_title_phrases(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower()

    def _strip_neutral_bracketed_segment(match: re.Match[str]) -> str:
        segment = match.group(0)
        inner = segment[1:-1].strip()
        if not inner:
            return " "
        normalized_inner = _normalize_for_matching(inner)
        if not normalized_inner:
            return " "
        inner_tokens = normalized_inner.split()
        if inner_tokens and all(token.isdigit() or token in _NEUTRAL_SEGMENT_TOKEN_ALLOWLIST for token in inner_tokens):
            return " "
        return f" {inner} "

    text = _BRACKETED_SEGMENT_RE.sub(_strip_neutral_bracketed_segment, text)
    for pattern in _NEUTRAL_TITLE_PHRASE_RES:
        text = pattern.sub(" ", text)
    text = _TRAILING_HYPHEN_SUFFIX_RE.sub(" ", text)
    text = _NON_ALNUM_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_title_for_mb_lookup(raw_title: str, *, query_flags: dict | None = None) -> str:
    _ = query_flags
    text = unicodedata.normalize("NFKC", str(raw_title or "")).lower()
    def _strip_or_keep_bracketed(match: re.Match[str]) -> str:
        segment = match.group(0)
        inner = segment[1:-1].strip()
        inner_lower = inner.lower()
        if "live" in inner_lower or "acoustic" in inner_lower:
            return f" {inner} "
        return " "

    text = _BRACKETED_SEGMENT_RE.sub(_strip_or_keep_bracketed, text)
    text = _TRANSPORT_TOKEN_RE.sub(" ", text)
    text = _TRAILING_HYPHEN_SUFFIX_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _score_value(recording: dict[str, Any]) -> float:
    raw = recording.get("score")
    if raw is None:
        raw = recording.get("ext:score")
    try:
        score = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if score > 1.0:
        score = score / 100.0
    return max(0.0, min(score, 1.0))


def _extract_youtube_video_id(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = urllib.parse.urlparse(text)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if "youtube.com" in host:
        qs = urllib.parse.parse_qs(parsed.query)
        for key in ("v", "vi"):
            raw = qs.get(key)
            if not raw:
                continue
            candidate = str(raw[0] or "").strip()
            if _YOUTUBE_ID_RE.match(candidate):
                return candidate
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] in {"shorts", "embed", "live", "watch"}:
            candidate = str(parts[1] or "").strip()
            if _YOUTUBE_ID_RE.match(candidate):
                return candidate
    if "youtu.be" in host:
        candidate = str(path.lstrip("/").split("/")[0] or "").strip()
        if _YOUTUBE_ID_RE.match(candidate):
            return candidate
    return None


def _canonicalize_youtube_watch_url(value: Any) -> str | None:
    video_id = _extract_youtube_video_id(value)
    if not video_id:
        return None
    return f"https://www.youtube.com/watch?v={video_id}"


def _extract_youtube_relationship_urls(entity: Any) -> list[str]:
    if not isinstance(entity, dict):
        return []
    relation_lists = []
    for key in ("url-relation-list", "relation-list"):
        value = entity.get(key)
        if isinstance(value, list):
            relation_lists.extend([entry for entry in value if isinstance(entry, dict)])

    urls: list[str] = []
    seen = set()
    for rel in relation_lists:
        target = str(rel.get("target") or "").strip()
        normalized_url = _canonicalize_youtube_watch_url(target)
        if not normalized_url:
            continue
        rel_type = str(rel.get("type") or "").strip().lower()
        rel_attrs = rel.get("attribute-list")
        rel_attrs_lower = {
            str(attr or "").strip().lower()
            for attr in (rel_attrs if isinstance(rel_attrs, list) else [])
            if str(attr or "").strip()
        }
        has_allowed_hint = any(hint in rel_type for hint in _MB_YOUTUBE_REL_ALLOWED_HINTS)
        # Keep this strict to authoritative media relationships only.
        if not has_allowed_hint and "official" not in rel_attrs_lower:
            continue
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        urls.append(normalized_url)
    return urls


def _collect_mb_youtube_urls(*entities: Any, max_urls: int = 3) -> list[str]:
    urls: list[str] = []
    seen = set()
    for entity in entities:
        for url in _extract_youtube_relationship_urls(entity):
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
            if len(urls) >= max(1, int(max_urls or 3)):
                return urls
    return urls


def _artist_credit_string(value: Any) -> str:
    credits = value if isinstance(value, list) else []
    parts: list[str] = []
    for entry in credits:
        if isinstance(entry, str):
            parts.append(entry)
            continue
        if not isinstance(entry, dict):
            continue
        artist_obj = entry.get("artist") if isinstance(entry.get("artist"), dict) else {}
        name = str(entry.get("name") or artist_obj.get("name") or "").strip()
        joinphrase = str(entry.get("joinphrase") or "").strip()
        if name:
            parts.append(name)
        if joinphrase:
            parts.append(joinphrase)
    return "".join(parts).strip()


def _extract_release_year(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) >= 4 and text[:4].isdigit():
        return text[:4]
    return None


def _classify_release_bucket(release_payload: dict) -> str:
    release = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
    if not isinstance(release, dict):
        return "excluded"
    release_group = release.get("release-group", {})
    if not isinstance(release_group, dict):
        return "excluded"

    primary_type = str(release_group.get("primary-type") or "").strip().lower()
    secondary_types_raw = release_group.get("secondary-type-list", [])
    secondary_types = {
        str(value or "").strip().lower()
        for value in (secondary_types_raw if isinstance(secondary_types_raw, list) else [])
        if str(value or "").strip()
    }

    if primary_type in {"album", "ep"}:
        if {"compilation", "retrospective"} & secondary_types:
            return "compilation"
        if {"live", "soundtrack", "remix"} & secondary_types:
            return "excluded"
        return "album"
    if primary_type == "single":
        return "single"
    return "excluded"


def _resolve_track_position(release_payload: dict[str, Any], recording_mbid: str) -> tuple[int | None, int | None]:
    release = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
    media = release.get("medium-list", []) if isinstance(release, dict) else []
    if not isinstance(media, list):
        return None, None
    for medium in media:
        if not isinstance(medium, dict):
            continue
        disc_number = _safe_int(medium.get("position"))
        track_list = medium.get("track-list", [])
        if not isinstance(track_list, list):
            continue
        for track in track_list:
            if not isinstance(track, dict):
                continue
            recording = track.get("recording") if isinstance(track.get("recording"), dict) else {}
            if str(recording.get("id") or "").strip() != recording_mbid:
                continue
            track_number = _safe_int(track.get("position"))
            return track_number, disc_number
    return None, None


def _resolve_track_context(
    release_payload: dict[str, Any],
    recording_mbid: str,
) -> tuple[int | None, int | None, dict[str, Any]]:
    release = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
    media = release.get("medium-list", []) if isinstance(release, dict) else []
    if not isinstance(media, list):
        return None, None, {}
    for medium in media:
        if not isinstance(medium, dict):
            continue
        disc_number = _safe_int(medium.get("position"))
        track_list = medium.get("track-list", [])
        if not isinstance(track_list, list):
            continue
        for track in track_list:
            if not isinstance(track, dict):
                continue
            recording = track.get("recording") if isinstance(track.get("recording"), dict) else {}
            if str(recording.get("id") or "").strip() != recording_mbid:
                continue
            track_number = _safe_int(track.get("position"))
            return track_number, disc_number, track
    return None, None, {}


def _collect_mb_title_aliases(*values: Any) -> list[str]:
    aliases: list[str] = []
    seen: set[str] = set()

    def _append(value: Any) -> None:
        text = str(value or "").strip()
        if not text:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        aliases.append(text)

    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            _append(value)
            continue
        if isinstance(value, dict):
            _append(value.get("name"))
            _append(value.get("sort-name"))
            _append(value.get("alias"))
            continue
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    _append(entry.get("name"))
                    _append(entry.get("sort-name"))
                    _append(entry.get("alias"))
                else:
                    _append(entry)
    return aliases


def search_music_metadata(artist=None, album=None, track=None, mode="auto", offset=0, limit=20):
    artist_value = str(artist or "").strip()
    album_value = str(album or "").strip()
    track_value = str(track or "").strip()
    mode_value = str(mode or "auto").strip().lower() or "auto"
    if mode_value not in {"auto", "artist", "album", "track"}:
        mode_value = "auto"

    limit_value = min(max(1, int(limit or 20)), 15)
    offset_value = max(0, int(offset or 0))

    resolved_mode = mode_value
    if artist_value and album_value and track_value:
        resolved_mode = "track"
        route_case = "artist_album_track"
    elif artist_value and track_value:
        resolved_mode = "track"
        route_case = "artist_track"
    elif artist_value and album_value:
        resolved_mode = "album"
        route_case = "artist_album"
    elif album_value:
        resolved_mode = "album"
        route_case = "album_only"
    elif track_value:
        resolved_mode = "track"
        route_case = "track_only"
    elif artist_value:
        resolved_mode = "artist"
        route_case = "artist_only"
    else:
        route_case = "empty"

    response = {
        "artists": [],
        "albums": [],
        "tracks": [],
        "mode_used": resolved_mode,
        "offset": offset_value,
        "limit": limit_value,
    }
    if route_case == "empty":
        return response

    mb_service = get_musicbrainz_service()

    def _mb_call(func):
        try:
            return mb_service._call_with_retry(func)  # noqa: SLF001
        except Exception:
            return None

    def _artist_credit_text(value: Any) -> str:
        if not isinstance(value, list):
            return ""
        parts: list[str] = []
        for entry in value:
            if isinstance(entry, str):
                parts.append(entry)
                continue
            if not isinstance(entry, dict):
                continue
            artist_obj = entry.get("artist") if isinstance(entry.get("artist"), dict) else {}
            name = str(entry.get("name") or artist_obj.get("name") or "").strip()
            joinphrase = str(entry.get("joinphrase") or "")
            if name:
                parts.append(name)
            if joinphrase:
                parts.append(joinphrase)
        return "".join(parts).strip()

    def _field(value: str) -> str:
        escaped = str(value or "").replace("\\", "\\\\").replace('"', '\\"')
        return f"\"{escaped}\""

    if resolved_mode == "artist":
        artist_query = f"artist:{_field(artist_value)}"
        logger.info(
            "[MUSICBRAINZ] metadata_search endpoint=search_artists query=%s",
            artist_query,
        )
        payload = _mb_call(
            lambda: musicbrainzngs.search_artists(
                query=artist_query,
                limit=limit_value,
                offset=offset_value,
            )
        )
        artist_list = payload.get("artist-list", []) if isinstance(payload, dict) else []
        for artist_item in artist_list[:limit_value]:
            if not isinstance(artist_item, dict):
                continue
            response["artists"].append(
                {
                    "artist_mbid": artist_item.get("id"),
                    "name": artist_item.get("name"),
                    "country": artist_item.get("country"),
                    "disambiguation": artist_item.get("disambiguation"),
                }
            )
        return response

    if resolved_mode == "album":
        primary_type_clause = "primarytype:(album OR ep)"
        if route_case == "artist_album":
            release_group_query = (
                f"artist:{_field(artist_value)} AND "
                f"release:{_field(album_value)} AND "
                f"{primary_type_clause}"
            )
        else:
            release_group_query = f"release:{_field(album_value)} AND {primary_type_clause}"
        logger.info(
            "[MUSICBRAINZ] metadata_search endpoint=search_release_groups query=%s",
            release_group_query,
        )
        payload = _mb_call(
            lambda: musicbrainzngs.search_release_groups(
                query=release_group_query,
                limit=limit_value,
                offset=offset_value,
            )
        )
        group_list = payload.get("release-group-list", []) if isinstance(payload, dict) else []
        for group in group_list[:limit_value]:
            if not isinstance(group, dict):
                continue
            response["albums"].append(
                {
                    "release_group_mbid": group.get("id"),
                    "release_mbid": None,
                    "title": group.get("title"),
                    "artist": _artist_credit_text(group.get("artist-credit")),
                    "release_year": _extract_release_year(group.get("first-release-date")),
                }
            )
        return response

    if resolved_mode == "track":
        if route_case == "artist_album_track":
            recording_query = (
                f"artist:{_field(artist_value)} AND "
                f"recording:{_field(track_value)} AND "
                f"release:{_field(album_value)}"
            )
        elif route_case == "artist_track":
            recording_query = (
                f"artist:{_field(artist_value)} AND "
                f"recording:{_field(track_value)}"
            )
        else:
            recording_query = f"recording:{_field(track_value)}"
        logger.info(
            "[MUSICBRAINZ] metadata_search endpoint=search_recordings query=%s",
            recording_query,
        )
        payload = _mb_call(
            lambda: musicbrainzngs.search_recordings(
                query=recording_query,
                limit=limit_value,
                offset=offset_value,
            )
        )
        recording_list = payload.get("recording-list", []) if isinstance(payload, dict) else []
        for recording in recording_list[:limit_value]:
            if not isinstance(recording, dict):
                continue
            recording_mbid = str(recording.get("id") or "").strip()
            track_title = str(recording.get("title") or "").strip()
            artist_name = _artist_credit_text(recording.get("artist-credit")) or None
            duration_ms = _safe_int(recording.get("length"))
            releases = recording.get("release-list", []) if isinstance(recording.get("release-list"), list) else []
            first_release = releases[0] if releases and isinstance(releases[0], dict) else {}
            release_mbid = first_release.get("id")
            release_group = first_release.get("release-group") if isinstance(first_release.get("release-group"), dict) else {}
            release_group_mbid = release_group.get("id")
            album_title = first_release.get("title")
            release_year = _extract_release_year(first_release.get("date"))
            response["tracks"].append(
                {
                    "recording_mbid": recording_mbid,
                    "release_mbid": release_mbid,
                    "release_group_mbid": release_group_mbid,
                    "artist": artist_name,
                    "track": track_title,
                    "album": album_title,
                    "release_year": release_year,
                    "track_number": None,
                    "disc_number": None,
                    "duration_ms": duration_ms,
                }
            )
        return response

    return response


def _collect_isrc(recording: dict[str, Any]) -> bool:
    isrcs = recording.get("isrcs")
    if isinstance(isrcs, list):
        return any(str(item or "").strip() for item in isrcs)
    return bool(str(recording.get("isrc") or "").strip())


def _is_variant_explicitly_requested(track: str, album: str | None) -> bool:
    combined = f"{track} {album or ''}".strip()
    return bool(_extract_variant_triggers(combined))


def resolve_best_mb_pair(
    mb_service: Any,
    *,
    artist: str | None,
    track: str,
    album: str | None = None,
    duration_ms: int | None = None,
    country_preference: str = "US",
    allow_non_album_fallback: bool = False,
    debug: bool = False,
    min_recording_score: float = 0.0,
    threshold: float = 0.78,
    max_duration_delta_ms: int | None = None,
) -> dict[str, Any] | None:
    resolve_best_mb_pair.last_failure_reasons = []
    failure_reasons: set[str] = set()
    bucket_counts: dict[str, int] = {"album": 0, "compilation": 0, "single": 0, "excluded": 0}
    rejected_counts_by_reason: dict[str, int] = {}

    def _add_failure(reason: str) -> None:
        failure_reasons.add(reason)
        rejected_counts_by_reason[reason] = int(rejected_counts_by_reason.get(reason, 0)) + 1

    def _normalize_threshold(value: Any, *, default: float) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = default
        if parsed > 1.0:
            parsed = parsed / 100.0
        if parsed < 0.0:
            return 0.0
        if parsed > 1.0:
            return 1.0
        return parsed

    expected_artist = str(artist or "").strip()
    expected_track_raw = str(track or "").strip()
    expected_track_lookup = _normalize_title_for_mb_lookup(expected_track_raw)
    expected_track_for_score = _strip_neutral_title_phrases(expected_track_raw) or expected_track_raw
    expected_track = expected_track_raw
    expected_album = str(album or "").strip() or None
    prefer_country = str(country_preference or "").strip().upper() or None
    binding_threshold = _normalize_threshold(threshold, default=0.78)
    binding_threshold_score = binding_threshold * 100.0
    duration_delta_limit_ms = _safe_int(max_duration_delta_ms) or MAX_DURATION_DELTA_MS
    log_duration_reject_detail = bool(expected_album) or (max_duration_delta_ms is not None)

    if debug:
        logger.debug(
            {
                "message": "mb_title_normalized",
                "original": expected_track_raw,
                "normalized": expected_track_lookup,
                "scoring_normalized": expected_track_for_score,
            }
        )

    recordings_payload = mb_service.search_recordings(
        expected_artist or None,
        expected_track_lookup or expected_track_raw,
        album=expected_album,
        limit=5,
    )
    recording_list = []
    if isinstance(recordings_payload, dict):
        raw = recordings_payload.get("recording-list")
        if isinstance(raw, list):
            recording_list = [entry for entry in raw if isinstance(entry, dict)]
    ranked_recordings = sorted(recording_list, key=lambda rec: (-_score_value(rec), str(rec.get("id") or "")))
    if not ranked_recordings:
        _add_failure("no_recording_candidates")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons)
        logger.info(
            {
                "message": "mb_pair_selection_failed",
                "reasons": sorted(failure_reasons),
                "bucket_counts": bucket_counts,
                "rejected_counts_by_reason": rejected_counts_by_reason,
            }
        )
        return None

    variant_allowed = _is_variant_explicitly_requested(expected_track, expected_album)
    all_candidates: list[dict[str, Any]] = []
    saw_album_type = False

    for recording in ranked_recordings:
        recording_score = _score_value(recording)
        if recording_score < float(min_recording_score):
            _add_failure("recording_below_threshold")
            continue
        recording_mbid = str(recording.get("id") or "").strip()
        if not recording_mbid:
            _add_failure("recording_missing_mbid")
            continue
        recording_title = str(recording.get("title") or "").strip()
        recording_title_for_score = _strip_neutral_title_phrases(recording_title) or recording_title
        recording_artist = _artist_credit_string(recording.get("artist-credit"))
        recording_duration_ms = _safe_int(recording.get("length"))

        try:
            recording_payload = mb_service.get_recording(
                recording_mbid,
                includes=["releases", "artists", "isrcs", "aliases", "url-rels"],
            )
        except Exception as e:
            logger.error(
                {
                    "message": "mb_recording_fetch_failed",
                    "recording_mbid": recording_mbid,
                    "error": str(e),
                }
            )
            raise
        recording_data = recording_payload.get("recording", {}) if isinstance(recording_payload, dict) else {}
        release_items = recording_data.get("release-list", []) if isinstance(recording_data, dict) else []
        release_items = [entry for entry in release_items if isinstance(entry, dict) and str(entry.get("id") or "").strip()]
        if not release_items:
            _add_failure("no_release_candidates_for_recording")
            continue

        for release_item in release_items:
            release_id = str(release_item.get("id") or "").strip()
            if not release_id:
                continue
            release_payload = mb_service.get_release(
                release_id,
                includes=[
                    "release-groups",
                    "media",
                    "recordings",
                    "artists",
                    "aliases",
                    "url-rels",
                    "recording-rels",
                    "release-rels",
                ],
            )
            release = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
            if not isinstance(release, dict):
                _add_failure("invalid_release_payload")
                continue

            status = str(release.get("status") or release_item.get("status") or "").strip().lower()
            if status != "official":
                _add_failure("non_official_release")
                continue

            release_group = release.get("release-group")
            if not isinstance(release_group, dict):
                release_group = release_item.get("release-group") if isinstance(release_item.get("release-group"), dict) else {}
            bucket = _classify_release_bucket(release_payload)
            bucket_counts[bucket] = int(bucket_counts.get(bucket, 0)) + 1
            if bucket == "excluded":
                _add_failure("invalid_release_type")
                continue
            if bucket == "album":
                saw_album_type = True

            track_number, disc_number = _resolve_track_position(release_payload, recording_mbid)
            if track_number is None or disc_number is None:
                _add_failure("track_not_found_in_release")
                continue

            release_title = str(release.get("title") or release_item.get("title") or "").strip()
            release_date = str(release.get("date") or release_item.get("date") or "").strip() or None
            release_year = _extract_release_year(release_date)
            release_group_id = str(release_group.get("id") or "").strip() or None
            country = str(release.get("country") or release_item.get("country") or "").strip().upper() or None

            _, _, matched_track = _resolve_track_context(release_payload, recording_mbid)
            recording_disambiguation = str(recording_data.get("disambiguation") or recording.get("disambiguation") or "").strip() or None
            track_disambiguation = str(matched_track.get("disambiguation") or "").strip() or None
            release_disambiguation = str(release.get("disambiguation") or release_item.get("disambiguation") or "").strip() or None
            title_aliases = _collect_mb_title_aliases(
                recording_title,
                recording_data.get("alias-list"),
                recording_data.get("aliases"),
                matched_track.get("title"),
                matched_track.get("alias-list"),
                matched_track.get("aliases"),
                release.get("title"),
                release.get("alias-list"),
                release.get("aliases"),
                release_item.get("title"),
                release_item.get("alias-list"),
                release_item.get("aliases"),
            )
            if recording_disambiguation:
                title_aliases.extend(
                    _collect_mb_title_aliases(
                        f"{recording_title} {recording_disambiguation}",
                        recording_disambiguation,
                    )
                )
            if track_disambiguation and matched_track.get("title"):
                title_aliases.extend(
                    _collect_mb_title_aliases(
                        f"{matched_track.get('title')} {track_disambiguation}",
                        track_disambiguation,
                    )
                )
            if release_disambiguation and release_title:
                title_aliases.extend(
                    _collect_mb_title_aliases(
                        f"{release_title} {release_disambiguation}",
                    )
                )
            release_item_disambiguation = str(release_item.get("disambiguation") or "").strip() or None
            if release_item_disambiguation and str(release_item.get("title") or "").strip():
                title_aliases.extend(
                    _collect_mb_title_aliases(
                        f"{release_item.get('title')} {release_item_disambiguation}",
                    )
                )
            title_aliases = _collect_mb_title_aliases(title_aliases)
            mb_youtube_urls = _collect_mb_youtube_urls(
                recording_data,
                matched_track,
                release,
                release_item,
                max_urls=3,
            )

            variant_triggers = _extract_variant_triggers(recording_title)
            if variant_triggers and not variant_allowed:
                _add_failure("disallowed_variant")
                logger.info(
                    {
                        "message": "mb_pair_rejected",
                        "recording_mbid": recording_mbid,
                        "release_mbid": release_id,
                        "reason": "disallowed_variant",
                        "variant_triggers": variant_triggers,
                        "normalized_title_for_scoring": recording_title_for_score,
                    }
                )
                if debug:
                    logger.debug(
                        {
                            "message": "mb_pair_rejected",
                            "recording_mbid": recording_mbid,
                            "release_mbid": release_id,
                            "reason": "disallowed_variant",
                            "variant_triggers": variant_triggers,
                            "normalized_title_for_scoring": recording_title_for_score,
                        }
                    )
                continue

            duration_delta_ms = None
            if duration_ms is not None and recording_duration_ms is not None:
                duration_delta_ms = abs(int(duration_ms) - int(recording_duration_ms))
            if recording_duration_ms is not None and recording_duration_ms < PREVIEW_REJECT_MS:
                if duration_ms is None or int(duration_ms) >= 60000:
                    _add_failure("preview_duration")
                    if debug:
                        logger.debug({"message": "mb_pair_rejected", "recording_mbid": recording_mbid, "release_mbid": release_id, "reason": "preview_duration"})
                    continue
            if _PREVIEW_RE.search(recording_title):
                _add_failure("preview_title")
                if debug:
                    logger.debug({"message": "mb_pair_rejected", "recording_mbid": recording_mbid, "release_mbid": release_id, "reason": "preview_title"})
                continue

            release_artist = _artist_credit_string(release.get("artist-credit"))
            candidate_artist = recording_artist or release_artist
            if expected_artist:
                artist_similarity = _token_similarity(expected_artist, candidate_artist)
                if not candidate_artist:
                    artist_similarity = max(0.6, recording_score)
            else:
                artist_similarity = 1.0
            title_similarity = _token_similarity(expected_track_for_score, recording_title_for_score or expected_track_for_score)
            album_similarity = _token_similarity(expected_album, release_title) if expected_album else 0.0
            if expected_album and bucket == "compilation" and album_similarity < 0.40:
                _add_failure("compilation_album_mismatch")
                if debug:
                    logger.debug(
                        {
                            "message": "mb_pair_rejected",
                            "recording_mbid": recording_mbid,
                            "release_mbid": release_id,
                            "reason": "compilation_album_mismatch",
                            "album_similarity": album_similarity,
                        }
                    )
                continue
            if duration_ms is not None and recording_duration_ms is not None:
                duration_similarity = max(
                    0.0,
                    1.0 - (float(duration_delta_ms or 0) / float(duration_delta_limit_ms)),
                )
            else:
                duration_similarity = 0.5

            correctness = (
                artist_similarity * 40.0
                + title_similarity * 30.0
                + duration_similarity * 20.0
                + album_similarity * 10.0
            )

            completeness = 0.0
            if release_group_id:
                completeness += 18.0
            if release_date:
                completeness += 14.0
            if track_number and disc_number:
                completeness += 20.0
            if release_title:
                completeness += 18.0
            if bool(release.get("label-info-list")):
                completeness += 8.0
            if bool(str(release.get("barcode") or "").strip()):
                completeness += 6.0
            if _collect_isrc(recording):
                completeness += 8.0
            if status == "official":
                completeness += 8.0

            country_bonus = 6.0 if (prefer_country and country == prefer_country) else 0.0
            total = correctness * CORRECTNESS_WEIGHT + completeness * COMPLETENESS_WEIGHT + country_bonus

            if duration_delta_ms is not None and duration_delta_ms > duration_delta_limit_ms:
                _add_failure("duration_delta_gt_limit")
                if log_duration_reject_detail:
                    logger.info(
                        {
                            "message": "duration_reject_detail",
                            "recording_mbid": recording_mbid,
                            "mb_duration_ms": duration_ms,
                            "candidate_duration_ms": recording_duration_ms,
                            "delta_ms": duration_delta_ms,
                            "limit_ms": duration_delta_limit_ms,
                        }
                    )
                if debug:
                    logger.debug(
                        {
                            "message": "mb_pair_rejected",
                            "recording_mbid": recording_mbid,
                            "release_mbid": release_id,
                            "reason": "duration_delta_gt_limit",
                            "duration_delta_ms": duration_delta_ms,
                            "duration_delta_limit_ms": duration_delta_limit_ms,
                            "normalized_title_for_scoring": recording_title_for_score,
                        }
                    )
                continue

            candidate = {
                "recording_mbid": recording_mbid,
                "mb_release_id": release_id,
                "mb_release_group_id": release_group_id,
                "album": release_title or None,
                "release_date": release_date or release_year,
                "track_number": int(track_number),
                "disc_number": int(disc_number),
                "duration_ms": recording_duration_ms,
                "country": country,
                "release_year": int(release_year) if release_year and release_year.isdigit() else 9999,
                "correctness": correctness,
                "completeness": completeness,
                "country_bonus": country_bonus,
                "total": total,
                "bucket": bucket,
                "duration_delta_ms": duration_delta_ms if duration_delta_ms is not None else 999999999,
                "mb_recording_title": recording_title or None,
                "track_disambiguation": recording_disambiguation or track_disambiguation,
                "track_aliases": title_aliases,
                "mb_youtube_urls": mb_youtube_urls,
            }
            if debug:
                logger.debug(
                    {
                        "message": "mb_pair_candidate_scored",
                        "recording_mbid": recording_mbid,
                        "release_mbid": release_id,
                        "release_group_mbid": release_group_id,
                        "correctness": round(correctness, 4),
                        "completeness": round(completeness, 4),
                        "country_bonus": round(country_bonus, 4),
                        "total": round(total, 4),
                        "duration_delta_ms": duration_delta_ms,
                        "country": country,
                        "album": release_title,
                        "expected_track_scoring_normalized": expected_track_for_score,
                        "candidate_track_scoring_normalized": recording_title_for_score,
                    }
                )
            all_candidates.append(candidate)

    if not all_candidates:
        if not saw_album_type:
            _add_failure("no_official_album")
        fail_reasons = sorted(failure_reasons or {"no_valid_release_for_recording"})
        if not failure_reasons:
            rejected_counts_by_reason["no_valid_release_for_recording"] = int(
                rejected_counts_by_reason.get("no_valid_release_for_recording", 0)
            ) + 1
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons or {"no_valid_release_for_recording"})
        logger.info(
            {
                "message": "mb_pair_selection_failed",
                "reasons": fail_reasons,
                "bucket_counts": bucket_counts,
                "rejected_counts_by_reason": rejected_counts_by_reason,
            }
        )
        return None

    eligible = [c for c in all_candidates if float(c.get("correctness") or 0.0) >= CORRECTNESS_FLOOR]
    if not eligible:
        _add_failure("correctness_below_floor")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons)
        logger.info(
            {
                "message": "mb_pair_selection_failed",
                "reasons": sorted(failure_reasons),
                "bucket_counts": bucket_counts,
                "rejected_counts_by_reason": rejected_counts_by_reason,
            }
        )
        return None

    for candidate in eligible:
        raw_total = float(candidate.get("total") or 0.0)
        bucket = str(candidate.get("bucket") or "excluded")
        bucket_multiplier = BUCKET_MULTIPLIERS.get(bucket, 1.0)
        candidate["bucket_multiplier"] = bucket_multiplier
        candidate["final_score"] = raw_total * bucket_multiplier

    album_compilation_candidates = [
        c for c in eligible
        if str(c.get("bucket") or "") in {"album", "compilation"}
    ]
    if album_compilation_candidates:
        final_pool = album_compilation_candidates
    else:
        _add_failure("no_album_or_compilation_candidate")
        final_pool = eligible

    if not final_pool:
        _add_failure("single_fallback_failed")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons)
        logger.info(
            {
                "message": "mb_pair_selection_failed",
                "reasons": sorted(failure_reasons),
                "bucket_counts": bucket_counts,
                "rejected_counts_by_reason": rejected_counts_by_reason,
            }
        )
        return None

    threshold_pool = [c for c in final_pool if float(c.get("final_score") or 0.0) >= binding_threshold_score]
    if not threshold_pool:
        _add_failure("mb_binding_below_threshold")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons)
        logger.info(
            {
                "message": "mb_pair_selection_failed",
                "reasons": sorted(failure_reasons),
                "bucket_counts": bucket_counts,
                "rejected_counts_by_reason": rejected_counts_by_reason,
            }
        )
        return None

    ranked = sorted(
        threshold_pool,
        key=lambda c: (
            -float(c.get("final_score") or 0.0),
            int(c.get("duration_delta_ms") or 999999999),
            0 if str(c.get("country") or "").upper() == "US" else 1,
            int(c.get("release_year") or 9999),
            str(c.get("mb_release_id") or ""),
            str(c.get("recording_mbid") or ""),
        ),
    )
    selected = ranked[0]
    logger.info(
        {
            "message": "mb_pair_selected",
            "recording_mbid": selected.get("recording_mbid"),
            "release_mbid": selected.get("mb_release_id"),
            "release_group_mbid": selected.get("mb_release_group_id"),
            "country": selected.get("country"),
            "release_year": selected.get("release_year"),
            "track_number": selected.get("track_number"),
            "disc_number": selected.get("disc_number"),
            "album": selected.get("album"),
            "bucket": selected.get("bucket"),
            "bucket_multiplier": selected.get("bucket_multiplier"),
            "mb_youtube_urls": selected.get("mb_youtube_urls") or [],
        }
    )
    resolve_best_mb_pair.last_failure_reasons = []
    return {
        "recording_mbid": selected.get("recording_mbid"),
        "mb_release_id": selected.get("mb_release_id"),
        "mb_release_group_id": selected.get("mb_release_group_id"),
        "album": selected.get("album"),
        "release_date": selected.get("release_date"),
        "track_number": selected.get("track_number"),
        "disc_number": selected.get("disc_number"),
        "duration_ms": selected.get("duration_ms"),
        "mb_recording_title": selected.get("mb_recording_title"),
        "track_disambiguation": selected.get("track_disambiguation"),
        "track_aliases": selected.get("track_aliases"),
        "mb_youtube_urls": selected.get("mb_youtube_urls") or [],
    }
