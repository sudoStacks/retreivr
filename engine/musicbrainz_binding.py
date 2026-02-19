from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any

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
_DISALLOWED_VARIANT_RE = re.compile(
    r"\b(live|acoustic|stripped|cover|karaoke|instrumental|radio\s*edit)\b",
    re.IGNORECASE,
)
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

    if primary_type == "album":
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


def _collect_isrc(recording: dict[str, Any]) -> bool:
    isrcs = recording.get("isrcs")
    if isinstance(isrcs, list):
        return any(str(item or "").strip() for item in isrcs)
    return bool(str(recording.get("isrc") or "").strip())


def _is_variant_explicitly_requested(track: str, album: str | None) -> bool:
    combined = f"{track} {album or ''}".strip()
    return bool(_DISALLOWED_VARIANT_RE.search(combined))


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
) -> dict[str, Any] | None:
    resolve_best_mb_pair.last_failure_reasons = []
    failure_reasons: set[str] = set()
    bucket_counts: dict[str, int] = {"album": 0, "compilation": 0, "single": 0, "excluded": 0}
    expected_artist = str(artist or "").strip()
    expected_track_raw = str(track or "").strip()
    expected_track_lookup = _normalize_title_for_mb_lookup(expected_track_raw)
    expected_track = expected_track_raw
    expected_album = str(album or "").strip() or None
    prefer_country = str(country_preference or "").strip().upper() or None

    if debug:
        logger.debug(
            {
                "message": "mb_title_normalized",
                "original": expected_track_raw,
                "normalized": expected_track_lookup,
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
        failure_reasons.add("no_recording_candidates")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons)
        logger.info({"message": "mb_pair_selection_failed", "reasons": sorted(failure_reasons), "bucket_counts": bucket_counts})
        return None

    variant_allowed = _is_variant_explicitly_requested(expected_track, expected_album)
    all_candidates: list[dict[str, Any]] = []
    saw_album_type = False
    saw_country_match = False

    for recording in ranked_recordings:
        recording_score = _score_value(recording)
        if recording_score < float(min_recording_score):
            failure_reasons.add("recording_below_threshold")
            continue
        recording_mbid = str(recording.get("id") or "").strip()
        if not recording_mbid:
            failure_reasons.add("recording_missing_mbid")
            continue
        recording_title = str(recording.get("title") or "").strip()
        recording_artist = _artist_credit_string(recording.get("artist-credit"))
        recording_duration_ms = _safe_int(recording.get("length"))

        try:
            recording_payload = mb_service.get_recording(
                recording_mbid,
                includes=["releases", "artists", "isrcs"],
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
            failure_reasons.add("no_release_candidates_for_recording")
            continue

        for release_item in release_items:
            release_id = str(release_item.get("id") or "").strip()
            if not release_id:
                continue
            release_payload = mb_service.get_release(
                release_id,
                includes=["release-groups", "media", "recordings", "artists"],
            )
            release = release_payload.get("release", {}) if isinstance(release_payload, dict) else {}
            if not isinstance(release, dict):
                failure_reasons.add("invalid_release_payload")
                continue

            status = str(release.get("status") or release_item.get("status") or "").strip().lower()
            if status != "official":
                failure_reasons.add("non_official_release")
                continue

            release_group = release.get("release-group")
            if not isinstance(release_group, dict):
                release_group = release_item.get("release-group") if isinstance(release_item.get("release-group"), dict) else {}
            bucket = _classify_release_bucket(release_payload)
            bucket_counts[bucket] = int(bucket_counts.get(bucket, 0)) + 1
            if bucket == "excluded":
                failure_reasons.add("invalid_release_type")
                continue
            if bucket == "album":
                saw_album_type = True
            elif bucket == "single":
                if allow_non_album_fallback:
                    pass
                else:
                    failure_reasons.add("non_album_release_type")
                    continue

            track_number, disc_number = _resolve_track_position(release_payload, recording_mbid)
            if track_number is None or disc_number is None:
                failure_reasons.add("track_not_found_in_release")
                continue

            release_title = str(release.get("title") or release_item.get("title") or "").strip()
            release_date = str(release.get("date") or release_item.get("date") or "").strip() or None
            release_year = _extract_release_year(release_date)
            release_group_id = str(release_group.get("id") or "").strip() or None
            country = str(release.get("country") or release_item.get("country") or "").strip().upper() or None
            if prefer_country and country == prefer_country:
                saw_country_match = True

            if _DISALLOWED_VARIANT_RE.search(recording_title) and not variant_allowed:
                failure_reasons.add("disallowed_variant")
                if debug:
                    logger.debug({"message": "mb_pair_rejected", "recording_mbid": recording_mbid, "release_mbid": release_id, "reason": "disallowed_variant"})
                continue

            duration_delta_ms = None
            if duration_ms is not None and recording_duration_ms is not None:
                duration_delta_ms = abs(int(duration_ms) - int(recording_duration_ms))
                if duration_delta_ms > MAX_DURATION_DELTA_MS:
                    failure_reasons.add("duration_delta_gt_12s")
                    if debug:
                        logger.debug({"message": "mb_pair_rejected", "recording_mbid": recording_mbid, "release_mbid": release_id, "reason": "duration_delta_gt_12s", "duration_delta_ms": duration_delta_ms})
                    continue
            if recording_duration_ms is not None and recording_duration_ms < PREVIEW_REJECT_MS:
                if duration_ms is None or int(duration_ms) >= 60000:
                    failure_reasons.add("preview_duration")
                    if debug:
                        logger.debug({"message": "mb_pair_rejected", "recording_mbid": recording_mbid, "release_mbid": release_id, "reason": "preview_duration"})
                    continue
            if _PREVIEW_RE.search(recording_title):
                failure_reasons.add("preview_title")
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
            title_similarity = _token_similarity(expected_track, recording_title or expected_track)
            album_similarity = _token_similarity(expected_album, release_title) if expected_album else 0.0
            if expected_album and bucket == "compilation" and album_similarity < 0.40:
                failure_reasons.add("compilation_album_mismatch")
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
                duration_similarity = max(0.0, 1.0 - (float(duration_delta_ms or 0) / float(MAX_DURATION_DELTA_MS)))
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
                    }
                )
            all_candidates.append(candidate)

    if not all_candidates:
        if not saw_album_type:
            failure_reasons.add("no_official_album")
        if prefer_country and not saw_country_match:
            failure_reasons.add("no_us_release")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons or {"no_valid_release_for_recording"})
        logger.info(
            {
                "message": "mb_pair_selection_failed",
                "reasons": sorted(failure_reasons or {"no_valid_release_for_recording"}),
                "bucket_counts": bucket_counts,
            }
        )
        return None

    eligible = [c for c in all_candidates if float(c.get("correctness") or 0.0) >= CORRECTNESS_FLOOR]
    if not eligible:
        failure_reasons.add("correctness_below_floor")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons)
        logger.info({"message": "mb_pair_selection_failed", "reasons": sorted(failure_reasons), "bucket_counts": bucket_counts})
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
        failure_reasons.add("no_album_or_compilation_candidate")
        final_pool = eligible

    if not final_pool:
        failure_reasons.add("single_fallback_failed")
        resolve_best_mb_pair.last_failure_reasons = sorted(failure_reasons)
        logger.info({"message": "mb_pair_selection_failed", "reasons": sorted(failure_reasons), "bucket_counts": bucket_counts})
        return None

    ranked = sorted(
        final_pool,
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
    }
