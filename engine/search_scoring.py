import re
import unicodedata

_WEIGHTS = {
    "artist": 0.30,
    "track": 0.35,
    "album": 0.15,
    "duration": 0.15,
    "bonus": 0.05,
}

_BRACKET_JUNK_RE = re.compile(
    r"[\(\[\{][^\)\]\}]*?"
    r"(official|official\s+audio|official\s+music\s+video|official\s+video|"
    r"lyrics?|lyric\s+video|audio|video|visualizer|hd|4k|topic)"
    r"[^\)\]\}]*?[\)\]\}]",
    re.IGNORECASE,
)
_TOPIC_SUFFIX_RE = re.compile(r"\s*-\s*topic\s*$", re.IGNORECASE)
_FEAT_RE = re.compile(r"\b(feat\.?|ft\.?|featuring)\b", re.IGNORECASE)

_BANNED_TOKENS = {"cover", "tribute", "karaoke", "reaction", "8d", "nightcore", "slowed"}

_MUSIC_SOURCE_MULTIPLIERS = {
    "youtube_music": 1.06,
    "youtube": 1.00,
    "soundcloud": 0.94,
    "bandcamp": 0.90,
}

_MUSIC_REJECT_PATTERNS = (
    (
        "disallowed_variant",
        re.compile(
            r"\b("
            r"live|acoustic|instrumental|karaoke|cover|tribute|remix|extended\s+mix|"
            r"sped\s*up|slowed(?:\s+down)?|nightcore|stripped|radio\s+edit"
            r")\b",
            re.IGNORECASE,
        ),
    ),
    ("preview_variant", re.compile(r"\b(preview|snippet|teaser|short\s+version)\b", re.IGNORECASE)),
    ("session_variant", re.compile(r"\b(cmt\s*\d+\s*sessions?)\b", re.IGNORECASE)),
)

_MUSIC_VARIANT_TERMS = ("live", "acoustic", "stripped", "radio edit", "karaoke", "instrumental")

_MUSIC_NOISE_PATTERNS = (
    ("official_video_noise", re.compile(r"\b(official\s+video|music\s+video|visualizer|lyric\s+video)\b", re.IGNORECASE), 8.0),
    ("remaster_noise", re.compile(r"\bremaster(?:ed)?(?:\s+\d{2,4})?\b", re.IGNORECASE), 5.0),
    ("session_noise", re.compile(r"\bcmt\s*\d+\s*sessions?\b", re.IGNORECASE), 7.0),
)


def _music_query_variant_flags(query):
    query_lower = str(query or "").lower()
    return {term for term in _MUSIC_VARIANT_TERMS if term in query_lower}


def _music_duration_points(expected_sec, candidate_sec, *, max_delta_ms=12000, hard_cap_ms=35000):
    if expected_sec is None or candidate_sec is None:
        return 0.0, "duration_missing", None
    try:
        expected_ms = int(expected_sec) * 1000
        candidate_ms = int(candidate_sec) * 1000
    except Exception:
        return 0.0, "duration_unparseable", None
    delta_ms = abs(candidate_ms - expected_ms)
    if candidate_ms < max(45000, int(expected_ms * 0.45)):
        return 0.0, "preview_duration", delta_ms
    if delta_ms > int(hard_cap_ms):
        return 0.0, "duration_over_hard_cap", delta_ms
    if delta_ms > int(max_delta_ms):
        return 0.0, "duration_out_of_bounds", delta_ms
    if delta_ms <= 2000:
        return 20.0, None, delta_ms
    if delta_ms <= 5000:
        return 16.0, None, delta_ms
    if delta_ms <= 8000:
        return 10.0, None, delta_ms
    return 4.0, None, delta_ms


def _music_source_multiplier(source):
    return _MUSIC_SOURCE_MULTIPLIERS.get(str(source or "").lower(), 1.0)


def _music_source_authority_points(expected, candidate):
    source = str(candidate.get("source") or "").lower()
    title_lower = str(candidate.get("title") or "").lower()
    uploader_lower = str(candidate.get("uploader") or candidate.get("artist_detected") or "").lower()
    official = bool(candidate.get("official"))
    signal = 0.25
    if source in {"youtube_music", "youtube"} and ("topic" in uploader_lower or "provided to youtube" in title_lower):
        signal = 0.50
    elif official:
        signal = 0.40
    elif source == "bandcamp":
        signal = 0.35
    elif source == "soundcloud":
        signal = 0.30
    authority_points = 8.0 * signal

    expected_artist_tokens = tokenize(expected.get("artist"))
    uploader_tokens = tokenize(candidate.get("uploader") or candidate.get("artist_detected"))
    uploader_artist_overlap = token_overlap_score(expected_artist_tokens, uploader_tokens)

    channel_authority_bonus = 0.0
    if source in {"youtube", "youtube_music"}:
        strong_artist_match = uploader_artist_overlap >= 0.80
        topic_channel = "topic" in uploader_lower
        official_channel = official or ("official artist channel" in uploader_lower)
        if strong_artist_match and (topic_channel or official_channel):
            channel_authority_bonus = 4.0
        elif uploader_artist_overlap >= 0.65 and official_channel:
            channel_authority_bonus = 2.0

    return min(12.0, authority_points + channel_authority_bonus)


def _music_reject_reason(expected, candidate):
    query_flags = _music_query_variant_flags(expected.get("query"))
    title = str(candidate.get("title") or "")
    uploader = str(candidate.get("uploader") or candidate.get("artist_detected") or "")
    haystack = f"{title} {uploader}".strip()
    for reason, pattern in _MUSIC_REJECT_PATTERNS:
        match = pattern.search(haystack)
        if not match:
            continue
        token = str(match.group(1) or "").lower().strip()
        if reason == "disallowed_variant" and token in query_flags:
            continue
        return reason
    return None


def clamp01(value):
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return float(value)


def normalize_text(value):
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = text.lower().strip()
    text = _FEAT_RE.sub("feat", text)
    text = _TOPIC_SUFFIX_RE.sub("", text)
    text = _BRACKET_JUNK_RE.sub(" ", text)
    text = re.sub(r"[^\w\s/&]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(value):
    normalized = normalize_text(value)
    if not normalized:
        return []
    return normalized.split()


def token_overlap_score(expected_tokens, candidate_tokens):
    if not expected_tokens or not candidate_tokens:
        return 0.0
    expected = set(expected_tokens)
    candidate = set(candidate_tokens)
    if not expected:
        return 0.0
    return len(expected & candidate) / len(expected)


def duration_score(expected_sec, candidate_sec):
    if expected_sec is None or candidate_sec is None:
        return 0.60
    try:
        delta = abs(int(expected_sec) - int(candidate_sec))
    except Exception:
        return 0.60
    if delta <= 2:
        return 1.00
    if delta <= 5:
        return 0.90
    if delta <= 10:
        return 0.75
    if delta <= 20:
        return 0.50
    return 0.20


def _has_remaster(tokens):
    return any(token.startswith("remaster") for token in tokens)


def _has_live(tokens):
    return "live" in tokens


def _has_banned(tokens):
    return any(token in _BANNED_TOKENS for token in tokens)


def penalty_multiplier(expected_track_tokens, candidate_track_tokens, artist_score):
    penalty = 1.0
    if _has_banned(candidate_track_tokens):
        penalty *= 0.10
    if _has_live(expected_track_tokens) != _has_live(candidate_track_tokens):
        penalty *= 0.85
    if _has_remaster(expected_track_tokens) != _has_remaster(candidate_track_tokens):
        penalty *= 0.92
    if artist_score == 0.0:
        penalty *= 0.50
    return penalty




def _canonical_bonus(expected, candidate):
    canonical = candidate.get("canonical_metadata") or candidate.get("canonical")
    if not isinstance(canonical, dict):
        return 0.0

    signals = []

    canonical_artist = canonical.get("artist")
    if canonical_artist:
        artist_score = token_overlap_score(tokenize(canonical_artist), tokenize(candidate.get("artist_detected") or candidate.get("uploader")))
        if artist_score > 0:
            signals.append(artist_score)

    canonical_duration = canonical.get("duration_sec")
    if canonical_duration is None:
        canonical_duration = canonical.get("duration")
    if canonical_duration is not None and candidate.get("duration_sec") is not None:
        dur_score = duration_score(canonical_duration, candidate.get("duration_sec"))
        if dur_score > 0.60:
            signals.append(dur_score)

    canonical_ids = canonical.get("external_ids") or {}
    canonical_isrc = canonical_ids.get("isrc")
    candidate_isrc = candidate.get("isrc")
    if canonical_isrc and candidate_isrc and str(canonical_isrc).upper() == str(candidate_isrc).upper():
        signals.append(1.0)

    canonical_track_count = canonical.get("track_count") or canonical.get("album_track_count")
    candidate_track_count = candidate.get("track_count")
    if canonical_track_count and candidate_track_count:
        try:
            if int(canonical_track_count) == int(candidate_track_count):
                signals.append(1.0)
        except Exception:
            pass

    if not signals:
        return 0.0
    return clamp01(sum(signals) / len(signals))


def score_candidate(expected, candidate, *, source_modifier=1.0):
    if str(expected.get("media_intent") or "").strip().lower() == "music_track":
        expected_artist = tokenize(expected.get("artist"))
        expected_track = tokenize(expected.get("track"))
        expected_album = tokenize(expected.get("album"))

        candidate_artist = tokenize(candidate.get("artist_detected") or candidate.get("uploader"))
        candidate_track = tokenize(candidate.get("track_detected") or candidate.get("title"))
        candidate_album = tokenize(candidate.get("album_detected"))

        artist_overlap = token_overlap_score(expected_artist, candidate_artist)
        track_overlap = token_overlap_score(expected_track, candidate_track)
        album_overlap = token_overlap_score(expected_album, candidate_album) if expected_album else 0.0

        rejection_reason = _music_reject_reason(expected, candidate)
        duration_max_delta_ms = expected.get("duration_max_delta_ms")
        if duration_max_delta_ms is None:
            duration_max_delta_ms = 12000
        duration_hard_cap_ms = expected.get("duration_hard_cap_ms")
        if duration_hard_cap_ms is None:
            duration_hard_cap_ms = 35000
        duration_pts, duration_reject_reason, duration_delta_ms = _music_duration_points(
            expected.get("duration_hint_sec"),
            candidate.get("duration_sec"),
            max_delta_ms=duration_max_delta_ms,
            hard_cap_ms=duration_hard_cap_ms,
        )
        if duration_reject_reason == "duration_missing":
            duration_penalty = 12.0
        else:
            duration_penalty = 0.0
        if duration_reject_reason in {"preview_duration", "duration_out_of_bounds", "duration_over_hard_cap"}:
            rejection_reason = duration_reject_reason

        if expected_album:
            track_pts = 30.0 * track_overlap
            artist_pts = 24.0 * artist_overlap
            album_pts = 18.0 * album_overlap
        else:
            track_pts = 39.0 * track_overlap
            artist_pts = 33.0 * artist_overlap
            album_pts = 0.0

        source_authority_pts = _music_source_authority_points(expected, candidate)
        authority_channel_match = source_authority_pts >= 8.0

        noise_penalty = 0.0
        penalty_reasons = []
        title = str(candidate.get("title") or "")
        title_lower = title.lower()
        for reason, pattern, points in _MUSIC_NOISE_PATTERNS:
            if pattern.search(title):
                noise_penalty += points
                penalty_reasons.append(reason)

        expected_has_feat = bool(_FEAT_RE.search(str(expected.get("track") or "")))
        candidate_has_feat = bool(_FEAT_RE.search(title))
        if candidate_has_feat and not expected_has_feat:
            noise_penalty += 4.0
            penalty_reasons.append("feat_mismatch_noise")

        if expected_album and album_overlap < 0.35:
            noise_penalty += 10.0
            penalty_reasons.append("album_mismatch_penalty")

        expected_artist_text = normalize_text(expected.get("artist"))
        candidate_artist_text = normalize_text(candidate.get("artist_detected") or candidate.get("uploader"))
        if (
            "cover" in title_lower
            and expected_artist_text
            and candidate_artist_text
            and candidate_artist_text != expected_artist_text
        ):
            rejection_reason = rejection_reason or "cover_artist_mismatch"

        floor_failed = (
            track_pts < 20.0
            or artist_pts < 15.0
            or (bool(expected_album) and album_pts < 8.0)
        )
        if floor_failed and not rejection_reason:
            rejection_reason = "floor_check_failed"

        raw_score_100 = (
            track_pts
            + artist_pts
            + album_pts
            + duration_pts
            + source_authority_pts
            - noise_penalty
            - duration_penalty
        )
        multiplier = _music_source_multiplier(candidate.get("source"))
        final_score_100 = max(0.0, min(100.0, raw_score_100 * multiplier))
        final_score = final_score_100 / 100.0

        return {
            "score_artist": artist_overlap,
            "score_track": track_overlap,
            "score_album": album_overlap if expected_album else 0.0,
            "score_duration": (duration_pts / 20.0) if duration_pts > 0 else 0.0,
            "source_modifier": source_modifier,
            "penalty_multiplier": 1.0,
            "final_score": final_score,
            "duration_delta_ms": duration_delta_ms,
            "rejection_reason": rejection_reason,
            "authority_channel_match": authority_channel_match,
            "title_noise_score": noise_penalty,
            "score_breakdown": {
                "track_pts": track_pts,
                "artist_pts": artist_pts,
                "album_pts": album_pts,
                "duration_pts": duration_pts,
                "source_authority_pts": source_authority_pts,
                "noise_penalty": noise_penalty,
                "duration_penalty": duration_penalty,
                "source_multiplier": multiplier,
                "raw_score_100": raw_score_100,
                "final_score_100": final_score_100,
                "penalty_reasons": penalty_reasons,
            },
        }

    expected_artist = tokenize(expected.get("artist"))
    expected_track = tokenize(expected.get("track"))
    expected_album = tokenize(expected.get("album"))

    candidate_artist = tokenize(candidate.get("artist_detected") or candidate.get("uploader"))
    candidate_track = tokenize(candidate.get("track_detected") or candidate.get("title"))
    candidate_album = tokenize(candidate.get("album_detected"))

    artist_score = token_overlap_score(expected_artist, candidate_artist)
    track_score = token_overlap_score(expected_track, candidate_track)

    if expected_album:
        album_score = token_overlap_score(expected_album, candidate_album)
    else:
        album_score = 0.60

    duration = duration_score(expected.get("duration_hint_sec"), candidate.get("duration_sec"))

    bonus_score = _canonical_bonus(expected, candidate)
    weighted_sum = (
        _WEIGHTS["artist"] * artist_score
        + _WEIGHTS["track"] * track_score
        + _WEIGHTS["album"] * album_score
        + _WEIGHTS["duration"] * duration
        + _WEIGHTS["bonus"] * bonus_score
    )

    penalties = penalty_multiplier(expected_track, candidate_track, artist_score)
    final_score = clamp01(weighted_sum) * source_modifier * penalties

    return {
        "score_artist": artist_score,
        "score_track": track_score,
        "score_album": album_score,
        "score_duration": duration,
        "source_modifier": source_modifier,
        "penalty_multiplier": penalties,
        "final_score": final_score,
    }


def rank_candidates(scored_candidates, *, source_priority=None):
    source_priority = source_priority or []
    source_rank = {str(src): idx for idx, src in enumerate(source_priority)}

    def _to_float(value, default):
        try:
            return float(value)
        except Exception:
            return default

    # Deterministic ordering for equal/near-equal scores:
    # (-score, source_priority_rank, duration_delta_ms, title_noise_score, source, candidate_id)
    ranked = sorted(
        scored_candidates,
        key=lambda item: (
            -float(item.get("final_score", 0.0)),
            source_rank.get(str(item.get("source") or ""), len(source_rank) + 1000),
            _to_float(item.get("duration_delta_ms"), float("inf")),
            _to_float(
                item.get("title_noise_score"),
                _to_float((item.get("score_breakdown") or {}).get("noise_penalty"), float("inf")),
            ),
            str(item.get("source") or ""),
            str(item.get("candidate_id") or ""),
        ),
    )
    results = []
    for rank, item in enumerate(ranked, start=1):
        item["rank"] = rank
        results.append(item)
    return results


def select_best_candidate(scored_candidates, min_score, *, source_priority=None):
    eligible = [
        candidate
        for candidate in (scored_candidates or [])
        if float(candidate.get("final_score", 0.0)) >= float(min_score)
    ]
    if not eligible:
        return None
    ranked = rank_candidates(eligible, source_priority=source_priority)
    return ranked[0] if ranked else None
