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
    r"[\(\[\{][^\)\]\}]*?(official|lyrics|audio|video|visualizer)[^\)\]\}]*?[\)\]\}]",
    re.IGNORECASE,
)
_FEAT_RE = re.compile(r"\b(feat\.?|ft\.?|featuring)\b", re.IGNORECASE)

_BANNED_TOKENS = {"cover", "tribute", "karaoke", "reaction", "8d", "nightcore", "slowed"}


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
    # Deterministic ordering for equal scores:
    # (-score, source, candidate_id)
    ranked = sorted(
        scored_candidates,
        key=lambda item: (
            -float(item.get("final_score", 0.0)),
            str(item.get("source") or ""),
            str(item.get("candidate_id") or ""),
        ),
    )
    results = []
    for rank, item in enumerate(ranked, start=1):
        item["rank"] = rank
        results.append(item)
    return results


def select_best_candidate(scored_candidates, min_score):
    for candidate in scored_candidates:
        if candidate.get("final_score", 0.0) >= min_score:
            return candidate
    return None
