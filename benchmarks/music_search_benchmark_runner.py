from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
import types
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_search_modules():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    title_mod = _load_module("engine.music_title_normalization", _ROOT / "engine" / "music_title_normalization.py")
    scoring_mod = _load_module("engine.search_scoring", _ROOT / "engine" / "search_scoring.py")
    return title_mod, scoring_mod


_TITLE_MOD, _SCORING_MOD = _load_search_modules()
relaxed_search_title = _TITLE_MOD.relaxed_search_title
rank_candidates = _SCORING_MOD.rank_candidates
score_candidate = _SCORING_MOD.score_candidate
classify_music_title_variants = _SCORING_MOD.classify_music_title_variants

MUSIC_TRACK_THRESHOLD = 0.78
STRICT_MAX_DELTA_MS = 12000
EXPANDED_MAX_DELTA_MS = 35000
HARD_CAP_MS = 35000
PASS_B_MIN_TITLE = 0.92
PASS_B_MIN_ARTIST = 0.92
ALBUM_COHERENCE_MAX_BOOST = 0.03
ALBUM_COHERENCE_TIE_WINDOW = 0.03
DISALLOWED_VARIANT_RE = re.compile(
    r"\b(live|acoustic|instrumental|karaoke|cover|tribute|remix|extended\s+mix|sped\s*up|slowed(?:\s+down)?|nightcore|stripped|radio\s+edit)\b",
    re.IGNORECASE,
)

INJECTED_REJECTION_KEYS = (
    "duration_fail",
    "title_similarity_fail",
    "artist_similarity_fail",
    "variant_blocked",
    "unavailable",
)


def _classify_injected_rejection_bucket(reason: str | None) -> str:
    value = str(reason or "").strip().lower()
    if value in {"duration_out_of_bounds", "duration_over_hard_cap", "preview_duration", "pass_b_duration"}:
        return "duration_fail"
    if value in {"disallowed_variant", "preview_variant", "session_variant", "cover_artist_mismatch"}:
        return "variant_blocked"
    if value in {"low_artist_similarity", "pass_b_artist_similarity", "pass_b_authority"}:
        return "artist_similarity_fail"
    if value in {"low_title_similarity", "floor_check_failed", "low_album_similarity", "pass_b_track_similarity"}:
        return "title_similarity_fail"
    if value.startswith("source_unavailable") or value.startswith("unavailable"):
        return "unavailable"
    return "title_similarity_fail"


def _empty_injected_rejection_mix() -> Counter[str]:
    return Counter({key: 0 for key in INJECTED_REJECTION_KEYS})


def _has_wrong_variant(title: str, *, allowed_tokens: set[str]) -> bool:
    for match in DISALLOWED_VARIANT_RE.finditer(str(title or "")):
        token = str(match.group(1) or "").strip().lower()
        if token and token not in allowed_tokens:
            return True
    return False


@dataclass
class TrackRunResult:
    album_id: str
    track_id: str
    resolved: bool
    selected_pass: str | None
    selected_rung: int | None
    selected_candidate_id: str | None
    selected_score: float | None
    wrong_variant_flag: bool
    failure_reason: str | None
    rejection_counts: dict[str, int]
    avg_candidate_score: float | None
    candidate_count: int
    expected_selected_candidate_id: str | None
    expected_match: bool
    retrieved_candidate_ids: list[str]
    decision_accepted_selection: dict[str, Any] | None
    decision_rejected_candidates: list[dict[str, Any]]
    decision_final_rejection: dict[str, Any] | None
    decision_candidate_variant_distribution: dict[str, int]
    decision_selected_candidate_variant_tags: list[str]
    decision_top_rejected_variant_tags: list[str]
    coherence_boost_applied: int
    coherence_selected_delta: float
    coherence_near_miss: bool
    mb_injected_selected: bool
    mb_injected_rejection_counts: dict[str, int]


def _build_ladder(artist: str, track: str, album: str | None) -> list[dict[str, Any]]:
    artist_v = str(artist or "").strip()
    track_v = str(track or "").strip()
    album_v = str(album or "").strip()
    relaxed_track = relaxed_search_title(track_v) or track_v
    ladder = [
        {
            "rung": 0,
            "label": "canonical_full",
            "query": " ".join(
                part
                for part in [f'"{artist_v}"', f'"{track_v}"', f'"{album_v}"' if album_v else ""]
                if part
            ).strip(),
        },
        {
            "rung": 1,
            "label": "canonical_no_album",
            "query": " ".join(part for part in [f'"{artist_v}"', f'"{track_v}"'] if part).strip(),
        },
        {
            "rung": 2,
            "label": "relaxed_no_album",
            "query": " ".join(part for part in [f'"{artist_v}"', f'"{relaxed_track}"'] if part).strip(),
        },
        {
            "rung": 3,
            "label": "official_audio_fallback",
            "query": " ".join(part for part in [artist_v, relaxed_track, "official audio"] if part).strip(),
        },
        {
            "rung": 4,
            "label": "legacy_topic_fallback",
            "query": " ".join(part for part in [artist_v, "-", track_v, "topic"] if part).strip(),
        },
        {
            "rung": 5,
            "label": "legacy_audio_fallback",
            "query": " ".join(part for part in [artist_v, "-", track_v, "audio"] if part).strip(),
        },
    ]
    seen = set()
    out = []
    for entry in ladder:
        query = str(entry.get("query") or "").strip()
        if not query or query in seen:
            continue
        seen.add(query)
        out.append(entry)
    return out


def _render_template(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        rendered = value
        for key, repl in context.items():
            rendered = rendered.replace("{{" + key + "}}", str(repl))
        return rendered
    if isinstance(value, list):
        return [_render_template(item, context) for item in value]
    if isinstance(value, dict):
        return {k: _render_template(v, context) for k, v in value.items()}
    return value


def _score_candidates(
    expected_base: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    duration_max_delta_ms: int,
) -> list[dict[str, Any]]:
    expected = dict(expected_base)
    expected["duration_max_delta_ms"] = int(duration_max_delta_ms)
    scored = []
    for candidate in candidates:
        item = dict(candidate)
        item.update(score_candidate(expected, item, source_modifier=1.0))
        scored.append(item)
    return rank_candidates(scored, source_priority=["youtube_music", "youtube", "soundcloud", "bandcamp"])


def _parse_raw_meta(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        loaded = json.loads(value)
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _candidate_authority_family(candidate: dict[str, Any]) -> str | None:
    source = str(candidate.get("source") or "").strip().lower() or "unknown"
    for key in ("channel_id", "uploader_id", "channel_url", "uploader_url"):
        value = str(candidate.get(key) or "").strip()
        if value:
            return f"{source}:{value.lower()}"
    raw_meta = _parse_raw_meta(candidate.get("raw_meta_json"))
    for key in ("channel_id", "uploader_id", "channel_url", "uploader_url"):
        value = str(raw_meta.get(key) or "").strip()
        if value:
            return f"{source}:{value.lower()}"
    uploader = str(candidate.get("uploader") or candidate.get("artist_detected") or "").strip().lower()
    if source in {"youtube", "youtube_music"} and uploader.endswith("- topic"):
        return f"{source}:topic"
    return None


def _apply_album_coherence_tiebreak(
    ranked: list[dict[str, Any]],
    *,
    family_counts: Counter[str] | None,
) -> tuple[list[dict[str, Any]], int]:
    if not ranked or not family_counts:
        return ranked, 0
    top_non_rejected = [
        float(item.get("final_score") or 0.0)
        for item in ranked
        if not item.get("rejection_reason")
    ]
    if not top_non_rejected:
        return ranked, 0
    top_score = max(top_non_rejected)
    max_family_count = max(family_counts.values()) if family_counts else 0
    if max_family_count <= 0:
        return ranked, 0

    boosted: list[dict[str, Any]] = []
    applied = 0
    for item in ranked:
        candidate = dict(item)
        candidate["coherence_delta"] = float(candidate.get("coherence_delta") or 0.0)
        base_score = float(candidate.get("final_score") or 0.0)
        candidate["base_final_score"] = base_score
        if candidate.get("rejection_reason"):
            boosted.append(candidate)
            continue
        if (top_score - base_score) > ALBUM_COHERENCE_TIE_WINDOW:
            boosted.append(candidate)
            continue
        family = _candidate_authority_family(candidate)
        if not family:
            boosted.append(candidate)
            continue
        count = int(family_counts.get(family) or 0)
        if count <= 0:
            boosted.append(candidate)
            continue
        delta = min(ALBUM_COHERENCE_MAX_BOOST, ALBUM_COHERENCE_MAX_BOOST * (count / float(max_family_count)))
        if delta <= 0:
            boosted.append(candidate)
            continue
        candidate["coherence_family"] = family
        candidate["coherence_delta"] = delta
        candidate["final_score"] = min(1.0, base_score + delta)
        applied += 1
        boosted.append(candidate)
    if applied <= 0:
        return ranked, 0
    return rank_candidates(boosted, source_priority=["youtube_music", "youtube", "soundcloud", "bandcamp"]), applied


def retrieve_candidates(ctx: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [dict(c) for c in (ctx.get("candidates") or []) if isinstance(c, dict)]
    rung = int(ctx.get("rung") or 0)
    first_rung = int(ctx.get("first_rung") or 0)
    injected = [dict(c) for c in (ctx.get("injected_candidates") or []) if isinstance(c, dict)]
    if rung != first_rung or not injected:
        return candidates
    deduped: list[dict[str, Any]] = []
    seen = set()
    for candidate in list(injected) + list(candidates):
        url = str(candidate.get("url") or "").strip().lower()
        candidate_id = str(candidate.get("candidate_id") or "").strip().lower()
        key = url or candidate_id or json.dumps(candidate, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _music_similarity_thresholds(expected_base: dict[str, Any]) -> dict[str, float]:
    has_album = bool(str(expected_base.get("album") or "").strip())
    if has_album:
        return {
            "title_similarity": (20.0 / 30.0),
            "artist_similarity": (15.0 / 24.0),
            "album_similarity": (8.0 / 18.0),
        }
    return {
        "title_similarity": (20.0 / 39.0),
        "artist_similarity": (15.0 / 33.0),
        "album_similarity": 0.0,
    }


def _build_candidate_observation(
    candidate: dict[str, Any],
    *,
    reason: str,
    expected_base: dict[str, Any],
    pass_name: str,
) -> dict[str, Any]:
    thresholds = _music_similarity_thresholds(expected_base)
    max_delta_ms = STRICT_MAX_DELTA_MS if pass_name == "strict" else EXPANDED_MAX_DELTA_MS
    hard_cap_ms = int(expected_base.get("duration_hard_cap_ms") or HARD_CAP_MS)
    title_similarity = max(
        float(candidate.get("score_track") or 0.0),
        min(float(candidate.get("score_track_relaxed") or 0.0) * 0.90, 1.0),
    )
    artist_similarity = float(candidate.get("score_artist") or 0.0)
    album_similarity = float(candidate.get("score_album") or 0.0)
    try:
        duration_delta_ms = int(candidate.get("duration_delta_ms")) if candidate.get("duration_delta_ms") is not None else None
    except Exception:
        duration_delta_ms = None
    final_score = float(candidate.get("final_score") or 0.0)
    reason_value = str(reason or "").strip().lower()
    gate = "score_threshold"
    metric: dict[str, Any] = {
        "name": "final_score",
        "value": final_score,
        "threshold": MUSIC_TRACK_THRESHOLD,
        "margin_to_pass": MUSIC_TRACK_THRESHOLD - final_score,
        "direction": ">=",
    }
    if reason_value in {"duration_out_of_bounds", "pass_b_duration"}:
        gate = "duration_delta_ms"
        metric = {
            "name": "duration_delta_ms",
            "value": duration_delta_ms,
            "threshold": max_delta_ms,
            "margin_to_pass": (float(duration_delta_ms) - float(max_delta_ms)) if duration_delta_ms is not None else None,
            "direction": "<=",
        }
    elif reason_value == "duration_over_hard_cap":
        gate = "duration_hard_cap_ms"
        metric = {
            "name": "duration_delta_ms",
            "value": duration_delta_ms,
            "threshold": hard_cap_ms,
            "margin_to_pass": (float(duration_delta_ms) - float(hard_cap_ms)) if duration_delta_ms is not None else None,
            "direction": "<=",
        }
    elif reason_value == "preview_duration":
        expected_sec = expected_base.get("duration_hint_sec")
        try:
            expected_ms = int(expected_sec) * 1000 if expected_sec is not None else None
            candidate_ms = int(candidate.get("duration_sec")) * 1000 if candidate.get("duration_sec") is not None else None
        except Exception:
            expected_ms, candidate_ms = None, None
        min_ms = max(45000, int(expected_ms * 0.45)) if expected_ms is not None else 45000
        gate = "preview_duration_min_ms"
        metric = {
            "name": "candidate_duration_ms",
            "value": candidate_ms,
            "threshold": min_ms,
            "margin_to_pass": (float(min_ms) - float(candidate_ms)) if candidate_ms is not None else None,
            "direction": ">=",
        }
    elif reason_value in {"low_title_similarity", "floor_check_failed", "pass_b_track_similarity"}:
        threshold = PASS_B_MIN_TITLE if reason_value == "pass_b_track_similarity" else thresholds["title_similarity"]
        gate = "title_similarity"
        metric = {
            "name": "title_similarity",
            "value": title_similarity,
            "threshold": threshold,
            "margin_to_pass": threshold - title_similarity,
            "direction": ">=",
        }
    elif reason_value in {"low_artist_similarity", "cover_artist_mismatch", "pass_b_artist_similarity"}:
        threshold = PASS_B_MIN_ARTIST if reason_value == "pass_b_artist_similarity" else thresholds["artist_similarity"]
        gate = "artist_similarity"
        metric = {
            "name": "artist_similarity",
            "value": artist_similarity,
            "threshold": threshold,
            "margin_to_pass": threshold - artist_similarity,
            "direction": ">=",
        }
    elif reason_value == "low_album_similarity":
        gate = "album_similarity"
        metric = {
            "name": "album_similarity",
            "value": album_similarity,
            "threshold": thresholds["album_similarity"],
            "margin_to_pass": thresholds["album_similarity"] - album_similarity,
            "direction": ">=",
        }
    elif reason_value in {"disallowed_variant", "preview_variant", "session_variant"}:
        gate = "variant_alignment"
        metric = {
            "name": "variant_alignment",
            "value": 0.0,
            "threshold": 1.0,
            "margin_to_pass": 1.0,
            "direction": "==",
        }
    elif reason_value == "pass_b_authority":
        authority_value = 1.0 if bool(candidate.get("authority_channel_match")) else 0.0
        gate = "authority_channel_match"
        metric = {
            "name": "authority_channel_match",
            "value": authority_value,
            "threshold": 1.0,
            "margin_to_pass": 1.0 - authority_value,
            "direction": "==",
        }
    return {
        "candidate_id": candidate.get("candidate_id"),
        "source": candidate.get("source"),
        "title": candidate.get("title"),
        "variant_tags": sorted(classify_music_title_variants(candidate.get("title"))),
        "rejection_reason": reason_value or "score_threshold",
        "top_failed_gate": gate,
        "nearest_pass_margin": metric,
        "final_score": final_score,
        "pass": pass_name,
    }


def _nearest_rejection(rejected_candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rejected_candidates:
        return None

    def _margin_value(entry: dict[str, Any]) -> float:
        metric = entry.get("nearest_pass_margin") if isinstance(entry.get("nearest_pass_margin"), dict) else {}
        value = metric.get("margin_to_pass")
        try:
            return abs(float(value))
        except Exception:
            return float("inf")

    ranked = sorted(
        rejected_candidates,
        key=lambda entry: (
            _margin_value(entry),
            -float(entry.get("final_score") or 0.0),
            str(entry.get("candidate_id") or ""),
        ),
    )
    return ranked[0] if ranked else None


def _collect_failure_motifs(track_results: list[TrackRunResult]) -> dict[str, int]:
    motifs: Counter[str] = Counter()
    risky_accept_tags = {
        "lyric_video",
        "live",
        "remaster",
        "radio_edit",
        "sped_up",
        "slowed",
        "nightcore",
        "8d",
        "extended",
        "edit",
        "cut",
    }
    for track in track_results:
        edge_accepted = track.decision_accepted_selection if isinstance(track.decision_accepted_selection, dict) else {}
        selected_tags = {str(tag or "").strip() for tag in (track.decision_selected_candidate_variant_tags or []) if str(tag or "").strip()}
        top_rejected_tags = {str(tag or "").strip() for tag in (track.decision_top_rejected_variant_tags or []) if str(tag or "").strip()}
        failure_reason = str(track.failure_reason or "").strip().lower()

        if bool(track.wrong_variant_flag) or bool(selected_tags & risky_accept_tags):
            motifs["wrong_variant_accept"] += 1
        artist_similarity = edge_accepted.get("top_supporting_features", {}).get("artist_similarity") if isinstance(edge_accepted.get("top_supporting_features"), dict) else None
        try:
            if track.resolved and artist_similarity is not None and float(artist_similarity) < 0.50:
                motifs["wrong_artist_accept"] += 1
        except Exception:
            pass
        if failure_reason == "duration_filtered":
            motifs["duration_drift"] += 1
        if failure_reason.startswith("source_unavailable:") or failure_reason.startswith("unavailable"):
            motifs["unavailable"] += 1
        if "lyric_video" in top_rejected_tags:
            motifs["lyric_video_rejection"] += 1
        if "remaster" in top_rejected_tags:
            motifs["remaster_rejection"] += 1
        if "live" in top_rejected_tags:
            motifs["live_rejection"] += 1
        if {"sped_up", "slowed", "nightcore", "8d"} & top_rejected_tags:
            motifs["tempo_fx_rejection"] += 1
    return dict(sorted(motifs.items(), key=lambda item: (-int(item[1]), item[0])))


def _top_rejected_variant_tags(rejected_candidates: list[dict[str, Any]]) -> list[str]:
    tag_counts: Counter[str] = Counter()
    for entry in rejected_candidates:
        tags = entry.get("variant_tags") if isinstance(entry.get("variant_tags"), list) else []
        for tag in tags:
            key = str(tag or "").strip()
            if key:
                tag_counts[key] += 1
    return [tag for tag, _count in sorted(tag_counts.items(), key=lambda item: (-int(item[1]), item[0]))]


def rank_and_gate(
    ctx: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    expected_base = dict(ctx.get("expected_base") or {})
    family_counts = ctx.get("family_counts")
    rejection_counts: Counter[str] = Counter()
    total_candidate_scores: list[float] = []
    coherence_near_miss = False
    mb_rejections: Counter[str] = _empty_injected_rejection_mix()
    coherence_boost_applied = 0
    failure_reason = "no_candidate_above_threshold"
    rejected_candidates: list[dict[str, Any]] = []
    candidate_variant_distribution: Counter[str] = Counter()
    for candidate in candidates:
        for tag in classify_music_title_variants(candidate.get("title")):
            candidate_variant_distribution[str(tag)] += 1

    ranked_a = _score_candidates(expected_base, candidates, duration_max_delta_ms=STRICT_MAX_DELTA_MS)
    ranked_a, applied_a = _apply_album_coherence_tiebreak(
        ranked_a,
        family_counts=family_counts,
    )
    coherence_boost_applied += applied_a
    for candidate in ranked_a:
        reason = str(candidate.get("rejection_reason") or "").strip()
        if reason:
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason=reason,
                    expected_base=expected_base,
                    pass_name="strict",
                )
            )
        elif float(candidate.get("final_score", 0.0)) < MUSIC_TRACK_THRESHOLD:
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason="score_threshold",
                    expected_base=expected_base,
                    pass_name="strict",
                )
            )
        if candidate.get("final_score") is not None:
            total_candidate_scores.append(float(candidate.get("final_score") or 0.0))
        if reason:
            rejection_counts[reason] += 1
            if reason == "floor_check_failed" and float(candidate.get("score_track") or 0.0) < 0.70:
                rejection_counts["low_title_similarity"] += 1
        if reason and str(candidate.get("source") or "").strip().lower() == "mb_relationship":
            mb_rejections[_classify_injected_rejection_bucket(reason)] += 1
        if (
            not reason
            and (MUSIC_TRACK_THRESHOLD - ALBUM_COHERENCE_MAX_BOOST)
            <= float(candidate.get("final_score") or 0.0)
            < MUSIC_TRACK_THRESHOLD
        ):
            coherence_near_miss = True
    eligible_a = [
        c for c in ranked_a
        if not c.get("rejection_reason")
        and float(c.get("final_score", 0.0)) >= MUSIC_TRACK_THRESHOLD
    ]
    if eligible_a:
        selected = eligible_a[0]
        selected_score = float(selected.get("final_score") or 0.0)
        runner_up_score = None
        for item in ranked_a:
            if item.get("candidate_id") == selected.get("candidate_id"):
                continue
            if item.get("rejection_reason"):
                continue
            runner_up_score = float(item.get("final_score") or 0.0)
            break
        title_similarity = max(
            float(selected.get("score_track") or 0.0),
            min(float(selected.get("score_track_relaxed") or 0.0) * 0.90, 1.0),
        )
        return {
            "selected": selected,
            "selected_pass": "strict",
            "selected_score": selected_score,
            "failure_reason": None,
            "rejection_counts": dict(rejection_counts),
            "total_candidate_scores": total_candidate_scores,
            "coherence_boost_applied": coherence_boost_applied,
            "coherence_near_miss": coherence_near_miss,
            "mb_injected_rejection_counts": dict((key, int(mb_rejections.get(key) or 0)) for key in INJECTED_REJECTION_KEYS),
            "decision_rejected_candidates": rejected_candidates,
            "decision_accepted_selection": {
                "selected_candidate_id": selected.get("candidate_id"),
                "selected_score": selected_score,
                "runner_up_score": runner_up_score,
                "runner_up_gap": selected_score - float(runner_up_score or 0.0),
                "top_supporting_features": {
                    "duration_delta_ms": selected.get("duration_delta_ms"),
                    "title_similarity": title_similarity,
                    "artist_similarity": float(selected.get("score_artist") or 0.0),
                    "variant_alignment": True,
                    "variant_tags": sorted(classify_music_title_variants(selected.get("title"))),
                },
            },
            "decision_final_rejection": None,
            "decision_candidate_variant_distribution": dict(sorted(candidate_variant_distribution.items(), key=lambda item: item[0])),
            "decision_selected_candidate_variant_tags": sorted(classify_music_title_variants(selected.get("title"))),
            "decision_top_rejected_variant_tags": _top_rejected_variant_tags(rejected_candidates),
        }

    ranked_b = _score_candidates(expected_base, candidates, duration_max_delta_ms=EXPANDED_MAX_DELTA_MS)
    ranked_b, applied_b = _apply_album_coherence_tiebreak(
        ranked_b,
        family_counts=family_counts,
    )
    coherence_boost_applied += applied_b
    eligible_b = []
    for candidate in ranked_b:
        candidate_reason = str(candidate.get("rejection_reason") or "").strip()
        if candidate_reason:
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason=candidate_reason,
                    expected_base=expected_base,
                    pass_name="expanded",
                )
            )
            continue
        if (
            (MUSIC_TRACK_THRESHOLD - ALBUM_COHERENCE_MAX_BOOST)
            <= float(candidate.get("final_score") or 0.0)
            < MUSIC_TRACK_THRESHOLD
        ):
            coherence_near_miss = True
        if float(candidate.get("final_score", 0.0)) < MUSIC_TRACK_THRESHOLD:
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason="score_threshold",
                    expected_base=expected_base,
                    pass_name="expanded",
                )
            )
            continue
        try:
            delta_ok = candidate.get("duration_delta_ms") is not None and int(candidate.get("duration_delta_ms")) <= EXPANDED_MAX_DELTA_MS
        except Exception:
            delta_ok = False
        if not delta_ok:
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason="pass_b_duration",
                    expected_base=expected_base,
                    pass_name="expanded",
                )
            )
            rejection_counts["pass_b_duration"] += 1
            if str(candidate.get("source") or "").strip().lower() == "mb_relationship":
                mb_rejections[_classify_injected_rejection_bucket("pass_b_duration")] += 1
            continue
        if float(candidate.get("score_track", 0.0)) < PASS_B_MIN_TITLE:
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason="pass_b_track_similarity",
                    expected_base=expected_base,
                    pass_name="expanded",
                )
            )
            rejection_counts["pass_b_track_similarity"] += 1
            if str(candidate.get("source") or "").strip().lower() == "mb_relationship":
                mb_rejections[_classify_injected_rejection_bucket("pass_b_track_similarity")] += 1
            continue
        if float(candidate.get("score_artist", 0.0)) < PASS_B_MIN_ARTIST:
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason="pass_b_artist_similarity",
                    expected_base=expected_base,
                    pass_name="expanded",
                )
            )
            rejection_counts["pass_b_artist_similarity"] += 1
            if str(candidate.get("source") or "").strip().lower() == "mb_relationship":
                mb_rejections[_classify_injected_rejection_bucket("pass_b_artist_similarity")] += 1
            continue
        if not bool(candidate.get("authority_channel_match")):
            rejected_candidates.append(
                _build_candidate_observation(
                    candidate,
                    reason="pass_b_authority",
                    expected_base=expected_base,
                    pass_name="expanded",
                )
            )
            rejection_counts["pass_b_authority"] += 1
            if str(candidate.get("source") or "").strip().lower() == "mb_relationship":
                mb_rejections[_classify_injected_rejection_bucket("pass_b_authority")] += 1
            continue
        eligible_b.append(candidate)
    if eligible_b:
        selected = eligible_b[0]
        selected_score = float(selected.get("final_score") or 0.0)
        runner_up_score = None
        for item in ranked_b:
            if item.get("candidate_id") == selected.get("candidate_id"):
                continue
            if item.get("rejection_reason"):
                continue
            if float(item.get("final_score", 0.0)) < MUSIC_TRACK_THRESHOLD:
                continue
            runner_up_score = float(item.get("final_score") or 0.0)
            break
        title_similarity = max(
            float(selected.get("score_track") or 0.0),
            min(float(selected.get("score_track_relaxed") or 0.0) * 0.90, 1.0),
        )
        return {
            "selected": selected,
            "selected_pass": "expanded",
            "selected_score": selected_score,
            "failure_reason": None,
            "rejection_counts": dict(rejection_counts),
            "total_candidate_scores": total_candidate_scores,
            "coherence_boost_applied": coherence_boost_applied,
            "coherence_near_miss": coherence_near_miss,
            "mb_injected_rejection_counts": dict((key, int(mb_rejections.get(key) or 0)) for key in INJECTED_REJECTION_KEYS),
            "decision_rejected_candidates": rejected_candidates,
            "decision_accepted_selection": {
                "selected_candidate_id": selected.get("candidate_id"),
                "selected_score": selected_score,
                "runner_up_score": runner_up_score,
                "runner_up_gap": selected_score - float(runner_up_score or 0.0),
                "top_supporting_features": {
                    "duration_delta_ms": selected.get("duration_delta_ms"),
                    "title_similarity": title_similarity,
                    "artist_similarity": float(selected.get("score_artist") or 0.0),
                    "variant_alignment": True,
                    "variant_tags": sorted(classify_music_title_variants(selected.get("title"))),
                },
            },
            "decision_final_rejection": None,
            "decision_candidate_variant_distribution": dict(sorted(candidate_variant_distribution.items(), key=lambda item: item[0])),
            "decision_selected_candidate_variant_tags": sorted(classify_music_title_variants(selected.get("title"))),
            "decision_top_rejected_variant_tags": _top_rejected_variant_tags(rejected_candidates),
        }

    pass_a_reasons = {str(c.get("rejection_reason") or "") for c in ranked_a if c.get("rejection_reason")}
    if "duration_out_of_bounds" in pass_a_reasons or "duration_over_hard_cap" in pass_a_reasons:
        failure_reason = "duration_filtered"
    nearest = _nearest_rejection(rejected_candidates)
    final_rejection = None
    if nearest:
        final_rejection = {
            "failure_reason": failure_reason,
            "top_failed_gate": nearest.get("top_failed_gate"),
            "nearest_pass_margin": nearest.get("nearest_pass_margin"),
            "candidate_id": nearest.get("candidate_id"),
        }
    return {
        "selected": None,
        "selected_pass": None,
        "selected_score": None,
        "failure_reason": failure_reason,
        "rejection_counts": dict(rejection_counts),
        "total_candidate_scores": total_candidate_scores,
        "coherence_boost_applied": coherence_boost_applied,
        "coherence_near_miss": coherence_near_miss,
        "mb_injected_rejection_counts": dict((key, int(mb_rejections.get(key) or 0)) for key in INJECTED_REJECTION_KEYS),
        "decision_rejected_candidates": rejected_candidates,
        "decision_accepted_selection": None,
        "decision_final_rejection": final_rejection,
        "decision_candidate_variant_distribution": dict(sorted(candidate_variant_distribution.items(), key=lambda item: item[0])),
        "decision_selected_candidate_variant_tags": [],
        "decision_top_rejected_variant_tags": _top_rejected_variant_tags(rejected_candidates),
    }


def _evaluate_track(
    album_id: str,
    artist: str,
    album: str,
    track: dict[str, Any],
    fixture: dict[str, Any],
    *,
    album_coherence_counts: Counter[str] | None = None,
    enable_alias_matching: bool = True,
    enable_mb_relationship_injection: bool = True,
) -> TrackRunResult:
    track_name = str(track.get("track") or "").strip()
    duration_ms = track.get("duration_ms")
    duration_hint_sec = int(duration_ms) // 1000 if duration_ms is not None else None
    ladder = _build_ladder(artist, track_name, album)
    rung_candidates = fixture.get("rungs") if isinstance(fixture.get("rungs"), list) else []
    expected_selected_candidate_id = str(fixture.get("expect_selected_candidate_id") or "").strip() or None
    expected_match = bool(fixture.get("expect_match", True))
    failure_reason_override = str(
        track.get("failure_reason_override")
        or fixture.get("failure_reason_override")
        or ""
    ).strip() or None

    context = {
        "artist": artist,
        "album": album,
        "track": track_name,
        "recording_mbid": str(track.get("recording_mbid") or ""),
    }

    selected = None
    selected_pass = None
    selected_rung = None
    selected_score = None
    failure_reason = "no_candidates"
    total_candidate_scores = []
    rejection_counts: Counter[str] = Counter()
    all_candidate_count = 0
    coherence_boost_applied = 0
    coherence_near_miss = False
    mb_injected_selected = False
    mb_injected_rejection_counts: Counter[str] = _empty_injected_rejection_mix()
    retrieved_candidate_ids: list[str] = []
    seen_retrieved_ids: set[str] = set()
    decision_rejected_candidates: list[dict[str, Any]] = []
    decision_accepted_selection: dict[str, Any] | None = None
    decision_final_rejection: dict[str, Any] | None = None
    decision_candidate_variant_distribution: Counter[str] = Counter()
    decision_selected_candidate_variant_tags: list[str] = []
    decision_top_rejected_variant_tags: list[str] = []

    injected_candidates = []
    if enable_mb_relationship_injection:
        raw_injected = fixture.get("mb_injected_candidates") if isinstance(fixture.get("mb_injected_candidates"), list) else []
        injected_candidates = [dict(c) for c in _render_template(raw_injected, context) if isinstance(c, dict)]
        for candidate in injected_candidates:
            candidate["source"] = "mb_relationship"
            candidate["mb_injected"] = True
            candidate.setdefault("candidate_id", f"mb-injected-{len(candidate.get('url') or '')}")

    for rung in ladder:
        rung_idx = int(rung.get("rung") or 0)
        raw = rung_candidates[rung_idx] if rung_idx < len(rung_candidates) else []
        rendered = _render_template(raw, context)
        candidates = retrieve_candidates(
            {
                "candidates": rendered,
                "rung": rung_idx,
                "first_rung": int(ladder[0].get("rung") or 0) if ladder else 0,
                "injected_candidates": injected_candidates,
            }
        )
        all_candidate_count += len(candidates)
        for candidate in candidates:
            candidate_id = str(candidate.get("candidate_id") or "").strip()
            if not candidate_id or candidate_id in seen_retrieved_ids:
                continue
            seen_retrieved_ids.add(candidate_id)
            retrieved_candidate_ids.append(candidate_id)
        if not candidates:
            continue

        expected_base = {
            "artist": artist,
            "track": track_name,
            "album": album,
            "query": rung.get("query"),
            "media_intent": "music_track",
            "duration_hint_sec": duration_hint_sec,
            "duration_hard_cap_ms": HARD_CAP_MS,
            "variant_allow_tokens": {"live"} if "live" in track_name.lower() else set(),
            "track_aliases": track.get("track_aliases") if enable_alias_matching else None,
            "track_disambiguation": track.get("track_disambiguation") if enable_alias_matching else None,
        }
        rank_result = rank_and_gate(
            {
                "expected_base": expected_base,
                "family_counts": album_coherence_counts,
            },
            candidates,
        )
        total_candidate_scores.extend(
            float(v) for v in (rank_result.get("total_candidate_scores") or [])
        )
        rejection_counts.update(
            Counter({str(k): int(v) for k, v in (rank_result.get("rejection_counts") or {}).items()})
        )
        mb_injected_rejection_counts.update(
            Counter(
                {
                    str(k): int(v)
                    for k, v in (rank_result.get("mb_injected_rejection_counts") or {}).items()
                }
            )
        )
        for item in (rank_result.get("decision_rejected_candidates") or []):
            if isinstance(item, dict):
                decision_rejected_candidates.append(dict(item))
        for tag, count in (rank_result.get("decision_candidate_variant_distribution") or {}).items():
            tag_key = str(tag or "").strip()
            if tag_key:
                decision_candidate_variant_distribution[tag_key] += int(count or 0)
        if isinstance(rank_result.get("decision_accepted_selection"), dict):
            decision_accepted_selection = dict(rank_result.get("decision_accepted_selection"))
            decision_final_rejection = None
            decision_selected_candidate_variant_tags = list(rank_result.get("decision_selected_candidate_variant_tags") or [])
        elif isinstance(rank_result.get("decision_final_rejection"), dict):
            decision_final_rejection = dict(rank_result.get("decision_final_rejection"))
        if isinstance(rank_result.get("decision_top_rejected_variant_tags"), list):
            decision_top_rejected_variant_tags = list(rank_result.get("decision_top_rejected_variant_tags") or [])
        coherence_boost_applied += int(rank_result.get("coherence_boost_applied") or 0)
        coherence_near_miss = bool(coherence_near_miss or rank_result.get("coherence_near_miss"))
        if rank_result.get("selected") is not None:
            selected = dict(rank_result.get("selected") or {})
            selected_pass = str(rank_result.get("selected_pass") or "").strip() or None
            selected_rung = rung_idx
            selected_score = float(rank_result.get("selected_score") or 0.0)
            break
        failure_reason = str(rank_result.get("failure_reason") or "no_candidate_above_threshold")

    selected_candidate_id = str(selected.get("candidate_id") or "").strip() if isinstance(selected, dict) else None
    mb_injected_selected = bool(isinstance(selected, dict) and str(selected.get("source") or "").strip().lower() == "mb_relationship")
    selected_title = str(selected.get("title") or "") if isinstance(selected, dict) else ""
    allowed_variant_tokens = {"live"} if "live" in track_name.lower() else set()
    wrong_variant_flag = _has_wrong_variant(selected_title, allowed_tokens=allowed_variant_tokens)
    if expected_selected_candidate_id and selected_candidate_id and selected_candidate_id != expected_selected_candidate_id:
        wrong_variant_flag = True
    if selected is not None and album_coherence_counts is not None:
        family = _candidate_authority_family(selected)
        if family:
            album_coherence_counts[family] += 1
    coherence_selected_delta = float((selected or {}).get("coherence_delta") or 0.0) if isinstance(selected, dict) else 0.0
    resolved = selected is not None
    final_failure_reason = None if resolved else (failure_reason_override or failure_reason)
    if (
        not resolved
        and injected_candidates
        and isinstance(final_failure_reason, str)
        and final_failure_reason.startswith("source_unavailable")
    ):
        mb_injected_rejection_counts[_classify_injected_rejection_bucket(final_failure_reason)] += 1

    return TrackRunResult(
        album_id=album_id,
        track_id=str(track.get("recording_mbid") or track_name),
        resolved=resolved,
        selected_pass=selected_pass,
        selected_rung=selected_rung,
        selected_candidate_id=selected_candidate_id,
        selected_score=selected_score,
        wrong_variant_flag=wrong_variant_flag,
        failure_reason=final_failure_reason,
        rejection_counts=dict(rejection_counts),
        avg_candidate_score=(sum(total_candidate_scores) / len(total_candidate_scores)) if total_candidate_scores else None,
        candidate_count=all_candidate_count,
        expected_selected_candidate_id=expected_selected_candidate_id,
        expected_match=expected_match,
        retrieved_candidate_ids=list(retrieved_candidate_ids),
        decision_accepted_selection=decision_accepted_selection,
        decision_rejected_candidates=decision_rejected_candidates,
        decision_final_rejection=decision_final_rejection,
        decision_candidate_variant_distribution=dict(sorted(decision_candidate_variant_distribution.items(), key=lambda item: item[0])),
        decision_selected_candidate_variant_tags=sorted({str(tag) for tag in decision_selected_candidate_variant_tags if str(tag or "").strip()}),
        decision_top_rejected_variant_tags=decision_top_rejected_variant_tags,
        coherence_boost_applied=coherence_boost_applied,
        coherence_selected_delta=coherence_selected_delta,
        coherence_near_miss=coherence_near_miss,
        mb_injected_selected=mb_injected_selected,
        mb_injected_rejection_counts=dict((key, int(mb_injected_rejection_counts.get(key) or 0)) for key in INJECTED_REJECTION_KEYS),
    )


def _classify_missing_hint(track: TrackRunResult) -> tuple[str, str, dict[str, Any]]:
    reason = str(track.failure_reason or "").strip().lower()
    candidate_count = int(track.candidate_count or 0)
    rejection_counts = Counter({str(k): int(v) for k, v in (track.rejection_counts or {}).items()})

    if reason.startswith("source_unavailable") or reason.startswith("unavailable"):
        unavailable_class = reason.split(":", 1)[1] if ":" in reason else "unknown"
        return (
            "unavailable",
            "Unavailable (blocked/removed)",
            {"failure_reason": reason, "unavailable_class": unavailable_class},
        )

    duration_filtered = (
        int(rejection_counts.get("duration_out_of_bounds") or 0)
        + int(rejection_counts.get("duration_over_hard_cap") or 0)
        + int(rejection_counts.get("pass_b_duration") or 0)
    )
    if candidate_count >= 3 and duration_filtered >= max(2, int(candidate_count * 0.7)):
        return (
            "likely_wrong_mb_recording_length",
            "Likely wrong MB recording length (duration mismatch persistent across many candidates)",
            {"duration_filtered": duration_filtered, "candidate_count": candidate_count},
        )

    if int(rejection_counts.get("low_title_similarity") or 0) > 0:
        return (
            "recoverable_alias_matching",
            "Recoverable by alias matching (low title sim)",
            {"low_title_similarity": int(rejection_counts.get("low_title_similarity") or 0)},
        )

    if bool(track.coherence_near_miss):
        return (
            "recoverable_coherence",
            "Recoverable by coherence (near-miss tie-break)",
            {
                "coherence_near_miss": True,
                "coherence_selected_delta": float(track.coherence_selected_delta or 0.0),
            },
        )

    if candidate_count == 0 or reason == "no_candidates":
        return (
            "recoverable_ladder_extension",
            "Recoverable by ladder extension (no candidates)",
            {"candidate_count": candidate_count, "failure_reason": reason or "no_candidates"},
        )

    # Fallback classification for unresolved tracks that do not emit a stronger signal.
    return (
        "recoverable_ladder_extension",
        "Recoverable by ladder extension (no candidates)",
        {"candidate_count": candidate_count, "failure_reason": reason or "unknown"},
    )


def run_benchmark(
    dataset: dict[str, Any],
    *,
    enable_album_coherence: bool = True,
    enable_alias_matching: bool = True,
    enable_mb_relationship_injection: bool = True,
) -> dict[str, Any]:
    fixtures = dataset.get("fixtures") if isinstance(dataset.get("fixtures"), dict) else {}
    albums = dataset.get("albums") if isinstance(dataset.get("albums"), list) else []

    track_results: list[TrackRunResult] = []
    per_album: dict[str, dict[str, Any]] = {}
    rejection_totals: Counter[str] = Counter()
    coherence_boost_events = 0
    coherence_selected_tracks = 0
    mb_injected_rejection_totals: Counter[str] = _empty_injected_rejection_mix()

    for album in albums:
        if not isinstance(album, dict):
            continue
        album_id = str(album.get("album_id") or album.get("release_group_mbid") or "").strip() or "unknown_album"
        artist = str(album.get("artist") or "").strip()
        album_title = str(album.get("title") or "").strip()
        tracks = album.get("tracks") if isinstance(album.get("tracks"), list) else []
        album_coherence_counts: Counter[str] | None = (
            Counter() if (enable_album_coherence and len(tracks) > 1) else None
        )
        album_total = 0
        album_resolved = 0
        album_mb_injected_success = 0
        album_mb_injected_rejections: Counter[str] = _empty_injected_rejection_mix()
        for track in tracks:
            if not isinstance(track, dict):
                continue
            fixture_key = str(track.get("fixture") or "").strip()
            fixture = fixtures.get(fixture_key) if fixture_key else None
            if not isinstance(fixture, dict):
                raise ValueError(f"missing fixture '{fixture_key}' for album '{album_id}'")
            result = _evaluate_track(
                album_id,
                artist,
                album_title,
                track,
                fixture,
                album_coherence_counts=album_coherence_counts,
                enable_alias_matching=enable_alias_matching,
                enable_mb_relationship_injection=enable_mb_relationship_injection,
            )
            track_results.append(result)
            album_total += 1
            if result.resolved:
                album_resolved += 1
            coherence_boost_events += int(result.coherence_boost_applied or 0)
            if float(result.coherence_selected_delta or 0.0) > 0.0:
                coherence_selected_tracks += 1
            rejection_totals.update(result.rejection_counts)
            mb_injected_rejection_totals.update(result.mb_injected_rejection_counts)
            album_mb_injected_rejections.update(result.mb_injected_rejection_counts)
            if result.mb_injected_selected:
                album_mb_injected_success += 1
        per_album[album_id] = {
            "artist": artist,
            "title": album_title,
            "release_group_mbid": album.get("release_group_mbid"),
            "tracks_total": album_total,
            "tracks_resolved": album_resolved,
            "completion_percent": (album_resolved / album_total * 100.0) if album_total else 0.0,
            "mb_injected_success": album_mb_injected_success,
            "injected_rejection_mix": dict(
                (key, int(album_mb_injected_rejections.get(key) or 0))
                for key in INJECTED_REJECTION_KEYS
            ),
        }

    total_tracks = len(track_results)
    resolved_tracks = sum(1 for tr in track_results if tr.resolved)
    wrong_variant_count = sum(1 for tr in track_results if tr.wrong_variant_flag)
    retrieval_eval_tracks = [
        tr for tr in track_results
        if str(tr.expected_selected_candidate_id or "").strip()
    ]
    retrieval_recall: dict[str, dict[str, float | int]] = {}
    for k in (1, 3, 5, 10):
        hits = 0
        for tr in retrieval_eval_tracks:
            expected_id = str(tr.expected_selected_candidate_id or "").strip()
            retrieved_top_k = [str(cid or "").strip() for cid in (tr.retrieved_candidate_ids or [])[:k]]
            if expected_id and expected_id in retrieved_top_k:
                hits += 1
        evaluated = len(retrieval_eval_tracks)
        recall_value = (hits / evaluated * 100.0) if evaluated else 0.0
        retrieval_recall[str(k)] = {"hits": hits, "evaluated": evaluated, "recall_percent": recall_value}
    avg_selected_score = (
        sum(float(tr.selected_score or 0.0) for tr in track_results if tr.selected_score is not None)
        / max(1, sum(1 for tr in track_results if tr.selected_score is not None))
    )
    avg_candidate_score = (
        sum(float(tr.avg_candidate_score or 0.0) for tr in track_results if tr.avg_candidate_score is not None)
        / max(1, sum(1 for tr in track_results if tr.avg_candidate_score is not None))
    )
    unresolved = [tr for tr in track_results if not tr.resolved]
    unresolved_reasons = Counter(str(tr.failure_reason or "unknown") for tr in unresolved)
    unavailable_unresolved = sum(
        1
        for tr in unresolved
        if str(tr.failure_reason or "").startswith("source_unavailable")
        or str(tr.failure_reason or "").startswith("unavailable")
    )
    no_viable_unresolved = len(unresolved) - unavailable_unresolved
    missing_hints = []
    missing_hint_counts: Counter[str] = Counter()
    for tr in unresolved:
        code, label, evidence = _classify_missing_hint(tr)
        missing_hint_counts[label] += 1
        missing_hints.append(
            {
                "album_id": tr.album_id,
                "track_id": tr.track_id,
                "hint_code": code,
                "hint_label": label,
                "evidence": evidence,
            }
        )
    failure_motifs = _collect_failure_motifs(track_results)

    return {
        "schema_version": 2,
        "dataset_name": dataset.get("dataset_name"),
        "albums_total": len(per_album),
        "tracks_total": total_tracks,
        "tracks_resolved": resolved_tracks,
        "completion_percent": (resolved_tracks / total_tracks * 100.0) if total_tracks else 0.0,
        "wrong_variant_flags": wrong_variant_count,
        "retrieval_metrics": {
            "evaluated_tracks": len(retrieval_eval_tracks),
            "recall_at_k": retrieval_recall,
        },
        "avg_selected_score": avg_selected_score,
        "avg_candidate_score": avg_candidate_score,
        "coherence_boost_events": coherence_boost_events,
        "coherence_selected_tracks": coherence_selected_tracks,
        "rejection_mix": dict(rejection_totals),
        "mb_injected_rejection_mix": dict(
            (key, int(mb_injected_rejection_totals.get(key) or 0))
            for key in INJECTED_REJECTION_KEYS
        ),
        "mb_injected_success_total": sum(int(v.get("mb_injected_success") or 0) for v in per_album.values()),
        "unresolved_failure_reasons": dict(unresolved_reasons),
        "unresolved_classification": {
            "source_unavailable": unavailable_unresolved,
            "no_viable_match": no_viable_unresolved,
        },
        "why_missing": {
            "hint_counts": dict(sorted(missing_hint_counts.items(), key=lambda item: (-int(item[1]), item[0]))),
            "tracks": missing_hints,
        },
        "failure_motifs": failure_motifs,
        "per_album": per_album,
        "per_track": [
            {
                "album_id": tr.album_id,
                "track_id": tr.track_id,
                "resolved": tr.resolved,
                "selected_pass": tr.selected_pass,
                "selected_rung": tr.selected_rung,
                "selected_candidate_id": tr.selected_candidate_id,
                "selected_score": tr.selected_score,
                "wrong_variant_flag": tr.wrong_variant_flag,
                "failure_reason": tr.failure_reason,
                "rejection_counts": tr.rejection_counts,
                "avg_candidate_score": tr.avg_candidate_score,
                "candidate_count": tr.candidate_count,
                "expected_selected_candidate_id": tr.expected_selected_candidate_id,
                "expected_match": tr.expected_match,
                "retrieved_candidate_ids": tr.retrieved_candidate_ids,
                "decision_edge": {
                    "accepted_selection": tr.decision_accepted_selection,
                    "rejected_candidates": tr.decision_rejected_candidates,
                    "final_rejection": tr.decision_final_rejection,
                    "candidate_variant_distribution": tr.decision_candidate_variant_distribution,
                    "selected_candidate_variant_tags": tr.decision_selected_candidate_variant_tags,
                    "top_rejected_variant_tags": tr.decision_top_rejected_variant_tags,
                },
                "coherence_boost_applied": tr.coherence_boost_applied,
                "coherence_selected_delta": tr.coherence_selected_delta,
                "coherence_near_miss": tr.coherence_near_miss,
                "mb_injected_selected": tr.mb_injected_selected,
                "mb_injected_rejection_counts": tr.mb_injected_rejection_counts,
            }
            for tr in track_results
        ],
    }


def evaluate_gate(summary: dict[str, Any], gate: dict[str, Any]) -> tuple[bool, list[str]]:
    baseline = gate.get("baseline") if isinstance(gate.get("baseline"), dict) else {}
    tolerance = gate.get("tolerance") if isinstance(gate.get("tolerance"), dict) else {}
    completion_drop_limit = float(tolerance.get("max_completion_drop_pct_points", 0.5))
    wrong_variant_increase_limit = int(tolerance.get("max_wrong_variant_increase", 0))

    completion = float(summary.get("completion_percent") or 0.0)
    wrong_variant = int(summary.get("wrong_variant_flags") or 0)
    baseline_completion = float(baseline.get("completion_percent") or 0.0)
    baseline_wrong_variant = int(baseline.get("wrong_variant_flags") or 0)

    failures: list[str] = []
    if completion < (baseline_completion - completion_drop_limit):
        failures.append(
            f"completion regression: current={completion:.2f}% baseline={baseline_completion:.2f}% "
            f"allowed_drop={completion_drop_limit:.2f}pp"
        )
    if wrong_variant > (baseline_wrong_variant + wrong_variant_increase_limit):
        failures.append(
            f"wrong-variant regression: current={wrong_variant} baseline={baseline_wrong_variant} "
            f"allowed_increase={wrong_variant_increase_limit}"
        )
    return (len(failures) == 0, failures)


def _print_table(summary: dict[str, Any]) -> None:
    print("\nMusic Search Benchmark")
    print("======================")
    print(f"Dataset: {summary.get('dataset_name')}")
    print(f"Albums: {summary.get('albums_total')}  Tracks: {summary.get('tracks_total')}")
    print(
        "Completion: "
        f"{float(summary.get('completion_percent') or 0.0):.2f}% "
        f"({summary.get('tracks_resolved')}/{summary.get('tracks_total')})"
    )
    print(f"Wrong-variant flags: {summary.get('wrong_variant_flags')}")
    retrieval_metrics = summary.get("retrieval_metrics") if isinstance(summary.get("retrieval_metrics"), dict) else {}
    recall_at_k = retrieval_metrics.get("recall_at_k") if isinstance(retrieval_metrics.get("recall_at_k"), dict) else {}
    if recall_at_k:
        recall_parts = []
        for k in (1, 3, 5, 10):
            bucket = recall_at_k.get(str(k)) if isinstance(recall_at_k.get(str(k)), dict) else {}
            recall_parts.append(f"R@{k}={float(bucket.get('recall_percent') or 0.0):.2f}%")
        print(f"Retrieval recall: {'  '.join(recall_parts)}")
    print(f"Avg selected score: {float(summary.get('avg_selected_score') or 0.0):.4f}")
    print(f"Avg candidate score: {float(summary.get('avg_candidate_score') or 0.0):.4f}")
    print(
        "Coherence: "
        f"boost_events={int(summary.get('coherence_boost_events') or 0)} "
        f"selected_with_boost={int(summary.get('coherence_selected_tracks') or 0)}"
    )
    print(
        "MB injected: "
        f"selected_total={int(summary.get('mb_injected_success_total') or 0)}"
    )

    print("\nTop unresolved reasons:")
    unresolved = summary.get("unresolved_failure_reasons") or {}
    if unresolved:
        for reason, count in sorted(unresolved.items(), key=lambda item: (-int(item[1]), item[0])):
            print(f"  - {reason}: {count}")
    else:
        print("  - none")
    unresolved_class = summary.get("unresolved_classification") or {}
    print(
        "Unresolved classes: "
        f"source_unavailable={int(unresolved_class.get('source_unavailable') or 0)} "
        f"no_viable_match={int(unresolved_class.get('no_viable_match') or 0)}"
    )

    print("\nTop rejection mix:")
    rej = summary.get("rejection_mix") or {}
    if rej:
        for reason, count in sorted(rej.items(), key=lambda item: (-int(item[1]), item[0]))[:10]:
            print(f"  - {reason}: {count}")
    else:
        print("  - none")
    print("\nMB injected rejection mix:")
    mb_rej = summary.get("mb_injected_rejection_mix") or {}
    if mb_rej:
        for reason, count in sorted(mb_rej.items(), key=lambda item: (-int(item[1]), item[0]))[:10]:
            print(f"  - {reason}: {count}")
    else:
        print("  - none")

    print("\nWhy missing:")
    why_missing = summary.get("why_missing") if isinstance(summary.get("why_missing"), dict) else {}
    hint_counts = why_missing.get("hint_counts") if isinstance(why_missing.get("hint_counts"), dict) else {}
    hint_tracks = why_missing.get("tracks") if isinstance(why_missing.get("tracks"), list) else []
    if hint_counts:
        for label, count in sorted(hint_counts.items(), key=lambda item: (-int(item[1]), item[0])):
            print(f"  - {label}: {count}")
    else:
        print("  - none")
    if hint_tracks:
        print("\nWhy missing (per track):")
        for item in hint_tracks:
            if not isinstance(item, dict):
                continue
            album_id = str(item.get("album_id") or "")
            track_id = str(item.get("track_id") or "")
            label = str(item.get("hint_label") or "")
            evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
            failure_reason = str(evidence.get("failure_reason") or "").strip()
            suffix = f" reason={failure_reason}" if failure_reason else ""
            print(f"  - {album_id}:{track_id} -> {label}{suffix}")

    print("\nAlbum completion:")
    print("artist | album | completion | resolved/total")
    print("-" * 72)
    per_album = summary.get("per_album") or {}
    rows = sorted(
        per_album.values(),
        key=lambda row: (float(row.get("completion_percent") or 0.0), str(row.get("artist") or ""), str(row.get("title") or "")),
    )
    for row in rows:
        artist = str(row.get("artist") or "")[:22]
        title = str(row.get("title") or "")[:26]
        completion = float(row.get("completion_percent") or 0.0)
        resolved = int(row.get("tracks_resolved") or 0)
        total = int(row.get("tracks_total") or 0)
        print(f"{artist:<22} | {title:<26} | {completion:>6.2f}% | {resolved}/{total}")


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run deterministic music search benchmark fixtures.")
    parser.add_argument(
        "--dataset",
        default="benchmarks/music_search_album_dataset.json",
        help="Path to benchmark dataset JSON.",
    )
    parser.add_argument(
        "--output-json",
        default="benchmarks/music_search_benchmark_results.json",
        help="Path to write benchmark summary JSON.",
    )
    parser.add_argument(
        "--gate-config",
        default="benchmarks/music_search_benchmark_gate.json",
        help="Path to regression gate JSON.",
    )
    parser.add_argument(
        "--enforce-gate",
        action="store_true",
        help="Fail with non-zero exit code if gate fails.",
    )
    parser.add_argument(
        "--disable-alias-matching",
        action="store_true",
        help="Disable MB-derived alias/disambiguation title variants for A/B comparison.",
    )
    parser.add_argument(
        "--disable-mb-relationship-injection",
        action="store_true",
        help="Disable MB relationship YouTube candidate injection for A/B comparison.",
    )
    args = parser.parse_args(argv)

    dataset_path = Path(args.dataset)
    output_path = Path(args.output_json)
    gate_path = Path(args.gate_config)

    dataset = _load_json(dataset_path)
    summary = run_benchmark(
        dataset,
        enable_alias_matching=not bool(args.disable_alias_matching),
        enable_mb_relationship_injection=not bool(args.disable_mb_relationship_injection),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    _print_table(summary)
    print(f"\nJSON summary written to: {output_path}")

    if args.enforce_gate:
        gate = _load_json(gate_path)
        ok, failures = evaluate_gate(summary, gate)
        if ok:
            print("\nRegression gate: PASS")
            return 0
        print("\nRegression gate: FAIL")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
