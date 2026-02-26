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
    mb_injected_rejection_counts: Counter[str] = Counter()

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
        candidates = _render_template(raw, context)
        candidates = [c for c in candidates if isinstance(c, dict)]
        if rung_idx == 0 and injected_candidates:
            candidates = [dict(c) for c in injected_candidates] + candidates
        all_candidate_count += len(candidates)
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

        ranked_a = _score_candidates(expected_base, candidates, duration_max_delta_ms=STRICT_MAX_DELTA_MS)
        ranked_a, applied_a = _apply_album_coherence_tiebreak(
            ranked_a,
            family_counts=album_coherence_counts,
        )
        coherence_boost_applied += applied_a
        for c in ranked_a:
            if c.get("final_score") is not None:
                total_candidate_scores.append(float(c.get("final_score") or 0.0))
            reason = str(c.get("rejection_reason") or "").strip()
            if reason:
                rejection_counts[reason] += 1
                if reason == "floor_check_failed" and float(c.get("score_track") or 0.0) < 0.70:
                    rejection_counts["low_title_similarity"] += 1
                if str(c.get("source") or "").strip().lower() == "mb_relationship":
                    if reason in {"duration_out_of_bounds", "duration_over_hard_cap", "preview_duration"}:
                        mb_injected_rejection_counts["mb_injected_failed_duration"] += 1
                    elif reason in {"disallowed_variant", "preview_variant", "session_variant", "cover_artist_mismatch"}:
                        mb_injected_rejection_counts["mb_injected_failed_variant"] += 1
                    elif reason == "low_title_similarity":
                        mb_injected_rejection_counts["mb_injected_failed_title"] += 1
                    elif reason == "low_artist_similarity":
                        mb_injected_rejection_counts["mb_injected_failed_artist"] += 1
                    else:
                        mb_injected_rejection_counts[f"mb_injected_failed_{reason}"] += 1
            if (
                not reason
                and (MUSIC_TRACK_THRESHOLD - ALBUM_COHERENCE_MAX_BOOST)
                <= float(c.get("final_score") or 0.0)
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
            selected_pass = "strict"
            selected_rung = rung_idx
            selected_score = float(selected.get("final_score") or 0.0)
            break

        ranked_b = _score_candidates(expected_base, candidates, duration_max_delta_ms=EXPANDED_MAX_DELTA_MS)
        ranked_b, applied_b = _apply_album_coherence_tiebreak(
            ranked_b,
            family_counts=album_coherence_counts,
        )
        coherence_boost_applied += applied_b
        eligible_b = []
        for c in ranked_b:
            if c.get("rejection_reason"):
                continue
            if (
                (MUSIC_TRACK_THRESHOLD - ALBUM_COHERENCE_MAX_BOOST)
                <= float(c.get("final_score") or 0.0)
                < MUSIC_TRACK_THRESHOLD
            ):
                coherence_near_miss = True
            if float(c.get("final_score", 0.0)) < MUSIC_TRACK_THRESHOLD:
                continue
            try:
                delta_ok = c.get("duration_delta_ms") is not None and int(c.get("duration_delta_ms")) <= EXPANDED_MAX_DELTA_MS
            except Exception:
                delta_ok = False
            if not delta_ok:
                rejection_counts["pass_b_duration"] += 1
                if str(c.get("source") or "").strip().lower() == "mb_relationship":
                    mb_injected_rejection_counts["mb_injected_failed_duration"] += 1
                continue
            if float(c.get("score_track", 0.0)) < PASS_B_MIN_TITLE:
                rejection_counts["pass_b_track_similarity"] += 1
                if str(c.get("source") or "").strip().lower() == "mb_relationship":
                    mb_injected_rejection_counts["mb_injected_failed_title"] += 1
                continue
            if float(c.get("score_artist", 0.0)) < PASS_B_MIN_ARTIST:
                rejection_counts["pass_b_artist_similarity"] += 1
                if str(c.get("source") or "").strip().lower() == "mb_relationship":
                    mb_injected_rejection_counts["mb_injected_failed_artist"] += 1
                continue
            if not bool(c.get("authority_channel_match")):
                rejection_counts["pass_b_authority"] += 1
                if str(c.get("source") or "").strip().lower() == "mb_relationship":
                    mb_injected_rejection_counts["mb_injected_failed_authority"] += 1
                continue
            eligible_b.append(c)

        if eligible_b:
            selected = eligible_b[0]
            selected_pass = "expanded"
            selected_rung = rung_idx
            selected_score = float(selected.get("final_score") or 0.0)
            break

        pass_a_reasons = {str(c.get("rejection_reason") or "") for c in ranked_a if c.get("rejection_reason")}
        if "duration_out_of_bounds" in pass_a_reasons or "duration_over_hard_cap" in pass_a_reasons:
            failure_reason = "duration_filtered"
        else:
            failure_reason = "no_candidate_above_threshold"

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
        coherence_boost_applied=coherence_boost_applied,
        coherence_selected_delta=coherence_selected_delta,
        coherence_near_miss=coherence_near_miss,
        mb_injected_selected=mb_injected_selected,
        mb_injected_rejection_counts=dict(mb_injected_rejection_counts),
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
    mb_injected_rejection_totals: Counter[str] = Counter()

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
        }

    total_tracks = len(track_results)
    resolved_tracks = sum(1 for tr in track_results if tr.resolved)
    wrong_variant_count = sum(1 for tr in track_results if tr.wrong_variant_flag)
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

    return {
        "schema_version": 2,
        "dataset_name": dataset.get("dataset_name"),
        "albums_total": len(per_album),
        "tracks_total": total_tracks,
        "tracks_resolved": resolved_tracks,
        "completion_percent": (resolved_tracks / total_tracks * 100.0) if total_tracks else 0.0,
        "wrong_variant_flags": wrong_variant_count,
        "avg_selected_score": avg_selected_score,
        "avg_candidate_score": avg_candidate_score,
        "coherence_boost_events": coherence_boost_events,
        "coherence_selected_tracks": coherence_selected_tracks,
        "rejection_mix": dict(rejection_totals),
        "mb_injected_rejection_mix": dict(mb_injected_rejection_totals),
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
