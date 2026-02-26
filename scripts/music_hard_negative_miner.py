from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_RISKY_ACCEPT_TAGS = {
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


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _extract_tracks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    tracks = payload.get("per_track")
    if isinstance(tracks, list):
        return [item for item in tracks if isinstance(item, dict)]
    return []


def _coerce_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if _extract_tracks(payload):
        return payload
    if any(key in payload for key in ("per_track", "failure_motifs", "run_type", "dataset_name")):
        return payload
    for key in ("run_summary", "summary", "benchmark_summary", "benchmark_results", "result", "payload"):
        candidate = payload.get(key)
        if not isinstance(candidate, dict):
            continue
        if _extract_tracks(candidate):
            return candidate
        if any(inner in candidate for inner in ("per_track", "failure_motifs", "run_type", "dataset_name")):
            return candidate
    return payload


def _track_motifs(track: dict[str, Any]) -> set[str]:
    motifs: set[str] = set()
    edge = track.get("decision_edge") if isinstance(track.get("decision_edge"), dict) else {}
    accepted = edge.get("accepted_selection") if isinstance(edge.get("accepted_selection"), dict) else {}
    failure_reason = str(track.get("failure_reason") or "").strip().lower()
    wrong_variant_flag = bool(track.get("wrong_variant_flag"))
    selected_tags = {
        str(tag or "").strip()
        for tag in (edge.get("selected_candidate_variant_tags") or [])
        if str(tag or "").strip()
    }
    top_rejected_tags = {
        str(tag or "").strip()
        for tag in (edge.get("top_rejected_variant_tags") or [])
        if str(tag or "").strip()
    }

    if wrong_variant_flag or bool(selected_tags & _RISKY_ACCEPT_TAGS):
        motifs.add("wrong_variant_accept")
    top_features = accepted.get("top_supporting_features") if isinstance(accepted.get("top_supporting_features"), dict) else {}
    artist_similarity = top_features.get("artist_similarity")
    try:
        if artist_similarity is not None and float(artist_similarity) < 0.50:
            motifs.add("wrong_artist_accept")
    except Exception:
        pass
    if failure_reason == "duration_filtered":
        motifs.add("duration_drift")
    if failure_reason.startswith("source_unavailable:") or failure_reason.startswith("unavailable"):
        motifs.add("unavailable")
    if "lyric_video" in top_rejected_tags:
        motifs.add("lyric_video_rejection")
    if "remaster" in top_rejected_tags:
        motifs.add("remaster_rejection")
    if "live" in top_rejected_tags:
        motifs.add("live_rejection")
    if {"sped_up", "slowed", "nightcore", "8d"} & top_rejected_tags:
        motifs.add("tempo_fx_rejection")
    return motifs


def _candidate_stub_from_edge(entry: dict[str, Any], fallback_index: int) -> dict[str, Any]:
    candidate_id = str(entry.get("candidate_id") or f"candidate-{fallback_index}").strip()
    source = str(entry.get("source") or "youtube").strip() or "youtube"
    title = str(entry.get("title") or "{{track}}").strip() or "{{track}}"
    return {
        "candidate_id": candidate_id,
        "source": source,
        "title": title,
        "uploader": "{{artist}} - Topic",
        "artist_detected": "{{artist}}",
        "track_detected": "{{track}}",
        "album_detected": "{{album}}",
        "duration_sec": 210,
        "url": f"https://example.test/{source}/{candidate_id}",
    }


def build_fixture_stub_payload(
    summary_payload: dict[str, Any],
    *,
    fixture_prefix: str = "hn",
    max_candidates: int = 6,
) -> dict[str, Any]:
    summary_payload = _coerce_summary_payload(summary_payload)
    tracks = _extract_tracks(summary_payload)
    generated_fixtures: dict[str, Any] = {}
    album_stubs: list[dict[str, Any]] = []
    motif_counts: Counter[str] = Counter()
    fixture_index = 1

    for track in tracks:
        motifs = _track_motifs(track)
        if not motifs:
            continue
        motif_counts.update(motifs)
        edge = track.get("decision_edge") if isinstance(track.get("decision_edge"), dict) else {}
        rejected = edge.get("rejected_candidates") if isinstance(edge.get("rejected_candidates"), list) else []
        accepted = edge.get("accepted_selection") if isinstance(edge.get("accepted_selection"), dict) else None
        candidates: list[dict[str, Any]] = []
        for idx, entry in enumerate(rejected):
            if not isinstance(entry, dict):
                continue
            candidates.append(_candidate_stub_from_edge(entry, idx))
            if len(candidates) >= max_candidates:
                break
        if accepted and len(candidates) < max_candidates:
            candidates.append(
                {
                    "candidate_id": str(accepted.get("selected_candidate_id") or f"suspect-selected-{fixture_index}"),
                    "source": "youtube_music",
                    "title": "{{track}}",
                    "uploader": "{{artist}} - Topic",
                    "artist_detected": "{{artist}}",
                    "track_detected": "{{track}}",
                    "album_detected": "{{album}}",
                    "duration_sec": 210,
                    "url": f"https://example.test/youtube_music/suspect-selected-{fixture_index}",
                }
            )

        fixture_key = f"{fixture_prefix}_{fixture_index:03d}"
        generated_fixtures[fixture_key] = {
            "expect_match": False,
            "notes": f"Generated hard-negative candidate set. Motifs={sorted(motifs)}. Review and edit before merging.",
            "rungs": [candidates, [], [], [], [], []],
        }
        album_stubs.append(
            {
                "album_id": f"todo-{fixture_key}",
                "artist": "TODO_ARTIST",
                "title": "TODO_ALBUM",
                "release_group_mbid": "TODO_RELEASE_GROUP_MBID",
                "tracks": [
                    {
                        "recording_mbid": str(track.get("track_id") or "TODO_RECORDING_MBID"),
                        "track": "TODO_TRACK_TITLE",
                        "duration_ms": 210000,
                        "fixture": fixture_key,
                    }
                ],
            }
        )
        fixture_index += 1

    summary_motif_counts = summary_payload.get("failure_motifs")
    if isinstance(summary_motif_counts, dict):
        normalized_summary_counts: Counter[str] = Counter()
        for key, value in summary_motif_counts.items():
            label = str(key or "").strip()
            if not label:
                continue
            try:
                normalized_summary_counts[label] += int(value)
            except Exception:
                continue
        if normalized_summary_counts:
            motif_counts.update(normalized_summary_counts)

    sorted_motifs = dict(sorted(motif_counts.items(), key=lambda item: (-int(item[1]), item[0])))
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source_run_type": summary_payload.get("run_type"),
        "source_dataset_name": summary_payload.get("dataset_name"),
        "motif_counts": sorted_motifs,
        "top_recurring_failure_motifs": sorted_motifs,
        "generated_fixtures": generated_fixtures,
        "album_stubs": album_stubs,
        "instructions": [
            "This file is generated for review only.",
            "Do not auto-merge generated fixtures into benchmarks/music_search_album_dataset.json.",
            "Manually validate candidate realism and expected winner before adding fixtures.",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate hard-negative fixture stubs from run summary artifacts.")
    parser.add_argument("--input", required=True, help="Path to run_summary.json or benchmark summary JSON.")
    parser.add_argument(
        "--output",
        default="benchmarks/generated_fixture_stubs/hard_negative_fixture_candidates.json",
        help="Output JSON path for generated fixture candidates.",
    )
    parser.add_argument("--fixture-prefix", default="hn", help="Prefix for generated fixture keys.")
    parser.add_argument("--max-candidates", type=int, default=6, help="Maximum rung-0 candidates per generated fixture.")
    args = parser.parse_args(argv)

    payload = _load_json(Path(args.input))
    output_payload = build_fixture_stub_payload(
        payload,
        fixture_prefix=str(args.fixture_prefix or "hn"),
        max_candidates=max(1, int(args.max_candidates or 6)),
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Generated fixture stub file: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
