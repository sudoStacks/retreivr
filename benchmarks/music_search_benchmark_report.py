from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _fmt_delta(value: float) -> str:
    if value > 0:
        return f"+{value:.2f}"
    return f"{value:.2f}"


def build_report(summary: dict[str, Any], gate: dict[str, Any]) -> str:
    baseline = gate.get("baseline") if isinstance(gate.get("baseline"), dict) else {}
    tolerance = gate.get("tolerance") if isinstance(gate.get("tolerance"), dict) else {}

    completion_now = float(summary.get("completion_percent") or 0.0)
    completion_base = float(baseline.get("completion_percent") or 0.0)
    completion_delta = completion_now - completion_base

    wrong_now = int(summary.get("wrong_variant_flags") or 0)
    wrong_base = int(baseline.get("wrong_variant_flags") or 0)
    wrong_delta = wrong_now - wrong_base

    completion_drop_limit = float(tolerance.get("max_completion_drop_pct_points") or 0.0)
    wrong_increase_limit = int(tolerance.get("max_wrong_variant_increase") or 0)

    completion_pass = completion_now >= (completion_base - completion_drop_limit)
    wrong_pass = wrong_now <= (wrong_base + wrong_increase_limit)
    gate_pass = completion_pass and wrong_pass

    unresolved = summary.get("unresolved_classification") if isinstance(summary.get("unresolved_classification"), dict) else {}
    why_missing = summary.get("why_missing") if isinstance(summary.get("why_missing"), dict) else {}
    hint_counts = why_missing.get("hint_counts") if isinstance(why_missing.get("hint_counts"), dict) else {}

    lines = [
        "# Music Search Benchmark Delta",
        "",
        f"- Dataset: `{summary.get('dataset_name')}`",
        f"- Tracks resolved: `{int(summary.get('tracks_resolved') or 0)}/{int(summary.get('tracks_total') or 0)}`",
        f"- Gate result: `{'PASS' if gate_pass else 'FAIL'}`",
        "",
        "## Before vs After",
        "",
        "| Metric | Baseline | Current | Delta | Gate |",
        "|---|---:|---:|---:|---|",
        (
            "| Completion % | "
            f"{completion_base:.2f} | {completion_now:.2f} | {_fmt_delta(completion_delta)} | "
            f"{'PASS' if completion_pass else 'FAIL'} (limit: -{completion_drop_limit:.2f}pp) |"
        ),
        (
            "| Wrong variant flags | "
            f"{wrong_base} | {wrong_now} | {wrong_delta:+d} | "
            f"{'PASS' if wrong_pass else 'FAIL'} (limit: +{wrong_increase_limit}) |"
        ),
        "",
        "## Run Snapshot",
        "",
        f"- Avg selected score: `{float(summary.get('avg_selected_score') or 0.0):.4f}`",
        f"- Avg candidate score: `{float(summary.get('avg_candidate_score') or 0.0):.4f}`",
        (
            "- Unresolved: "
            f"`source_unavailable={int(unresolved.get('source_unavailable') or 0)}`, "
            f"`no_viable_match={int(unresolved.get('no_viable_match') or 0)}`"
        ),
        "",
        "## Why Missing Hints",
        "",
    ]

    if hint_counts:
        for label, count in sorted(hint_counts.items(), key=lambda item: (-int(item[1]), item[0])):
            lines.append(f"- {label}: `{int(count)}`")
    else:
        lines.append("- none")

    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate markdown delta report for music benchmark.")
    parser.add_argument("--summary", required=True, help="Benchmark summary JSON path.")
    parser.add_argument("--gate-config", required=True, help="Benchmark gate JSON path.")
    parser.add_argument("--output-md", required=True, help="Output markdown report path.")
    args = parser.parse_args(argv)

    summary = _load_json(Path(args.summary))
    gate = _load_json(Path(args.gate_config))
    report = build_report(summary, gate)
    out_path = Path(args.output_md)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
