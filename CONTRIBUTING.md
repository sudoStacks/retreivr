# Contributing
Thanks for taking the time to contribute.

## Quick guidelines
- Keep changes focused and minimal; avoid large refactors unless discussed first.
- Do not commit secrets, tokens, logs, or local config files.
- Keep config.json semantics stable unless explicitly agreed.
- Use Python 3.11 for local development.

## Ways to help
- Bug reports with clear reproduction steps and logs.
- Small fixes (UI polish, docs clarity, error handling).
- Improvements to Docker docs and deployment examples.

## Development notes
- Install deps: `pip install -r requirements.txt`
- Run the API: `uvicorn api.main:app --host 127.0.0.1 --port 8000`
- Open the UI at `http://127.0.0.1:8000`

## Submitting a PR
- Describe the problem and why the change is needed.
- Keep diffs readable and avoid unnecessary formatting changes.
- Update docs if behavior or UX changes.

## Search Benchmark Policy
- Any PR that changes search ladder/scoring behavior or matching thresholds must pass `music-search-benchmark` CI.
- The benchmark gate is mandatory for those PRs: no completion/precision regression beyond configured tolerance.
- The PR must include the benchmark delta output published by CI (artifact + job summary markdown).
- The CI run publishes two artifacts for review:
  - `music_search_benchmark_results.json` (raw summary)
  - `music_search_benchmark_report.md` (before/after delta summary vs baseline)
- Before pushing search-related changes, run locally:
  - `python -m benchmarks.music_search_benchmark_runner --dataset benchmarks/music_search_album_dataset.json --output-json benchmarks/music_search_benchmark_results.json --gate-config benchmarks/music_search_benchmark_gate.json --enforce-gate`
  - `python -m benchmarks.music_search_benchmark_report --summary benchmarks/music_search_benchmark_results.json --gate-config benchmarks/music_search_benchmark_gate.json --output-md benchmarks/music_search_benchmark_report.md`

## Changelog Policy
- Every PR that changes search ladder/scoring/matching behavior must add a `CHANGELOG.md` entry under the target release.
- CI enforces this for search/scoring file changes.
- Changelog entries must include:
  - What changed
  - Why it changed
  - Benchmark impact summary (completion delta and wrong-variant delta)

## Code style
- Prefer explicit and readable code over cleverness.
- Keep comments short and only where they add clarity.
