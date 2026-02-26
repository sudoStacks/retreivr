# Music Search Benchmark Harness

This benchmark is a deterministic, fixture-driven quality gate for Music Mode matching.

It is designed to improve recall without weakening canonical precision.

## Unavailable-content classification map

The runtime classifier maps known yt-dlp error signals to deterministic classes:

- `removed_or_deleted`
  - `video has been removed`
  - `this video is unavailable`
- `private_or_members_only`
  - `private video`
  - `members-only`
- `age_restricted`
  - `sign in to confirm your age`
  - `age-restricted`
- `region_restricted`
  - `not available in your country`
  - `geo-restricted`
- `format_unavailable`
  - `requested format is not available`
- `drm_protected`
  - `drm protected`

Transient network failures are excluded from unavailable classification (timeouts, connection resets, temporary failures, 5xx/service unavailable).

## What it measures

- Retrieval quality before ranking/gating:
  - `retrieval_metrics.recall_at_k` for `k={1,3,5,10}` based on `expected_selected_candidate_id` presence in retrieved candidates
- Album completion rate (`resolved tracks / total tracks`)
- Precision proxy: wrong-variant acceptance flags
- Rejection mix (aggregate candidate rejection reasons)
- Per-track rejection reason counts
- Unresolved split: `source_unavailable` vs `no_viable_match`
- Average selected candidate score
- Average scored-candidate score
- Album-coherence tie-break activity (`coherence_boost_events`, `coherence_selected_tracks`)
- Alias diagnostics via rejection mix (`low_title_similarity`)
- MB relationship injection diagnostics:
  - `mb_injected_success_total`
  - `mb_injected_rejection_mix` (for example `mb_injected_failed_duration`, `mb_injected_failed_variant`)

## MB Relationship Injection Design (Canonical-First)

- Relationship URLs are discovered from MusicBrainz metadata only (recording/release URL relationships).
- Only canonicalized YouTube watch URLs are considered for injection.
- Injected candidates are marked as `source=mb_relationship` for observability.
- Injection does **not** auto-accept: candidates still pass the full existing scoring and hard gates (title/artist similarity, duration bounds, variant blocking, and threshold checks).
- If injected candidates fail gates, the normal ladder continues unchanged.

## Retrieval/Ranking Split

Music Mode evaluation is explicitly split into two phases:

- `retrieve_candidates(ctx) -> [Candidate]`
  - Broad, fixture-driven candidate collection (high recall objective).
- `rank_and_gate(ctx, candidates) -> SelectionResult`
  - Existing strict scoring and acceptance gates (precision objective).

This split is reporting-only in terms of benchmark output; acceptance thresholds and gate behavior remain unchanged.

## Dataset

Dataset file:
- `benchmarks/music_search_album_dataset.json`

Gate config:
- `benchmarks/music_search_benchmark_gate.json`

Dataset shape:
- `albums[]` with MB release-group MBIDs, album metadata, and track MBIDs
- `fixtures{}` with deterministic candidate lists per rung

No network calls are needed. No media download is performed.

## Run locally

```bash
python3 -m benchmarks.music_search_benchmark_runner \
  --dataset benchmarks/music_search_album_dataset.json \
  --output-json benchmarks/music_search_benchmark_results.json
```

Run with regression gate:

```bash
python3 -m benchmarks.music_search_benchmark_runner \
  --dataset benchmarks/music_search_album_dataset.json \
  --output-json benchmarks/music_search_benchmark_results.json \
  --gate-config benchmarks/music_search_benchmark_gate.json \
  --enforce-gate
```

Run A/B without MB alias/disambiguation variants:

```bash
python3 -m benchmarks.music_search_benchmark_runner \
  --dataset benchmarks/music_search_album_dataset.json \
  --output-json /tmp/music_search_benchmark_no_alias.json \
  --disable-alias-matching
```

Alternative wrapper:

```bash
python3 scripts/music_search_benchmark.py --enforce-gate
```

Hard-negative fixture mining (review-only):

```bash
python3 scripts/music_hard_negative_miner.py \
  --input /path/to/run_summary.json \
  --output benchmarks/generated_fixture_stubs/hard_negative_fixture_candidates.json
```

The miner accepts either a direct `run_summary.json`/benchmark summary payload or wrapped artifacts
(for example `{"summary": {...}}` or `{"run_summary": {...}}`). It emits:

- `generated_fixtures`: compact fixture stubs with placeholder metadata for manual curation
- `album_stubs`: track/album placeholders ready to copy into dataset drafts
- `top_recurring_failure_motifs`: motif counts (wrong artist, lyric/remaster/live variants, duration drift, unavailable, etc.)

Generated output is intentionally review-only and must not be auto-merged into the benchmark dataset.

## Adding albums

1. Add a new album entry to `albums[]`:
- `album_id` (stable slug)
- `artist`
- `title`
- `release_group_mbid`
- optional `notes`
- `tracks[]` with:
  - `recording_mbid`
  - `track`
  - `duration_ms`
  - `fixture` (fixture key)

2. Reuse an existing fixture or add a new one in `fixtures{}`.

3. Run benchmark and inspect:
- unresolved reasons
- rejection mix
- wrong-variant flags
- coherence metrics (boost events + boosted selections)

4. If this is an intentional baseline improvement, update gate baseline values in:
- `benchmarks/music_search_benchmark_gate.json`

## CI

Workflow:
- `.github/workflows/music-search-benchmark.yml`

Default behavior:
- PRs that touch search ladder/scoring files run this workflow automatically.
- The benchmark gate is enforced (`--enforce-gate`), and failing tolerance blocks CI.
- CI publishes:
  - `music_search_benchmark_results.json`
  - `music_search_benchmark_report.md`
- CI also runs a runtime smoke contract test for album `run_summary.json` schema stability.
- Always runnable manually via `workflow_dispatch`.

## Governance

- Search scoring logic is benchmark-gated.
- Threshold or hard-gate changes must include all of:
  - explicit gate-config update (`benchmarks/music_search_benchmark_gate.json`)
  - updated benchmark baseline values
  - changelog note summarizing precision/completion impact
- Dataset-only tuning that effectively relaxes acceptance behavior is not allowed.
