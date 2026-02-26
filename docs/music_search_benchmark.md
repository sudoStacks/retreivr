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
- Always runnable manually via `workflow_dispatch`.
