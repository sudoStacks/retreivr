# Changelog

All notable changes to this project will be documented here.

## v0.9.8 — Fast Discovery + Video Preview + Adapter Extensibility

### High-Level
This release focuses on homepage search UX and discovery speed: incremental/as-resolved rendering, lightweight video discovery, and embedded preview support for supported sources, while keeping deterministic acquisition/download behavior unchanged.

### Added
- Homepage video preview flow with `Preview` action on result cards for supported adapters.
- Preview can be triggered from video title/thumbnail clicks (same behavior as `Preview` button).
- Structured custom adapter framework with user-facing `config/custom_search_adapters.example.yaml` template.
- New video discovery adapters integrated for `rumble` and `archive_org`.
- Discovery timing instrumentation and source-level adapter progress logging.
- New API endpoint `GET /api/version/latest` to resolve latest version from GHCR tags (with release fallback).

### Changed
- Search discovery pipeline refactored for lightweight-first behavior:
  - canonical metadata resolution removed from initial generic/video discovery path
  - strict per-source timeout budget for discovery
  - bounded discovery result set per source
  - fallback/retry behavior for timed-out sources without blocking first visible results
- YouTube lightweight discovery migrated to Innertube search (`youtubei/v1/search`) for fast candidate return.
- Homepage results rendering now updates progressively as adapters resolve (no all-at-once batch wait).
- Candidate row rendering is now non-blocking: rows render first, job-state enrichment follows asynchronously.
- Home polling loop hardened to prevent overlapping async polls and reduce UI update latency.
- Search DB connections tuned for concurrent read/write responsiveness (`WAL`, `busy_timeout`) during progressive candidate writes.
- Source selection UX refined:
  - dynamic source list from backend
  - video-mode source filtering to remove non-video adapters from the selection list
- Info page version check now tracks GHCR-published versions (container-first) instead of browser-side GitHub release lookup.

### Removed
- Legacy local YouTube search cache pipeline and schema artifacts (`search_query_cache` and related indexes).
- `x` and `bitchute` default source adapter exposure from active source selection paths.

### Fixed
- Multi-source visibility issue where one adapter’s results could hide or starve others in combined searches.
- Homepage candidate refresh race conditions that caused delayed or dropped incremental rows.
- Rumble/archive preview wiring and embed behavior reliability on homepage preview modal.
- Search-card metadata regressions (including posted-date rendering on result cards).

## v0.9.7 — Community Cache + Local Search Cache + Watcher/Telegram Hardening

### High-Level
This release improves cache-first search/resolution performance, stabilizes watcher/Telegram reporting, and refines music/music-video UX without changing deterministic scoring thresholds.
Added Music Video Mode with similar UX as Music Mode, but attempts to download official music videos from official channels as priority while still embedding musicbrainz metadata.

### Added
- Community transport cache lookup + publish outbox (local JSONL proposals, opt-in).
- Local SQLite search cache for homepage search with cache-first replay + background refresh.
- Deterministic community reverse-index rebuild from local dataset snapshots.
- Music/MV metadata-first UX upgrades: MB IDs on cards, artwork, album/track navigation improvements.

### Changed
- Homepage search now surfaces cached candidates immediately and continues normal adapter resolution in parallel.
- Candidate dedupe now merges provenance/metadata deterministically and removes cache/adapter duplicate rows.
- Community lookup remains hint-only and is injected before normal ladder execution; fallback behavior is unchanged if cache is missing/unavailable.
- Config defaults/schema expanded for community lookup/publish and local search-cache controls with backward-compatible defaulting.
- Generic/video scoring now includes a small logarithmic `view_count` tie-break bonus when metadata already contains view counts (no extra fetch calls).
- Navigation cleanup: page naming/hash alias behavior aligned around `Advanced`.

### Fixed
- Search/control-flow safety fixes so cache failures/timeouts do not block request resolution.
- Restricted-content filtering improved for adapter normalization and cache replay.
- Telegram scheduler/watcher summaries hardened: cleaner headers, title-first item labels, duplicate dispatch reduction.
- Watcher stability improvements: batch-level messaging behavior, restart resilience, and non-blocking polling paths.
- Music UI regressions fixed across `View Tracks`, card actions, artwork loading/layout, and mode-specific interaction behavior.

## v0.9.6 — Runtime Distribution + Music Match Robustness

### High-Level
This release focuses on three themes: faster music retrieval/download throughput, safer/more deterministic matching, and better runtime visibility. It also adds Docker-first runtime packaging and tightens import/job execution behavior for large queues.

### Added
- Runtime distribution: GHCR versioned images, release bundle zip assets, and `README-runtime.md`.
- Deterministic music benchmark gate with CI artifacts (`results` plus markdown delta report).
- Runtime album `run_summary.json` diagnostics (completion, unresolved classes, rejection mix, per-track detail).
- Import-only low-confidence quarantine flow to `Music/Needs Review` via `music_track_review`.

### Changed
- Music matching/retrieval pipeline hardened and made faster without lowering gates:
  - deterministic multi-rung fallback and bounded duration expansion
  - per-rung source retrieval parallelized across `youtube_music`, `youtube`, `soundcloud`, `bandcamp` with deterministic merge
  - candidate pre-resolution overlaps with active downloads, with bounded lookahead (`music_preresolve_lookahead`, default `3`, cap `4`)
  - stronger in-process MB resolution cache reuse for repeated `(recording_mbid, release_mbid)` pairs
  - conservative default fragment concurrency for music downloads (`yt_dlp_opts.concurrent_fragment_downloads=2`, overrideable)
- Import/runtime throughput improvements:
  - bounded parallel MB binding (`import_mb_binding_workers`, default `4`, cap `5`)
  - import row dedupe before MB lookup for exact `(artist/title/album)` repeats, while preserving per-track enqueue/progress fan-out
  - worker poll interval reduced from `5s` to `1s`
  - bounded same-source job concurrency via per-source semaphores (default `2`, cap `4`; configurable with `max_concurrent_jobs_per_source` or `source_concurrency`)
- EP-aware handling expanded:
  - EP retrieval refinement rung for album runs
  - EP release groups included in album metadata search (`primarytype:album OR ep`)
  - EP releases accepted in album-run pair resolution and enrichment fallback
- Scoring/gating robustness improved:
  - featured-artist normalization (`feat/ft/featuring`) and `album_artist` aware artist/channel scoring
  - source-aware album gating for weak-metadata adapters (`youtube_music`, `youtube`, `soundcloud`)
  - clearer retrieval-vs-ranking separation with `recall@k` benchmark metrics (`k={1,3,5,10}`)
- Runtime observability expanded:
  - decision-edge diagnostics, richer rejected-candidate metadata, injected-candidate rejection mix, EP refinement telemetry, variant tagging, benchmark motif rollups
  - lightweight runtime metrics for per-source active slots, queue age, resolve latency, and resolution-cache hit rate
- UI/status usability refresh:
  - full-width `Operations Status` and `Download Queue`
  - wider/scrollable queue table using user-facing title column
  - `Clear Failed` queue action
  - `Metrics` panel moved to Info
  - Home search now routes YouTube playlist URLs through playlist enqueue on `Search & Download` (instead of direct-URL mode), while `Search` shows a clear prompt to use download mode for playlist URLs

### Fixed
- Reduced false-negative music rejects tied to featured-credit tokenization and missing album metadata.
- Corrected playlist/library import progress reporting (completed-work snapshots, no pre-item drift, finalization shown as active).
- Fixed MusicBrainz include-contract error in album metadata fetch (`genres` include misuse).
- Fixed MP4 post-processing regression where incompatible audio could persist.
- Fixed Home/header UI consistency regressions after layout/navigation changes.
- Fixed worker retry-loop regression causing duplicate downloads after successful writes (adapter store wiring and post-download crash path).

## v0.9.5 — Music Mode Hardening + Playlist File Import

### Added
- MusicBrainz-first Music Mode API surface:
  - `GET /api/music/search` (metadata-only discovery)
  - `POST /api/music/enqueue` (track enqueue from canonical MB payload)
  - `POST /api/music/album/download` (album enqueue by release group)
- Playlist file import flow with batch finalize support for M3U/M3U8, CSV, Apple Music XML/plist, and Soundiiz JSON.
- Music failure diagnostics persistence (`music_failures`) with status visibility.
- Regression test coverage expanded across MB binding/scoring, yt-dlp option contracts, import behavior, and Music Mode UI flow.

### Changed
- Enforced strict MB-bound canonical metadata for `music_track` before enqueue/execution, with fail-fast behavior instead of degraded naming fallbacks.
- Hardened deterministic MB pair selection and bucket handling (album > compilation > single fallback), including stronger duration/variant rejection and stable tie-breaks.
- Consolidated download payload/option authority: unified enqueue payload building, explicit `final_format` (video) vs `music_final_format` (audio), and consistent `audio_mode` handling for music.
- Aligned direct URL and worker paths on canonical yt-dlp option/CLI building with controlled retry escalation and safer non-fatal metadata probing.
- Stabilized Music Mode UX into a metadata-first Home flow with toggle gating, stale-response protection, and a single authoritative track enqueue path.
- UI/UX fixes and updates across Home search, advanced options layout, status indicators, and delivery controls.
- Unified visual theme across Home/Info/Status/Config and renamed the `Advanced Search` nav label to `Info` for clearer navigation semantics.
- Client delivery was brought back on Home flows and now obeys the delivery toggle for both candidate and direct URL downloads.
- Playlist file import now runs as a background job (`POST /api/import/playlist` returns `202` + `job_id`) with progress polling via `GET /api/import/playlist/jobs/{job_id}` to avoid blocking the request path.
- Scheduler, watcher, and scheduled Spotify sync ticks now pause while playlist imports are active to reduce runtime contention and UX conflicts.
- Home import UX now enforces explicit confirmation, disables import controls while running, and displays a live progress modal with status counters and errors.
- Album-run canonicalization now enforces one release context across all queued tracks:
  - consistent `album`, `album_artist`, `release_date`, `mb_release_id`, `mb_release_group_id`, and `artwork_url`
  - consistent album-wide genre best-effort (MusicBrainz release/release-group first, resolved fallback only when available)
- Music library pathing now always keys artist folders from canonical `album_artist` (not per-track featured artist credits).
- Music path builder now creates `Disc N` folders only when `disc_total > 1` (single-disc albums no longer force `Disc 1`).
- Album-run queue payloads now propagate `track_total` and `disc_total` for downstream tag correctness.
- Metadata worker now prefers album-run `artwork_url` for all tracks in an album run, with release-art fallback only when needed.
- Consolidated music path contract authority across both runtime workers:
  - shared relative music layout builder now drives both `engine.job_queue` and `download.worker`
  - single source of truth for Disc-folder inclusion rules
- Consolidated shared music contract helpers for integer parsing/track formatting and canonical metadata coercion to reduce drift between worker stacks.
- Consolidated music root resolution logic into shared path utilities used by metadata download worker flows.

### Fixed
- MusicBrainz recording include contract errors (`InvalidIncludeError`) in binding fetch paths.
- Search destination regression caused by missing `build_output_template` import.
- Direct URL preview blank-card fallback by returning safe title/uploader/thumbnail values on extraction failure.
- Album enqueue hard-failure behavior: partial track failures now return a success summary instead of a full 500 response.
- Fixed Home UI state drift and delivery-mode edge cases for clearer, consistent behavior.
- Fixed album-mode metadata drift where featured-artist tracks could branch into separate artist folders.
- Fixed inconsistent album artwork across tracks in the same album run by enforcing run-level cover art preference.
- Fixed missing/partial track/disc total tag writes so players can reliably show `Track X of Y` and `Disc A of B`.
- Fixed lingering single-disc `Disc 1` folder creation in the secondary download worker path by migrating it to the shared layout authority.

## v0.9.4 — Filesystem Layout Stabilization

### Changed
- Docker runtime now defaults to canonical root-level directories:
  /downloads, /data, /config, /logs, /tokens
- No RETREIVR_*_DIR overrides required in container deployments
- Unified database path resolution (removed legacy RETREIVR_DB_PATH fallbacks)

### Migration Notes
- Legacy /app/data deployments must remap volumes
- Optional SQL path rewrite may be required if absolute paths were stored


## [v0.9.3] – Canonical Authority & Scheduler Hardening

This release establishes Retreivr’s canonical authority model and locks in deterministic orchestration behavior. v0.9.3 is a stability milestone focused on correctness, idempotency, and clean archival output.

### Highlights

- MusicBrainz is now the canonical metadata authority.
- Spotify downgraded to optional intent ingestion (OAuth + Premium required).
- Spotify API usage (playlists, saved tracks, metadata hints) now strictly requires OAuth configuration and an active Premium subscription.
- Deterministic playlist snapshot hashing and diffing.
- Idempotent scheduler ticks (no duplicate enqueues).
- MKV set as the default video container.
- Integration tests added for full pipeline and snapshot behavior.

---

### Added

- Structured `PlaylistRunSummary` with:
  - `added`
  - `skipped`
  - `completed`
  - `failed`
- Stable playlist snapshot hashing using normalized item sets.
- Crash-safe restart behavior for scheduler runs.
- Active-job duplicate detection (queued / claimed / downloading / postprocessing states).
- Integration tests covering:
  - Full music flow (search → resolve → download → embed → persist)
  - Spotify intent conversion (MB-first enforcement)
  - Playlist reorder behavior (no re-enqueue)
  - Crash/restart idempotency
- MKV default container policy for video downloads.

---

### Changed

- Canonical metadata resolution is now MusicBrainz-first in all ingestion paths.
- Spotify metadata is treated as hints only and never overrides MusicBrainz canonical results.
- Legacy resolver paths removed.
- Duplicate MusicBrainz client stacks consolidated into a single service layer.
- Canonical naming enforced:
  - No video IDs in filenames
  - No upload dates in filenames
  - Zero-padded music track numbers
- Video metadata embedding now occurs after final container merge, ensuring metadata survives remux.
- Scheduler diff logic hardened to ignore reorder-only changes.
- Snapshot persistence made deterministic to prevent unnecessary DB churn.

---

### Fixed

- Prevented duplicate active-job enqueue on scheduler restart.
- Eliminated reorder-triggered playlist re-downloads.
- Fixed snapshot instability caused by unordered playlist items.
- Prevented metadata failures from corrupting or blocking completed downloads.

---

### Notes

- This release prioritizes stability over new feature expansion.
- v0.9.3 marks the transition to a canonical, deterministic ingestion engine.
- MKV is now the default video container to preserve codec fidelity and improve metadata support.
- Spotify integration depends on the official Spotify Web API and requires valid OAuth credentials plus Premium account validation; without these, Spotify playlist sync and metadata ingestion remain disabled.

## [v0.9.2] – Search Engine Dialed In // Home Page UI Update

Highlights

This release hardens the download pipeline (especially audio-only MP3), improves observability, and simplifies the Home UI ahead of broader feature work. Video downloads remain stable and unchanged.

⸻

🚀 Improvements • Reliable MP3 audio-only downloads • Audio mode now uses a robust bestaudio[acodec!=none]/bestaudio/best selector. • Prevents unnecessary video downloads when targeting MP3. • Matches known-working yt-dlp CLI behavior. • Works consistently for direct URLs and queued jobs. • Safer yt-dlp option handling • Avoids forced merge/remux unless explicitly required. • Reduces ffmpeg post-processing failures. • Audio and video paths are now clearly separated and predictable. • yt-dlp CLI observability • Job workers now log the exact yt-dlp CLI command executed (with secrets redacted). • Makes debugging format, cookie, and extractor issues significantly easier.

⸻

🧠 Behavior Fixes • Post-processing failures are now terminal • ffmpeg / post-processing errors correctly mark jobs as FAILED. • Prevents silent re-queue loops and misleading “Queued” states in the UI. • Video pipeline preserved • Default video behavior (bestvideo+bestaudio/best) remains unchanged. • MP4 / MKV / WebM downloads continue to work as before.

⸻

🎧 Music & Metadata • Music metadata enrichment remains optional • Failed or low-confidence enrichment no longer blocks successful downloads. • Clear logging when metadata is skipped due to confidence thresholds.

⸻

🖥 UI / UX • Home page cleanup • Reorganized source filters and advanced options into a single compact row. • Reduced visual noise without removing functionality. • Improved spacing and alignment for music mode, format, and destination controls. • Advanced Search remains available • Advanced functionality is still accessible via the dedicated Advanced Search page.

⸻

🧹 Internal / Maintenance • Improved internal option auditing logs. • Better separation between search, enqueue, and execution logic. • No schema or config migrations required.

⸻

⚠️ Known Notes • Client-side (“download to this device”) delivery is still being refined and may be disabled or hidden in some UI paths.

## [v0.9.1] – Runtime Stability & Direct URL Fixes

This release focuses on restoring and hardening runtime stability after refactors since yt-archiver v1.2.0. Primary goals were correctness, predictability, and eliminating regressions in downloads, scheduling, and search flows.

Fixed:

Restored reliable Direct URL downloads for video and audio (mp3/m4a/etc).
Corrected yt-dlp invocation for audio formats (uses extract-audio instead of merge-output-format).
Fixed Direct URL runs appearing permanently queued in the Home UI.
Prevented empty or zero-byte output files from being recorded as completed.
Fixed scheduler playlist downloads producing incorrect formats or audio-only output.
Ensured scheduler and direct downloads can run concurrently without interference.
Fixed missing database schema initialization for search-related tables.
Normalized all filesystem paths via paths.py and environment variables (Docker-safe).
Fixed Advanced Search “Failed to load requests” error caused by search DB store calling service-only logic.
Fixed Home screen results remaining stuck in “Queued” by restoring reliable search request status hydration.
Unified search job database usage to a single canonical path to prevent schema and state mismatches.
Changed:

Direct URL playlist links are now explicitly rejected with a clear user-facing error message.
Direct URL runs bypass the job queue but still report progress and completion via run status.
Search-only results can now be downloaded individually via the Home results UI.
Default video downloads respect configured format preferences (e.g., webm/mp4).
Metadata enrichment failures no longer block or corrupt completed downloads.
Notes:

Playlist URLs must be added via Scheduler / Playlist configuration, not Direct URL mode.
Kill-download button is not guaranteed during active runs and remains experimental.
Watcher functionality is present but considered beta and may change in later releases.

## [v0.9.0] – Retreivr Rebrand Release // Music Mode and Metadata
- Project renamed to Retreivr
- Repository migrated to new namespace
- Branding updated across documentation, UI, and Docker artifacts

Added:
- Music mode (opt-in per playlist and per single-URL run) with audio-focused metadata and music-safe naming.
- Smart Search feature - aggregating results from multiple top-tier sources and scoring quality and metadata acquisition, ranking results, and simplifying the entire media-searching process for users.
- yt-dlp cookies support (Netscape cookies.txt) for improved YouTube Music metadata.
- Music filename template support (artist/album/track hierarchy).
- Music metadata enrichment pipeline (MusicBrainz + optional AcoustID + artwork) with background tagging.
- UI controls for music mode, cookies, music template, and metadata enrichment options.
- Config UI controls for watcher enable, backoff timing, and downtime window.
- Per-playlist subscribe mode (only download new videos after first run).
- Single-playlist run controls in the Web UI.
- Single-URL delivery modes (server library or one-time client download via HTTP).
- Button to Kill downloads in progress (cancel active run from Status Section).
- Adaptive watcher with per-playlist state persisted in SQLite.
- Watch policy config with downtime windows and backoff timing.

Changed:
- Metadata flow now prefers yt-dlp music fields when music mode is enabled.
- Music metadata tagging can overwrite existing yt-dlp tags by default (configurable).
- Music URLs (music.youtube.com) auto-enable music mode for single URL runs.
- Music mode download URLs use music.youtube.com when enabled.
- Music mode respects video formats (does not force audio when final_format is a video).
- Download execution uses a native single-call path with explicit JS runtime/solver and a muxed-video requirement, with hardened fallback on failure.
- Watcher uses a single supervisor loop and deterministic scheduling.
- Downloads respect downtime windows and defer until downtime ends.
- Watcher batches detections with a quiet-window strategy and sends one Telegram summary per batch.
- Status now reports current phase and last error for active runs.

### Summary
This v0.9.0 release marks the official rebrand of the project from **YouTube-Archiver** to **Retreivr**.
The version number has been reset to reflect a new product identity and roadmap.

### Important Notes
- v1.2.0 was the **final release** under the YouTube-Archiver name.
- Versioning resumes at v0.9.0 under the Retreivr name.
- Functionality is unchanged in this release.
- Future releases may introduce breaking changes as APIs, environment variables, and UI contracts are stabilized.

|vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv|
|##################################|
|//////////////////////////////////|
|\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\|
|##################################|
|^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^|

## [v1.2.0] – Final YouTube-Archiver Release

### Summary
Final release published under the YouTube-Archiver name before rebranding.

### Changes

1.2.0 - OAuth Web UI Helper
Added:
- Web UI OAuth helper per account (launches Google auth URL + paste code to save token).
- API endpoints to support OAuth in the Web UI flow.

Notes:
- Docker pulls remain available via GHCR:
- `docker pull ghcr.io/sudoStacks/retreivr:latest`
- `image: ghcr.io/sudoStacks/retreivr:latest`

1.1.2 - Publish Docker to GHCR Latest
No changes from v1.1.1 - just creating new tag to automatically publish to GHCR repo to make docker pulls directly from there.

Docker Pull Command:
docker pull ghcr.io/sudoStacks/retreivr:latest

Docker Compose Line:
image: ghcr.io/sudoStacks/retreivr:latest

1.1.1 - Minor Patch
Added:
- prompts to verify you wish to remove items on 'Remove' button press

Changed:
- hardened version control by clearing caches.
- better date/time format in Status block.
- clearer log entries when Google Oauth runs to verify playlists.

1.1.0 - Version Control and YT-DLP Update Maintenance
Added:
- App version display and GitHub release check in Status.
- Manual yt-dlp update button (requires restart).

Changed:
- Dockerfile build arg for `YT_ARCHIVER_VERSION`.

1.0.1 - Frontend UI Updates
Added:
- Multi-page Web UI (Home, Config, Downloads, History, Logs) with top navigation.
- Separate playlist progress vs per-video download progress indicators.
- Downloads + History filters with limits, and internal scrolling tables.
- Mobile navigation menu + collapsible filters for small screens.
- Telegram summary truncation to avoid oversized messages.
- Playlist name field in config editor (non-functional label).

Changed:
- Default Downloads/History limit to 50.
- Light-mode header styling and mobile spacing tweaks.

Fixed:
- History “Copy URL” now copies the YouTube URL, not API download URL.
- Config/playlist download paths normalized to remain relative to /downloads.

1.0.0 - First public release
Added:
- Docker-first deployment with explicit volume paths.
- FastAPI backend serving API + static Web UI.
- Web UI for config, runs, status, logs, history, downloads, and cleanup.
- Built-in scheduler (no cron, no systemd).
- SQLite-backed history and state.
- Optional Telegram summaries and optional Basic auth.
- Home Assistant-friendly status and metrics endpoints via API.
- desktop GUI deprecated

Prior to 1.0.0
- no official release
- only python scripts released, no official package
- archiver.py ran independently
- a desktop GUI was created, paving way for the eventual webUI
