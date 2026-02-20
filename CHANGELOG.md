# Changelog

All notable changes to this project will be documented here.

## v0.9.5 — Music Mode Hardening + Playlist File Import

### Added
- Music Mode UI race-safety guardrails:
  - in-flight metadata search sequence token to prevent stale render after toggle changes
  - render-time guard to skip metadata paint when Music Mode is no longer enabled
- Album enqueue UX summary in Home:
  - reports `Album queued: X tracks`
  - reports skipped count when partial success occurs
  - optional expandable failed-track details list (track + reason)
- Music-track yt-dlp pipeline invariant logging:
  - structured `music_track_opts_validated` event for each music_track job
  - structured `music_track_opts_invalid` event and early fail on invalid audio opts
- Runtime regression tests for music-mode hardening:
  - music track yt-dlp format assertion (`bestaudio/best` + audio postprocessing)
  - metadata probe fallback retry behavior (`bestaudio/best` → `best`)
  - album download partial-success response behavior (per-track failures isolated)
  - duration-threshold split coverage (strict default vs album import override)
- Playlist file import parsing layer for:
  - M3U/M3U8
  - CSV
  - Apple Music XML/plist-style exports
  - Soundiiz JSON
- MusicBrainz-first import resolution pipeline (`TrackIntent` → MB recording match → `music_track` enqueue).
- `POST /api/import/playlist` backend endpoint (multipart upload, validation, structured summary).
- `POST /api/import/playlist/{batch_id}/finalize` endpoint to generate M3U from completed rows only.
- CLI `--import-file` support to process playlist files without UI.
- CLI `--import-finalize-m3u` opt-in finalize flow (completed rows only).
- Home UI import controls (file input + import action + result summary).
- Stable `import_batch_id` attached to import result and all import-enqueued jobs.
- Canonical M3U generation from `download_history` completed entries only.
- Explicit `music_final_format` config support (`mp3` default) alongside `final_format` (`mkv` default for video).
- Regression tests to enforce media pipeline separation:
  - music jobs must build audio-only yt-dlp options
  - video jobs must build video yt-dlp options
  - invariant guard must fail if music opts include video merge fields
- Hardened music-track scoring regression suite:
  - correct match acceptance
  - 30s preview rejection
  - live/acoustic rejection
  - remaster penalty
  - cover-artist rejection
  - album-mismatch penalty
  - YouTube Music source preference
  - deterministic tie-break ordering validation
- Mandatory release-enrichment regression coverage:
  - missing track metadata is synchronously enriched from MusicBrainz before path build
  - invalid/no-official-album release cases fail fast before folder creation
  - missing release-group metadata is completed during enrichment
  - no fallback to `Unknown Album` for music-track path generation
- MB bound-pair regression coverage:
  - deterministic recording→release→release-group binding validation
  - US-release preference and album-hint preference checks
  - deterministic tie-break by release identifier when candidates are otherwise equal
- End-to-end MB-binding acceptance suite across all music acquisition paths:
  - import path MB-bound before enqueue
  - manual search enqueue MB-bound before enqueue
  - direct URL music enqueue MB-bound before enqueue
  - deterministic same-input MB pair selection checks
  - fail-fast validation when no acceptable MB pair exists
  - log-order checks proving `mb_pair_selected` occurs before `job_enqueued`
- Music title-normalization regression coverage for Music Mode MB binding:
  - strips transport-noise suffixes/tokens (`[Music Video]`, `(Official Video)`, `(Official Audio)`, `visualizer`)
  - preserves semantic variants like `live` for downstream scoring/rejection behavior
  - verifies standard (non-music) mode is unaffected
- Music Mode verification scenarios for noisy title binding and live-variant rejection.
- Bucket-priority regression coverage for MB pair selection:
  - album beats compilation when both pass
  - compilation beats single when no album survives
  - single fallback allowed only when no album/compilation candidates survive
  - compilation is rejected on explicit album-hint mismatch
  - deterministic repeated-input selection for `(recording_mbid, mb_release_id, mb_release_group_id)`
- Music Mode metadata-first search/enqueue API:
  - `GET /api/music/search` returns MB-bound recording candidates (no transport IDs)
  - `POST /api/music/enqueue` enqueues a selected canonical MB recording payload
- Import failure diagnostics visibility:
  - persisted `music_failures` rows with batch/artist/track/reasons/query context
  - `GET /api/music/failures` status endpoint
  - Status page “Music failures” panel with count + list + refresh
- Regression tests for:
  - manual candidate parsing/binding inputs
  - MB bucket fallback behavior (single fallback, compilation precedence)
  - MB threshold override behavior
  - search destination NameError smoke path

### Changed
- Home defaults are now explicit and stable on first load:
  - Music Mode forced OFF at init
  - Music Search panel hidden by default
  - Standard search shown by default
  - Import Playlist remains collapsed unless explicitly toggled
- Music Search panel is now metadata-first only:
  - removed legacy destination override controls from the panel
  - removed top-result “Search & Download” button from panel actions
  - retained Artist / Album / Track / Mode / Max candidates + Search
- Music Search `Max candidates` now matches backend guardrail:
  - UI cap set to 15
  - client request limit clamped to 1..15 before API call
- Album enqueue success messaging now treats partial results as success (not error) while surfacing skipped-track diagnostics.
- MusicBrainz duration-reject diagnostics now emit structured delta details (`duration_reject_detail`) for album-context or explicit duration-override paths.
- Music-mode yt-dlp selector now uses `bestaudio/best` (removed stricter audio codec filter) while preserving canonical audio extraction postprocessors.
- Music downloads now pass canonical `output_template` metadata through the shared yt-dlp options builder path (no separate inline music opts branch).
- Album download track binding now uses a wider duration tolerance (`25s`) while single-track/manual binding keeps strict default duration tolerance.
- Music Mode enqueue now enforces MBID-based contracts on music-specific paths.
- Album enqueue observability now reports structured summary fields (`added`, `skipped_existing`, `skipped_completed`).
- Worker/state hardening completed to reduce orphan-risk for CLAIMED/DOWNLOADING transitions.
- Import pipeline now has zero fallback to generic transport search (no SearchResolutionService, no adapter URL query fallback).
- Unified DownloadJob payload construction across import/Spotify/search/direct/scheduler via canonical builder (`build_download_job_payload`) with stable output_template schema and final_format propagation.
- Output-template schema now always carries both:
  - `final_format` (video container)
  - `music_final_format` (music audio format)
- Music execution contract is now strict:
  - music media types always run audio-mode yt-dlp
  - output codec comes from `music_final_format`
  - video `final_format` no longer leaks into music imports/downloads
- Web UI config note now clarifies current single “Final format” field is video-oriented; music uses `music_final_format` in config.
- Music-track candidate selection now evaluates all scored candidates and picks the best eligible match (no first-hit selection).
- Deterministic music tie-break ordering is now explicit:
  1. higher source priority
  2. smaller duration delta
  3. lower title-noise score
- Added optional `debug_music_scoring` config flag:
  - when enabled, logs per-candidate score components, penalties, duration delta, final score, and acceptance decision
  - when disabled, verbose per-candidate scoring logs are suppressed
- Music-track file finalization now enforces synchronous, deterministic release enrichment before canonical folder construction.
- Canonical music path build now hard-fails if required release metadata is incomplete (`album`, `release_date`, `track_number`, `disc_number`, `mb_release_id`, `mb_release_group_id`).
- Deterministic release enrichment now:
  - resolves recording releases with MusicBrainz release/release-group/media includes
  - filters to Official Album releases only
  - prefers release hint when present
  - applies stable ordering (release date asc, release id lexicographic)
  - extracts disc/track from matched recording in release media
  - raises explicit enrichment errors when no valid release or recording-track mapping is found
- Music-track canonical metadata now carries explicit bound-release keys (`recording_mbid`, `mb_release_id`, `mb_release_group_id`, `album`, `release_date`, `track_number`, `disc_number`) end-to-end before path build.
- Added structured MB-pair observability in import resolution:
  - `mb_pair_selected` with selected recording/release/release-group + track/disc/year/album
  - `mb_pair_selection_failed` with explicit reason list (`no_official_album`, `no_us_release`, `track_not_found_in_release`, etc.)
- MB binding now converges through a single shared scorer (`engine/musicbrainz_binding.py`) to avoid per-path drift.
- Best MB match selection now scores recording+release pairs with:
  - correctness-first weighting (artist/title/duration/variant rejection)
  - completeness-secondary weighting (release-group/date/track+disc/album + metadata bonuses)
  - deterministic tie-break ordering for stable repeatable selection
- Release classification now uses deterministic MB release buckets (`album`, `compilation`, `single`, `excluded`) before final selection.
- Bucket priority weighting is now explicit in final selection scoring (`album` 1.00, `compilation` 0.96, `single` 0.92), applied only after hard rejects and correctness-floor checks.
- Explicit album hints now guard against random compilation matches via `compilation_album_mismatch` rejection when similarity is too low.
- Final candidate pool now enforces album/compilation-first selection with single fallback only when no album/compilation candidates survive.
- MB binding observability now logs selected bucket details (`bucket`, `bucket_multiplier`) and per-run failure bucket counts (`bucket_counts`) on `mb_pair_selection_failed`.
- Search request destination resolution now safely imports and resolves via `build_output_template` (fixes `build_output_template not defined` regression).
- Manual Music Mode candidate enqueue now derives MB binding inputs from selected candidate metadata (title/uploader/duration) instead of weak free-text query fields.
- Music thresholds are now explicit and separated:
  - `music_mb_binding_threshold` (pre-enqueue MB binding gate)
  - `music_source_match_threshold` (adapter-source candidate acceptance gate)
  - post-download enrichment threshold remains under `music_metadata.confidence_threshold`
- Current UI threshold wiring now drives both music binding + source match thresholds, with enrichment threshold displayed separately.
- Home Music Mode now supports metadata-first UX:
  - Music Mode search calls `/api/music/search` and renders canonical MB result cards
  - card-level Download actions call `/api/music/enqueue`
  - non-music (normal) search flow remains unchanged
- Home page layout now hosts the Media Search Console directly in Music Mode:
  - moved from Advanced into Home
  - gated by `music-mode-toggle`
  - standard free-text search and import panels are wrapped in explicit Home containers
- Music Mode Home submit now hard-switches to metadata-only flow:
  - calls `GET /api/music/search`
  - skips standard adapter-backed `/api/search/requests` polling/render path when Music Mode is ON
- Home Music Mode submit now has an explicit top-level short-circuit guard:
  - checks `music-mode-toggle` first
  - calls `performMusicModeSearch(query)` and returns immediately
  - prevents legacy `/api/search/requests` execution in Music Mode
- `/api/music/search` now supports mode-specific metadata search params:
  - `q`, `mode` (`auto|artist|album|track`), `offset`, `limit`
  - returns grouped MusicBrainz metadata (`artists`, `albums`, `tracks`) with `mode_used`
- Music Mode frontend search now exclusively calls:
  - `GET /api/music/search?q=...&mode=...&offset=0&limit=50`
  - no legacy request creation, polling, or resolution calls in Music Mode path
- `/api/music/album/download` now accepts `release_group_mbid` and enqueues album tracks via MB release-group → release → tracklist expansion before worker acquisition.
- Home album-card enqueue payload now sends `release_group_mbid` (not legacy `release_group_id`) and confirms `tracks_enqueued` after queueing.
- Music Mode metadata render target is now isolated:
  - renders only into `#music-results-container`
  - no writes from music metadata renderer into legacy home/advanced polling result containers
- Music toggle show/hide behavior is now symmetric:
  - ON: shows `#music-mode-console`, hides `#standard-search-container`
  - OFF: hides music console, shows standard search, clears `#music-results-container`, resets legacy search state
- Music Mode now defaults to OFF on initial Home render:
  - `music-mode-toggle` is unchecked by default
  - init guard forces standard-search-visible + music-console-hidden when toggle is off
- Music/Search visibility defaults are now enforced in markup:
  - `#music-mode-console` is hidden by default via HTML class
  - `#import-playlist-panel` is hidden by default via HTML class
- Music console controls are now metadata-only:
  - replaced legacy intent selector with `#music-mode-select` (`auto|artist|album|track`)
  - removed min score/source priority/include/lossless controls from music metadata console
- Music console title updated from `Media Search Console` to `Music Search`.
- Track enqueue from Music Mode now posts MB IDs only:
  - payload is limited to `recording_mbid` and `release_mbid`
  - no title/query/adapter payload fields are sent from track cards
- Legacy source-selection validation now runs only in standard mode:
  - “Select at least one source” is bypassed in Music Mode metadata search flow
- Legacy Media Search Console submit handlers remain disabled so no parallel music submit path executes.
- Home search flow is explicitly single-path for Music Mode:
  - Home submit -> `submitHomeSearch` -> hard short-circuit -> `performMusicModeSearch` -> return
- Music toggle controller is now cleanly event-driven:
  - no load-time auto-show of music console
  - console visibility changes only on `music-mode-toggle` change
  - standard toggle remains always visible
- Import playlist panel controller is now cleanly event-driven:
  - `import-playlist-toggle` only toggles hidden state
  - no automatic unhide on page load
- Music Mode search now uses structured backend fields end-to-end:
  - `/api/music/search` accepts `artist`, `album`, `track`, `mode`, `offset`, `limit`
  - no single-field `q` composition in endpoint handling
  - frontend `performMusicModeSearch` sends structured params directly
- Music metadata search query construction is now strictly fielded:
  - artist-only -> `search_artists`
  - album routes -> `search_release_groups`
  - track routes -> `search_recordings`
  - combined artist/album/track routes use explicit field conjunctions
- `/api/music/search` discovery path is now confirmed metadata-only:
  - no adapter search calls
  - no `resolve_best_mb_pair` invocation in search endpoint path
  - no transport scoring or duration acceptance logic in metadata search
- Search resilience guardrails added for MusicBrainz metadata calls:
  - hard cap `limit <= 15`
  - immediate empty structured response when all fields are empty
  - MB call exceptions are swallowed in search path and return structured empty/partial results instead of HTTP 500
- Pre-enqueue MB binding is now enforced for all music acquisition paths (import, manual search enqueue, direct URL music enqueue).
- Worker music-track execution now enforces binding invariants (`recording_mbid` + `mb_release_id`) before adapter resolution.
- Manual/direct enqueue paths now prioritize canonical metadata fields for expected artist/track/album/duration inputs used by music scoring.
- Music client-delivery fast-lane bypass is now blocked for music jobs so MB binding is never skipped.
- Music Mode enqueue failure now returns structured API errors for MB bind failures (`music_mode_mb_binding_failed`) with reason payload.
- Web UI now shows explicit fail-fast messaging when Music Mode binding is rejected: `Music Mode rejected — No canonical album release found`.

### Fixed
- Fixed `sqlite3.Row` `.get()` misuse in queue row handling by normalizing rows to dicts before dict-style access.
- Added third music metadata-probe fallback attempt with no explicit `format` key after `bestaudio/best` and `best` retries.
- Music Mode no longer shows legacy “Select at least one source” validation in metadata search flow.
- Music metadata results no longer render after Music Mode is toggled OFF while requests are in flight.
- Album queue UI now correctly reflects backend HTTP 200 partial success responses instead of treating skipped tracks as fatal.
- Music metadata probe failures now retry with permissive probe format (`best`) before failing the job.
- Album download endpoint no longer returns HTTP 500 for a single-track failure; it returns HTTP 200 with partial success summary and failed-track reasons.
- Music adapter-search failure handling now logs structured failure context and applies retryability classification instead of hardcoded non-retryable failures.
- Canonical job dedupe race reduced with DB-level canonical ID uniqueness handling.
- Deterministic search ranking tie-break behavior stabilized in general resolution paths.
- Music path canonicalization enforced (disc folder presence, zero-padded tracks, NFC normalization).
- Fixed MusicBrainz recording include contract to prevent `InvalidIncludeError`:
  - recording fetch now uses only valid recording includes (`releases`, `artists`, `isrcs`)
  - release-group/media/recordings includes are fetched from release entity lookups only

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
