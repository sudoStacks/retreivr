# Music discovery, playlist review and Jellyfin synchronization

Retreivr now has an additive **Music → Discover & Sync** view. Existing Import,
Music playback, Review, Spotify import, metadata and download workflows remain
available. The new view stages discovery for review rather than immediately
acquiring music.

## Architecture

- `metadata/importers/dispatcher.py` and `TrackIntent` remain the parser interface.
  Apple XML also exposes `parse_playlists()` to retain named playlist membership,
  source IDs, source order and deliberate repetitions. CSV, M3U/M3U8 and Soundiiz
  JSON use their existing adapters. MusicBrainz lists can be added as adapters.
- `engine/discovery_identity.py` scores recording identity, collapses duplicate
  recording candidates, checks duration and existing variant rules, and requires
  a confidence margin. Ambiguous and probable matches need an explicit choice.
- `engine/discovery.py` stages identities, checks the existing library index and
  calls the existing import job builder and `DownloadJobStore` for acquisition.
  Before enqueue, recording membership and track positions are checked against
  the selected MusicBrainz release. There is no new download queue or downloader.
- `MusicBrainzService` supplies shared caching, throttling and retry behavior for
  recording lookup, genre/artist discovery, release groups and release tracks.
- `engine/jellyfin_playlists.py` uses Jellyfin API item IDs and playlist entry IDs.
- `engine/resolution_evidence.py` adds release-scoped evidence alongside the
  existing recording-level community cache. `SearchResolutionService` consumes
  this evidence, probes cached transports and applies its existing scoring gates.
- `engine/discovery_worker.py` runs one persistent metadata/resolution job at a
  time. It cannot submit acquisition. `api/music_discovery.py` integrates with
  the current application's database, config and queue.

## Playlist workflow

1. Preview an exported Apple Music XML file. No Apple filesystem paths are read
   or required. Metadata includes title, artist, album artist, album, track/disc,
   year, genre, duration, persistent ID, Store/Apple Music ID, ISRC and recording
   MBID when present. Date Added is not treated as the release year.
2. Select a saved playlist and resolve identities. Work is persistent and resumes
   after interruption. Refresh status to see track results. Counts distinguish
   pending, high confidence, probable, ambiguous, unresolved and existing locally.
3. Review candidate metadata/MBIDs for probable or ambiguous results. Unresolved
   entries remain in the source playlist; they are never silently downloaded.
4. Select tracks or acquire all resolved missing tracks. This enters Retreivr's
   normal queue, resolver/cache, metadata/tagging and library pipeline. Local
   ownership is checked again immediately before enqueue.
5. Preview Jellyfin matches, then explicitly create/refresh the managed playlist.
   Repeat after newly acquired tracks have been scanned by Jellyfin.

Automatic recording admission requires score >= 0.94 and a margin >= 0.06 over
another plausible recording. Scores 0.75–0.94 are probable. A close second
candidate is ambiguous. These are conservative matching scores, not calibrated
probabilities. Apple IDs are retained as provenance; they are not treated as
MusicBrainz IDs.

Importing identical content retains decisions and acquisition references. Apple
playlist persistent IDs identify updates across renames/membership changes.
Other formats use adapter + playlist name; rename the file to create a distinct
playlist with the same format. Changed membership replaces the staged snapshot
and requires another identity review; existing queued downloads are not
cancelled. Changed imports are refused while that playlist is resolving.

Limits: 10 MiB, 100 playlists and 10,000 track memberships per import. XML
references without a corresponding track dictionary remain unresolved positions.
The original Import screen retains its existing immediate acquisition behavior;
use Discover & Sync for the staged, review-first workflow.

## Jellyfin configuration

Use the existing Settings Jellyfin fields, plus the new owner/library fields:

```json
{
  "arr": {
    "jellyfin": {
      "base_url": "http://jellyfin:8096",
      "api_key": "YOUR_API_KEY",
      "access_token": "PLAYLIST_OWNER_USER_TOKEN",
      "user_id": "JELLYFIN_USER_UUID",
      "library_id": ""
    }
  },
  "music_discovery": {
    "interval_seconds": 10,
    "max_attempts": 3
  }
}
```

`access_token` must be the playlist owner user token: Jellyfin 10.10 removal
operations derive the owner from authentication, and a server API key does not
carry a user identity. The existing API-key field remains available for other
Jellyfin operations and as a fallback if it already contains a user token.
`user_id` is required and checked against `GET /Users/Me` before any playlist write;
`library_id` optionally limits matching to a music library. Save Settings and use
**Test Jellyfin**. Tokens are sent only in `X-Emby-Token`, never query parameters.
Redirects are refused; errors exclude upstream URLs, bodies and exception text.

API operations were checked against the official [Jellyfin 10.10.7 playlist
controller](https://github.com/jellyfin/jellyfin/blob/v10.10.7/Jellyfin.Api/Controllers/PlaylistsController.cs)
and [Jellyfin SDK playlist API](https://typescript-sdk.jellyfin.org/classes/generated-client.PlaylistApi.html):
`GET /Items`, `POST /Playlists`, `GET /Playlists/{id}/Items`,
`POST /Playlists/{id}/Items`, and `DELETE /Playlists/{id}/Items` with EntryIds.
No generated M3U paths are used for this integration.

Provider MBIDs take precedence. Metadata fallback requires title, artist and
album agreement and a compatible duration. Multiple plausible items are reported
as ambiguous. Source repetitions remain intact in Retreivr. Jellyfin 10.10 applies
`Distinct()` to additions, so the sync preserves first-occurrence order and
explicitly reports how many source repetitions cannot be represented remotely.
Repeated sync does not append additional entries. This follows the official
[PlaylistManager implementation](https://github.com/jellyfin/jellyfin/blob/v10.10.7/Emby.Server.Implementations/Playlists/PlaylistManager.cs). A managed playlist is named `Name [Retreivr <stable id>]`
so an interrupted create can be recovered by a deterministic name. Retreivr only
reconciles this managed playlist, never unrelated playlists or library files.

Membership replacement is not transactional in Jellyfin. A failed update can
leave partial membership; the next sync reads and repairs it using the saved
remote ID. If create timed out and a name lookup cannot find the result, Retreivr
stops rather than risk a duplicate create. Verify the outcome in Jellyfin before
administrator recovery of that link record. Preserve the local database when
moving instances; it contains playlist ownership/idempotency state. Run one API
process per database (the current application deployment model).

## Artist subscriptions and genre discovery

The built-in `musicbrainz_genre` provider searches MusicBrainz artist tags using
the existing client strategy. It returns relevance-ranked MBIDs, not a commercial
popularity chart. Artist-name search exposes disambiguation and MBIDs for review.
Other chart/list providers can implement `preview(service, query, limit)` and
register in `PROVIDERS`; core acquisition does not depend on a chart website.

Choose Albums, EPs, Singles and optional Live/Compilation secondary types before
subscribing. Defaults are studio albums, at most 25 release groups per refresh.
The API also accepts Remix, Soundtrack, Demo, DJ-mix and Mixtape/Street policies.
Refresh checks up to 1,000 release groups per artist through the shared throttle,
retains the newest qualifying groups under the configured bound, and deduplicates
by artist MBID and release-group MBID. One preferred release edition is selected
with Retreivr's existing official/earliest-release selection.

Refresh subscriptions manually whenever you want to discover newly released
material. Disable stops refresh; remove stops subscription management and does
not delete music or cancel earlier explicit acquisitions. Select one or up to 25
releases, preview their tracks, then acquire selected/all missing tracks. Preview
and refresh never acquire automatically. Releases already stored from earlier
refreshes remain visible for review.

## Cache and dedicated resolver-only operation

Evidence includes recording/release/artist/release-group identity, YouTube ID/URL,
source, confidence, score evidence, resolver identifier, created/verified times
and invalid/dead status. A release-scoped mapping is only used for that release.
Fresh evidence requires score >= 0.94 and age <= 30 days. It is labelled
`metadata_matched`, **not playback/acquisition verified**. Failed transport
validation falls through to the resolver rather than forcing the cached source.

The existing optional community lookup remains available. Dead/invalid,
low-confidence (<0.78) and dated entries older than 90 days are excluded. Legacy
undated records remain hints and still pass transport probing and scoring. Fresh
high-confidence local records skip remote lookup. A weaker community response
cannot overwrite a stronger local mapping. High-confidence cached candidates
that pass the normal gates avoid the external search ladder; rejected cache
candidates fall back to normal search.

The normal UI can seed resolver-only source jobs from any reviewed playlist or
release preview. For a dedicated machine, run the CLI **instead of the API or
normal Retreivr download worker**:

```sh
python3 scripts/music_cache_builder.py \
  --config /path/to/config.json \
  --db /path/to/builder.sqlite3 \
  --import-file /path/to/Country.xml
```

Or seed a bounded artist discography:

```sh
python3 scripts/music_cache_builder.py \
  --config /path/to/config.json \
  --db /path/to/builder.sqlite3 \
  --artist-mbid MUSICBRAINZ_ARTIST_UUID --artist-name 'Artist name'
```

Restart without an input option to continue persisted jobs. `--once` handles one
ready job and exits. Reseeding deduplicates identities; recent evidence is skipped,
old successful jobs can be refreshed after 30 days, and terminal failures remain
terminal. Three attempts by default, exponential backoff, a minimum two-second
job interval, single-job concurrency, provider throttles and a heartbeat lease
bound workload. SIGINT/SIGTERM stops taking new jobs; interrupted playlist work
resumes from pending rows, and a killed process's lease expires after two minutes.
Individual external requests still use their provider timeouts.

The resolver-only service has no acquisition store, explicitly rejects enqueue,
and no downloader loop is started. Source probing may transfer metadata; it does
not download media. Do not run multiple builder processes against the same DB.
The builder initially writes local evidence. Existing community publication
remains separate; unacquired metadata matches are not falsely published as
playback-verified results. Large multi-decade catalog coverage still requires
supplying bounded playlist/artist inputs; there is no automatic global crawler.

## Migration / upgrade

No destructive migration or manual SQL is required. After backing up the existing
SQLite/config files, start the updated application normally:

```sh
python3 -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Startup creates additive tables in the existing Retreivr DB:
`discovery_playlists`, `discovery_tracks`, `artist_subscriptions`,
`subscription_releases`, `discovery_jobs`, `jellyfin_playlist_links`,
`resolution_evidence`, `discovery_metrics`. Creation is idempotent and preserves
all existing tables. Existing configuration gains defaults through the normal
config loader. No production service restart or deployment is performed by this
source change. A rollback to the previous code can leave these extra tables in
place; do not discard the DB if you need to retain managed Jellyfin links.

## Validation

```sh
python3 -m pytest -q
node --check webUI/app.js
node --check webUI/discovery.js
python3 -m compileall -q api engine metadata scripts/music_cache_builder.py
python3 -m benchmarks.music_search_benchmark_runner \
  --dataset benchmarks/music_search_album_dataset.json \
  --output-json /tmp/retreivr-benchmark.json \
  --gate-config benchmarks/music_search_benchmark_gate.json --enforce-gate
git diff --check
```

Implementation validation (2026-09-21): **793 passed, 1 skipped**, including
45 new feature/UI tests. Existing FastAPI/Pydantic deprecation warnings remain.
The search benchmark passed: 41/42 tracks (97.62%), +0.12 percentage points
against the configured baseline, zero wrong-variant flags. Python compilation,
JavaScript syntax and `git diff --check` passed. The isolated `--once` CLI smoke
test exited 0, reported SQLite integrity `ok`, and created no download queue table.
No formatter/type-checker is configured in this repository.

Tests use fixture MusicBrainz/Jellyfin responses and temporary SQLite databases.
They cover parser ordering/identifiers, normalization, ambiguous/probable matches,
variant rejection, source edition binding, library detection, repeat imports and
acquisition, subscriptions/releases, migrations, API routes, Jellyfin pagination,
redacted errors, repeated sync/lost-create recovery, cache freshness/scope and
builder deduplication/retry/resume/no-acquisition contracts.

Live validation still required: saved credentials/user permissions on your
Jellyfin version; reported duplicate handling and actual Jellyfin playback;
post-acquisition library rescan/refresh; browser desktop/mobile layout and
interactive controls; sustained provider behavior for a dedicated builder.
No browser was connected in the implementation session. Automated tests do not
substitute for these live checks.
