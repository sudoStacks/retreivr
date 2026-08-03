# MusicBrainz Integration

## Purpose

Retreivr uses MusicBrainz for:

- Album candidate discovery from user search queries.
- Canonical release selection for a chosen release-group.
- Canonical track ordering/metadata before enqueueing per-track download jobs.
- Canonical metadata authority during search-time resolution, with Spotify used only as gated fallback.
- Authoritative YouTube URL relationships that can start remote playback before a
  local download exists.

This is additive to the existing search/download pipeline. It does not auto-download on search.

## Remote Playback Contract

Music playback remains local-first. When a selected track is not downloaded,
Retreivr resolves a YouTube-family source in this order:

1. YouTube URL relationships already attached to the selected MusicBrainz entity.
2. Retreivr's recording resolution index and verified community cache.
3. A bounded, metadata-scored YouTube search fallback.

The browser plays remote sources through the YouTube IFrame adapter instead of
probing and proxying an audio stream. The iframe remains visible in a persistent
mini-player while the user browses other Retreivr sections; hiding or destroying
the iframe would interrupt playback. Downloading remains a separate explicit
action and preserves the deterministic MusicBrainz metadata pathway.

## Album Download Flow

1. User enables Music Mode and searches on Home.
2. Backend queries MusicBrainz release-groups and returns album candidates.
3. User explicitly picks a candidate and clicks download.
4. Backend resolves a best release inside the selected release-group.
5. Backend fetches tracklist for that release and enqueues one `music_track` job per track.
6. Worker resolves playable audio for each track using normal adapters and existing postprocessing/tagging.

## Album Candidate API

- Canonical route: `GET /api/music/albums/search?q=<query>&limit=<n>`
  - Returns release-group candidates directly from the centralized MusicBrainzService search path.
- Compatibility route: `POST /api/music/album/candidates`
  - Calls the same canonical search implementation internally and returns the legacy envelope:
    - `{ "status": "ok", "album_candidates": [...] }`

## Rate Limiting and Request Behavior

MusicBrainz calls are centralized in `metadata/services/musicbrainz_service.py`:

- Real User-Agent is always sent:
  - `Retreivr/<version> (+repo/contact)` (configurable via `MUSICBRAINZ_USER_AGENT`)
- Timeout is centralized (default 10s).
- Retries are centralized for transient failures.
- Client-side rate limit is enforced:
  - default `1 request/second`
  - configurable via `MUSICBRAINZ_MIN_INTERVAL_SECONDS`

## Caching

Caching is implemented with:

- In-memory cache (process-local reads).

No DB schema/table changes are required.

Cache keys:

- `album_search:<query>`
- `release_group:<id>`
- `release_tracks:<release_id>`

TTL:

- Album search: 24 hours
- Release-group release listing: 24 hours
- Release tracks: 7 days

## Known Limitations

- Ambiguous titles/artists can still return mixed candidates.
- Regional release differences can change selected release details.
- Live/compilation/soundtrack/remix filtering is heuristic, not perfect.
- Track duration availability depends on MusicBrainz completeness.
