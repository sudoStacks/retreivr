# MusicBrainz Integration

## Purpose

Retreivr uses MusicBrainz for:

- Album candidate discovery from user search queries.
- Canonical release selection for a chosen release-group.
- Canonical track ordering/metadata before enqueueing per-track download jobs.

This is additive to the existing search/download pipeline. It does not auto-download on search.

## Album Download Flow

1. User enables Music Mode and searches on Home.
2. Backend queries MusicBrainz release-groups and returns album candidates.
3. User explicitly picks a candidate and clicks download.
4. Backend resolves a best release inside the selected release-group.
5. Backend fetches tracklist for that release and enqueues one `music_track` job per track.
6. Worker resolves playable audio for each track using normal adapters and existing postprocessing/tagging.

## Rate Limiting and Request Behavior

MusicBrainz calls are centralized in `app/musicbrainz/client.py`:

- Real User-Agent is always sent:
  - `Retreivr/<version> (+repo/contact)` (configurable via `MUSICBRAINZ_USER_AGENT`)
- Base URL is centralized (default `https://musicbrainz.org`, configurable).
- Timeout is centralized (default 10s).
- Retries are centralized for transient failures (429/5xx).
- Client-side rate limit is enforced:
  - default `1 request/second`
  - configurable via `MUSICBRAINZ_MIN_INTERVAL_SECONDS`

Log format:

- `[MUSICBRAINZ] request=<endpoint> status=<code> cache=<hit/miss>`

## Caching

Caching is implemented with:

- In-memory cache (fast process-local reads).
- On-disk JSON cache (`.cache/musicbrainz_cache.json` by default, configurable with `MUSICBRAINZ_CACHE_PATH`).

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
