# External Intake API

Retreivr exposes `POST /api/intake` as a stable ingestion boundary for external
applications such as:

- Jellyfin plugins
- Browser extensions
- Local scripts
- Automation tools

This endpoint accepts a normalized JSON package and writes it into Retreivr's
unified download queue.

## Endpoint

- Method: `POST`
- Path: `/api/intake`
- Content-Type: `application/json`

## Authentication

Authentication follows the server's existing API policy:

- If Basic auth is disabled, no auth is required.
- If Basic auth is enabled, send the same Basic auth credentials used for the
  rest of the Retreivr API.

Current Basic auth environment variables:

- `YT_ARCHIVER_BASIC_AUTH_USER`
- `YT_ARCHIVER_BASIC_AUTH_PASS`

## Request Schema

Top-level fields:

- `source_url`: string, required
- `url`: string, optional alias for `source_url`
- `media_class`: string, optional
- `media_intent`: string, optional
- `metadata`: object, optional
- `delivery`: object, optional
- `provenance`: object, optional
- `force_redownload`: boolean, optional, default `false`

### `delivery`

- `destination`: string, optional
- `final_format`: string, optional
- `media_mode`: string, optional

### `provenance`

- `origin`: string, optional
- `origin_id`: string, optional
- `source`: string, optional
- `external_id`: string, optional
- `submitted_by`: string, optional

## Media Class Mapping

Retreivr currently normalizes `media_class` as follows:

- `music`, `audio`, `track`, `song` -> queued as `music`
- `audiobook`, `podcast` -> queued as `music`
- `video`, `music_video`, `movie`, `episode` -> queued as `video`
- `book`, `pdf`, `ebook`, `document` -> queued as generic `video`-class download with `book` intent
- missing or `auto` -> defaults to `video` with `download` intent

Notes:

- `audiobook` is currently routed through the existing audio pipeline.
- `book` and `pdf` are currently queued as generic downloads, not a dedicated
  book-finalization pipeline.

## Metadata Guidance

`metadata` is intentionally flexible. Retreivr currently recognizes and
normalizes these common fields when present:

- `title`
- `track`
- `artist`
- `album_artist`
- `author`
- `album`
- `series`
- `track_number`
- `disc_number`
- `release_date`
- `duration_ms`
- `mbid`
- `recording_mbid`
- `mb_recording_id`

Current normalization behavior:

- `title` is mirrored into `track` when `track` is missing
- `author` is mapped to `artist` / `album_artist` when those are missing
- `series` is mapped to `album` when `album` is missing
- `track_number` is mirrored into `track_num`
- `disc_number` is mirrored into `disc_num`
- `release_date` is mirrored into `date`
- `recording_mbid` and `mb_recording_id` can populate `mbid`

## Example: Audiobook From Jellyfin

```json
{
  "source_url": "https://media.example.test/audiobooks/book-01.mp3",
  "media_class": "audiobook",
  "metadata": {
    "title": "Chapter 1",
    "author": "Example Author",
    "series": "Example Series",
    "duration_ms": 123000
  },
  "delivery": {
    "destination": "Books/Audiobooks",
    "final_format": "mp3"
  },
  "provenance": {
    "origin": "jellyfin_plugin",
    "origin_id": "item-123",
    "external_id": "jf-item-123"
  }
}
```

## Example: PDF From Browser Extension

```json
{
  "source_url": "https://files.example.test/docs/paper.pdf",
  "media_class": "pdf",
  "metadata": {
    "title": "Distributed Systems Notes",
    "author": "A. Writer"
  },
  "delivery": {
    "destination": "Books/PDFs"
  },
  "provenance": {
    "origin": "browser_extension",
    "origin_id": "tab-42"
  }
}
```

## Success Response

Status code: `202 Accepted`

Example response:

```json
{
  "status": "accepted",
  "job_id": "job-123",
  "created": true,
  "dedupe_reason": null,
  "effective_media_type": "music",
  "effective_media_intent": "audiobook",
  "origin": "jellyfin_plugin",
  "source_url": "https://media.example.test/audiobooks/book-01.mp3"
}
```

## Error Responses

Examples:

- `400` with `source_url is required`
- `400` with `unsupported media_class`
- `503` if the unified queue store is unavailable

## Integration Notes

- Prefer stable `origin` values such as `jellyfin_plugin`, `browser_extension`,
  or `paperless_script`.
- Use `origin_id` or `external_id` to preserve the caller's object identity.
- Prefer logical destination names over embedding fragile absolute filesystem
  paths in callers unless you control the deployment.
