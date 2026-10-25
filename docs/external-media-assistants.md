# External conversational media assistants

Retreivr can be paired with an external conversational controller such as a
Telegram bot, Home Assistant automation, n8n workflow, or LLM agent. The
controller is optional: Retreivr remains responsible for discovery,
metadata, destinations, queueing, downloads and notifications.

## Video contract

1. A controller calls `POST /api/video/search/preview` with `query` and an
   optional `limit` (1–10; use 3 for chat).
2. Retreivr returns preview-only candidates. This endpoint never creates a
   queue item or search job.
3. The controller displays the returned title, channel, thumbnail and URL.
   It must not invent candidates or replace a selected URL.
4. The owner explicitly confirms one candidate.
5. The controller submits that exact URL to `POST /api/run` with
   `single_url` and `media_mode: "video"`.

For direct URLs, call `POST /api/direct-url/resolve` first and show the
returned preview before calling `/api/run`.

## Music album contract

1. Resolve an album and artist with `POST /api/music/intent/preview` using
   `media_intent: "music_album"`, `country: "US"`, and bounded `limit`/`offset`.
   This returns previewable official US MusicBrainz releases without queueing.
2. Display the returned release title, artist, artwork, release page and exact
   release MBID. If a title is ambiguous, let the owner choose a different
   candidate; do not silently substitute a release.
3. Only after the owner selects Download, submit the selected
   `release_group_mbid` and `release_mbid` to `POST /api/music/album/download`
   with `media_mode: "music"` and `fast_enqueue: true`. This creates normal
   music jobs, using the configured music destination, home format and enabled
   post-finalization exports. Do not override `destination` or `final_format`
   unless the owner explicitly asks for it.

Music and video preview routes do not infer each other's intent. The external
controller should classify ordinary chat text or ask for clarification before
calling a route; the download action always requires explicit confirmation.

## Safety recommendations

- Keep the controller's Retreivr access on the private network.
- Allow direct queueing only for one owner-confirmed URL or candidate.
- Use opaque, expiring callback IDs in chat buttons; never put a privileged
  command or arbitrary shell payload in callback data.
- Send bulk imports, saved-channel subscriptions, Jellyfin library changes,
  and user-permission changes to a separate approval workflow.
- Do not expose Retreivr API credentials, Telegram tokens, paths, cookies or
  download logs to the model or chat.

## Telegram delivery

Treat delivery as a post-download option. Confirm Retreivr has completed the
file, enforce a conservative bot-file-size cap, and otherwise send a completed
notification plus the user's normal Jellyfin/library location. Do not
transcode or delete media merely to satisfy chat delivery.
