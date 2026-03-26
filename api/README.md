Retreivr API

This API is designed to be consumed by internal tools (CLI/Web UI) and external
monitoring systems such as Home Assistant. JSON responses aim to be stable and
backwards-compatible: keys are always present, and new fields are additive.

Key endpoints
- GET /api/status
- GET /api/schedule
- GET /api/metrics
- POST /api/run
- POST /api/intake
- GET /api/history
- GET /api/logs

External intake
`POST /api/intake` accepts a normalized acquisition package from external tools
such as Jellyfin plugins, browser extensions, or local scripts.

Full schema and examples: `docs/intake_api.md`

Example payload:
{
  "source_url": "https://media.example.test/file.mp3",
  "media_class": "audiobook",
  "metadata": {
    "title": "Chapter 1",
    "author": "Example Author",
    "series": "Example Series"
  },
  "delivery": {
    "destination": "Books/Audiobooks",
    "final_format": "mp3"
  },
  "provenance": {
    "origin": "jellyfin_plugin",
    "origin_id": "item-123"
  }
}

Current media class handling:
- `music`, `audio`, `track`, `song` -> queued as music
- `audiobook`, `podcast` -> queued as music with audiobook/podcast intent
- `video`, `music_video`, `movie`, `episode` -> queued as video
- `book`, `pdf`, `ebook`, `document` -> queued as generic download with book intent

Status JSON (stable)
Example response:
{
  "schema_version": 1,
  "server_time": "2024-01-01T12:00:00+00:00",
  "state": "idle",
  "running": false,
  "run_id": null,
  "started_at": null,
  "finished_at": null,
  "error": null,
  "status": {
    "run_successes": [],
    "run_failures": [],
    "runtime_warned": false,
    "single_download_ok": null,
    "current_playlist_id": null,
    "current_video_id": null,
    "current_video_title": null,
    "progress_current": null,
    "progress_total": null,
    "progress_percent": null,
    "last_completed": null,
    "last_completed_at": null
  }
}

Metrics JSON (stable)
Example response:
{
  "schema_version": 1,
  "server_time": "2024-01-01T12:00:00+00:00",
  "downloads_dir": "/path/to/downloads",
  "downloads_files": 12,
  "downloads_bytes": 123456789,
  "disk_total_bytes": 100000000000,
  "disk_free_bytes": 50000000000,
  "disk_used_bytes": 50000000000,
  "disk_free_percent": 50.0
}

Schedule JSON (stable)
Example response:
{
  "schema_version": 1,
  "server_time": "2024-01-01T12:00:00+00:00",
  "schedule": {
    "enabled": true,
    "mode": "interval",
    "interval_hours": 6,
    "run_on_startup": false
  },
  "enabled": true,
  "last_run": "2024-01-01T06:00:00+00:00",
  "next_run": "2024-01-01T12:00:00+00:00"
}

Home Assistant examples

REST sensors:
sensor:
  - platform: rest
    name: Retreivr Status
    resource: http://YOUR_SERVER:8000/api/status
    value_template: "{{ value_json.state }}"
    json_attributes:
      - running
      - run_id
      - started_at
      - finished_at
      - error
      - status
  - platform: rest
    name: Retreivr Schedule
    resource: http://YOUR_SERVER:8000/api/schedule
    value_template: "{{ value_json.enabled }}"
    json_attributes:
      - schedule
      - last_run
      - next_run
  - platform: rest
    name: Retreivr Metrics
    resource: http://YOUR_SERVER:8000/api/metrics
    value_template: "{{ value_json.downloads_files }}"
    json_attributes:
      - downloads_bytes
      - disk_total_bytes
      - disk_free_bytes
      - disk_free_percent

REST command to start a run:
rest_command:
  retreivr_run:
    url: http://YOUR_SERVER:8000/api/run
    method: POST
    headers:
      content-type: application/json
    payload: "{}"

REST command to run a single URL:
rest_command:
  retreivr_run_single:
    url: http://YOUR_SERVER:8000/api/run
    method: POST
    headers:
      content-type: application/json
    payload: '{"single_url":"{{ url }}"}'

REST command to update schedule:
rest_command:
  retreivr_schedule:
    url: http://YOUR_SERVER:8000/api/schedule
    method: POST
    headers:
      content-type: application/json
    payload: '{"enabled":true,"mode":"interval","interval_hours":6,"run_on_startup":false}'

Authentication (optional)
Basic auth is disabled by default. To enable it, set both:
- YT_ARCHIVER_BASIC_AUTH_USER
- YT_ARCHIVER_BASIC_AUTH_PASS

When enabled, all API and Web UI routes require Basic auth.

Reverse proxy compatibility
If running behind a reverse proxy, set:
- YT_ARCHIVER_TRUST_PROXY=1

This enables support for X-Forwarded-For / X-Forwarded-Proto / X-Forwarded-Host headers.
