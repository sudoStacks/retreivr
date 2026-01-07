YouTube Archiver API

This API is designed to be consumed by internal tools (CLI/Web UI) and external
monitoring systems such as Home Assistant. JSON responses aim to be stable and
backwards-compatible: keys are always present, and new fields are additive.

Key endpoints
- GET /api/status
- GET /api/schedule
- GET /api/metrics
- POST /api/run
- GET /api/history
- GET /api/logs

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
    name: YouTube Archiver Status
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
    name: YouTube Archiver Schedule
    resource: http://YOUR_SERVER:8000/api/schedule
    value_template: "{{ value_json.enabled }}"
    json_attributes:
      - schedule
      - last_run
      - next_run
  - platform: rest
    name: YouTube Archiver Metrics
    resource: http://YOUR_SERVER:8000/api/metrics
    value_template: "{{ value_json.downloads_files }}"
    json_attributes:
      - downloads_bytes
      - disk_total_bytes
      - disk_free_bytes
      - disk_free_percent

REST command to start a run:
rest_command:
  youtube_archiver_run:
    url: http://YOUR_SERVER:8000/api/run
    method: POST
    headers:
      content-type: application/json
    payload: "{}"

REST command to run a single URL:
rest_command:
  youtube_archiver_run_single:
    url: http://YOUR_SERVER:8000/api/run
    method: POST
    headers:
      content-type: application/json
    payload: '{"single_url":"{{ url }}"}'

REST command to update schedule:
rest_command:
  youtube_archiver_schedule:
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
