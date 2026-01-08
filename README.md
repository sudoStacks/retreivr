# YouTube Archiver

YouTube Archiver is a self-hosted tool for archiving YouTube playlists or individual videos with a clean Web UI, a simple API, and a Docker-first deployment model. For most users, the recommended install and best experience is the Docker container.

It’s designed to run unattended, keep history in a local SQLite database, and let you inspect status, logs, and completed downloads from a browser. Files are downloaded to disk once and can be streamed or downloaded directly to your device via the Web UI.

This is not a cloud service and it does not require any hosted components.

What it’s good at
	•	Keeping personal or shared YouTube playlists in sync
	•	Running scheduled archive jobs without cron or babysitting
	•	Downloading a single URL on demand
	•	Reviewing status, progress, logs, and history from a browser
	•	Downloading completed files directly from the server
	•	Running cleanly in Docker with explicit, safe volume mappings

Highlights
	•	Mobile-friendly Web UI served by the API
	•	Built-in scheduler (no cron, no systemd)
	•	Docker-safe path handling and volume layout
	•	Background runs with live playlist + video progress
	•	SQLite history with search, filter, and sort
	•	Optional Telegram run summaries
	•	Optional Basic auth and reverse-proxy support
	•	Home Assistant–friendly status and metrics endpoints

## Quick start (Docker - recommended)
```bash
cp docker/docker-compose.yml.example docker/docker-compose.yml
cp .env.example .env
docker compose -f docker/docker-compose.yml up -d
```
Open the Web UI at `http://YOUR_HOST:8090`.

## Requirements
Docker deployment:
- Docker Engine or Docker Desktop
- docker compose (v2)

Local/source deployment (optional):
- Python 3.11 only
- ffmpeg on PATH
- Node.js or Deno only if you use a JS runtime for extractor workarounds

## Configuration
1) Copy the sample config:
```bash
cp config/config_sample.json config/config.json
```

2) (OPTIONAL) Create a Google Cloud OAuth client (Desktop app) and place client secret JSONs in `tokens/`.

3) (OPTIONAL - Also available in webUI) Generate OAuth tokens:
```bash
python setup_oauth.py --account family_tv tokens/client_secret_family.json tokens/token_family.json
```

4) Edit `config/config.json`:
- `accounts` paths to client_secret and token JSONs (optional if you only use public playlists)
- `playlists` with `playlist_id`, `folder`, optional `account`, optional `final_format`
- `final_format` default (webm/mp4/mkv/mp3)
- `js_runtime` to avoid extractor issues (node:/path or deno:/path)
- `single_download_folder` default for single-URL downloads
- `telegram` optional bot_token/chat_id for summaries (see Telegram setup below)
- `schedule` optional interval scheduler

## Path strategy (Docker)
The app always writes to `/downloads`. You control where that maps on your system.
Example mapping to `/Media` on the host:
```
- /Media:/downloads
```
Example playlist folder values (relative to `/downloads` inside the container):
- `Videos/ChannelName`
- `Music/Artist/Album`

Do not put absolute host paths in `config.json` when using Docker.

## Web UI
The Web UI is served by the API and talks only to REST endpoints. It provides:
	•	Home page with run controls, status, schedule, and metrics
	•	Config page (including schedule controls and optional playlist names)
	•	Downloads page with search and limit controls
	•	History page with search, filter, sort, and limit controls
	•	Logs page with manual refresh
	•	Live playlist progress + per-video download progress
	•	Download buttons for completed files
	•	Manual cleanup for temporary files

## API overview
Common endpoints:
	•	GET /api/status
	•	GET /api/metrics
	•	GET /api/schedule
	•	POST /api/run
	•	GET /api/history
	•	GET /api/logs


OpenAPI docs are available at `/docs`.

## Telegram notifications (optional)
You must create your own bot and provide both the bot token and chat ID.

Quick setup:
1) Talk to @BotFather in Telegram and create a bot to get the token.
2) Start a chat with the new bot and send a message.
3) Get your chat ID by visiting:
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
   Look for `"chat":{"id":...}` in the response.
4) Set these in `config.json`:
```
"telegram": {
  "bot_token": "YOUR_BOT_TOKEN",
  "chat_id": "YOUR_CHAT_ID"
}
```

Notes:
	•	For group chats, add the bot to the group and send a message first.
	•	Group chat IDs are usually negative numbers.

## Updating
Containers are disposable; your real data lives in mounted volumes. A safe update flow is:
```bash
docker compose pull
docker compose down
docker compose up -d
```
This preserves your config, database, logs, tokens, and downloads.

## Security model
	•	Local-first design; no hosted or cloud mode
	•	Optional Basic auth (off by default)
	•	Reverse-proxy friendly (X-Forwarded-* headers supported)
	•	No secrets exposed to frontend JavaScript
	•	OAuth tokens are stored locally and transparently under TOKENS_DIR

## Non-goals
This project does not attempt to:
	•	Circumvent DRM
	•	Auto-update yt-dlp at runtime
	•	Act as a hosted or cloud service
	•	Collect telemetry or usage data
	•	Bypass platform terms of service

## Notes
	•	Keep config/config.json, tokens/, and the SQLite database out of version control
	•	Downloads are staged in a temp directory and atomically copied to their final location
	•	“Clear temporary files” only removes working directories (temp downloads + yt-dlp temp)
	•	YT_ARCHIVER_* environment variables can override paths (see .env.example)

## Release
Current release: v1.0.0 (first public release). See `CHANGELOG.md` for details.

## Contributing
Contributions are welcome. Please read `CONTRIBUTING.md` before opening a PR.

## Security
Security issues should be reported privately. See `SECURITY.md`.

## License
MIT. See `LICENSE`.
