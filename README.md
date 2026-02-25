<p align="center">
  <img src="webUI/app_icon.png" width="220" alt="Retreivr Logo" />
</p>

<h1 align="center">Retreivr</h1>

<p align="center">
  Self-hosted media acquisition that stays deterministic, tidy, and boring in the best way.
</p>

---

## What Is Retreivr?
Retreivr is a self-hosted download engine for building and maintaining a clean local media archive.

It takes your intent (URL, playlist, search, or Spotify sync), resolves targets, downloads media, applies canonical naming/metadata rules, and writes predictable files to disk.

Retreivr is not a streaming server. It is the acquisition layer.

## Why People Use It
- Deterministic downloads (idempotent and repeatable)
- Clean filesystem structure (no random naming chaos)
- MusicBrainz-first metadata authority for music workflows
- Unified queue + scheduler + watcher flows
- Web UI and API for control and automation
- Optional Telegram summaries

## 0.9.6 Highlights
- Desktop launcher foundation for local macOS/Windows installs
- Guided Docker/runtime setup with preflight checks and onboarding checklist
- Launcher-managed compose/config bootstrap for faster first run
- Improved diagnostics, error guidance, and update awareness in launcher

---

## Quick Start (Local Workstation)

### Option A) Desktop Launcher (Recommended for non-server local installs)
- Download the launcher for your OS from Releases:
  - macOS (`.dmg` / `.app`)
  - Windows (installer package)
- Open the launcher and follow the wizard:
  - verify Docker Desktop is installed/running
  - choose folders and settings
  - generate compose + start Retreivr
- Open Web UI:
```text
http://localhost:8090
```

The launcher is intended for local machine use and does not require manually cloning/copying compose/config files for normal setup.

### Option B) Docker Compose (Manual)

#### 1) Prepare files
```bash
cp docker/docker-compose.yml.example docker/docker-compose.yml
cp .env.example .env
```

#### 2) Start Retreivr
```bash
docker compose -f docker/docker-compose.yml up -d
```

#### 3) Open Web UI
```text
http://localhost:8090
```
Default mapping is `8090:8000` (`host:container`).

#### 4) Initial setup in UI
- Open `Config` page
- Add your playlist/search settings
- Set destination folders (under `/downloads` in container)
- Optional: configure Spotify OAuth and Telegram

---

## Canonical Docker Mounts
Use these container paths for predictable behavior:
- `/downloads` media output
- `/data` runtime DB/temp
- `/config` config.json
- `/logs` logs
- `/tokens` auth/cookies

---

## Local Run (No Docker)
Requirements:
- Python `3.11.x`
- `ffmpeg` on PATH

Run:
```bash
python3.11 scripts/archiver.py --config data/config/config.json
```

Run API/UI locally:
```bash
python3.11 -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```
Then open:
```text
http://localhost:8000
```

---

## Useful Endpoints
- `GET /api/status`
- `GET /api/metrics`
- `POST /api/run`
- `GET /api/download_jobs`
- `POST /api/import/playlist`
- `GET /docs` (OpenAPI)

---

## Upgrade Notes
If you are upgrading to `0.9.6`, pull latest image and restart:
```bash
docker compose -f docker/docker-compose.yml pull
docker compose -f docker/docker-compose.yml up -d
```

Your mounted `/data`, `/downloads`, `/config`, and `/tokens` persist.

---

## Scope Boundaries
Retreivr does:
- Acquire media reliably
- Normalize metadata and output structure
- Keep playlist/sync ingestion deterministic

Retreivr does not:
- Stream media
- Replace Plex/Jellyfin players
- Bypass DRM/protected content
