<p align="center">
  <img src="webUI/app_icon.png" width="220" alt="Retreivr Logo" />
</p>

<h1 align="center">Retreivr</h1>

<p align="center">
  Self-hosted media acquisition that stays deterministic, tidy, and boring in the best way.
</p>

<p align="center">
  Follow us on X: <a href="https://x.com/sudoStacks">https://x.com/sudoStacks</a>
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
- Used for intentional media consumption... avoid getting sucked into the algorithms
- Optional Telegram summaries

## 0.9.14 Highlights
- Automated Community Cache publisher worker for verified transport matches
- Community Cache settings in the UI for lookup, publishing, repo/branch targeting, and PR behavior
- Scheduled outbox ingestion that can write dataset updates and open/update a GitHub PR automatically
- Runtime config changes now refresh the publish worker without a restart

## Release Outputs
- GitHub Container Registry image: `ghcr.io/sudostacks/retreivr:<tag>`
- Docker Hub image: `sudostacks/retreivr:<tag>`
- GitHub Release asset: `retreivr-docker-starter-<tag>.zip`

The Docker starter bundle contains:
- `docker-compose.yml`
- `.env.example`
- `config/config.json.example`
- `README-runtime.md`

---

## Quick Start (Local Workstation)

### Option A) Docker Compose (Manual)

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

## Cache Configuration
Retreivr supports one cache concept in the current pipeline:

- `community_cache_lookup_enabled`: Enables reading shared community transport hints (MBID -> transport IDs). Defaults to `true`. This accelerates discovery only; MusicBrainz remains canonical metadata authority.
- `community_cache_publish_enabled`: Enables local proposal emission for contributing matches. Defaults to `false` (opt-in).

Related controls:

- `community_cache_publish_mode`: `off | dry_run | write_outbox`
- `community_cache_publish_min_score`
- `community_cache_publish_outbox_dir`
- `community_cache_publish_repo`
- `community_cache_publish_target_branch`
- `community_cache_publish_branch`
- `community_cache_publish_open_pr`
- `community_cache_publish_poll_minutes`
- `community_cache_publish_token_env`
- `community_cache_publish_batch_size`

---

## Upgrade Notes
If you are upgrading to `0.9.14`, pull the latest image and restart:
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

## License

Retreivr is licensed under the GNU Affero General Public License v3.0 (AGPLv3).
See the LICENSE file for full details.
