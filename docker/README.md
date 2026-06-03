Docker assets for Retreivr.

Quick start
- Build: `docker build -f docker/Dockerfile -t retreivr:latest .`
- Compose: use `docker/docker-compose.yml.example` as your base.
- Default runtime is Retreivr-only. Optional ARR/Jellyfin/VPN services are enabled later through compose profiles.
- Release asset: each tagged release also publishes `retreivr-docker-starter-<tag>.zip` with compose, env, config example, and runtime README.

Compose profiles (single canonical file)
- Canonical file: `docker/docker-compose.yml.example`
- Default: `docker compose up -d` starts `retreivr` only.
- Enable optional services by profile:
  - `--profile arr` -> `radarr`, `sonarr`, `readarr`, `prowlarr`
  - `--profile subtitles` -> `bazarr`
  - `--profile downloader` -> `qbittorrent` (uses `gluetun` network)
  - `--profile vpn` -> `gluetun`
  - `--profile jellyfin` -> `jellyfin`
  - `--profile hostctl` -> `retreivr-hostctl` (required for direct-manage mode)

Common examples
- Retreivr only: `docker compose up -d`
- ARR core: `docker compose --profile arr up -d`
- ARR + downloader + VPN: `docker compose --profile arr --profile downloader --profile vpn up -d`
- ARR + Jellyfin: `docker compose --profile arr --profile jellyfin up -d`
- Direct-manage helper enabled: `docker compose --profile hostctl up -d`

Ports
- Internal container port: 8000
- Suggested host mapping: `8090:8000` (`host:container`)
- Access URL with default mapping: `http://localhost:8090`
- Optional profile host ports:
  - `8080` -> gluetun/qBittorrent WebUI
  - `7878` -> Radarr
  - `8989` -> Sonarr
  - `8787` -> Readarr
  - `9696` -> Prowlarr
  - `6767` -> Bazarr
  - `8096` -> Jellyfin

Port conflict remediation
- If `docker compose up` fails on a port bind error:
  - stop the process currently using the host port, or
  - remap the host side in compose (example `7879:7878` for Radarr), then re-run.
- Retreivr Setup now runs preflight checks and will report blocking conflicts with hints before apply.

Volumes + paths
- `/config` → config JSON
- `/downloads` → completed media
- `/data` → SQLite + temp dirs
- `/logs` → logs
- `/tokens` → OAuth tokens + client secrets

Legacy `/app/*` mounts are still supported via explicit `RETREIVR_*_DIR` overrides, but not recommended.

Use relative paths inside `config.json` (e.g. `folder: "YouTube/Channel"`).

Version build arg
```bash
docker build -f docker/Dockerfile --build-arg RETREIVR_VERSION=1.0.0 -t retreivr:latest .
```

Notes
- Bind to all interfaces in containers with `RETREIVR_HOST=0.0.0.0` if needed.
- Consider running as a non-root user with a fixed UID/GID to match volume permissions.
- Optional services are designed to be enabled from Retreivr's `Setup` UI, which writes a managed env block and generates the exact `docker compose --profile ... up -d` command to run.
- `resolution_api.upstream_base_url` is optional. Leave local cache sync off unless this node should mirror another Retreivr Resolution API node.
- If you do enable local cache sync, set:
  - `resolution_api.upstream_base_url`
  - `resolution_api.sync_enabled=true`
  - `resolution_api.local_node_id`
