# Initial Setup

Retreivr is designed to come up cleanly in `Retreivr-only` mode first, then let you opt into ARR, downloader, VPN, and Jellyfin services from the Retreivr UI.

This keeps first boot simple and avoids shipping an ARR stack that starts automatically before you want it.

## What Starts By Default

With the example files and a normal:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Retreivr starts first.

Optional services such as:
- Radarr
- Sonarr
- Readarr
- Prowlarr
- Bazarr
- qBittorrent
- gluetun / VPN
- Jellyfin

are present in the compose file, but gated behind Docker Compose profiles and remain off until you enable them.

## Quick Start

1. Prepare the runtime files:

```bash
cp docker/docker-compose.yml.example docker/docker-compose.yml
cp .env.example .env
```

2. Adjust only the minimum required values in `.env`:
- timezone
- media/download roots if needed
- user/group ids if needed

3. Start Retreivr:

```bash
docker compose -f docker/docker-compose.yml up -d
```

4. Open the UI:

```text
http://localhost:8090
```

## Guided Setup Flow

Open `Setup` in the Retreivr UI. The setup experience is designed as the main control plane for the stack.

Modules currently include:
- Core Retreivr
- Storage / media folders
- YouTube / cookies / API
- Telegram notifications
- TMDb
- ARR stack
- VPN / gluetun
- Jellyfin

Each module shows:
- what it is for
- whether it is required or optional
- current status
- inline settings
- validation or test actions
- next steps

## Recommended Operator Path

### 1. Start in Retreivr-only mode

This is the default and the recommended first boot.

Use it to:
- verify storage paths
- configure YouTube / Telegram if desired
- configure TMDb for `Movies & TV`
- decide whether you want ARR, qBittorrent, VPN, or Jellyfin

### 2. Configure `Movies & TV`

`Movies & TV` discovery requires a TMDb API key.

If no TMDb key is present:
- Retreivr shows a guided landing instead of blank shelves or broken cards
- you can enter the key inline
- once saved, native movie/TV discovery unlocks immediately

ARR is not required for discovery.

Without ARR configured:
- search/browse still works
- `Add to ...` actions remain disabled until ARR is connected

### 3. Enable the ARR stack only if you want it

From `Setup` / `Settings`, enable the services you want:
- Radarr
- Sonarr
- Readarr
- Prowlarr
- Bazarr
- qBittorrent
- gluetun / VPN
- Jellyfin

Retreivr then:
- stores the managed stack settings it owns
- writes supported values into `.env`
- generates the exact `docker compose --profile ... up -d` command to run

Run the generated command, then return to Retreivr and re-check health/configuration.

## Connections and Services

Retreivr includes dedicated pages for:

### `Connections`
- service reachability
- ARR and qBittorrent auto-configuration
- VPN policy and health
- TMDb / ARR readiness

### `Services`
- service inventory
- stack-level status views
- guided control-plane surface for optional infrastructure

## ARR Auto-Configuration

Once the optional services are running and reachable, Retreivr can best-effort configure:
- Radarr
- Sonarr
- Readarr
- Prowlarr
- qBittorrent

Current configuration targets include:
- root folders
- quality profile selection where applicable
- qBittorrent download client wiring
- Prowlarr application links
- qBittorrent categories and save paths

This is intended to minimize or eliminate the need to open ARR UIs during normal setup.

## VPN / Gluetun

Retreivr exposes VPN policy and health in the UI.

Current model:
- qBittorrent through VPN is the primary enforced route
- other service routing policies are surfaced as managed settings and health expectations

Retreivr shows:
- provider
- configured control URL
- expected routed services
- external IP when available from the control endpoint

## Music and Music Player

Retreivr separates acquisition from playback:

### `Music`
- favorites
- genres
- suggested artists
- artist browse
- albums / tracks / download actions

### `Music Player`
- library browsing
- playlists
- queue
- recently played
- local-first radio stations

The player uses:
- downloaded local files first
- cached streamable matches second

Playback history, playlists, and station state are stored locally.

## Operational Notes

- Retreivr is the control plane and acquisition layer.
- ARR handles movies, TV, and books automation.
- Jellyfin is optional playback infrastructure.
- Retreivr does not ship as a media server and does not require ARR by default.

## Recommended First-Run Checklist

1. Start Retreivr only
2. Open `Setup`
3. Confirm storage roots
4. Configure TMDb for `Movies & TV`
5. Configure YouTube / Telegram if needed
6. Decide whether to enable ARR, qBittorrent, VPN, and Jellyfin
7. Run the generated compose profile command
8. Return to `Connections` and run verification / auto-config
9. Use `Music`, `Home`, and `Movies & TV`

## Troubleshooting Guidance

If optional services do not appear:
- confirm you re-ran compose with the generated `--profile` flags
- confirm the `.env` file was written to the expected path
- use `Connections` to verify service reachability

If `Movies & TV` looks empty:
- confirm the TMDb API key is configured
- the page should show a setup landing, not blank cards, when the key is missing

If ARR add actions stay disabled:
- confirm ARR is enabled and reachable
- confirm `Connections` reports the service as connected/configured
