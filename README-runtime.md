# Retreivr Docker Starter Bundle

## What This Is

This starter bundle provides the minimum files required to run Retreivr with Docker Compose, without cloning the full repository.

## Requirements

- Docker
- Docker Compose

## Setup Instructions

### Step 1 — Download starter bundle

Download `retreivr-docker-starter-<version>.zip` from the GitHub Release assets.

### Step 2 — Extract files

Extract the bundle to a directory where you want to run Retreivr.

### Step 3 — Copy config template

Create your runtime config file:

```bash
cp config/config.json.example config/config.json
```

### Step 4 — Edit config.json

Update `config/config.json` for your environment and preferences.

### Step 5 — Start Retreivr

Run:

```bash
docker compose up -d
```

## Updating

To update to a newer release:

1. Pull the new image referenced by the updated compose file.
2. Replace `docker-compose.yml` with the version from the new runtime bundle if it changed.
3. Restart the service:

```bash
docker compose pull
docker compose up -d
```

## Versioning

The starter bundle version matches the Docker image tag for that release.

## Notes

- Full source code and development files are available in the main repository.
- The starter bundle is intended for simplified Docker deployment only.
- Optional custom search adapters can be defined in `config/custom_search_adapters.yaml`.
  - Start from `config/custom_search_adapters.example.yaml`.
  - Point `custom_search_adapters_file` in `config/config.json` to your YAML/JSON file path.
  - Custom adapters that load successfully are exposed automatically in Home source selection.
