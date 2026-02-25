# Retreivr Runtime Deployment

## What This Is

This runtime bundle provides the minimum files required to run Retreivr with Docker, without cloning the full repository.

## Requirements

- Docker
- Docker Compose

## Setup Instructions

### Step 1 — Download release bundle

Download `retreivr-runtime-<version>.zip` from the GitHub Release.

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

The runtime bundle version matches the Docker image tag for that release.

## Notes

- Full source code and development files are available in the main repository.
- The runtime bundle is intended for simplified Docker deployment only.
