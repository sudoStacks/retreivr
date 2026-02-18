# Retreivr Path Resolution

Retreivr resolves filesystem paths at runtime from `engine/paths.py`.

## Path Resolution Order

For each core path:

1. Environment variable override (`RETREIVR_*_DIR`)
2. Runtime defaults from `engine/paths.py`
3. Container or local filesystem behavior based on runtime detection

## Runtime Defaults

### Docker/container runtime

When `/.dockerenv` exists or `/data` exists, defaults are:

- `DATA_DIR=/data`
- `DOWNLOADS_DIR=/downloads`
- `CONFIG_DIR=/config`
- `LOG_DIR=/logs`
- `TOKENS_DIR=/tokens`

No `RETREIVR_*_DIR` overrides are required when mounts match this layout.

### Local/source runtime

Defaults remain developer-friendly and project-relative:

- `DATA_DIR=<project>/data`
- `DOWNLOADS_DIR=<project>/data/downloads`
- `CONFIG_DIR=<project>/data/config`
- `LOG_DIR=<project>/data/logs`
- `TOKENS_DIR=<project>/data/tokens`

## Supported Environment Variables

| Variable | Purpose | Typical container path |
|---|---|---|
| `RETREIVR_CONFIG_DIR` | config.json location | `/config` |
| `RETREIVR_DATA_DIR` | temp files, runtime data | `/data` |
| `RETREIVR_DOWNLOADS_DIR` | final downloads | `/downloads` |
| `RETREIVR_LOG_DIR` | logs | `/logs` |
| `RETREIVR_TOKENS_DIR` | cookies/auth tokens | `/tokens` |

## Recommended Docker Layout

```yaml
services:
  retreivr:
    volumes:
      - /host/media:/downloads
      - /host/data:/data
      - /host/config:/config
      - /host/logs:/logs
      - /host/tokens:/tokens
```

## Migration Note: Legacy `/app/data` Deployments

Older deployments often used `/app/data` for everything. Migrate by remapping volumes to the canonical split layout.

If your DB stores absolute old download paths, rewrite them after backup:

```sql
UPDATE downloads
SET filepath = REPLACE(filepath, '/app/data/downloads', '/downloads')
WHERE filepath LIKE '/app/data/downloads%';

UPDATE download_history
SET destination = REPLACE(destination, '/app/data/downloads', '/downloads')
WHERE destination LIKE '/app/data/downloads%';

UPDATE download_jobs
SET file_path = REPLACE(file_path, '/app/data/downloads', '/downloads')
WHERE file_path LIKE '/app/data/downloads%';

UPDATE download_jobs
SET resolved_destination = REPLACE(resolved_destination, '/app/data/downloads', '/downloads')
WHERE resolved_destination LIKE '/app/data/downloads%';

UPDATE downloaded_music_tracks
SET file_path = REPLACE(file_path, '/app/data/downloads', '/downloads')
WHERE file_path LIKE '/app/data/downloads%';
```

## Notes

- If a volume is not mounted, files may be written into the container layer and appear missing from host storage.
- UI browse roots (`/downloads`, `/config`, `/tokens`) map directly to backend browse roots.
