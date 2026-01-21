# Retreivr Path Resolution

Retreivr resolves all filesystem paths at runtime using environment variables.
Docker image builds do NOT hardcode any paths.

## Path Resolution Order

For each path:

1. Environment variable (highest priority)
2. engine/paths.py default (relative to PROJECT_ROOT)
3. Container filesystem (last resort)

## Supported Environment Variables

| Variable | Purpose | Typical Container Path |
|--------|--------|------------------------|
| RETREIVR_CONFIG_DIR | config.json location | /app/config |
| RETREIVR_DATA_DIR | temp files, runtime data | /app/data |
| RETREIVR_DOWNLOADS_DIR | final downloads | /app/downloads |
| RETREIVR_LOG_DIR | logs | /app/logs |
| RETREIVR_TOKENS_DIR | cookies / auth tokens | /app/tokens |

## Recommended Docker Layout

All paths should be mounted explicitly:

```yaml
services:
  retreivr:
    volumes:
      - ./config:/app/config
      - ./downloads:/app/downloads
      - ./data:/app/data
      - ./logs:/app/logs
      - ./tokens:/app/tokens
    environment:
      RETREIVR_CONFIG_DIR: /app/config
      RETREIVR_DOWNLOADS_DIR: /app/downloads
      RETREIVR_DATA_DIR: /app/data
      RETREIVR_LOG_DIR: /app/logs
      RETREIVR_TOKENS_DIR: /app/tokens
```

## Important Notes
	•	/app is the container root.
	•	If a volume is not mounted, files may be written into the container and appear “missing”.
	•	UI browse paths (/downloads, /config, /tokens) are mapped to these directories.

