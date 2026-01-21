Docker assets for Retreivr.

Quick start
- Build: `docker build -f docker/Dockerfile -t retreivr:latest .`
- Compose: use `docker/docker-compose.yml.example` as your base.

Ports
- Internal: 8000
- Suggested host mapping: 8090

Volumes + paths
- `/app/config` → config JSON
- `/app/downloads` → completed media
- `/app/data` → SQLite + temp dirs
- `/app/logs` → logs
- `/app/tokens` → OAuth tokens + client secrets
** /app must be included in the path names as of v0.9.1

Use relative paths inside `config.json` (e.g. `folder: "YouTube/Channel"`).

Version build arg
```bash
docker build -f docker/Dockerfile --build-arg RETREIVR_VERSION=0.9.1 -t retreivr:latest .
```

Notes
- Bind to all interfaces in containers with `RETREIVR_HOST=0.0.0.0` if needed.
- Consider running as a non-root user with a fixed UID/GID to match volume permissions.
