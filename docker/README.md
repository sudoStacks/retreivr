Docker assets for Retreivr.

Quick start
- Build: `docker build -f docker/Dockerfile -t retreivr:latest .`
- Compose: use `docker/docker-compose.yml.example` as your base.

Ports
- Internal container port: 8000
- Suggested host mapping: `8090:8000` (`host:container`)
- Access URL with default mapping: `http://localhost:8090`

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
docker build -f docker/Dockerfile --build-arg RETREIVR_VERSION=0.9.1 -t retreivr:latest .
```

Notes
- Bind to all interfaces in containers with `RETREIVR_HOST=0.0.0.0` if needed.
- Consider running as a non-root user with a fixed UID/GID to match volume permissions.
