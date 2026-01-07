Docker assets placeholder.

This directory is reserved for Docker-related files (Dockerfile/compose/scripts).
When a Dockerfile is added, the suggested exposed port is 8090.

Dockerization prep notes
- Bind to all interfaces in containers by setting `YT_ARCHIVER_HOST=0.0.0.0`.
- Expose port 8090 (or any host-mapped port) and keep internal port 8000 unless overridden.
- Use volume mounts + env vars to make paths portable:
  - `/config` → `YT_ARCHIVER_CONFIG_DIR=/config`
  - `/downloads` → `YT_ARCHIVER_DOWNLOADS_DIR=/downloads`
  - `/data` → `YT_ARCHIVER_DATA_DIR=/data`
  - `/logs` → `YT_ARCHIVER_LOG_DIR=/logs`
  - `/tokens` → `YT_ARCHIVER_TOKENS_DIR=/tokens`
- Use relative paths in `config.json` (e.g. `folder: "YouTube/Channel"`).
- Consider running as a non-root user with a fixed UID/GID to match your mounted volume permissions.
