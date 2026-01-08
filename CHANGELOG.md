# Changelog
All notable changes to this project will be documented here.
## Prior to 1.0.0
- no official release
- only python scripts released, no official package
- archiver.py ran independently
- a desktop GUI was created, paving way for the eventual webUI

## 1.0.0 - First public release
Added:
- Docker-first deployment with explicit volume paths.
- FastAPI backend serving API + static Web UI.
- Web UI for config, runs, status, logs, history, downloads, and cleanup.
- Built-in scheduler (no cron, no systemd).
- SQLite-backed history and state.
- Optional Telegram summaries and optional Basic auth.
- Home Assistant-friendly status and metrics endpoints via API.
- desktop GUI deprecated

## 1.0.1
Added:
- Multi-page Web UI (Home, Config, Downloads, History, Logs) with top navigation.
- Separate playlist progress vs per-video download progress indicators.
- Downloads + History filters with limits, and internal scrolling tables.
- Mobile navigation menu + collapsible filters for small screens.
- Telegram summary truncation to avoid oversized messages.
- Playlist name field in config editor (non-functional label).

Changed:
- Default Downloads/History limit to 50.
- Light-mode header styling and mobile spacing tweaks.

Fixed:
- History “Copy URL” now copies the YouTube URL, not API download URL.
- Config/playlist download paths normalized to remain relative to /downloads.
