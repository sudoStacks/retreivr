# YouTube Archiver

YouTube playlist archiver: downloads high-quality videos sequentially, retries across multiple extractor profiles, embeds rich metadata + cover art, and copies to your library while tracking history in SQLite. Optional helpers include a GUI for easy edit of config.json and running single-url downloads, and a one-time OAuth token generator.

 Use at your own discretion - low volume should not trigger any bans or blacklisting but this is not guaranteed.

 Videos download as webm first attempt, then mp4 - final format (webm, mp4, mkv) can be configured in config.json. 

 Telegram messages optional - sends a summary only if there were attempted downloads. Nice feature to have setup!

## v2.0 highlights
- GUI upgrades: direct single-URL downloads (no OAuth needed), run full playlist archiver from the GUI, per-playlist format overrides, progress indicators.
- Public playlists supported without OAuth when `account` is blank; playlists with an `account` use OAuth only (no public fallback).
- Empty playlists are treated as no-ops (not reported as failures).
- `js_runtime` is normalized (node/deno path or bare name) and honored for all runs.

## What’s here
- `archiver.py` — main downloader (sequential, retries, metadata embed, filename templating, optional Telegram summary).
- `config/config_sample.json` — copy to `config/config.json` and fill in your accounts/playlists/telegram ID's/node path, etc.
- `setup_oauth.py` — headless OAuth helper to generate token JSONs.
- `config_gui.py` — optional GUI to edit `config.json` (accounts are read-only; manage token paths on disk), to run Single-URL downloads and to run the full playlist archiver.py script as well.

## Requirements
- Python 3.10+
- ffmpeg on PATH (for metadata/cover art)
- yt-dlp (installed via `requirements.txt`)
- Deno or Node.js runtime (auto-detected, or set `js_runtime`/`YT_DLP_JS_RUNTIME`)
- Google Cloud project with YouTube Data API v3 enabled (Only if archiving private playlists)

## Install
```bash
git clone https://github.com/yourname/youtube-archiver.git
cd youtube-archiver
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```
Install ffmpeg via your package manager (Homebrew `brew install ffmpeg`, Debian/Ubuntu `sudo apt-get install ffmpeg`, etc.).

## Configure
1) Copy the sample: `cp config/config_sample.json config/config.json`
2) Create OAuth client in Google Cloud (Desktop app) and download `client_secret_*.json` into `tokens/`.
3) Generate tokens (one per account):
   ```bash
   python setup_oauth.py --account family_tv tokens/client_secret_family.json tokens/token_family.json
   ```
4) Edit `config/config.json`:
   - `accounts` → paths to client_secret and token JSONs (optional if you only pull public playlists/direct URLs)
   - `playlists` → playlist_id, folder, account, optional remove_after_download, optional `final_format` override (set to `mp3` for audio-only on that playlist)
   - `filename_template` → Python `%` template with `title`, `uploader`, `upload_date`, `ext`
   - `final_format` → `webm`, `mp4`, `mkv`, or `mp3`; can be blank (keep downloaded container) or overridden per-playlist; `mp3` triggers audio-only extraction
   - `js_runtime` → strongly recommended; set to `node:/path/to/node` or `deno:/path/to/deno` (bare names/paths are accepted and normalized) to avoid SABR/missing-format issues; used for playlists and single URL runs
   - `single_download_folder` → optional default folder for direct single-URL downloads
   - `telegram` → optional bot_token/chat_id for summaries
   - `yt_dlp_opts` → optional extra yt-dlp options to merge

Download order: prefers WebM VP9/Opus (1080p → 720p), then MP4 (1080p → 720p). Metadata embedding keeps the original container; MP4→WebM remux is skipped to avoid broken files.

## Run (headless)
```bash
source .venv/bin/activate
umask 0002                        # if you need group-writable files (e.g., NFS/SMB)
python archiver.py --config config/config.json
```
- Uses `temp_downloads/` for work files, logs to `logs/archiver.log`, history in `database/db.sqlite`, lockfile at `/tmp/yt_archiver.lock`.
- Ensure the user running the archiver owns the repo/logs/db/temp folders and can write to the temp directory (`/tmp/yt-dlp` by default).

Single URL (no OAuth required, format optional):
```bash
python archiver.py --config config/config.json --single-url "https://www.youtube.com/watch?v=dQw4w9WgXcQ" --destination ~/Downloads --format mp3 --js-runtime "node:/usr/bin/node"
```

### Cron example (run as a non-root user)
```
*/30 * * * * umask 0002; cd /opt/Scripts/youtube-archiver && /opt/Scripts/youtube-archiver/.venv/bin/python3 archiver.py --config /opt/Scripts/youtube-archiver/config/config.json >> /opt/Scripts/youtube-archiver/logs/cron.log 2>&1
```
[I ran in Debian LXC and created a 'media' user with UID 1000, and added this to the media crontab - was required for my setup to get permissions correct!]

### Optional GUI
```bash
python config_gui.py
```
Pick your `config.json`, edit playlists/Telegram/template/final format, and save. Accounts are displayed read-only; manage OAuth files on disk.
GUI capabilities:
- Single URL downloader (no OAuth needed) with format/folder picker
- Run full playlist archiver from the GUI
- Per-playlist format overrides, audio-only via `mp3`
- Progress indicators for single and full runs

## Notes & tips
- Keep `tokens/` and real `config/config.json` out of version control (.gitignore already does).
- If you need a custom temp path, set it via `yt_dlp_opts` → `"paths": { "temp": "/path/you/own" }`.
- ffmpeg is required for metadata embedding and cover art.
- Public playlists with `account` blank use unauthenticated download; playlists with `account` set use OAuth only. Empty playlists are skipped quietly.

## License
MIT — see LICENSE.
