#!/usr/bin/env python3
"""
YouTube playlist archiver with robust retries, metadata embedding, and clean filenames.
- Sequential downloads to avoid throttling; retries across multiple extractor profiles.
- Embedded metadata (title/channel/date/description/tags/URL) and thumbnail as cover art.
- Optional final format copy (webm/mp4/mkv) and filename templating.
- Background copy to destination and SQLite history to avoid duplicate downloads.
- Optional Telegram summary after each run.
"""

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime
from threading import Thread

import requests
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from yt_dlp import YoutubeDL

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=os.path.join("logs", "archiver.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

DB_PATH = "database/db.sqlite"

MAX_VIDEO_RETRIES = 4        # Hard cap per video
EXTRACTOR_RETRIES = 2        # Times to retry each extractor before moving on
_RUNTIME_WARNED = False


# ─────────────────────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────────────────────
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            video_id TEXT PRIMARY KEY,
            playlist_id TEXT,
            downloaded_at TIMESTAMP,
            filepath TEXT
        )
    """)
    conn.commit()
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Filename helpers
# ─────────────────────────────────────────────────────────────────────────────
def sanitize_for_filesystem(name, maxlen=180):
    """Remove characters unsafe for filenames and trim length."""
    if not name:
        return ""
    name = re.sub(r"[\\/:*?\"<>|]+", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    try:
        import unicodedata
        name = unicodedata.normalize("NFC", name)
    except ImportError:
        pass
    if len(name) > maxlen:
        name = name[:maxlen].rstrip()
    return name


def pretty_filename(title, channel, upload_date):
    """Cleaner filename for media servers: 'Title - Channel (MM-YYYY)'"""
    title_s = sanitize_for_filesystem(title)
    channel_s = sanitize_for_filesystem(channel)
    if upload_date and len(upload_date) == 8 and upload_date.isdigit():
        mm = upload_date[4:6]
        yyyy = upload_date[0:4]
        return f"{title_s} - {channel_s} ({mm}-{yyyy})"
    else:
        return f"{title_s} - {channel_s}"


# ─────────────────────────────────────────────────────────────────────────────
# Config + API
# ─────────────────────────────────────────────────────────────────────────────
def load_config(path):
    with open(path, "r") as f:
        return json.load(f)


def load_credentials(token_path):
    with open(token_path, "r") as f:
        data = json.load(f)
    return Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
    )


def youtube_service(creds):
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


def build_youtube_clients(accounts, config):
    """
    Build one YouTube API client per configured account for this run.
    Any account that fails auth is skipped (logged) to avoid aborting the run.
    """
    clients = {}
    if not isinstance(accounts, dict):
        return clients
    for name, acc in accounts.items():
        token_path = acc.get("token")
        if not token_path:
            logging.error("Account %s has no 'token' path configured; skipping", name)
            continue
        try:
            creds = load_credentials(token_path)
            clients[name] = youtube_service(creds)
        except RefreshError as e:
            logging.error("OAuth refresh failed for account %s: %s", name, e)
        except Exception:
            logging.exception("Failed to initialize YouTube client for account %s", name)
    return clients


def normalize_js_runtime(js_runtime):
    """Accept bare binary names or paths; return 'name:/full/path' or None."""
    if not js_runtime:
        return None
    if ":" in js_runtime:
        return js_runtime
    path = shutil.which(js_runtime)
    prefix = "node"
    if path and "deno" in os.path.basename(path).lower():
        prefix = "deno"
    elif path and "node" in os.path.basename(path).lower():
        prefix = "node"
    elif os.path.exists(js_runtime):
        path = js_runtime
        prefix = "deno" if "deno" in os.path.basename(js_runtime).lower() else "node"
    if path:
        return f"{prefix}:{path}"
    return None


def resolve_js_runtime(config, override=None):
    runtime = override or config.get("js_runtime") or os.environ.get("YT_DLP_JS_RUNTIME")
    runtime = normalize_js_runtime(runtime)
    if runtime:
        return runtime

    deno = shutil.which("deno")
    if deno:
        return f"deno:{deno}"

    node = shutil.which("node")
    if node:
        return f"node:{node}"

    return None


def get_playlist_videos(youtube, playlist_id):
    videos = []
    page = None
    while True:
        resp = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page,
        ).execute()
        for item in resp.get("items", []):
            videos.append({
                "videoId": item["contentDetails"].get("videoId"),
                "playlistItemId": item.get("id"),
            })
        page = resp.get("nextPageToken")
        if not page:
            break
    return videos


def get_video_metadata(youtube, video_id):
    """Return title, channel, upload_date (YYYYMMDD), description, tags, url, thumbnail_url."""
    resp = youtube.videos().list(
        part="snippet,contentDetails",
        id=video_id,
    ).execute(num_retries=2)

    items = resp.get("items")
    if not items:
        return None

    snip = items[0]["snippet"]
    upload_date = snip.get("publishedAt", "")[:10].replace("-", "")

    thumbnails = snip.get("thumbnails", {}) or {}
    thumb_url = (
        thumbnails.get("maxres", {}).get("url")
        or thumbnails.get("standard", {}).get("url")
        or thumbnails.get("high", {}).get("url")
        or thumbnails.get("medium", {}).get("url")
        or thumbnails.get("default", {}).get("url")
    )

    return {
        "video_id": video_id,
        "title": snip.get("title"),
        "channel": snip.get("channelTitle"),
        "upload_date": upload_date,
        "description": snip.get("description") or "",
        "tags": snip.get("tags") or [],
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "thumbnail_url": thumb_url,
    }


def extract_video_id(url):
    """Best-effort video ID extraction from a YouTube URL."""
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.netloc and "youtu.be" in parsed.netloc and parsed.path:
            return parsed.path.strip("/").split("/")[0]
        qs = urllib.parse.parse_qs(parsed.query or "")
        if "v" in qs and qs["v"]:
            return qs["v"][0]
    except Exception:
        pass
    return None


def get_playlist_videos_fallback(playlist_id):
    """Fetch playlist entries without OAuth (yt-dlp extract_flat).
    Returns (videos, had_error).
    """
    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
    opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,
        "forceipv4": True,
    }
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(playlist_url, download=False)
            entries = info.get("entries") or []
            vids = []
            for entry in entries:
                vid = entry.get("id") or entry.get("url")
                if vid:
                    vids.append({"videoId": vid})
            return vids, False
    except Exception:
        logging.exception("yt-dlp playlist fallback failed for %s", playlist_id)
        return [], True


def get_video_metadata_fallback(video_id_or_url):
    """Metadata without OAuth using yt-dlp (no download)."""
    if video_id_or_url.startswith("http"):
        video_url = video_id_or_url
        vid = extract_video_id(video_id_or_url) or video_id_or_url
    else:
        video_url = f"https://www.youtube.com/watch?v={video_id_or_url}"
        vid = video_id_or_url

    opts = {
        "quiet": True,
        "skip_download": True,
        "forceipv4": True,
    }

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except Exception:
        logging.exception("yt-dlp metadata fallback failed for %s", video_url)
        return None

    if not info:
        return None

    upload_date = info.get("upload_date") or ""
    thumb_url = (
        (info.get("thumbnail") or "")
    )
    return {
        "video_id": vid,
        "title": info.get("title"),
        "channel": info.get("uploader"),
        "upload_date": upload_date,
        "description": info.get("description") or "",
        "tags": info.get("tags") or [],
        "url": video_url,
        "thumbnail_url": thumb_url,
    }


def resolve_video_metadata(yt_client, video_id, allow_public_fallback=True):
    """Try OAuth API first, then yt-dlp fallback (if allowed), then stub metadata."""
    meta = None
    if yt_client:
        try:
            meta = get_video_metadata(yt_client, video_id)
        except HttpError:
            logging.exception("Metadata fetch failed %s", video_id)
        except RefreshError as e:
            logging.error("OAuth refresh failed while fetching video %s: %s", video_id, e)
    if not meta and allow_public_fallback:
        meta = get_video_metadata_fallback(video_id)

    if not meta:
        vid = extract_video_id(video_id) or video_id
        base_url = video_id if isinstance(video_id, str) and str(video_id).startswith("http") else f"https://www.youtube.com/watch?v={vid}"
        meta = {
            "video_id": vid,
            "title": vid,
            "channel": "",
            "upload_date": "",
            "description": "",
            "tags": [],
            "url": base_url,
            "thumbnail_url": None,
        }
    return meta


# ─────────────────────────────────────────────────────────────────────────────
# Async copy worker
# ─────────────────────────────────────────────────────────────────────────────
def async_copy(src, dst, callback):
    def run():
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            callback(True, dst)
        except Exception as e:
            logging.exception("Copy failed: %s", e)
            callback(False, dst)

    t = Thread(target=run, daemon=True)
    t.start()
    return t


# ─────────────────────────────────────────────────────────────────────────────
# Telegram notification
# ─────────────────────────────────────────────────────────────────────────────
def telegram_notify(config, message):
    tg = config.get("telegram")
    if not tg:
        return

    token = tg.get("bot_token")
    chat_id = tg.get("chat_id")
    if not token or not chat_id:
        return

    text = urllib.parse.quote_plus(message)
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={text}"

    try:
        urllib.request.urlopen(url, timeout=10).read()
    except Exception as e:
        logging.error("Telegram notify failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Partial file check
# ─────────────────────────────────────────────────────────────────────────────
def is_partial_file_stuck(temp_dir, vid):
    """Detect if partial .part file is frozen or empty."""
    if not os.path.isdir(temp_dir):
        return False
    for f in os.listdir(temp_dir):
        if f.startswith(vid) and f.endswith(".part"):
            p = os.path.join(temp_dir, f)
            try:
                size = os.path.getsize(p)
                # 0 bytes or <512KB after significant time = stuck
                if size < 1024 * 512:
                    return True
            except Exception:
                return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Metadata embedding
# ─────────────────────────────────────────────────────────────────────────────
def embed_metadata(local_file, meta, video_id):
    """Embed title/channel/date/description/tags/url + thumbnail into local_file (in place)."""
    if not meta:
        return

    title = meta.get("title") or video_id
    channel = meta.get("channel") or ""
    upload_date = meta.get("upload_date") or ""
    description = meta.get("description") or ""
    tags = meta.get("tags") or []
    url = meta.get("url") or f"https://www.youtube.com/watch?v={video_id}"
    thumb_url = meta.get("thumbnail_url")

    # Convert YYYYMMDD -> YYYY-MM-DD if possible
    date_tag = ""
    if len(upload_date) == 8 and upload_date.isdigit():
        date_tag = f"{upload_date[0:4]}-{upload_date[4:6]}-{upload_date[6:8]}"

    keywords = ", ".join(tags) if tags else ""
    comment = f"YouTubeID={video_id} URL={url}"

    # Download thumbnail (best effort)
    thumb_path = None
    if thumb_url:
        try:
            os.makedirs("/tmp/yt-dlp/thumbs", exist_ok=True)
            thumb_path = os.path.join("/tmp/yt-dlp/thumbs", f"{video_id}.jpg")
            resp = requests.get(thumb_url, timeout=15)
            if resp.ok and resp.content:
                with open(thumb_path, "wb") as f:
                    f.write(resp.content)
            else:
                thumb_path = None
        except Exception:
            logging.exception("Thumbnail download failed for %s", video_id)
            thumb_path = None

    # Keep the same container extension to avoid invalid remuxes (e.g., MP4 into WebM)
    base_ext = os.path.splitext(local_file)[1] or ".webm"
    ext_lower = base_ext.lower()
    audio_only = ext_lower in [".mp3", ".m4a", ".opus", ".aac", ".flac"]
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=f".tagged{base_ext}", dir=os.path.dirname(local_file))
    os.close(tmp_fd)

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            local_file,
        ]

        # Attach thumbnail as Matroska attachment if we have one
        if thumb_path and os.path.exists(thumb_path) and not audio_only:
            cmd.extend([
                "-attach", thumb_path,
                "-metadata:s:t", "mimetype=image/jpeg",
                "-metadata:s:t", "filename=cover.jpg",
            ])

        # Core metadata
        if title:
            cmd.extend(["-metadata", f"title={title}"])
        if channel:
            cmd.extend(["-metadata", f"artist={channel}"])
        if date_tag:
            cmd.extend(["-metadata", f"date={date_tag}"])
        if description:
            cmd.extend(["-metadata", f"description={description}"])
        if keywords:
            cmd.extend(["-metadata", f"keywords={keywords}"])
        if comment:
            cmd.extend(["-metadata", f"comment={comment}"])

        # Copy streams, don't re-encode
        cmd.extend([
            "-c",
            "copy",
            tmp_path,
        ])

        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        os.replace(tmp_path, local_file)
        logging.info("[%s] Metadata embedded successfully", video_id)
    except subprocess.CalledProcessError:
        logging.exception("ffmpeg metadata embedding failed for %s", video_id)
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
    except Exception:
        logging.exception("Unexpected error during metadata embedding for %s", video_id)
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
    finally:
        if thumb_path:
            try:
                os.unlink(thumb_path)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# yt-dlp (WEBM + MP4 fallback)
# ─────────────────────────────────────────────────────────────────────────────
def download_with_ytdlp(video_url, temp_dir, js_runtime=None, meta=None, config=None,
                        target_format=None, audio_only=False):
    vid = extract_video_id(video_url) or (video_url.split("v=")[-1] if "v=" in video_url else "video")
    if meta and meta.get("video_id"):
        vid = meta.get("video_id")
    js_runtime = normalize_js_runtime(js_runtime)

    FORMAT_WEBM = (
        # Preferred: WebM (VP9/Opus)
        "bestvideo[ext=webm][height<=1080]+bestaudio[ext=webm]/"
        "bestvideo[ext=webm][height<=720]+bestaudio[ext=webm]/"
        # Fallback: MP4 (H.264/AAC)
        "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/"
        "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]"
    )

    audio_formats = {"mp3", "m4a", "aac", "opus", "flac"}
    inherited_fmt = None
    if not target_format and config:
        inherited_fmt = config.get("final_format")
    target_fmt = (target_format or inherited_fmt or "").lower() or None
    audio_mode = audio_only or (target_fmt in audio_formats)
    preferred_exts = []

    if audio_mode:
        format_selector = "bestaudio/best"
        preferred_exts.append(target_fmt or "mp3")
    else:
        format_selector = FORMAT_WEBM
        if target_fmt:
            preferred_exts.append(target_fmt)
        preferred_exts.extend(["webm", "mp4", "mkv", "m4a", "opus"])

    extractor_chain = [
        ("android", {
            "User-Agent": "com.google.android.youtube/19.42.37 (Linux; Android 14)",
            "Accept-Language": "en-US,en;q=0.9",
        }),
        ("tv_embedded", {
            "User-Agent": "Mozilla/5.0 (SmartTV; Linux; Tizen 6.5) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }),
        ("web", {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
                " AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }),
    ]

    global _RUNTIME_WARNED
    if not js_runtime and not _RUNTIME_WARNED:
        logging.warning("No JS runtime configured/detected; set js_runtime in config or pass --js-runtime to reduce SABR/missing format issues.")
        _RUNTIME_WARNED = True

    for attempt in range(MAX_VIDEO_RETRIES):
        logging.info(f"[{vid}] Download attempt {attempt+1}/{MAX_VIDEO_RETRIES}")

        for client_name, headers in extractor_chain:
            logging.info(f"[{vid}] Trying extractor: {client_name}")

            for _ in range(EXTRACTOR_RETRIES):
                # Reset temp dir if stuck
                if os.path.exists(temp_dir):
                    if is_partial_file_stuck(temp_dir, vid):
                        logging.warning(f"[{vid}] Stuck partial detected, wiping temp_dir")
                        shutil.rmtree(temp_dir, ignore_errors=True)

                shutil.rmtree(temp_dir, ignore_errors=True)
                os.makedirs(temp_dir, exist_ok=True)

                opts = {
                    "outtmpl": os.path.join(temp_dir, "%(id)s.%(ext)s"),
                    "paths": {"temp": "/tmp/yt-dlp"},
                    "format": format_selector,
                    "quiet": True,
                    "continuedl": True,
                    "socket_timeout": 120,
                    "retries": 5,
                    "forceipv4": True,
                    "http_headers": headers,
                    "extractor_args": {"youtube": [f"player_client={client_name}"]},
                    "remote_components": ["ejs:github"],
                }

                if audio_mode:
                    opts["postprocessors"] = [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": target_fmt or "mp3",
                        "preferredquality": "0",
                    }]

                # Allow caller to inject/override yt-dlp options via config (non-critical settings)
                if config and config.get("yt_dlp_opts"):
                    try:
                        user_opts = config.get("yt_dlp_opts") or {}
                        opts.update(user_opts)
                    except Exception:
                        logging.exception("Failed to merge yt_dlp_opts from config")

                # Enforce the format selector even if user opts provided their own format
                opts["format"] = format_selector
                if audio_mode:
                    opts["postprocessors"] = [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": target_fmt or "mp3",
                        "preferredquality": "0",
                    }]

                if js_runtime:
                    runtime_name, runtime_path = js_runtime.split(":", 1)
                    opts["js_runtimes"] = {runtime_name: {"path": runtime_path}}

                try:
                    with YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(video_url, download=True)
                except Exception as e:
                    logging.warning(f"[{vid}] {client_name} failed: {e}")
                    continue

                vid_for_files = info.get("id") or vid

                if not info:
                    logging.warning(f"[{vid}] No info returned from extractor {client_name}")
                    continue

                # Prefer .webm if present, else accept mp4
                chosen = None
                search_exts = preferred_exts + ["webm", "mp4", "mkv", "m4a", "opus", "mp3", "aac", "flac"]
                for ext in search_exts:
                    candidate = os.path.join(temp_dir, f"{vid_for_files}.{ext}")
                    if os.path.exists(candidate):
                        chosen = candidate
                        break
                if not chosen:
                    for f in os.listdir(temp_dir):
                        if f.startswith(vid_for_files) and not f.endswith(".part"):
                            chosen = os.path.join(temp_dir, f)
                            break

                if chosen:
                    logging.info(f"[{vid}] SUCCESS via {client_name} → {os.path.basename(chosen)}")

                    # Embed metadata first
                    embed_metadata(chosen, meta, vid)

                    # Post-processing final format conversion (if needed)
                    desired_ext = target_fmt or (config.get("final_format") if config else None)
                    if desired_ext and not audio_mode:
                        current_ext = os.path.splitext(chosen)[1].lstrip(".").lower()
                        # Avoid container mismatch: don't force mp4 -> webm without re-encode
                        if current_ext == "mp4" and desired_ext == "webm":
                            logging.warning("[%s] Skipping mp4->webm container copy to avoid invalid file; consider final_format=mp4", vid)
                        elif current_ext != desired_ext:
                            base = os.path.splitext(chosen)[0]
                            converted = f"{base}.{desired_ext}"
                            try:
                                subprocess.run(
                                    ["ffmpeg", "-y", "-i", chosen, "-c", "copy", converted],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
                                )
                                os.remove(chosen)
                                chosen = converted
                            except Exception:
                                logging.exception("Final format conversion failed for %s", vid)

                    return chosen

                logging.warning(f"[{vid}] Extractor {client_name} produced no usable output")

        logging.warning(f"[{vid}] All extractors failed this attempt.")

    logging.error(f"[{vid}] PERMANENT FAILURE after {MAX_VIDEO_RETRIES} attempts.")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────
def run_single_download(config, video_url, destination=None, final_format_override=None):
    """Download a single URL (no OAuth required)."""
    js_runtime = resolve_js_runtime(config)
    meta = resolve_video_metadata(None, extract_video_id(video_url) or video_url)

    vid = meta.get("video_id") or extract_video_id(video_url) or "video"
    temp_dir = os.path.join("temp_downloads", vid)

    dest_dir = os.path.expanduser(destination or config.get("single_download_folder") or "downloads")
    os.makedirs(dest_dir, exist_ok=True)

    local_file = download_with_ytdlp(
        video_url,
        temp_dir,
        js_runtime,
        meta,
        config,
        target_format=final_format_override,
    )
    if not local_file:
        logging.error("Download FAILED: %s", video_url)
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False

    ext = os.path.splitext(local_file)[1].lstrip(".") or final_format_override or config.get("final_format") or "webm"

    template = config.get("filename_template")
    if template:
        try:
            cleaned_name = template % {
                "title": sanitize_for_filesystem(meta.get("title") or vid),
                "uploader": sanitize_for_filesystem(meta.get("channel") or ""),
                "upload_date": meta.get("upload_date") or "",
                "ext": ext
            }
        except Exception:
            cleaned_name = f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}_{vid[:8]}.{ext}"
    else:
        cleaned_name = f"{pretty_filename(meta.get('title'), meta.get('channel'), meta.get('upload_date'))}_{vid[:8]}.{ext}"

    final_path = os.path.join(dest_dir, cleaned_name)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    shutil.copy2(local_file, final_path)
    shutil.rmtree(temp_dir, ignore_errors=True)

    logging.info("Direct download saved to %s", final_path)
    return True


def run_once(config):
    LOCK_FILE = "/tmp/yt_archiver.lock"

    run_successes = []
    run_failures = []

    if os.path.exists(LOCK_FILE):
        logging.warning("Lockfile present — skipping run")
        return

    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

    conn = init_db()
    cur = conn.cursor()

    accounts = config.get("accounts", {}) or {}
    playlists = config.get("playlists", []) or []
    js_runtime = resolve_js_runtime(config)
    global_final_format = config.get("final_format")

    pending_copies = []
    yt_clients = build_youtube_clients(accounts, config) if accounts else {}

    try:
        for pl in playlists:
            playlist_id = pl.get("playlist_id") or pl.get("id")
            target_folder = pl.get("folder") or pl.get("directory")
            account = pl.get("account")
            remove_after = pl.get("remove_after_download", False)
            playlist_format = pl.get("final_format") or global_final_format

            if not playlist_id or not target_folder:
                logging.error("Playlist entry missing id or folder: %s", pl)
                continue

            yt = yt_clients.get(account) if account else None
            allow_public = not account

            videos = []
            fetch_error = False
            fallback_error = False
            if account and not yt:
                logging.error("No valid YouTube client for account '%s'; skipping playlist %s", account, playlist_id)
                run_failures.append(f"{playlist_id} (auth)")
                continue

            if yt:
                try:
                    videos = get_playlist_videos(yt, playlist_id)
                except HttpError:
                    logging.exception("Playlist fetch failed %s", playlist_id)
                    fetch_error = True
                    run_failures.append(f"{playlist_id} (auth)")
                    continue
                except RefreshError as e:
                    logging.error("OAuth refresh failed for account %s while fetching playlist %s: %s", account, playlist_id, e)
                    run_failures.append(f"{playlist_id} (auth)")
                    yt_clients[account] = None
                    continue

            if not videos and allow_public:
                videos, fallback_error = get_playlist_videos_fallback(playlist_id)

            if not videos:
                if fetch_error or fallback_error:
                    logging.error("No videos found for playlist %s (auth or public fetch failed)", playlist_id)
                    run_failures.append(f"{playlist_id} (auth)")
                else:
                    logging.info("Playlist %s is empty; skipping.", playlist_id)
                continue

            for entry in videos:
                vid = entry.get("videoId") or entry.get("id")
                if not vid:
                    continue

                cur.execute("SELECT video_id FROM downloads WHERE video_id=?", (vid,))
                if cur.fetchone():
                    continue

                meta = resolve_video_metadata(yt, vid, allow_public_fallback=allow_public)

                logging.info("START download: %s (%s)", vid, meta.get("title"))

                video_url = meta.get("url") or f"https://www.youtube.com/watch?v={vid}"
                temp_dir = os.path.join("temp_downloads", vid)

                local_file = download_with_ytdlp(
                    video_url,
                    temp_dir,
                    js_runtime,
                    meta,
                    config,
                    target_format=playlist_format,
                )
                if not local_file:
                    logging.warning("Download FAILED: %s", vid)
                    run_failures.append(meta.get("title") or vid)
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    continue

                # Determine extension based on the resulting file or playlist/default format
                ext = os.path.splitext(local_file)[1].lstrip(".") or playlist_format or "webm"

                # Build filename using filename_template if present
                template = config.get("filename_template")
                if template:
                    try:
                        cleaned_name = template % {
                            "title": sanitize_for_filesystem(meta.get("title") or vid),
                            "uploader": sanitize_for_filesystem(meta.get("channel") or ""),
                            "upload_date": meta.get("upload_date") or "",
                            "ext": ext
                        }
                    except Exception:
                        cleaned_name = f"{pretty_filename(meta['title'], meta['channel'], meta['upload_date'])}_{vid[:8]}.{ext}"
                else:
                    cleaned_name = f"{pretty_filename(meta['title'], meta['channel'], meta['upload_date'])}_{vid[:8]}.{ext}"

                final_path = os.path.join(target_folder, cleaned_name)

                def after_copy(success, dst, video_id=vid, playlist=playlist_id,
                               entry_id=entry.get("playlistItemId"),
                               temp=temp_dir, remove=remove_after, yt_service=yt):

                    if success:
                        logging.info("Copy OK → %s", dst)
                        run_successes.append(cleaned_name)
                        try:
                            with sqlite3.connect(DB_PATH, check_same_thread=False) as c:
                                c.execute(
                                    "INSERT INTO downloads (video_id, playlist_id, downloaded_at, filepath)"
                                    " VALUES (?, ?, ?, ?)",
                                    (video_id, playlist, datetime.utcnow(), dst)
                                )
                                c.commit()
                        except Exception:
                            logging.exception("DB insert failed for %s", video_id)
                    else:
                        logging.error("Copy FAILED for %s", video_id)
                        run_failures.append(cleaned_name)

                    shutil.rmtree(temp, ignore_errors=True)

                    if success and remove and entry_id and yt_service:
                        try:
                            yt_service.playlistItems().delete(id=entry_id).execute()
                        except Exception:
                            logging.exception("Failed removing %s", video_id)

                t = async_copy(local_file, final_path, after_copy)
                pending_copies.append(t)
                logging.info("COPY started in background → next download begins")

        for t in pending_copies:
            t.join()
        logging.info("\n" + ("-" * 80) + "\n")
        logging.info("Run complete.")
        logging.info("\n" + ("-" * 80) + "\n \n \n")

    finally:
        conn.close()
        try:
            # Telegram Summary
            if run_successes or run_failures:
                msg = "YouTube Archiver Summary\n"
                msg += f"✔ Success: {len(run_successes)}\n"
                msg += f"✖ Failed: {len(run_failures)}\n\n"

                if run_successes:
                    msg += "Downloaded:\n" + "\n".join(f"• {t}" for t in run_successes) + "\n\n"
                if run_failures:
                    msg += "Failed:\n" + "\n".join(f"• {t}" for t in run_failures)

                telegram_notify(config, msg)
            os.remove(LOCK_FILE)
        except FileNotFoundError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Entry
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.json")
    parser.add_argument("--single-url", help="Download a single URL and exit (no playlist scan).")
    parser.add_argument("--destination", help="Destination directory for --single-url downloads.")
    parser.add_argument("--format", dest="final_format_override", help="Override final format/container (e.g., mp3, mp4, webm, mkv).")
    parser.add_argument("--js-runtime", help="Force JS runtime (e.g., node:/usr/bin/node or deno:/usr/bin/deno).")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        logging.error("Config file not found: %s", args.config)
        return

    config = load_config(args.config)
    if args.js_runtime:
        config["js_runtime"] = args.js_runtime
    if args.single_url:
        ok = run_single_download(config, args.single_url, args.destination, args.final_format_override)
        if not ok:
            sys.exit(1)
        return

    run_once(config)


if __name__ == "__main__":
    main()
