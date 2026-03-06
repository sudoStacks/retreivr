import logging
import hashlib
import threading
import time
from urllib.parse import urlparse
from typing import Protocol, runtime_checkable

import requests
from yt_dlp import YoutubeDL

from engine.json_utils import safe_json_dumps

_DISCOVERY_YTDLP_INSTANCES = {}
_DISCOVERY_YTDLP_INSTANCE_LOCKS = {}
_DISCOVERY_YTDLP_REGISTRY_LOCK = threading.Lock()

_YOUTUBE_INNERTUBE_SEARCH_URL = "https://www.youtube.com/youtubei/v1/search"
_YOUTUBE_INNERTUBE_CLIENT_VERSION = "2.20240304.00.00"

def _is_http_url(value):
    if not value or not isinstance(value, str):
        return False
    try:
        return urlparse(value).scheme in ("http", "https")
    except Exception:
        return False


def _extract_youtube_video_id(value):
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").strip("/")
    if host == "youtu.be":
        head = path.split("/")[0]
        return head or None
    if "youtube.com" in host:
        if parsed.query:
            parts = parsed.query.split("&")
            for part in parts:
                if part.startswith("v="):
                    vid = part[2:].strip()
                    if vid:
                        return vid
        path_parts = [part for part in path.split("/") if part]
        if len(path_parts) >= 2 and path_parts[0] in {"shorts", "embed", "live"}:
            return path_parts[1]
    return None


def _extract_text_from_runs(value):
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    if not isinstance(value, dict):
        return None
    simple = value.get("simpleText")
    if isinstance(simple, str) and simple.strip():
        return simple.strip()
    runs = value.get("runs")
    if not isinstance(runs, list):
        return None
    parts = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        text = run.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    out = "".join(parts).strip()
    return out if out else None


def _walk_video_renderers(node):
    if isinstance(node, dict):
        video = node.get("videoRenderer")
        if isinstance(video, dict):
            yield video
        for value in node.values():
            yield from _walk_video_renderers(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk_video_renderers(value)


def youtube_fast_search(query: str, limit: int = 10):
    text = str(query or "").strip()
    if not text:
        return []
    capped_limit = max(1, min(int(limit or 10), 50))
    started = time.perf_counter()
    results = []
    try:
        payload = {
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": _YOUTUBE_INNERTUBE_CLIENT_VERSION,
                }
            },
            "query": text,
        }
        response = requests.post(
            _YOUTUBE_INNERTUBE_SEARCH_URL,
            json=payload,
            timeout=2.0,
        )
        if not response.ok:
            return []
        data = response.json() if response.content else {}
        for renderer in _walk_video_renderers(data):
            video_id = str(renderer.get("videoId") or "").strip()
            if not video_id:
                continue
            title = _extract_text_from_runs(renderer.get("title")) or f"YouTube Video ({video_id})"
            owner = renderer.get("ownerText") or renderer.get("longBylineText") or renderer.get("shortBylineText")
            channel = _extract_text_from_runs(owner) or "YouTube"
            results.append(
                {
                    "video_id": video_id,
                    "title": title,
                    "channel": channel,
                    "url": f"https://youtube.com/watch?v={video_id}",
                    "thumbnail_url": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                }
            )
            if len(results) >= capped_limit:
                break
        return results
    except Exception:
        return []
    finally:
        duration_ms = int((time.perf_counter() - started) * 1000)
        logging.info(
            safe_json_dumps(
                {
                    "event": "youtube_fast_search_complete",
                    "query": text,
                    "duration_ms": duration_ms,
                    "result_count": len(results),
                }
            )
        )


def _is_restricted_entry(entry):
    if not isinstance(entry, dict):
        return False
    try:
        age_limit = entry.get("age_limit")
        if age_limit is not None and int(age_limit) >= 18:
            return True
    except Exception:
        pass
    availability = str(entry.get("availability") or "").strip().lower()
    if availability and (
        "age" in availability
        or "adult" in availability
        or availability in {"needs_auth", "login_required"}
    ):
        return True
    return False


class SearchAdapter:
    source = ""

    def search_track(self, artist, track, album=None, limit=5, *, lightweight=False, timeout_budget_sec=None):
        raise NotImplementedError

    def search_album(self, artist, album, limit=5, *, lightweight=False, timeout_budget_sec=None):
        raise NotImplementedError

    def expand_album_to_tracks(self, candidate_album):
        return None

    def source_modifier(self, candidate):
        return 1.0

    def _candidate_thumbnail_url(self, entry):
        return None


@runtime_checkable
class MusicSearchAdapter(Protocol):
    source: str

    def search_music_track(self, query: str, limit: int) -> list[dict]:
        ...


class _YtDlpSearchMixin(SearchAdapter):
    search_prefix = ""
    supports_lightweight = False

    def _get_discovery_ydl(self, *, socket_timeout):
        timeout_value = max(1.0, float(socket_timeout or 2.0))
        key = (str(self.source or "").strip().lower(), round(timeout_value, 2))
        with _DISCOVERY_YTDLP_REGISTRY_LOCK:
            instance = _DISCOVERY_YTDLP_INSTANCES.get(key)
            instance_lock = _DISCOVERY_YTDLP_INSTANCE_LOCKS.get(key)
            if instance is not None and instance_lock is not None:
                return instance, instance_lock
            opts = {
                "skip_download": True,
                "quiet": True,
                "no_warnings": True,
                "ignoreerrors": True,
                "noplaylist": True,
                "cachedir": False,
                "socket_timeout": timeout_value,
                # Discovery must stay shallow and avoid deep metadata probing.
                "extract_flat": True,
                "lazy_playlist": True,
                "retries": 0,
                "extractor_retries": 0,
            }
            instance = YoutubeDL(opts)
            instance_lock = threading.Lock()
            _DISCOVERY_YTDLP_INSTANCES[key] = instance
            _DISCOVERY_YTDLP_INSTANCE_LOCKS[key] = instance_lock
            return instance, instance_lock

    def _search(self, query, limit, *, lightweight=False, timeout_budget_sec=None):
        if not query:
            return []
        search_term = f"{self.search_prefix}{limit}:{query}"
        socket_timeout = 10.0
        if timeout_budget_sec is not None:
            try:
                socket_timeout = max(1.0, float(timeout_budget_sec))
            except Exception:
                socket_timeout = 10.0
        opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "noplaylist": True,
            "cachedir": False,
            "socket_timeout": socket_timeout,
        }
        if lightweight:
            opts["extract_flat"] = True
            opts["lazy_playlist"] = True
            opts["retries"] = 0
            opts["extractor_retries"] = 0
        try:
            if lightweight:
                ydl, ydl_lock = self._get_discovery_ydl(socket_timeout=socket_timeout)
                with ydl_lock:
                    info = ydl.extract_info(search_term, download=False)
            else:
                with YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(search_term, download=False)
        except Exception:
            logging.exception("Search failed for source=%s query=%s", self.source, query)
            return []

        entries = info.get("entries") if isinstance(info, dict) else None
        if not entries:
            return []

        results = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if _is_restricted_entry(entry):
                logging.debug("Skipping restricted search result from %s", self.source)
                continue
            url = entry.get("webpage_url")
            if not _is_http_url(url):
                # yt-dlp search extractors often expose internal extractor URLs (e.g. bandcampsearch5)
                # which must never be treated as real URLs.
                logging.debug(
                    "Skipping non-http search result from %s: %r",
                    self.source,
                    entry.get("url"),
                )
                continue

            title = entry.get("title")
            if not title:
                continue
            uploader = entry.get("uploader") or entry.get("channel")
            video_id = entry.get("id") or _extract_youtube_video_id(url)
            thumbnail_url = self._candidate_thumbnail_url(entry)
            if not thumbnail_url and video_id:
                thumbnail_url = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
            if lightweight:
                candidate = {
                    "source": self.source,
                    "video_id": str(video_id).strip() if video_id else None,
                    "url": url,
                    "title": title,
                    "uploader": uploader,
                    "channel": uploader,
                    "thumbnail_url": thumbnail_url,
                    # Preserve result-row compatibility with existing scoring/render paths.
                    "artist_detected": uploader,
                    "album_detected": None,
                    "track_detected": title,
                    "duration_sec": None,
                    "artwork_url": None,
                    "raw_meta_json": "{}",
                    "official": False,
                    "isrc": None,
                    "track_count": None,
                    "view_count": None,
                }
                results.append(candidate)
                continue
            isrc = entry.get("isrc")
            if not isrc:
                isrcs = entry.get("isrcs")
                if isinstance(isrcs, list) and isrcs:
                    isrc = isrcs[0]
            track_count = entry.get("track_count") or entry.get("n_entries") or entry.get("playlist_count")
            candidate = {
                "source": self.source,
                "video_id": str(video_id).strip() if video_id else None,
                "url": url,
                "title": title,
                "uploader": uploader,
                "channel": uploader,
                "artist_detected": entry.get("artist") or uploader,
                "album_detected": entry.get("album"),
                "track_detected": entry.get("track") or entry.get("title"),
                "duration_sec": entry.get("duration"),
                "artwork_url": entry.get("thumbnail"),
                "raw_meta_json": safe_json_dumps(entry),
                "official": bool(entry.get("is_official")),
                "isrc": isrc,
                "track_count": track_count,
                "view_count": entry.get("view_count"),
            }
            candidate["thumbnail_url"] = None
            if thumbnail_url:
                candidate["thumbnail_url"] = thumbnail_url
            results.append(candidate)
        return results

    def search_track(self, artist, track, album=None, limit=5, *, lightweight=False, timeout_budget_sec=None):
        query = f"{artist} {track}".strip()
        if album:
            query = f"{query} {album}".strip()
        allow_lightweight = bool(lightweight and self.supports_lightweight)
        return self._search(query, limit, lightweight=allow_lightweight, timeout_budget_sec=timeout_budget_sec)

    def search_album(self, artist, album, limit=5, *, lightweight=False, timeout_budget_sec=None):
        query = f"{artist} {album}".strip()
        allow_lightweight = bool(lightweight and self.supports_lightweight)
        return self._search(query, limit, lightweight=allow_lightweight, timeout_budget_sec=timeout_budget_sec)

    def search_music_track(self, query, limit=5):
        # Explicit music search interface for deterministic worker orchestration.
        results = []
        for candidate in self._search(query, limit) or []:
            candidate = dict(candidate)
            url = candidate.get("url")
            candidate_id = candidate.get("candidate_id")
            if not candidate_id:
                stable_seed = str(url or candidate.get("title") or "")
                candidate_id = hashlib.sha1(stable_seed.encode("utf-8")).hexdigest()[:16]
            duration_sec = candidate.get("duration_sec")
            duration_ms = None
            try:
                if duration_sec is not None:
                    duration_ms = int(duration_sec) * 1000
            except Exception:
                duration_ms = None
            normalized = {
                "source": self.source,
                "candidate_id": candidate_id,
                "title": candidate.get("title"),
                "artist": candidate.get("artist_detected") or candidate.get("uploader"),
                "duration_ms": duration_ms,
                "url": url,
                "extra": {
                    "uploader": candidate.get("uploader"),
                    "album_detected": candidate.get("album_detected"),
                    "track_detected": candidate.get("track_detected"),
                    "artwork_url": candidate.get("artwork_url"),
                    "raw_meta_json": candidate.get("raw_meta_json"),
                    "official": candidate.get("official"),
                    "isrc": candidate.get("isrc"),
                    "track_count": candidate.get("track_count"),
                    "thumbnail_url": candidate.get("thumbnail_url"),
                },
                # Preserve legacy scoring inputs.
                "uploader": candidate.get("uploader"),
                "artist_detected": candidate.get("artist_detected"),
                "album_detected": candidate.get("album_detected"),
                "track_detected": candidate.get("track_detected"),
                "duration_sec": duration_sec,
                "artwork_url": candidate.get("artwork_url"),
                "raw_meta_json": candidate.get("raw_meta_json"),
                "official": candidate.get("official"),
                "isrc": candidate.get("isrc"),
                "track_count": candidate.get("track_count"),
                "view_count": candidate.get("view_count"),
                "thumbnail_url": candidate.get("thumbnail_url"),
            }
            results.append(normalized)
        return results


class YouTubeMusicAdapter(_YtDlpSearchMixin):
    source = "youtube_music"
    search_prefix = "ytsearch"
    supports_lightweight = True

    def _candidate_thumbnail_url(self, entry):
        video_id = entry.get("id")
        if not isinstance(video_id, str):
            return None
        video_id = video_id.strip()
        if not video_id:
            return None
        return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

    def source_modifier(self, candidate):
        if candidate.get("official"):
            return 1.0
        return 0.90


class YouTubeAdapter(_YtDlpSearchMixin):
    source = "youtube"
    search_prefix = "ytsearch"
    supports_lightweight = True

    def _candidate_thumbnail_url(self, entry):
        video_id = entry.get("id")
        if not isinstance(video_id, str):
            return None
        video_id = video_id.strip()
        if not video_id:
            return None
        return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

    def source_modifier(self, candidate):
        # Neutral weight for general video content
        return 0.85


class SoundCloudAdapter(_YtDlpSearchMixin):
    source = "soundcloud"
    search_prefix = "scsearch"

    def _candidate_thumbnail_url(self, entry):
        artwork_url = entry.get("artwork_url")
        if not isinstance(artwork_url, str):
            return None
        artwork_url = artwork_url.strip()
        if not artwork_url:
            return None
        upgraded = artwork_url.replace("-large", "-t500x500")
        return upgraded if _is_http_url(upgraded) else None

    def source_modifier(self, candidate):
        return 0.95


class BandcampAdapter(_YtDlpSearchMixin):
    source = "bandcamp"
    search_prefix = "bandcampsearch"

    def _candidate_thumbnail_url(self, entry):
        for key in ("thumbnail_url", "image"):
            value = entry.get(key)
            if isinstance(value, str):
                value = value.strip()
                if value and _is_http_url(value):
                    return value
        return None

    def source_modifier(self, candidate):
        return 1.05


def default_adapters():
    adapters = [BandcampAdapter(), YouTubeMusicAdapter(), YouTubeAdapter(), SoundCloudAdapter()]
    return {adapter.source: adapter for adapter in adapters}
