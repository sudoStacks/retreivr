import logging
import hashlib
import html
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urljoin, urlparse
from typing import Protocol, runtime_checkable

import requests
from yt_dlp import YoutubeDL

from engine.json_utils import safe_json_dumps

_DISCOVERY_YTDLP_INSTANCES = {}
_DISCOVERY_YTDLP_INSTANCE_LOCKS = {}
_DISCOVERY_YTDLP_REGISTRY_LOCK = threading.Lock()

_YOUTUBE_INNERTUBE_SEARCH_URL = "https://www.youtube.com/youtubei/v1/search"
_YOUTUBE_INNERTUBE_CLIENT_VERSION = "2.20240304.00.00"
_DUCKDUCKGO_HTML_SEARCH_URL = "https://duckduckgo.com/html/"
_DEFAULT_SITE_SEARCH_TIMEOUT_SEC = 4.0
_DEFAULT_SITE_SEARCH_TIMEOUT_LIGHTWEIGHT_SEC = 2.0
_SEARCH_LINK_RE = re.compile(
    r"<a[^>]+class=(?:\"[^\"]*result__a[^\"]*\"|'[^']*result__a[^']*')[^>]+href=(?:\"([^\"]+)\"|'([^']+)')[^>]*>(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
_ANY_LINK_RE = re.compile(
    r"<a[^>]+href=(?:\"([^\"]+)\"|'([^']+)')[^>]*>(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")

def _is_http_url(value):
    if not value or not isinstance(value, str):
        return False
    try:
        return urlparse(value).scheme in ("http", "https")
    except Exception:
        return False


def _sanitize_source_name(value):
    source = str(value or "").strip().lower()
    if not source:
        return ""
    source = re.sub(r"[^a-z0-9_]+", "_", source)
    source = source.strip("_")
    return source


def _normalize_search_phrase(*parts):
    ordered = []
    seen = set()
    for part in parts:
        text = str(part or "").strip()
        if not text:
            continue
        for token in text.split():
            key = token.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            ordered.append(token.strip())
    return " ".join(ordered).strip()


def _normalize_domain_list(domains):
    if isinstance(domains, str):
        domains = [domains]
    if not isinstance(domains, (list, tuple, set)):
        return []
    out = []
    for value in domains:
        domain = str(value or "").strip().lower()
        if not domain:
            continue
        domain = domain.replace("https://", "").replace("http://", "")
        domain = domain.split("/")[0].strip(".")
        if domain:
            out.append(domain)
    deduped = []
    seen = set()
    for domain in out:
        if domain in seen:
            continue
        seen.add(domain)
        deduped.append(domain)
    return deduped


def _host_matches_domains(url, domains):
    if not domains:
        return True
    try:
        host = (urlparse(url).hostname or "").strip().lower()
    except Exception:
        return False
    if not host:
        return False
    for domain in domains:
        normalized = str(domain or "").strip().lower()
        if not normalized:
            continue
        if host == normalized or host.endswith(f".{normalized}"):
            return True
    return False


def _strip_html_tags(value):
    text = _HTML_TAG_RE.sub("", str(value or ""))
    return html.unescape(text).strip()


def _coerce_text(value):
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, list):
        for item in value:
            text = _coerce_text(item)
            if text:
                return text
    return None


def _clean_result_title(value, *, fallback=None):
    text = _coerce_text(value) or _coerce_text(fallback) or ""
    text = re.sub(r"\s+", " ", text).strip()
    return text or "Untitled"


def _normalize_mediatype_values(value):
    values = []
    if isinstance(value, str):
        text = value.strip().lower()
        if text:
            values.append(text)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            text = str(item or "").strip().lower()
            if text:
                values.append(text)
    elif value is not None:
        text = str(value).strip().lower()
        if text:
            values.append(text)
    return values


def _rumble_oembed_enrich(url, *, timeout_sec):
    try:
        response = requests.get(
            "https://rumble.com/api/Media/oembed.json",
            params={"url": url},
            timeout=max(0.5, float(timeout_sec or 1.2)),
            headers={"User-Agent": "Mozilla/5.0"},
        )
    except Exception:
        return {}
    if not response.ok:
        return {}
    try:
        payload = response.json()
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    html_snippet = _coerce_text(payload.get("html")) or ""
    embed_url = None
    if html_snippet:
        match = re.search(r'src=["\']([^"\']+)["\']', html_snippet, re.IGNORECASE)
        if match:
            candidate_url = str(match.group(1) or "").strip()
            if candidate_url.startswith("//"):
                candidate_url = f"https:{candidate_url}"
            if _is_http_url(candidate_url):
                embed_url = candidate_url
    return {
        "title": _coerce_text(payload.get("title")),
        "thumbnail_url": _coerce_text(payload.get("thumbnail_url")),
        "uploader": _coerce_text(payload.get("author_name")),
        "upload_date": _coerce_text(payload.get("upload_date") or payload.get("pubdate")),
        "embed_url": embed_url,
    }


def _extract_anchor_links(html_body, *, base_url):
    rows = []
    for match in _ANY_LINK_RE.finditer(str(html_body or "")):
        href = (match.group(1) or match.group(2) or "").strip()
        if not href:
            continue
        if href.startswith("//"):
            href = f"https:{href}"
        elif href.startswith("/"):
            href = urljoin(base_url, href)
        title = _strip_html_tags(match.group(3))
        rows.append({"url": href, "title": title})
    return rows


def _unwrap_duckduckgo_result_url(url):
    raw = str(url or "").strip()
    if not raw:
        return None
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith("/"):
        raw = f"https://duckduckgo.com{raw}"
    if not _is_http_url(raw):
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    host = (parsed.hostname or "").lower()
    if host.endswith("duckduckgo.com"):
        query = parse_qs(parsed.query or "")
        uddg_values = query.get("uddg") or []
        if uddg_values:
            resolved = unquote(str(uddg_values[0] or "").strip())
            return resolved if _is_http_url(resolved) else None
    return raw


def _duckduckgo_site_search(query, *, domains, limit, timeout_sec):
    q = str(query or "").strip()
    if not q:
        return []
    started = time.perf_counter()
    domain_terms = " OR ".join(f"site:{domain}" for domain in domains if domain)
    full_query = f"({domain_terms}) {q}".strip() if domain_terms else q
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(
            _DUCKDUCKGO_HTML_SEARCH_URL,
            params={"q": full_query},
            headers=headers,
            timeout=max(0.5, float(timeout_sec or _DEFAULT_SITE_SEARCH_TIMEOUT_SEC)),
        )
    except Exception:
        logging.exception("duckduckgo_site_search_failed query=%s", full_query)
        return []
    if not response.ok:
        return []
    html_body = response.text or ""
    rows = []
    seen_urls = set()
    max_results = max(1, int(limit or 5))
    def _consume_matches(matches):
        for match in matches:
            href_value = match.group(1) or match.group(2)
            href = _unwrap_duckduckgo_result_url(href_value)
            if not href or not _is_http_url(href):
                continue
            if not _host_matches_domains(href, domains):
                continue
            key = href.lower()
            if key in seen_urls:
                continue
            seen_urls.add(key)
            title = _strip_html_tags(match.group(3))
            if not title:
                title = href
            rows.append({"url": href, "title": title})
            if len(rows) >= max_results:
                break

    _consume_matches(_SEARCH_LINK_RE.finditer(html_body))
    if not rows:
        _consume_matches(_ANY_LINK_RE.finditer(html_body))
    logging.info(
        safe_json_dumps(
            {
                "event": "site_search_complete",
                "query": q,
                "domains": list(domains or []),
                "duration_ms": int((time.perf_counter() - started) * 1000),
                "result_count": len(rows),
            }
        )
    )
    return rows


@dataclass(frozen=True)
class CustomAdapterSpec:
    source: str
    domains: tuple[str, ...]
    source_modifier: float = 0.8
    query_suffix: str = ""


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
            posted_label = _extract_text_from_runs(renderer.get("publishedTimeText"))
            results.append(
                {
                    "video_id": video_id,
                    "title": title,
                    "channel": channel,
                    "url": f"https://youtube.com/watch?v={video_id}",
                    "thumbnail_url": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                    "posted_label": posted_label,
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


class SiteSearchAdapter(SearchAdapter):
    def __init__(
        self,
        *,
        source,
        domains,
        source_modifier_value=0.8,
        query_suffix="",
    ):
        self.source = _sanitize_source_name(source)
        self.domains = tuple(_normalize_domain_list(domains))
        try:
            self._source_modifier_value = float(source_modifier_value)
        except Exception:
            self._source_modifier_value = 0.8
        self._source_modifier_value = max(0.1, min(2.0, self._source_modifier_value))
        self.query_suffix = str(query_suffix or "").strip()

    def _search(self, query, limit, *, lightweight=False, timeout_budget_sec=None):
        if not query:
            return []
        timeout_sec = _DEFAULT_SITE_SEARCH_TIMEOUT_LIGHTWEIGHT_SEC if lightweight else _DEFAULT_SITE_SEARCH_TIMEOUT_SEC
        if timeout_budget_sec is not None:
            try:
                timeout_sec = min(timeout_sec, max(0.5, float(timeout_budget_sec)))
            except Exception:
                pass
        effective_query = str(query).strip()
        if self.query_suffix:
            effective_query = f"{effective_query} {self.query_suffix}".strip()
        rows = _duckduckgo_site_search(
            effective_query,
            domains=self.domains,
            limit=limit,
            timeout_sec=timeout_sec,
        )
        out = []
        for row in rows:
            url = row.get("url")
            title = row.get("title")
            if not _is_http_url(url) or not title:
                continue
            out.append(
                {
                    "source": self.source,
                    "video_id": None,
                    "url": url,
                    "title": title,
                    "uploader": None,
                    "channel": None,
                    "thumbnail_url": None,
                    "artist_detected": None,
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
            )
        return out

    def search_track(self, artist, track, album=None, limit=5, *, lightweight=False, timeout_budget_sec=None):
        query = _normalize_search_phrase(artist, track)
        if album:
            query = _normalize_search_phrase(query, album)
        return self._search(query, limit, lightweight=lightweight, timeout_budget_sec=timeout_budget_sec)

    def search_album(self, artist, album, limit=5, *, lightweight=False, timeout_budget_sec=None):
        query = _normalize_search_phrase(artist, album)
        return self._search(query, limit, lightweight=lightweight, timeout_budget_sec=timeout_budget_sec)

    def source_modifier(self, candidate):
        return self._source_modifier_value


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
                    "upload_date": entry.get("upload_date"),
                    "duration_sec": None,
                    "artwork_url": None,
                    "raw_meta_json": safe_json_dumps(
                        {
                            "upload_date": entry.get("upload_date"),
                            "timestamp": entry.get("timestamp"),
                            "release_timestamp": entry.get("release_timestamp"),
                            "release_date": entry.get("release_date"),
                        }
                    ),
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


class RumbleAdapter(SiteSearchAdapter):
    def __init__(self):
        super().__init__(
            source="rumble",
            domains=("rumble.com",),
            source_modifier_value=0.84,
            query_suffix="video",
        )

    def _search(self, query, limit, *, lightweight=False, timeout_budget_sec=None):
        q = str(query or "").strip()
        if not q:
            return []
        timeout_sec = 2.0 if lightweight else 4.0
        if timeout_budget_sec is not None:
            try:
                timeout_sec = min(timeout_sec, max(0.5, float(timeout_budget_sec)))
            except Exception:
                pass
        started = time.perf_counter()
        try:
            response = requests.get(
                "https://rumble.com/search/video",
                params={"q": q},
                timeout=timeout_sec,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            html_body = response.text if response.ok else ""
            links = _extract_anchor_links(html_body, base_url="https://rumble.com")
            rows = []
            seen = set()
            for link in links:
                url = str(link.get("url") or "").strip()
                if not _is_http_url(url) or not _host_matches_domains(url, self.domains):
                    continue
                path = (urlparse(url).path or "").strip().lower()
                if not path.startswith("/v"):
                    continue
                key = url.lower()
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"url": url, "title": _clean_result_title(link.get("title"), fallback="Rumble Video")})
                if len(rows) >= max(1, int(limit or 5)):
                    break
            # Enrich from Rumble oEmbed for stable title + thumbnail.
            if rows:
                oembed_timeout = 1.0 if lightweight else 1.5
                max_workers = min(4, len(rows))
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = {
                        pool.submit(_rumble_oembed_enrich, row["url"], timeout_sec=oembed_timeout): row
                        for row in rows
                        if _is_http_url(row.get("url"))
                    }
                    for fut in as_completed(futures):
                        row = futures[fut]
                        try:
                            enriched = fut.result() or {}
                        except Exception:
                            enriched = {}
                        if _coerce_text(enriched.get("title")):
                            row["title"] = _clean_result_title(enriched.get("title"), fallback=row.get("title"))
                        if _is_http_url(enriched.get("thumbnail_url")):
                            row["thumbnail_url"] = enriched.get("thumbnail_url")
                        if _coerce_text(enriched.get("uploader")):
                            row["uploader"] = enriched.get("uploader")
                        if _coerce_text(enriched.get("upload_date")):
                            row["upload_date"] = enriched.get("upload_date")
                        if _is_http_url(enriched.get("embed_url")):
                            row["embed_url"] = enriched.get("embed_url")
        except Exception:
            rows = []
        logging.info(
            safe_json_dumps(
                {
                    "event": "site_search_complete",
                    "query": q,
                    "domains": list(self.domains),
                    "duration_ms": int((time.perf_counter() - started) * 1000),
                    "result_count": len(rows),
                }
            )
        )
        return [
            {
                "source": self.source,
                "video_id": None,
                "url": row.get("url"),
                "title": _clean_result_title(row.get("title"), fallback="Rumble Video"),
                "uploader": row.get("uploader"),
                "channel": row.get("uploader"),
                "thumbnail_url": row.get("thumbnail_url"),
                "artist_detected": row.get("uploader"),
                "album_detected": None,
                "track_detected": _clean_result_title(row.get("title"), fallback="Rumble Video"),
                "upload_date": row.get("upload_date"),
                "duration_sec": None,
                "artwork_url": row.get("thumbnail_url"),
                "raw_meta_json": safe_json_dumps(
                    {
                        "upload_date": row.get("upload_date"),
                        "embed_url": row.get("embed_url"),
                    }
                ),
                "official": False,
                "isrc": None,
                "track_count": None,
                "view_count": None,
            }
            for row in rows
            if _is_http_url(row.get("url"))
        ]


class ArchiveOrgAdapter(SiteSearchAdapter):
    def __init__(self):
        super().__init__(
            source="archive_org",
            domains=("archive.org",),
            source_modifier_value=0.82,
            query_suffix="video",
        )

    def _search(self, query, limit, *, lightweight=False, timeout_budget_sec=None):
        q = str(query or "").strip()
        if not q:
            return []
        timeout_sec = 2.0 if lightweight else 4.0
        if timeout_budget_sec is not None:
            try:
                timeout_sec = min(timeout_sec, max(0.5, float(timeout_budget_sec)))
            except Exception:
                pass
        started = time.perf_counter()
        rows = []
        try:
            response = requests.get(
                "https://archive.org/advancedsearch.php",
                params={
                    # Prefer video-like records while keeping recall for broad queries.
                    "q": f"({q}) AND mediatype:(movies OR movingimage OR video)",
                    "fl[]": ["identifier", "title", "creator", "mediatype", "publicdate", "date", "addeddate"],
                    "rows": max(1, int(limit or 5)),
                    "page": 1,
                    "output": "json",
                },
                timeout=timeout_sec,
            )
            payload = response.json() if response.ok else {}
            docs = ((payload or {}).get("response") or {}).get("docs") or []
            for doc in docs:
                if not isinstance(doc, dict):
                    continue
                identifier = str(doc.get("identifier") or "").strip()
                if not identifier:
                    continue
                media_types = _normalize_mediatype_values(doc.get("mediatype"))
                if media_types and not any(mt in {"movies", "movingimage", "video"} for mt in media_types):
                    continue
                title = _clean_result_title(doc.get("title"), fallback=identifier)
                thumbnail_url = f"https://archive.org/services/img/{identifier}"
                rows.append(
                    {
                        "url": f"https://archive.org/details/{identifier}",
                        "title": title,
                        "uploader": _coerce_text(doc.get("creator")),
                        "thumbnail_url": thumbnail_url,
                        "publish_date": _coerce_text(doc.get("publicdate") or doc.get("date") or doc.get("addeddate")),
                    }
                )
                if len(rows) >= max(1, int(limit or 5)):
                    break
        except Exception:
            rows = []
        logging.info(
            safe_json_dumps(
                {
                    "event": "site_search_complete",
                    "query": q,
                    "domains": list(self.domains),
                    "duration_ms": int((time.perf_counter() - started) * 1000),
                    "result_count": len(rows),
                }
            )
        )
        return [
            {
                "source": self.source,
                "video_id": None,
                "url": row.get("url"),
                "title": row.get("title"),
                "uploader": row.get("uploader"),
                "channel": row.get("uploader"),
                "thumbnail_url": row.get("thumbnail_url"),
                "artist_detected": row.get("uploader"),
                "album_detected": None,
                "track_detected": row.get("title"),
                "publish_date": row.get("publish_date"),
                "duration_sec": None,
                "artwork_url": row.get("thumbnail_url"),
                "raw_meta_json": safe_json_dumps(
                    {
                        "publish_date": row.get("publish_date"),
                    }
                ),
                "official": False,
                "isrc": None,
                "track_count": None,
                "view_count": None,
            }
            for row in rows
            if _is_http_url(row.get("url"))
        ]


BUILTIN_SITE_ADAPTER_SPECS = ()


def _load_adapter_file(path):
    extension = os.path.splitext(path)[1].strip().lower()
    with open(path, "r", encoding="utf-8") as handle:
        if extension == ".json":
            return json.load(handle)
        if extension in {".yaml", ".yml"}:
            try:
                import yaml  # type: ignore
            except Exception:
                logging.warning("custom_adapter_yaml_unavailable path=%s", path)
                return None
            return yaml.safe_load(handle)
    return None


def _parse_custom_adapter_specs(raw_payload):
    if not isinstance(raw_payload, dict):
        return []
    raw_adapters = raw_payload.get("adapters")
    if not isinstance(raw_adapters, list):
        return []
    specs = []
    for raw in raw_adapters:
        if not isinstance(raw, dict):
            continue
        if raw.get("enabled") is False:
            continue
        adapter_type = str(raw.get("type") or "site_search").strip().lower()
        if adapter_type != "site_search":
            continue
        source = _sanitize_source_name(raw.get("source"))
        if not source:
            continue
        domains = tuple(_normalize_domain_list(raw.get("domains") or raw.get("domain")))
        if not domains:
            continue
        try:
            source_modifier = float(raw.get("source_modifier", 0.8))
        except Exception:
            source_modifier = 0.8
        query_suffix = str(raw.get("query_suffix") or "").strip()
        specs.append(
            CustomAdapterSpec(
                source=source,
                domains=domains,
                source_modifier=source_modifier,
                query_suffix=query_suffix,
            )
        )
    return specs


def _resolve_custom_adapter_paths(config):
    cfg = config if isinstance(config, dict) else {}
    configured = cfg.get("custom_search_adapters_file")
    paths = []
    if isinstance(configured, str) and configured.strip():
        paths.append(configured.strip())
    elif isinstance(configured, (list, tuple, set)):
        for value in configured:
            if isinstance(value, str) and value.strip():
                paths.append(value.strip())
    if not paths:
        paths = [
            "config/custom_search_adapters.yaml",
            "config/custom_search_adapters.yml",
            "config/custom_search_adapters.json",
        ]
    deduped = []
    seen = set()
    for path in paths:
        key = os.path.abspath(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _load_custom_adapters(config=None):
    adapters = []
    for path in _resolve_custom_adapter_paths(config):
        if not os.path.exists(path):
            continue
        try:
            payload = _load_adapter_file(path)
            specs = _parse_custom_adapter_specs(payload)
            for spec in specs:
                adapters.append(
                    SiteSearchAdapter(
                        source=spec.source,
                        domains=spec.domains,
                        source_modifier_value=spec.source_modifier,
                        query_suffix=spec.query_suffix,
                    )
                )
            logging.info(
                safe_json_dumps(
                    {
                        "event": "custom_search_adapters_loaded",
                        "path": path,
                        "count": len(specs),
                    }
                )
            )
        except Exception:
            logging.exception("custom_search_adapters_load_failed path=%s", path)
    return adapters


def default_adapters(config=None):
    adapters = [
        BandcampAdapter(),
        YouTubeMusicAdapter(),
        YouTubeAdapter(),
        SoundCloudAdapter(),
        RumbleAdapter(),
        ArchiveOrgAdapter(),
    ]
    adapters.extend(_load_custom_adapters(config=config))
    return {adapter.source: adapter for adapter in adapters if getattr(adapter, "source", "")}
