import logging
import os
import threading
import time
from collections import OrderedDict

import musicbrainzngs
import requests


logger = logging.getLogger(__name__)

_DEFAULT_MAX_CACHE_ENTRIES = 512
_DEFAULT_CACHE_TTL_SECONDS = 6 * 60 * 60
_DEFAULT_COVER_CACHE_TTL_SECONDS = 24 * 60 * 60
_DEFAULT_MIN_INTERVAL_SECONDS = 1.0


class _TTLCache:
    def __init__(self, *, max_entries=_DEFAULT_MAX_CACHE_ENTRIES, ttl_seconds=_DEFAULT_CACHE_TTL_SECONDS):
        self.max_entries = int(max_entries)
        self.ttl_seconds = int(ttl_seconds)
        self._lock = threading.Lock()
        self._entries = OrderedDict()

    def get(self, key):
        now = time.time()
        with self._lock:
            value = self._entries.get(key)
            if not value:
                return None
            expires_at, payload = value
            if expires_at < now:
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return payload

    def set(self, key, payload, *, ttl_seconds=None):
        ttl = self.ttl_seconds if ttl_seconds is None else max(1, int(ttl_seconds))
        expires_at = time.time() + ttl
        with self._lock:
            self._entries[key] = (expires_at, payload)
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)


class MusicBrainzService:
    def __init__(self, *, debug=None):
        self._init_lock = threading.Lock()
        self._initialized = False
        self._cache = _TTLCache()
        self._cover_cache = _TTLCache(ttl_seconds=_DEFAULT_COVER_CACHE_TTL_SECONDS)
        self._request_lock = threading.Lock()
        self._last_request_ts = 0.0
        if debug is None:
            env_debug = str(os.environ.get("RETREIVR_MUSICBRAINZ_DEBUG", "")).strip().lower()
            self._debug = env_debug in {"1", "true", "yes", "on"}
        else:
            self._debug = bool(debug)
        self._metrics_lock = threading.Lock()
        self._metrics = {
            "total_requests": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "retries": 0,
            "cover_art_requests": 0,
            "cover_art_failures": 0,
        }

    def _debug_log(self, message, *args):
        if self._debug:
            logger.debug(message, *args)

    def _inc_metric(self, key, amount=1):
        with self._metrics_lock:
            self._metrics[key] = int(self._metrics.get(key, 0)) + int(amount)

    def _respect_rate_limit(self):
        with self._request_lock:
            now = time.monotonic()
            wait_for = _DEFAULT_MIN_INTERVAL_SECONDS - (now - self._last_request_ts)
            if wait_for > 0:
                self._debug_log("[MUSICBRAINZ] rate-limit sleep %.3fs", wait_for)
                time.sleep(wait_for)
            self._last_request_ts = time.monotonic()

    def _ensure_initialized(self):
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            logging.getLogger("musicbrainzngs").setLevel(logging.WARNING)
            musicbrainzngs.set_useragent(
                "retreivr",
                "0.9.0",
                "https://github.com/Retreivr/retreivr",
            )
            if hasattr(musicbrainzngs, "set_rate_limit"):
                try:
                    musicbrainzngs.set_rate_limit(1.0, 1)
                except TypeError:
                    try:
                        musicbrainzngs.set_rate_limit(limit_or_interval=1.0, new_requests=1)
                    except Exception:
                        pass
                except Exception:
                    pass
            self._initialized = True

    def _call_with_retry(self, fn, *, attempts=3, base_delay=0.3):
        self._ensure_initialized()
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                self._respect_rate_limit()
                self._inc_metric("total_requests")
                return fn()
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                self._inc_metric("retries")
                delay = base_delay * (2 ** (attempt - 1))
                self._debug_log("[MUSICBRAINZ] retry attempt=%s delay=%.3fs error=%s", attempt, delay, exc)
                time.sleep(base_delay * (2 ** (attempt - 1)))
        if last_error:
            raise last_error
        return None

    def get_metrics(self):
        with self._metrics_lock:
            return dict(self._metrics)

    def cover_art_url(self, release_id):
        rid = str(release_id or "").strip()
        if not rid:
            return None
        return f"https://coverartarchive.org/release/{rid}/front"

    def search_recordings(self, artist, title, *, album=None, limit=5):
        key = f"search_recordings:{artist}|{title}|{album or ''}|{int(limit or 5)}"
        cached = self._cache.get(key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", key)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", key)

        query = {"artist": artist, "recording": title}
        if album:
            query["release"] = album

        payload = self._call_with_retry(
            lambda: musicbrainzngs.search_recordings(limit=int(limit or 5), **query)
        )
        self._cache.set(key, payload)
        return payload

    def search_releases(self, artist, album, *, limit=5):
        key = f"search_releases:{artist}|{album}|{int(limit or 5)}"
        cached = self._cache.get(key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", key)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", key)

        query = {"artist": artist, "release": album}
        payload = self._call_with_retry(
            lambda: musicbrainzngs.search_releases(limit=int(limit or 5), **query)
        )
        self._cache.set(key, payload)
        return payload

    def get_release(self, release_id, *, includes=None):
        rid = str(release_id or "").strip()
        if not rid:
            return None
        includes_tuple = tuple(includes or ())
        key = f"get_release:{rid}|{','.join(includes_tuple)}"
        cached = self._cache.get(key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", key)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", key)

        payload = self._call_with_retry(
            lambda: musicbrainzngs.get_release_by_id(
                rid,
                includes=list(includes_tuple) if includes_tuple else [],
            )
        )
        self._cache.set(key, payload)
        return payload

    def get_recording(self, recording_id, *, includes=None):
        rid = str(recording_id or "").strip()
        if not rid:
            return None
        includes_tuple = tuple(includes or ())
        key = f"get_recording:{rid}|{','.join(includes_tuple)}"
        cached = self._cache.get(key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", key)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", key)

        payload = self._call_with_retry(
            lambda: musicbrainzngs.get_recording_by_id(
                rid,
                includes=list(includes_tuple) if includes_tuple else [],
            )
        )
        self._cache.set(key, payload)
        return payload

    def fetch_cover_art(self, release_id, *, timeout=10):
        self._inc_metric("cover_art_requests")
        url = self.cover_art_url(release_id)
        if not url:
            self._inc_metric("cover_art_failures")
            return None

        cached = self._cover_cache.get(url)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", url)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", url)
        self._debug_log("[MUSICBRAINZ] cover art fetch url=%s", url)

        def _request():
            return requests.get(url, timeout=timeout)

        resp = self._call_with_retry(_request, attempts=3, base_delay=0.4)
        if not resp or resp.status_code != 200:
            self._inc_metric("cover_art_failures")
            return None
        payload = {
            "url": url,
            "data": resp.content,
            "mime": resp.headers.get("Content-Type", "image/jpeg"),
        }
        self._cover_cache.set(url, payload)
        return payload


_MUSICBRAINZ_SERVICE = None
_MUSICBRAINZ_SERVICE_LOCK = threading.Lock()


def get_musicbrainz_service():
    global _MUSICBRAINZ_SERVICE
    if _MUSICBRAINZ_SERVICE is not None:
        return _MUSICBRAINZ_SERVICE
    with _MUSICBRAINZ_SERVICE_LOCK:
        if _MUSICBRAINZ_SERVICE is None:
            _MUSICBRAINZ_SERVICE = MusicBrainzService()
    return _MUSICBRAINZ_SERVICE
