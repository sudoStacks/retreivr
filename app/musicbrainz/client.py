import logging
import os
import threading
import time
from typing import Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.musicbrainz.cache import MusicBrainzCache

logger = logging.getLogger(__name__)

MUSICBRAINZ_BASE_URL = os.getenv("MUSICBRAINZ_BASE_URL", "https://musicbrainz.org")
MUSICBRAINZ_USER_AGENT = os.getenv(
    "MUSICBRAINZ_USER_AGENT",
    "Retreivr/1.0 (+https://github.com/retreivr/retreivr)",
)
MUSICBRAINZ_TIMEOUT_SECONDS = float(os.getenv("MUSICBRAINZ_TIMEOUT_SECONDS", "10"))
MUSICBRAINZ_MIN_INTERVAL_SECONDS = float(os.getenv("MUSICBRAINZ_MIN_INTERVAL_SECONDS", "1.0"))

SEARCH_TTL_SECONDS = 24 * 60 * 60
RELEASE_GROUP_TTL_SECONDS = 24 * 60 * 60
RELEASE_TRACKS_TTL_SECONDS = 7 * 24 * 60 * 60


class MusicBrainzClient:
    def __init__(self) -> None:
        self.base_url = MUSICBRAINZ_BASE_URL.rstrip("/") + "/"
        self.timeout_seconds = MUSICBRAINZ_TIMEOUT_SECONDS
        self.min_interval_seconds = max(0.0, MUSICBRAINZ_MIN_INTERVAL_SECONDS)
        self._cache = MusicBrainzCache()
        self._rate_lock = threading.Lock()
        self._last_request_ts = 0.0
        self._session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def _sleep_for_rate_limit(self) -> None:
        with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_ts
            wait_for = self.min_interval_seconds - elapsed
            if wait_for > 0:
                time.sleep(wait_for)
            self._last_request_ts = time.monotonic()

    def get_json(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        cache_key: str | None = None,
        ttl_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        if cache_key:
            cached = self._cache.get(cache_key)
            if isinstance(cached, dict):
                logger.info(f"[MUSICBRAINZ] request={endpoint} status=200 cache=hit")
                return cached

        self._sleep_for_rate_limit()
        url = urljoin(self.base_url, endpoint.lstrip("/"))
        try:
            resp = self._session.get(
                url,
                params=params or {},
                headers={"User-Agent": MUSICBRAINZ_USER_AGENT},
                timeout=self.timeout_seconds,
            )
            status = int(resp.status_code)
            logger.info(f"[MUSICBRAINZ] request={endpoint} status={status} cache=miss")
            if status != 200:
                return None
            payload = resp.json() if resp.content else {}
            if not isinstance(payload, dict):
                return None
            if cache_key and ttl_seconds:
                self._cache.set(cache_key, payload, ttl_seconds)
            return payload
        except Exception:
            logger.info(f"[MUSICBRAINZ] request={endpoint} status=error cache=miss")
            return None


_CLIENT: MusicBrainzClient | None = None
_CLIENT_LOCK = threading.Lock()


def get_musicbrainz_client() -> MusicBrainzClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = MusicBrainzClient()
    return _CLIENT
