import logging
import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime

import musicbrainzngs
import requests


logger = logging.getLogger(__name__)
MUSICBRAINZ_USER_AGENT = os.getenv(
    "MUSICBRAINZ_USER_AGENT",
    "Retreivr/1.0 (+https://github.com/retreivr/retreivr)",
)

_DEFAULT_MAX_CACHE_ENTRIES = 512
_DEFAULT_CACHE_TTL_SECONDS = 6 * 60 * 60
_DEFAULT_COVER_CACHE_TTL_SECONDS = 24 * 60 * 60
_DEFAULT_MIN_INTERVAL_SECONDS = 1.0
_SEARCH_TTL_SECONDS = 24 * 60 * 60
_RELEASE_GROUP_TTL_SECONDS = 24 * 60 * 60
_RELEASE_TRACKS_TTL_SECONDS = 7 * 24 * 60 * 60
_NOISE_WORDS = {
    "album",
    "full",
    "official",
    "audio",
    "music",
    "track",
    "single",
    "version",
    "deluxe",
    "remastered",
    "bonus",
}


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
                "1.0",
                "https://github.com/retreivr/retreivr",
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

    def _safe_int(self, value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _tokenize(self, text):
        return [tok for tok in re.split(r"[^a-z0-9]+", (text or "").lower()) if tok]

    def _remove_noise_tokens(self, tokens):
        return [tok for tok in tokens if tok not in _NOISE_WORDS]

    def _split_artist_album(self, query):
        text = str(query or "").strip()
        if not text:
            return "", ""
        lowered = text.lower()
        for sep in (" - ", " – ", " — ", " by ", " : "):
            idx = lowered.find(sep)
            if idx > 0:
                left = text[:idx].strip()
                right = text[idx + len(sep) :].strip()
                if left and right:
                    return left, right
        raw_tokens = [tok for tok in re.split(r"\s+", text) if tok]
        if len(raw_tokens) < 3:
            return text, text
        split_at = max(1, len(raw_tokens) // 2)
        artist = " ".join(raw_tokens[:split_at]).strip()
        album = " ".join(raw_tokens[split_at:]).strip()
        return artist or text, album or text

    def _lucene_escape(self, text):
        return str(text or "").replace("\\", "\\\\").replace('"', '\\"')

    def _artist_credit_text(self, artist_credit):
        if not isinstance(artist_credit, list):
            return ""
        parts = []
        for part in artist_credit:
            if isinstance(part, str):
                parts.append(part)
                continue
            if isinstance(part, dict):
                name = part.get("name")
                if isinstance(name, str) and name.strip():
                    parts.append(name.strip())
                join = part.get("joinphrase")
                if isinstance(join, str) and join:
                    parts.append(join)
        return "".join(parts).strip()

    def _token_overlap(self, query_tokens, text):
        if not query_tokens or not text:
            return 0.0
        a = set(query_tokens)
        b = set(self._tokenize(text))
        if not a:
            return 0.0
        return len(a & b) / len(a)

    def _parse_date(self, value):
        text = str(value or "").strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

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

    def search_release_groups(self, query, *, limit=10):
        cleaned_query = str(query or "").strip()
        if not cleaned_query:
            return []
        query_tokens = self._tokenize(cleaned_query)
        clean_tokens = self._remove_noise_tokens(query_tokens) or query_tokens
        normalized_query = " ".join(clean_tokens).strip() or cleaned_query
        artist_fragment, album_fragment = self._split_artist_album(normalized_query)
        lucene_parts = ['primarytype:"album"']
        if artist_fragment:
            lucene_parts.append(f'artist:"{self._lucene_escape(artist_fragment)}"')
        if album_fragment:
            lucene_parts.append(f'releasegroup:"{self._lucene_escape(album_fragment)}"')
        else:
            lucene_parts.append(f'"{self._lucene_escape(normalized_query)}"')

        cache_key = f"album_search:{cleaned_query}:{int(limit or 10)}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", cache_key)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", cache_key)

        payload = self._call_with_retry(
            lambda: musicbrainzngs.search_release_groups(
                query=" AND ".join(lucene_parts),
                limit=max(10, min(int(limit or 10), 100)),
            )
        )
        groups = payload.get("release-group-list", []) if isinstance(payload, dict) else []
        allow_live = "live" in clean_tokens
        allow_compilation = "compilation" in clean_tokens
        candidates = []
        for group in groups:
            if not isinstance(group, dict):
                continue
            secondary_types_raw = group.get("secondary-type-list") or []
            secondary_types = [str(value) for value in secondary_types_raw if isinstance(value, str)]
            secondary_lower = {value.lower() for value in secondary_types}
            artist_credit = self._artist_credit_text(group.get("artist-credit"))
            base_score = self._safe_int(group.get("ext:score"), default=0)
            overlap = self._token_overlap(clean_tokens, artist_credit)
            adjusted = base_score + int(overlap * 30)
            if overlap >= 0.5:
                adjusted += 10
            if "live" in secondary_lower and not allow_live:
                adjusted -= 25
            if "compilation" in secondary_lower and not allow_compilation:
                adjusted -= 25
            if "soundtrack" in secondary_lower:
                adjusted -= 20
            if "remix" in secondary_lower:
                adjusted -= 20
            primary_type = str(group.get("primary-type") or "")
            if primary_type and primary_type.lower() != "album":
                adjusted -= 15
            adjusted = max(0, min(100, adjusted))
            candidates.append(
                {
                    "release_group_id": group.get("id"),
                    "title": group.get("title"),
                    "artist_credit": artist_credit,
                    "first_release_date": group.get("first-release-date"),
                    "primary_type": group.get("primary-type"),
                    "secondary_types": secondary_types,
                    "score": int(adjusted),
                    "track_count": None,
                }
            )
        candidates.sort(key=lambda c: c.get("score", 0), reverse=True)
        candidates = candidates[: max(1, min(int(limit or 10), 50))]
        top_title = candidates[0]["title"] if candidates else "-"
        top_score = candidates[0]["score"] if candidates else 0
        logger.info("[MUSIC] candidates_count=%s top=%s (%s) query=%s", len(candidates), top_title, top_score, query)
        self._cache.set(cache_key, candidates, ttl_seconds=_SEARCH_TTL_SECONDS)
        return candidates

    def pick_best_release_with_reason(self, release_group_id, *, prefer_country=None):
        rgid = str(release_group_id or "").strip()
        if not rgid:
            return {"release_id": None, "reason": "missing_release_group_id"}
        cache_key = f"release_group:{rgid}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", cache_key)
            releases = cached
        else:
            self._inc_metric("cache_misses")
            self._debug_log("[MUSICBRAINZ] cache miss key=%s", cache_key)
            payload = self._call_with_retry(
                lambda: musicbrainzngs.get_release_group_by_id(
                    rgid,
                    includes=["releases", "artist-credits"],
                )
            )
            release_group_payload = payload.get("release-group", {}) if isinstance(payload, dict) else {}
            releases = []
            if isinstance(release_group_payload, dict):
                releases = release_group_payload.get("release-list", []) or []
            if not releases and isinstance(payload, dict):
                releases = payload.get("release-list", []) or []
            self._cache.set(cache_key, releases, ttl_seconds=_RELEASE_GROUP_TTL_SECONDS)
        if not releases:
            return {"release_id": None, "reason": "no_releases"}

        preferred_country = (prefer_country or "").strip().upper() or None
        parsed_releases = [r for r in releases if isinstance(r, dict)]
        if not parsed_releases:
            return {"release_id": None, "reason": "no_ranked_release"}

        official_releases = [
            r for r in parsed_releases
            if str(r.get("status") or "").strip().lower() == "official"
        ]
        pool = official_releases or parsed_releases

        matched_country = []
        if preferred_country:
            matched_country = [
                r for r in pool
                if str(r.get("country") or "").strip().upper() == preferred_country
            ]
            if matched_country:
                pool = matched_country

        def _release_sort_key(release):
            release_date = self._parse_date(release.get("date"))
            release_id = str(release.get("id") or "")
            return (release_date is None, release_date, release_id)

        pool = sorted(pool, key=_release_sort_key)
        best_release = pool[0] if pool else None
        if not best_release:
            return {"release_id": None, "reason": "no_ranked_release"}

        reasons = []
        if best_release in official_releases:
            reasons.append("official")
        if preferred_country and best_release in matched_country:
            reasons.append(f"country:{preferred_country}")
        reasons.append("earliest")
        reason = ",".join(reasons)

        return {
            "release_id": best_release.get("id"),
            "reason": reason,
        }

    def pick_best_release(self, release_group_id):
        return self.pick_best_release_with_reason(release_group_id).get("release_id")

    def fetch_release_tracks(self, release_id):
        rid = str(release_id or "").strip()
        if not rid:
            return []
        cache_key = f"release_tracks:{rid}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", cache_key)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", cache_key)
        payload = self._call_with_retry(
            lambda: musicbrainzngs.get_release_by_id(
                rid,
                includes=["recordings", "artist-credits"],
            )
        )
        def _canonical_artist_from_credit(artist_credit_list):
            if not isinstance(artist_credit_list, list):
                return ""
            parts = []
            for ac in artist_credit_list:
                if not isinstance(ac, dict):
                    continue
                name = ac.get("artist", {}).get("name") or ac.get("name") or ""
                if name:
                    parts.append(str(name))
                joinphrase = ac.get("joinphrase")
                if joinphrase:
                    parts.append(str(joinphrase))
            return "".join(parts).strip()

        release_payload = payload.get("release", {}) if isinstance(payload, dict) else {}
        media = release_payload.get("medium-list", []) if isinstance(release_payload, dict) else []
        release_artist = _canonical_artist_from_credit(release_payload.get("artist-credit"))
        album_title = release_payload.get("title")
        release_date = release_payload.get("date")
        tracks = []
        for disc in media:
            if not isinstance(disc, dict):
                continue
            disc_number = self._safe_int(disc.get("position"), default=0) or None
            for track in disc.get("track-list", []) or []:
                if not isinstance(track, dict):
                    continue
                recording = track.get("recording") or {}
                track_artist = (
                    _canonical_artist_from_credit(recording.get("artist-credit"))
                    or release_artist
                )
                tracks.append(
                    {
                        "title": recording.get("title") or track.get("title"),
                        "recording_mbid": recording.get("id"),
                        "track_number": self._safe_int(track.get("position"), default=0) or None,
                        "disc_number": disc_number,
                        "artist": track_artist,
                        "album": album_title,
                        "release_date": release_date,
                        "duration_ms": self._safe_int(recording.get("length"), default=0) or None,
                        "artwork_url": None,
                    }
                )
        self._cache.set(cache_key, tracks, ttl_seconds=_RELEASE_TRACKS_TTL_SECONDS)
        return tracks

    def fetch_release_group_cover_art_url(self, release_group_id, *, timeout=8):
        rgid = str(release_group_id or "").strip()
        if not rgid:
            return None
        key = f"cover_art_release_group:{rgid}"
        cached = self._cover_cache.get(key)
        if cached is not None:
            self._inc_metric("cache_hits")
            self._debug_log("[MUSICBRAINZ] cache hit key=%s", key)
            return cached
        self._inc_metric("cache_misses")
        self._debug_log("[MUSICBRAINZ] cache miss key=%s", key)
        self._inc_metric("cover_art_requests")

        def _request():
            self._debug_log("[MUSICBRAINZ] cover art fetch release_group=%s", rgid)
            return requests.get(
                f"https://coverartarchive.org/release-group/{rgid}",
                timeout=timeout,
                headers={"User-Agent": MUSICBRAINZ_USER_AGENT},
            )

        resp = self._call_with_retry(_request, attempts=3, base_delay=0.4)
        if not resp or resp.status_code != 200:
            self._inc_metric("cover_art_failures")
            return None
        payload = resp.json() if resp.content else {}
        images = payload.get("images", []) if isinstance(payload, dict) else []
        cover_url = None
        if images:
            first = images[0] if isinstance(images[0], dict) else {}
            thumbs = first.get("thumbnails", {}) if isinstance(first.get("thumbnails"), dict) else {}
            cover_url = thumbs.get("small") or thumbs.get("250") or first.get("image")
        self._cover_cache.set(key, cover_url, ttl_seconds=_DEFAULT_COVER_CACHE_TTL_SECONDS)
        return cover_url


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
