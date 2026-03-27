import os
from typing import Any
from urllib.parse import quote
import concurrent.futures

import requests


TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w342"
DEFAULT_TIMEOUT = 15
ALLOWED_MOVIE_CERTIFICATIONS = {"", "G", "PG", "PG-13", "R", "NR", "NOT RATED"}
ALLOWED_TV_CERTIFICATIONS = {"", "TV-Y", "TV-Y7", "TV-G", "TV-PG", "TV-14", "TV-MA"}


class ArrServiceError(RuntimeError):
    pass


def _normalize_base_url(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = f"http://{value}"
    return value.rstrip("/")


def _trimmed(value: Any) -> str:
    return str(value or "").strip()


def get_arr_config(config: dict | None) -> dict:
    cfg = config if isinstance(config, dict) else {}
    arr = cfg.get("arr")
    if not isinstance(arr, dict):
        arr = {}
    radarr = arr.get("radarr")
    if not isinstance(radarr, dict):
        radarr = {}
    sonarr = arr.get("sonarr")
    if not isinstance(sonarr, dict):
        sonarr = {}
    return {
        "tmdb_api_key": _trimmed(arr.get("tmdb_api_key")),
        "radarr": {
            "base_url": _normalize_base_url(radarr.get("base_url")),
            "api_key": _trimmed(radarr.get("api_key")),
        },
        "sonarr": {
            "base_url": _normalize_base_url(sonarr.get("base_url")),
            "api_key": _trimmed(sonarr.get("api_key")),
        },
    }


def _tmdb_request(config: dict | None, path: str, *, params: dict | None = None) -> dict:
    arr_cfg = get_arr_config(config)
    api_key = arr_cfg.get("tmdb_api_key") or os.getenv("TMDB_API_KEY", "").strip()
    if not api_key:
        raise ArrServiceError("TMDb API key is not configured")
    merged = {"api_key": api_key}
    if params:
        merged.update(params)
    response = requests.get(
        f"{TMDB_API_BASE}{path}",
        params=merged,
        timeout=DEFAULT_TIMEOUT,
    )
    if response.status_code >= 400:
        raise ArrServiceError(f"tmdb_error status={response.status_code}")
    return response.json()


def _pick_tmdb_video(payload: dict) -> dict | None:
    videos = payload.get("results") or []
    if not isinstance(videos, list):
        return None
    ranked = []
    for item in videos:
        if not isinstance(item, dict):
            continue
        site = _trimmed(item.get("site")).lower()
        key = _trimmed(item.get("key"))
        if site != "youtube" or not key:
            continue
        item_type = _trimmed(item.get("type")).lower()
        official = bool(item.get("official"))
        score = 0
        if item_type == "trailer":
            score += 20
        elif item_type == "teaser":
            score += 10
        if official:
            score += 5
        ranked.append((score, item))
    if not ranked:
        return None
    ranked.sort(key=lambda entry: entry[0], reverse=True)
    return ranked[0][1]


def _arr_request(
    service_cfg: dict,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json_body: Any = None,
) -> Any:
    base_url = _normalize_base_url(service_cfg.get("base_url"))
    api_key = _trimmed(service_cfg.get("api_key"))
    if not base_url or not api_key:
        raise ArrServiceError("ARR server is not configured")
    response = requests.request(
        method.upper(),
        f"{base_url}/api/v3/{path.lstrip('/')}",
        headers={"X-Api-Key": api_key},
        params=params,
        json=json_body,
        timeout=DEFAULT_TIMEOUT,
    )
    if response.status_code >= 400:
        body = response.text[:300]
        raise ArrServiceError(f"arr_error status={response.status_code} path={path} body={body}")
    if not response.content:
        return None
    return response.json()


def _poster_url(poster_path: str | None) -> str:
    path = _trimmed(poster_path)
    if not path:
        return ""
    return f"{TMDB_IMAGE_BASE}{path}"


def _normalize_movie_certification(value: Any) -> str:
    text = _trimmed(value).upper()
    aliases = {
        "UNRATED": "NOT RATED",
        "NR.": "NR",
    }
    return aliases.get(text, text)


def _normalize_tv_certification(value: Any) -> str:
    return _trimmed(value).upper()


def _movie_allowed(raw: dict) -> bool:
    if bool(raw.get("adult")):
        return False
    certification = _normalize_movie_certification(raw.get("us_certification"))
    return certification in ALLOWED_MOVIE_CERTIFICATIONS


def _tv_allowed(raw: dict) -> bool:
    if bool(raw.get("adult")):
        return False
    certification = _normalize_tv_certification(raw.get("us_certification"))
    return certification in ALLOWED_TV_CERTIFICATIONS


def _get_movie_us_certification(config: dict | None, tmdb_id: int) -> str:
    payload = _tmdb_request(config, f"/movie/{tmdb_id}/release_dates")
    for country in payload.get("results") or []:
        if _trimmed(country.get("iso_3166_1")).upper() != "US":
            continue
        releases = country.get("release_dates") or []
        for release in releases:
            certification = _normalize_movie_certification(release.get("certification"))
            if certification:
                return certification
    return ""


def _get_tv_us_certification(config: dict | None, tmdb_id: int) -> str:
    payload = _tmdb_request(config, f"/tv/{tmdb_id}/content_ratings")
    for country in payload.get("results") or []:
        if _trimmed(country.get("iso_3166_1")).upper() != "US":
            continue
        return _normalize_tv_certification(country.get("rating"))
    return ""


def _enrich_movie_certifications(config: dict | None, rows: list[dict]) -> list[dict]:
    if not rows:
        return rows
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(rows))) as executor:
        futures = {
            executor.submit(_get_movie_us_certification, config, int(row["tmdb_id"])): row
            for row in rows
            if row.get("tmdb_id")
        }
        for future, row in futures.items():
            try:
                row["us_certification"] = future.result()
            except Exception:
                row["us_certification"] = ""
    return rows


def _enrich_tv_certifications(config: dict | None, rows: list[dict]) -> list[dict]:
    if not rows:
        return rows
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(rows))) as executor:
        futures = {
            executor.submit(_get_tv_us_certification, config, int(row["tmdb_id"])): row
            for row in rows
            if row.get("tmdb_id")
        }
        for future, row in futures.items():
            try:
                row["us_certification"] = future.result()
            except Exception:
                row["us_certification"] = ""
    return rows


def _movie_year(item: dict) -> str:
    date_value = _trimmed(item.get("release_date"))
    return date_value[:4] if len(date_value) >= 4 else ""


def _tv_year(item: dict) -> str:
    date_value = _trimmed(item.get("first_air_date"))
    return date_value[:4] if len(date_value) >= 4 else ""


def search_tmdb_movies(config: dict | None, query: str, *, limit: int = 20) -> list[dict]:
    payload = _tmdb_request(config, "/search/movie", params={"query": query, "include_adult": "false"})
    results = []
    seed = (payload.get("results") or [])[: max(1, int(limit) * 2)]
    for raw in seed:
        tmdb_id = raw.get("id")
        if not tmdb_id:
            continue
        results.append(
            {
                "kind": "movie",
                "tmdb_id": int(tmdb_id),
                "adult": bool(raw.get("adult")),
                "title": _trimmed(raw.get("title")) or "Unknown title",
                "original_title": _trimmed(raw.get("original_title")),
                "year": _movie_year(raw),
                "overview": _trimmed(raw.get("overview")),
                "language": _trimmed(raw.get("original_language")),
                "popularity": raw.get("popularity"),
                "rating": raw.get("vote_average"),
                "us_certification": "",
                "tmdb_url": f"https://www.themoviedb.org/movie/{int(tmdb_id)}",
                "poster_url": _poster_url(raw.get("poster_path")),
                "backdrop_url": _poster_url(raw.get("backdrop_path")),
            }
        )
    _enrich_movie_certifications(config, results)
    filtered = [row for row in results if _movie_allowed(row)]
    return filtered[: max(1, int(limit))]


def search_tmdb_tv(config: dict | None, query: str, *, limit: int = 20) -> list[dict]:
    payload = _tmdb_request(config, "/search/tv", params={"query": query, "include_adult": "false"})
    results = []
    seed = (payload.get("results") or [])[: max(1, int(limit) * 2)]
    for raw in seed:
        tmdb_id = raw.get("id")
        if not tmdb_id:
            continue
        results.append(
            {
                "kind": "tv",
                "tmdb_id": int(tmdb_id),
                "adult": bool(raw.get("adult")),
                "title": _trimmed(raw.get("name")) or "Unknown title",
                "original_title": _trimmed(raw.get("original_name")),
                "year": _tv_year(raw),
                "overview": _trimmed(raw.get("overview")),
                "language": _trimmed(raw.get("original_language")),
                "popularity": raw.get("popularity"),
                "rating": raw.get("vote_average"),
                "us_certification": "",
                "tmdb_url": f"https://www.themoviedb.org/tv/{int(tmdb_id)}",
                "poster_url": _poster_url(raw.get("poster_path")),
                "backdrop_url": _poster_url(raw.get("backdrop_path")),
            }
        )
    _enrich_tv_certifications(config, results)
    filtered = [row for row in results if _tv_allowed(row)]
    return filtered[: max(1, int(limit))]


def test_radarr_connection(config: dict | None) -> dict:
    service_cfg = get_arr_config(config)["radarr"]
    if not service_cfg["base_url"] or not service_cfg["api_key"]:
        return {"configured": False, "reachable": False, "message": "Radarr is not configured"}
    try:
        payload = _arr_request(service_cfg, "GET", "system/status")
    except Exception as exc:
        return {"configured": True, "reachable": False, "message": str(exc)}
    version = _trimmed((payload or {}).get("version"))
    return {
        "configured": True,
        "reachable": True,
        "message": f"Connected to Radarr{f' {version}' if version else ''}".strip(),
        "version": version,
    }


def test_sonarr_connection(config: dict | None) -> dict:
    service_cfg = get_arr_config(config)["sonarr"]
    if not service_cfg["base_url"] or not service_cfg["api_key"]:
        return {"configured": False, "reachable": False, "message": "Sonarr is not configured"}
    try:
        payload = _arr_request(service_cfg, "GET", "system/status")
    except Exception as exc:
        return {"configured": True, "reachable": False, "message": str(exc)}
    version = _trimmed((payload or {}).get("version"))
    return {
        "configured": True,
        "reachable": True,
        "message": f"Connected to Sonarr{f' {version}' if version else ''}".strip(),
        "version": version,
    }


def _status_record(added: bool, downloaded: bool, arr_id: Any = None) -> dict:
    status = "not_added"
    if downloaded:
        status = "downloaded"
    elif added:
        status = "added"
    return {
        "status": status,
        "added": added,
        "downloaded": downloaded,
        "arr_id": arr_id,
    }


def _movie_status_from_entry(entry: dict) -> dict:
    return _status_record(
        True,
        bool(entry.get("hasFile")),
        entry.get("id"),
    )


def _series_is_downloaded(entry: dict) -> bool:
    if entry.get("statistics", {}).get("sizeOnDisk"):
        return True
    if entry.get("statistics", {}).get("episodeFileCount"):
        return True
    if entry.get("episodeFileCount"):
        return True
    return False


def _series_status_from_entry(entry: dict) -> dict:
    return _status_record(
        True,
        _series_is_downloaded(entry),
        entry.get("id"),
    )


def get_movie_status_map(config: dict | None, tmdb_ids: list[int | str]) -> dict[str, dict]:
    service_cfg = get_arr_config(config)["radarr"]
    if not service_cfg["base_url"] or not service_cfg["api_key"] or not tmdb_ids:
        return {}
    try:
        payload = _arr_request(service_cfg, "GET", "movie")
    except Exception:
        return {}
    wanted = {str(int(value)) for value in tmdb_ids if str(value).strip().isdigit()}
    statuses = {}
    for entry in payload or []:
        tmdb_id = entry.get("tmdbId")
        if tmdb_id is None:
          continue
        key = str(int(tmdb_id))
        if key not in wanted:
            continue
        statuses[key] = _movie_status_from_entry(entry)
    return statuses


def get_series_status_map(config: dict | None, tmdb_ids: list[int | str]) -> dict[str, dict]:
    service_cfg = get_arr_config(config)["sonarr"]
    if not service_cfg["base_url"] or not service_cfg["api_key"] or not tmdb_ids:
        return {}
    try:
        payload = _arr_request(service_cfg, "GET", "series")
    except Exception:
        return {}
    wanted = {str(int(value)) for value in tmdb_ids if str(value).strip().isdigit()}
    statuses = {}
    for entry in payload or []:
        tmdb_id = entry.get("tmdbId")
        if tmdb_id is None:
            continue
        key = str(int(tmdb_id))
        if key not in wanted:
            continue
        statuses[key] = _series_status_from_entry(entry)
    return statuses


def _first_root_folder(service_cfg: dict) -> str:
    folders = _arr_request(service_cfg, "GET", "rootfolder") or []
    for item in folders:
        path = _trimmed(item.get("path"))
        if path:
            return path
    raise ArrServiceError("ARR root folder is not configured")


def _first_quality_profile_id(service_cfg: dict) -> int:
    profiles = _arr_request(service_cfg, "GET", "qualityprofile") or []
    for item in profiles:
        profile_id = item.get("id")
        if isinstance(profile_id, int):
            return profile_id
    raise ArrServiceError("ARR quality profile is not configured")


def add_movie_to_radarr(config: dict | None, tmdb_id: int | str) -> dict:
    service_cfg = get_arr_config(config)["radarr"]
    if not service_cfg["base_url"] or not service_cfg["api_key"]:
        raise ArrServiceError("Radarr is not configured")
    status_map = get_movie_status_map(config, [tmdb_id])
    existing = status_map.get(str(int(tmdb_id)))
    if existing:
        return existing
    lookup = _arr_request(
        service_cfg,
        "GET",
        "movie/lookup",
        params={"term": f"tmdb:{int(tmdb_id)}"},
    ) or []
    candidate = next((item for item in lookup if int(item.get("tmdbId") or 0) == int(tmdb_id)), None)
    if not candidate:
        raise ArrServiceError("Movie lookup failed for TMDb id")
    payload = dict(candidate)
    payload.pop("id", None)
    payload["qualityProfileId"] = payload.get("qualityProfileId") or _first_quality_profile_id(service_cfg)
    payload["rootFolderPath"] = payload.get("rootFolderPath") or _first_root_folder(service_cfg)
    payload["monitored"] = True
    payload["addOptions"] = {"searchForMovie": True}
    created = _arr_request(service_cfg, "POST", "movie", json_body=payload) or {}
    return _movie_status_from_entry(created)


def add_series_to_sonarr(config: dict | None, tmdb_id: int | str) -> dict:
    service_cfg = get_arr_config(config)["sonarr"]
    if not service_cfg["base_url"] or not service_cfg["api_key"]:
        raise ArrServiceError("Sonarr is not configured")
    status_map = get_series_status_map(config, [tmdb_id])
    existing = status_map.get(str(int(tmdb_id)))
    if existing:
        return existing
    lookup = _arr_request(
        service_cfg,
        "GET",
        "series/lookup",
        params={"term": f"tmdb:{int(tmdb_id)}"},
    ) or []
    candidate = next((item for item in lookup if int(item.get("tmdbId") or 0) == int(tmdb_id)), None)
    if not candidate:
        raise ArrServiceError("Series lookup failed for TMDb id")
    payload = dict(candidate)
    payload.pop("id", None)
    payload["qualityProfileId"] = payload.get("qualityProfileId") or _first_quality_profile_id(service_cfg)
    payload["rootFolderPath"] = payload.get("rootFolderPath") or _first_root_folder(service_cfg)
    payload["monitored"] = True
    payload["seasonFolder"] = True
    payload["addOptions"] = {"searchForMissingEpisodes": True}
    created = _arr_request(service_cfg, "POST", "series", json_body=payload) or {}
    return _series_status_from_entry(created)


def build_movie_search_response(config: dict | None, query: str, *, limit: int = 20) -> dict:
    results = search_tmdb_movies(config, query, limit=limit)
    tmdb_ids = [item["tmdb_id"] for item in results]
    statuses = get_movie_status_map(config, tmdb_ids)
    radarr = test_radarr_connection(config)
    for item in results:
        item["arr_status"] = statuses.get(str(item["tmdb_id"])) or _status_record(False, False)
    return {"results": results, "connection": radarr}


def build_tv_search_response(config: dict | None, query: str, *, limit: int = 20) -> dict:
    results = search_tmdb_tv(config, query, limit=limit)
    tmdb_ids = [item["tmdb_id"] for item in results]
    statuses = get_series_status_map(config, tmdb_ids)
    sonarr = test_sonarr_connection(config)
    for item in results:
        item["arr_status"] = statuses.get(str(item["tmdb_id"])) or _status_record(False, False)
    return {"results": results, "connection": sonarr}


def get_bulk_status(config: dict | None, kind: str, tmdb_ids: list[int | str]) -> dict[str, dict]:
    normalized = str(kind or "").strip().lower()
    if normalized == "movie":
        return get_movie_status_map(config, tmdb_ids)
    if normalized == "tv":
        return get_series_status_map(config, tmdb_ids)
    raise ArrServiceError("Unsupported ARR status kind")


def get_tmdb_trailer(config: dict | None, kind: str, tmdb_id: int | str) -> dict:
    normalized = str(kind or "").strip().lower()
    if normalized not in {"movie", "tv"}:
        raise ArrServiceError("Unsupported ARR trailer kind")
    payload = _tmdb_request(config, f"/{normalized}/{int(tmdb_id)}/videos")
    picked = _pick_tmdb_video(payload)
    if not picked:
        return {"available": False}
    key = _trimmed(picked.get("key"))
    video_type = _trimmed(picked.get("type"))
    name = _trimmed(picked.get("name")) or "Trailer"
    return {
        "available": True,
        "source": "youtube",
        "type": video_type,
        "title": name,
        "watch_url": f"https://www.youtube.com/watch?v={quote(key)}",
        "embed_url": f"https://www.youtube.com/embed/{quote(key)}?autoplay=1&rel=0&modestbranding=1",
        "hover_embed_url": f"https://www.youtube.com/embed/{quote(key)}?autoplay=1&mute=1&controls=0&rel=0&modestbranding=1&playsinline=1",
    }
