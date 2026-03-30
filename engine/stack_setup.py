from __future__ import annotations

import json
import os
import secrets
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests


STACK_MODULES = (
    "core",
    "youtube",
    "telegram",
    "tmdb",
    "arr",
    "vpn",
    "jellyfin",
    "storage",
)

PROFILE_MAP = {
    "enable_arr_stack": "arr",
    "enable_qbittorrent": "downloader",
    "enable_vpn": "vpn",
    "enable_jellyfin": "jellyfin",
    "enable_bazarr": "subtitles",
    "enable_readarr": "books",
}

MANAGED_INTERNAL_URLS = {
    "radarr": "http://radarr:7878",
    "sonarr": "http://sonarr:8989",
    "readarr": "http://readarr:8787",
    "prowlarr": "http://prowlarr:9696",
    "bazarr": "http://bazarr:6767",
    "qbittorrent": "http://qbittorrent:8080",
    "gluetun": "http://gluetun:8000/v1/publicip/ip",
    "jellyfin": "http://jellyfin:8096",
}


def _trimmed(value: Any) -> str:
    return str(value or "").strip()


def normalize_stack_config(config: dict | None) -> dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    setup = cfg.get("setup") if isinstance(cfg.get("setup"), dict) else {}
    stack = setup.get("stack") if isinstance(setup.get("stack"), dict) else {}
    normalized = {
        "enable_arr_stack": bool(stack.get("enable_arr_stack")),
        "enable_radarr": bool(stack.get("enable_radarr")),
        "enable_sonarr": bool(stack.get("enable_sonarr")),
        "enable_readarr": bool(stack.get("enable_readarr")),
        "enable_prowlarr": bool(stack.get("enable_prowlarr")),
        "enable_bazarr": bool(stack.get("enable_bazarr")),
        "enable_qbittorrent": bool(stack.get("enable_qbittorrent")),
        "enable_vpn": bool(stack.get("enable_vpn")),
        "enable_jellyfin": bool(stack.get("enable_jellyfin")),
        "enable_hostctl": bool(stack.get("enable_hostctl")),
        "env_path": _trimmed(stack.get("env_path")) or ".env",
        "compose_profiles": list(stack.get("compose_profiles") or []),
        "media_root": _trimmed(stack.get("media_root")) or "./media",
        "movies_root": _trimmed(stack.get("movies_root")) or "./media/movies",
        "tv_root": _trimmed(stack.get("tv_root")) or "./media/tv",
        "downloads_root": _trimmed(stack.get("downloads_root")) or "./downloads",
        "books_root": _trimmed(stack.get("books_root")) or "./media/books",
    }
    if normalized["enable_arr_stack"]:
        if normalized["enable_radarr"] or normalized["enable_sonarr"] or normalized["enable_readarr"] or normalized["enable_prowlarr"]:
            normalized["enable_arr_stack"] = True
    normalized["compose_profiles"] = derive_profiles(normalized)
    return normalized


def derive_profiles(stack: dict[str, Any]) -> list[str]:
    profiles: list[str] = []
    if any(bool(stack.get(key)) for key in ("enable_radarr", "enable_sonarr", "enable_readarr", "enable_prowlarr", "enable_arr_stack")):
        profiles.append("arr")
    for key, profile in PROFILE_MAP.items():
        if profile == "arr":
            continue
        if bool(stack.get(key)) and profile not in profiles:
            profiles.append(profile)
    if bool(stack.get("enable_hostctl")) and "hostctl" not in profiles:
        profiles.append("hostctl")
    return profiles


def managed_service_feature_map() -> dict[str, tuple[str, ...]]:
    return {
        "movies": ("enable_arr_stack", "enable_radarr", "enable_prowlarr"),
        "tv": ("enable_arr_stack", "enable_sonarr", "enable_prowlarr"),
        "books": ("enable_arr_stack", "enable_readarr", "enable_prowlarr"),
        "subtitles": ("enable_bazarr",),
        "downloader": ("enable_qbittorrent",),
        "vpn": ("enable_vpn",),
        "jellyfin": ("enable_jellyfin",),
    }


def normalize_managed_plan(config: dict[str, Any] | None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    setup_cfg = cfg.get("setup") if isinstance(cfg.get("setup"), dict) else {}
    managed_cfg = setup_cfg.get("managed_stack") if isinstance(setup_cfg.get("managed_stack"), dict) else {}
    stack = normalize_stack_config(cfg)
    arr_cfg = cfg.get("arr") if isinstance(cfg.get("arr"), dict) else {}
    vpn_cfg = arr_cfg.get("vpn") if isinstance(arr_cfg.get("vpn"), dict) else {}
    source = payload if isinstance(payload, dict) else {}
    enabled_features = managed_cfg.get("enabled_features") if isinstance(managed_cfg.get("enabled_features"), dict) else {}
    feature_map = managed_service_feature_map()
    features = {
        key: bool(source.get(key, enabled_features.get(key, False)))
        for key in feature_map.keys()
    }
    if features["movies"] or features["tv"] or features["books"]:
        features["downloader"] = bool(source.get("downloader", features["downloader"] or True))
    if features["downloader"] and bool(source.get("vpn", enabled_features.get("vpn", False))):
        features["vpn"] = True
    direct_manage = bool(source.get("direct_manage", managed_cfg.get("direct_manage_requested")))
    selected_services: list[str] = []
    next_stack = dict(stack)
    for stack_key in (
        "enable_arr_stack",
        "enable_radarr",
        "enable_sonarr",
        "enable_readarr",
        "enable_prowlarr",
        "enable_bazarr",
        "enable_qbittorrent",
        "enable_vpn",
        "enable_jellyfin",
        "enable_hostctl",
    ):
        next_stack[stack_key] = False
    for feature_name, stack_keys in feature_map.items():
        if not features.get(feature_name):
            continue
        for stack_key in stack_keys:
            next_stack[stack_key] = True
    if next_stack.get("enable_bazarr"):
        next_stack["enable_arr_stack"] = True
        next_stack["enable_radarr"] = True
        next_stack["enable_sonarr"] = True
    if next_stack.get("enable_qbittorrent") and not next_stack.get("enable_vpn"):
        next_stack["enable_vpn"] = bool(source.get("vpn_required_if_downloader", False))
    for path_key in ("env_path", "media_root", "movies_root", "tv_root", "downloads_root", "books_root"):
        if path_key in source:
            next_stack[path_key] = _trimmed(source.get(path_key))
    next_stack["compose_profiles"] = derive_profiles(next_stack)
    for key, label in (
        ("enable_radarr", "radarr"),
        ("enable_sonarr", "sonarr"),
        ("enable_readarr", "readarr"),
        ("enable_prowlarr", "prowlarr"),
        ("enable_bazarr", "bazarr"),
        ("enable_qbittorrent", "qbittorrent"),
        ("enable_vpn", "gluetun"),
        ("enable_jellyfin", "jellyfin"),
    ):
        if next_stack.get(key):
            selected_services.append(label)
    if direct_manage:
        selected_services.append("retreivr-hostctl")
        next_stack["enable_hostctl"] = True
    selected_services = list(dict.fromkeys(selected_services))
    vpn_provider = _trimmed(source.get("vpn_provider")) or _trimmed(vpn_cfg.get("provider")) or "gluetun"
    vpn_credentials = {
        "provider": vpn_provider,
        "openvpn_user": _trimmed(source.get("vpn_openvpn_user")),
        "openvpn_password": _trimmed(source.get("vpn_openvpn_password")),
        "wireguard_private_key": _trimmed(source.get("vpn_wireguard_private_key")),
        "server_countries": _trimmed(source.get("vpn_server_countries")),
    }
    internal_urls = {
        "radarr": MANAGED_INTERNAL_URLS["radarr"] if next_stack.get("enable_radarr") else "",
        "sonarr": MANAGED_INTERNAL_URLS["sonarr"] if next_stack.get("enable_sonarr") else "",
        "readarr": MANAGED_INTERNAL_URLS["readarr"] if next_stack.get("enable_readarr") else "",
        "prowlarr": MANAGED_INTERNAL_URLS["prowlarr"] if next_stack.get("enable_prowlarr") else "",
        "bazarr": MANAGED_INTERNAL_URLS["bazarr"] if next_stack.get("enable_bazarr") else "",
        "qbittorrent": MANAGED_INTERNAL_URLS["qbittorrent"] if next_stack.get("enable_qbittorrent") else "",
        "jellyfin": MANAGED_INTERNAL_URLS["jellyfin"] if next_stack.get("enable_jellyfin") else "",
        "vpn_control": MANAGED_INTERNAL_URLS["gluetun"] if next_stack.get("enable_vpn") else "",
    }
    credentials = managed_cfg.get("generated_credentials") if isinstance(managed_cfg.get("generated_credentials"), dict) else {}
    qb_username = _trimmed(credentials.get("qbittorrent_username")) or _trimmed((arr_cfg.get("qbittorrent") or {}).get("username")) or "retreivr"
    qb_password = _trimmed(credentials.get("qbittorrent_password")) or _trimmed((arr_cfg.get("qbittorrent") or {}).get("password")) or secrets.token_urlsafe(18)
    return {
        "mode": "managed",
        "apply_mode": "direct" if direct_manage else "manual",
        "direct_manage_requested": direct_manage,
        "selected_services": selected_services,
        "enabled_features": features,
        "stack": next_stack,
        "internal_urls": internal_urls,
        "generated_credentials": {
            "qbittorrent_username": qb_username,
            "qbittorrent_password": qb_password,
        },
        "vpn": vpn_credentials,
    }


def normalize_existing_stack_payload(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    normalized: dict[str, Any] = {"mode": "existing", "services": {}}
    for service_name in ("radarr", "sonarr", "readarr", "prowlarr", "bazarr", "qbittorrent", "jellyfin"):
        service_payload = source.get(service_name) if isinstance(source.get(service_name), dict) else {}
        enabled = bool(service_payload.get("enabled"))
        normalized["services"][service_name] = {
            "enabled": enabled,
            "base_url": _trimmed(service_payload.get("base_url")),
            "api_key": _trimmed(service_payload.get("api_key")),
            "username": _trimmed(service_payload.get("username")),
            "password": _trimmed(service_payload.get("password")),
        }
    return normalized

def build_compose_command(stack: dict[str, Any]) -> str:
    profiles = derive_profiles(stack)
    profile_flags = " ".join(f"--profile {profile}" for profile in profiles)
    parts = ["docker compose"]
    if profile_flags:
        parts.append(profile_flags)
    parts.append("up -d")
    return " ".join(parts)


def managed_env_values(config: dict[str, Any], stack: dict[str, Any]) -> dict[str, str]:
    arr_cfg = config.get("arr") if isinstance(config.get("arr"), dict) else {}
    qb_cfg = arr_cfg.get("qbittorrent") if isinstance(arr_cfg.get("qbittorrent"), dict) else {}
    jelly_cfg = arr_cfg.get("jellyfin") if isinstance(arr_cfg.get("jellyfin"), dict) else {}
    vpn_cfg = arr_cfg.get("vpn") if isinstance(arr_cfg.get("vpn"), dict) else {}
    return {
        "RETREIVR_STACK_ENABLE_ARR": "1" if stack.get("enable_arr_stack") else "0",
        "RETREIVR_STACK_ENABLE_RADARR": "1" if stack.get("enable_radarr") else "0",
        "RETREIVR_STACK_ENABLE_SONARR": "1" if stack.get("enable_sonarr") else "0",
        "RETREIVR_STACK_ENABLE_READARR": "1" if stack.get("enable_readarr") else "0",
        "RETREIVR_STACK_ENABLE_PROWLARR": "1" if stack.get("enable_prowlarr") else "0",
        "RETREIVR_STACK_ENABLE_BAZARR": "1" if stack.get("enable_bazarr") else "0",
        "RETREIVR_STACK_ENABLE_QBITTORRENT": "1" if stack.get("enable_qbittorrent") else "0",
        "RETREIVR_STACK_ENABLE_VPN": "1" if stack.get("enable_vpn") else "0",
        "RETREIVR_STACK_ENABLE_JELLYFIN": "1" if stack.get("enable_jellyfin") else "0",
        "RETREIVR_STACK_ENABLE_HOSTCTL": "1" if stack.get("enable_hostctl") else "0",
        "RETREIVR_MEDIA_ROOT": str(stack.get("media_root") or "./media"),
        "RETREIVR_MOVIES_ROOT": str(stack.get("movies_root") or "./media/movies"),
        "RETREIVR_TV_ROOT": str(stack.get("tv_root") or "./media/tv"),
        "RETREIVR_DOWNLOADS_ROOT": str(stack.get("downloads_root") or "./downloads"),
        "RETREIVR_BOOKS_ROOT": str(stack.get("books_root") or "./media/books"),
        "RETREIVR_QBITTORRENT_BASE_URL": _trimmed(qb_cfg.get("base_url")) or "http://qbittorrent:8080",
        "RETREIVR_QBITTORRENT_USERNAME": _trimmed(qb_cfg.get("username")) or "retreivr",
        "RETREIVR_QBITTORRENT_PASSWORD": _trimmed(qb_cfg.get("password")),
        "RETREIVR_JELLYFIN_BASE_URL": _trimmed(jelly_cfg.get("base_url")) or "http://jellyfin:8096",
        "RETREIVR_VPN_CONTROL_URL": _trimmed(vpn_cfg.get("control_url")) or "http://gluetun:8000/v1/publicip/ip",
        "RETREIVR_VPN_ROUTE_QBITTORRENT": "1" if vpn_cfg.get("route_qbittorrent", True) else "0",
        "RETREIVR_VPN_ROUTE_PROWLARR": "1" if vpn_cfg.get("route_prowlarr") else "0",
        "RETREIVR_VPN_ROUTE_RETREIVR": "1" if vpn_cfg.get("route_retreivr") else "0",
        "RETREIVR_HOSTCTL_URL": os.environ.get("RETREIVR_HOSTCTL_URL") or "http://retreivr-hostctl:8010",
        "GLUETUN_PROVIDER": _trimmed(vpn_cfg.get("provider")),
    }


def write_managed_env_block(env_path: str | os.PathLike[str], values: dict[str, str]) -> Path:
    target = Path(env_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    start_marker = "# >>> RETREIVR MANAGED STACK >>>"
    end_marker = "# <<< RETREIVR MANAGED STACK <<<"
    existing = target.read_text(encoding="utf-8") if target.exists() else ""
    lines = existing.splitlines()
    out: list[str] = []
    inside = False
    for line in lines:
        if line.strip() == start_marker:
            inside = True
            continue
        if line.strip() == end_marker:
            inside = False
            continue
        if not inside:
            out.append(line)
    if out and out[-1] != "":
        out.append("")
    out.append(start_marker)
    for key in sorted(values.keys()):
        out.append(f"{key}={values[key]}")
    out.append(end_marker)
    out.append("")
    target.write_text("\n".join(out), encoding="utf-8")
    return target


def _repo_root_from_env() -> Path:
    return Path(os.environ.get("RETREIVR_WORKSPACE") or Path(__file__).resolve().parents[1]).resolve()


def _managed_config_root(service_name: str) -> Path:
    return _repo_root_from_env() / "config" / service_name


def _discover_servarr_api_key(service_name: str) -> str:
    config_xml = _managed_config_root(service_name) / "config.xml"
    if not config_xml.exists():
        return ""
    try:
        text = config_xml.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    marker_start = "<ApiKey>"
    marker_end = "</ApiKey>"
    start = text.find(marker_start)
    end = text.find(marker_end)
    if start == -1 or end == -1 or end <= start:
        return ""
    return text[start + len(marker_start):end].strip()


def discover_managed_service_keys(config: dict[str, Any]) -> dict[str, str]:
    keys: dict[str, str] = {}
    stack = normalize_stack_config(config)
    for service_name, stack_key in (
        ("radarr", "enable_radarr"),
        ("sonarr", "enable_sonarr"),
        ("readarr", "enable_readarr"),
        ("prowlarr", "enable_prowlarr"),
    ):
        if stack.get(stack_key):
            api_key = _discover_servarr_api_key(service_name)
            if api_key:
                keys[service_name] = api_key
    return keys


def apply_managed_service_defaults(config: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    updated = dict(config or {})
    arr_cfg = dict(updated.get("arr") or {})
    stack = dict(plan.get("stack") or {})
    internal_urls = plan.get("internal_urls") if isinstance(plan.get("internal_urls"), dict) else {}
    credentials = plan.get("generated_credentials") if isinstance(plan.get("generated_credentials"), dict) else {}
    for service_name in ("radarr", "sonarr", "readarr", "prowlarr", "bazarr", "jellyfin"):
        service_cfg = dict(arr_cfg.get(service_name) or {})
        if internal_urls.get(service_name):
            service_cfg["base_url"] = internal_urls.get(service_name)
        arr_cfg[service_name] = service_cfg
    qb_cfg = dict(arr_cfg.get("qbittorrent") or {})
    if internal_urls.get("qbittorrent"):
        qb_cfg["base_url"] = internal_urls.get("qbittorrent")
    qb_cfg["username"] = _trimmed(credentials.get("qbittorrent_username")) or _trimmed(qb_cfg.get("username")) or "retreivr"
    qb_cfg["password"] = _trimmed(credentials.get("qbittorrent_password")) or _trimmed(qb_cfg.get("password"))
    qb_cfg["download_dir"] = str(stack.get("downloads_root") or qb_cfg.get("download_dir") or "./downloads")
    arr_cfg["qbittorrent"] = qb_cfg
    vpn_cfg = dict(arr_cfg.get("vpn") or {})
    if internal_urls.get("vpn_control"):
        vpn_cfg["control_url"] = internal_urls.get("vpn_control")
    vpn_data = plan.get("vpn") if isinstance(plan.get("vpn"), dict) else {}
    if vpn_data:
        vpn_cfg["provider"] = _trimmed(vpn_data.get("provider")) or _trimmed(vpn_cfg.get("provider")) or "gluetun"
    vpn_cfg["enabled"] = bool(stack.get("enable_vpn"))
    vpn_cfg["route_qbittorrent"] = True
    arr_cfg["vpn"] = vpn_cfg
    discovered_keys = discover_managed_service_keys(updated)
    for service_name, api_key in discovered_keys.items():
        service_cfg = dict(arr_cfg.get(service_name) or {})
        service_cfg["api_key"] = api_key
        arr_cfg[service_name] = service_cfg
    updated["arr"] = arr_cfg
    return updated


def build_managed_status(config: dict[str, Any]) -> dict[str, Any]:
    setup_cfg = config.get("setup") if isinstance(config.get("setup"), dict) else {}
    managed = setup_cfg.get("managed_stack") if isinstance(setup_cfg.get("managed_stack"), dict) else {}
    stack = normalize_stack_config(config)
    internal_urls = managed.get("internal_urls") if isinstance(managed.get("internal_urls"), dict) else {}
    selected_services = list(managed.get("selected_services") or [])
    health = build_connections_status(config)
    hostctl_cfg = {
        "configured": bool(stack.get("enable_hostctl")),
        "reachable": False,
        "status": "not_enabled",
    }
    hostctl_url = _trimmed(os.environ.get("RETREIVR_HOSTCTL_URL")) or "http://retreivr-hostctl:8010"
    if stack.get("enable_hostctl"):
        reachable, message = _probe_json(f"{hostctl_url}/health")
        hostctl_cfg = {"configured": True, "reachable": reachable, "status": message, "base_url": hostctl_url}
    phase = _trimmed(managed.get("phase")) or "idle"
    return {
        "mode": "managed",
        "apply_mode": "direct" if bool(managed.get("direct_manage_requested")) else "manual",
        "phase": phase,
        "phase_message": _trimmed(managed.get("phase_message")),
        "last_error": _trimmed(managed.get("last_error")),
        "resume_ready": bool(managed.get("resume_ready")),
        "selected_services": selected_services,
        "internal_urls": internal_urls,
        "hostctl": hostctl_cfg,
        "services": health,
        "last_health": managed.get("last_health") if isinstance(managed.get("last_health"), dict) else {},
        "last_configure_result": managed.get("last_configure_result") if isinstance(managed.get("last_configure_result"), dict) else {},
    }


def build_existing_status(config: dict[str, Any]) -> dict[str, Any]:
    setup_cfg = config.get("setup") if isinstance(config.get("setup"), dict) else {}
    existing = setup_cfg.get("existing_stack") if isinstance(setup_cfg.get("existing_stack"), dict) else {}
    return {
        "mode": "existing",
        "selected_services": list(existing.get("selected_services") or []),
        "services": existing.get("discovered_health") if isinstance(existing.get("discovered_health"), dict) else {},
    }


def _probe_json(url: str, *, headers: dict[str, str] | None = None, timeout: float = 4.0) -> tuple[bool, str]:
    try:
        response = requests.get(url, headers=headers or {}, timeout=timeout)
        if response.ok:
            return True, "reachable"
        return False, f"http_{response.status_code}"
    except Exception as exc:  # noqa: BLE001
        return False, exc.__class__.__name__


def _service_api_version(service_name: str) -> str:
    return "v1" if service_name == "prowlarr" else "v3"


def _service_cfg(config: dict[str, Any], service_name: str) -> dict[str, Any]:
    arr_cfg = config.get("arr") if isinstance(config.get("arr"), dict) else {}
    service_cfg = arr_cfg.get(service_name) if isinstance(arr_cfg.get(service_name), dict) else {}
    return {
        "base_url": _trimmed(service_cfg.get("base_url")).rstrip("/"),
        "api_key": _trimmed(service_cfg.get("api_key")),
    }


def _servarr_request(
    service_name: str,
    service_cfg: dict[str, Any],
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: Any = None,
    timeout: float = 12.0,
) -> Any:
    base_url = _trimmed(service_cfg.get("base_url")).rstrip("/")
    api_key = _trimmed(service_cfg.get("api_key"))
    if not base_url or not api_key:
        raise RuntimeError(f"{service_name} is not configured")
    version = _service_api_version(service_name)
    response = requests.request(
        method.upper(),
        f"{base_url}/api/{version}/{path.lstrip('/')}",
        headers={"X-Api-Key": api_key},
        params=params,
        json=json_body,
        timeout=timeout,
    )
    if response.status_code >= 400:
        body = response.text[:300]
        raise RuntimeError(f"{service_name} {method.upper()} {path} failed: http_{response.status_code} {body}")
    if not response.content:
        return None
    content_type = response.headers.get("content-type", "")
    if "json" in content_type.lower():
        return response.json()
    text = response.text.strip()
    return text


def _qbit_login_session(base_url: str, username: str, password: str) -> requests.Session:
    normalized = _trimmed(base_url).rstrip("/")
    if not normalized or not username or not password:
        raise RuntimeError("qBittorrent is not fully configured")
    session = requests.Session()
    session.headers.update({"Referer": f"{normalized}/", "Origin": normalized})
    response = session.post(
        f"{normalized}/api/v2/auth/login",
        data={"username": username, "password": password},
        timeout=10.0,
    )
    if response.status_code >= 400 or "fail" in response.text.lower():
        raise RuntimeError(f"qBittorrent login failed: http_{response.status_code}")
    return session


def _qbit_request(
    session: requests.Session,
    base_url: str,
    method: str,
    path: str,
    *,
    data: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> Any:
    normalized = _trimmed(base_url).rstrip("/")
    response = session.request(
        method.upper(),
        f"{normalized}/api/v2/{path.lstrip('/')}",
        data=data,
        timeout=timeout,
    )
    if response.status_code >= 400:
        body = response.text[:300]
        raise RuntimeError(f"qBittorrent {method.upper()} {path} failed: http_{response.status_code} {body}")
    content_type = response.headers.get("content-type", "")
    if "json" in content_type.lower():
        return response.json()
    return response.text.strip()


def _set_field(fields: Any, name: str, value: Any) -> None:
    if not isinstance(fields, list):
        return
    for field in fields:
        if not isinstance(field, dict):
            continue
        if str(field.get("name") or "") == name:
            field["value"] = value
            return


def _first_int_id(rows: Any) -> int | None:
    if not isinstance(rows, list):
        return None
    for item in rows:
        if isinstance(item, dict) and isinstance(item.get("id"), int):
            return int(item["id"])
    return None


def _ensure_root_folder(service_name: str, service_cfg: dict[str, Any], desired_path: str) -> dict[str, Any]:
    path = _trimmed(desired_path)
    if not path:
        return {"status": "skipped", "message": "No target path provided"}
    folders = _servarr_request(service_name, service_cfg, "GET", "rootfolder") or []
    for folder in folders:
        existing = _trimmed((folder or {}).get("path"))
        if existing.rstrip("/") == path.rstrip("/"):
            return {"status": "exists", "path": existing}
    created = _servarr_request(service_name, service_cfg, "POST", "rootfolder", json_body={"path": path}) or {}
    return {"status": "created", "path": _trimmed((created or {}).get("path")) or path}


def _extract_qbit_connection(qb_cfg: dict[str, Any]) -> dict[str, Any]:
    base_url = _trimmed(qb_cfg.get("base_url")).rstrip("/")
    parsed = urlsplit(base_url if "://" in base_url else f"http://{base_url}")
    host = parsed.hostname or "qbittorrent"
    port = parsed.port or (443 if parsed.scheme == "https" else 8080)
    return {
        "base_url": base_url,
        "host": host,
        "port": port,
        "useSsl": parsed.scheme == "https",
        "urlBase": parsed.path or "",
    }


def _build_download_client_payload(service_name: str, schema: dict[str, Any], existing: dict[str, Any] | None, qb_cfg: dict[str, Any], category: str) -> dict[str, Any]:
    payload = dict(existing or schema or {})
    payload.pop("id", None)
    payload["enable"] = True
    payload["priority"] = int(payload.get("priority") or 1)
    payload["name"] = str((existing or {}).get("name") or f"Retreivr qBittorrent ({service_name.title()})")
    payload.setdefault("implementation", "QBittorrent")
    payload.setdefault("implementationName", "qBittorrent")
    payload.setdefault("configContract", "QBittorrentSettings")
    payload.setdefault("protocol", "torrent")
    fields = [dict(item) for item in (payload.get("fields") or []) if isinstance(item, dict)]
    if not fields:
        fields = [
            {"name": "host", "value": ""},
            {"name": "port", "value": 8080},
            {"name": "useSsl", "value": False},
            {"name": "urlBase", "value": ""},
            {"name": "username", "value": ""},
            {"name": "password", "value": ""},
            {"name": "category", "value": ""},
            {"name": "tvCategory", "value": ""},
            {"name": "movieCategory", "value": ""},
        ]
    payload["fields"] = fields
    conn = _extract_qbit_connection(qb_cfg)
    _set_field(fields, "host", conn["host"])
    _set_field(fields, "port", conn["port"])
    _set_field(fields, "useSsl", conn["useSsl"])
    _set_field(fields, "urlBase", conn["urlBase"])
    _set_field(fields, "username", _trimmed(qb_cfg.get("username")))
    _set_field(fields, "password", _trimmed(qb_cfg.get("password")))
    _set_field(fields, "movieCategory", category)
    _set_field(fields, "tvCategory", category)
    _set_field(fields, "category", category)
    if service_name == "radarr":
        payload["enableCompletedDownloadHandling"] = True
        payload["removeCompletedDownloads"] = False
    elif service_name == "sonarr":
        payload["enableCompletedDownloadHandling"] = True
        payload["removeCompletedDownloads"] = False
    elif service_name == "readarr":
        payload["enableCompletedDownloadHandling"] = True
        payload["removeCompletedDownloads"] = False
    return payload


def _ensure_download_client(service_name: str, service_cfg: dict[str, Any], qb_cfg: dict[str, Any], category: str) -> dict[str, Any]:
    if not _trimmed(qb_cfg.get("base_url")) or not _trimmed(qb_cfg.get("username")) or not _trimmed(qb_cfg.get("password")):
        return {"status": "skipped", "message": "qBittorrent credentials are incomplete"}
    clients = _servarr_request(service_name, service_cfg, "GET", "downloadclient") or []
    existing = None
    if isinstance(clients, list):
        for item in clients:
            if not isinstance(item, dict):
                continue
            implementation = str(item.get("implementation") or item.get("implementationName") or "").lower()
            if "qbit" in implementation:
                existing = item
                break
    schema_rows = _servarr_request(service_name, service_cfg, "GET", "downloadclient/schema") or []
    schema = next(
        (
            item for item in schema_rows
            if isinstance(item, dict) and "qbit" in str(item.get("implementation") or item.get("implementationName") or "").lower()
        ),
        {},
    )
    payload = _build_download_client_payload(service_name, schema, existing, qb_cfg, category)
    if existing and existing.get("id") is not None:
        payload["id"] = existing["id"]
        updated = _servarr_request(service_name, service_cfg, "PUT", f"downloadclient/{int(existing['id'])}", json_body=payload) or {}
        return {"status": "updated", "name": _trimmed((updated or {}).get("name")) or payload["name"]}
    created = _servarr_request(service_name, service_cfg, "POST", "downloadclient", json_body=payload) or {}
    return {"status": "created", "name": _trimmed((created or {}).get("name")) or payload["name"]}


def _ensure_arr_service(service_name: str, config: dict[str, Any], stack: dict[str, Any]) -> dict[str, Any]:
    service_cfg = _service_cfg(config, service_name)
    if not service_cfg["base_url"] or not service_cfg["api_key"]:
        return {"status": "not_configured", "message": f"{service_name} is not configured"}
    result: dict[str, Any] = {"status": "connected", "actions": []}
    _servarr_request(service_name, service_cfg, "GET", "system/status")
    path_map = {
        "radarr": str(stack.get("movies_root") or stack.get("media_root") or "./media/movies"),
        "sonarr": str(stack.get("tv_root") or stack.get("media_root") or "./media/tv"),
        "readarr": str(stack.get("books_root") or "./media/books"),
    }
    category_map = {
        "radarr": _trimmed((((config.get("arr") or {}).get("qbittorrent") or {}).get("category_movies"))) or "movies",
        "sonarr": _trimmed((((config.get("arr") or {}).get("qbittorrent") or {}).get("category_tv"))) or "tv",
        "readarr": _trimmed((((config.get("arr") or {}).get("qbittorrent") or {}).get("category_books"))) or "books",
    }
    if service_name in path_map:
        root_result = _ensure_root_folder(service_name, service_cfg, path_map[service_name])
        result["root_folder"] = root_result
        result["actions"].append(f"root:{root_result.get('status')}")
        profiles = _servarr_request(service_name, service_cfg, "GET", "qualityprofile") or []
        profile_id = _first_int_id(profiles)
        result["quality_profile"] = {"status": "found" if profile_id else "missing", "id": profile_id}
        if service_name in ("radarr", "sonarr", "readarr"):
            qb_cfg = ((config.get("arr") or {}).get("qbittorrent") or {}) if isinstance((config.get("arr") or {}).get("qbittorrent"), dict) else {}
            dl_result = _ensure_download_client(service_name, service_cfg, qb_cfg, category_map[service_name])
            result["download_client"] = dl_result
            result["actions"].append(f"download_client:{dl_result.get('status')}")
    return result


def _application_payload(template: dict[str, Any], existing: dict[str, Any] | None, target_name: str, target_cfg: dict[str, Any], prowlarr_base_url: str) -> dict[str, Any]:
    payload = dict(existing or template or {})
    payload.pop("id", None)
    payload["enable"] = True
    payload["name"] = str((existing or {}).get("name") or f"Retreivr {target_name}")
    payload.setdefault("implementation", target_name)
    payload.setdefault("implementationName", target_name)
    payload.setdefault("configContract", f"{target_name}Settings")
    payload.setdefault("syncLevel", "fullSync")
    fields = [dict(item) for item in (payload.get("fields") or []) if isinstance(item, dict)]
    if not fields:
        fields = [
            {"name": "apiKey", "value": ""},
            {"name": "baseUrl", "value": ""},
            {"name": "prowlarrUrl", "value": ""},
            {"name": "syncLevel", "value": "fullSync"},
        ]
    payload["fields"] = fields
    _set_field(fields, "apiKey", _trimmed(target_cfg.get("api_key")))
    _set_field(fields, "baseUrl", _trimmed(target_cfg.get("base_url")))
    _set_field(fields, "prowlarrUrl", prowlarr_base_url)
    _set_field(fields, "syncLevel", "fullSync")
    return payload


def _ensure_prowlarr_applications(config: dict[str, Any]) -> dict[str, Any]:
    prowlarr_cfg = _service_cfg(config, "prowlarr")
    if not prowlarr_cfg["base_url"] or not prowlarr_cfg["api_key"]:
        return {"status": "not_configured", "message": "Prowlarr is not configured"}
    _servarr_request("prowlarr", prowlarr_cfg, "GET", "system/status")
    existing_rows = _servarr_request("prowlarr", prowlarr_cfg, "GET", "applications") or []
    schema_rows = _servarr_request("prowlarr", prowlarr_cfg, "GET", "applications/schema") or []
    results: dict[str, Any] = {"status": "connected", "applications": {}}
    for target_name in ("Radarr", "Sonarr", "Readarr"):
        target_key = target_name.lower()
        target_cfg = _service_cfg(config, target_key)
        if not target_cfg["base_url"] or not target_cfg["api_key"]:
            results["applications"][target_key] = {"status": "not_configured"}
            continue
        existing = next(
            (
                item for item in existing_rows
                if isinstance(item, dict) and str(item.get("implementation") or item.get("implementationName") or "").lower() == target_name.lower()
            ),
            None,
        )
        template = next(
            (
                item for item in schema_rows
                if isinstance(item, dict) and str(item.get("implementation") or item.get("implementationName") or "").lower() == target_name.lower()
            ),
            {},
        )
        payload = _application_payload(template, existing, target_name, target_cfg, prowlarr_cfg["base_url"])
        if existing and existing.get("id") is not None:
            payload["id"] = existing["id"]
            _servarr_request("prowlarr", prowlarr_cfg, "PUT", f"applications/{int(existing['id'])}", json_body=payload)
            results["applications"][target_key] = {"status": "updated"}
        else:
            _servarr_request("prowlarr", prowlarr_cfg, "POST", "applications", json_body=payload)
            results["applications"][target_key] = {"status": "created"}
    return results


def auto_configure_services(config: dict[str, Any]) -> dict[str, Any]:
    stack = normalize_stack_config(config)
    arr_cfg = config.get("arr") if isinstance(config.get("arr"), dict) else {}
    results: dict[str, Any] = {}

    qb_cfg = arr_cfg.get("qbittorrent") if isinstance(arr_cfg.get("qbittorrent"), dict) else {}
    try:
        if _trimmed(qb_cfg.get("base_url")) and _trimmed(qb_cfg.get("username")) and _trimmed(qb_cfg.get("password")):
            session = _qbit_login_session(_trimmed(qb_cfg.get("base_url")), _trimmed(qb_cfg.get("username")), _trimmed(qb_cfg.get("password")))
            download_root = _trimmed(qb_cfg.get("download_dir")) or str(stack.get("downloads_root") or "./downloads")
            _qbit_request(session, _trimmed(qb_cfg.get("base_url")), "POST", "app/setPreferences", data={"json": json.dumps({"save_path": download_root})})
            categories = _qbit_request(session, _trimmed(qb_cfg.get("base_url")), "GET", "torrents/categories") or {}
            category_map = {
                _trimmed(qb_cfg.get("category_movies")) or "movies": f"{download_root.rstrip('/')}/{_trimmed(qb_cfg.get('category_movies')) or 'movies'}",
                _trimmed(qb_cfg.get("category_tv")) or "tv": f"{download_root.rstrip('/')}/{_trimmed(qb_cfg.get('category_tv')) or 'tv'}",
                _trimmed(qb_cfg.get("category_books")) or "books": f"{download_root.rstrip('/')}/{_trimmed(qb_cfg.get('category_books')) or 'books'}",
            }
            category_actions = []
            for category_name, save_path in category_map.items():
                if category_name in categories:
                    _qbit_request(session, _trimmed(qb_cfg.get("base_url")), "POST", "torrents/editCategory", data={"category": category_name, "savePath": save_path})
                    category_actions.append(f"{category_name}:updated")
                else:
                    _qbit_request(session, _trimmed(qb_cfg.get("base_url")), "POST", "torrents/createCategory", data={"category": category_name, "savePath": save_path})
                    category_actions.append(f"{category_name}:created")
            results["qbittorrent"] = {"status": "configured", "download_root": download_root, "categories": category_actions}
        else:
            results["qbittorrent"] = {"status": "not_configured"}
    except Exception as exc:  # noqa: BLE001
        results["qbittorrent"] = {"status": "needs_attention", "message": str(exc)}

    for service_name in ("radarr", "sonarr", "readarr"):
        try:
            results[service_name] = _ensure_arr_service(service_name, config, stack)
        except Exception as exc:  # noqa: BLE001
            results[service_name] = {"status": "needs_attention", "message": str(exc)}

    try:
        results["prowlarr"] = _ensure_prowlarr_applications(config)
    except Exception as exc:  # noqa: BLE001
        results["prowlarr"] = {"status": "needs_attention", "message": str(exc)}

    return results


def build_setup_status(config: dict[str, Any]) -> dict[str, Any]:
    arr_cfg = config.get("arr") if isinstance(config.get("arr"), dict) else {}
    telegram_cfg = config.get("telegram") if isinstance(config.get("telegram"), dict) else {}
    youtube_cfg = config.get("youtube") if isinstance(config.get("youtube"), dict) else {}
    music_cfg = config.get("music") if isinstance(config.get("music"), dict) else {}
    stack = normalize_stack_config(config)
    completed_modules = set(((config.get("setup") or {}).get("completed_modules") or []))
    tmdb_ready = bool(_trimmed(arr_cfg.get("tmdb_api_key")))
    telegram_ready = bool(_trimmed(telegram_cfg.get("bot_token")) and _trimmed(telegram_cfg.get("chat_id")))
    youtube_ready = bool(((youtube_cfg.get("cookies") or {}).get("enabled")))
    storage_ready = bool(_trimmed(music_cfg.get("library_path")) or _trimmed(config.get("single_download_folder")))
    arr_enabled = any(stack.get(key) for key in ("enable_arr_stack", "enable_radarr", "enable_sonarr", "enable_readarr", "enable_prowlarr"))
    setup_cfg = config.get("setup") if isinstance(config.get("setup"), dict) else {}
    service_mgmt = setup_cfg.get("service_management") if isinstance(setup_cfg.get("service_management"), dict) else {}
    restart_required = bool(setup_cfg.get("restart_required"))
    last_applied_at = _trimmed(setup_cfg.get("last_applied_at"))
    last_applied_command = _trimmed(setup_cfg.get("last_applied_command"))
    last_applied_env_path = _trimmed(setup_cfg.get("last_applied_env_path"))
    managed_status = build_managed_status(config)
    existing_status = build_existing_status(config)
    modules = {
        "core": {"required": True, "status": "verified", "title": "Core Retreivr", "summary": "Retreivr is installed and reachable."},
        "youtube": {"required": False, "status": "verified" if youtube_ready else "needs_input", "title": "YouTube / Cookies", "summary": "Optional for watcher and authenticated YouTube flows."},
        "telegram": {"required": False, "status": "verified" if telegram_ready else "needs_input", "title": "Telegram Notifications", "summary": "Optional operator notifications and summaries."},
        "tmdb": {"required": False, "status": "verified" if tmdb_ready else "needs_input", "title": "TMDb", "summary": "Required for native Movies & TV discovery."},
        "arr": {"required": False, "status": "verified" if arr_enabled else "optional", "title": "ARR Stack", "summary": "Optional Radarr/Sonarr/Readarr/Prowlarr stack."},
        "vpn": {"required": False, "status": "verified" if stack.get("enable_vpn") else "optional", "title": "VPN / Gluetun", "summary": "Recommended for torrent-backed flows."},
        "jellyfin": {"required": False, "status": "verified" if stack.get("enable_jellyfin") else "optional", "title": "Jellyfin", "summary": "Optional playback server integration."},
        "storage": {"required": True, "status": "verified" if storage_ready else "needs_input", "title": "Storage / Media Folders", "summary": "Media and download roots for Retreivr and optional stack services."},
    }
    for key, payload in modules.items():
        payload["complete"] = key in completed_modules or payload["status"] == "verified"
    return {
        "modules": modules,
        "service_management": {
            "mode": _trimmed(service_mgmt.get("mode")) or "none",
            "apply_mode": _trimmed(service_mgmt.get("apply_mode")) or "manual",
        },
        "managed_stack": managed_status,
        "existing_stack": existing_status,
        "stack": {
            **stack,
            "compose_profiles": derive_profiles(stack),
            "compose_command": build_compose_command(stack),
            "vpn_policy": build_stack_apply_summary(config, stack).get("vpn_policy") or {},
            "restart_required": restart_required,
            "last_applied_at": last_applied_at,
            "last_applied_command": last_applied_command,
            "last_applied_env_path": last_applied_env_path,
        },
    }


def build_stack_apply_summary(config: dict[str, Any], stack: dict[str, Any]) -> dict[str, Any]:
    profiles = derive_profiles(stack)
    arr_cfg = config.get("arr") if isinstance(config.get("arr"), dict) else {}
    vpn_cfg = arr_cfg.get("vpn") if isinstance(arr_cfg.get("vpn"), dict) else {}
    enabled_services: list[str] = []
    for key, label in (
        ("enable_radarr", "Radarr"),
        ("enable_sonarr", "Sonarr"),
        ("enable_readarr", "Readarr"),
        ("enable_prowlarr", "Prowlarr"),
        ("enable_bazarr", "Bazarr"),
        ("enable_qbittorrent", "qBittorrent"),
        ("enable_vpn", "VPN / Gluetun"),
        ("enable_jellyfin", "Jellyfin"),
        ("enable_hostctl", "Retreivr Host Control"),
    ):
        if stack.get(key):
            enabled_services.append(label)
    return {
        "profiles": profiles,
        "enabled_services": enabled_services,
        "compose_command": build_compose_command(stack),
        "managed_env_values": managed_env_values(config, stack),
        "restart_required": True,
        "paths": {
            "media_root": str(stack.get("media_root") or "./media"),
            "movies_root": str(stack.get("movies_root") or "./media/movies"),
            "tv_root": str(stack.get("tv_root") or "./media/tv"),
            "downloads_root": str(stack.get("downloads_root") or "./downloads"),
            "books_root": str(stack.get("books_root") or "./media/books"),
        },
        "vpn_policy": {
            "enabled": bool(vpn_cfg.get("enabled")),
            "provider": _trimmed(vpn_cfg.get("provider")) or "gluetun",
            "route_qbittorrent": bool(vpn_cfg.get("route_qbittorrent", True)),
            "route_prowlarr": bool(vpn_cfg.get("route_prowlarr")),
            "route_retreivr": bool(vpn_cfg.get("route_retreivr")),
        },
    }


def build_connections_status(config: dict[str, Any]) -> dict[str, Any]:
    arr_cfg = config.get("arr") if isinstance(config.get("arr"), dict) else {}
    stack = normalize_stack_config(config)
    result: dict[str, Any] = {}
    for service_name, endpoint in (
        ("radarr", "system/status"),
        ("sonarr", "system/status"),
        ("readarr", "system/status"),
        ("prowlarr", "system/status"),
    ):
        service_cfg = arr_cfg.get(service_name) if isinstance(arr_cfg.get(service_name), dict) else {}
        base_url = _trimmed(service_cfg.get("base_url")).rstrip("/")
        api_key = _trimmed(service_cfg.get("api_key"))
        if not base_url:
            result[service_name] = {"configured": False, "reachable": False, "status": "not_configured"}
            continue
        headers = {"X-Api-Key": api_key} if api_key else {}
        reachable, message = _probe_json(f"{base_url}/api/{_service_api_version(service_name)}/{endpoint}", headers=headers)
        target_path = {
            "radarr": str(stack.get("movies_root") or "./media/movies"),
            "sonarr": str(stack.get("tv_root") or "./media/tv"),
            "readarr": str(stack.get("books_root") or "./media/books"),
            "prowlarr": "",
        }.get(service_name, "")
        result[service_name] = {"configured": True, "reachable": reachable, "status": message, "base_url": base_url, "target_path": target_path}
    bazarr_cfg = arr_cfg.get("bazarr") if isinstance(arr_cfg.get("bazarr"), dict) else {}
    bazarr_base = _trimmed(bazarr_cfg.get("base_url")).rstrip("/")
    if bazarr_base:
        headers = {"X-Api-Key": _trimmed(bazarr_cfg.get("api_key"))} if _trimmed(bazarr_cfg.get("api_key")) else {}
        reachable, message = _probe_json(f"{bazarr_base}/api/system/status", headers=headers)
        result["bazarr"] = {"configured": True, "reachable": reachable, "status": message, "base_url": bazarr_base, "target_path": ""}
    else:
        result["bazarr"] = {"configured": False, "reachable": False, "status": "not_configured"}
    qb_cfg = arr_cfg.get("qbittorrent") if isinstance(arr_cfg.get("qbittorrent"), dict) else {}
    qb_base = _trimmed(qb_cfg.get("base_url")).rstrip("/")
    if qb_base:
        reachable, message = _probe_json(f"{qb_base}/api/v2/app/version")
        result["qbittorrent"] = {"configured": True, "reachable": reachable, "status": message, "base_url": qb_base, "download_root": _trimmed(qb_cfg.get("download_dir")) or str(stack.get("downloads_root") or "./downloads")}
    else:
        result["qbittorrent"] = {"configured": False, "reachable": False, "status": "not_configured"}
    jelly_cfg = arr_cfg.get("jellyfin") if isinstance(arr_cfg.get("jellyfin"), dict) else {}
    jelly_base = _trimmed(jelly_cfg.get("base_url")).rstrip("/")
    if jelly_base:
        headers = {"X-Emby-Token": _trimmed(jelly_cfg.get("api_key"))} if _trimmed(jelly_cfg.get("api_key")) else {}
        reachable, message = _probe_json(f"{jelly_base}/System/Info/Public", headers=headers)
        result["jellyfin"] = {"configured": True, "reachable": reachable, "status": message, "base_url": jelly_base, "target_path": str(stack.get("media_root") or "./media")}
    else:
        result["jellyfin"] = {"configured": False, "reachable": False, "status": "not_configured"}
    vpn_cfg = arr_cfg.get("vpn") if isinstance(arr_cfg.get("vpn"), dict) else {}
    control_url = _trimmed(vpn_cfg.get("control_url"))
    expected_routes = {
        "qbittorrent": bool(vpn_cfg.get("route_qbittorrent", True)),
        "prowlarr": bool(vpn_cfg.get("route_prowlarr")),
        "retreivr": bool(vpn_cfg.get("route_retreivr")),
    }
    if control_url:
        try:
            response = requests.get(control_url, timeout=4.0)
            if response.ok:
                body = response.text.strip()
                result["vpn"] = {
                    "configured": True,
                    "reachable": True,
                    "status": "reachable",
                    "base_url": control_url,
                    "provider": _trimmed(vpn_cfg.get("provider")) or "gluetun",
                    "external_ip": body[:128] if body else "",
                    "kill_switch_expected": bool(expected_routes.get("qbittorrent")),
                    "expected_routes": expected_routes,
                }
            else:
                result["vpn"] = {
                    "configured": True,
                    "reachable": False,
                    "status": f"http_{response.status_code}",
                    "base_url": control_url,
                    "provider": _trimmed(vpn_cfg.get("provider")) or "gluetun",
                    "kill_switch_expected": bool(expected_routes.get("qbittorrent")),
                    "expected_routes": expected_routes,
                }
        except Exception as exc:  # noqa: BLE001
            result["vpn"] = {
                "configured": True,
                "reachable": False,
                "status": exc.__class__.__name__,
                "base_url": control_url,
                "provider": _trimmed(vpn_cfg.get("provider")) or "gluetun",
                "kill_switch_expected": bool(expected_routes.get("qbittorrent")),
                "expected_routes": expected_routes,
            }
    else:
        result["vpn"] = {
            "configured": bool(vpn_cfg.get("enabled")),
            "reachable": False,
            "status": "not_configured",
            "provider": _trimmed(vpn_cfg.get("provider")) or "gluetun",
            "kill_switch_expected": bool(expected_routes.get("qbittorrent")),
            "expected_routes": expected_routes,
        }
    return result
