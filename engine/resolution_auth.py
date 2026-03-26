from __future__ import annotations

import hmac
from typing import Any


def _normalize_nodes(config: dict[str, Any] | None) -> list[dict[str, str]]:
    cfg = config if isinstance(config, dict) else {}
    resolution_cfg = cfg.get("resolution_api") if isinstance(cfg.get("resolution_api"), dict) else {}
    raw_nodes = resolution_cfg.get("nodes") if isinstance(resolution_cfg.get("nodes"), list) else []
    nodes: list[dict[str, str]] = []
    for item in raw_nodes:
        if not isinstance(item, dict):
            continue
        node_id = str(item.get("node_id") or "").strip()
        api_key = str(item.get("api_key") or "").strip()
        if node_id and api_key:
            nodes.append({"node_id": node_id, "api_key": api_key})
    return nodes


def resolve_node_auth(
    config: dict[str, Any] | None,
    *,
    provided_key: str | None,
    provided_node_id: str | None = None,
    allow_anonymous: bool = False,
) -> dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    resolution_cfg = cfg.get("resolution_api") if isinstance(cfg.get("resolution_api"), dict) else {}
    require_api_key = bool(resolution_cfg.get("require_api_key", False))
    configured_nodes = _normalize_nodes(cfg)
    key = str(provided_key or "").strip()
    requested_node = str(provided_node_id or "").strip()

    if not configured_nodes and not require_api_key:
        return {
            "authenticated": False,
            "node_id": requested_node or None,
            "mode": "anonymous",
        }

    if not key:
        if allow_anonymous and not require_api_key:
            return {
                "authenticated": False,
                "node_id": requested_node or None,
                "mode": "anonymous",
            }
        raise ValueError("api_key_required")

    for node in configured_nodes:
        if hmac.compare_digest(key, node["api_key"]):
            node_id = node["node_id"]
            if requested_node and requested_node != node_id:
                raise ValueError("node_id_mismatch")
            return {
                "authenticated": True,
                "node_id": node_id,
                "mode": "api_key",
            }

    raise ValueError("invalid_api_key")
