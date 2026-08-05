from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from engine import stack_setup


def test_build_stack_preflight_reports_port_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    stack = {
        "enable_arr_stack": True,
        "enable_radarr": True,
        "enable_sonarr": False,
        "enable_readarr": False,
        "enable_prowlarr": False,
        "enable_bazarr": False,
        "enable_qbittorrent": False,
        "enable_vpn": False,
        "enable_jellyfin": False,
        "enable_hostctl": False,
    }
    monkeypatch.setattr(stack_setup, "_docker_compose_available", lambda: (True, "ok"))
    monkeypatch.setattr(stack_setup, "_resolve_compose_file", lambda _root: Path("/tmp/docker-compose.yml"))
    monkeypatch.setattr(stack_setup, "_is_local_port_available", lambda port: int(port) != 7878)

    preflight = stack_setup.build_stack_preflight({}, stack, project_dir="/tmp")

    assert preflight["ok"] is False
    assert any(item.get("type") == "port_conflict" and int(item.get("host_port") or 0) == 7878 for item in preflight["conflicts"])
    assert preflight["checks"]["docker_compose"]["ok"] is True


def test_build_stack_preflight_reports_managed_port_overlap(monkeypatch: pytest.MonkeyPatch) -> None:
    stack = {
        "enable_arr_stack": True,
        "enable_radarr": True,
        "enable_vpn": True,
    }
    monkeypatch.setattr(stack_setup, "_docker_compose_available", lambda: (True, "ok"))
    monkeypatch.setattr(stack_setup, "_resolve_compose_file", lambda _root: Path("/tmp/docker-compose.yml"))
    monkeypatch.setattr(stack_setup, "_is_local_port_available", lambda _port: True)
    monkeypatch.setattr(
        stack_setup,
        "PROFILE_HOST_PORTS",
        {
            "arr": [{"service": "radarr", "host_port": 7878, "container_port": 7878}],
            "vpn": [{"service": "gluetun", "host_port": 7878, "container_port": 8080}],
        },
    )

    preflight = stack_setup.build_stack_preflight({}, stack, project_dir="/tmp")

    assert preflight["ok"] is False
    assert any(item.get("type") == "port_overlap" and item.get("services") == ["radarr", "gluetun"] for item in preflight["conflicts"])


def test_wireguard_config_is_preserved_exported_and_preflighted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "provider.conf"
    config_path.write_text("[Interface]\nPrivateKey = secret\n", encoding="utf-8")
    plan = stack_setup.normalize_managed_plan(
        {},
        {"vpn": True, "vpn_wireguard_config_path": str(config_path)},
    )
    configured = stack_setup.apply_managed_service_defaults({}, plan)
    stack = dict(plan["stack"])

    assert configured["arr"]["vpn"]["wireguard_config_path"] == str(config_path)
    assert configured["arr"]["vpn"]["provider"] == "custom"
    env = stack_setup.managed_env_values(configured, stack)
    assert env["GLUETUN_WIREGUARD_CONFIG"] == str(config_path)
    assert env["GLUETUN_VPN_TYPE"] == "wireguard"

    monkeypatch.setattr(stack_setup, "_docker_compose_available", lambda: (True, "ok"))
    monkeypatch.setattr(stack_setup, "_resolve_compose_file", lambda _root: Path("/tmp/docker-compose.yml"))
    monkeypatch.setattr(stack_setup, "_is_local_port_available", lambda _port: True)
    preflight = stack_setup.build_stack_preflight(configured, stack, project_dir=tmp_path)
    assert preflight["ok"] is True
    assert preflight["checks"]["wireguard_config"]["status"] == "ready"


def test_wireguard_config_preflight_rejects_missing_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = {
        "arr": {"vpn": {"wireguard_config_path": str(tmp_path / "missing.conf")}},
        "setup": {"stack": {"enable_vpn": True}},
    }
    stack = stack_setup.normalize_stack_config(config)
    monkeypatch.setattr(stack_setup, "_docker_compose_available", lambda: (True, "ok"))
    monkeypatch.setattr(stack_setup, "_resolve_compose_file", lambda _root: Path("/tmp/docker-compose.yml"))
    monkeypatch.setattr(stack_setup, "_is_local_port_available", lambda _port: True)

    preflight = stack_setup.build_stack_preflight(config, stack, project_dir=tmp_path)

    assert preflight["ok"] is False
    assert any(item.get("type") == "wireguard_config_invalid" for item in preflight["conflicts"])


def test_build_connections_status_includes_reason_and_state() -> None:
    services = stack_setup.build_connections_status({})
    assert isinstance(services, dict)
    assert "radarr" in services
    assert services["radarr"]["reason_code"] == "not_configured"
    assert services["radarr"]["state"] == "unknown"
    assert "retry_hint" in services["radarr"]


def _build_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[TestClient, object]:
    import engine.core  # noqa: F401
    import threading

    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.worker_engine = SimpleNamespace(store=object())
    module.app.state.search_service = None
    module.app.state.paths = SimpleNamespace(db_path=str(tmp_path / "db.sqlite3"), single_downloads_dir=str(tmp_path / "downloads"))
    module.app.state.playlist_import_jobs = {}
    module.app.state.playlist_import_jobs_lock = threading.Lock()
    module.app.state.config_path = str(tmp_path / "config.json")
    module.app.state.loaded_config = {
        "setup": {
            "stack": {
                "enable_arr_stack": False,
                "enable_radarr": False,
                "enable_sonarr": False,
                "enable_readarr": False,
                "enable_prowlarr": False,
                "enable_bazarr": False,
                "enable_qbittorrent": False,
                "enable_vpn": False,
                "enable_jellyfin": False,
            }
        },
        "arr": {},
    }

    def _persist(payload: dict) -> dict:
        module.app.state.loaded_config = payload
        return payload

    monkeypatch.setattr(module, "_persist_config_payload", _persist)
    return TestClient(module.app), module


def test_setup_preflight_endpoint_returns_structured_payload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    client, module = _build_client(monkeypatch, tmp_path)
    monkeypatch.setattr(
        module,
        "build_stack_preflight",
        lambda *_args, **_kwargs: {"ok": True, "conflicts": [], "warnings": [], "fix_hints": [], "checks": {"docker_compose": {"ok": True}}},
    )

    response = client.get("/api/setup/preflight")
    assert response.status_code == 200
    payload = response.json()
    assert payload["preflight"]["ok"] is True
    assert "checks" in payload["preflight"]


def test_setup_preflight_post_uses_draft_stack_payload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    client, module = _build_client(monkeypatch, tmp_path)

    def _preflight(_cfg: dict, stack: dict, **_kwargs):
        return {"ok": False if stack.get("enable_radarr") else True, "conflicts": [{"type": "port_conflict"}] if stack.get("enable_radarr") else [], "warnings": [], "fix_hints": [], "checks": {}}

    monkeypatch.setattr(module, "build_stack_preflight", _preflight)

    response = client.post("/api/setup/preflight", json={"stack": {"enable_radarr": True, "enable_arr_stack": True}})
    assert response.status_code == 200
    payload = response.json()
    assert payload["stack"]["enable_radarr"] is True
    assert payload["preflight"]["ok"] is False


def test_existing_connect_requires_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    client, _module = _build_client(monkeypatch, tmp_path)
    response = client.post(
        "/api/setup/existing/connect",
        json={
            "radarr": {"enabled": True, "base_url": "http://radarr:7878", "api_key": ""},
        },
    )
    assert response.status_code == 400
    detail = response.json().get("detail") or {}
    assert detail.get("error") == "validation_failed"
    assert any(item.get("service") == "radarr" and item.get("field") == "api_key" for item in detail.get("errors") or [])


def test_managed_apply_blocks_on_failed_preflight(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    client, module = _build_client(monkeypatch, tmp_path)
    module.app.state.loaded_config["setup"]["managed_stack"] = {
        "enabled_features": {"movies": True},
        "direct_manage_requested": False,
    }
    monkeypatch.setattr(module, "build_stack_preflight", lambda *_args, **_kwargs: {"ok": False, "conflicts": [{"type": "port_conflict"}], "warnings": [], "fix_hints": [], "checks": {}})

    response = client.post("/api/setup/managed/apply")
    assert response.status_code == 409
    detail = response.json().get("detail") or {}
    assert detail.get("error") == "preflight_failed"


def test_managed_apply_direct_mode_requires_reachable_hostctl(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    client, module = _build_client(monkeypatch, tmp_path)
    module.app.state.loaded_config["setup"]["managed_stack"] = {
        "enabled_features": {"movies": True},
        "direct_manage_requested": True,
    }
    monkeypatch.setattr(module, "build_stack_preflight", lambda *_args, **_kwargs: {"ok": True, "conflicts": [], "warnings": [], "fix_hints": [], "checks": {}})
    monkeypatch.setattr(module, "write_managed_env_block", lambda *_args, **_kwargs: tmp_path / ".env")

    def _hostctl_request(method: str, *_args, **_kwargs):
        if method.upper() == "GET":
            raise RuntimeError("hostctl_down")
        return {"status": "ok"}

    monkeypatch.setattr(module, "_hostctl_request", _hostctl_request)

    response = client.post("/api/setup/managed/apply")
    assert response.status_code == 409
    detail = response.json().get("detail") or {}
    assert detail.get("error") == "hostctl_unreachable"


def test_services_health_response_includes_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    client, module = _build_client(monkeypatch, tmp_path)
    monkeypatch.setattr(
        module,
        "_service_health_summary",
        lambda *_args, **_kwargs: {
            "radarr": {"state": "connected", "reachable": True, "configured": True},
            "sonarr": {"state": "needs_attention", "reachable": False, "configured": True},
            "readarr": {"state": "failed", "reachable": False, "configured": True},
            "prowlarr": {"state": "unknown", "reachable": False, "configured": False},
        },
    )

    response = client.get("/api/services/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"] == {"connected": 1, "needs_attention": 1, "failed": 1, "unknown": 1}
