from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@pytest.fixture()
def api_module(tmp_path: Path, monkeypatch):
    import engine.core  # noqa: F401

    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    module.app.state.paths = SimpleNamespace(db_path=str(tmp_path / "retreivr.sqlite3"))
    module.app.state.loaded_config = {}
    module.app.state.config = {}
    monkeypatch.setattr(module, "_read_config_or_404", lambda: {})
    return module


def _capture_prefs(api_module, monkeypatch) -> dict:
    saved: dict = {}
    monkeypatch.setattr(api_module, "_current_loaded_config", lambda: {})
    monkeypatch.setattr(api_module, "_persist_config_payload", lambda payload: saved.update(payload))
    return saved


def test_video_results_limit_preference_is_persisted(api_module, monkeypatch) -> None:
    saved = _capture_prefs(api_module, monkeypatch)
    client = TestClient(api_module.app)

    resp = client.put("/api/config/ui-preferences", json={"home_video_results_limit": 48})

    assert resp.status_code == 200
    assert resp.json()["home_video_results_limit"] == 48
    assert saved["ui_preferences"]["home_video_results_limit"] == 48


def test_video_results_limit_all_option_is_persisted_as_zero(api_module, monkeypatch) -> None:
    saved = _capture_prefs(api_module, monkeypatch)
    client = TestClient(api_module.app)

    resp = client.put("/api/config/ui-preferences", json={"home_video_results_limit": 0})

    assert resp.status_code == 200
    assert resp.json()["home_video_results_limit"] == 0
    assert saved["ui_preferences"]["home_video_results_limit"] == 0


def test_video_results_limit_out_of_range_is_rejected(api_module, monkeypatch) -> None:
    _capture_prefs(api_module, monkeypatch)
    client = TestClient(api_module.app)

    resp = client.put("/api/config/ui-preferences", json={"home_video_results_limit": 500})

    assert resp.status_code == 400
