from __future__ import annotations

import importlib
import sys

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(sys, "version_info", (3, 11, 0, "final", 0), raising=False)
    monkeypatch.setattr(sys, "version", "3.11.9", raising=False)
    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()
    return TestClient(module.app)


def test_api_library_reconcile_returns_summary(monkeypatch) -> None:
    client = _build_client(monkeypatch)

    monkeypatch.setattr(
        "api.main.reconcile_library",
        lambda *, db_path, config: {
            "scan_roots": ["/downloads/Music"],
            "files_seen": 10,
            "audio_files_seen": 4,
            "video_files_seen": 2,
            "jobs_inserted": 3,
            "history_inserted": 3,
            "isrc_records_inserted": 2,
            "skipped_existing_jobs": 1,
            "skipped_missing_identity": 0,
            "skipped_unsupported": 6,
            "errors": 0,
        },
    )

    response = client.post("/api/library/reconcile")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["jobs_inserted"] == 3
    assert payload["history_inserted"] == 3
    assert payload["isrc_records_inserted"] == 2
    assert payload["video_files_seen"] == 2
