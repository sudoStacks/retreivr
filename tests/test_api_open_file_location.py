from __future__ import annotations

import importlib
import os
import sys
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def _build_client(monkeypatch: pytest.MonkeyPatch, tmp_path) -> tuple[TestClient, object, str]:
    import engine.core  # noqa: F401

    sys.modules.pop("api.main", None)
    module = importlib.import_module("api.main")
    module.app.router.on_startup.clear()
    module.app.router.on_shutdown.clear()

    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(module, "DOWNLOADS_DIR", str(downloads_dir))
    module.app.state.browse_roots = {"downloads": str(downloads_dir)}
    module.app.state.paths = SimpleNamespace(
        db_path=str(tmp_path / "test.db"),
        single_downloads_dir=str(downloads_dir),
    )
    return TestClient(module.app), module, str(downloads_dir)


def test_api_files_open_location_uses_file_id_and_selects_file(monkeypatch, tmp_path) -> None:
    client, module, downloads_dir = _build_client(monkeypatch, tmp_path)
    target = tmp_path / "downloads" / "Artist" / "Track.mp3"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"audio")
    file_id = module._encode_file_id(os.path.relpath(str(target), downloads_dir))

    captured: dict[str, object] = {}

    def _fake_launch(path: str, *, select_file: bool) -> None:
        captured["path"] = path
        captured["select_file"] = select_file

    monkeypatch.setattr(module, "_launch_os_location", _fake_launch)

    response = client.post("/api/files/open-location", json={"file_id": file_id})
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["select_file"] is True
    assert captured["select_file"] is True
    assert captured["path"] == os.path.realpath(str(target))


def test_api_files_open_location_rejects_path_outside_allowed_roots(monkeypatch, tmp_path) -> None:
    client, module, _downloads_dir = _build_client(monkeypatch, tmp_path)
    outside = tmp_path / "outside" / "video.mp4"
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.write_bytes(b"video")

    monkeypatch.setattr(module, "_launch_os_location", lambda *_args, **_kwargs: None)

    response = client.post("/api/files/open-location", json={"path": str(outside)})
    assert response.status_code == 403
    assert response.json()["detail"] == "Path not allowed"


def test_api_files_open_location_accepts_directory_path(monkeypatch, tmp_path) -> None:
    client, module, downloads_dir = _build_client(monkeypatch, tmp_path)
    target_dir = tmp_path / "downloads" / "Music" / "Artist"
    target_dir.mkdir(parents=True, exist_ok=True)

    captured: dict[str, object] = {}

    def _fake_launch(path: str, *, select_file: bool) -> None:
        captured["path"] = path
        captured["select_file"] = select_file

    monkeypatch.setattr(module, "_launch_os_location", _fake_launch)

    response = client.post("/api/files/open-location", json={"path": os.path.relpath(str(target_dir), downloads_dir)})
    assert response.status_code == 200
    assert response.json()["select_file"] is False
    assert captured["path"] == os.path.realpath(str(target_dir))
    assert captured["select_file"] is False
