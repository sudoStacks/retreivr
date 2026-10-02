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


# Facebook video titles carry a "<views> views · <reactions> reactions "
# prefix plus whatever emoji the original poster used. Neither is filtered by
# _safe_filename (which only strips quotes/newlines), so this is the exact
# shape of filename that reached the download endpoint and 500'd.
EMOJI_FILENAME = "4.8M views · 2.2K reactions Very accurate ☠️\U0001f602 It's FOSS.mkv"


def test_content_disposition_ascii_filename(monkeypatch, tmp_path):
    client, module, _ = _build_client(monkeypatch, tmp_path)
    header = module._content_disposition("Track 01.mp3")
    header.encode("latin-1")  # must not raise
    assert header == "attachment; filename=\"Track 01.mp3\"; filename*=UTF-8''Track%2001.mp3"


def test_content_disposition_emoji_filename_is_latin1_safe(monkeypatch, tmp_path):
    client, module, _ = _build_client(monkeypatch, tmp_path)
    header = module._content_disposition(EMOJI_FILENAME)
    header.encode("latin-1")  # this previously raised UnicodeEncodeError


def test_content_disposition_emoji_filename_round_trips_via_rfc5987(monkeypatch, tmp_path):
    from urllib.parse import unquote

    client, module, _ = _build_client(monkeypatch, tmp_path)
    name = "Café \U0001f600.mkv"
    header = module._content_disposition(name)
    star_param = next(p for p in header.split("; ") if p.startswith("filename*="))
    encoded = star_param[len("filename*=UTF-8''"):]
    assert unquote(encoded) == name


def test_content_disposition_blank_ascii_fallback_uses_download(monkeypatch, tmp_path):
    client, module, _ = _build_client(monkeypatch, tmp_path)
    assert '"download"' in module._content_disposition("\U0001f600")


def test_download_endpoint_serves_emoji_named_file_without_500(monkeypatch, tmp_path):
    # Reproduces the exact user-facing bug: a completed Facebook download
    # whose title (and therefore filename) contains emoji 500'd when clicking
    # "download file" in the UI, even though the download itself had already
    # succeeded and the file was sitting on disk.
    client, module, downloads_dir = _build_client(monkeypatch, tmp_path)
    target = tmp_path / "downloads" / "Singles" / EMOJI_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"not a real video")
    file_id = module._encode_file_id(os.path.relpath(str(target), downloads_dir))

    response = client.get(f"/api/files/{file_id}/download")

    assert response.status_code == 200
    assert response.content == b"not a real video"
    assert "filename*=UTF-8''" in response.headers["content-disposition"]
