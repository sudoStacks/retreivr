from __future__ import annotations

import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest

fastapi = pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")
requests = pytest.importorskip("requests")
playwright_sync = pytest.importorskip("playwright.sync_api")

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
import uvicorn


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def _build_webui_test_app() -> FastAPI:
    app = FastAPI()
    state: dict[str, Any] = {
        "request_id": "req-1",
        "item_id": "item-1",
        "candidate_id": "cand-1",
        "job_id": "job-1",
        "request_status": "completed",
        "job_status": "",
    }

    @app.get("/api/paths")
    def api_paths() -> dict[str, str]:
        return {"config_dir": "config", "downloads_dir": "downloads", "tokens_dir": "tokens"}

    @app.get("/api/version")
    def api_version() -> dict[str, str]:
        return {"app_version": "0.0.0-test"}

    @app.get("/api/status")
    def api_status() -> dict[str, Any]:
        return {
            "running": False,
            "run_id": None,
            "started_at": None,
            "finished_at": None,
            "watcher": {"enabled": False, "paused": False},
            "scheduler": {"enabled": False},
            "status": {"run_successes": [], "run_failures": []},
            "watcher_status": {"state": "idle", "pending_playlists_count": 0, "batch_active": False},
        }

    @app.get("/api/spotify/status")
    def api_spotify_status() -> dict[str, Any]:
        return {"oauth_connected": False}

    @app.get("/api/metrics")
    def api_metrics() -> dict[str, Any]:
        return {
            "downloads_files": 0,
            "downloads_bytes": 0,
            "disk_free_bytes": 1024 * 1024 * 1024,
            "disk_total_bytes": 2 * 1024 * 1024 * 1024,
            "disk_free_percent": 50,
        }

    @app.get("/api/schedule")
    def api_schedule() -> dict[str, Any]:
        return {"schedule": {"enabled": False, "interval_hours": 6, "run_on_startup": False}}

    @app.get("/api/logs")
    def api_logs() -> PlainTextResponse:
        return PlainTextResponse("ok\n")

    @app.get("/api/files")
    def api_files() -> list[dict[str, Any]]:
        return []

    @app.get("/api/history")
    def api_history() -> list[dict[str, Any]]:
        return []

    @app.get("/api/download_jobs")
    def api_download_jobs(limit: int = 50) -> dict[str, Any]:
        if state["job_status"]:
            return {
                "jobs": [
                    {
                        "id": state["job_id"],
                        "origin": "search",
                        "origin_id": state["request_id"],
                        "url": "https://www.youtube.com/watch?v=stub123",
                        "status": state["job_status"],
                    }
                ]
            }
        return {"jobs": []}

    @app.post("/api/search/requests")
    def api_create_search_request(_payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "request_id": state["request_id"],
            "music_mode": False,
            "music_resolution": None,
            "music_candidates": [],
        }

    @app.get("/api/search/requests")
    def api_list_search_requests() -> dict[str, Any]:
        return {
            "requests": [
                {
                    "id": state["request_id"],
                    "status": state["request_status"],
                    "created_at": "2026-01-01T00:00:00Z",
                    "media_type": "video",
                }
            ]
        }

    @app.get("/api/search/requests/{request_id}")
    def api_get_search_request(request_id: str) -> dict[str, Any]:
        return {
            "request": {
                "id": request_id,
                "status": state["request_status"],
                "media_type": "video",
                "resolved_destination": "downloads",
            },
            "items": [
                {
                    "id": state["item_id"],
                    "request_id": state["request_id"],
                    "status": "candidate_found",
                    "candidate_count": 1,
                    "media_type": "video",
                    "position": 1,
                    "allow_download": True,
                }
            ],
        }

    @app.get("/api/search/items/{item_id}/candidates")
    def api_get_candidates(item_id: str) -> dict[str, Any]:
        if item_id != state["item_id"]:
            return {"candidates": []}
        return {
            "candidates": [
                {
                    "id": state["candidate_id"],
                    "url": "https://www.youtube.com/watch?v=stub123",
                    "title": "Smoke Candidate",
                    "source": "youtube",
                    "allow_download": True,
                    "final_score": 99,
                    "job_status": state["job_status"] or None,
                }
            ]
        }

    @app.post("/api/search/items/{item_id}/enqueue")
    def api_enqueue_candidate(item_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        if item_id != state["item_id"] or payload.get("candidate_id") != state["candidate_id"]:
            return {"created": False, "job_id": None}
        state["job_status"] = "queued"
        return {"created": True, "job_id": state["job_id"]}

    @app.post("/api/search/resolve/once")
    def api_resolve_once() -> dict[str, Any]:
        return {"request_id": state["request_id"]}

    @app.post("/api/import/playlist")
    async def api_import_playlist() -> dict[str, Any]:
        return {
            "total_tracks": 4,
            "resolved": 3,
            "unresolved": 1,
            "enqueued": 3,
            "failed": 0,
        }

    @app.get("/api/search/queue")
    def api_search_queue() -> dict[str, Any]:
        return {"jobs": []}

    app.mount("/", StaticFiles(directory=str(Path("webUI").resolve()), html=True), name="webui")
    return app


@pytest.fixture()
def webui_server() -> str:
    app = _build_webui_test_app()
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config=config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            response = requests.get(base_url, timeout=0.25)
            if response.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        server.should_exit = True
        thread.join(timeout=5)
        pytest.fail("Failed to start local FastAPI test server for WebUI smoke test.")

    try:
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.fixture()
def page():
    with sync_playwright() as pw:
        try:
            browser = pw.chromium.launch(headless=True)
        except PlaywrightError as exc:
            pytest.skip(f"Playwright browser not available: {exc}")
        context = browser.new_context()
        page = context.new_page()
        try:
            yield page
        finally:
            context.close()
            browser.close()


def test_webui_home_search_download_status_without_legacy_run_errors(webui_server: str, page) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    page.goto(webui_server, wait_until="networkidle")
    page.fill("#home-search-input", "smoke test query")
    page.click("#home-search-only")

    page.wait_for_selector("#home-results .home-result-card", timeout=10000)
    page.wait_for_selector('button[data-action="home-download"]', timeout=10000)
    page.click('button[data-action="home-download"]')

    page.wait_for_function(
        """() => {
          const el = document.querySelector("#home-search-message");
          return !!el && /Enqueued job/i.test(el.textContent || "");
        }""",
        timeout=10000,
    )
    page.wait_for_function(
        """() => {
          const state = document.querySelector(".home-candidate-state");
          return !!state && /queued/i.test(state.textContent || "");
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"
    assert not any("legacy-run" in msg.lower() or "#run-" in msg.lower() for msg in console_errors)


def test_webui_home_import_playlist_smoke(webui_server: str, page, tmp_path: Path) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    import_file = tmp_path / "playlist.m3u"
    import_file.write_text("#EXTM3U\n#EXTINF:123,Artist - Title\ntrack.mp3\n", encoding="utf-8")

    page.goto(webui_server, wait_until="networkidle")
    page.set_input_files("#home-import-file", str(import_file))
    page.click("#home-import-button")
    page.wait_for_function(
        """() => {
          const el = document.querySelector("#home-import-summary");
          const text = (el && el.textContent) || "";
          return text.includes("Total: 4")
            && text.includes("Resolved: 3")
            && text.includes("Enqueued: 3")
            && text.includes("Unresolved: 1");
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"
