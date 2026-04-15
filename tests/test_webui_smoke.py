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
        "run_id": "run-1",
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
    def api_status(run_id: str | None = None) -> dict[str, Any]:
        return {
            "running": False,
            "run_id": run_id or state["run_id"],
            "started_at": None,
            "finished_at": None,
            "state": "completed" if run_id else "idle",
            "status": {
                "client_delivery_id": "delivery-1" if run_id else None,
                "client_delivery_filename": "direct-preview.mp3" if run_id else None,
            } if run_id else {"run_successes": [], "run_failures": []},
            "watcher": {"enabled": False, "paused": False},
            "scheduler": {"enabled": False},
            "watcher_status": {"state": "idle", "pending_playlists_count": 0, "batch_active": False},
        }

    @app.post("/api/run", status_code=202)
    def api_run(_payload: dict[str, Any]) -> dict[str, Any]:
        return {"run_id": state["run_id"], "status": "started"}

    @app.post("/api/direct-url/resolve")
    def api_direct_url_resolve(payload: dict[str, Any]) -> dict[str, Any]:
        url = str(payload.get("url") or "https://www.youtube.com/watch?v=stub123")
        media_mode = str(payload.get("media_mode") or "video")
        is_playlist = "list=" in url
        preview = {
            "title": "Stub Direct Preview",
            "uploader": "Stub Channel",
            "thumbnail_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
            "url": url,
            "source": "youtube",
            "duration_sec": 123,
        }
        if is_playlist:
            preview = {
                "playlist_title": "Stub Playlist",
                "thumbnail_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
                "first_video_id": "stub123",
                "url": url,
                "source": "youtube",
            }
            if media_mode in {"music", "music_video"}:
                return {
                    "result_type": "music_album",
                    "playlist_id": "PLstub",
                    "preview": preview,
                    "music_album": {
                        "title": "Stub Playlist",
                        "artist": "YouTube Playlist",
                        "artwork_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
                        "direct_playlist_url": url,
                        "playlist_id": "PLstub",
                        "first_video_id": "stub123",
                        "is_direct_url_result": True,
                    },
                }
            return {
                "result_type": "home_result",
                "playlist_id": "PLstub",
                "preview": preview,
                "home_item": {
                    "status": "candidate_found",
                    "allow_download": True,
                    "media_type": "video",
                    "artist": "Stub Channel",
                    "album": None,
                    "track": "Stub Playlist",
                    "duration_sec": None,
                    "transient_kind": "playlist_url",
                    "source_url": url,
                },
                "home_candidates": [
                    {
                        "title": "Stub Playlist",
                        "artist_detected": "Stub Channel",
                        "album_detected": None,
                        "track_detected": None,
                        "final_score": None,
                        "source": "youtube",
                        "url": url,
                        "thumbnail_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
                        "allow_download": True,
                        "job_status": "",
                        "duration_sec": None,
                        "playlist_id": "PLstub",
                    }
                ],
            }
        if media_mode in {"music", "music_video"}:
            return {
                "result_type": "music_track",
                "preview": preview,
                "music_track": {
                    "direct_result_key": f"direct:{url}",
                    "direct_url": url,
                    "source_url": url,
                    "source": "youtube",
                    "track": "Stub Direct Preview",
                    "artist": "Stub Channel",
                    "album": "",
                    "duration_ms": 123000,
                    "artwork_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
                    "media_mode": media_mode,
                    "is_direct_url_result": True,
                },
            }
        return {
            "result_type": "home_result",
            "preview": preview,
            "home_item": {
                "status": "candidate_found",
                "allow_download": True,
                "media_type": "video",
                "artist": "Stub Channel",
                "album": None,
                "track": "Stub Direct Preview",
                "duration_sec": 123,
                "transient_kind": "direct_url",
                "source_url": url,
            },
            "home_candidates": [
                {
                    "title": "Stub Direct Preview",
                    "artist_detected": "Stub Channel",
                    "album_detected": None,
                    "track_detected": None,
                    "final_score": None,
                    "source": "youtube",
                    "url": url,
                    "thumbnail_url": "https://i.ytimg.com/vi/stub123/hqdefault.jpg",
                    "allow_download": True,
                    "job_status": "",
                    "duration_sec": 123,
                    "playlist_id": None,
                }
            ],
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


def test_webui_home_direct_url_preview_uses_standard_result_card(webui_server: str, page) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    page.goto(webui_server, wait_until="networkidle")
    page.fill("#home-search-input", "https://www.youtube.com/watch?v=stub123")
    page.click("#home-search-only")

    page.wait_for_selector("#home-results .home-result-card", timeout=10000)
    page.wait_for_selector("#home-results .home-candidate-row", timeout=10000)
    page.wait_for_selector('#home-results button[data-action="home-download"]', timeout=10000)
    page.wait_for_function(
        """() => {
          const text = document.querySelector("#home-results .home-result-card strong")?.textContent || "";
          return /Stub Direct Preview/.test(text);
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"


def test_webui_music_direct_url_preview_uses_music_card(webui_server: str, page) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    page.goto(f"{webui_server}#music", wait_until="networkidle")
    page.fill("#music-header-query", "https://www.youtube.com/watch?v=stub123")
    page.click("#music-header-submit")

    page.wait_for_selector("#music-results-container .home-result-card", timeout=10000)
    page.wait_for_selector("#music-results-container .music-download-btn", timeout=10000)
    page.wait_for_function(
        """() => {
          const text = document.querySelector("#music-results-container .home-candidate-title")?.textContent || "";
          return /Stub Direct Preview/.test(text);
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"
