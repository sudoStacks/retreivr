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
    silent_data_uri = (
        "data:audio/wav;base64,"
        "UklGRiQAAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQAAAAA="
    )

    def _library_summary_payload(version: int = 1) -> dict[str, Any]:
        artists = [
            {"artist": "Artist One", "artist_key": "artist one", "album_count": 2 if version == 1 else 1, "track_count": 3 if version == 1 else 1, "artwork_url": ""},
            {"artist": "Artist Two", "artist_key": "artist two", "album_count": 1, "track_count": 1, "artwork_url": ""},
        ]
        if version == 1:
            albums = [
                {"artist": "Artist One", "artist_key": "artist one", "album": "Alpha Album", "album_key": "alpha album", "track_count": 2, "artwork_url": ""},
                {"artist": "Artist One", "artist_key": "artist one", "album": "Beta Album", "album_key": "beta album", "track_count": 1, "artwork_url": ""},
                {"artist": "Artist Two", "artist_key": "artist two", "album": "Gamma Album", "album_key": "gamma album", "track_count": 1, "artwork_url": ""},
            ]
            tracks = [
                {"id": "t1", "title": "Alpha Track 1", "artist": "Artist One", "artist_key": "artist one", "album": "Alpha Album", "album_key": "alpha album", "recording_mbid": "rec-alpha", "stream_url": silent_data_uri, "artwork_url": ""},
                {"id": "t2", "title": "Alpha Track 2", "artist": "Artist One", "artist_key": "artist one", "album": "Alpha Album", "album_key": "alpha album", "recording_mbid": "rec-alpha-2", "stream_url": silent_data_uri, "artwork_url": ""},
                {"id": "t3", "title": "Beta Track", "artist": "Artist One", "artist_key": "artist one", "album": "Beta Album", "album_key": "beta album", "recording_mbid": "rec-beta", "stream_url": silent_data_uri, "artwork_url": ""},
                {"id": "t4", "title": "Gamma Track", "artist": "Artist Two", "artist_key": "artist two", "album": "Gamma Album", "album_key": "gamma album", "recording_mbid": "rec-gamma", "stream_url": silent_data_uri, "artwork_url": ""},
            ]
        else:
            albums = [
                {"artist": "Artist One", "artist_key": "artist one", "album": "Beta Album", "album_key": "beta album", "track_count": 1, "artwork_url": ""},
                {"artist": "Artist Two", "artist_key": "artist two", "album": "Gamma Album", "album_key": "gamma album", "track_count": 1, "artwork_url": ""},
            ]
            tracks = [
                {"id": "t3", "title": "Beta Track", "artist": "Artist One", "artist_key": "artist one", "album": "Beta Album", "album_key": "beta album", "recording_mbid": "rec-beta", "stream_url": silent_data_uri, "artwork_url": ""},
                {"id": "t4", "title": "Gamma Track", "artist": "Artist Two", "artist_key": "artist two", "album": "Gamma Album", "album_key": "gamma album", "recording_mbid": "rec-gamma", "stream_url": silent_data_uri, "artwork_url": ""},
            ]
        return {"artists": artists, "albums": albums, "tracks": tracks}

    state: dict[str, Any] = {
        "request_id": "req-1",
        "item_id": "item-1",
        "candidate_id": "cand-1",
        "job_id": "job-1",
        "run_id": "run-1",
        "request_status": "completed",
        "job_status": "",
        "library_summary_version": 1,
        "import_job_id": "imp-1",
        "setup_preflight_conflict": True,
        "config": {
            "arr": {
                "tmdb_api_key": "",
                "vpn": {"enabled": False, "provider": "gluetun"},
                "jellyfin": {"base_url": "", "api_key": ""},
                "radarr": {"base_url": "", "api_key": ""},
                "sonarr": {"base_url": "", "api_key": ""},
                "readarr": {"base_url": "", "api_key": ""},
                "prowlarr": {"base_url": "", "api_key": ""},
                "bazarr": {"base_url": "", "api_key": ""},
                "qbittorrent": {"base_url": "", "username": "", "password": ""},
            },
            "telegram": {"bot_token": "", "chat_id": ""},
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
                    "enable_hostctl": False,
                    "env_path": ".env",
                    "media_root": "./media",
                    "movies_root": "./media/movies",
                    "tv_root": "./media/tv",
                    "downloads_root": "./downloads",
                    "books_root": "./media/books",
                },
                "service_management": {"mode": "none", "apply_mode": "manual"},
            },
        },
    }

    def _preflight_payload(stack: dict[str, Any] | None = None) -> dict[str, Any]:
        current = stack or (((state.get("config") or {}).get("setup") or {}).get("stack") or {})
        conflict = bool(state.get("setup_preflight_conflict")) and bool(current.get("enable_radarr")) is False
        if conflict:
            return {
                "ok": False,
                "conflicts": [{"type": "port_conflict", "message": "Host port 7878 is already in use.", "host_port": 7878}],
                "warnings": [],
                "fix_hints": ["Stop the process using port 7878 or remap Radarr host port."],
                "checks": {"docker_compose": {"ok": True, "status": "ok"}, "compose_file": {"ok": True, "path": "docker/docker-compose.yml.example"}},
            }
        return {
            "ok": True,
            "conflicts": [],
            "warnings": [],
            "fix_hints": [],
            "checks": {"docker_compose": {"ok": True, "status": "ok"}, "compose_file": {"ok": True, "path": "docker/docker-compose.yml.example"}},
        }

    @app.get("/api/paths")
    def api_paths() -> dict[str, str]:
        return {"config_dir": "config", "downloads_dir": "downloads", "tokens_dir": "tokens"}

    @app.get("/api/version")
    def api_version() -> dict[str, str]:
        return {"app_version": "0.0.0-test"}

    @app.get("/api/config")
    def api_config_get() -> dict[str, Any]:
        return dict(state["config"])

    @app.put("/api/config")
    def api_config_put(payload: dict[str, Any]) -> dict[str, Any]:
        state["config"] = payload
        return payload

    @app.get("/api/setup/status")
    def api_setup_status() -> dict[str, Any]:
        cfg = state["config"]
        stack = dict((((cfg.get("setup") or {}).get("stack")) or {}))
        stack.setdefault("compose_profiles", [])
        stack.setdefault("compose_command", "docker compose up -d")
        stack.setdefault("restart_required", False)
        return {
            "modules": {
                "core": {"status": "verified", "title": "Core Retreivr", "summary": "ok", "required": True, "complete": True},
                "arr": {"status": "optional", "title": "ARR Stack", "summary": "optional", "required": False, "complete": False},
                "storage": {"status": "verified", "title": "Storage", "summary": "ok", "required": True, "complete": True},
            },
            "service_management": dict((((cfg.get("setup") or {}).get("service_management")) or {"mode": "none", "apply_mode": "manual"})),
            "managed_stack": {"mode": "managed", "selected_services": [], "services": {}, "phase": "idle", "phase_message": "", "last_error": ""},
            "existing_stack": {"mode": "existing", "selected_services": [], "services": {}},
            "stack": stack,
            "preflight": _preflight_payload(stack),
            "security": {"admin_pin_enabled": False, "admin_pin_session_minutes": 30, "session_valid": True},
        }

    @app.post("/api/setup/preflight")
    def api_setup_preflight(payload: dict[str, Any]) -> dict[str, Any]:
        stack = dict((payload.get("stack") if isinstance(payload.get("stack"), dict) else {}) or {})
        return {"preflight": _preflight_payload(stack), "stack": stack}

    @app.post("/api/setup/stack")
    def api_setup_stack(payload: dict[str, Any]) -> dict[str, Any]:
        cfg = state["config"]
        setup = dict(cfg.get("setup") or {})
        setup["stack"] = dict(payload or {})
        cfg["setup"] = setup
        state["config"] = cfg
        return api_setup_status()

    @app.post("/api/setup/managed/plan")
    def api_setup_managed_plan(_payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "planned",
            "managed_stack": {"mode": "managed"},
            "stack": dict((((state["config"].get("setup") or {}).get("stack")) or {})),
            "preflight": _preflight_payload(),
        }

    @app.post("/api/setup/existing/discover")
    def api_setup_existing_discover(_payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "discovered", "existing_stack": {"mode": "existing", "selected_services": []}}

    @app.post("/api/setup/managed/apply")
    def api_setup_managed_apply() -> dict[str, Any]:
        return {"status": "prepared", "compose_command": "docker compose up -d", "profiles": []}

    @app.post("/api/setup/existing/connect")
    def api_setup_existing_connect(_payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "connected", "existing_stack": {"mode": "existing", "selected_services": []}}

    @app.post("/api/setup/apply-stack")
    def api_setup_apply_stack() -> dict[str, Any]:
        return {"status": "written", "env_path": ".env", "compose_command": "docker compose up -d", "profiles": [], "enabled_services": []}

    @app.get("/api/services/health")
    def api_services_health() -> dict[str, Any]:
        return {"services": {"radarr": {"configured": False, "reachable": False, "status": "not_configured", "state": "unknown", "reason_code": "not_configured", "retry_hint": "Configure Radarr in Setup first."}}, "summary": {"connected": 0, "needs_attention": 0, "failed": 0, "unknown": 1}}

    @app.post("/api/services/autoconfigure")
    def api_services_autoconfigure() -> dict[str, Any]:
        return {"status": "completed", "services": {}, "summary": {"connected": 0, "needs_attention": 0, "failed": 0}}

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
        if media_mode == "music":
            from fastapi import HTTPException

            raise HTTPException(status_code=400, detail="Direct URLs must be searched from the Video page.")
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
            "job_id": state["import_job_id"],
            "status": {
                "phase": "queued",
                "message": "Queued",
                "total_tracks": 4,
                "processed_tracks": 0,
                "resolved": 0,
                "enqueued": 0,
                "failed": 0,
                "unresolved": 0,
            },
        }

    @app.post("/api/import/playlist/preflight")
    async def api_import_playlist_preflight() -> dict[str, Any]:
        return {
            "filename": "music_import.m3u",
            "detected_format": "m3u",
            "file_size_bytes": 64,
            "file_size_mb": 0.01,
            "total_tracks": 4,
            "unique_track_estimate": 4,
            "duplicate_in_file_count": 0,
            "missing": {"artist": 0, "title": 0, "album": 4},
            "metadata_richness": {"artist": 4, "title": 4, "album": 0, "duration_ms": 4},
            "estimated_parser_strain": "low",
            "recommendation": "This file is suitable for direct import.",
            "max_concurrent_downloads_default": 2,
            "max_concurrent_downloads_min": 1,
            "max_concurrent_downloads_max": 6,
        }

    @app.get("/api/import/playlist")
    async def api_import_playlist_snapshot() -> dict[str, Any]:
        return {
            "active": False,
            "active_count": 0,
            "current_job": None,
            "recent_batches": [
                {
                    "batch_id": "batch-smoke",
                    "source_format": "m3u",
                    "started_at": "2026-01-01T00:00:00+00:00",
                    "finished_at": "2026-01-01T00:01:00+00:00",
                    "total_tracks": 4,
                    "resolved_count": 3,
                    "enqueued_count": 3,
                    "unresolved_count": 1,
                }
            ],
        }

    @app.get("/api/import/playlist/jobs/{job_id}")
    async def api_import_playlist_job(job_id: str) -> dict[str, Any]:
        return {
            "job_id": job_id,
            "status": {
                "phase": "completed",
                "message": "Completed",
                "total_tracks": 4,
                "processed_tracks": 4,
                "resolved": 3,
                "enqueued": 3,
                "failed": 0,
                "unresolved": 1,
                "duplicate_skipped": 0,
                "playlist_entries": 2,
            },
        }

    @app.get("/api/player/library")
    def api_player_library() -> dict[str, Any]:
        summary = _library_summary_payload(state["library_summary_version"])
        return {"items": summary["tracks"]}

    @app.get("/api/player/library/summary")
    def api_player_library_summary(limit: int = 2000) -> dict[str, Any]:
        _ = limit
        return {"summary": _library_summary_payload(state["library_summary_version"])}

    @app.get("/api/player/stations")
    def api_player_stations() -> dict[str, Any]:
        return {"stations": []}

    @app.get("/api/player/history")
    def api_player_history() -> dict[str, Any]:
        return {"history": []}

    @app.get("/api/player/playlists")
    def api_player_playlists() -> dict[str, Any]:
        return {"playlists": []}

    @app.get("/api/player/community-cache")
    def api_player_community_cache() -> dict[str, Any]:
        return {"items": []}

    @app.get("/api/music/search")
    def api_music_search(
        artist: str = "",
        album: str = "",
        track: str = "",
        mode: str = "auto",
        offset: int = 0,
        limit: int = 48,
    ) -> dict[str, Any]:
        _ = (artist, album, mode, offset, limit)
        term = track or "Smoke Track"
        return {
            "artists": [],
            "albums": [],
            "tracks": [
                {
                    "track": term,
                    "artist": "Artist One",
                    "album": "Alpha Album",
                    "duration_ms": 123000,
                    "recording_mbid": "rec-alpha",
                    "mb_release_id": "rel-alpha",
                    "mb_release_group_id": "rg-alpha",
                    "track_number": 1,
                }
            ],
        }

    @app.get("/resolve/recording/{recording_mbid}")
    def api_resolve_recording(recording_mbid: str) -> dict[str, Any]:
        return {
            "recording_mbid": recording_mbid,
            "selection": {"selected_url": "https://www.youtube.com/watch?v=stub123"},
            "best_source": {"source": "youtube"},
        }

    @app.post("/api/music/preview")
    def api_music_preview(_payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "preview_type": "video",
            "source": "youtube",
            "source_url": "https://www.youtube.com/watch?v=stub123",
            "video_id": "stub123",
            "title": "Smoke Track",
        }

    @app.post("/api/test/library-summary/version/{version}")
    def api_set_library_version(version: int) -> dict[str, Any]:
        state["library_summary_version"] = 2 if version >= 2 else 1
        return {"ok": True, "version": state["library_summary_version"]}

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
          const title = document.querySelector("#home-results .home-candidate-title")?.textContent || "";
          const hasHeader = !!document.querySelector("#home-results .home-result-card .home-result-header");
          return /Stub Direct Preview/.test(title) && !hasHeader;
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"


def test_webui_music_direct_url_is_blocked_with_warning(webui_server: str, page) -> None:
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

    page.wait_for_function(
        """() => {
          const toasts = Array.from(document.querySelectorAll(".app-toast"));
          const hasWarning = toasts.some((el) => (el.textContent || "").includes("URLs must be searched from the Video page."));
          const flash = document.querySelector("#music-header-query.input-error-flash");
          return hasWarning && !!flash;
        }""",
        timeout=10000,
    )
    page.wait_for_function(
        """() => !document.querySelector("#music-results-container .home-result-card")""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"


def test_webui_music_import_tab_preflights_and_starts_import(
    webui_server: str, page, tmp_path: Path
) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    import_file = tmp_path / "music_import.m3u"
    import_file.write_text("#EXTM3U\n#EXTINF:123,Artist One - Alpha Track 1\ntrack.mp3\n", encoding="utf-8")

    page.goto(f"{webui_server}#music", wait_until="networkidle")
    page.click('button[data-music-section="import"]')
    page.wait_for_selector("#music-import-view.active", timeout=10000)
    page.set_input_files("#music-import-file", str(import_file))
    page.click("#music-import-preflight-button")
    page.wait_for_function(
        """() => {
          const el = document.querySelector("#music-import-preflight-stats");
          const text = (el && el.textContent) || "";
          return text.includes("Tracks") && text.includes("4") && text.includes("m3u");
        }""",
        timeout=10000,
    )
    page.eval_on_selector("#music-import-concurrency", "el => { el.value = '1'; el.dispatchEvent(new Event('input', { bubbles: true })); }")
    page.click("#music-import-start-button")

    page.wait_for_function(
        """() => {
          const el = document.querySelector("#music-import-progress-stats");
          const text = (el && el.textContent) || "";
          return text.includes("Resolved") && text.includes("3")
            && text.includes("Enqueued") && text.includes("3")
            && text.includes("Unresolved") && text.includes("1");
        }""",
        timeout=10000,
    )
    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"


def test_webui_music_search_result_play_updates_player_state(webui_server: str, page) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    page.goto(f"{webui_server}#music", wait_until="networkidle")
    page.click('.music-app-nav[data-music-section="favorites"]')
    page.wait_for_function(
        """() => {
          const favorites = document.querySelector("#music-player-favorites");
          return !!favorites;
        }""",
        timeout=10000,
    )
    page.click('.music-app-nav[data-music-section="browse"]')
    page.fill("#search-track", "Alpha Track 1")
    page.click("#search-create-only")
    page.wait_for_selector(".music-search-play-btn", timeout=10000)
    page.click(".music-search-play-btn")
    page.wait_for_function(
        """() => {
          const title = document.querySelector("#music-player-now-title")?.textContent || "";
          const source = document.querySelector("#music-player-audio")?.getAttribute("src") || "";
          return /Alpha Track 1/.test(title) && source.startsWith("data:audio/");
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"


def test_webui_music_library_filter_transition_and_refresh_resilience(webui_server: str, page) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    page.goto(f"{webui_server}#music", wait_until="networkidle")
    page.click('.music-app-nav[data-music-section="library"]')
    page.wait_for_selector("#music-library-grid", timeout=10000)

    page.click('#music-library-section [data-music-library-mode="artists"]')
    page.wait_for_selector('#music-library-grid [data-library-type="artist"]', timeout=10000)
    page.click('#music-library-grid [data-action="music-library-open-artist"]')
    page.wait_for_selector('#music-library-grid [data-library-type="album"]', timeout=10000)
    page.click('#music-library-grid [data-action="music-library-open-album"]')
    page.wait_for_selector('#music-library-grid [data-library-type="track"]', timeout=10000)

    response = requests.post(f"{webui_server}/api/test/library-summary/version/2", timeout=2)
    assert response.status_code == 200

    page.click("#music-library-refresh")
    page.wait_for_function(
        """() => {
          const trackCards = document.querySelectorAll('#music-library-grid [data-library-type="track"]');
          const empty = document.querySelector('#music-library-grid .home-results-empty');
          return trackCards.length > 0 && (!empty || !/No tracks available/i.test(empty.textContent || ""));
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"


def test_webui_setup_wizard_preflight_conflict_disables_apply(webui_server: str, page) -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    def on_console(msg) -> None:
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("console", on_console)
    page.on("pageerror", lambda err: page_errors.append(str(err)))

    page.goto(webui_server, wait_until="networkidle")

    page.wait_for_function(
        """() => {
          const title = document.querySelector(".setup-wizard-title");
          return !!title && /Where should Retreivr keep your files\\?/i.test(title.textContent || "");
        }""",
        timeout=10000,
    )
    page.click('[data-setup-nav="next"]')
    page.click('[data-setup-choice="arr_setup_mode"][data-value="none"]')
    page.click('[data-setup-choice="wants_tmdb"][data-value="false"]')
    page.click('[data-setup-choice="enable_vpn"][data-value="false"]')
    page.click('[data-setup-choice="wants_youtube"][data-value="false"]')
    page.click('[data-setup-choice="wants_telegram"][data-value="false"]')
    page.click('[data-setup-choice="enable_jellyfin"][data-value="false"]')
    page.click('[data-setup-nav="next"]')

    page.wait_for_function(
        """() => {
          const title = document.querySelector(".setup-wizard-title");
          return !!title && /Preflight checks before apply/i.test(title.textContent || "");
        }""",
        timeout=10000,
    )
    page.click('[data-setup-action="run-preflight"]')
    page.wait_for_function(
        """() => {
          const text = document.body.textContent || "";
          const apply = document.querySelector('[data-setup-action="apply-env"]');
          return /Blocking issues/i.test(text) && !!apply && apply.hasAttribute("disabled");
        }""",
        timeout=10000,
    )

    assert not page_errors, f"Page JS errors detected: {page_errors}"
    assert not console_errors, f"Console errors detected: {console_errors}"
