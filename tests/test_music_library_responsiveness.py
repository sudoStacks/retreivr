from __future__ import annotations

import asyncio
import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from api import main as module
from engine.job_queue import record_download_history
from library.music_index import (
    get_music_library_index_state,
    list_indexed_music,
    mark_music_library_index_stale,
    rebuild_music_library_index,
)


def _indexed_track(path: Path, *, title: str = "Indexed Track") -> dict:
    return {
        "id": str(path),
        "local_path": str(path),
        "title": title,
        "artist": "Indexed Artist",
        "artist_key": "indexed artist",
        "album": "Indexed Album",
        "album_key": "indexed album",
        "stream_url": f"/api/player/stream/local?path={path}",
        "downloaded_at": 123,
        "size_bytes": 456,
        "file_ext": ".flac",
        "media_type": "audio/flac",
        "artwork_local_path": None,
        "recording_mbid": "recording-1",
        "mb_release_id": "release-1",
        "mb_release_group_id": "group-1",
    }


def test_music_library_index_is_persistent_and_queryable(tmp_path: Path) -> None:
    db_path = str(tmp_path / "library.sqlite")
    media_path = tmp_path / "Artist" / "Album" / "Track.flac"
    result = rebuild_music_library_index(
        db_path,
        {},
        scanner=lambda _config, *, limit: [_indexed_track(media_path)],
    )

    assert result["status"] == "ready"
    assert result["item_count"] == 1
    assert get_music_library_index_state(db_path)["item_count"] == 1
    assert list_indexed_music(db_path, limit=10)[0]["title"] == "Indexed Track"
    mark_music_library_index_stale(db_path)
    assert get_music_library_index_state(db_path)["status"] == "stale"
    assert list_indexed_music(db_path, limit=10)[0]["title"] == "Indexed Track"


def test_completed_music_download_marks_index_stale(tmp_path: Path) -> None:
    db_path = str(tmp_path / "library.sqlite")
    media_path = tmp_path / "Track.flac"
    media_path.write_bytes(b"test")
    rebuild_music_library_index(db_path, {}, scanner=lambda _config, *, limit: [])
    job = SimpleNamespace(
        id="job-1",
        url="https://example.test/audio.flac",
        origin="manual",
        origin_id=None,
        input_url="https://example.test/audio.flac",
        external_id="track-1",
        source="direct",
        canonical_url="https://example.test/audio.flac",
        output_template={},
    )

    record_download_history(db_path, job, str(media_path), meta={"title": "Track"})

    assert get_music_library_index_state(db_path)["status"] == "stale"


@pytest.mark.asyncio
async def test_library_api_reads_index_without_scanning_filesystem(tmp_path: Path, monkeypatch) -> None:
    db_path = str(tmp_path / "library.sqlite")
    rebuild_music_library_index(
        db_path,
        {},
        scanner=lambda _config, *, limit: [_indexed_track(tmp_path / "Track.flac")],
    )
    monkeypatch.setattr(module.app.state, "paths", SimpleNamespace(db_path=db_path), raising=False)
    monkeypatch.setattr(
        module,
        "scan_local_library",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("request performed a filesystem scan")),
    )

    heartbeat_seen = False

    async def _heartbeat() -> None:
        nonlocal heartbeat_seen
        await asyncio.sleep(0)
        heartbeat_seen = True

    library, summary, _ = await asyncio.gather(
        module.api_player_library(limit=10),
        module.api_player_library_summary(limit=10),
        _heartbeat(),
    )

    assert heartbeat_seen is True
    assert library["items"][0]["title"] == "Indexed Track"
    assert library["index_state"]["status"] == "ready"
    assert summary["summary"]["tracks"][0]["title"] == "Indexed Track"


@pytest.mark.asyncio
async def test_liveness_is_constant_time_and_dependency_free(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "build_resolution_health",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("liveness touched a dependency")),
    )
    payload = await module.api_health_live()
    assert payload["status"] == "ok"
    assert payload["service"] == "retreivr"


def test_music_player_loads_partial_data_and_preferences_independently() -> None:
    source = (Path(module.WEBUI_DIR) / "app.js").read_text(encoding="utf-8")
    start = source.index("async function loadMusicPlayerView()")
    end = source.index("async function playMusicPlayerItem", start)
    function_source = source[start:end]

    assert 'fetchJson("/api/music/preferences")' in function_source
    assert "Promise.allSettled(requests)" in function_source
    assert "renderMusicLanding();" in function_source
    assert "Indexing music library" in function_source
    assert 'if (requestedMusicSection === "browse")' in source
    for status in ("loading", "loaded", "empty", "failed", "stale"):
        assert f'"{status}"' in function_source or f'"{status}"' in source
    assert 'musicDatasetCountLabel("favorites"' in source
    assert 'musicDatasetCountLabel("history"' in source


def test_async_routes_are_true_coroutines_or_constant_time_liveness() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    allowed_without_await = {
        "api_health_live",
        "api_update_ytdlp",
        "api_player_library_index_refresh",
    }
    violations = []
    for node in tree.body:
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        is_route = any(
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and isinstance(decorator.func.value, ast.Name)
            and decorator.func.value.id == "app"
            for decorator in node.decorator_list
        )
        if not is_route or node.name in allowed_without_await:
            continue
        if not any(isinstance(child, ast.Await) for child in ast.walk(node)):
            violations.append(node.name)
    assert violations == []
