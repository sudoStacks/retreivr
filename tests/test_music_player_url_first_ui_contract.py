from __future__ import annotations

from pathlib import Path


APP_JS = Path(__file__).resolve().parent.parent / "webUI" / "app.js"


def test_local_music_playback_prefers_indexed_source_url_before_local_fallback() -> None:
    source = APP_JS.read_text()
    player_start = source.index("async function playMusicPlayerItem")
    player_end = source.index("function clearMusicPlayerCurrentState", player_start)
    player_source = source[player_start:player_end]

    assert "async function resolveRecordingIndexedStreamUrl(recordingMbid)" in source
    assert "async function resolveRecordingIndexedStreamUrlWithTimeout(recordingMbid, timeoutMs = 900)" in source
    assert "const resolved = await resolveRecordingStreamUrl(payload.recording_mbid, buildPlayableResolutionMeta(payload))" in player_source
    assert "resolveRecordingIndexedStreamUrlWithTimeout(payload.recording_mbid, 900)" not in player_source
    assert player_source.index("const resolved = await resolveRecordingStreamUrl") < player_source.index("if (!payload.stream_url && payload.local_path)")


def test_album_playback_uses_common_queue_skip_resolution_path() -> None:
    source = APP_JS.read_text()
    start = source.index("async function playMusicAlbumFromSearch")
    end = source.index("async function resolveDirectUrl", start)
    album_source = source[start:end]

    assert "setPlayerQueue(queueItems)" in album_source
    assert "await playPlayerQueueIndex(0)" in album_source
    assert "const firstUnresolved = queueItems[0]" not in album_source
    assert "return;" not in album_source[album_source.index("setPlayerQueue(queueItems)"):]


def test_queue_clicks_use_queue_resolution_not_direct_player_call() -> None:
    source = APP_JS.read_text()
    queue_branch_start = source.index('if (playButton.closest("#music-player-queue"))')
    queue_branch_end = source.index('} else if (playButton.closest(".music-player-playlist-items"))', queue_branch_start)
    queue_branch = source[queue_branch_start:queue_branch_end]

    assert "await playPlayerQueueIndex" in queue_branch
    assert "await playMusicPlayerItem" not in queue_branch
