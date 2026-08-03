from __future__ import annotations

from pathlib import Path


APP_JS = Path(__file__).resolve().parent.parent / "webUI" / "app.js"
INDEX_HTML = Path(__file__).resolve().parent.parent / "webUI" / "index.html"


def test_local_music_playback_wins_before_remote_resolution() -> None:
    source = APP_JS.read_text()
    player_start = source.index("async function playMusicPlayerItem")
    player_end = source.index("function clearMusicPlayerCurrentState", player_start)
    player_source = source[player_start:player_end]

    assert "async function resolveRecordingIndexedStreamUrl(recordingMbid)" in source
    assert "async function resolveRecordingIndexedStreamUrlWithTimeout(recordingMbid, timeoutMs = 900)" in source
    assert "const resolved = await resolveRecordingStreamUrl(payload.recording_mbid, buildPlayableResolutionMeta(payload))" in player_source
    assert "resolveRecordingIndexedStreamUrlWithTimeout(payload.recording_mbid, 900)" not in player_source
    assert player_source.index("if (payload.local_path)") < player_source.index("const resolved = await resolveRecordingStreamUrl")
    assert "if (!payload.stream_url && !hasDirectVideo && canResolvePlayableItem(payload))" in player_source


def test_youtube_transport_recreates_destroyed_host_and_uses_persistent_visible_player() -> None:
    source = APP_JS.read_text()
    create_start = source.index("function createYTPlayer")
    create_end = source.index("// Active-player helpers", create_start)
    create_source = source[create_start:create_end]
    play_start = source.index("async function playMusicPlayerItem")
    play_end = source.index("function clearMusicPlayerCurrentState", play_start)
    play_source = source[play_start:play_end]

    assert "function resetYouTubePlayerHost()" in source
    assert "destroyYTPlayer();" in create_source
    assert 'const frame = $("#music-player-video-frame")' in create_source
    assert "openMusicPlayerScreen({ showVideo: true });" not in play_source
    assert 'activePlayerIsYT() && normalized !== "player"' not in source
    assert "syncMusicPlayerVideoShell();" in play_source
    assert "syncBottomPlayerShell();" in play_source

    markup = INDEX_HTML.read_text()
    assert "YouTube remote source" in markup
    music_panel_end = markup.index('<div id="music-bottom-player"')
    assert markup.rfind("</section>", 0, music_panel_end) > markup.index('id="music-panel"')
    assert "music-player-video-mini" in markup
    assert 'id="music-player-video-hide"' not in markup
    assert 'id="music-player-video-toggle"' not in markup


def test_album_playback_uses_common_queue_skip_resolution_path() -> None:
    source = APP_JS.read_text()
    start = source.index("async function playMusicAlbumFromSearch")
    end = source.index("async function resolveDirectUrl", start)
    album_source = source[start:end]

    assert "setPlayerQueue(queueItems)" in album_source
    assert "_prefetchNextUnresolved(1)" in album_source
    assert "await playPlayerQueueIndex(0)" in album_source
    assert album_source.index("_prefetchNextUnresolved(1)") < album_source.index("await playPlayerQueueIndex(0)")
    assert "const firstUnresolved = queueItems[0]" not in album_source
    assert "return;" not in album_source[album_source.index("setPlayerQueue(queueItems)"):]


def test_queue_clicks_use_queue_resolution_not_direct_player_call() -> None:
    source = APP_JS.read_text()
    queue_branch_start = source.index('if (playButton.closest("#music-player-queue"))')
    queue_branch_end = source.index('} else if (playButton.closest(".music-player-playlist-items"))', queue_branch_start)
    queue_branch = source[queue_branch_start:queue_branch_end]

    assert "await playPlayerQueueIndex" in queue_branch
    assert "await playMusicPlayerItem" not in queue_branch
