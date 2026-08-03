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


def test_youtube_transport_recreates_destroyed_host_and_keeps_stationary_player() -> None:
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
    assert 'id="music-player-full-video-slot"' in markup
    assert markup.index('id="music-player-video-shell"') < music_panel_end
    assert 'id="music-player-mini-video-slot"' not in markup
    assert 'id="music-bottom-player-minimize"' in markup
    assert 'id="music-bottom-player-close"' in markup
    assert "moveMusicPlayerVideoShell" not in source
    assert 'activePlayerIsYT() && target !== "music"' not in source
    assert 'const shouldHide = !hasPlayerContent || playerPageOpen;' in source


def test_genre_and_artist_play_build_diverse_shuffled_queues() -> None:
    source = APP_JS.read_text()
    artist_start = source.index("async function playMusicArtistFromBrowse")
    genre_start = source.index("async function playMusicGenreFromBrowse", artist_start)
    album_start = source.index("async function playMusicAlbumFromSearch", genre_start)
    artist_source = source[artist_start:genre_start]
    genre_source = source[genre_start:album_start]

    assert "fetchMusicTracksForArtist(nextQuery" in artist_source
    assert "state.playerShuffle = true" in artist_source
    assert "playMusicAlbumFromSearch(albums[0])" not in artist_source
    assert "selectedArtists.map" in genre_source
    assert "artistCount < 2" in genre_source
    assert "state.playerShuffle = true" in genre_source
    assert "playMusicArtistFromBrowse(artists[0])" not in genre_source


def test_music_card_play_surfaces_immediate_cancelable_loading_state() -> None:
    source = APP_JS.read_text()

    assert "function beginMusicPlaybackLoading" in source
    assert "function finishMusicPlaybackLoading" in source
    assert "function cancelMusicPlaybackLoading" in source
    assert 'shell.classList.toggle("is-loading", !!loading)' in source
    assert "music-loading-spinner" in source
    assert "const loadingToken = beginMusicPlaybackLoading" in source
    assert "if (!isMusicPlaybackLoadingCurrent(loadingToken)) return;" in source
    assert "cancelMusicPlaybackLoading();" in source[source.index("function clearMusicPlayerCurrentState"):]


def test_music_queue_prefetches_five_tracks_and_preserves_artwork() -> None:
    source = APP_JS.read_text()
    prefetch_start = source.index("function _prefetchNextUnresolved")
    prefetch_end = source.index("async function playNextPlayerItem", prefetch_start)
    prefetch_source = source[prefetch_start:prefetch_end]

    assert "const prefetchLimit = 5;" in prefetch_source
    assert 'prefetch_state: "resolving"' in prefetch_source
    assert 'prefetch_state: "ready"' in prefetch_source
    assert "state.playerQueue[capturedIndex]?.artwork_url || resolved.artwork_url" in prefetch_source


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
