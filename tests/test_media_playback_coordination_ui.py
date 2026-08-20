from pathlib import Path


APP_SOURCE = (Path(__file__).resolve().parents[1] / "webUI" / "app.js").read_text(encoding="utf-8")


def test_recent_video_search_executes_immediately() -> None:
    handler = APP_SOURCE[APP_SOURCE.index('const videoDiscoveryDefault = $("#video-discovery-default")'):]
    handler = handler[:handler.index('const homeViewAdvanced = $("#home-view-advanced")')]
    assert 'const searchButton = $("#home-search-only")' in handler
    assert "searchButton.click();" in handler


def test_external_video_paths_pause_music_without_clearing_queue() -> None:
    pause_helper = APP_SOURCE[APP_SOURCE.index("function pauseMusicForExternalMedia"):]
    pause_helper = pause_helper[:pause_helper.index("function activePlayerIsPaused")]
    assert "activePlayerPause();" in pause_helper
    assert "clearQueue" not in pause_helper
    assert "clearMusicPlayerCurrentState" not in pause_helper
    assert "pauseMusicForExternalMedia();" in APP_SOURCE[APP_SOURCE.index("function openHomePreviewModal"):]
    assert "pauseMusicForExternalMedia();" in APP_SOURCE[APP_SOURCE.index("function openLibraryVideoModal"):]


def test_page_lifecycle_detaches_restored_media_surfaces() -> None:
    cleanup = APP_SOURCE[APP_SOURCE.index("function stopTransientPlayback"):]
    cleanup = cleanup[:cleanup.index("// Create a YT.Player")]
    assert "activePlayerPause();" in cleanup
    assert "destroyYTPlayer();" in cleanup
    assert 'pauseAndDetachMediaElement($("#music-player-audio"));' in cleanup
    assert 'pauseAndDetachMediaElement($("#library-video-player"));' in cleanup
    assert 'pauseAndDetachMediaElement($("#home-preview-audio"));' in cleanup
    assert 'homePreviewFrame.src = "about:blank";' in cleanup
    assert "clearQueue" not in cleanup
    assert "clearMusicPlayerCurrentState" not in cleanup

    init = APP_SOURCE[APP_SOURCE.index("async function init"):]
    init = init[:init.index("window.addEventListener(\"DOMContentLoaded\", init);")]
    assert "stopTransientPlayback({ closeModals: true });" in init
    assert 'window.addEventListener("pagehide", () => { stopTransientPlayback({ closeModals: true }); });' in APP_SOURCE
    assert 'window.addEventListener("beforeunload", () => { stopTransientPlayback({ closeModals: true }); });' in APP_SOURCE


def test_downloaded_album_cards_use_local_tracks_for_playback() -> None:
    album_card = APP_SOURCE[APP_SOURCE.index("function createMusicAlbumCard"):]
    album_card = album_card[:album_card.index("function getMusicLandingRecentArtists")]
    assert "const hasLocalAlbum = localAlbumTracks.length > 0;" in album_card
    assert "if (!releaseGroupMbid && !hasLocalAlbum)" in album_card
    assert "setPlayerQueue(queue);" in album_card
    assert "await playPlayerQueueIndex(0);" in album_card


def test_view_albums_cannot_reuse_partial_warmup_cache() -> None:
    artist_card = APP_SOURCE[APP_SOURCE.index("function createMusicArtistCard"):]
    artist_card = artist_card[:artist_card.index("function createMusicAlbumCard")]
    fetcher = APP_SOURCE[APP_SOURCE.index("async function fetchMusicAlbumsByArtist"):]
    fetcher = fetcher[:fetcher.index("async function fetchMusicTracksByAlbum")]

    assert "{ limit: 48, bypassInFlight: true, bypassCache: true }" in artist_card
    assert "getMusicArtistAlbumsCacheKey(artist, cappedLimit)" in fetcher
    assert "if (!bypassCache && cacheKey" in fetcher
