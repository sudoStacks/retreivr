from pathlib import Path


APP_JS = Path("webUI/app.js")


def test_album_track_views_inherit_known_album_artwork():
    source = APP_JS.read_text()

    assert "function applyAlbumArtworkToTracks" in source
    assert "setCachedAlbumCoverUrl(trackReleaseGroupMbid, trackArtwork)" in source

    first_album_handler = source.index("function createMusicAlbumCard")
    delegated_handler = source.index('const viewTracksButton = event.target.closest(".album-view-tracks-btn")')
    render_call = source.index("renderMusicModeResults(", first_album_handler)
    delegated_render_call = source.index("renderMusicModeResults(", delegated_handler)

    assert source.index("const hydratedTracks = applyAlbumArtworkToTracks(tracks", first_album_handler) < render_call
    assert source.index("const hydratedTracks = applyAlbumArtworkToTracks(tracks", delegated_handler) < delegated_render_call
    assert "{ artists: [], albums: [], tracks: hydratedTracks, mode_used: \"track\" }" in source


def test_track_cards_apply_immediate_art_before_remote_queue():
    source = APP_JS.read_text()
    card_start = source.index("function createMusicTrackResultCard")
    card_end = source.index("function renderMusicModeResults", card_start)
    card_source = source[card_start:card_end]

    immediate_index = card_source.index("const immediateTrackArtwork = getImmediateAlbumArtworkUrl(result)")
    set_image_index = card_source.index("trackThumb.setImage(immediateTrackArtwork)")
    remote_fetch_index = card_source.index("fetchHomeAlbumCoverUrl(releaseGroupMbid)")

    assert immediate_index < set_image_index < remote_fetch_index


def test_music_thumbnail_jobs_start_after_track_cards_are_created():
    source = APP_JS.read_text()
    render_start = source.index("function renderMusicModeResults")
    render_end = source.index("async function performMusicModeSearch", render_start)
    render_source = source[render_start:render_end]

    track_card_index = render_source.index("createMusicTrackResultCard(result, thumbnailJobs, renderToken)")
    scheduler_index = render_source.index("runPrioritizedThumbnailJobs(thumbnailJobs, renderToken")

    assert track_card_index < scheduler_index


def test_music_landing_uses_home_snapshot_without_initial_artwork_hydration():
    source = APP_JS.read_text()
    render_start = source.index("function renderMusicLanding")
    render_end = source.index("function clearMusicResultsHistory", render_start)
    render_source = source[render_start:render_end]

    assert 'fetchJson("/api/music/home")' in source
    assert "const homeSnapshot = state.musicHomeSnapshot || {}" in render_source
    assert "skipArtworkHydration: true" in render_source
    assert "Rotating Spotify playlists from your Retreivr taste profile" in render_source

    warm_index = render_source.index("warmMusicGenreArtistCaches(topGenreSeeds")
    snapshot_recs_index = render_source.index("snapshotGenreRecommendations.slice")
    assert snapshot_recs_index < warm_index


def test_music_home_continue_rows_render_snapshot_artwork():
    source = APP_JS.read_text()
    row_start = source.index("function createMusicLandingTrackRow")
    row_end = source.index("function getSpotifyPlaylistCardMeta", row_start)
    row_source = source[row_start:row_end]

    assert "getMusicLibraryArtworkUrl(item)" in row_source
    assert "music-card-thumb-shell loaded music-player-track-art" in row_source
    assert "classList.remove('loaded','loading')" in row_source
    assert "classList.add('no-art')" in row_source


def test_music_home_spotify_and_genres_have_stable_fallback_artwork():
    source = APP_JS.read_text()
    spotify_start = source.index("function renderSpotifyPlaylistCard")
    spotify_end = source.index("function updateSpotifyPlaylistCardStatus", spotify_start)
    spotify_source = source[spotify_start:spotify_end]

    genre_start = source.index("function createMusicGenreCard")
    genre_end = source.index("async function browseMusicGenre", genre_start)
    genre_source = source[genre_start:genre_end]

    assert 'getStableArtworkFallbackUrl("Spotify", card.genre || title)' in spotify_source
    assert '"assets/no_artwork.png"' not in spotify_source
    assert 'getStableArtworkFallbackUrl("Genre", displayGenre)' in genre_source
    assert "skipArtworkHydration" in genre_source


def test_spotify_playlist_preview_opens_track_result_view_without_inline_tracks():
    source = APP_JS.read_text()
    card_start = source.index("function renderSpotifyPlaylistCard")
    card_end = source.index("function updateSpotifyPlaylistCardStatus", card_start)
    card_source = source[card_start:card_end]
    preview_start = source.index("async function previewSpotifyPlaylistUrl")
    preview_end = source.index("async function playSpotifyPlaylistUrl", preview_start)
    preview_source = source[preview_start:preview_end]

    assert "spotify-playlist-preview-list" not in card_source
    assert 'data-action="spotify-playlist-preview"' in card_source
    assert "renderMusicModeResults(" in preview_source
    assert 'mode_used: "spotify_playlist"' in preview_source
    assert "{ pushHistory: true }" in preview_source
    assert "normalizeSpotifyPlaylistTrackResults(response?.tracks, response?.playlist || {})" in source


def test_spotify_playlist_card_actions_match_preview_import_export_play_contract():
    source = APP_JS.read_text()
    card_start = source.index("function renderSpotifyPlaylistCard")
    card_end = source.index("function updateSpotifyPlaylistCardStatus", card_start)
    card_source = source[card_start:card_end]
    click_start = source.index('if (action === "spotify-playlist-open")')
    click_end = source.index('const btn = event.target.closest(".music-download-btn")', click_start)
    click_source = source[click_start:click_end]

    assert 'class="button danger small home-candidate-download-primary"' in card_source
    assert 'data-action="spotify-playlist-import"' in card_source
    assert 'data-action="spotify-playlist-export"' in card_source
    assert "Export File" in card_source
    assert 'data-action="spotify-playlist-play"' in card_source
    assert "exportSpotifyPlaylistUrl(playlistUrl)" in click_source
    assert "playSpotifyPlaylistUrl(playlistUrl, spotifyBtn)" in click_source
    assert "previewSpotifyPlaylistUrl(playlistUrl)" in click_source


def test_spotify_playlist_play_resolves_first_playable_before_starting_queue():
    source = APP_JS.read_text()
    play_start = source.index("async function playSpotifyPlaylistUrl")
    play_end = source.index("async function importSpotifyPlaylistUrl", play_start)
    play_source = source[play_start:play_end]

    assert "resolveFirstPlayableQueueItem(queue, { searchLimit: 8 })" in play_source
    assert "moveResolvedQueueItemToFront(queue, firstPlayable)" in play_source
    assert play_source.index("resolveFirstPlayableQueueItem(queue") < play_source.index("await playPlayerQueueIndex(0)")
    assert "No playable playlist tracks resolved yet." in play_source
    assert 'state.playerQueuePlaybackMode = "resolve_first"' in play_source


def test_resolve_first_queue_mode_does_not_recursively_burn_through_indexes():
    source = APP_JS.read_text()
    queue_start = source.index("async function playPlayerQueueIndex")
    queue_end = source.index("// Background-resolve a rolling lookahead", queue_start)
    queue_source = source[queue_start:queue_end]
    next_start = source.index("async function playNextPlayerItem")
    next_end = source.index("async function playPreviousPlayerItem", next_start)
    next_source = source[next_start:next_end]

    assert 'state.playerQueuePlaybackMode === "resolve_first"' in queue_source
    assert "await playNextPlayerItem({ autoAdvance: true })" not in queue_source
    assert "_prefetchNextUnresolved(targetIndex + 1)" in queue_source
    assert 'state.playerQueuePlaybackMode === "resolve_first"' in next_source
    assert "resolveFirstPlayableQueueItem(state.playerQueue.slice(startIndex), { searchLimit: 8 })" in next_source


def test_music_mode_prefers_audio_stream_proxy_for_youtube_resolution():
    source = APP_JS.read_text()
    helper_start = source.index("function normalizeResolvedPlaybackSource")
    helper_end = source.index("function buildPlayerNowContextText", helper_start)
    helper_source = source[helper_start:helper_end]
    resolver_start = source.index("async function resolveRecordingStreamUrl")
    resolver_end = source.index("// Resolve a search-result track", resolver_start)
    resolver_source = source[resolver_start:resolver_end]

    assert 'state.homeMediaMode !== "music_video"' in helper_source
    assert "buildPreviewStreamUrl(sourceUrl)" in helper_source
    assert "video_id: null" in helper_source
    assert "normalizeResolvedPlaybackSource(result)" in resolver_source


def test_music_header_search_uses_result_cache_and_preserves_back_target():
    source = APP_JS.read_text()
    header_start = source.index("async function runMusicHeaderSearch")
    header_end = source.index("function getMusicTopMatchCandidate", header_start)
    header_source = source[header_start:header_end]
    search_start = source.index("async function performMusicModeSearch")
    search_end = source.index("function clearLegacyHomeSearchState", search_start)
    search_source = source[search_start:search_end]

    assert "musicSearchResultCache" in header_source
    assert "renderMusicModeResults(cached.payload, cached.displayQuery || trimmedQuery, { pushHistory: !!state.homeMusicCurrentView })" in header_source
    assert "state.musicSearchResultCache[searchCacheKey]" in search_source
    assert "renderMusicModeResults(payload, displayQuery, { pushHistory: shouldPushHistory })" in search_source


def test_music_browse_back_button_renders_for_loading_empty_and_results():
    source = APP_JS.read_text()
    helper_start = source.index("function renderMusicBrowseBackButton")
    helper_end = source.index("function focusMusicResults", helper_start)
    helper_source = source[helper_start:helper_end]
    loading_start = source.index("function renderMusicBrowseLoading")
    loading_end = source.index("function renderArrBrowseLoading", loading_start)
    loading_source = source[loading_start:loading_end]
    results_start = source.index("function renderMusicModeResults")
    results_end = source.index("function fetchMusicAlbumsByArtist", results_start)
    results_source = source[results_start:results_end]

    assert 'backButton.textContent = "← Home"' in helper_source
    assert "renderMusicModeResults(previous.response, previous.query, { pushHistory: false })" in helper_source
    assert "renderMusicBrowseBackButton({ fallbackHome: true })" in loading_source
    assert "state.homeMusicCurrentView = null" not in loading_source
    assert "renderMusicBrowseBackButton({ fallbackHome: true })" in results_source
    assert "renderMusicBrowseBackButton({ artists: visibleArtists, albums, tracks, fallbackHome: true })" in results_source
