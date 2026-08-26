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
