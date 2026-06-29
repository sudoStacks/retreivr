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
