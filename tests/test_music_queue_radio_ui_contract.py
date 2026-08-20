from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "webUI" / "app.js").read_text(encoding="utf-8")
CSS = (ROOT / "webUI" / "styles.css").read_text(encoding="utf-8")
HTML = (ROOT / "webUI" / "index.html").read_text(encoding="utf-8")
PROJECT_VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]


def test_prebuilt_mix_queues_obey_visible_order() -> None:
    artist_start = APP.index("async function playMusicArtistFromBrowse")
    genre_start = APP.index("async function playMusicGenreFromBrowse")
    album_start = APP.index("async function playMusicAlbumFromSearch")

    assert "state.playerShuffle = false" in APP[artist_start:genre_start]
    assert "state.playerShuffle = false" in APP[genre_start:album_start]
    assert "visibleNextIndex < state.playerQueue.length" in APP
    assert "await playPlayerQueueIndex(visibleNextIndex)" in APP


def test_advancing_consumes_played_queue_items() -> None:
    play_start = APP.index("async function playPlayerQueueIndex")
    prefetch_start = APP.index("function _prefetchNextUnresolved", play_start)
    source = APP[play_start:prefetch_start]

    assert "state.playerQueue = state.playerQueue.slice(targetIndex)" in source
    assert 'data-queue-position="${isCurrent ? "current" : "up-next"}"' in APP
    assert 'isCurrent ? "Now playing" : `Up next · ${relativePosition}`' in APP
    assert "1 playing · ${upcomingCount} up next" in APP


def test_station_prime_appends_without_replacing_reordered_queue() -> None:
    prime_start = APP.index("function scheduleStationPrime")
    clear_start = APP.index("function clearActiveStationPlayback", prime_start)
    source = APP[prime_start:clear_start]

    assert "const currentQueue" in source
    assert "setPlayerQueue([...currentQueue, ...additions]" in source
    assert "setPlayerQueue(payload.queue" not in source


def test_radio_and_favorites_use_dedicated_music_shelf_cards() -> None:
    assert 'class="music-shelf-grid music-station-grid"' in APP
    assert 'class="music-shelf-grid music-favorites-grid"' in APP
    assert 'class="music-shelf-card music-station-grid-card' in APP
    assert 'class="music-shelf-card music-player-favorite-card"' in APP
    assert ".music-shelf-card-art" in CSS
    assert "aspect-ratio: 1 / 1" in CSS
    assert ".music-shelf-card-actions" in CSS
    assert "grid-template-columns: repeat(auto-fill, minmax(var(--music-card-min, 230px), 1fr))" in CSS
    assert ".music-station-grid-card .music-shelf-card-actions" in CSS
    assert "music-station-secondary-actions" in APP
    assert "text-overflow: ellipsis" in CSS
    assert f"styles.css?v={PROJECT_VERSION}" in HTML
    assert f"app.js?v={PROJECT_VERSION}" in HTML


def test_music_import_is_first_class_music_tab() -> None:
    radio_index = HTML.index('data-music-section="radio"')
    import_index = HTML.index('data-music-section="import"')
    assert radio_index < import_index
    assert 'id="music-import-view"' in HTML
    assert 'id="music-import-preflight-button"' in HTML
    assert 'id="music-import-concurrency"' in HTML
    assert 'id="music-import-progress-stats"' in HTML
    assert 'id="music-import-history"' in HTML
    assert '"/api/import/playlist/preflight"' in APP
    assert '"max_concurrent_downloads"' in APP
    assert 'renderMusicImportTab();' in APP
