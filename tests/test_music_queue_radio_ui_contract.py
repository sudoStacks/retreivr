from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "webUI" / "app.js").read_text(encoding="utf-8")
CSS = (ROOT / "webUI" / "styles.css").read_text(encoding="utf-8")
HTML = (ROOT / "webUI" / "index.html").read_text(encoding="utf-8")


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


def test_radio_cards_have_large_static_responsive_layout() -> None:
    assert "grid-template-columns: repeat(auto-fill, minmax(290px, 340px))" in CSS
    assert ".music-station-grid-card .music-card-thumb-shell" in CSS
    assert "min-height: 260px" in CSS
    assert ".music-station-grid-card .home-candidate-action" in CSS
    assert "position: static" in CSS
    assert "styles.css?v=1.0.10" in HTML
    assert "app.js?v=1.0.10" in HTML
