from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_JS = REPO_ROOT / "webUI" / "app.js"
INDEX_HTML = REPO_ROOT / "webUI" / "index.html"


def test_movies_search_exists_only_in_the_global_header_surface() -> None:
    markup = INDEX_HTML.read_text(encoding="utf-8")
    movies_start = markup.index('id="movies-tv-panel"')
    movies_end = markup.index('id="books-panel"', movies_start)
    movies_markup = markup[movies_start:movies_end]
    parking_start = markup.index('id="topbar-node-parking"')
    header_search_start = markup.index('id="movies-tv-header-search"')

    assert parking_start < header_search_start < movies_start
    assert markup.count('id="movies-tv-header-search"') == 1
    assert markup.count('id="movies-tv-search-input"') == 1
    assert 'id="movies-tv-search-input"' not in movies_markup
    assert 'class="movies-tv-search-row"' not in movies_markup
    assert 'id="movies-tv-results-view"' in movies_markup
    assert 'id="movies-tv-results-list"' in movies_markup


def test_movies_header_search_drives_the_main_results_pipeline() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    getter_start = source.index("function getMoviesTvSearchRowEl()")
    getter_end = source.index("function getMoviesTvFiltersPanelEl", getter_start)
    getter_source = source[getter_start:getter_end]
    mount_start = source.index("function mountTopbarForPage(page)")
    mount_end = source.index("function getMoviesTvSearchRowEl", mount_start)
    mount_source = source[mount_start:mount_end]
    search_start = source.index("async function performArrSearch()")
    search_end = source.index("async function addArrItem", search_start)
    search_source = source[search_start:search_end]
    wire_start = source.index('const moviesTvSearchButton = $("#movies-tv-search-button")')
    wire_end = source.index('const moviesTvSearchClear = $("#movies-tv-search-clear")', wire_start)
    wire_source = source[wire_start:wire_end]

    assert 'return $("#movies-tv-header-search")' in getter_source
    assert 'if (page === "movies-tv")' in mount_source
    assert "searchHost.appendChild(searchRow)" in mount_source
    assert 'const input = $("#movies-tv-search-input")' in search_source
    assert 'state.arrMode === "tv" ? "/api/arr/search/tv" : "/api/arr/search/movies"' in search_source
    assert "renderArrResults()" in search_source
    assert 'setMoviesTvSection("search")' in search_source
    assert "focusMoviesTvResults()" in search_source
    assert 'moviesTvSearchButton.addEventListener("click", performArrSearch)' in wire_source
    assert 'event.key === "Enter"' in wire_source
    assert "performArrSearch()" in wire_source
