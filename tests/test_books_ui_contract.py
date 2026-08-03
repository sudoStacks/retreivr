from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_JS = REPO_ROOT / "webUI" / "app.js"
INDEX_HTML = REPO_ROOT / "webUI" / "index.html"


def test_books_has_a_native_landing_and_home_launcher() -> None:
    markup = INDEX_HTML.read_text(encoding="utf-8")

    launcher_start = markup.index('id="home-launcher-books"')
    launcher_tag = markup[launcher_start:markup.index(">", launcher_start)]
    assert "hidden" not in launcher_tag
    assert 'id="books-panel" data-page="books"' in markup
    assert 'id="books-search-input"' in markup
    assert 'id="books-search-button"' in markup
    assert 'class="books-discovery-hero"' in markup
    assert 'data-books-browse-query="science fiction"' in markup


def test_disabled_books_shows_an_enable_gate_instead_of_home() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    set_page_start = source.index("function setPage(page)")
    set_page_end = source.index("function mountMusicPageNodes", set_page_start)
    set_page_source = source[set_page_start:set_page_end]
    sync_start = source.index("function syncBooksNavigation()")
    sync_end = source.index("function setBooksSection", sync_start)
    sync_source = source[sync_start:sync_end]

    assert 'const target = allowed.has(normalized) ? normalized : "home";' in set_page_source
    assert 'normalized === "books" && !state.config?.books?.enabled' not in set_page_source
    assert 'setPage("home")' not in sync_source
    assert '$("#books-disabled-gate")?.classList.toggle("hidden", enabled)' in sync_source
    assert 'button.classList.toggle("hidden", !enabled)' in sync_source


def test_books_quick_browse_uses_the_native_search_path() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    start = source.index("function initializeBooksUi()")
    end = source.index("function setHomeSection", start)
    books_ui = source[start:end]

    assert "[data-books-browse-query]" in books_ui
    assert "button.dataset.booksBrowseQuery" in books_ui
    assert "performBooksSearch();" in books_ui


def test_public_book_cards_have_a_one_click_download_path() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    render_start = source.index("function renderBooksResults()")
    render_end = source.index("function renderBooksLibrary", render_start)
    render_source = source[render_start:render_end]
    download_start = source.index("async function downloadOpenLibraryBook")
    download_end = source.index("async function acquireBookFromUrl", download_start)
    download_source = source[download_start:download_end]

    assert "item?.download_available" in render_source
    assert 'data-book-action="download"' in render_source
    assert ">Download</button>" in render_source
    assert 'fetchJson("/api/books/acquire/openlibrary"' in download_source
    assert "await loadBooksLibrary();" in download_source
