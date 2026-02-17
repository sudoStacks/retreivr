from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_legacy_run_panel_removed_from_index_html() -> None:
    html = _read("webUI/index.html")
    assert 'id="run-panel"' not in html
    assert 'data-page="legacy-run"' not in html
    assert 'id="run-message"' not in html
    assert 'id="run-single"' not in html
    assert 'id="run-playlist-once"' not in html


def test_home_and_advanced_sections_present() -> None:
    html = _read("webUI/index.html")
    assert 'data-page="home"' in html
    assert 'data-page="advanced"' in html
    assert 'id="home-search-input"' in html
    assert 'id="home-advanced-toggle"' in html
    assert 'id="home-search-only"' in html
    assert 'id="home-search-download"' in html


def test_no_legacy_run_listener_bindings_in_app_js() -> None:
    js = _read("webUI/app.js")
    assert '#run-' not in js
    assert 'run-single-url' not in js
    assert 'run-playlist-id' not in js
    assert 'browse-run-destination' not in js
