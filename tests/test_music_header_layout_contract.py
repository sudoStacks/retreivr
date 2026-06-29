from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
STYLES = REPO_ROOT / "webUI" / "styles.css"


def _block(source: str, selector: str) -> str:
    start = source.index(selector)
    open_brace = source.index("{", start)
    close_brace = source.index("}", open_brace)
    return source[open_brace + 1:close_brace]


def test_music_top_row_wraps_toolbar_instead_of_fixed_grid() -> None:
    source = STYLES.read_text(encoding="utf-8")
    top_row = _block(source, "#music-panel .music-top-row")
    toolbar = _block(source, ".music-toolbar-slot")
    actions = _block(source, ".music-results-toolbar-actions")

    assert "display: flex" in top_row
    assert "flex-wrap: wrap" in top_row
    assert "grid-template-columns" not in top_row
    assert "max-width: 100%" in toolbar
    assert "flex-wrap: wrap" in actions
