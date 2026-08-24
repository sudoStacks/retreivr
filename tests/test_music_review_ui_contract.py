from __future__ import annotations

from pathlib import Path


_APP_JS = (Path(__file__).resolve().parents[1] / "webUI" / "app.js").read_text(encoding="utf-8")


def test_closed_review_cards_do_not_create_media_requests() -> None:
    assert '${previewOpen ? `<${previewTag} controls preload="metadata"' in _APP_JS
    assert "function setReviewPreviewOpen(reviewId)" in _APP_JS
    assert 'preview?.querySelector("audio, video")?.remove()' in _APP_JS


def test_unchanged_review_poll_does_not_rebuild_the_card_dom() -> None:
    assert 'const reviewChanged = nextSignature !== state.reviewItemsSignature' in _APP_JS
    assert 'state.currentPage === "review" && (reviewChanged || !listEl?.children?.length)' in _APP_JS


def test_consequential_review_actions_require_confirmation() -> None:
    assert "Quarantined files will be deleted." in _APP_JS
    assert "Accept all ${ids.length} pending review item" in _APP_JS
    assert "from this artist?" in _APP_JS
    assert "from this album?" in _APP_JS


def test_review_queue_has_select_all_action() -> None:
    index_html = (Path(__file__).resolve().parents[1] / "webUI" / "index.html").read_text(encoding="utf-8")
    assert 'id="review-select-all"' in index_html
    assert 'const reviewSelectAll = $("#review-select-all")' in _APP_JS
    assert "state.reviewSelectedIds = new Set(ids)" in _APP_JS


def test_album_artwork_does_not_wait_for_backend_probe() -> None:
    function_body = _APP_JS.split("async function fetchHomeAlbumCoverUrl(albumId)", 1)[1].split(
        "function buildHomeResultsStatusInfo", 1
    )[0]
    assert "Promise.resolve(directCoverUrl)" in function_body
    assert 'fetchJson(`/api/music/album/art/' not in function_body
