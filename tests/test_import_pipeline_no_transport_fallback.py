from __future__ import annotations

from pathlib import Path


def test_import_pipeline_has_no_generic_transport_search_references() -> None:
    source = (Path(__file__).resolve().parent.parent / "engine" / "import_pipeline.py").read_text(
        encoding="utf-8"
    )
    forbidden_tokens = (
        "SearchResolutionService",
        "search_engine",
        "search_adapter",
        "adapters",
        "music.youtube.com/search?q=",
        "youtube.com/search?q=",
    )
    for token in forbidden_tokens:
        assert token not in source
