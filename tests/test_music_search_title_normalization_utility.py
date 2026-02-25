from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_title_normalization():
    if "engine" not in sys.modules:
        engine_pkg = types.ModuleType("engine")
        engine_pkg.__path__ = [str(_ROOT / "engine")]  # type: ignore[attr-defined]
        sys.modules["engine"] = engine_pkg
    return _load_module("engine.music_title_normalization", _ROOT / "engine" / "music_title_normalization.py")


def test_relaxed_search_title_strips_known_parenthetical_tags() -> None:
    norm = _load_title_normalization()
    raw = "Young (Live) (Deluxe Edition) (Remastered 2019)"
    assert norm.relaxed_search_title(raw) == "Young"


def test_relaxed_search_title_preserves_unknown_parenthetical_context() -> None:
    norm = _load_title_normalization()
    raw = "Song Title (From the Motion Picture)"
    assert norm.relaxed_search_title(raw) == "Song Title From the Motion Picture"


def test_extract_parenthetical_tags_detects_known_tags() -> None:
    norm = _load_title_normalization()
    raw = "Track (Live) [Deluxe Edition] {Remastered 2011}"
    tags = norm.extract_parenthetical_tags(raw)
    assert tags == {"live", "deluxe", "remastered"}


def test_has_live_intent_detects_live_in_parenthetical() -> None:
    norm = _load_title_normalization()
    assert norm.has_live_intent("Artist", "Song (Live)", "Album") is True
