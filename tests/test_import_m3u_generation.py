from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

_MODULE_PATH = Path(__file__).resolve().parent.parent / "engine" / "import_m3u_builder.py"
_SPEC = importlib.util.spec_from_file_location("engine_import_m3u_builder", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
write_import_m3u = _MODULE.write_import_m3u


def test_write_import_m3u_writes_header_and_resolved_paths(tmp_path) -> None:
    playlist_root = tmp_path / "Playlists"
    resolved_paths = [
        "/downloads/Music/Artist/Album (2020)/Disc 1/01 - Track One.mp3",
        "/downloads/Music/Artist/Album (2020)/Disc 1/02 - Track Two.mp3",
    ]

    m3u_path = write_import_m3u(
        import_name="My Import",
        resolved_track_paths=resolved_paths,
        playlist_root=playlist_root,
    )

    assert m3u_path == playlist_root / "My Import.m3u"
    assert m3u_path.exists() is True
    assert m3u_path.read_text(encoding="utf-8").splitlines() == [
        "#EXTM3U",
        "/downloads/Music/Artist/Album (2020)/Disc 1/01 - Track One.mp3",
        "/downloads/Music/Artist/Album (2020)/Disc 1/02 - Track Two.mp3",
    ]


def test_write_import_m3u_skips_empty_and_deduplicates(tmp_path) -> None:
    playlist_root = tmp_path / "Playlists"
    m3u_path = write_import_m3u(
        import_name="Batch",
        resolved_track_paths=[
            "",
            "   ",
            "/downloads/Music/A/Alpha/Disc 1/01 - A.mp3",
            "/downloads/Music/A/Alpha/Disc 1/01 - A.mp3",
        ],
        playlist_root=playlist_root,
    )

    assert m3u_path.read_text(encoding="utf-8").splitlines() == [
        "#EXTM3U",
        "/downloads/Music/A/Alpha/Disc 1/01 - A.mp3",
    ]
