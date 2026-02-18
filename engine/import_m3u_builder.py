from __future__ import annotations

from pathlib import Path
from typing import Iterable

from playlist.export import sanitize_playlist_name


def write_import_m3u(
    *,
    import_name: str,
    resolved_track_paths: Iterable[str],
    playlist_root: str | Path = "/Playlists",
) -> Path:
    root = Path(playlist_root)
    root.mkdir(parents=True, exist_ok=True)

    safe_name = sanitize_playlist_name(import_name) or "import"
    target_path = root / f"{safe_name}.m3u"
    temp_path = root / f".{safe_name}.m3u.tmp"

    lines: list[str] = ["#EXTM3U"]
    seen: set[str] = set()
    for path in resolved_track_paths:
        text = str(path or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        lines.append(Path(text).as_posix())

    temp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temp_path.replace(target_path)
    return target_path
