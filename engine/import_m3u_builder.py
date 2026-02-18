from __future__ import annotations

import sqlite3
from pathlib import Path

from playlist.export import sanitize_playlist_name


def write_import_m3u_from_batch(
    *,
    import_batch_id: str,
    playlist_name: str,
    db_path: str,
    playlist_root: str | Path = "/Playlists",
) -> int:
    batch_id = str(import_batch_id or "").strip()
    if not batch_id:
        return 0

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT output_path
            FROM download_history
            WHERE import_batch_id = ?
              AND status = 'completed'
            ORDER BY source_index ASC
            """,
            (batch_id,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    entries: list[str] = []
    seen: set[str] = set()
    for row in rows:
        output_path = str(row["output_path"] or "").strip()
        if not output_path or output_path in seen:
            continue
        seen.add(output_path)
        entries.append(Path(output_path).as_posix())
    if not entries:
        return 0

    root = Path(playlist_root)
    root.mkdir(parents=True, exist_ok=True)

    safe_name = sanitize_playlist_name(playlist_name) or "import"
    target_path = root / f"{safe_name}.m3u"
    temp_path = root / f".{safe_name}.m3u.tmp"

    lines = ["#EXTM3U", *entries]
    temp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temp_path.replace(target_path)
    return len(entries)
