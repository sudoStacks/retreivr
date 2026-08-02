from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from playlist.export import sanitize_playlist_name


def resolve_import_playlist_root(config: dict | None) -> Path:
    cfg = config if isinstance(config, dict) else {}
    music_root = Path(str(cfg.get("music_download_folder") or "Music"))
    return Path(
        str(
            cfg.get("playlists_folder")
            or cfg.get("playlist_export_folder")
            or (music_root / "Playlists")
        )
    )


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
        rows = []
        try:
            item_columns = {
                str(row[1])
                for row in cur.execute("PRAGMA table_info(import_batch_items)").fetchall()
            }
            metadata_select = ", ".join(
                f"i.{column} AS {column}" if column in item_columns else f"NULL AS {column}"
                for column in ("artist", "title", "album")
            )
            cur.execute(
                f"""
                SELECT i.source_index, i.linked_job_id, j.file_path AS output_path,
                       {metadata_select}
                FROM import_batch_items AS i
                JOIN download_jobs AS j ON j.id = i.linked_job_id
                WHERE i.batch_id = ?
                  AND j.status = 'completed'
                  AND COALESCE(j.file_path, '') <> ''
                ORDER BY i.source_index ASC
                """,
                (batch_id,),
            )
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            rows = []

        # Compatibility for batches created before import items were linked to
        # download jobs. New history rows also retain these fields for audit.
        if not rows:
            try:
                cur.execute(
                    """
                    SELECT source_index, NULL AS linked_job_id, output_path,
                           NULL AS artist, NULL AS title, NULL AS album
                    FROM download_history
                    WHERE import_batch_id = ?
                      AND status = 'completed'
                    ORDER BY source_index ASC
                    """,
                    (batch_id,),
                )
                rows = cur.fetchall()
            except sqlite3.OperationalError:
                rows = []
    finally:
        conn.close()

    _sync_player_playlist(
        db_path=str(db_path),
        import_batch_id=batch_id,
        playlist_name=playlist_name,
        rows=rows,
    )

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


def _sync_player_playlist(
    *,
    db_path: str,
    import_batch_id: str,
    playlist_name: str,
    rows: list[sqlite3.Row],
) -> None:
    from engine.music_player import ensure_music_player_tables

    safe_name = sanitize_playlist_name(playlist_name) or "Imported Playlist"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        ensure_music_player_tables(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM music_player_playlists WHERE import_batch_id=? LIMIT 1",
            (import_batch_id,),
        )
        existing = cur.fetchone()
        if existing:
            playlist_id = int(existing["id"])
            cur.execute(
                "UPDATE music_player_playlists SET name=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (safe_name, playlist_id),
            )
        else:
            cur.execute(
                "INSERT INTO music_player_playlists (name, import_batch_id) VALUES (?, ?)",
                (safe_name, import_batch_id),
            )
            playlist_id = int(cur.lastrowid)

        for row in rows:
            output_path = str(row["output_path"] or "").strip()
            if not output_path:
                continue
            path = Path(output_path)
            fallback_title = re.sub(r"^\d+\s*-\s*", "", path.stem).strip() or path.stem
            album_dir = path.parent.parent if path.parent.name.lower().startswith("disc ") else path.parent
            fallback_album = album_dir.name
            fallback_artist = album_dir.parent.name
            source_index = int(row["source_index"] or 0)
            item_id = (
                str(row["linked_job_id"] or "").strip()
                or f"import:{import_batch_id}:{source_index}"
            )
            item_values = (
                str(row["title"] or "").strip() or fallback_title,
                str(row["artist"] or "").strip() or fallback_artist,
                str(row["album"] or "").strip() or fallback_album,
                output_path,
                source_index,
            )
            existing_item = cur.execute(
                "SELECT id FROM music_player_playlist_items WHERE playlist_id=? AND item_id=? LIMIT 1",
                (playlist_id, item_id),
            ).fetchone()
            if existing_item:
                cur.execute(
                    """
                    UPDATE music_player_playlist_items
                    SET title=?, artist=?, album=?, local_path=?, source_kind='local', position=?
                    WHERE id=?
                    """,
                    (*item_values, int(existing_item["id"])),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO music_player_playlist_items (
                        playlist_id, item_id, title, artist, album, stream_url,
                        local_path, source_kind, position
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?, 'local', ?)
                    """,
                    (playlist_id, item_id, *item_values),
                )
        cur.execute(
            "UPDATE music_player_playlists SET updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (playlist_id,),
        )
        conn.commit()
    finally:
        conn.close()
