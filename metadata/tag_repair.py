"""Repair missing or polluted local music title tags."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from metadata.tagger import apply_tags, clean_display_title, read_music_tags

AUDIO_EXTENSIONS = {".mp3", ".m4a", ".mp4", ".m4b", ".flac", ".ogg", ".opus", ".wav", ".aac"}


def repair_music_library_tags(
    config: dict[str, Any],
    *,
    db_path: str | Path | None = None,
    limit: int = 500,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Scan configured music roots and repair missing or filename-polluted titles."""
    roots = _music_roots(config)
    scanned = repaired = skipped = failed = 0
    items: list[dict[str, Any]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if scanned >= limit:
                return {
                    "scanned": scanned,
                    "repaired": repaired,
                    "skipped": skipped,
                    "failed": failed,
                    "items": items,
                }
            if not path.is_file() or path.suffix.lower() not in AUDIO_EXTENSIONS:
                continue
            scanned += 1
            result = _repair_one(path, root=root, db_path=db_path, dry_run=dry_run)
            items.append(result)
            status = result.get("status")
            if status == "repaired":
                repaired += 1
            elif status == "failed":
                failed += 1
            else:
                skipped += 1
    return {"scanned": scanned, "repaired": repaired, "skipped": skipped, "failed": failed, "items": items}


def _repair_one(path: Path, *, root: Path, db_path: str | Path | None, dry_run: bool) -> dict[str, Any]:
    try:
        resolved = path.resolve()
        if not _is_under_root(resolved, root.resolve()):
            return {"path": str(path), "status": "skipped", "reason": "outside_root"}
        existing = read_music_tags(str(resolved))
        current_title = str(existing.get("title") or "").strip()
        clean_current_title = clean_display_title(current_title)
        needs_title = not current_title or current_title != clean_current_title
        if not needs_title:
            return {"path": str(resolved), "status": "skipped", "reason": "title_ok"}

        metadata = _lookup_retreivr_metadata(db_path, resolved, existing) if db_path else None
        fallback_title = clean_display_title(path.stem)
        title = clean_display_title((metadata or {}).get("title") or fallback_title)
        if not title:
            return {"path": str(resolved), "status": "skipped", "reason": "no_title_candidate"}

        tags = {
            "title": title,
            "artist": (metadata or {}).get("artist") or existing.get("artist"),
            "album": (metadata or {}).get("album") or existing.get("album"),
            "album_artist": (metadata or {}).get("album_artist") or existing.get("album_artist"),
            "track_number": (metadata or {}).get("track_number") or existing.get("track_number"),
            "disc_number": (metadata or {}).get("disc_number") or existing.get("disc_number"),
            "recording_id": (metadata or {}).get("recording_id") or existing.get("recording_id"),
            "mb_release_id": (metadata or {}).get("mb_release_id") or existing.get("mb_release_id"),
            "mb_release_group_id": (metadata or {}).get("mb_release_group_id") or existing.get("mb_release_group_id"),
        }
        if not dry_run:
            apply_tags(str(resolved), tags, artwork=None, allow_overwrite=True, dry_run=False)
        return {
            "path": str(resolved),
            "status": "repaired",
            "dry_run": dry_run,
            "old_title": current_title or None,
            "new_title": title,
            "metadata_source": (metadata or {}).get("_source") or "filename",
            "recording_mbid": tags.get("recording_id"),
        }
    except Exception as exc:
        return {"path": str(path), "status": "failed", "reason": str(exc)}


def _music_roots(config: dict[str, Any]) -> list[Path]:
    roots: list[Path] = []
    for key in ("music_library_path", "music_download_folder"):
        value = config.get(key)
        if value:
            roots.append(Path(str(value)).expanduser())
    music_cfg = config.get("music") if isinstance(config.get("music"), dict) else {}
    for key in ("library_path", "download_folder"):
        value = music_cfg.get(key)
        if value:
            roots.append(Path(str(value)).expanduser())
    seen: set[str] = set()
    unique: list[Path] = []
    for root in roots:
        marker = os.path.abspath(str(root))
        if marker not in seen:
            seen.add(marker)
            unique.append(root)
    return unique


def _is_under_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _lookup_retreivr_metadata(db_path: str | Path | None, path: Path, existing: dict[str, Any]) -> dict[str, Any] | None:
    if not db_path or not Path(db_path).exists():
        return None
    recording_mbid = str(existing.get("recording_id") or "").strip()
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        by_job = _lookup_download_job_metadata(conn, path, recording_mbid)
        if by_job:
            return by_job
        if recording_mbid:
            return _lookup_import_item_metadata(conn, recording_mbid)
    return None


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)).fetchone()
    return bool(row)


def _lookup_download_job_metadata(conn: sqlite3.Connection, path: Path, recording_mbid: str) -> dict[str, Any] | None:
    if not _table_exists(conn, "download_jobs"):
        return None
    rows = conn.execute(
        """
        SELECT file_path, output_template, updated_at
        FROM download_jobs
        WHERE status='completed'
          AND (
            file_path=?
            OR output_template LIKE ?
          )
        ORDER BY updated_at DESC
        LIMIT 25
        """,
        (str(path), f"%{recording_mbid}%" if recording_mbid else "\u0000"),
    ).fetchall()
    for row in rows:
        metadata = _metadata_from_output_template(row["output_template"], recording_mbid)
        if metadata:
            metadata["_source"] = "download_jobs"
            return metadata
    return None


def _metadata_from_output_template(raw: str | None, recording_mbid: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw or "{}")
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    metadata = payload.get("canonical_metadata") if isinstance(payload.get("canonical_metadata"), dict) else payload
    mbid = str(
        metadata.get("recording_mbid")
        or metadata.get("mb_recording_id")
        or metadata.get("recording_id")
        or ""
    ).strip()
    if recording_mbid and mbid and mbid.lower() != recording_mbid.lower():
        return None
    return _normalize_metadata_dict(metadata)


def _lookup_import_item_metadata(conn: sqlite3.Connection, recording_mbid: str) -> dict[str, Any] | None:
    if not _table_exists(conn, "import_batch_items"):
        return None
    row = conn.execute(
        """
        SELECT title, artist, album, album_artist, recording_mbid, mb_release_id, mb_release_group_id, input_metadata_json
        FROM import_batch_items
        WHERE lower(recording_mbid)=lower(?)
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (recording_mbid,),
    ).fetchone()
    if not row:
        return None
    merged = dict(row)
    try:
        source = json.loads(row["input_metadata_json"] or "{}")
        if isinstance(source, dict):
            merged = {**source, **merged}
    except Exception:
        pass
    metadata = _normalize_metadata_dict(merged)
    metadata["_source"] = "import_batch_items"
    return metadata


def _normalize_metadata_dict(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": source.get("title") or source.get("track"),
        "artist": source.get("artist"),
        "album": source.get("album"),
        "album_artist": source.get("album_artist") or source.get("albumArtist"),
        "track_number": source.get("track_number") or source.get("track_num"),
        "disc_number": source.get("disc_number") or source.get("disc_num"),
        "recording_id": source.get("recording_mbid") or source.get("mb_recording_id") or source.get("recording_id"),
        "mb_release_id": source.get("mb_release_id"),
        "mb_release_group_id": source.get("mb_release_group_id"),
    }
