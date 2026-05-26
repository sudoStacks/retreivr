from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from metadata.tag_repair import repair_music_library_tags


def test_repair_music_library_tags_uses_download_job_metadata(tmp_path: Path, monkeypatch) -> None:
    music_root = tmp_path / "Music"
    track = music_root / "Artist" / "Album" / "01 - Dirty Title.mp3"
    track.parent.mkdir(parents=True)
    track.write_bytes(b"audio")

    db_path = tmp_path / "db.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE download_jobs (
                id TEXT PRIMARY KEY,
                status TEXT,
                file_path TEXT,
                output_template TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO download_jobs (id, status, file_path, output_template, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "job-1",
                "completed",
                str(track),
                json.dumps(
                    {
                        "canonical_metadata": {
                            "title": "Clean Title",
                            "artist": "Artist",
                            "album": "Album",
                            "album_artist": "Artist",
                            "track_number": 1,
                            "recording_mbid": "rec-1",
                        }
                    }
                ),
                "2026-05-26T00:00:00Z",
            ),
        )

    monkeypatch.setattr(
        "metadata.tag_repair.read_music_tags",
        lambda _path: {"title": "01 - Dirty Title", "recording_id": "rec-1"},
    )
    applied = {}

    def _apply(path, tags, artwork, **kwargs):
        applied["path"] = path
        applied["tags"] = tags
        applied["artwork"] = artwork
        applied["kwargs"] = kwargs

    monkeypatch.setattr("metadata.tag_repair.apply_tags", _apply)

    result = repair_music_library_tags(
        {"music": {"library_path": str(music_root)}},
        db_path=db_path,
        dry_run=False,
    )

    assert result["repaired"] == 1
    assert applied["path"] == str(track)
    assert applied["tags"]["title"] == "Clean Title"
    assert applied["tags"]["recording_id"] == "rec-1"
    assert result["items"][0]["metadata_source"] == "download_jobs"


def test_repair_music_library_tags_dry_run_falls_back_to_clean_filename(tmp_path: Path, monkeypatch) -> None:
    music_root = tmp_path / "Music"
    track = music_root / "Artist" / "Album" / "02 - Filename Title.mp3"
    track.parent.mkdir(parents=True)
    track.write_bytes(b"audio")

    monkeypatch.setattr("metadata.tag_repair.read_music_tags", lambda _path: {})

    result = repair_music_library_tags(
        {"music": {"library_path": str(music_root)}},
        dry_run=True,
    )

    assert result["repaired"] == 1
    assert result["items"][0]["new_title"] == "Filename Title"
    assert result["items"][0]["dry_run"] is True
