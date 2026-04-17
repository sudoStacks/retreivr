"""Persistence helpers for saved Movies & TV titles."""

from __future__ import annotations

import sqlite3
from typing import Any

from db.migrations import ensure_saved_titles_table
from engine.paths import DB_PATH


def _normalize_kind(kind: str) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized in {"movie", "movies"}:
        return "movie"
    if normalized in {"tv", "show", "shows", "series"}:
        return "tv"
    raise ValueError("kind must be movie or tv")


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or str(DB_PATH), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    ensure_saved_titles_table(conn)
    return conn


def _row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "kind": str(row["kind"]),
        "tmdb_id": int(row["tmdb_id"]),
        "title": str(row["title"] or "").strip(),
        "original_title": str(row["original_title"] or "").strip(),
        "year": str(row["year"] or "").strip(),
        "poster_url": str(row["poster_url"] or "").strip(),
        "overview": str(row["overview"] or "").strip(),
        "tmdb_url": str(row["tmdb_url"] or "").strip(),
        "language": str(row["language"] or "").strip(),
        "popularity": float(row["popularity"]) if row["popularity"] is not None else None,
        "rating": float(row["rating"]) if row["rating"] is not None else None,
        "saved": True,
        "saved_at": str(row["saved_at"] or "").strip(),
    }


class SavedTitleStore:
    """Simple persistence wrapper for Movies & TV saved titles."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        return _connect(self.db_path)

    def ensure_schema(self) -> None:
        conn = self._connect()
        conn.close()

    def get_saved_status_map(self, kind: str, tmdb_ids: list[int | str]) -> dict[str, bool]:
        normalized = _normalize_kind(kind)
        wanted = sorted({int(value) for value in tmdb_ids if str(value or "").strip().isdigit()})
        if not wanted:
            return {}
        placeholders = ", ".join("?" for _ in wanted)
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT tmdb_id
                FROM saved_titles
                WHERE kind=? AND tmdb_id IN ({placeholders})
                """,
                (normalized, *wanted),
            )
            return {str(int(row["tmdb_id"])): True for row in cur.fetchall()}
        finally:
            conn.close()

    def list_saved_titles(self, kind: str, *, limit: int = 10) -> list[dict[str, Any]]:
        normalized = _normalize_kind(kind)
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT kind, tmdb_id, title, original_title, year, poster_url, overview,
                       tmdb_url, language, popularity, rating, saved_at
                FROM saved_titles
                WHERE kind=?
                ORDER BY datetime(saved_at) DESC, id DESC
                LIMIT ?
                """,
                (normalized, max(1, int(limit))),
            )
            return [_row_to_item(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def save_title(self, kind: str, item: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_kind(kind)
        try:
            tmdb_id = int(item.get("tmdb_id"))
        except Exception as exc:  # noqa: BLE001
            raise ValueError("tmdb_id is required") from exc
        title = str(item.get("title") or "").strip()
        if not title:
            raise ValueError("title is required")
        record = {
            "kind": normalized,
            "tmdb_id": tmdb_id,
            "title": title,
            "original_title": str(item.get("original_title") or "").strip(),
            "year": str(item.get("year") or "").strip(),
            "poster_url": str(item.get("poster_url") or "").strip(),
            "overview": str(item.get("overview") or "").strip(),
            "tmdb_url": str(item.get("tmdb_url") or "").strip(),
            "language": str(item.get("language") or "").strip(),
            "popularity": float(item.get("popularity")) if item.get("popularity") not in (None, "") else None,
            "rating": float(item.get("rating")) if item.get("rating") not in (None, "") else None,
        }
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO saved_titles (
                    kind, tmdb_id, title, original_title, year, poster_url,
                    overview, tmdb_url, language, popularity, rating, saved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(kind, tmdb_id) DO UPDATE SET
                    title=excluded.title,
                    original_title=excluded.original_title,
                    year=excluded.year,
                    poster_url=excluded.poster_url,
                    overview=excluded.overview,
                    tmdb_url=excluded.tmdb_url,
                    language=excluded.language,
                    popularity=excluded.popularity,
                    rating=excluded.rating,
                    saved_at=CURRENT_TIMESTAMP
                """,
                (
                    record["kind"],
                    record["tmdb_id"],
                    record["title"],
                    record["original_title"],
                    record["year"],
                    record["poster_url"],
                    record["overview"],
                    record["tmdb_url"],
                    record["language"],
                    record["popularity"],
                    record["rating"],
                ),
            )
            conn.commit()
            cur.execute(
                """
                SELECT kind, tmdb_id, title, original_title, year, poster_url, overview,
                       tmdb_url, language, popularity, rating, saved_at
                FROM saved_titles
                WHERE kind=? AND tmdb_id=?
                LIMIT 1
                """,
                (normalized, tmdb_id),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("saved title write failed")
            return _row_to_item(row)
        finally:
            conn.close()

    def remove_title(self, kind: str, tmdb_id: int | str) -> bool:
        normalized = _normalize_kind(kind)
        numeric = int(tmdb_id)
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM saved_titles WHERE kind=? AND tmdb_id=?",
                (normalized, numeric),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
