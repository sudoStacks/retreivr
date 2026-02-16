"""SQLite persistence for optional Spotify OAuth tokens."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class SpotifyOAuthToken:
    access_token: str
    refresh_token: str
    expires_at: int  # epoch seconds
    scope: str


class SpotifyOAuthStore:
    """Single-row SQLite storage for Spotify OAuth tokens."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        """Create token table when it does not already exist."""
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS spotify_oauth_tokens (
                    id INTEGER PRIMARY KEY,
                    access_token TEXT NOT NULL,
                    refresh_token TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def save(self, token: SpotifyOAuthToken) -> None:
        """Upsert a single token row using fixed key ``id=1``."""
        updated_at = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO spotify_oauth_tokens (id, access_token, refresh_token, expires_at, scope, updated_at)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    access_token=excluded.access_token,
                    refresh_token=excluded.refresh_token,
                    expires_at=excluded.expires_at,
                    scope=excluded.scope,
                    updated_at=excluded.updated_at
                """,
                (
                    token.access_token,
                    token.refresh_token,
                    int(token.expires_at),
                    token.scope,
                    updated_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def load(self) -> Optional[SpotifyOAuthToken]:
        """Load token from row ``id=1``; return ``None`` when absent."""
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT access_token, refresh_token, expires_at, scope
                FROM spotify_oauth_tokens
                WHERE id=1
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            return SpotifyOAuthToken(
                access_token=str(row["access_token"]),
                refresh_token=str(row["refresh_token"]),
                expires_at=int(row["expires_at"]),
                scope=str(row["scope"]),
            )
        finally:
            conn.close()

    def clear(self) -> None:
        """Delete stored token row."""
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM spotify_oauth_tokens WHERE id=1")
            conn.commit()
        finally:
            conn.close()
