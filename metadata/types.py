"""Structured metadata types for music processing."""

from __future__ import annotations


class MusicMetadata:
    """Validated, structured music metadata container."""

    title: str
    artist: str
    album: str
    album_artist: str
    track_num: int
    disc_num: int
    date: str
    genre: str
    isrc: str | None
    mbid: str | None
    artwork: bytes | None
    lyrics: str | None

    def __init__(
        self,
        *,
        title: str,
        artist: str,
        album: str,
        album_artist: str,
        track_num: int,
        disc_num: int,
        date: str,
        genre: str,
        isrc: str | None = None,
        mbid: str | None = None,
        artwork: bytes | None = None,
        lyrics: str | None = None,
    ) -> None:
        """Initialize and validate metadata values."""
        self.title = self._require_non_empty_str("title", title)
        self.artist = self._require_non_empty_str("artist", artist)
        self.album = self._require_non_empty_str("album", album)
        self.album_artist = self._require_non_empty_str("album_artist", album_artist)
        self.track_num = self._require_positive_int("track_num", track_num)
        self.disc_num = self._require_positive_int("disc_num", disc_num)
        self.date = self._require_non_empty_str("date", date)
        self.genre = self._require_non_empty_str("genre", genre)
        self.isrc = self._optional_str("isrc", isrc)
        self.mbid = self._optional_str("mbid", mbid)
        self.artwork = self._optional_bytes("artwork", artwork)
        self.lyrics = self._optional_str("lyrics", lyrics)

    @staticmethod
    def _require_non_empty_str(field: str, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a string")
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(f"{field} must be a non-empty string")
        return cleaned

    @staticmethod
    def _require_positive_int(field: str, value: int) -> int:
        if not isinstance(value, int):
            raise TypeError(f"{field} must be an integer")
        if value <= 0:
            raise ValueError(f"{field} must be > 0")
        return value

    @staticmethod
    def _optional_str(field: str, value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a string or None")
        cleaned = value.strip()
        return cleaned or None

    @staticmethod
    def _optional_bytes(field: str, value: bytes | None) -> bytes | None:
        if value is None:
            return None
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError(f"{field} must be bytes or None")
        return bytes(value)

    def __repr__(self) -> str:
        """Return a concise debug representation of this metadata."""
        return (
            "MusicMetadata("
            f"title={self.title!r}, artist={self.artist!r}, album={self.album!r}, "
            f"album_artist={self.album_artist!r}, track_num={self.track_num!r}, "
            f"disc_num={self.disc_num!r}, date={self.date!r}, genre={self.genre!r}, "
            f"isrc={self.isrc!r}, mbid={self.mbid!r}, "
            f"artwork={'<bytes>' if self.artwork is not None else None}, "
            f"lyrics={self.lyrics!r})"
        )


class CanonicalMetadata(MusicMetadata):
    """Canonical structured metadata model used across runtime pipelines."""

    def __repr__(self) -> str:
        return (
            "CanonicalMetadata("
            f"title={self.title!r}, artist={self.artist!r}, album={self.album!r}, "
            f"album_artist={self.album_artist!r}, track_num={self.track_num!r}, "
            f"disc_num={self.disc_num!r}, date={self.date!r}, genre={self.genre!r}, "
            f"isrc={self.isrc!r}, mbid={self.mbid!r}, "
            f"artwork={'<bytes>' if self.artwork is not None else None}, "
            f"lyrics={self.lyrics!r})"
        )


# Backward-compatible alias for existing imports.
MusicMetadata = CanonicalMetadata

__all__ = ["CanonicalMetadata", "MusicMetadata"]
