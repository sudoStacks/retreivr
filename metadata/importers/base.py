from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class TrackIntent:
    artist: str | None
    title: str | None
    album: str | None
    raw_line: str
    source_format: str
    album_artist: str | None = None
    track_number: int | None = None
    disc_number: int | None = None
    release_date: str | None = None
    genre: str | None = None
    duration_ms: int | None = None


class BaseImporter(ABC):
    @abstractmethod
    def parse(self, file_bytes: bytes) -> list[TrackIntent]:
        """Parse playlist file bytes into normalized track intents."""
        raise NotImplementedError
