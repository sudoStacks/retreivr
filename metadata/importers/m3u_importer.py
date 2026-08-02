from __future__ import annotations

from pathlib import Path

from .base import BaseImporter, TrackIntent


class M3UImporter(BaseImporter):
    SOURCE_FORMAT = "m3u"

    def parse(self, file_bytes: bytes) -> list[TrackIntent]:
        text = file_bytes.decode("utf-8-sig")
        intents: list[TrackIntent] = []
        pending_artist: str | None = None
        pending_title: str | None = None
        pending_duration_ms: int | None = None

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.upper() == "#EXTM3U":
                continue
            if line.startswith("#EXTINF"):
                pending_artist, pending_title, pending_duration_ms = _parse_extinf(line)
                continue
            if line.startswith("#"):
                continue

            if pending_artist or pending_title:
                artist = pending_artist
                title = pending_title
                raw_value = raw_line
            else:
                base = Path(line).name or line
                artist, title = _split_artist_title(base)
                raw_value = base

            intents.append(
                TrackIntent(
                    artist=artist,
                    title=title,
                    album=None,
                    raw_line=raw_value,
                    source_format=self.SOURCE_FORMAT,
                    duration_ms=pending_duration_ms,
                )
            )
            pending_artist = None
            pending_title = None
            pending_duration_ms = None

        return intents


def _parse_extinf(line: str) -> tuple[str | None, str | None, int | None]:
    # #EXTINF:<seconds>,Artist - Title
    parts = line.split(",", 1)
    if len(parts) != 2:
        return None, None, None
    duration_ms = None
    try:
        seconds = float(parts[0].split(":", 1)[1].strip())
        if seconds > 0:
            duration_ms = int(round(seconds * 1000))
    except (IndexError, TypeError, ValueError):
        duration_ms = None
    artist, title = _split_artist_title(parts[1].strip())
    return artist, title, duration_ms


def _split_artist_title(value: str) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    cleaned = value.strip()
    if " - " in cleaned:
        left, right = cleaned.split(" - ", 1)
        artist = left.strip() or None
        title = right.strip() or None
        return artist, title
    return None, cleaned or None
