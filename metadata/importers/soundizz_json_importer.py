from __future__ import annotations

import json

from .base import BaseImporter, TrackIntent


class SoundiizJSONImporter(BaseImporter):
    SOURCE_FORMAT = "soundiiz_json"

    def parse(self, file_bytes: bytes) -> list[TrackIntent]:
        payload = json.loads(file_bytes.decode("utf-8-sig"))
        records = _extract_records(payload)

        intents: list[TrackIntent] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            artist = _coerce(record.get("artist"))
            if not artist:
                artist = _coerce(record.get("artists"))
            title = _coerce(record.get("title")) or _coerce(record.get("name"))
            album = _coerce(record.get("album"))
            raw_line = " | ".join(part for part in (artist, title, album) if part) or json.dumps(
                record,
                sort_keys=True,
            )
            intents.append(
                TrackIntent(
                    artist=artist,
                    title=title,
                    album=album,
                    raw_line=raw_line,
                    source_format=self.SOURCE_FORMAT,
                    album_artist=_coerce(record.get("albumArtist") or record.get("album_artist")),
                    track_number=_safe_int(record.get("trackNumber") or record.get("track_number")),
                    disc_number=_safe_int(record.get("discNumber") or record.get("disc_number")),
                    release_date=_coerce(
                        record.get("releaseDate")
                        or record.get("release_date")
                        or record.get("year")
                    ),
                    genre=_coerce(record.get("genre") or record.get("genres")),
                    duration_ms=_duration_ms(record),
                )
            )
        return intents


def _extract_records(payload: object) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        tracks = payload.get("tracks")
        if isinstance(tracks, list):
            return tracks
        items = payload.get("items")
        if isinstance(items, list):
            return items
    return []


def _coerce(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return _coerce(value.get("name") or value.get("title"))
    if isinstance(value, list):
        values = [_coerce(item) for item in value]
        text = ", ".join(item for item in values if item)
        return text or None
    text = str(value).strip()
    return text or None


def _safe_int(value: object) -> int | None:
    try:
        parsed = int(float(str(value or "").strip()))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _duration_ms(record: dict) -> int | None:
    millis = _safe_int(record.get("durationMs") or record.get("duration_ms"))
    if millis is not None:
        return millis
    try:
        seconds = float(str(record.get("duration") or record.get("durationSeconds") or "").strip())
    except (TypeError, ValueError):
        return None
    return int(round(seconds * 1000)) if seconds > 0 else None
