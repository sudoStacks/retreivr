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
            title = _coerce(record.get("title"))
            album = _coerce(record.get("album"))
            raw_line = " | ".join(part for part in (artist, title, album) if part) or json.dumps(record, sort_keys=True)
            intents.append(
                TrackIntent(
                    artist=artist,
                    title=title,
                    album=album,
                    raw_line=raw_line,
                    source_format=self.SOURCE_FORMAT,
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
    text = str(value).strip()
    return text or None
