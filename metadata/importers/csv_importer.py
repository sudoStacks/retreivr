from __future__ import annotations

import csv
import io

from .base import BaseImporter, TrackIntent


class CSVImporter(BaseImporter):
    SOURCE_FORMAT = "csv"

    def parse(self, file_bytes: bytes) -> list[TrackIntent]:
        text = file_bytes.decode("utf-8-sig")
        stream = io.StringIO(text, newline="")
        reader = csv.reader(stream)

        try:
            header = next(reader)
        except StopIteration:
            return []

        header_map = {_normalize_header(name): idx for idx, name in enumerate(header)}
        artist_idx = _first_index(header_map, "artist", "artists", "artist name", "artist names", "artist name(s)")
        title_idx = _first_index(header_map, "title", "track", "track title", "track name", "name")
        album_idx = _first_index(header_map, "album", "album title", "album name")
        has_named_columns = any(idx is not None for idx in (artist_idx, title_idx, album_idx))

        intents: list[TrackIntent] = []
        for row in reader:
            if not row:
                continue
            row_text = ",".join(row)
            if has_named_columns:
                artist = _get_value(row, artist_idx)
                title = _get_value(row, title_idx)
                album = _get_value(row, album_idx)
                intents.append(
                    TrackIntent(
                        artist=artist,
                        title=title,
                        album=album,
                        raw_line=row_text,
                        source_format=self.SOURCE_FORMAT,
                        album_artist=_get_value(row, _first_index(header_map, "album artist", "albumartist")),
                        track_number=_safe_int(
                            _get_value(
                                row,
                                _first_index(header_map, "track number", "track no", "track #", "tracknumber"),
                            )
                        ),
                        disc_number=_safe_int(
                            _get_value(
                                row,
                                _first_index(header_map, "disc number", "disc no", "disc #", "discnumber"),
                            )
                        ),
                        release_date=_get_value(
                            row,
                            _first_index(header_map, "release date", "released", "date", "year"),
                        ),
                        genre=_get_value(row, _first_index(header_map, "genre", "genres")),
                        duration_ms=_duration_ms(row, header_map),
                    )
                )
            else:
                intents.append(
                    TrackIntent(
                        artist=None,
                        title=None,
                        album=None,
                        raw_line=row_text,
                        source_format=self.SOURCE_FORMAT,
                    )
                )
        return intents


def _get_value(row: list[str], idx: int | None) -> str | None:
    if idx is None:
        return None
    if idx < 0 or idx >= len(row):
        return None
    value = str(row[idx]).strip()
    return value or None


def _normalize_header(value: object) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").replace("-", " ").split())


def _first_index(header_map: dict[str, int], *names: str) -> int | None:
    for name in names:
        key = _normalize_header(name)
        if key in header_map:
            return header_map[key]
    return None


def _safe_int(value: object) -> int | None:
    try:
        parsed = int(float(str(value or "").strip()))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _duration_ms(row: list[str], header_map: dict[str, int]) -> int | None:
    millis = _safe_int(_get_value(row, _first_index(header_map, "duration ms", "duration (ms)", "milliseconds")))
    if millis is not None:
        return millis
    seconds_value = _get_value(row, _first_index(header_map, "duration seconds", "duration sec", "seconds"))
    try:
        seconds = float(str(seconds_value or "").strip())
    except (TypeError, ValueError):
        return None
    return int(round(seconds * 1000)) if seconds > 0 else None
