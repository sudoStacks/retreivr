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

        header_map = {str(name).strip().lower(): idx for idx, name in enumerate(header)}
        artist_idx = header_map.get("artist")
        title_idx = header_map.get("title")
        album_idx = header_map.get("album")
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
