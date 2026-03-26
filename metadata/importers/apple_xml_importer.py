from __future__ import annotations

import xml.etree.ElementTree as ET

from .base import BaseImporter, TrackIntent


class AppleXMLImporter(BaseImporter):
    SOURCE_FORMAT = "apple_xml"

    def parse(self, file_bytes: bytes) -> list[TrackIntent]:
        root = ET.fromstring(file_bytes)
        plist_value = _parse_plist_node(root)
        tracks_container = _extract_tracks_container(plist_value)

        intents: list[TrackIntent] = []
        for _, track_data in tracks_container.items():
            if not isinstance(track_data, dict):
                continue
            artist = _clean(track_data.get("Artist"))
            title = _clean(track_data.get("Name"))
            album = _clean(track_data.get("Album"))
            album_artist = _clean(track_data.get("Album Artist"))
            track_number = _safe_int(track_data.get("Track Number"))
            disc_number = _safe_int(track_data.get("Disc Number"))
            release_date = _clean(track_data.get("Year")) or _clean(track_data.get("Date Added"))
            genre = _clean(track_data.get("Genre"))
            total_time = _safe_int(track_data.get("Total Time"))
            raw_line = " | ".join(part for part in (artist, title, album) if part) or ""
            intents.append(
                TrackIntent(
                    artist=artist,
                    title=title,
                    album=album,
                    raw_line=raw_line,
                    source_format=self.SOURCE_FORMAT,
                    album_artist=album_artist,
                    track_number=track_number,
                    disc_number=disc_number,
                    release_date=release_date,
                    genre=genre,
                    duration_ms=total_time,
                )
            )
        return intents


def _extract_tracks_container(parsed: object) -> dict:
    if not isinstance(parsed, dict):
        return {}
    tracks = parsed.get("Tracks")
    if isinstance(tracks, dict):
        return tracks
    return {}


def _parse_plist_node(element: ET.Element):
    if element.tag == "plist":
        children = list(element)
        if not children:
            return {}
        return _parse_plist_node(children[0])

    if element.tag == "dict":
        result = {}
        children = list(element)
        i = 0
        while i < len(children):
            key_elem = children[i]
            if key_elem.tag != "key":
                i += 1
                continue
            key = key_elem.text or ""
            value_elem = children[i + 1] if i + 1 < len(children) else None
            if value_elem is None:
                result[key] = None
                break
            result[key] = _parse_plist_node(value_elem)
            i += 2
        return result

    if element.tag == "array":
        return [_parse_plist_node(child) for child in list(element)]

    if element.tag in {"string", "date", "data", "key"}:
        return element.text or ""

    if element.tag in {"integer", "real"}:
        text = (element.text or "").strip()
        return text

    if element.tag == "true":
        return True
    if element.tag == "false":
        return False

    return element.text or ""


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
