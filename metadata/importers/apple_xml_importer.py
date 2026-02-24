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
            raw_line = " | ".join(part for part in (artist, title, album) if part) or ""
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
