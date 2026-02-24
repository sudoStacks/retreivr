from __future__ import annotations

from .apple_xml_importer import AppleXMLImporter
from .base import BaseImporter, TrackIntent
from .csv_importer import CSVImporter
from .m3u_importer import M3UImporter
from .soundizz_json_importer import SoundiizJSONImporter


def detect_format(filename: str, file_bytes: bytes) -> BaseImporter:
    lower_name = str(filename or "").strip().lower()

    if lower_name.endswith((".m3u", ".m3u8")):
        return M3UImporter()
    if lower_name.endswith(".csv"):
        return CSVImporter()
    if lower_name.endswith(".json"):
        return SoundiizJSONImporter()
    if lower_name.endswith((".xml", ".plist")):
        return AppleXMLImporter()

    sniff = file_bytes.lstrip()[:200].lower()
    if sniff.startswith(b"#extm3u"):
        return M3UImporter()
    if sniff.startswith(b"{") or sniff.startswith(b"["):
        return SoundiizJSONImporter()
    if sniff.startswith(b"<?xml") or b"<plist" in sniff:
        return AppleXMLImporter()

    raise ValueError(f"unsupported playlist format: {filename}")


def import_playlist(file_bytes: bytes, filename: str) -> list[TrackIntent]:
    importer = detect_format(filename, file_bytes)
    return importer.parse(file_bytes)
