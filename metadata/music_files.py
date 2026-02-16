"""Music filename and tagging helpers."""

from __future__ import annotations

import mimetypes
import os
import re
from typing import Any

try:
    from mutagen.flac import FLAC, Picture
    from mutagen.id3 import APIC, ID3, TALB, TCON, TDRC, TIT2, TPE1, TPE2, TPOS, TRCK, TSRC, TXXX, USLT
except ImportError:  # pragma: no cover - exercised via unit-test monkeypatching
    FLAC = None
    Picture = None
    APIC = ID3 = TALB = TCON = TDRC = TIT2 = TPE1 = TPE2 = TPOS = TRCK = TSRC = TXXX = USLT = None

from metadata.music_metadata import MusicMetadata

_FS_FORBIDDEN_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
_WHITESPACE_RE = re.compile(r"\s+")


def build_music_filename(metadata: MusicMetadata) -> str:
    """Build a strict music filename in the form `01 - Track Title.mp3`."""
    track_num = metadata.track_num if isinstance(metadata.track_num, int) and metadata.track_num > 0 else 0
    track_label = f"{track_num:02d}"
    title = _sanitize_filename_component(metadata.title or "Unknown Title")
    return f"{track_label} - {title}.mp3"


def tag_music_file(path: str, metadata: MusicMetadata) -> None:
    """Write music tags and embedded artwork for MP3/FLAC files."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".mp3":
        _tag_mp3(path, metadata)
        return
    if ext == ".flac":
        _tag_flac(path, metadata)
        return
    raise ValueError(f"Unsupported music format: {ext or '(none)'}")


def _sanitize_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    text = "".join(ch if ord(ch) >= 32 else " " for ch in text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text or None


def _sanitize_filename_component(value: str) -> str:
    text = _sanitize_text(value) or "Unknown"
    text = _FS_FORBIDDEN_CHARS_RE.sub("-", text)
    text = text.strip(" .")
    return text or "Unknown"


def _read_artwork_bytes(artwork: str | None) -> tuple[bytes, str] | None:
    if not artwork:
        return None
    if not os.path.exists(artwork):
        return None
    with open(artwork, "rb") as handle:
        data = handle.read()
    if not data:
        return None
    mime = mimetypes.guess_type(artwork)[0] or "image/jpeg"
    return data, mime


def _set_id3_text(audio: ID3, frame_cls: Any, value: str | int | None) -> None:
    normalized = _sanitize_text(str(value)) if value is not None else None
    if not normalized:
        return
    audio.add(frame_cls(encoding=3, text=[normalized]))


def _tag_mp3(path: str, metadata: MusicMetadata) -> None:
    if ID3 is None:
        raise RuntimeError("mutagen is required for MP3 tagging")
    audio = ID3()
    _set_id3_text(audio, TIT2, metadata.title)
    _set_id3_text(audio, TPE1, metadata.artist)
    _set_id3_text(audio, TALB, metadata.album)
    _set_id3_text(audio, TPE2, metadata.album_artist)
    _set_id3_text(audio, TRCK, metadata.track_num)
    _set_id3_text(audio, TPOS, metadata.disc_num)
    _set_id3_text(audio, TDRC, metadata.date)
    _set_id3_text(audio, TCON, metadata.genre)
    _set_id3_text(audio, TSRC, metadata.isrc)
    if metadata.mbid:
        audio.add(TXXX(encoding=3, desc="MBID", text=[_sanitize_text(metadata.mbid)]))
    if metadata.lyrics:
        audio.add(USLT(encoding=3, lang="eng", desc="Lyrics", text=_sanitize_text(metadata.lyrics)))

    artwork_blob = _read_artwork_bytes(metadata.artwork)
    if artwork_blob:
        data, mime = artwork_blob
        audio.add(APIC(encoding=3, mime=mime, type=3, desc="cover", data=data))
    audio.save(path, v2_version=4)


def _tag_flac(path: str, metadata: MusicMetadata) -> None:
    if FLAC is None or Picture is None:
        raise RuntimeError("mutagen is required for FLAC tagging")
    audio = FLAC(path)
    fields: dict[str, str | int | None] = {
        "title": metadata.title,
        "artist": metadata.artist,
        "album": metadata.album,
        "albumartist": metadata.album_artist,
        "tracknumber": metadata.track_num,
        "discnumber": metadata.disc_num,
        "date": metadata.date,
        "genre": metadata.genre,
        "isrc": metadata.isrc,
        "musicbrainz_trackid": metadata.mbid,
        "lyrics": metadata.lyrics,
    }
    for key, value in fields.items():
        normalized = _sanitize_text(str(value)) if value is not None else None
        if normalized:
            audio[key] = [normalized]

    artwork_blob = _read_artwork_bytes(metadata.artwork)
    if artwork_blob:
        data, mime = artwork_blob
        picture = Picture()
        picture.data = data
        picture.type = 3
        picture.mime = mime
        picture.desc = "cover"
        audio.clear_pictures()
        audio.add_picture(picture)
    audio.save()
