"""Audio tagging helpers for music files."""

from __future__ import annotations

import logging
import os
from typing import Any

try:
    from mutagen.id3 import APIC, ID3, TALB, TCON, TDRC, TIT2, TPE1, TPE2, TPOS, TRCK, TSRC, TXXX, USLT
except ImportError:  # pragma: no cover - handled in tests by monkeypatching
    APIC = ID3 = TALB = TCON = TDRC = TIT2 = TPE1 = TPE2 = TPOS = TRCK = TSRC = TXXX = USLT = None

from metadata.types import MusicMetadata

_LOG = logging.getLogger(__name__)


def tag_file(path: str, metadata: MusicMetadata) -> None:
    """Apply metadata tags to a music file using ID3v2.4 for MP3 files."""
    ext = os.path.splitext(path)[1].lower()
    if ext != ".mp3":
        raise ValueError(f"Unsupported file format for tagging: {ext or '(none)'}")
    _tag_mp3(path, metadata)


def _add_text_frame(audio: Any, frame_cls: Any, value: str | int | None) -> None:
    if value is None:
        return
    text = str(value).strip()
    if not text:
        return
    audio.add(frame_cls(encoding=3, text=[text]))


def _tag_mp3(path: str, metadata: MusicMetadata) -> None:
    if ID3 is None:
        raise RuntimeError("mutagen is required for MP3 tagging")

    audio = ID3()
    _add_text_frame(audio, TIT2, metadata.title)
    _add_text_frame(audio, TPE1, metadata.artist)
    _add_text_frame(audio, TALB, metadata.album)
    _add_text_frame(audio, TPE2, metadata.album_artist)
    _add_text_frame(audio, TRCK, metadata.track_num)
    _add_text_frame(audio, TPOS, metadata.disc_num)
    _add_text_frame(audio, TDRC, metadata.date)
    _add_text_frame(audio, TCON, metadata.genre)
    _add_text_frame(audio, TSRC, metadata.isrc)
    if metadata.mbid:
        audio.add(TXXX(encoding=3, desc="MBID", text=[metadata.mbid]))

    if metadata.lyrics:
        try:
            audio.add(USLT(encoding=3, lang="eng", desc="Lyrics", text=metadata.lyrics))
        except Exception:  # pragma: no cover - non-fatal branch
            _LOG.warning("Failed to write lyrics tag for %s", path, exc_info=True)

    if metadata.artwork:
        try:
            audio.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="cover", data=metadata.artwork))
        except Exception:  # pragma: no cover - non-fatal branch
            _LOG.warning("Failed to embed artwork for %s", path, exc_info=True)

    audio.save(path, v2_version=4)

