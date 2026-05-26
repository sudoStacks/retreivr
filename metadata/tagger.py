import logging
import os
import re

try:
    from mutagen import File as MutagenFile
except ImportError:  # pragma: no cover - optional dependency in tests
    MutagenFile = None
try:
    from mutagen.id3 import APIC, ID3, TCON, TDRC, TIT2, TPE1, TPE2, TALB, TRCK, TPOS, TXXX, USLT
except ImportError:  # pragma: no cover - optional dependency in tests
    APIC = ID3 = TCON = TDRC = TIT2 = TPE1 = TPE2 = TALB = TRCK = TPOS = TXXX = USLT = None
try:
    from mutagen.mp4 import MP4, MP4Cover
except ImportError:  # pragma: no cover - optional dependency in tests
    MP4 = MP4Cover = None

_TRACK_PREFIX_RE = re.compile(r"^\s*(?:\d{1,3})(?:\s*/\s*\d{1,3})?\s*[-._)]\s+")


def apply_tags(file_path, tags, artwork, *, source_title=None, allow_overwrite=False, dry_run=False):
    if dry_run:
        logging.info("Music metadata dry-run tags for %s: %s", os.path.basename(file_path), _format_tags(tags))
        return
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".mp3":
        _apply_id3_tags(file_path, tags, artwork, source_title, allow_overwrite)
        verify_tags(file_path, tags)
        return
    if ext in {".m4a", ".mp4", ".m4b"}:
        _apply_mp4_tags(file_path, tags, artwork, source_title, allow_overwrite)
        verify_tags(file_path, tags)
        return
    _apply_generic_tags(file_path, tags, artwork, source_title, allow_overwrite)
    verify_tags(file_path, tags)


def clean_display_title(value):
    """Return a player-facing title with any filename-style track prefix removed."""
    text = str(value or "").strip()
    cleaned = _TRACK_PREFIX_RE.sub("", text).strip()
    return cleaned or text


def read_music_tags(file_path):
    """Read the core music tags Retreivr needs for verification and repair."""
    ext = os.path.splitext(str(file_path))[1].lower()
    if ext == ".mp3":
        return _read_id3_core_tags(file_path)
    if ext in {".m4a", ".mp4", ".m4b"}:
        return _read_mp4_core_tags(file_path)
    return _read_generic_core_tags(file_path)


def verify_tags(file_path, expected):
    """Verify that required player-facing tags were actually embedded."""
    if not expected:
        return True
    read_tags = read_music_tags(file_path)
    expected_title = clean_display_title(expected.get("title"))
    actual_title = clean_display_title(read_tags.get("title"))
    if expected_title and actual_title != expected_title:
        raise RuntimeError(
            f"metadata verification failed for {os.path.basename(str(file_path))}: "
            f"title={actual_title!r} expected={expected_title!r}"
        )
    expected_track = _normalize_track(expected.get("track_number"))
    actual_track = _normalize_track(read_tags.get("track_number"))
    if expected_track and actual_track and actual_track != expected_track:
        raise RuntimeError(
            f"metadata verification failed for {os.path.basename(str(file_path))}: "
            f"track_number={actual_track!r} expected={expected_track!r}"
        )
    return True


def _read_id3_core_tags(file_path):
    if ID3 is None:
        raise RuntimeError("mutagen id3 support is required for MP3 tag verification")
    try:
        audio = ID3(file_path)
    except TypeError:
        audio = ID3()
    except Exception:
        return {}
    return {
        "title": _first_id3_text(audio, "TIT2"),
        "artist": _first_id3_text(audio, "TPE1"),
        "album": _first_id3_text(audio, "TALB"),
        "album_artist": _first_id3_text(audio, "TPE2"),
        "track_number": _first_id3_text(audio, "TRCK"),
        "disc_number": _first_id3_text(audio, "TPOS"),
        "recording_id": _first_id3_txxx(audio, "MBID"),
        "mb_release_id": _first_id3_txxx(audio, "MUSICBRAINZ_RELEASEID"),
        "mb_release_group_id": _first_id3_txxx(audio, "MUSICBRAINZ_RELEASEGROUPID"),
    }


def _read_mp4_core_tags(file_path):
    if MP4 is None:
        raise RuntimeError("mutagen mp4 support is required for MP4 tag verification")
    try:
        audio = MP4(file_path)
    except Exception:
        return {}
    tags = audio.tags or {}
    return {
        "title": _first_tag_value(tags.get("\xa9nam")),
        "artist": _first_tag_value(tags.get("\xa9ART")),
        "album": _first_tag_value(tags.get("\xa9alb")),
        "album_artist": _first_tag_value(tags.get("aART")),
        "track_number": _format_pair_tag(tags.get("trkn")),
        "disc_number": _format_pair_tag(tags.get("disk")),
        "recording_id": _first_mp4_freeform(tags, "MBID"),
        "mb_release_id": _first_mp4_freeform(tags, "MUSICBRAINZ_RELEASEID"),
        "mb_release_group_id": _first_mp4_freeform(tags, "MUSICBRAINZ_RELEASEGROUPID"),
    }


def _read_generic_core_tags(file_path):
    if MutagenFile is None:
        raise RuntimeError("mutagen is required for tag verification")
    try:
        audio = MutagenFile(file_path)
    except Exception:
        return {}
    if not audio or not getattr(audio, "tags", None):
        return {}
    tags = audio.tags
    return {
        "title": _first_tag_value(_tag_get(tags, "title")),
        "artist": _first_tag_value(_tag_get(tags, "artist")),
        "album": _first_tag_value(_tag_get(tags, "album")),
        "album_artist": _first_tag_value(_tag_get(tags, "albumartist") or _tag_get(tags, "album_artist")),
        "track_number": _first_tag_value(_tag_get(tags, "tracknumber")),
        "disc_number": _first_tag_value(_tag_get(tags, "discnumber")),
        "recording_id": _first_tag_value(_tag_get(tags, "mbid") or _tag_get(tags, "musicbrainz_trackid")),
        "mb_release_id": _first_tag_value(_tag_get(tags, "musicbrainz_releaseid")),
        "mb_release_group_id": _first_tag_value(_tag_get(tags, "musicbrainz_releasegroupid")),
    }


def _apply_id3_tags(file_path, tags, artwork, source_title, allow_overwrite):
    if ID3 is None:
        raise RuntimeError("mutagen id3 support is required for MP3 tagging")
    try:
        audio = ID3(file_path)
    except Exception:
        audio = ID3()
    changed = False
    changed |= _set_id3_text(audio, "TPE1", tags.get("artist"), allow_overwrite)
    changed |= _set_id3_text(audio, "TALB", tags.get("album"), allow_overwrite)
    changed |= _set_id3_text(audio, "TIT2", tags.get("title"), allow_overwrite)
    changed |= _set_id3_text(audio, "TPE2", tags.get("album_artist"), allow_overwrite)
    changed |= _set_id3_text(
        audio,
        "TRCK",
        _format_index_with_total(tags.get("track_number"), tags.get("track_total")),
        allow_overwrite,
    )
    changed |= _set_id3_text(
        audio,
        "TPOS",
        _format_index_with_total(tags.get("disc_number"), tags.get("disc_total")),
        allow_overwrite,
    )
    changed |= _set_id3_text(audio, "TDRC", tags.get("date") or tags.get("year"), allow_overwrite)
    changed |= _set_id3_text(audio, "TCON", tags.get("genre"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "SOURCE", "YouTube", allow_overwrite)
    if source_title:
        changed |= _set_id3_txxx(audio, "SOURCE_TITLE", source_title, allow_overwrite)
    if tags.get("recording_id"):
        changed |= _set_id3_txxx(audio, "MBID", tags.get("recording_id"), allow_overwrite)
    if tags.get("mb_release_id"):
        changed |= _set_id3_txxx(audio, "MUSICBRAINZ_RELEASEID", tags.get("mb_release_id"), allow_overwrite)
    if tags.get("mb_release_group_id"):
        changed |= _set_id3_txxx(audio, "MUSICBRAINZ_RELEASEGROUPID", tags.get("mb_release_group_id"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "RETREIVR_MANAGED", tags.get("retreivr_managed"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "RETREIVR_JOB_ID", tags.get("retreivr_job_id"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "RETREIVR_TRACE_ID", tags.get("retreivr_trace_id"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "RETREIVR_VERSION", tags.get("retreivr_version"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "RETREIVR_ACQUIRED_AT", tags.get("retreivr_acquired_at"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "RETREIVR_SOURCE", tags.get("retreivr_source"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "RETREIVR_SOURCE_ID", tags.get("retreivr_source_id"), allow_overwrite)
    changed |= _set_id3_txxx(audio, "ISRC", tags.get("isrc"), allow_overwrite)
    lyrics = tags.get("lyrics")
    if lyrics:
        if allow_overwrite:
            audio.delall("USLT")
        if allow_overwrite or not audio.getall("USLT"):
            try:
                audio.add(USLT(encoding=3, lang="eng", desc="Lyrics", text=str(lyrics)))
                changed = True
            except Exception:
                logging.warning("Failed to write lyrics tag for %s", file_path, exc_info=True)
    if artwork and (allow_overwrite or not audio.getall("APIC")):
        if allow_overwrite:
            for frame in audio.getall("APIC"):
                audio.delall("APIC")
        try:
            audio.add(
                APIC(
                    encoding=3,
                    mime=artwork.get("mime") or "image/jpeg",
                    type=3,
                    desc="cover",
                    data=artwork.get("data"),
                )
            )
            changed = True
        except Exception:
            logging.warning("Failed to embed artwork for %s", file_path, exc_info=True)
    if changed:
        audio.save(file_path)


def _apply_mp4_tags(file_path, tags, artwork, source_title, allow_overwrite):
    if MP4 is None:
        raise RuntimeError("mutagen mp4 support is required for MP4 tagging")
    audio = MP4(file_path)
    mp4_tags = audio.tags if audio.tags is not None else {}
    changed = False
    changed |= _set_mp4_value(mp4_tags, "\xa9ART", tags.get("artist"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "\xa9alb", tags.get("album"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "\xa9nam", tags.get("title"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "\xa9lyr", tags.get("lyrics"), allow_overwrite)
    changed |= _set_mp4_value(mp4_tags, "aART", tags.get("album_artist"), allow_overwrite)
    track_number = _normalize_track(tags.get("track_number"))
    track_total = _normalize_total(tags.get("track_total"), tags.get("track_number"))
    if track_number and (allow_overwrite or "trkn" not in mp4_tags):
        mp4_tags["trkn"] = [(track_number, track_total or 0)]
        changed = True
    disc_number = _normalize_track(tags.get("disc_number"))
    disc_total = _normalize_total(tags.get("disc_total"), tags.get("disc_number"))
    if disc_number and (allow_overwrite or "disk" not in mp4_tags):
        mp4_tags["disk"] = [(disc_number, disc_total or 0)]
        changed = True
    date_value = tags.get("date") or tags.get("year")
    if date_value and (allow_overwrite or "\xa9day" not in mp4_tags):
        mp4_tags["\xa9day"] = [str(date_value)]
        changed = True
    genre = tags.get("genre")
    if genre and (allow_overwrite or "\xa9gen" not in mp4_tags):
        mp4_tags["\xa9gen"] = [str(genre)]
        changed = True
    changed |= _set_mp4_freeform(mp4_tags, "SOURCE", "YouTube", allow_overwrite)
    if source_title:
        changed |= _set_mp4_freeform(mp4_tags, "SOURCE_TITLE", source_title, allow_overwrite)
    if tags.get("recording_id"):
        changed |= _set_mp4_freeform(mp4_tags, "MBID", tags.get("recording_id"), allow_overwrite)
    if tags.get("mb_release_id"):
        changed |= _set_mp4_freeform(mp4_tags, "MUSICBRAINZ_RELEASEID", tags.get("mb_release_id"), allow_overwrite)
    if tags.get("mb_release_group_id"):
        changed |= _set_mp4_freeform(mp4_tags, "MUSICBRAINZ_RELEASEGROUPID", tags.get("mb_release_group_id"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "RETREIVR_MANAGED", tags.get("retreivr_managed"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "RETREIVR_JOB_ID", tags.get("retreivr_job_id"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "RETREIVR_TRACE_ID", tags.get("retreivr_trace_id"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "RETREIVR_VERSION", tags.get("retreivr_version"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "RETREIVR_ACQUIRED_AT", tags.get("retreivr_acquired_at"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "RETREIVR_SOURCE", tags.get("retreivr_source"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "RETREIVR_SOURCE_ID", tags.get("retreivr_source_id"), allow_overwrite)
    changed |= _set_mp4_freeform(mp4_tags, "ISRC", tags.get("isrc"), allow_overwrite)
    if artwork and (allow_overwrite or "covr" not in mp4_tags):
        img_data = artwork.get("data")
        if img_data:
            cover = MP4Cover(img_data)
            mp4_tags["covr"] = [cover]
            changed = True
    if changed:
        audio.tags = mp4_tags
        audio.save()


def _apply_generic_tags(file_path, tags, artwork, source_title, allow_overwrite):
    if MutagenFile is None:
        raise RuntimeError("mutagen is required for generic tagging")
    audio = MutagenFile(file_path)
    if not audio:
        logging.warning("Music metadata tagging skipped: unsupported file %s", file_path)
        return
    if audio.tags is None:
        audio.add_tags()
    changed = False
    changed |= _set_generic(audio.tags, "artist", tags.get("artist"), allow_overwrite)
    changed |= _set_generic(audio.tags, "album", tags.get("album"), allow_overwrite)
    changed |= _set_generic(audio.tags, "title", tags.get("title"), allow_overwrite)
    changed |= _set_generic(audio.tags, "lyrics", tags.get("lyrics"), allow_overwrite)
    changed |= _set_generic(audio.tags, "albumartist", tags.get("album_artist"), allow_overwrite)
    changed |= _set_generic(
        audio.tags,
        "tracknumber",
        _format_index_with_total(tags.get("track_number"), tags.get("track_total")),
        allow_overwrite,
    )
    changed |= _set_generic(
        audio.tags,
        "discnumber",
        _format_index_with_total(tags.get("disc_number"), tags.get("disc_total")),
        allow_overwrite,
    )
    changed |= _set_generic(audio.tags, "date", tags.get("date") or tags.get("year"), allow_overwrite)
    changed |= _set_generic(audio.tags, "genre", tags.get("genre"), allow_overwrite)
    changed |= _set_generic(audio.tags, "source", "YouTube", allow_overwrite)
    if source_title:
        changed |= _set_generic(audio.tags, "source_title", source_title, allow_overwrite)
    if tags.get("recording_id"):
        changed |= _set_generic(audio.tags, "mbid", tags.get("recording_id"), allow_overwrite)
    if tags.get("mb_release_id"):
        changed |= _set_generic(audio.tags, "musicbrainz_releaseid", tags.get("mb_release_id"), allow_overwrite)
    if tags.get("mb_release_group_id"):
        changed |= _set_generic(audio.tags, "musicbrainz_releasegroupid", tags.get("mb_release_group_id"), allow_overwrite)
    changed |= _set_generic(audio.tags, "retreivr_managed", tags.get("retreivr_managed"), allow_overwrite)
    changed |= _set_generic(audio.tags, "retreivr_job_id", tags.get("retreivr_job_id"), allow_overwrite)
    changed |= _set_generic(audio.tags, "retreivr_trace_id", tags.get("retreivr_trace_id"), allow_overwrite)
    changed |= _set_generic(audio.tags, "retreivr_version", tags.get("retreivr_version"), allow_overwrite)
    changed |= _set_generic(audio.tags, "retreivr_acquired_at", tags.get("retreivr_acquired_at"), allow_overwrite)
    changed |= _set_generic(audio.tags, "retreivr_source", tags.get("retreivr_source"), allow_overwrite)
    changed |= _set_generic(audio.tags, "retreivr_source_id", tags.get("retreivr_source_id"), allow_overwrite)
    changed |= _set_generic(audio.tags, "isrc", tags.get("isrc"), allow_overwrite)
    if changed:
        audio.save()


def _set_id3_text(audio, frame_id, value, allow_overwrite):
    if value is None or value == "":
        return False
    if audio.getall(frame_id):
        if not allow_overwrite:
            return False
        audio.delall(frame_id)
    text_value = str(value)
    frame_map = {
        "TPE1": TPE1,
        "TALB": TALB,
        "TIT2": TIT2,
        "TPE2": TPE2,
        "TRCK": TRCK,
        "TPOS": TPOS,
        "TDRC": TDRC,
        "TCON": TCON,
    }
    frame_cls = frame_map.get(frame_id)
    if not frame_cls:
        return False
    audio.add(frame_cls(encoding=3, text=[text_value]))
    return True


def _set_id3_txxx(audio, desc, value, allow_overwrite):
    if value is None or value == "":
        return False
    for frame in audio.getall("TXXX"):
        if frame.desc == desc:
            if not allow_overwrite:
                return False
            audio.delall("TXXX")
            break
    audio.add(TXXX(encoding=3, desc=desc, text=[str(value)]))
    return True


def _set_mp4_value(tags, key, value, allow_overwrite):
    if value is None or value == "":
        return False
    if key in tags:
        if not allow_overwrite:
            return False
    tags[key] = [str(value)]
    return True


def _set_mp4_freeform(tags, key, value, allow_overwrite):
    if value is None or value == "":
        return False
    atom = f"----:com.apple.iTunes:{key}"
    if atom in tags:
        if not allow_overwrite:
            return False
    tags[atom] = [str(value).encode("utf-8")]
    return True


def _set_generic(tags, key, value, allow_overwrite):
    if value is None or value == "":
        return False
    if key in tags:
        existing = tags.get(key)
        if existing and not allow_overwrite:
            return False
    tags[key] = [str(value)]
    return True


def _tag_get(tags, key):
    try:
        return tags.get(key)
    except Exception:
        return None


def _first_tag_value(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        for item in value:
            text = _decode_tag_value(item)
            if text:
                return text
        return None
    return _decode_tag_value(value)


def _decode_tag_value(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8").strip() or None
        except Exception:
            return value.decode("utf-8", errors="ignore").strip() or None
    text = str(value).strip()
    return text or None


def _first_id3_text(audio, frame_id):
    try:
        frames = audio.getall(frame_id)
    except Exception:
        frames = []
    for frame in frames:
        text = _first_tag_value(getattr(frame, "text", None))
        if text:
            return text
    return None


def _first_id3_txxx(audio, desc):
    try:
        frames = audio.getall("TXXX")
    except Exception:
        frames = []
    for frame in frames:
        if str(getattr(frame, "desc", "") or "") != desc:
            continue
        text = _first_tag_value(getattr(frame, "text", None))
        if text:
            return text
    return None


def _first_mp4_freeform(tags, name):
    return _first_tag_value(tags.get(f"----:com.apple.iTunes:{name}"))


def _format_pair_tag(value):
    if not value:
        return None
    pair = value[0] if isinstance(value, (list, tuple)) else value
    if isinstance(pair, (list, tuple)) and pair:
        number = _normalize_track(pair[0])
        total = _normalize_positive_int(pair[1]) if len(pair) > 1 else None
        if number and total:
            return f"{number}/{total}"
        if number:
            return str(number)
    return _first_tag_value(value)


def _normalize_track(value):
    if value is None or value == "":
        return None
    try:
        return int(str(value).split("/")[0])
    except Exception:
        return None


def _normalize_positive_int(value):
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _extract_fraction_total(value):
    if value is None:
        return None
    text = str(value).strip()
    if "/" not in text:
        return None
    tail = text.split("/", 1)[1].strip()
    return _normalize_positive_int(tail)


def _normalize_total(explicit_total, fraction_value):
    return _normalize_positive_int(explicit_total) or _extract_fraction_total(fraction_value)


def _format_index_with_total(index_value, total_value):
    number = _normalize_track(index_value)
    if number is None:
        return None
    total = _normalize_total(total_value, index_value)
    if total:
        return f"{number}/{total}"
    return str(number)


def _format_tags(tags):
    compact = {}
    for key, value in (tags or {}).items():
        if value is None or value == "":
            continue
        compact[key] = value
    return compact
