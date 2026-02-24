import logging
import os

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


def apply_tags(file_path, tags, artwork, *, source_title=None, allow_overwrite=False, dry_run=False):
    if dry_run:
        logging.info("Music metadata dry-run tags for %s: %s", os.path.basename(file_path), _format_tags(tags))
        return
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".mp3":
        _apply_id3_tags(file_path, tags, artwork, source_title, allow_overwrite)
        return
    if ext in {".m4a", ".mp4", ".m4b"}:
        _apply_mp4_tags(file_path, tags, artwork, source_title, allow_overwrite)
        return
    _apply_generic_tags(file_path, tags, artwork, source_title, allow_overwrite)


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
    mp4_tags = audio.tags or {}
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
