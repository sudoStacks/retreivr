import logging
import os
import threading
import time
import socket

from . import matcher
from .providers import acoustid as acoustid_provider
from .providers import artwork as artwork_provider
from .providers import musicbrainz as musicbrainz_provider
from .services.musicbrainz_service import get_musicbrainz_service
from .tagging_service import apply_tags
from .lyric_enrichment import fetch_lyrics


class MetadataWorker(threading.Thread):
    def __init__(self, work_queue):
        super().__init__(daemon=True)
        self._queue = work_queue

    def run(self):
        while True:
            item = self._queue.get()
            try:
                _process_item(item)
            except Exception as exc:
                if _is_recoverable_metadata_error(exc):
                    logging.warning("Music metadata worker skipped recoverable error: %s", exc)
                else:
                    logging.exception("Music metadata worker failed")
            finally:
                self._queue.task_done()
            rate_limit = item.get("config", {}).get("rate_limit_seconds", 1.5)
            try:
                rate = float(rate_limit)
            except (TypeError, ValueError):
                rate = 1.5
            if rate > 0:
                time.sleep(rate)


def _is_recoverable_metadata_error(exc: Exception) -> bool:
    if isinstance(exc, (FileNotFoundError, TimeoutError, socket.gaierror)):
        return True
    text = str(exc or "").strip().lower()
    if not text:
        return False
    return (
        "max() arg is an empty sequence" in text
        or "no candidates" in text
        or "timed out" in text
        or "temporary failure" in text
        or "name or service not known" in text
    )


def _process_item(item):
    file_path = item.get("file_path")
    if not file_path or not os.path.exists(file_path):
        logging.warning("Music metadata skipped: file missing (%s)", file_path)
        return
    config = item.get("config") or {}
    meta = item.get("meta") or {}
    source = matcher.parse_source(meta, file_path)
    if not source.get("title") or not source.get("artist"):
        logging.warning("Music metadata skipped: missing source artist/title (%s)", file_path)
        return

    duration = matcher.get_duration_seconds(file_path)
    candidates = musicbrainz_provider.search_recordings(
        source["artist"],
        source["title"],
        album=source.get("album"),
    )

    acoustid_hit = None
    if config.get("use_acoustid"):
        api_key = (config.get("acoustid_api_key") or "").strip()
        if api_key:
            logging.info(
                "Music metadata: acoustid lookup started file=%s",
                os.path.basename(file_path),
            )
            acoustid_hit = acoustid_provider.match_recording(file_path, api_key)
            if acoustid_hit:
                before_count = len(candidates) if isinstance(candidates, list) else 0
                candidates = matcher.merge_candidates(candidates, [acoustid_hit])
                after_count = len(candidates) if isinstance(candidates, list) else before_count
                logging.info(
                    "Music metadata: acoustid lookup matched recording_id=%s score=%.3f merged_candidates=%s->%s",
                    acoustid_hit.get("recording_id"),
                    float(acoustid_hit.get("acoustid_score") or 0.0),
                    before_count,
                    after_count,
                )
            else:
                logging.info(
                    "Music metadata: acoustid lookup returned no match file=%s",
                    os.path.basename(file_path),
                )
        else:
            logging.warning("Music metadata: acoustid enabled but API key is missing")

    best, score, score_breakdown = matcher.select_best_match(source, candidates, duration)
    if isinstance(acoustid_hit, dict):
        acoustid_recording_id = str(acoustid_hit.get("recording_id") or "").strip()
        best_recording_id = str((best or {}).get("recording_id") or "").strip()
        if acoustid_recording_id and best_recording_id and acoustid_recording_id == best_recording_id:
            logging.info(
                "Music metadata: acoustid-assisted candidate selected recording_id=%s",
                best_recording_id,
            )
    threshold = config.get("confidence_threshold", 70)
    best = best if isinstance(best, dict) else {}
    match_ok = bool(best) and score >= threshold

    # Always apply at least source-derived tags, even when MB confidence is low.
    source_artist = source.get("artist") or ""
    source_title = source.get("title") or ""
    source_album = source.get("album") or ""
    tags = {
        "artist": meta.get("artist") or source_artist or best.get("artist"),
        "album": meta.get("album") or source_album or best.get("album"),
        "title": meta.get("track") or meta.get("title") or source_title or best.get("title"),
        "track_number": meta.get("track_number") or best.get("track_number"),
        "track_total": meta.get("track_total"),
        "year": best.get("year") if match_ok else None,
        "date": meta.get("release_date") or meta.get("date") or (best.get("date") if match_ok else None) or (best.get("year") if match_ok else None),
        "disc_number": meta.get("disc_number") or meta.get("disc") or best.get("disc_number"),
        "disc_total": meta.get("disc_total"),
        "genre": meta.get("genre") or (best.get("genre") if match_ok else None),
        "album_artist": meta.get("album_artist") or (best.get("album_artist") if match_ok else None) or (best.get("artist") if match_ok else None) or source_artist,
        "recording_id": (best.get("recording_id") if match_ok else None) or meta.get("mb_recording_id") or meta.get("recording_mbid"),
        "mb_recording_id": meta.get("mb_recording_id") or meta.get("recording_mbid") or best.get("recording_id"),
        "mb_release_id": meta.get("mb_release_id") or (best.get("release_id") if match_ok else None),
    }
    # Allow selective enrichment when a component is strongly matched,
    # even if total score misses threshold.
    artist_component = int((score_breakdown or {}).get("artist_score") or 0)
    track_component = int((score_breakdown or {}).get("track_score") or 0)
    album_component = int((score_breakdown or {}).get("album_score") or 0)
    if not match_ok and best:
        if artist_component >= 88 and not tags.get("artist"):
            tags["artist"] = best.get("artist")
        if track_component >= 88 and not tags.get("title"):
            tags["title"] = best.get("title")
        if album_component >= 90 and not tags.get("album"):
            tags["album"] = best.get("album")

    release_id = meta.get("mb_release_id") or (best.get("release_id") if match_ok else None)
    release_group_id = meta.get("mb_release_group_id") or (best.get("release_group_id") if match_ok else None)
    artwork = None
    if config.get("embed_artwork"):
        max_size_px = config.get("max_artwork_size_px", 1500)
        artwork_url = str(meta.get("artwork_url") or "").strip()
        thumbnail_url = str(meta.get("thumbnail_url") or "").strip()
        if artwork_url:
            artwork = artwork_provider.fetch_artwork_from_url(
                artwork_url,
                max_size_px=max_size_px,
            )
        if artwork is None and release_id:
            artwork = artwork_provider.fetch_artwork(
                release_id,
                max_size_px=max_size_px,
            )
        if artwork is None and release_group_id:
            try:
                group_cover_url = get_musicbrainz_service().fetch_release_group_cover_art_url(
                    release_group_id,
                    timeout=8,
                )
            except Exception:
                group_cover_url = None
            if group_cover_url:
                artwork = artwork_provider.fetch_artwork_from_url(
                    group_cover_url,
                    max_size_px=max_size_px,
                )
        if artwork is None and thumbnail_url and thumbnail_url != artwork_url:
            artwork = artwork_provider.fetch_artwork_from_url(
                thumbnail_url,
                max_size_px=max_size_px,
            )

    display_artist = tags.get("artist") or "-"
    display_title = tags.get("title") or "-"
    display_album = tags.get("album") or "-"
    if match_ok:
        logging.info(
            "Metadata matched (%s%%) artist=%s track=%s album=%s - %s / %s / %s",
            score,
            artist_component,
            track_component,
            album_component,
            display_artist,
            display_title,
            display_album,
        )
    else:
        logging.warning(
            "Metadata partial (%s%% < %s%%) artist=%s track=%s album=%s - %s / %s / %s",
            score,
            threshold,
            artist_component,
            track_component,
            album_component,
            display_artist,
            display_title,
            display_album,
        )

    # Optional lyrics enrichment (non-fatal)
    if config.get("enable_lyrics"):
        try:
            lyrics_result = fetch_lyrics(
                artist=tags.get("artist") or "",
                title=tags.get("title") or "",
                album=tags.get("album"),
                config=config,
            )
            if lyrics_result and lyrics_result.lyrics:
                tags["lyrics"] = lyrics_result.lyrics
                tags["lyrics_source"] = lyrics_result.source
                tags["lyrics_confidence"] = lyrics_result.confidence
        except Exception:
            logging.exception("Lyrics enrichment failed (non-fatal)")

    dry_run = bool(config.get("dry_run"))
    logging.debug(
        "Music metadata tag keys for %s: %s",
        os.path.basename(file_path),
        sorted([key for key, value in tags.items() if value not in (None, "")]),
    )
    try:
        apply_tags(
            file_path,
            tags,
            artwork,
            source_title=source.get("source_title"),
            allow_overwrite=bool(config.get("allow_overwrite_tags", True)),
            dry_run=dry_run,
        )
    except Exception as exc:
        logging.error(
            "music_metadata_tagging_failed file=%s ext=%s error=%s",
            file_path,
            os.path.splitext(file_path)[1].lower(),
            exc,
            exc_info=True,
        )
        return
