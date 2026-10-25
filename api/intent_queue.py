"""Intent queue adapter extracted from api.main for better isolation."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

from fastapi import HTTPException

from engine.canonical_ids import build_music_track_canonical_id, extract_external_track_canonical_id
from engine.core import load_config
from engine.job_queue import build_download_job_payload, resolve_media_type, resolve_source


class IntentQueueAdapter:
    """Queue adapter that writes intent payloads into the unified download queue."""

    def __init__(self, *, state: Any, config_loader=load_config) -> None:
        self._state = state
        self._config_loader = config_loader

    def enqueue(self, payload: dict) -> dict[str, object]:
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="intent_enqueue_invalid_payload")

        state = self._state if self._state is not None else SimpleNamespace()
        engine = getattr(state, "worker_engine", None)
        store = getattr(engine, "store", None) if engine is not None else None
        if store is None:
            raise HTTPException(status_code=503, detail="intent_queue_store_unavailable")
        try:
            runtime_config = self._config_loader(getattr(state, "config_path", None))
        except Exception:
            runtime_config = {}
        base_dir = getattr(getattr(state, "paths", None), "single_downloads_dir", None)

        media_intent = str(payload.get("media_intent") or "").strip() or "track"
        origin = str(payload.get("origin") or "").strip() or ("spotify_playlist" if payload.get("playlist_id") else "intent")
        origin_id = str(
            payload.get("origin_id")
            or payload.get("playlist_id")
            or payload.get("spotify_track_id")
            or "manual"
        ).strip() or "manual"
        destination = str(payload.get("destination") or "").strip() or None
        final_format = str(payload.get("final_format") or "").strip() or None
        force_redownload = bool(payload.get("force_redownload"))
        canonical_metadata = payload.get("canonical_metadata") if isinstance(payload.get("canonical_metadata"), dict) else {}
        is_music_mode_origin = bool(payload.get("music_mode")) or str(payload.get("media_type") or "").strip().lower() == "music" or any(
            bool(str(payload.get(key) or "").strip())
            for key in (
                "recording_mbid",
                "mb_recording_id",
                "mb_release_id",
                "mb_release_group_id",
                "release_id",
                "release_group_id",
            )
        ) or any(
            bool(str(canonical_metadata.get(key) or "").strip())
            for key in (
                "recording_mbid",
                "mb_recording_id",
                "mb_release_id",
                "mb_release_group_id",
                "release_id",
                "release_group_id",
            )
        )

        def _to_dict(value):
            if isinstance(value, dict):
                return dict(value)
            if value is None:
                return {}
            out = {}
            for key in (
                "title",
                "artist",
                "album",
                "album_artist",
                "track_num",
                "disc_num",
                "date",
                "genre",
                "isrc",
                "mbid",
                "lyrics",
            ):
                if hasattr(value, key):
                    out[key] = getattr(value, key)
            return out

        def _build_music_track_canonical_id(
            artist,
            album,
            track_number,
            track,
            *,
            recording_mbid=None,
            mb_release_id=None,
            mb_release_group_id=None,
            disc_number=None,
        ):
            return build_music_track_canonical_id(
                artist,
                album,
                track_number,
                track,
                recording_mbid=recording_mbid,
                mb_release_id=mb_release_id,
                mb_release_group_id=mb_release_group_id,
                disc_number=disc_number,
            )

        def _enqueue_music_query_job(
            artist: str,
            track: str,
            album: str | None = None,
            *,
            recording_mbid: str | None = None,
            mb_release_id: str | None = None,
            mb_release_group_id: str | None = None,
            media_mode: str | None = None,
        ) -> dict[str, object]:
            def _optional_pos_int(value):
                if value is None or str(value).strip() == "":
                    return None
                try:
                    parsed = int(value)
                except (TypeError, ValueError):
                    return None
                return parsed if parsed > 0 else None

            normalized_artist = str(artist or "").strip()
            normalized_track = str(track or "").strip()
            normalized_album = str(album or "").strip() or None
            normalized_album_artist = str(payload.get("album_artist") or "").strip() or None
            normalized_recording_mbid = str(recording_mbid or "").strip() or None
            normalized_release_mbid = str(mb_release_id or "").strip() or None
            normalized_release_group_mbid = str(mb_release_group_id or "").strip() or None
            normalized_track_number = _optional_pos_int(payload.get("track_number"))
            normalized_disc_number = _optional_pos_int(payload.get("disc_number"))
            normalized_track_total = _optional_pos_int(payload.get("track_total"))
            normalized_disc_total = _optional_pos_int(payload.get("disc_total"))
            normalized_release_date = str(payload.get("release_date") or "").strip() or None
            normalized_artwork_url = str(payload.get("artwork_url") or "").strip() or None
            normalized_genre = str(payload.get("genre") or "").strip() or None
            normalized_release_primary_type = str(payload.get("release_primary_type") or "").strip() or None
            release_secondary_raw = payload.get("release_secondary_types")
            normalized_release_secondary_types = []
            if isinstance(release_secondary_raw, (list, tuple, set)):
                for value in release_secondary_raw:
                    text = str(value or "").strip()
                    if text:
                        normalized_release_secondary_types.append(text)
            normalized_mb_youtube_urls = (
                list(payload.get("mb_youtube_urls"))
                if isinstance(payload.get("mb_youtube_urls"), (list, tuple, set))
                else []
            )
            if not normalized_artist or not normalized_track:
                raise HTTPException(status_code=400, detail="intent_enqueue_missing_artist_or_track")
            query = quote(f"{normalized_artist} {normalized_track}".strip())
            url = f"https://music.youtube.com/search?q={query}"
            normalized_media_mode = str(media_mode or "").strip().lower()
            target_media_type = "video" if normalized_media_mode == "music_video" else "music"
            canonical_metadata = {
                "artist": normalized_artist,
                "track": normalized_track,
                "album": normalized_album,
                "album_artist": normalized_album_artist,
                "release_date": normalized_release_date,
                "track_number": normalized_track_number,
                "disc_number": normalized_disc_number,
                "track_total": normalized_track_total,
                "disc_total": normalized_disc_total,
                "artwork_url": normalized_artwork_url,
                "genre": normalized_genre,
                "duration_ms": payload.get("duration_ms"),
                "recording_mbid": normalized_recording_mbid,
                "mb_recording_id": normalized_recording_mbid,
                "mb_release_id": normalized_release_mbid,
                "mb_release_group_id": normalized_release_group_mbid,
                "mb_youtube_urls": normalized_mb_youtube_urls,
                "release_primary_type": normalized_release_primary_type,
                "release_secondary_types": normalized_release_secondary_types,
            }
            canonical_id = _build_music_track_canonical_id(
                normalized_artist,
                normalized_album,
                payload.get("track_number"),
                normalized_track,
                recording_mbid=normalized_recording_mbid,
                mb_release_id=normalized_release_mbid,
                mb_release_group_id=normalized_release_group_mbid,
                disc_number=payload.get("disc_number"),
            )
            enqueue_payload = build_download_job_payload(
                config=runtime_config,
                origin=origin,
                origin_id=origin_id,
                media_type=target_media_type,
                media_intent="music_track",
                media_mode=normalized_media_mode,
                source="youtube_music",
                url=url,
                input_url=url,
                destination=destination,
                base_dir=base_dir,
                final_format_override=final_format,
                resolved_metadata=canonical_metadata,
                output_template_overrides={
                    "audio_mode": target_media_type == "music",
                    "album_artist": normalized_album_artist,
                    "track_number": normalized_track_number,
                    "disc_number": normalized_disc_number,
                    "track_total": normalized_track_total,
                    "disc_total": normalized_disc_total,
                    "release_date": normalized_release_date,
                    "duration_ms": payload.get("duration_ms"),
                    "artwork_url": normalized_artwork_url,
                    "genre": normalized_genre,
                    "recording_mbid": normalized_recording_mbid,
                    "mb_recording_id": normalized_recording_mbid,
                    "mb_release_id": normalized_release_mbid,
                    "mb_release_group_id": normalized_release_group_mbid,
                    "mb_youtube_urls": normalized_mb_youtube_urls,
                    "release_primary_type": normalized_release_primary_type,
                    "release_secondary_types": normalized_release_secondary_types,
                },
                canonical_id=canonical_id,
            )
            job_id, created, dedupe_reason = store.enqueue_job(
                **enqueue_payload,
                force_requeue=force_redownload,
            )
            logging.info(
                "Intent payload queued playlist_id=%s spotify_track_id=%s job_id=%s created=%s dedupe_reason=%s",
                payload.get("playlist_id"),
                payload.get("spotify_track_id"),
                job_id,
                bool(created),
                dedupe_reason,
            )
            return {
                "job_id": job_id,
                "created": bool(created),
                "dedupe_reason": dedupe_reason,
            }

        if media_intent == "music_track":
            recording_mbid = str(
                payload.get("recording_mbid")
                or payload.get("mb_recording_id")
                or ""
            ).strip()
            if not recording_mbid:
                logging.error("[MUSIC] enqueue_rejected missing_recording_mbid")
                raise HTTPException(status_code=400, detail="recording_mbid required for music_track enqueue")
            return _enqueue_music_query_job(
                str(payload.get("artist") or ""),
                str(payload.get("track") or payload.get("title") or ""),
                str(payload.get("album") or ""),
                recording_mbid=recording_mbid,
                mb_release_id=str(payload.get("mb_release_id") or payload.get("release_id") or ""),
                mb_release_group_id=str(payload.get("mb_release_group_id") or payload.get("release_group_id") or ""),
                media_mode=str(payload.get("media_mode") or ""),
            )

        resolved_media = payload.get("resolved_media") if isinstance(payload.get("resolved_media"), dict) else {}
        media_url = str(resolved_media.get("media_url") or payload.get("url") or "").strip()
        if not media_url:
            if is_music_mode_origin:
                logging.warning("[MUSIC] enqueue_rejected music_mode_requires_mbid")
                raise HTTPException(status_code=400, detail="music_mode_requires_mbid")
            fallback_artist = str(payload.get("artist") or "").strip()
            fallback_track = str(payload.get("track") or payload.get("title") or "").strip()
            fallback_album = str(payload.get("album") or "").strip() or None
            if fallback_artist and fallback_track:
                return _enqueue_music_query_job(
                    fallback_artist,
                    fallback_track,
                    fallback_album,
                    media_mode=str(payload.get("media_mode") or ""),
                )
            logging.warning("Intent enqueue skipped: no media URL or searchable artist/title available")
            raise HTTPException(status_code=400, detail="intent_enqueue_missing_media_url")
        source = str(resolved_media.get("source_id") or payload.get("source") or resolve_source(media_url)).strip() or "unknown"
        music_metadata = _to_dict(payload.get("music_metadata"))
        external_ids = music_metadata.get("external_ids") if isinstance(music_metadata.get("external_ids"), dict) else {}
        canonical_id = str(
            music_metadata.get("isrc")
            or music_metadata.get("mbid")
            or ""
        ).strip() or extract_external_track_canonical_id(
            external_ids,
            fallback_spotify_id=payload.get("spotify_track_id"),
        )
        requested_media_type = str(payload.get("media_type") or "").strip().lower()
        if requested_media_type in {"music", "audio"}:
            target_media_type = "music"
        elif requested_media_type == "video":
            target_media_type = "video"
        elif music_metadata or is_music_mode_origin:
            target_media_type = "music"
        else:
            target_media_type = resolve_media_type(runtime_config, url=media_url)
        enqueue_payload = build_download_job_payload(
            config=runtime_config,
            origin=origin,
            origin_id=origin_id,
            media_type=target_media_type,
            media_intent=media_intent,
            media_mode=str(payload.get("media_mode") or ""),
            source=source,
            url=media_url,
            input_url=media_url,
            destination=destination,
            base_dir=base_dir,
            final_format_override=final_format,
            resolved_metadata=music_metadata,
            output_template_overrides={
                "audio_mode": target_media_type == "music",
                "duration_ms": resolved_media.get("duration_ms"),
                "kind": payload.get("kind"),
            },
            canonical_id=canonical_id,
            external_id=str(payload.get("external_id") or "").strip() or None,
        )
        job_id, created, dedupe_reason = store.enqueue_job(
            **enqueue_payload,
            force_requeue=force_redownload,
        )
        logging.info(
            "Intent payload queued playlist_id=%s spotify_track_id=%s job_id=%s created=%s dedupe_reason=%s",
            payload.get("playlist_id"),
            payload.get("spotify_track_id"),
            job_id,
            bool(created),
            dedupe_reason,
        )
        return {
            "job_id": job_id,
            "created": bool(created),
            "dedupe_reason": dedupe_reason,
        }
