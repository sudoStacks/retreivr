import logging

from engine.search_scoring import token_overlap_score, tokenize
from metadata.providers.base import CanonicalMetadataProvider
from metadata.services.musicbrainz_service import get_musicbrainz_service



def search_recordings(artist, title, album=None, limit=5):
    if not artist or not title:
        return []
    service = get_musicbrainz_service()
    try:
        result = service.search_recordings(artist, title, album=album, limit=limit)
    except Exception:
        logging.exception("MusicBrainz search failed")
        return []
    recordings = result.get("recording-list") or []
    candidates = []
    release_lookup_cache = {}
    for rec in recordings:
        candidate = _recording_to_candidate(rec, release_lookup_cache=release_lookup_cache)
        if candidate:
            candidates.append(candidate)
    return candidates


def _recording_to_candidate(rec, *, release_lookup_cache=None):
    recording_id = rec.get("id")
    title = rec.get("title")
    artist = _extract_artist(rec)
    duration = _parse_duration(rec.get("length"))
    release = None
    release_id = None
    release_date = None
    track_number = None
    release_list = rec.get("release-list") or []
    if release_list:
        release = release_list[0]
        release_id = release.get("id")
        release_date = release.get("date")
    if release_id and recording_id:
        track_number = _find_track_number(
            release_id,
            recording_id,
            release_lookup_cache=release_lookup_cache,
        )
    year = release_date.split("-")[0] if release_date else None
    return {
        "recording_id": recording_id,
        "title": title,
        "artist": artist,
        "album": release.get("title") if release else None,
        "album_artist": _extract_release_artist(release) if release else None,
        "track_number": track_number,
        "release_id": release_id,
        "year": year,
        "duration": duration,
    }


def _extract_artist(rec):
    credit = rec.get("artist-credit") or []
    if credit and isinstance(credit[0], dict):
        artist = credit[0].get("artist", {}).get("name")
        if artist:
            return artist
    return rec.get("artist-credit-phrase")


def _extract_release_artist(release):
    if not release:
        return None
    credit = release.get("artist-credit") or []
    if credit and isinstance(credit[0], dict):
        return credit[0].get("artist", {}).get("name")
    return release.get("artist-credit-phrase")


def _parse_duration(value):
    try:
        if value is None:
            return None
        return int(round(int(value) / 1000))
    except Exception:
        return None


def _find_track_number(release_id, recording_id, *, release_lookup_cache=None):
    cache = release_lookup_cache if isinstance(release_lookup_cache, dict) else {}
    release_data = cache.get(release_id)
    service = get_musicbrainz_service()
    if release_data is None:
        try:
            release_data = service.get_release(release_id, includes=["recordings"])
        except Exception:
            logging.debug("MusicBrainz release lookup failed for %s", release_id)
            return None
        if cache is not None:
            cache[release_id] = release_data
    media = (release_data.get("release") or {}).get("medium-list") or []
    for medium in media:
        tracks = medium.get("track-list") or []
        for track in tracks:
            recording = track.get("recording") or {}
            if recording.get("id") == recording_id:
                return track.get("position") or track.get("number")
    return None


def _year(value):
    if not value:
        return None
    return str(value).split("-")[0]


def _score_track_match(artist, track, album, candidate):
    artist_score = token_overlap_score(tokenize(artist), tokenize(candidate.get("artist")))
    track_score = token_overlap_score(tokenize(track), tokenize(candidate.get("title")))
    if album:
        album_score = token_overlap_score(tokenize(album), tokenize(candidate.get("album")))
        return (0.55 * track_score) + (0.35 * artist_score) + (0.10 * album_score)
    return (0.60 * track_score) + (0.40 * artist_score)


def _score_album_match(artist, album, candidate):
    artist_score = token_overlap_score(tokenize(artist), tokenize(candidate.get("artist_credit")))
    album_score = token_overlap_score(tokenize(album), tokenize(candidate.get("title")))
    return (0.60 * album_score) + (0.40 * artist_score)


class MusicBrainzMetadataProvider(CanonicalMetadataProvider):
    def __init__(self, *, min_confidence=0.70):
        self.min_confidence = float(min_confidence or 0.70)

    def resolve_track(self, artist, track, *, album=None):
        if not artist or not track:
            return None
        candidates = search_recordings(artist, track, album=album, limit=8)
        best_item = None
        best_score = 0.0
        for item in candidates:
            score = _score_track_match(artist, track, album, item)
            if score > best_score:
                best_score = score
                best_item = item
        if not best_item or best_score < self.min_confidence:
            return None
        return {
            "kind": "track",
            "provider": "musicbrainz",
            "artist": best_item.get("artist") or artist,
            "album": best_item.get("album") or album,
            "track": best_item.get("title") or track,
            "release_year": _year(best_item.get("year")),
            "album_type": None,
            "duration_sec": best_item.get("duration"),
            "artwork": [],
            "external_ids": {
                "musicbrainz_recording_id": best_item.get("recording_id"),
                "musicbrainz_release_id": best_item.get("release_id"),
                "isrc": None,
            },
            "track_number": best_item.get("track_number"),
            "disc_number": None,
            "album_track_count": None,
        }

    def resolve_album(self, artist, album):
        if not artist or not album:
            return None
        service = get_musicbrainz_service()
        candidates = service.search_release_groups(f"{artist} {album}", limit=5)
        best_item = None
        best_score = 0.0
        for item in candidates:
            score = _score_album_match(artist, album, item)
            if score > best_score:
                best_score = score
                best_item = item
        if not best_item or best_score < self.min_confidence:
            return None

        release_group_id = best_item.get("release_group_id")
        selection = service.pick_best_release_with_reason(release_group_id)
        release_id = selection.get("release_id") if isinstance(selection, dict) else None
        tracks_payload = service.fetch_release_tracks(release_id) if release_id else []
        tracks = []
        for item in tracks_payload:
            tracks.append(
                {
                    "title": item.get("title"),
                    "duration_sec": int((item.get("duration_ms") or 0) / 1000) if item.get("duration_ms") else None,
                    "track_number": item.get("track_number"),
                    "disc_number": item.get("disc_number"),
                }
            )
        return {
            "kind": "album",
            "provider": "musicbrainz",
            "artist": best_item.get("artist_credit") or artist,
            "album": best_item.get("title") or album,
            "release_year": _year(best_item.get("first_release_date")),
            "album_type": best_item.get("primary_type"),
            "artwork": [],
            "external_ids": {
                "musicbrainz_release_group_id": release_group_id,
                "musicbrainz_release_id": release_id,
            },
            "track_count": len(tracks) if tracks else None,
            "tracks": tracks,
        }
