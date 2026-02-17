import logging

from engine.search_scoring import token_overlap_score, tokenize
from metadata.providers.base import CanonicalMetadataProvider
from metadata.services.musicbrainz_service import get_musicbrainz_service


def _release_year(value):
    if not value:
        return None
    return str(value).split("-")[0]


def _score_track_match(artist, track, album, recording):
    artist_name = _extract_artist(recording)
    artist_score = token_overlap_score(tokenize(artist), tokenize(artist_name))
    track_score = token_overlap_score(tokenize(track), tokenize(recording.get("title")))
    if album:
        album_score = token_overlap_score(tokenize(album), tokenize(_extract_album_title(recording)))
        score = 0.55 * track_score + 0.35 * artist_score + 0.10 * album_score
    else:
        score = 0.60 * track_score + 0.40 * artist_score
    return score


def _score_album_match(artist, album, release):
    artist_name = _extract_release_artist(release)
    artist_score = token_overlap_score(tokenize(artist), tokenize(artist_name))
    album_score = token_overlap_score(tokenize(album), tokenize(release.get("title")))
    return 0.6 * album_score + 0.4 * artist_score


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


def _extract_album_title(rec):
    release_list = rec.get("release-list") or []
    if not release_list:
        return None
    return release_list[0].get("title")


def _parse_duration(value):
    try:
        if value is None:
            return None
        return int(round(int(value) / 1000))
    except Exception:
        return None


def _cover_art_url(release_id):
    return get_musicbrainz_service().cover_art_url(release_id)


class MusicBrainzMetadataProvider(CanonicalMetadataProvider):
    def __init__(self, *, cache=None, min_confidence=0.90):
        self.cache = cache
        self.min_confidence = float(min_confidence or 0.90)

    def resolve_track(self, artist, track, album=None):
        if not artist or not track:
            return None
        cache_key = None
        if self.cache:
            cache_key = f"mb:track:{artist}|{track}|{album or ''}"
            cached = self.cache.get(cache_key)
            if cached:
                return cached
        service = get_musicbrainz_service()
        try:
            result = service.search_recordings(artist, track, album=album, limit=8)
        except Exception:
            logging.exception("MusicBrainz search failed")
            return None
        recordings = result.get("recording-list") or []
        best = None
        best_score = 0.0
        for rec in recordings:
            score = _score_track_match(artist, track, album, rec)
            if score > best_score:
                best = rec
                best_score = score
        if not best or best_score < self.min_confidence:
            return None
        release_list = best.get("release-list") or []
        release = release_list[0] if release_list else {}
        release_id = release.get("id")
        canonical = {
            "kind": "track",
            "provider": "musicbrainz",
            "artist": _extract_artist(best) or artist,
            "album": release.get("title") or album,
            "track": best.get("title") or track,
            "release_year": _release_year(release.get("date")),
            "duration_sec": _parse_duration(best.get("length")),
            "artwork": [
                {"url": _cover_art_url(release_id), "width": None, "height": None}
            ]
            if release_id
            else [],
            "external_ids": {
                "musicbrainz_recording_id": best.get("id"),
                "musicbrainz_release_id": release_id,
            },
        }
        if self.cache and cache_key:
            self.cache.set(cache_key, canonical)
        return canonical

    def resolve_album(self, artist, album):
        if not artist or not album:
            return None
        cache_key = None
        if self.cache:
            cache_key = f"mb:album:{artist}|{album}"
            cached = self.cache.get(cache_key)
            if cached:
                return cached
        service = get_musicbrainz_service()
        try:
            result = service.search_releases(artist, album, limit=5)
        except Exception:
            logging.exception("MusicBrainz album search failed")
            return None
        releases = result.get("release-list") or []
        best = None
        best_score = 0.0
        for release in releases:
            score = _score_album_match(artist, album, release)
            if score > best_score:
                best = release
                best_score = score
        if not best or best_score < self.min_confidence:
            return None
        release_id = best.get("id")
        tracks = []
        if release_id:
            try:
                release_data = service.get_release(release_id, includes=["recordings"])
                media = (release_data.get("release") or {}).get("medium-list") or []
                for medium in media:
                    for track_data in medium.get("track-list") or []:
                        recording = track_data.get("recording") or {}
                        tracks.append(
                            {
                                "title": track_data.get("title") or recording.get("title"),
                                "duration_sec": _parse_duration(track_data.get("length") or recording.get("length")),
                                "track_number": track_data.get("position") or track_data.get("number"),
                                "disc_number": medium.get("position"),
                            }
                        )
            except Exception:
                logging.debug("MusicBrainz release lookup failed for %s", release_id)
        canonical = {
            "kind": "album",
            "provider": "musicbrainz",
            "artist": _extract_release_artist(best) or artist,
            "album": best.get("title") or album,
            "release_year": _release_year(best.get("date")),
            "artwork": [
                {"url": _cover_art_url(release_id), "width": None, "height": None}
            ]
            if release_id
            else [],
            "external_ids": {
                "musicbrainz_release_id": release_id,
            },
            "track_count": int(best.get("track-count") or 0) or None,
            "tracks": tracks,
        }
        if self.cache and cache_key:
            self.cache.set(cache_key, canonical)
        return canonical
