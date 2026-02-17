import logging

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
    for rec in recordings:
        candidate = _recording_to_candidate(rec)
        if candidate:
            candidates.append(candidate)
    return candidates


def _recording_to_candidate(rec):
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
        track_number = _find_track_number(release_id, recording_id)
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


def _find_track_number(release_id, recording_id):
    service = get_musicbrainz_service()
    try:
        release_data = service.get_release(release_id, includes=["recordings"])
    except Exception:
        logging.debug("MusicBrainz release lookup failed for %s", release_id)
        return None
    media = (release_data.get("release") or {}).get("medium-list") or []
    for medium in media:
        tracks = medium.get("track-list") or []
        for track in tracks:
            recording = track.get("recording") or {}
            if recording.get("id") == recording_id:
                return track.get("position") or track.get("number")
    return None
