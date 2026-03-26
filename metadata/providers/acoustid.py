import logging
import shutil


def match_recording(file_path, api_key):
    if not shutil.which("fpcalc"):
        logging.warning("fpcalc not found; skipping acoustid lookup")
        return None
    try:
        import acoustid
    except Exception:
        logging.warning("pyacoustid not installed; skipping acoustid lookup")
        return None
    try:
        results = list(acoustid.match(api_key, file_path))
    except Exception:
        logging.exception("AcoustID match failed")
        return None
    if not results:
        logging.info("AcoustID returned no candidates for %s", file_path)
        return None
    best = max(results, key=lambda item: item[0])
    score, recording_id, title, artist = best
    return {
        "recording_id": recording_id,
        "title": title,
        "artist": artist,
        "album": None,
        "album_artist": None,
        "track_number": None,
        "release_id": None,
        "year": None,
        "duration": None,
        "acoustid_score": score,
    }
