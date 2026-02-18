import io
import logging

from PIL import Image
from metadata.services.musicbrainz_service import get_musicbrainz_service


def fetch_artwork(release_id, max_size_px=1500):
    if not release_id:
        return None
    service = get_musicbrainz_service()
    try:
        payload = service.fetch_cover_art(release_id, timeout=10)
        if not payload:
            return None
    except Exception:
        logging.debug("Artwork download failed for release %s", release_id)
        return None
    content_type = payload.get("mime", "image/jpeg")
    data = payload.get("data")
    try:
        image = Image.open(io.BytesIO(data))
        if max_size_px:
            image.thumbnail((max_size_px, max_size_px))
        output = io.BytesIO()
        fmt = "JPEG" if content_type.endswith("jpeg") or content_type.endswith("jpg") else "PNG"
        image.save(output, format=fmt)
        data = output.getvalue()
        content_type = "image/jpeg" if fmt == "JPEG" else "image/png"
    except Exception:
        logging.debug("Artwork processing failed for release %s", release_id)
        return None
    return {
        "data": data,
        "mime": content_type,
    }
