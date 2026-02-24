import io
import logging

from PIL import Image
import requests
from metadata.services.musicbrainz_service import get_musicbrainz_service


def _normalize_artwork_blob(data, content_type, *, context):
    if not data:
        return None
    try:
        image = Image.open(io.BytesIO(data))
        max_size_px = context.get("max_size_px")
        if max_size_px:
            image.thumbnail((max_size_px, max_size_px))
        output = io.BytesIO()
        fmt = "JPEG" if str(content_type or "").endswith(("jpeg", "jpg")) else "PNG"
        image.save(output, format=fmt)
        return {
            "data": output.getvalue(),
            "mime": "image/jpeg" if fmt == "JPEG" else "image/png",
        }
    except Exception:
        logging.debug("Artwork processing failed for %s", context.get("label"))
        return None


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
    return _normalize_artwork_blob(
        payload.get("data"),
        payload.get("mime", "image/jpeg"),
        context={"label": f"release {release_id}", "max_size_px": max_size_px},
    )


def fetch_artwork_from_url(artwork_url, max_size_px=1500, timeout=10):
    url = str(artwork_url or "").strip()
    if not url:
        return None
    try:
        resp = requests.get(url, timeout=timeout)
    except Exception:
        logging.debug("Artwork URL download failed for %s", url)
        return None
    if not resp.ok or not resp.content:
        return None
    return _normalize_artwork_blob(
        resp.content,
        resp.headers.get("Content-Type") or "image/jpeg",
        context={"label": url, "max_size_px": max_size_px},
    )
