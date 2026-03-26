from __future__ import annotations

import os
from typing import Iterator

from fastapi import Request
from fastapi.responses import Response, StreamingResponse


def guess_browser_media_type(file_path: str, fallback: str | None = None) -> str:
    lowered = str(file_path or "").strip().lower()
    if lowered.endswith(".m4a"):
        return "audio/mp4"
    if lowered.endswith(".mp3"):
        return "audio/mpeg"
    if lowered.endswith(".aac"):
        return "audio/aac"
    if lowered.endswith(".wav"):
        return "audio/wav"
    return str(fallback or "application/octet-stream")


def iter_file_range(path: str, start: int, end: int, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
    remaining = max(0, end - start + 1)
    with open(path, "rb") as handle:
        handle.seek(max(0, start))
        while remaining > 0:
            chunk = handle.read(min(chunk_size, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk


def resolve_byte_range(range_header: str | None, file_size: int) -> tuple[int, int] | None:
    raw = str(range_header or "").strip()
    if not raw or not raw.lower().startswith("bytes=") or file_size <= 0:
        return None
    value = raw[6:].split(",", 1)[0].strip()
    if "-" not in value:
        return None
    start_text, end_text = value.split("-", 1)
    start_text = start_text.strip()
    end_text = end_text.strip()
    try:
        if start_text:
            start = int(start_text)
            end = int(end_text) if end_text else file_size - 1
        else:
            suffix = int(end_text)
            if suffix <= 0:
                return None
            start = max(0, file_size - suffix)
            end = file_size - 1
    except (TypeError, ValueError):
        return None
    if start < 0 or end < start or start >= file_size:
        return None
    end = min(end, file_size - 1)
    return start, end


def build_media_file_response(
    request: Request,
    file_path: str,
    *,
    media_type: str,
    content_disposition: str = "inline",
):
    file_size = int(os.path.getsize(file_path))
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": content_disposition,
    }
    byte_range = resolve_byte_range(request.headers.get("range"), file_size)
    if byte_range is None:
        headers["Content-Length"] = str(file_size)
        return StreamingResponse(
            iter_file_range(file_path, 0, max(0, file_size - 1)),
            media_type=media_type,
            headers=headers,
        )
    start, end = byte_range
    content_length = end - start + 1
    headers["Content-Length"] = str(content_length)
    headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    return StreamingResponse(
        iter_file_range(file_path, start, end),
        status_code=206,
        media_type=media_type,
        headers=headers,
    )


def build_invalid_range_response(file_size: int) -> Response:
    return Response(
        status_code=416,
        headers={"Content-Range": f"bytes */{max(0, int(file_size))}"},
    )
