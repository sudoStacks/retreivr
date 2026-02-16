"""Media validation helpers."""

from __future__ import annotations

import logging

from media.ffprobe import get_media_duration

logger = logging.getLogger(__name__)


def validate_duration(file_path: str, expected_ms: int, tolerance_seconds: float = 5.0) -> bool:
    """Validate that a media file duration is within tolerance of an expected value.

    The function resolves the actual duration in seconds by calling
    :func:`media.ffprobe.get_media_duration`, converts ``expected_ms`` from
    milliseconds to seconds, and compares the absolute delta.

    Returns:
        ``True`` when ``abs(actual_seconds - expected_seconds) <= tolerance_seconds``.
        ``False`` when the duration falls outside tolerance or probing fails.

    Constraints:
        - ``expected_ms`` and ``tolerance_seconds`` must be non-negative.
        - Any ffprobe/probe parsing error is handled non-fatally and returns ``False``.
    """
    if expected_ms < 0:
        logger.warning("Duration validation failed: expected_ms must be non-negative")
        return False
    if tolerance_seconds < 0:
        logger.warning("Duration validation failed: tolerance_seconds must be non-negative")
        return False

    try:
        actual_duration_seconds = get_media_duration(file_path)
    except Exception:
        logger.exception("Failed to probe media duration for path=%s", file_path)
        return False

    expected_seconds = expected_ms / 1000.0
    return abs(actual_duration_seconds - expected_seconds) <= tolerance_seconds
