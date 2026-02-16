"""Wrapper utilities for retrieving media information using ffprobe."""

from __future__ import annotations

import json
import subprocess


def get_media_duration(file_path: str) -> float:
    """Return media duration in seconds using ``ffprobe`` JSON output.

    The function executes ``ffprobe`` for the provided file, parses the JSON
    payload, and returns ``format.duration`` as a float.

    Raises:
        RuntimeError: If ``ffprobe`` execution fails or the command is missing.
        ValueError: If duration data is missing or not parseable as a float.
    """
    command = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        file_path,
    ]

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("ffprobe is not installed or not available in PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"ffprobe timed out while probing: {file_path}") from exc
    except subprocess.CalledProcessError as exc:
        stderr_text = (exc.stderr or "").strip()
        raise RuntimeError(f"ffprobe failed for {file_path}: {stderr_text or exc}") from exc

    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError(f"ffprobe returned invalid JSON for {file_path}") from exc

    duration_value = (payload.get("format") or {}).get("duration")
    if duration_value in (None, ""):
        raise ValueError(f"ffprobe did not return a duration for {file_path}")

    try:
        return float(duration_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"ffprobe returned a non-numeric duration for {file_path}") from exc
