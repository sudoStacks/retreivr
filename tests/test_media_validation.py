from __future__ import annotations

import shutil
import wave
from pathlib import Path

import pytest

from media.validation import validate_duration

_FFPROBE_AVAILABLE = shutil.which("ffprobe") is not None


def _write_silent_wav(path: Path, duration_seconds: float, sample_rate: int = 44_100) -> None:
    nframes = int(duration_seconds * sample_rate)
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(b"\x00\x00" * nframes)


@pytest.mark.skipif(not _FFPROBE_AVAILABLE, reason="ffprobe is required for duration probe tests")
def test_validate_duration_returns_true_within_tolerance(tmp_path: Path) -> None:
    audio_path = tmp_path / "short.wav"
    _write_silent_wav(audio_path, duration_seconds=1.0)

    assert validate_duration(str(audio_path), expected_ms=1_000, tolerance_seconds=0.5) is True


@pytest.mark.skipif(not _FFPROBE_AVAILABLE, reason="ffprobe is required for duration probe tests")
def test_validate_duration_returns_false_when_duration_differs_significantly(tmp_path: Path) -> None:
    audio_path = tmp_path / "short.wav"
    _write_silent_wav(audio_path, duration_seconds=1.0)

    assert validate_duration(str(audio_path), expected_ms=10_000, tolerance_seconds=1.0) is False


def test_validate_duration_returns_false_when_probe_fails(monkeypatch, tmp_path: Path) -> None:
    audio_path = tmp_path / "missing-or-invalid.wav"

    def _raise_probe_error(_file_path: str) -> float:
        raise RuntimeError("ffprobe failed")

    monkeypatch.setattr("media.validation.get_media_duration", _raise_probe_error)

    assert validate_duration(str(audio_path), expected_ms=1_000, tolerance_seconds=0.5) is False
