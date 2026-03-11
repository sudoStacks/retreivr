from __future__ import annotations

import sys
import types

from metadata.providers import acoustid as provider


def test_match_recording_returns_none_when_fpcalc_missing(monkeypatch, caplog) -> None:
    monkeypatch.setattr(provider.shutil, "which", lambda _name: None)

    result = provider.match_recording("/tmp/fake.mp3", "client-key")

    assert result is None
    assert "fpcalc not found" in caplog.text


def test_match_recording_returns_best_scored_hit(monkeypatch) -> None:
    monkeypatch.setattr(provider.shutil, "which", lambda _name: "/usr/bin/fpcalc")

    fake_module = types.ModuleType("acoustid")
    fake_module.match = lambda _api_key, _file_path: [
        (0.45, "rec-1", "Song A", "Artist A"),
        (0.98, "rec-2", "Song B", "Artist B"),
    ]
    monkeypatch.setitem(sys.modules, "acoustid", fake_module)

    result = provider.match_recording("/tmp/fake.mp3", "client-key")

    assert result is not None
    assert result["recording_id"] == "rec-2"
    assert result["title"] == "Song B"
    assert result["artist"] == "Artist B"
    assert result["acoustid_score"] == 0.98
