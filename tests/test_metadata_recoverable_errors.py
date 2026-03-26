from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_acoustid_empty_results_return_none(monkeypatch, tmp_path: Path) -> None:
    acoustid_module = _load_module("metadata_providers_acoustid_test", _ROOT / "metadata" / "providers" / "acoustid.py")

    class _FakeAcoustId:
        @staticmethod
        def match(api_key, file_path):
            _ = api_key, file_path
            return iter(())

    monkeypatch.setattr(acoustid_module.shutil, "which", lambda _: "/usr/bin/fpcalc")
    sys.modules["acoustid"] = _FakeAcoustId
    try:
        result = acoustid_module.match_recording(str(tmp_path / "file.m4a"), "key")
    finally:
        sys.modules.pop("acoustid", None)
    assert result is None
