from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_musicbrainz_service_module():
    if "musicbrainzngs" not in sys.modules:
        stub = types.ModuleType("musicbrainzngs")
        stub.set_useragent = lambda *args, **kwargs: None
        stub.set_rate_limit = lambda *args, **kwargs: None
        sys.modules["musicbrainzngs"] = stub
    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")
        requests_stub.get = lambda *args, **kwargs: None
        sys.modules["requests"] = requests_stub
    return _load_module(
        "metadata.services.musicbrainz_service_retry_tests",
        _ROOT / "metadata" / "services" / "musicbrainz_service.py",
    )


def test_ssl_eof_transient_errors_expand_attempts_and_use_staggered_backoff(monkeypatch) -> None:
    mb = _load_musicbrainz_service_module()
    service = mb.MusicBrainzService(debug=True)
    monkeypatch.setattr(service, "_respect_rate_limit", lambda: None)
    slept = []
    monkeypatch.setattr(mb.time, "sleep", lambda seconds: slept.append(seconds))

    calls = {"count": 0}

    def _always_fail():
        calls["count"] += 1
        raise RuntimeError("SSLEOFError: EOF occurred in violation of protocol")

    try:
        service._call_with_retry(_always_fail, attempts=3, base_delay=0.3)
        assert False, "expected retry exhaustion"
    except RuntimeError:
        pass

    # Transient SSL EOF should auto-expand retries to 5 total attempts.
    assert calls["count"] == 5
    # Sleeps happen between attempts (attempts-1).
    assert len(slept) == 4
    assert slept == sorted(slept)


def test_non_transient_error_respects_requested_attempts(monkeypatch) -> None:
    mb = _load_musicbrainz_service_module()
    service = mb.MusicBrainzService(debug=True)
    monkeypatch.setattr(service, "_respect_rate_limit", lambda: None)
    slept = []
    monkeypatch.setattr(mb.time, "sleep", lambda seconds: slept.append(seconds))

    calls = {"count": 0}

    def _always_fail_non_transient():
        calls["count"] += 1
        raise RuntimeError("invalid include requested")

    try:
        service._call_with_retry(_always_fail_non_transient, attempts=3, base_delay=0.3)
        assert False, "expected retry exhaustion"
    except RuntimeError:
        pass

    assert calls["count"] == 3
    assert len(slept) == 2

