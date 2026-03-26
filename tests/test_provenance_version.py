from __future__ import annotations

from library import provenance


def test_get_retreivr_version_falls_back_to_pyproject(monkeypatch) -> None:
    monkeypatch.delenv("RETREIVR_VERSION", raising=False)
    monkeypatch.setattr(provenance.importlib_metadata, "version", lambda _name: (_ for _ in ()).throw(Exception("missing")))

    assert provenance.get_retreivr_version() == "0.9.15"


def test_get_retreivr_version_prefers_env(monkeypatch) -> None:
    monkeypatch.setenv("RETREIVR_VERSION", "0.9.15")

    assert provenance.get_retreivr_version() == "0.9.15"
