from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WEBUI_APP = REPO_ROOT / "webUI" / "app.js"
WEBUI_INDEX = REPO_ROOT / "webUI" / "index.html"


def test_setup_wizard_includes_preflight_stage() -> None:
    source = WEBUI_APP.read_text(encoding="utf-8")
    assert 'id: "preflight"' in source
    assert 'data-setup-action="run-preflight"' in source
    assert 'await fetchJson("/api/setup/preflight",' in source


def test_existing_service_api_key_fields_present_in_wizard() -> None:
    source = WEBUI_APP.read_text(encoding="utf-8")
    expected = (
        "existing_radarr_api_key",
        "existing_sonarr_api_key",
        "existing_readarr_api_key",
        "existing_prowlarr_api_key",
        "existing_bazarr_api_key",
    )
    for key in expected:
        assert key in source, f"missing wizard field: {key}"
    assert "Add your Radarr API key to continue, or enable key discovery for Radarr." in source
    assert "Add your Sonarr API key to continue, or enable key discovery for Sonarr." in source
    assert "Add your Readarr API key to continue, or enable key discovery for Readarr." in source
    assert "Add your Prowlarr API key to continue, or enable key discovery for Prowlarr." in source
    assert "Add your Bazarr API key to continue, or enable key discovery for Bazarr." in source


def test_existing_connect_payload_uses_wizard_api_keys() -> None:
    source = WEBUI_APP.read_text(encoding="utf-8")
    assert "function buildSetupExistingConnectPayload" in source
    assert "api_key: draft.existing_radarr_api_key" in source
    assert "api_key: draft.existing_sonarr_api_key" in source
    assert "api_key: draft.existing_readarr_api_key" in source
    assert "api_key: draft.existing_prowlarr_api_key" in source
    assert "api_key: draft.existing_bazarr_api_key" in source
    assert "body: JSON.stringify(buildSetupExistingConnectPayload(state.setupWizard.draft))" in source


def test_settings_exposes_retreivr_paths_tab() -> None:
    source = WEBUI_INDEX.read_text(encoding="utf-8")
    assert 'data-settings-link="paths"' in source
    assert 'id="settings-paths"' in source
    assert 'id="cfg-stack-paths-save"' in source
