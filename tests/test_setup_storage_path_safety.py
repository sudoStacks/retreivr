from __future__ import annotations

import pytest
from fastapi import HTTPException

from api.main import _validate_stack_path_payload


@pytest.mark.parametrize("path", ["/downloads", "/downloads/Music", "/media", "/workspace/media", "../outside"])
def test_container_or_escaping_stack_paths_are_rejected(path: str) -> None:
    with pytest.raises(HTTPException) as exc_info:
        _validate_stack_path_payload({"downloads_root": path})

    assert exc_info.value.status_code == 422


@pytest.mark.parametrize("path", ["./downloads", "Movies", "/mnt/Media/Videos"])
def test_host_or_compose_relative_stack_paths_are_allowed(path: str) -> None:
    _validate_stack_path_payload({"downloads_root": path})


def test_guided_setup_picker_is_locked_to_host_root() -> None:
    source = open("webUI/app.js", encoding="utf-8").read()

    assert 'openBrowser(targetInput, spec.root, spec.mode, spec.ext, start, !!spec.showHidden, ["host"]);' in source
    assert "await saveSetupStack();" in source[source.index("async function saveSetupWizardProgress"):]
