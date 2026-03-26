from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "engine" / "resolution_auth.py"
    spec = importlib.util.spec_from_file_location("resolution_auth_test_module", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resolve_node_auth_accepts_matching_configured_key() -> None:
    module = _load_module()
    auth = module.resolve_node_auth(
        {
            "resolution_api": {
                "require_api_key": True,
                "nodes": [{"node_id": "nodeA", "api_key": "secret-a"}],
            }
        },
        provided_key="secret-a",
        provided_node_id="nodeA",
    )
    assert auth["authenticated"] is True
    assert auth["node_id"] == "nodeA"


def test_resolve_node_auth_rejects_node_mismatch() -> None:
    module = _load_module()
    try:
        module.resolve_node_auth(
            {
                "resolution_api": {
                    "require_api_key": True,
                    "nodes": [{"node_id": "nodeA", "api_key": "secret-a"}],
                }
            },
            provided_key="secret-a",
            provided_node_id="nodeB",
        )
    except ValueError as exc:
        assert str(exc) == "node_id_mismatch"
    else:
        raise AssertionError("expected node_id_mismatch")
