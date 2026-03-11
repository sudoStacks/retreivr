from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WEBUI_APP = REPO_ROOT / "webUI" / "app.js"
API_MAIN = REPO_ROOT / "api" / "main.py"


def _extract_function_block(source: str, func_name: str) -> str:
    marker = f"function {func_name}("
    start = source.find(marker)
    assert start >= 0, f"{func_name} not found"
    brace_start = source.find("{", start)
    assert brace_start >= 0, f"{func_name} opening brace not found"

    depth = 0
    i = brace_start
    while i < len(source):
        ch = source[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return source[start : i + 1]
        i += 1
    raise AssertionError(f"{func_name} closing brace not found")


def test_ui_config_save_contract_snapshot() -> None:
    app_js = WEBUI_APP.read_text(encoding="utf-8")
    block = _extract_function_block(app_js, "buildConfigFromForm")

    expected_fragments = [
        "base.enable_watcher = watcherEnabled;",
        "base.watch_policy = watcherPolicy;",
        "base.schedule = buildSchedulePayloadFromForm();",
        "base.telegram = { ...existingTelegram };",
        "base.accounts = accounts;",
        "base.playlists = playlists;",
    ]
    for fragment in expected_fragments:
        assert fragment in block, f"Missing UI config contract fragment: {fragment}"


def test_backend_config_contract_snapshot() -> None:
    main_py = API_MAIN.read_text(encoding="utf-8")

    expected_fragments = [
        "def _config_watcher_enabled(",
        'if isinstance(config.get("enable_watcher"), bool):',
        'watcher_cfg = config.get("watcher")',
        "policy = normalize_watch_policy(payload)",
        "enable_watcher = _config_watcher_enabled(payload)",
        "_apply_watch_policy(policy)",
    ]
    for fragment in expected_fragments:
        assert fragment in main_py, f"Missing backend config contract fragment: {fragment}"

