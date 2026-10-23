from __future__ import annotations

from pathlib import Path


_APP_JS = (Path(__file__).resolve().parents[1] / "webUI" / "app.js").read_text(encoding="utf-8")


def test_finished_direct_url_run_is_marked_completed_or_failed() -> None:
    # The server returns a run to state "idle" when it ends and never reports
    # "completed", so the Home card must derive the result from the finished run.
    body = _APP_JS.split("async function refreshHomeJobStatuses", 1)[1].split(
        "function startHomeJobPolling", 1
    )[0]
    assert 'runData.finished_at && String(runData.run_id || "") === String(candidate.run_id)' in body
    assert "statusPayload.run_failures" in body
    assert "statusPayload.run_successes" in body
    assert 'job = { status: "completed", last_error: "" }' in body
