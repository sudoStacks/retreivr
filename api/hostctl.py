from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException


app = FastAPI(title="Retreivr Host Control", version="0.1.0")


def _project_dir(payload: dict | None = None) -> Path:
    raw = ""
    if isinstance(payload, dict):
        raw = str(payload.get("project_dir") or "").strip()
    raw = raw or os.environ.get("RETREIVR_WORKSPACE") or "/workspace"
    return Path(raw).expanduser().resolve()


def _compose_cmd() -> list[str]:
    docker_bin = shutil.which("docker")
    if docker_bin:
        return [docker_bin, "compose"]
    docker_compose = shutil.which("docker-compose")
    if docker_compose:
        return [docker_compose]
    raise RuntimeError("docker_compose_not_available")


def _run_compose(payload: dict | None, extra_args: list[str]) -> dict:
    project_dir = _project_dir(payload)
    if not project_dir.exists():
        raise RuntimeError("project_dir_missing")
    cmd = _compose_cmd()
    profiles = list((payload or {}).get("profiles") or [])
    for profile in profiles:
        if str(profile).strip():
            cmd.extend(["--profile", str(profile).strip()])
    cmd.extend(extra_args)
    proc = subprocess.run(
        cmd,
        cwd=str(project_dir),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "ok": proc.returncode == 0,
        "command": " ".join(cmd),
        "cwd": str(project_dir),
        "stdout": proc.stdout[-8000:],
        "stderr": proc.stderr[-8000:],
        "returncode": proc.returncode,
    }


@app.get("/health")
async def health():
    try:
        cmd = _compose_cmd()
        return {"status": "ok", "compose_command": " ".join(cmd)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "unavailable", "message": str(exc)}


@app.post("/compose/apply")
async def compose_apply(payload: dict = Body(...)):
    result = _run_compose(payload, ["up", "-d"])
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result)
    return result


@app.post("/compose/restart")
async def compose_restart(payload: dict = Body(...)):
    result = _run_compose(payload, ["restart"])
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result)
    return result


@app.post("/compose/ps")
async def compose_ps(payload: dict = Body(default={})):
    result = _run_compose(payload, ["ps", "--format", "json"])
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result)
    return result

