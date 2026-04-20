from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Query


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


def _browse_root() -> Path:
    """
    Root directory exposed for host filesystem browsing.
    Defaults to / so all drives are reachable for Docker volume mapping.
    Override with RETREIVR_BROWSE_ROOT in the .env file if you want a narrower scope.
    """
    raw = os.environ.get("RETREIVR_BROWSE_ROOT", "").strip()
    return Path(raw).expanduser().resolve() if raw else Path("/")


def _resolve_browse_path(rel_path: str, root: Path) -> tuple[str, str]:
    rel_path = (rel_path or "").strip()
    if os.path.isabs(rel_path):
        raise HTTPException(status_code=400, detail="path must be relative")
    normalized = os.path.normpath(rel_path) if rel_path else ""
    if normalized in (".", os.curdir):
        normalized = ""
    if normalized.startswith(".."):
        raise HTTPException(status_code=403, detail="path not allowed")
    base = str(root)
    abs_path = os.path.realpath(os.path.join(base, normalized) if normalized else base)
    if os.path.commonpath([abs_path, base]) != base:
        raise HTTPException(status_code=403, detail="path not allowed")
    return normalized, abs_path


def _list_entries(base: str, directory: str, mode: str, ext: str, limit: int | None = None) -> list[dict]:
    entries = []
    try:
        scan = list(os.scandir(directory))
    except PermissionError:
        return []
    for entry in scan:
        if entry.name.startswith("."):
            continue
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
            is_file = entry.is_file(follow_symlinks=False)
        except OSError:
            continue
        if mode == "dir":
            if not is_dir:
                continue
        else:
            if not (is_dir or is_file):
                continue
            if is_file and ext and not entry.name.lower().endswith(ext):
                continue
        rel = os.path.relpath(entry.path, base)
        entries.append({
            "name": entry.name,
            "path": rel if rel != "." else "",
            "abs_path": entry.path,
            "type": "dir" if is_dir else "file",
        })
        if limit and len(entries) >= limit:
            break
    entries.sort(key=lambda e: (e["type"] != "dir", e["name"].lower()))
    return entries


@app.get("/workspace")
async def workspace():
    project = _project_dir()
    root = _browse_root()
    try:
        rel = os.path.relpath(str(project), str(root))
        browse_start = "" if rel == "." or rel.startswith("..") else rel
    except ValueError:
        browse_start = ""
    return {"workspace": str(project), "browse_start": browse_start}


@app.get("/browse")
async def browse(
    path: str = Query("", description="Relative path within the browse root"),
    mode: str = Query("dir"),
    ext: str = Query(""),
    limit: int | None = Query(None, ge=1, le=5000),
):
    mode = mode.lower()
    if mode not in {"dir", "file"}:
        raise HTTPException(status_code=400, detail="mode must be dir or file")
    ext = ext.strip().lower()
    if ext and not ext.startswith("."):
        ext = f".{ext}"
    root = _browse_root()
    rel_path, target = _resolve_browse_path(path, root)
    if not os.path.exists(target):
        raise HTTPException(status_code=404, detail=f"Path not found: {target}")
    if os.path.isfile(target):
        target = os.path.dirname(target)
        rel_path = os.path.relpath(target, str(root))
        if rel_path == ".":
            rel_path = ""
    parent = None
    if rel_path:
        parent = os.path.dirname(rel_path)
        if parent == ".":
            parent = ""
    try:
        entries = _list_entries(str(root), target, mode, ext, limit)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read directory: {exc}") from exc
    return {
        "root": "host",
        "path": rel_path,
        "abs_path": target,
        "parent": parent,
        "entries": entries,
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

