from __future__ import annotations

import os
from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any


_REPO_ROOT = Path(__file__).resolve().parent.parent


def _normalize_version(value: Any) -> str:
    text = str(value or "").strip()
    return text if text and text.lower() != "unknown" else ""


def _read_version_from_pyproject() -> str:
    pyproject_path = _REPO_ROOT / "pyproject.toml"
    try:
        content = pyproject_path.read_text(encoding="utf-8")
    except Exception:
        return ""
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped.startswith("version"):
            continue
        _, _, raw_value = stripped.partition("=")
        normalized = _normalize_version(raw_value.strip().strip("\"'"))
        if normalized:
            return normalized
    return ""


def get_retreivr_version() -> str:
    env_version = _normalize_version(os.environ.get("RETREIVR_VERSION"))
    if env_version:
        return env_version
    try:
        package_version = _normalize_version(importlib_metadata.version("retreivr"))
        if package_version:
            return package_version
    except Exception:
        pass
    pyproject_version = _read_version_from_pyproject()
    if pyproject_version:
        return pyproject_version
    return "unknown"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_file_provenance(
    *,
    job: Any | None = None,
    source: Any = None,
    source_id: Any = None,
    acquired_at: Any = None,
) -> dict[str, str]:
    provenance = {
        "retreivr_managed": "true",
        "retreivr_version": get_retreivr_version(),
        "retreivr_acquired_at": str(acquired_at or utc_now_iso()).strip(),
    }

    job_id = str(getattr(job, "id", "") or "").strip() if job is not None else ""
    trace_id = str(getattr(job, "trace_id", "") or "").strip() if job is not None else ""
    if job_id:
        provenance["retreivr_job_id"] = job_id
    if trace_id:
        provenance["retreivr_trace_id"] = trace_id

    normalized_source = str(source or getattr(job, "source", "") or "").strip()
    normalized_source_id = str(source_id or getattr(job, "external_id", "") or "").strip()
    if normalized_source:
        provenance["retreivr_source"] = normalized_source
    if normalized_source_id:
        provenance["retreivr_source_id"] = normalized_source_id

    return provenance
