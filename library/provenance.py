from __future__ import annotations

from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from typing import Any


def get_retreivr_version() -> str:
    try:
        return str(importlib_metadata.version("retreivr")).strip() or "unknown"
    except Exception:
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
