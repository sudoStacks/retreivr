import logging
import queue as queue_lib
import threading

from .worker import MetadataWorker

DEFAULT_METADATA_CONFIG = {
    "enabled": True,
    "confidence_threshold": 70,
    "use_acoustid": False,
    "acoustid_api_key": "",
    "embed_artwork": True,
    "allow_overwrite_tags": True,
    "max_artwork_size_px": 1500,
    "rate_limit_seconds": 1.5,
    "dry_run": False,
}

_QUEUE = queue_lib.Queue()
_WORKER = None
_LOCK = threading.Lock()


def normalize_metadata_config(config):
    normalized = dict(DEFAULT_METADATA_CONFIG)
    if isinstance(config, dict):
        raw = config.get("music_metadata")
        if isinstance(raw, dict):
            for key in DEFAULT_METADATA_CONFIG:
                if key in raw:
                    normalized[key] = raw[key]
    threshold = normalized.get("confidence_threshold")
    if not isinstance(threshold, int):
        normalized["confidence_threshold"] = DEFAULT_METADATA_CONFIG["confidence_threshold"]
    rate_limit = normalized.get("rate_limit_seconds")
    if not isinstance(rate_limit, (int, float)):
        normalized["rate_limit_seconds"] = DEFAULT_METADATA_CONFIG["rate_limit_seconds"]
    return normalized


def enqueue_metadata(file_path, meta, config):
    if not file_path:
        return False
    normalized = normalize_metadata_config(config)
    if not normalized.get("enabled"):
        return False
    item = {
        "file_path": file_path,
        "meta": meta or {},
        "config": normalized,
    }
    with _LOCK:
        global _WORKER
        if _WORKER is None or not _WORKER.is_alive():
            # TODO(metadata/queue.py::enqueue_metadata): migrate MetadataWorker lifecycle into the
            # unified engine download/metadata execution path (engine.job_queue postprocessing hook).
            _WORKER = MetadataWorker(_QUEUE)
            _WORKER.start()
            logging.info("Music metadata worker started")
    _QUEUE.put(item)
    return True
