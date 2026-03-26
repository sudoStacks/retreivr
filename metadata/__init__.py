try:
    from .queue import enqueue_metadata, process_metadata_now
except Exception:  # pragma: no cover - optional deps may be absent or partially stubbed in test env
    def enqueue_metadata(*_args, **_kwargs):
        raise RuntimeError("metadata queue dependencies are unavailable")

    def process_metadata_now(*_args, **_kwargs):
        raise RuntimeError("metadata queue dependencies are unavailable")

__all__ = ["enqueue_metadata", "process_metadata_now"]
