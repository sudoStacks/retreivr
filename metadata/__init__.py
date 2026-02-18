try:
    from .queue import enqueue_metadata
except ModuleNotFoundError:  # pragma: no cover - optional deps may be absent in test env
    def enqueue_metadata(*_args, **_kwargs):
        raise RuntimeError("metadata queue dependencies are unavailable")

__all__ = ["enqueue_metadata"]
