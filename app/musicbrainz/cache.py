import json
import os
import threading
import time
from pathlib import Path
from typing import Any


class MusicBrainzCache:
    def __init__(self, cache_path: str | None = None) -> None:
        path = cache_path or os.getenv("MUSICBRAINZ_CACHE_PATH") or ".cache/musicbrainz_cache.json"
        self._path = Path(path)
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, Any]] = {}
        self._loaded = False

    def _load_locked(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            if self._path.exists():
                payload = json.loads(self._path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    self._data = payload
        except Exception:
            self._data = {}

    def _persist_locked(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        tmp_path.write_text(json.dumps(self._data, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
        tmp_path.replace(self._path)

    def get(self, key: str) -> Any:
        now = time.time()
        with self._lock:
            self._load_locked()
            row = self._data.get(key)
            if not isinstance(row, dict):
                return None
            expires_at = float(row.get("expires_at") or 0.0)
            if expires_at <= now:
                self._data.pop(key, None)
                try:
                    self._persist_locked()
                except Exception:
                    pass
                return None
            return row.get("value")

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        now = time.time()
        with self._lock:
            self._load_locked()
            self._data[key] = {
                "expires_at": now + max(1, int(ttl_seconds)),
                "value": value,
            }
            self._persist_locked()
