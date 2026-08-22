#!/usr/bin/env python3
"""Move Retreivr Apple Music handoff files into Music's auto-import folder."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path


SOURCE = Path("/Volumes/Media/Music/_AppleMusic")
LOCAL_LIBRARY = Path("/Users/logan/Music/Music/Media.localized/Music")
AUTO_ADD = Path("/Users/logan/Music/Music/Media.localized/Automatically Add to Music.localized")
QUARANTINE = Path("/Volumes/Media/Music/_AppleMusic_skipped_duplicates")
LOG = Path("/private/tmp/retreivr-apple-music-importer.log")
LOCK = Path("/tmp/retreivr-apple-music-importer.lock")
EXTENSIONS = {".mp3", ".m4a", ".flac", ".aac", ".wav", ".alac"}
MIN_AGE_SECONDS = 120
STABILITY_WAIT_SECONDS = 3


def log(message: str) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().isoformat(timespec="seconds")
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"{stamp} {message}\n")


def acquire_lock() -> int:
    try:
        fd = os.open(str(LOCK), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(0)
    os.write(fd, str(os.getpid()).encode("ascii"))
    return fd


def release_lock(fd: int) -> None:
    os.close(fd)
    try:
        LOCK.unlink()
    except FileNotFoundError:
        pass


def audio_files(root: Path):
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in EXTENSIONS:
            yield path


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_stable(path: Path) -> bool:
    try:
        before = path.stat()
    except OSError:
        return False
    if time.time() - before.st_mtime < MIN_AGE_SECONDS:
        return False
    time.sleep(STABILITY_WAIT_SECONDS)
    try:
        after = path.stat()
    except OSError:
        return False
    return before.st_size == after.st_size and before.st_mtime == after.st_mtime


def unique_path(directory: Path, name: str) -> Path:
    candidate = directory / name
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    index = 2
    while True:
        candidate = directory / f"{stem} [{index}]{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def build_local_hashes() -> set[str]:
    hashes: set[str] = set()
    for path in audio_files(LOCAL_LIBRARY):
        try:
            hashes.add(file_hash(path))
        except OSError as exc:
            log(f"local_hash_failed path={json.dumps(str(path))} error={json.dumps(str(exc))}")
    return hashes


def move_duplicate(path: Path, reason: str) -> None:
    date_dir = QUARANTINE / datetime.now().strftime("%Y%m%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    target = unique_path(date_dir, path.name)
    shutil.move(str(path), str(target))
    log(f"duplicate_skipped reason={reason} src={json.dumps(str(path))} dst={json.dumps(str(target))}")


def import_file(path: Path) -> None:
    AUTO_ADD.mkdir(parents=True, exist_ok=True)
    target = unique_path(AUTO_ADD, path.name)
    shutil.move(str(path), str(target))
    log(f"moved_for_import src={json.dumps(str(path))} dst={json.dumps(str(target))}")


def main() -> int:
    if not SOURCE.exists():
        log(f"source_missing path={json.dumps(str(SOURCE))}")
        return 0
    if not AUTO_ADD.exists():
        log(f"auto_add_missing path={json.dumps(str(AUTO_ADD))}")
        return 0
    if not LOCAL_LIBRARY.exists():
        log(f"local_library_missing path={json.dumps(str(LOCAL_LIBRARY))}")
        return 0

    local_hashes = build_local_hashes()
    seen_this_run: set[str] = set()
    moved = skipped = pending = failed = 0
    for path in sorted(SOURCE.iterdir()):
        if not path.is_file() or path.suffix.lower() not in EXTENSIONS:
            continue
        if not is_stable(path):
            pending += 1
            continue
        try:
            digest = file_hash(path)
            if digest in local_hashes:
                move_duplicate(path, "already_in_local_library")
                skipped += 1
            elif digest in seen_this_run:
                move_duplicate(path, "duplicate_in_handoff_queue")
                skipped += 1
            else:
                import_file(path)
                seen_this_run.add(digest)
                moved += 1
        except Exception as exc:
            failed += 1
            log(f"file_failed path={json.dumps(str(path))} error={json.dumps(str(exc))}")
    log(f"run_complete moved={moved} skipped={skipped} pending={pending} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    lock_fd = acquire_lock()
    try:
        raise SystemExit(main())
    finally:
        release_lock(lock_fd)
