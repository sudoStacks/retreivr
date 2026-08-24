#!/usr/bin/env python3
"""Move Retreivr Apple Music handoff files into Music's auto-import folder."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path


SOURCE = Path("/Volumes/Media/Music/_AppleMusic")
LOCAL_LIBRARY = Path("/Users/logan/Music/Music/Music")
# This is the auto-add folder Apple Music created for the active local library.
# Do not nest this under Media.localized unless Apple Music recreates the
# library that way; the current macOS Music layout keeps it beside the media
# folder.
AUTO_ADD = Path("/Users/logan/Music/Music/Automatically Add to Music.localized")
LEGACY_AUTO_ADD = Path("/Users/logan/Music/Music/Media.localized/Automatically Add to Music.localized")
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
        stat_result = path.stat()
    except OSError:
        return False
    return time.time() - stat_result.st_mtime >= MIN_AGE_SECONDS


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


def build_existing_hashes() -> set[str]:
    hashes: set[str] = set()
    for root, label in ((LOCAL_LIBRARY, "local_library"), (AUTO_ADD, "auto_add")):
        for path in audio_files(root):
            try:
                hashes.add(file_hash(path))
            except OSError as exc:
                log(f"{label}_hash_failed path={json.dumps(str(path))} error={json.dumps(str(exc))}")
    return hashes


def move_duplicate(path: Path, reason: str) -> None:
    date_dir = QUARANTINE / datetime.now().strftime("%Y%m%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    target = unique_path(date_dir, path.name)
    shutil.move(str(path), str(target))
    log(f"duplicate_skipped reason={reason} src={json.dumps(str(path))} dst={json.dumps(str(target))}")


def import_file(path: Path, remove_source: bool = True) -> None:
    AUTO_ADD.mkdir(parents=True, exist_ok=True)
    target = unique_path(AUTO_ADD, path.name)
    temp = unique_path(AUTO_ADD, f".retreivr-import-{os.getpid()}-{path.name}.tmp")
    try:
        with path.open("rb") as src, temp.open("xb") as dst:
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                dst.write(chunk)
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(str(temp), str(target))
        if remove_source:
            path.unlink()
    except Exception:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass
        raise
    log(f"moved_for_import src={json.dumps(str(path))} dst={json.dumps(str(target))}")


def clean_legacy_auto_add(existing_hashes: set[str]) -> tuple[int, int, int]:
    if not LEGACY_AUTO_ADD.exists() or LEGACY_AUTO_ADD == AUTO_ADD:
        return 0, 0, 0

    moved = skipped = failed = 0
    for path in sorted(audio_files(LEGACY_AUTO_ADD)):
        if not is_stable(path):
            continue
        try:
            digest = file_hash(path)
            if digest in existing_hashes:
                move_duplicate(path, "already_in_local_or_auto_add_from_legacy_auto_add")
                skipped += 1
            else:
                import_file(path)
                existing_hashes.add(digest)
                moved += 1
        except Exception as exc:
            failed += 1
            log(f"legacy_auto_add_failed path={json.dumps(str(path))} error={json.dumps(str(exc))}")

    try:
        if not any(LEGACY_AUTO_ADD.iterdir()):
            LEGACY_AUTO_ADD.rmdir()
            log(f"legacy_auto_add_removed path={json.dumps(str(LEGACY_AUTO_ADD))}")
    except OSError as exc:
        log(f"legacy_auto_add_cleanup_skipped path={json.dumps(str(LEGACY_AUTO_ADD))} error={json.dumps(str(exc))}")

    return moved, skipped, failed


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

    existing_hashes = build_existing_hashes()
    legacy_moved, legacy_skipped, legacy_failed = clean_legacy_auto_add(existing_hashes)
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
            if digest in existing_hashes:
                move_duplicate(path, "already_in_local_or_auto_add")
                skipped += 1
            elif digest in seen_this_run:
                move_duplicate(path, "duplicate_in_handoff_queue")
                skipped += 1
            else:
                import_file(path)
                seen_this_run.add(digest)
                existing_hashes.add(digest)
                moved += 1
        except Exception as exc:
            failed += 1
            log(f"file_failed path={json.dumps(str(path))} error={json.dumps(str(exc))}")
    total_failed = failed + legacy_failed
    log(
        "run_complete "
        f"moved={moved} skipped={skipped} pending={pending} failed={failed} "
        f"legacy_moved={legacy_moved} legacy_skipped={legacy_skipped} legacy_failed={legacy_failed}"
    )
    return 1 if total_failed else 0


if __name__ == "__main__":
    lock_fd = acquire_lock()
    try:
        raise SystemExit(main())
    finally:
        release_lock(lock_fd)
