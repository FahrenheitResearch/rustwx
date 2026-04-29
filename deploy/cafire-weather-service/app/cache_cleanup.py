from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Settings, settings


BYTES_PER_GB = 1024**3
SAFE_CACHE_DIR_NAMES = {"cache", "rustwx-cache", "rustwx_cache"}


@dataclass(frozen=True)
class CacheFile:
    path: Path
    size_bytes: int
    mtime: float


def _gb(value: int | float) -> float:
    return float(value) / BYTES_PER_GB


def _safe_child(root: Path, path: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _validated_cache_root(root: Path) -> Path:
    resolved = root.resolve()
    if resolved == resolved.parent:
        raise ValueError(f"refusing to clean filesystem root: {resolved}")
    if len(resolved.parts) < 2:
        raise ValueError(f"refusing to clean unsafe cache root: {resolved}")
    if resolved.name.lower() not in SAFE_CACHE_DIR_NAMES:
        raise ValueError(f"refusing to clean non-cache directory: {resolved}")
    return resolved


def _scan_files(root: Path) -> list[CacheFile]:
    files: list[CacheFile] = []
    if not root.exists():
        return files
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            path = Path(dirpath) / filename
            try:
                stat = path.stat()
            except OSError:
                continue
            if not path.is_file():
                continue
            files.append(CacheFile(path=path, size_bytes=stat.st_size, mtime=stat.st_mtime))
    return files


def _delete_files(root: Path, files: list[CacheFile], *, max_bytes: int | None = None) -> dict[str, Any]:
    deleted_files = 0
    deleted_bytes = 0
    errors = 0
    for item in files:
        if max_bytes is not None and deleted_bytes >= max_bytes:
            break
        if not _safe_child(root, item.path):
            errors += 1
            continue
        try:
            item.path.unlink()
            deleted_files += 1
            deleted_bytes += item.size_bytes
        except OSError:
            errors += 1
    return {
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "deleted_gb": round(_gb(deleted_bytes), 3),
        "errors": errors,
    }


def _prune_empty_dirs(root: Path) -> int:
    if not root.exists():
        return 0
    removed = 0
    for dirpath, _, _ in os.walk(root, topdown=False):
        if dirpath == str(root):
            continue
        path = Path(dirpath)
        if not _safe_child(root, path):
            continue
        try:
            if any(path.iterdir()):
                continue
        except OSError:
            continue
        try:
            path.rmdir()
            removed += 1
        except OSError:
            continue
    return removed


def run_cache_cleanup(config: Settings = settings, *, dry_run: bool = False) -> dict[str, Any]:
    root = _validated_cache_root(config.rustwx_cache_dir)
    started = time.perf_counter()
    now = time.time()
    root.mkdir(parents=True, exist_ok=True)

    files = _scan_files(root)
    initial_cache_bytes = sum(item.size_bytes for item in files)
    initial_disk = shutil.disk_usage(root)
    max_age_cutoff = now - config.cache_cleanup_max_age_hours * 3600
    emergency_cutoff = now - config.cache_cleanup_emergency_min_age_hours * 3600

    aged_files = [item for item in files if item.mtime < max_age_cutoff]
    aged_files.sort(key=lambda item: item.mtime)
    aged_paths = {item.path for item in aged_files}
    aged_bytes = sum(item.size_bytes for item in aged_files)
    if dry_run:
        age_delete = {
            "deleted_files": len(aged_files),
            "deleted_bytes": aged_bytes,
            "deleted_gb": round(_gb(aged_bytes), 3),
            "errors": 0,
            "dry_run": True,
        }
    else:
        age_delete = _delete_files(root, aged_files)

    files_after_age = [item for item in files if item.path not in aged_paths] if dry_run else _scan_files(root)
    cache_bytes_after_age = sum(item.size_bytes for item in files_after_age)
    disk_after_age = initial_disk if dry_run else shutil.disk_usage(root)
    disk_free_after_age = initial_disk.free + age_delete["deleted_bytes"] if dry_run else disk_after_age.free

    max_cache_bytes = int(config.cache_cleanup_max_cache_gb * BYTES_PER_GB)
    target_cache_bytes = int(
        min(config.cache_cleanup_target_cache_gb, config.cache_cleanup_max_cache_gb) * BYTES_PER_GB
    )
    min_free_bytes = int(config.cache_cleanup_min_free_gb * BYTES_PER_GB)
    target_free_bytes = int(config.cache_cleanup_target_free_gb * BYTES_PER_GB)
    excess_cache_bytes = (
        max(0, cache_bytes_after_age - target_cache_bytes)
        if cache_bytes_after_age > max_cache_bytes
        else 0
    )
    free_deficit_bytes = (
        max(0, target_free_bytes - disk_free_after_age)
        if disk_free_after_age < min_free_bytes
        else 0
    )
    target_delete_bytes = max(excess_cache_bytes, free_deficit_bytes)

    emergency_delete: dict[str, Any]
    selected_emergency_paths: set[Path] = set()
    if target_delete_bytes > 0:
        emergency_files = [item for item in files_after_age if item.mtime < emergency_cutoff]
        emergency_files.sort(key=lambda item: item.mtime)
        if dry_run:
            selected: list[CacheFile] = []
            selected_bytes = 0
            for item in emergency_files:
                selected.append(item)
                selected_bytes += item.size_bytes
                if selected_bytes >= target_delete_bytes:
                    break
            selected_emergency_paths = {item.path for item in selected}
            emergency_delete = {
                "deleted_files": len(selected),
                "deleted_bytes": selected_bytes,
                "deleted_gb": round(_gb(selected_bytes), 3),
                "errors": 0,
                "dry_run": True,
            }
        else:
            emergency_delete = _delete_files(root, emergency_files, max_bytes=target_delete_bytes)
    else:
        emergency_delete = {"deleted_files": 0, "deleted_bytes": 0, "deleted_gb": 0.0, "errors": 0}

    removed_dirs = 0 if dry_run else _prune_empty_dirs(root)
    final_files = (
        [item for item in files_after_age if item.path not in selected_emergency_paths]
        if dry_run
        else _scan_files(root)
    )
    final_cache_bytes = sum(item.size_bytes for item in final_files)
    final_disk = initial_disk if dry_run else shutil.disk_usage(root)
    final_disk_free = (
        initial_disk.free + age_delete["deleted_bytes"] + emergency_delete["deleted_bytes"]
        if dry_run
        else final_disk.free
    )
    final_disk_used = (
        max(0, initial_disk.used - age_delete["deleted_bytes"] - emergency_delete["deleted_bytes"])
        if dry_run
        else final_disk.used
    )

    return {
        "ok": True,
        "dry_run": dry_run,
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "cache_root": str(root),
        "policy": {
            "max_age_hours": config.cache_cleanup_max_age_hours,
            "max_cache_gb": config.cache_cleanup_max_cache_gb,
            "target_cache_gb": min(config.cache_cleanup_target_cache_gb, config.cache_cleanup_max_cache_gb),
            "min_free_gb": config.cache_cleanup_min_free_gb,
            "target_free_gb": config.cache_cleanup_target_free_gb,
            "emergency_min_age_hours": config.cache_cleanup_emergency_min_age_hours,
        },
        "initial": {
            "cache_files": len(files),
            "cache_gb": round(_gb(initial_cache_bytes), 3),
            "disk_free_gb": round(_gb(initial_disk.free), 3),
            "disk_used_gb": round(_gb(initial_disk.used), 3),
        },
        "age_delete": age_delete,
        "emergency_delete": emergency_delete,
        "removed_empty_dirs": removed_dirs,
        "final": {
            "cache_files": len(final_files),
            "cache_gb": round(_gb(final_cache_bytes), 3),
            "disk_free_gb": round(_gb(final_disk_free), 3),
            "disk_used_gb": round(_gb(final_disk_used), 3),
        },
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Prune the local rustwx cache by age and disk headroom")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run_cache_cleanup(settings, dry_run=args.dry_run), indent=2))


if __name__ == "__main__":
    main()
