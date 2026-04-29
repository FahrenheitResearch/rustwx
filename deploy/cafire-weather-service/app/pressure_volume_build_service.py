from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Settings, settings
from .rustwx_client import available_forecast_hours, latest_full_hrrr_run, latest_run


SYNOPTIC_CYCLES = {0, 6, 12, 18}
STORE_NAME_PREFIX = "hrrr_ca_"
STORE_NAME_SUFFIX = "_wxsection"
STALE_BUILD_SECONDS = 12 * 3600


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
        tmp_name = handle.name
    os.replace(tmp_name, path)


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def store_name(date_yyyymmdd: str, cycle_utc: int) -> str:
    return f"{STORE_NAME_PREFIX}{date_yyyymmdd}_{cycle_utc:02d}z{STORE_NAME_SUFFIX}"


def store_is_ready(path: Path, *, expected_hours: int | None = None) -> bool:
    store = path / "store" if (path / "store").is_dir() else path
    required = ["manifest.json", "index.bin", "chunks.bin", "build_stats.json"]
    if not all((store / name).exists() for name in required):
        return False
    if expected_hours is None:
        return True
    manifest = read_json(store / "manifest.json")
    if not manifest:
        return False
    hours = manifest.get("forecast_hours") or manifest.get("time_values") or []
    return isinstance(hours, list) and len(hours) >= expected_hours


class PressureVolumeBuilder:
    def __init__(self, config: Settings):
        self.settings = config
        self.store_root = config.pressure_volume_builder_store_root
        self.status_path = config.pressure_volume_builder_status_path
        self.lock_path = self.store_root / "builder.lock"

    def run_forever(self) -> None:
        if not self.settings.pressure_volume_builder_enabled:
            self._write_status("disabled", {"enabled": False})
            print("pressure VolumeStore builder disabled", flush=True)
            return
        while True:
            try:
                result = self.run_once()
                print(json.dumps(result, indent=2), flush=True)
            except Exception as exc:  # pragma: no cover - operational worker
                self._write_status("error", {"error": str(exc)})
                print(json.dumps({"ok": False, "error": str(exc), "at_utc": now_iso()}), flush=True)
            time.sleep(self.settings.pressure_volume_builder_interval_sec)

    def run_once(self) -> dict[str, Any]:
        if not self.settings.pressure_volume_builder_enabled:
            return self._write_status("disabled", {"enabled": False})

        self.store_root.mkdir(parents=True, exist_ok=True)
        partial_result = self._run_partial_lane()
        target = latest_full_hrrr_run(self.settings)
        date = str(target["cycle"]["date_yyyymmdd"])
        cycle = int(target["cycle"]["hour_utc"])
        start_hour = self.settings.pressure_volume_builder_start_hour
        end_hour = self.settings.pressure_volume_builder_end_hour
        expected_count = end_hour - start_hour + 1
        name = store_name(date, cycle)
        final_dir = self.store_root / name
        build_dir = self.store_root / f"building_{name}"

        base = {
            "enabled": True,
            "target_run": target,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "start_hour": start_hour,
            "end_hour": end_hour,
            "expected_forecast_hours": expected_count,
            "store_root": str(self.store_root),
            "current_path": str(self.settings.pressure_volume_store_path),
            "shared_cache_dir": str(self.settings.rustwx_cache_dir),
            "builder_path": str(self.settings.pressure_volume_builder_path),
            "load_parallelism": self.settings.pressure_volume_builder_load_parallelism,
            "partial": partial_result,
        }

        if cycle not in SYNOPTIC_CYCLES:
            return self._write_status("waiting", {**base, "reason": "target run is not a synoptic cycle"})

        pressure_ready = self._pressure_data_ready(date, cycle, end_hour)
        if not pressure_ready["ready"]:
            return self._write_status("waiting_pressure_data", {**base, "pressure_availability": pressure_ready})

        external_build = self._active_build_status(build_dir)
        static_status = self._static_manifest_status(date, cycle, start_hour, end_hour)
        if external_build:
            return self._write_status("building_external", {**base, **external_build, "static_manifest": static_status})

        if store_is_ready(final_dir, expected_hours=expected_count):
            publish = self._publish_current(final_dir / "store")
            cleanup = self._cleanup_old_stores()
            return self._write_status(
                "ready",
                {
                    **base,
                    "store_dir": str(final_dir / "store"),
                    "static_manifest": static_status,
                    "publish": publish,
                    "cleanup": cleanup,
                    "skipped": True,
                    "reason": "store already built",
                },
            )

        if self.settings.pressure_volume_builder_require_static_manifest and not static_status["ready"]:
            return self._write_status(
                "waiting_static_manifest",
                {
                    **base,
                    "reason": "waiting for static-map worker manifest before pressure store build",
                    "static_manifest": static_status,
                },
            )

        if not self.settings.pressure_volume_builder_path.exists():
            return self._write_status(
                "error",
                {**base, "error": f"missing builder binary: {self.settings.pressure_volume_builder_path}"},
            )

        lock = self._try_lock()
        if not lock:
            return self._write_status("locked", {**base, "reason": "another pressure store build is active"})

        started = time.perf_counter()
        log_path = self.store_root / f"{name}.build.log"
        try:
            if build_dir.exists():
                shutil.rmtree(build_dir)
            self._write_status(
                "building",
                {
                    **base,
                    "build_dir": str(build_dir),
                    "log_path": str(log_path),
                    "static_manifest": static_status,
                    "started_at_utc": now_iso(),
                },
            )
            completed = self._run_builder(date, cycle, start_hour, end_hour, build_dir, log_path)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if completed.returncode != 0:
                failed_dir = self._move_failed_build(build_dir, name)
                return self._write_status(
                    "error",
                    {
                        **base,
                        "returncode": completed.returncode,
                        "elapsed_ms": elapsed_ms,
                        "log_path": str(log_path),
                        "failed_dir": str(failed_dir) if failed_dir else None,
                        "error": "pressure VolumeStore builder exited non-zero",
                    },
                )

            validation = self._validate_build(build_dir, expected_count)
            if not validation["ok"]:
                failed_dir = self._move_failed_build(build_dir, name)
                return self._write_status(
                    "error",
                    {
                        **base,
                        "elapsed_ms": elapsed_ms,
                        "log_path": str(log_path),
                        "failed_dir": str(failed_dir) if failed_dir else None,
                        "validation": validation,
                        "error": "pressure VolumeStore validation failed",
                    },
                )

            if final_dir.exists():
                shutil.rmtree(final_dir)
            os.replace(build_dir, final_dir)
            publish = self._publish_current(final_dir / "store")
            cleanup = self._cleanup_old_stores()
            return self._write_status(
                "ready",
                {
                    **base,
                    "store_dir": str(final_dir / "store"),
                    "report_path": str(final_dir / "report.json"),
                    "log_path": str(log_path),
                    "elapsed_ms": elapsed_ms,
                    "validation": validation,
                    "publish": publish,
                    "cleanup": cleanup,
                },
            )
        finally:
            self._unlock(lock)

    def _pressure_data_ready(self, date: str, cycle: int, end_hour: int) -> dict[str, Any]:
        try:
            hours = available_forecast_hours(self.settings, date, cycle, product="prs", source=self.settings.default_source)
        except Exception as exc:
            return {"ready": False, "error": str(exc), "product": "prs"}
        return {
            "ready": end_hour in hours,
            "product": "prs",
            "available_max": max(hours) if hours else None,
            "available_count": len(hours),
        }

    def _run_partial_lane(self) -> dict[str, Any]:
        if not self.settings.pressure_volume_partial_enabled:
            return {"enabled": False, "status": "disabled"}
        active_build = self._any_active_build_status()
        if active_build:
            return {"enabled": True, "status": "skipped_active_build", **active_build}
        try:
            target = latest_run(self.settings, self.settings.default_model, self.settings.default_source)
        except Exception as exc:
            return {"enabled": True, "status": "error", "error": str(exc)}

        date = str(target["cycle"]["date_yyyymmdd"])
        cycle = int(target["cycle"]["hour_utc"])
        try:
            available = available_forecast_hours(
                self.settings,
                date,
                cycle,
                product="prs",
                source=self.settings.default_source,
            )
        except Exception as exc:
            return {"enabled": True, "status": "waiting_pressure_data", "target_run": target, "error": str(exc)}
        max_available = min(max(available) if available else -1, self.settings.pressure_volume_partial_max_hour)
        contiguous_end = -1
        available_set = set(available)
        for hour in range(0, max_available + 1):
            if hour not in available_set:
                break
            contiguous_end = hour
        if contiguous_end < 0:
            return {
                "enabled": True,
                "status": "waiting_pressure_data",
                "target_run": target,
                "available_count": len(available),
            }

        static_status = self._static_manifest_status(date, cycle, 0, contiguous_end)
        if self.settings.pressure_volume_partial_require_static_manifest and not static_status["ready"]:
            return {
                "enabled": True,
                "status": "waiting_static_manifest",
                "target_run": target,
                "end_hour": contiguous_end,
                "static_manifest": static_status,
            }

        name = f"{store_name(date, cycle)}_f000_f{contiguous_end:03d}_partial"
        final_dir = self.store_root / name
        build_dir = self.store_root / f"building_{name}"
        expected_count = contiguous_end + 1
        if store_is_ready(final_dir, expected_hours=expected_count):
            publish = self._publish_link(self.settings.pressure_volume_partial_store_path, final_dir / "store")
            return {
                "enabled": True,
                "status": "ready",
                "target_run": target,
                "store_dir": str(final_dir / "store"),
                "end_hour": contiguous_end,
                "forecast_hour_count": expected_count,
                "publish": publish,
                "skipped": True,
            }
        if build_dir.exists() and not store_is_ready(build_dir, expected_hours=expected_count):
            return {
                "enabled": True,
                "status": "building_external",
                "target_run": target,
                "build_dir": str(build_dir),
                "end_hour": contiguous_end,
            }
        if not self.settings.pressure_volume_builder_path.exists():
            return {
                "enabled": True,
                "status": "error",
                "target_run": target,
                "error": f"missing builder binary: {self.settings.pressure_volume_builder_path}",
            }

        lock = self._try_lock()
        if not lock:
            return {"enabled": True, "status": "locked", "target_run": target}
        started = time.perf_counter()
        log_path = self.store_root / f"{name}.build.log"
        try:
            if build_dir.exists():
                shutil.rmtree(build_dir)
            completed = self._run_builder(date, cycle, 0, contiguous_end, build_dir, log_path)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if completed.returncode != 0:
                failed_dir = self._move_failed_build(build_dir, name)
                return {
                    "enabled": True,
                    "status": "error",
                    "target_run": target,
                    "returncode": completed.returncode,
                    "elapsed_ms": elapsed_ms,
                    "log_path": str(log_path),
                    "failed_dir": str(failed_dir) if failed_dir else None,
                }
            validation = self._validate_build(build_dir, expected_count)
            if not validation["ok"]:
                failed_dir = self._move_failed_build(build_dir, name)
                return {
                    "enabled": True,
                    "status": "error",
                    "target_run": target,
                    "elapsed_ms": elapsed_ms,
                    "validation": validation,
                    "failed_dir": str(failed_dir) if failed_dir else None,
                }
            if final_dir.exists():
                shutil.rmtree(final_dir)
            os.replace(build_dir, final_dir)
            publish = self._publish_link(self.settings.pressure_volume_partial_store_path, final_dir / "store")
            return {
                "enabled": True,
                "status": "ready",
                "target_run": target,
                "store_dir": str(final_dir / "store"),
                "end_hour": contiguous_end,
                "forecast_hour_count": expected_count,
                "elapsed_ms": elapsed_ms,
                "validation": validation,
                "publish": publish,
            }
        finally:
            self._unlock(lock)

    def _any_active_build_status(self) -> dict[str, Any] | None:
        for path in self.store_root.glob("building_*"):
            if not path.is_dir() or store_is_ready(path):
                continue
            age_sec = int(time.time() - path.stat().st_mtime)
            if age_sec <= STALE_BUILD_SECONDS:
                return {
                    "reason": "another pressure VolumeStore build directory is active",
                    "build_dir": str(path),
                    "build_age_sec": age_sec,
                }
        return None

    def _static_manifest_status(self, date: str, cycle: int, start_hour: int, end_hour: int) -> dict[str, Any]:
        path = self.settings.artifact_root / "hrrr" / "runs" / date / f"{cycle:02d}Z" / "manifest.json"
        manifest = read_json(path)
        expected = set(range(start_hour, end_hour + 1))
        if not manifest:
            return {"ready": False, "path": str(path), "present": False, "missing_count": len(expected)}
        rendered = {int(hour) for hour in manifest.get("forecast_hours", [])}
        missing = sorted(expected.difference(rendered))
        return {
            "ready": not missing,
            "path": str(path),
            "present": True,
            "rendered_count": len(rendered),
            "missing_count": len(missing),
            "missing_preview": missing[:12],
        }

    def _active_build_status(self, build_dir: Path) -> dict[str, Any] | None:
        if not build_dir.exists():
            return None
        age_sec = time.time() - build_dir.stat().st_mtime
        if store_is_ready(build_dir):
            return None
        if age_sec > STALE_BUILD_SECONDS:
            return {
                "reason": "stale build directory blocks automatic rebuild",
                "build_dir": str(build_dir),
                "build_age_sec": int(age_sec),
            }
        return {
            "reason": "build directory already exists; assuming an external/manual build is active",
            "build_dir": str(build_dir),
            "build_age_sec": int(age_sec),
        }

    def _run_builder(
        self,
        date: str,
        cycle: int,
        start_hour: int,
        end_hour: int,
        build_dir: Path,
        log_path: Path,
    ) -> subprocess.CompletedProcess[bytes]:
        west, east, south, north = self.settings.pressure_volume_builder_bounds
        cmd = [
            str(self.settings.pressure_volume_builder_path),
            "--date",
            date,
            "--cycle",
            str(cycle),
            "--start-hour",
            str(start_hour),
            "--end-hour",
            str(end_hour),
            "--source",
            self.settings.default_source,
            f"--west={west}",
            f"--east={east}",
            f"--south={south}",
            f"--north={north}",
            "--cache-dir",
            str(self.settings.rustwx_cache_dir),
            "--out-dir",
            str(build_dir),
            "--load-parallelism",
            str(self.settings.pressure_volume_builder_load_parallelism),
        ]
        env = os.environ.copy()
        env["RUSTWX_VOLUME_STORE_LOAD_PARALLELISM"] = str(self.settings.pressure_volume_builder_load_parallelism)
        with log_path.open("ab") as log:
            log.write((f"\n{now_iso()} starting {' '.join(cmd)}\n").encode("utf-8"))
            return subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, env=env, check=False)

    def _validate_build(self, build_dir: Path, expected_count: int) -> dict[str, Any]:
        report_path = build_dir / "report.json"
        report = read_json(report_path)
        store_dir = build_dir / "store"
        ok = store_is_ready(store_dir, expected_hours=expected_count)
        hours = []
        if report:
            hours = report.get("request", {}).get("forecast_hours", [])
            ok = ok and isinstance(hours, list) and len(hours) >= expected_count
        return {
            "ok": ok,
            "report_path": str(report_path),
            "store_dir": str(store_dir),
            "report_present": report is not None,
            "forecast_hour_count": len(hours) if isinstance(hours, list) else 0,
        }

    def _publish_current(self, store_dir: Path) -> dict[str, Any]:
        return self._publish_link(self.settings.pressure_volume_store_path, store_dir)

    def _publish_link(self, link_path: Path, store_dir: Path) -> dict[str, Any]:
        link_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_link = link_path.parent / f".{link_path.name}.tmp"
        if tmp_link.exists() or tmp_link.is_symlink():
            if tmp_link.is_dir() and not tmp_link.is_symlink():
                shutil.rmtree(tmp_link)
            else:
                tmp_link.unlink()
        os.symlink(store_dir, tmp_link, target_is_directory=True)
        os.replace(tmp_link, link_path)
        return {"link": str(link_path), "target": str(store_dir)}

    def _cleanup_old_stores(self) -> dict[str, Any]:
        stores = []
        for path in self.store_root.glob(f"{STORE_NAME_PREFIX}*{STORE_NAME_SUFFIX}"):
            if path.is_dir() and store_is_ready(path):
                stores.append(path)
        stores.sort(key=lambda item: item.name, reverse=True)
        keep = set(stores[: self.settings.pressure_volume_builder_keep_completed])
        removed = []
        for path in stores:
            if path in keep:
                continue
            shutil.rmtree(path)
            removed.append(str(path))
        return {"kept": [str(path) for path in stores if path in keep], "removed": removed}

    def _move_failed_build(self, build_dir: Path, name: str) -> Path | None:
        if not build_dir.exists():
            return None
        failed_dir = self.store_root / f"failed_{name}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        if failed_dir.exists():
            shutil.rmtree(failed_dir)
        os.replace(build_dir, failed_dir)
        return failed_dir

    def _try_lock(self) -> int | None:
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return None
        os.write(fd, json.dumps({"pid": os.getpid(), "started_at_utc": now_iso()}).encode("utf-8"))
        os.write(fd, b"\n")
        return fd

    def _unlock(self, fd: int | None) -> None:
        if fd is None:
            return
        os.close(fd)
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def _write_status(self, status: str, payload: dict[str, Any]) -> dict[str, Any]:
        result = {
            "ok": status not in {"error"},
            "enabled": self.settings.pressure_volume_builder_enabled,
            "status": status,
            "checked_at_utc": now_iso(),
            **payload,
        }
        atomic_write_json(self.status_path, result)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and publish the HRRR CA pressure VolumeStore")
    parser.add_argument("--once", action="store_true", help="Run one build check and exit")
    args = parser.parse_args()
    settings.ensure_dirs()
    builder = PressureVolumeBuilder(settings)
    if args.once:
        print(json.dumps(builder.run_once(), indent=2), flush=True)
        return
    builder.run_forever()


if __name__ == "__main__":
    main()
