from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
import json
import os
from typing import Any

from .config import Settings
from .rustwx_client import latest_full_hrrr_run, sample_point_timeseries


class MeteogramWarmManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._state: dict[str, Any] = {
            "enabled": settings.meteogram_warm_enabled,
            "status": "disabled" if not settings.meteogram_warm_enabled else "idle",
            "run": None,
            "target_run": None,
            "started_at_utc": None,
            "finished_at_utc": None,
            "error": None,
            "chunks": [],
            "warm_point": {
                "lat": settings.meteogram_warm_lat,
                "lon": settings.meteogram_warm_lon,
            },
            "forecast_hours": settings.meteogram_warm_hours,
            "variables": settings.meteogram_warm_variables or "rustwx-default",
        }

    def start(self) -> None:
        if not self.settings.meteogram_warm_enabled:
            return
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._thread = threading.Thread(target=self._loop, name="meteogram-warm", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def status(self) -> dict[str, Any]:
        if not self.settings.meteogram_warm_in_api:
            status = self._read_status_file()
            if status is not None:
                return status
        with self._lock:
            return dict(self._state)

    def preferred_run(self) -> dict[str, Any] | None:
        status = self.status()
        run = status.get("run") or status.get("target_run")
        return dict(run) if isinstance(run, dict) else None

    def refresh_async(self) -> None:
        if not self.settings.meteogram_warm_enabled:
            return
        threading.Thread(target=self._warm_once, name="meteogram-warm-manual", daemon=True).start()

    def run_forever(self) -> None:
        self._loop()

    def _loop(self) -> None:
        self._warm_once()
        while not self._stop.wait(self.settings.meteogram_warm_interval_sec):
            try:
                target = latest_full_hrrr_run(self.settings)
                current = self.preferred_run()
                if not current or current.get("cycle") != target.get("cycle"):
                    self._warm_once(target)
            except Exception as exc:  # pragma: no cover - live network path
                self._set_error(str(exc))

    def _warm_once(self, target: dict[str, Any] | None = None) -> None:
        if target is None:
            target = latest_full_hrrr_run(self.settings)
        with self._lock:
            if self._state.get("status") == "warming":
                return
            self._state.update(
                {
                    "status": "warming",
                    "target_run": target,
                    "started_at_utc": now_iso(),
                    "finished_at_utc": None,
                    "error": None,
                    "chunks": [],
                }
            )
            self._write_status_locked()

        date = target["cycle"]["date_yyyymmdd"]
        cycle = int(target["cycle"]["hour_utc"])
        hours = list(self.settings.meteogram_warm_hours)
        chunk_size = self.settings.meteogram_warm_chunk_size
        variables = self.settings.meteogram_warm_variables or None
        chunks = [hours[index : index + chunk_size] for index in range(0, len(hours), chunk_size)]

        try:
            for chunk in chunks:
                start = time.perf_counter()
                report = sample_point_timeseries(
                    settings=self.settings,
                    lat=self.settings.meteogram_warm_lat,
                    lon=self.settings.meteogram_warm_lon,
                    date_yyyymmdd=date,
                    cycle_utc=cycle,
                    forecast_hours=chunk,
                    variables=variables,
                )
                chunk_state = {
                    "hours": chunk,
                    "ok": True,
                    "elapsed_ms": int((time.perf_counter() - start) * 1000),
                    "reported_ms": report.get("total_ms"),
                    "blocker_count": len(report.get("blockers", [])),
                    "fetch_count": len(report.get("fetches", [])),
                }
                with self._lock:
                    self._state["chunks"].append(chunk_state)
                    self._write_status_locked()
            with self._lock:
                self._state.update(
                    {
                        "status": "ready",
                        "run": target,
                        "finished_at_utc": now_iso(),
                        "error": None,
                    }
                )
                self._write_status_locked()
        except Exception as exc:  # pragma: no cover - live network path
            self._set_error(str(exc))

    def _set_error(self, message: str) -> None:
        with self._lock:
            has_previous_run = bool(self._state.get("run"))
            self._state.update(
                {
                    "status": "ready" if has_previous_run else "error",
                    "error": message,
                    "finished_at_utc": now_iso(),
                }
            )
            self._write_status_locked()

    def _read_status_file(self) -> dict[str, Any] | None:
        path = self.settings.meteogram_warm_status_path
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _write_status_locked(self) -> None:
        path = self.settings.meteogram_warm_status_path
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._state, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()
