from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from typing import Any

from .config import Settings
from .meteogram_plot import PLOT_VARIABLES
from .rustwx_client import latest_full_hrrr_run, sample_point_timeseries_store, warm_point_timeseries_store


class FastMeteogramStoreManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._state: dict[str, Any] = {
            "enabled": settings.fast_meteogram_store_enabled,
            "status": "disabled" if not settings.fast_meteogram_store_enabled else "idle",
            "run": None,
            "target_run": None,
            "store_id": None,
            "started_at_utc": None,
            "finished_at_utc": None,
            "error": None,
            "bounds": list(settings.fast_meteogram_store_bounds),
            "forecast_hours": settings.meteogram_warm_hours,
            "variables": PLOT_VARIABLES,
            "build_report": None,
        }

    def start(self) -> None:
        if not self.settings.fast_meteogram_store_enabled:
            return
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._thread = threading.Thread(target=self._delayed_loop, name="fast-meteogram-store", daemon=True)
            self._thread.start()

    def _delayed_loop(self) -> None:
        # Let FastAPI finish startup before any Rust-backed live NOAA probe can
        # hold the Python runtime during cache warming.
        time.sleep(1.0)
        self._loop()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def ready_store_id(self) -> str | None:
        state = self.status()
        if state.get("status") != "ready":
            return None
        store_id = state.get("store_id")
        return str(store_id) if store_id else None

    def preferred_run(self) -> dict[str, Any] | None:
        state = self.status()
        run = state.get("run")
        return dict(run) if isinstance(run, dict) else None

    def sample(self, *, lat: float, lon: float, forecast_hours: list[int], method: str = "nearest") -> dict[str, Any] | None:
        store_id = self.ready_store_id()
        if not store_id:
            return None
        return sample_point_timeseries_store(
            store_id=store_id,
            lat=lat,
            lon=lon,
            forecast_hours=forecast_hours,
            method=method,
        )

    def _loop(self) -> None:
        self._warm_once()
        while not self._stop.wait(self.settings.fast_meteogram_store_interval_sec):
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
                }
            )

        date = target["cycle"]["date_yyyymmdd"]
        cycle = int(target["cycle"]["hour_utc"])
        try:
            report = warm_point_timeseries_store(
                settings=self.settings,
                date_yyyymmdd=date,
                cycle_utc=cycle,
                forecast_hours=list(self.settings.meteogram_warm_hours),
                variables=PLOT_VARIABLES,
                bounds=self.settings.fast_meteogram_store_bounds,
            )
            with self._lock:
                self._state.update(
                    {
                        "status": "ready",
                        "run": target,
                        "store_id": report.get("store_id"),
                        "finished_at_utc": now_iso(),
                        "error": None,
                        "build_report": report,
                    }
                )
        except Exception as exc:  # pragma: no cover - live network path
            self._set_error(str(exc))

    def _set_error(self, message: str) -> None:
        with self._lock:
            has_previous_store = bool(self._state.get("run") and self._state.get("store_id"))
            self._state.update(
                {
                    "status": "ready" if has_previous_store else "error",
                    "error": message,
                    "finished_at_utc": now_iso(),
                }
            )


def now_iso() -> str:
    return datetime.now(UTC).isoformat()
