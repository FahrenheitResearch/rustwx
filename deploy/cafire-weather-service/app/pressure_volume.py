from __future__ import annotations

import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import HTTPException

from .config import Settings


class PressureVolumeClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._base_url = (settings.pressure_volume_base_url or "").rstrip("/")

    def enabled(self) -> bool:
        return bool(self._settings.pressure_volume_enabled and self._base_url)

    def status(self) -> dict[str, Any]:
        if not self.enabled():
            return {
                "enabled": False,
                "status": "disabled",
                "base_url": self._base_url or None,
            }
        started = time.perf_counter()
        try:
            metadata = self._get_json("/api/metadata")
        except HTTPException as exc:
            return {
                "enabled": True,
                "status": "unavailable",
                "base_url": self._base_url,
                "detail": exc.detail,
            }
        return {
            "enabled": True,
            "status": "ready",
            "base_url": self._base_url,
            "metadata": metadata,
            "total_ms": int((time.perf_counter() - started) * 1000),
        }

    def profile(self, *, lat: float, lon: float) -> dict[str, Any]:
        if not self.enabled():
            raise HTTPException(status_code=503, detail="pressure volume sidecar is disabled")
        started = time.perf_counter()
        report = self._get_json("/api/point", {"lat": lat, "lon": lon})
        return {
            "source": "rustwx_pressure_volume_sidecar",
            "sidecar_url": self._base_url,
            "proxy_total_ms": int((time.perf_counter() - started) * 1000),
            **report,
        }

    def cross_section(
        self,
        *,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        hour: int,
        variable: str,
        spacing_km: float,
    ) -> dict[str, Any]:
        if not self.enabled():
            raise HTTPException(status_code=503, detail="pressure volume sidecar is disabled")
        started = time.perf_counter()
        report = self._get_json(
            "/api/cross-section",
            {
                "lat1": lat1,
                "lon1": lon1,
                "lat2": lat2,
                "lon2": lon2,
                "hour": hour,
                "variable": variable,
                "spacing_km": spacing_km,
            },
        )
        return {
            "source": "rustwx_pressure_volume_sidecar",
            "sidecar_url": self._base_url,
            "proxy_total_ms": int((time.perf_counter() - started) * 1000),
            **report,
        }

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = f"?{urlencode(params)}" if params else ""
        url = f"{self._base_url}{path}{query}"
        request = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=self._settings.pressure_volume_timeout_sec) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") or exc.reason
            raise HTTPException(status_code=502, detail=f"pressure volume sidecar HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise HTTPException(status_code=503, detail=f"pressure volume sidecar unavailable: {exc.reason}") from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail="pressure volume sidecar timed out") from exc
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=502, detail="pressure volume sidecar returned invalid JSON") from exc
