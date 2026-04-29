from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import point_in_california_buffer, settings
from .cross_section_artifacts import CrossSectionArtifactService
from .fast_store import FastMeteogramStoreManager
from .metrics import metrics
from .meteogram_plot import PLOT_VARIABLES, cached_meteogram_png, render_meteogram_png
from .pressure_volume import PressureVolumeClient
from .rustwx_client import (
    capabilities,
    domains,
    latest_full_hrrr_run,
    latest_run,
    rustwx_version,
    sample_point_timeseries,
    sample_point_timeseries_chunked,
)
from .warm import MeteogramWarmManager

app = FastAPI(
    title="CA Fire Weather API",
    version="0.1.0",
    description="Pilot API backed by rustwx for California HRRR fire-weather data.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
warm_manager = MeteogramWarmManager(settings)
fast_store_manager = FastMeteogramStoreManager(settings)
pressure_volume_client = PressureVolumeClient(settings)
cross_section_artifacts = CrossSectionArtifactService(settings)
meteogram_logger = logging.getLogger("uvicorn.error")
request_logger = logging.getLogger("uvicorn.error")


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="invalid API key")


def require_public_site() -> None:
    if not settings.public_site_enabled:
        raise HTTPException(status_code=404, detail="public site is disabled")


class MeteogramRequest(BaseModel):
    lat: float = Field(ge=-90.0, le=90.0)
    lon: float = Field(ge=-180.0, le=180.0)
    date_yyyymmdd: str | None = None
    cycle_utc: int | None = Field(default=None, ge=0, le=23)
    source: str | None = None
    forecast_hour_start: int = Field(default=0, ge=0, le=60)
    forecast_hour_end: int = Field(default=48, ge=0, le=60)
    forecast_hours: list[int] | None = None
    variables: list[str] | None = None
    method: str = "nearest"
    label: str | None = None
    force: bool = False


class PressureProfileRequest(BaseModel):
    lat: float = Field(ge=-90.0, le=90.0)
    lon: float = Field(ge=-180.0, le=180.0)


class PressureCrossSectionRequest(BaseModel):
    lat1: float = Field(ge=-90.0, le=90.0)
    lon1: float = Field(ge=-180.0, le=180.0)
    lat2: float = Field(ge=-90.0, le=90.0)
    lon2: float = Field(ge=-180.0, le=180.0)
    hour: int = Field(default=0, ge=0, le=60)
    variable: str = Field(default="TMP", min_length=1, max_length=32)
    spacing_km: float = Field(default=20.0, ge=1.0, le=100.0)


class PressureCrossSectionRenderRequest(BaseModel):
    lat1: float = Field(ge=-90.0, le=90.0)
    lon1: float = Field(ge=-180.0, le=180.0)
    lat2: float = Field(ge=-90.0, le=90.0)
    lon2: float = Field(ge=-180.0, le=180.0)
    hour: int = Field(default=0, ge=0, le=48)
    products: str | list[str] | None = "wind_speed"
    spacing_km: float = Field(default=5.0, ge=0.5, le=100.0)
    top_pressure_hpa: float | None = Field(default=None, ge=10.0, le=1000.0)
    width: int | None = Field(default=None, ge=600, le=2600)
    height: int | None = Field(default=None, ge=420, le=1800)
    route_name: str | None = Field(default=None, max_length=96)
    force: bool = False


class PressureCrossSectionLoopRequest(BaseModel):
    lat1: float = Field(ge=-90.0, le=90.0)
    lon1: float = Field(ge=-180.0, le=180.0)
    lat2: float = Field(ge=-90.0, le=90.0)
    lon2: float = Field(ge=-180.0, le=180.0)
    product: str = Field(default="wind_speed", min_length=1, max_length=64)
    hours: str | list[int] | None = "all"
    spacing_km: float = Field(default=5.0, ge=0.5, le=100.0)
    top_pressure_hpa: float | None = Field(default=None, ge=10.0, le=1000.0)
    width: int | None = Field(default=None, ge=600, le=2600)
    height: int | None = Field(default=None, ge=420, le=1800)
    route_name: str | None = Field(default=None, max_length=96)
    force: bool = False


def _truncate(value: object, limit: int = 240) -> str:
    text = str(value)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _run_fields(report: dict[str, Any] | None) -> dict[str, Any]:
    run = (report or {}).get("run") or {}
    if not isinstance(run, dict):
        return {}
    return {
        "date_yyyymmdd": run.get("date_yyyymmdd"),
        "cycle_utc": run.get("cycle_utc"),
        "source": run.get("source"),
    }


def _route_key(request: Request) -> str:
    path = request.url.path
    route = request.scope.get("route")
    template = getattr(route, "path", None)
    if template and template not in {"/", "/{path:path}"}:
        return f"{request.method} {template}"
    if path == "/":
        return f"{request.method} /"
    if path.startswith("/api/"):
        return f"{request.method} {path}"
    if path.startswith("/artifacts/"):
        return f"{request.method} /artifacts/*"
    if "." in path.rsplit("/", 1)[-1]:
        return f"{request.method} /static/*"
    return f"{request.method} {path}"


def _observe_route(path: str) -> bool:
    return path not in {"/health", "/api/v1/metrics", "/favicon.ico"}


def _validate_ca_cross_section(lat1: float, lon1: float, lat2: float, lon2: float) -> None:
    if not point_in_california_buffer(lat1, lon1) or not point_in_california_buffer(lat2, lon2):
        raise HTTPException(status_code=400, detail="cross-section endpoints are outside the California pilot bounds")
    if abs(lat1 - lat2) < 1.0e-6 and abs(lon1 - lon2) < 1.0e-6:
        raise HTTPException(status_code=400, detail="cross-section endpoints must be different")


def _request_payload(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


@app.middleware("http")
async def record_request_metrics(request: Request, call_next: Any) -> Any:
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = int(response.status_code)
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        total_ms = int((time.perf_counter() - started) * 1000)
        path = request.url.path
        if _observe_route(path):
            route = _route_key(request)
            metrics.observe_request(
                method=request.method,
                path=path,
                route=route,
                status_code=status_code,
                total_ms=total_ms,
            )
            if status_code >= 500 or total_ms >= 1000:
                request_logger.info(
                    json.dumps(
                        {
                            "event": "request_performance",
                            "method": request.method,
                            "route": route,
                            "status_code": status_code,
                            "total_ms": total_ms,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )


def _log_meteogram_performance(
    *,
    endpoint: str,
    request: MeteogramRequest,
    forecast_hours: list[int] | None,
    started: float,
    status_code: int,
    ok: bool,
    sample_path: str | None = None,
    report: dict[str, Any] | None = None,
    error: object | None = None,
) -> None:
    hours = forecast_hours or []
    payload: dict[str, Any] = {
        "event": "meteogram_performance",
        "endpoint": endpoint,
        "ok": ok,
        "status_code": status_code,
        "total_ms": int((time.perf_counter() - started) * 1000),
        "sample_path": sample_path,
        "lat": round(request.lat, 3),
        "lon": round(request.lon, 3),
        "method": request.method,
        "force": request.force,
        "custom_variables": bool(request.variables),
        "variable_count": len(request.variables or []),
        "forecast_hour_count": len(hours),
        "forecast_hour_start": min(hours) if hours else None,
        "forecast_hour_end": max(hours) if hours else None,
    }
    if report:
        payload.update(_run_fields(report))
        payload.update(
            {
                "cache_hit": report.get("cache_hit"),
                "fast_store_hit": report.get("fast_store_hit"),
                "sample_total_ms": report.get("sample_total_ms", report.get("total_ms")),
                "render_total_ms": report.get("render_total_ms"),
                "returned_hour_count": len(report.get("hours") or report.get("forecast_hours") or []),
                "fetch_count": len(report.get("fetches") or []),
                "blocker_count": len(report.get("blockers") or []),
            }
        )
    if error is not None:
        payload["error"] = _truncate(error)
        payload["error_type"] = type(error).__name__
    metrics.observe_meteogram(payload)
    message = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    if ok:
        meteogram_logger.info(message)
    else:
        meteogram_logger.warning(message)


@app.on_event("startup")
def startup() -> None:
    settings.ensure_dirs()
    if settings.meteogram_warm_in_api:
        warm_manager.start()
    fast_store_manager.start()


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "cafire-weather-service",
        "rustwx_version": rustwx_version(),
        "cache_dir": str(settings.rustwx_cache_dir),
        "artifact_root": str(settings.artifact_root),
        "r2_enabled": settings.r2_enabled(),
        "satellite_enabled": settings.satellite_enabled,
        "meteogram_warm": warm_manager.status(),
        "fast_meteogram_store": fast_store_manager.status(),
        "pressure_volume": pressure_volume_client.status(),
        "pressure_volume_builder": _pressure_volume_builder_status(),
        "pressure_cross_sections": cross_section_artifacts.status(),
    }


@app.get("/api/v1/capabilities", dependencies=[Depends(require_api_key)])
def api_capabilities() -> dict[str, Any]:
    return capabilities()


@app.get("/api/v1/domains", dependencies=[Depends(require_api_key)])
def api_domains(
    kind: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    return domains(kind, limit)


@app.get("/api/v1/latest", dependencies=[Depends(require_api_key)])
def api_latest(source: str | None = None) -> dict[str, Any]:
    return latest_run(settings, settings.default_model, source)


@app.get("/api/v1/latest-full", dependencies=[Depends(require_api_key)])
def api_latest_full(source: str | None = None) -> dict[str, Any]:
    return latest_full_hrrr_run(settings, source)


@app.get("/api/v1/products", dependencies=[Depends(require_api_key)])
def api_products() -> dict[str, Any]:
    caps = capabilities()
    model = next(item for item in caps["models"] if item["id"] == settings.default_model)
    return {
        "model": settings.default_model,
        "default_products": settings.default_products,
        "direct": model.get("direct_recipes", []),
        "light_derived": model.get("light_derived_recipes", []),
        "heavy_derived": model.get("heavy_derived_recipes", []),
        "windowed": model.get("windowed_products", []),
    }


def _artifact_pointer_payload(pointer_name: str = "latest.json") -> dict[str, Any]:
    local_latest = settings.artifact_root / "hrrr" / pointer_name
    if local_latest.exists():
        latest = json.loads(local_latest.read_text(encoding="utf-8"))
        return _resolve_artifact_manifest(latest)
    if settings.public_artifact_base_url:
        url = f"{settings.public_artifact_base_url.rstrip('/')}/hrrr/{pointer_name}"
        with urlopen(url, timeout=20) as response:  # nosec - configured public artifact URL
            latest = json.loads(response.read().decode("utf-8"))
        return _resolve_artifact_manifest(latest)
    raise HTTPException(status_code=404, detail="latest artifact manifest is not available")


def _latest_artifacts_payload() -> dict[str, Any]:
    return _artifact_pointer_payload("latest.json")


def _resolve_artifact_manifest(latest: dict[str, Any]) -> dict[str, Any]:
    if isinstance(latest.get("hours"), list):
        return latest

    manifest_key = latest.get("manifest_key")
    if isinstance(manifest_key, str) and manifest_key:
        local_manifest = settings.artifact_root / manifest_key
        if local_manifest.exists():
            return json.loads(local_manifest.read_text(encoding="utf-8"))

    manifest_url = latest.get("manifest_url")
    if isinstance(manifest_url, str) and manifest_url:
        with urlopen(manifest_url, timeout=20) as response:  # nosec - configured public artifact URL
            return json.loads(response.read().decode("utf-8"))

    if manifest_key:
        raise HTTPException(status_code=404, detail=f"artifact run manifest is not available: {manifest_key}")
    raise HTTPException(status_code=404, detail="latest artifact pointer does not include a run manifest")


def _latest_lightning_manifest_payload() -> dict[str, Any]:
    local_latest = settings.artifact_root / "lightning" / "latest.json"
    if local_latest.exists():
        return json.loads(local_latest.read_text(encoding="utf-8"))
    if settings.public_artifact_base_url:
        url = f"{settings.public_artifact_base_url.rstrip('/')}/lightning/latest.json"
        with urlopen(url, timeout=20) as response:  # nosec - configured public artifact URL
            return json.loads(response.read().decode("utf-8"))
    raise HTTPException(status_code=404, detail="latest lightning artifact manifest is not available")


def _latest_satellite_manifest_payload() -> dict[str, Any]:
    local_latest = settings.artifact_root / "satellite" / "latest.json"
    if local_latest.exists():
        return json.loads(local_latest.read_text(encoding="utf-8"))
    raise HTTPException(status_code=404, detail="latest satellite artifact manifest is not available")


def _pressure_volume_builder_status() -> dict[str, Any]:
    path = settings.pressure_volume_builder_status_path
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            return {
                "enabled": settings.pressure_volume_builder_enabled,
                "status": "status_read_error",
                "status_path": str(path),
                "error": str(exc),
            }
    return {
        "enabled": settings.pressure_volume_builder_enabled,
        "status": "disabled" if not settings.pressure_volume_builder_enabled else "no_status",
        "status_path": str(path),
    }


def _latest_lightning_data_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    for hour in manifest.get("hours", []):
        for upload in hour.get("uploaded", []):
            key = upload.get("key")
            if not isinstance(key, str) or not key.endswith("/glm_flashes.json"):
                continue
            local_path = settings.artifact_root / key
            if local_path.exists():
                return json.loads(local_path.read_text(encoding="utf-8"))
            url = upload.get("url")
            if isinstance(url, str) and url:
                with urlopen(url, timeout=20) as response:  # nosec - configured public artifact URL
                    return json.loads(response.read().decode("utf-8"))
    raise HTTPException(status_code=404, detail="latest lightning flash data is not available")


def _latest_lightning_geojson_payload() -> dict[str, Any]:
    manifest = _latest_lightning_manifest_payload()
    data = _latest_lightning_data_payload(manifest)
    features = []
    for index, flash in enumerate(data.get("flashes", [])):
        lat = flash.get("lat")
        lon = flash.get("lon")
        if lat is None or lon is None:
            continue
        features.append(
            {
                "type": "Feature",
                "id": index,
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "time_utc": flash.get("time_utc"),
                    "energy_j": flash.get("energy_j"),
                    "area_m2": flash.get("area_m2"),
                    "source_file": flash.get("source_file"),
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "features": features,
        "generated_at_utc": manifest.get("generated_at_utc"),
        "time_window": manifest.get("time_window"),
        "satellite": manifest.get("satellite"),
        "source": manifest.get("source"),
        "domain": manifest.get("domain"),
        "domain_label": manifest.get("domain_label"),
        "latest_glm_key": manifest.get("latest_glm_key"),
        "latest_glm_last_modified": manifest.get("latest_glm_last_modified"),
        "flash_count_total": manifest.get("flash_count_total"),
        "flash_count_in_domain": manifest.get("flash_count_in_domain"),
        "flash_count_drawn": manifest.get("flash_count_drawn"),
        "n_files": manifest.get("n_files"),
    }


@app.get("/api/v1/public/latest-artifacts", dependencies=[Depends(require_public_site)])
def public_latest_artifacts() -> dict[str, Any]:
    return _latest_artifacts_payload()


@app.get("/api/v1/public/latest-diurnal-artifacts", dependencies=[Depends(require_public_site)])
def public_latest_diurnal_artifacts() -> dict[str, Any]:
    return _artifact_pointer_payload("latest-diurnal.json")


@app.get("/api/v1/public/latest-lightning-artifacts", dependencies=[Depends(require_public_site)])
def public_latest_lightning_artifacts() -> dict[str, Any]:
    return _latest_lightning_manifest_payload()


@app.get("/api/v1/public/latest-satellite-artifacts", dependencies=[Depends(require_public_site)])
def public_latest_satellite_artifacts() -> dict[str, Any]:
    return _latest_satellite_manifest_payload()


@app.get("/api/v1/public/latest-lightning.geojson", dependencies=[Depends(require_public_site)])
def public_latest_lightning_geojson() -> dict[str, Any]:
    return _latest_lightning_geojson_payload()


@app.get("/api/v1/public/products", dependencies=[Depends(require_public_site)])
def public_products() -> dict[str, Any]:
    return api_products()


@app.get("/api/v1/public/warm-status", dependencies=[Depends(require_public_site)])
def public_warm_status() -> dict[str, Any]:
    return {
        "fetch_decode_cache": warm_manager.status(),
        "fast_store": fast_store_manager.status(),
        "pressure_volume": pressure_volume_client.status(),
        "pressure_volume_builder": _pressure_volume_builder_status(),
        "pressure_cross_sections": cross_section_artifacts.status(),
    }


@app.get("/api/v1/public/pressure-volume/status", dependencies=[Depends(require_public_site)])
def public_pressure_volume_status() -> dict[str, Any]:
    return pressure_volume_client.status()


@app.get("/api/v1/public/pressure-volume-builder/status", dependencies=[Depends(require_public_site)])
def public_pressure_volume_builder_status() -> dict[str, Any]:
    return _pressure_volume_builder_status()


@app.get("/api/v1/public/cross-section-products", dependencies=[Depends(require_public_site)])
def public_cross_section_products() -> dict[str, Any]:
    return cross_section_artifacts.product_catalog()


@app.post("/api/v1/public/pressure-profile", dependencies=[Depends(require_public_site)])
def public_pressure_profile(request: PressureProfileRequest) -> dict[str, Any]:
    if not point_in_california_buffer(request.lat, request.lon):
        raise HTTPException(status_code=400, detail="point is outside the California pilot bounds")
    return pressure_volume_client.profile(lat=request.lat, lon=request.lon)


@app.post("/api/v1/public/cross-section", dependencies=[Depends(require_public_site)])
def public_pressure_cross_section(request: PressureCrossSectionRequest) -> dict[str, Any]:
    _validate_ca_cross_section(request.lat1, request.lon1, request.lat2, request.lon2)
    return pressure_volume_client.cross_section(
        lat1=request.lat1,
        lon1=request.lon1,
        lat2=request.lat2,
        lon2=request.lon2,
        hour=request.hour,
        variable=request.variable,
        spacing_km=request.spacing_km,
    )


@app.post("/api/v1/public/cross-section-render", dependencies=[Depends(require_public_site)])
def public_pressure_cross_section_render(request: PressureCrossSectionRenderRequest) -> dict[str, Any]:
    _validate_ca_cross_section(request.lat1, request.lon1, request.lat2, request.lon2)
    return cross_section_artifacts.render_still(_request_payload(request))


@app.post("/api/v1/public/cross-section-loop", dependencies=[Depends(require_public_site)])
def public_pressure_cross_section_loop(request: PressureCrossSectionLoopRequest) -> dict[str, Any]:
    _validate_ca_cross_section(request.lat1, request.lon1, request.lat2, request.lon2)
    return cross_section_artifacts.render_loop(_request_payload(request))


@app.post("/api/v1/warm/refresh", dependencies=[Depends(require_api_key)])
def api_refresh_warm() -> dict[str, Any]:
    warm_manager.refresh_async()
    return warm_manager.status()


@app.get("/api/v1/metrics", dependencies=[Depends(require_api_key)])
def api_metrics() -> dict[str, Any]:
    return metrics.snapshot()


def _forecast_hours_from_request(request: MeteogramRequest) -> list[int]:
    if request.forecast_hours:
        return sorted({hour for hour in request.forecast_hours if 0 <= hour <= 48})
    end = min(request.forecast_hour_end, 48)
    return list(range(max(request.forecast_hour_start, 0), end + 1))


def _preferred_public_run(request: MeteogramRequest) -> tuple[str | None, int | None]:
    date_yyyymmdd = request.date_yyyymmdd
    cycle_utc = request.cycle_utc
    fast_run = fast_store_manager.preferred_run()
    warm_run = fast_run or warm_manager.preferred_run()
    if warm_run and date_yyyymmdd is None and cycle_utc is None:
        date_yyyymmdd = warm_run["cycle"]["date_yyyymmdd"]
        cycle_utc = int(warm_run["cycle"]["hour_utc"])
    return date_yyyymmdd, cycle_utc


@app.post("/api/v1/public/meteogram", dependencies=[Depends(require_public_site)])
def public_meteogram(request: MeteogramRequest) -> dict[str, Any]:
    started = time.perf_counter()
    forecast_hours: list[int] = []
    sample_path: str | None = None
    report: dict[str, Any] | None = None
    try:
        if not point_in_california_buffer(request.lat, request.lon):
            raise HTTPException(status_code=400, detail="point is outside the California pilot bounds")
        forecast_hours = _forecast_hours_from_request(request)
        date_yyyymmdd, cycle_utc = _preferred_public_run(request)
        if request.date_yyyymmdd is None and request.cycle_utc is None and not request.variables:
            try:
                report = fast_store_manager.sample(
                    lat=request.lat,
                    lon=request.lon,
                    forecast_hours=forecast_hours,
                    method=request.method,
                )
                if report is not None:
                    report["fast_store_hit"] = True
                    sample_path = "fast_store"
                    _log_meteogram_performance(
                        endpoint="/api/v1/public/meteogram",
                        request=request,
                        forecast_hours=forecast_hours,
                        started=started,
                        status_code=200,
                        ok=True,
                        sample_path=sample_path,
                        report=report,
                    )
                    return report
            except Exception:
                report = None
        if date_yyyymmdd is None or cycle_utc is None:
            sample_path = "rustwx_latest"
            report = sample_point_timeseries(
                settings=settings,
                lat=request.lat,
                lon=request.lon,
                date_yyyymmdd=date_yyyymmdd,
                cycle_utc=cycle_utc,
                source=request.source,
                forecast_hours=forecast_hours,
                variables=request.variables,
                method=request.method,
            )
        else:
            sample_path = "rustwx_chunked"
            report = sample_point_timeseries_chunked(
                settings=settings,
                lat=request.lat,
                lon=request.lon,
                date_yyyymmdd=date_yyyymmdd,
                cycle_utc=cycle_utc,
                source=request.source,
                forecast_hours=forecast_hours,
                variables=request.variables,
                method=request.method,
            )
        _log_meteogram_performance(
            endpoint="/api/v1/public/meteogram",
            request=request,
            forecast_hours=forecast_hours,
            started=started,
            status_code=200,
            ok=True,
            sample_path=sample_path,
            report=report,
        )
        return report
    except HTTPException as exc:
        _log_meteogram_performance(
            endpoint="/api/v1/public/meteogram",
            request=request,
            forecast_hours=forecast_hours,
            started=started,
            status_code=exc.status_code,
            ok=False,
            sample_path=sample_path,
            report=report,
            error=exc.detail,
        )
        raise
    except Exception as exc:
        _log_meteogram_performance(
            endpoint="/api/v1/public/meteogram",
            request=request,
            forecast_hours=forecast_hours,
            started=started,
            status_code=500,
            ok=False,
            sample_path=sample_path,
            report=report,
            error=exc,
        )
        raise


@app.post("/api/v1/public/meteogram.png", dependencies=[Depends(require_public_site)])
def public_meteogram_png(request: MeteogramRequest) -> dict[str, Any]:
    started = time.perf_counter()
    forecast_hours: list[int] = []
    sample_path: str | None = None
    report: dict[str, Any] | None = None
    try:
        if not point_in_california_buffer(request.lat, request.lon):
            raise HTTPException(status_code=400, detail="point is outside the California pilot bounds")
        if request.forecast_hour_end < request.forecast_hour_start:
            raise HTTPException(status_code=400, detail="forecast_hour_end must be >= start")
        forecast_hours = _forecast_hours_from_request(request)
        if not forecast_hours:
            raise HTTPException(status_code=400, detail="no valid forecast hours requested")
        date_yyyymmdd, cycle_utc = _preferred_public_run(request)
        label = request.label or "Selected point"
        if date_yyyymmdd is not None and cycle_utc is not None and not request.force:
            cached = cached_meteogram_png(
                settings=settings,
                date_yyyymmdd=date_yyyymmdd,
                cycle_utc=cycle_utc,
                lat=request.lat,
                lon=request.lon,
                forecast_hours=forecast_hours,
                label=label,
            )
            if cached is not None:
                _log_meteogram_performance(
                    endpoint="/api/v1/public/meteogram.png",
                    request=request,
                    forecast_hours=forecast_hours,
                    started=started,
                    status_code=200,
                    ok=True,
                    sample_path="artifact_cache",
                    report=cached,
                )
                return cached
        variables = sorted(set(PLOT_VARIABLES + (request.variables or [])))
        if request.date_yyyymmdd is None and request.cycle_utc is None and not request.variables:
            try:
                report = fast_store_manager.sample(
                    lat=request.lat,
                    lon=request.lon,
                    forecast_hours=forecast_hours,
                    method=request.method,
                )
                if report is not None:
                    report["fast_store_hit"] = True
                    sample_path = "fast_store"
            except Exception:
                report = None
        if report is not None:
            pass
        elif date_yyyymmdd is None or cycle_utc is None:
            sample_path = "rustwx_latest"
            report = sample_point_timeseries(
                settings=settings,
                lat=request.lat,
                lon=request.lon,
                date_yyyymmdd=date_yyyymmdd,
                cycle_utc=cycle_utc,
                source=request.source,
                forecast_hours=forecast_hours,
                variables=variables,
                method=request.method,
            )
        else:
            sample_path = "rustwx_chunked"
            report = sample_point_timeseries_chunked(
                settings=settings,
                lat=request.lat,
                lon=request.lon,
                date_yyyymmdd=date_yyyymmdd,
                cycle_utc=cycle_utc,
                source=request.source,
                forecast_hours=forecast_hours,
                variables=variables,
                method=request.method,
            )
        if not report.get("hours"):
            raise HTTPException(status_code=502, detail="rustwx returned no meteogram hours")
        try:
            rendered = render_meteogram_png(
                settings=settings,
                report=report,
                label=label,
                force=request.force,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"meteogram PNG render failed: {exc}") from exc
        _log_meteogram_performance(
            endpoint="/api/v1/public/meteogram.png",
            request=request,
            forecast_hours=forecast_hours,
            started=started,
            status_code=200,
            ok=True,
            sample_path=sample_path,
            report={**report, **rendered},
        )
        return rendered
    except HTTPException as exc:
        _log_meteogram_performance(
            endpoint="/api/v1/public/meteogram.png",
            request=request,
            forecast_hours=forecast_hours,
            started=started,
            status_code=exc.status_code,
            ok=False,
            sample_path=sample_path,
            report=report,
            error=exc.detail,
        )
        raise
    except Exception as exc:
        _log_meteogram_performance(
            endpoint="/api/v1/public/meteogram.png",
            request=request,
            forecast_hours=forecast_hours,
            started=started,
            status_code=500,
            ok=False,
            sample_path=sample_path,
            report=report,
            error=exc,
        )
        raise


@app.post("/api/v1/meteogram", dependencies=[Depends(require_api_key)])
def api_meteogram(request: MeteogramRequest) -> dict[str, Any]:
    if not settings.allow_outside_california and not point_in_california_buffer(
        request.lat, request.lon
    ):
        raise HTTPException(status_code=400, detail="point is outside the California pilot bounds")
    if request.forecast_hour_end < request.forecast_hour_start:
        raise HTTPException(status_code=400, detail="forecast_hour_end must be >= start")
    return sample_point_timeseries(
        settings=settings,
        lat=request.lat,
        lon=request.lon,
        date_yyyymmdd=request.date_yyyymmdd,
        cycle_utc=request.cycle_utc,
        source=request.source,
        forecast_hour_start=request.forecast_hour_start,
        forecast_hour_end=request.forecast_hour_end,
        forecast_hours=request.forecast_hours,
        variables=request.variables,
        method=request.method,
    )


app.mount("/artifacts", StaticFiles(directory=settings.artifact_root, check_dir=False), name="artifacts")

STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
