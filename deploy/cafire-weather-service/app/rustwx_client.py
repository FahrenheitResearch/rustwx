from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import rustwx

from .config import Settings


def _json_call(payload: str) -> dict[str, Any]:
    return json.loads(payload)


def capabilities() -> dict[str, Any]:
    return _json_call(rustwx.agent_capabilities_json())


def domains(kind: str | None = None, limit: int | None = None) -> dict[str, Any]:
    return _json_call(rustwx.list_domains_json(kind, limit))


def available_forecast_hours(
    settings: Settings,
    date_yyyymmdd: str,
    cycle_utc: int,
    product: str = "sfc",
    source: str | None = None,
) -> list[int]:
    payload = _json_call(
        rustwx.available_forecast_hours_json(
            settings.default_model,
            date_yyyymmdd,
            cycle_utc,
            product,
            source or settings.default_source,
        )
    )
    if isinstance(payload, list):
        return sorted(int(hour) for hour in payload)
    return sorted(int(hour) for hour in payload.get("forecast_hours", []))


def latest_run(settings: Settings, model: str = "hrrr", source: str | None = None) -> dict[str, Any]:
    selected_source = source or settings.default_source
    errors: list[str] = []
    now = datetime.now(UTC)
    for day_offset in range(0, 3):
        date = (now - timedelta(days=day_offset)).strftime("%Y%m%d")
        try:
            return _json_call(rustwx.latest_run_json(model, date, selected_source))
        except Exception as exc:  # pragma: no cover - depends on live NOAA availability
            errors.append(f"{date}: {exc}")
    raise RuntimeError("unable to resolve latest HRRR run: " + "; ".join(errors))


def latest_full_hrrr_run(settings: Settings, source: str | None = None) -> dict[str, Any]:
    """Return the newest synoptic HRRR cycle with f048 actually available."""
    selected_source = source or settings.default_source
    now = datetime.now(UTC)
    errors: list[str] = []
    seen: set[tuple[str, int]] = set()
    for hour_offset in range(0, 96):
        cycle_time = now - timedelta(hours=hour_offset)
        if cycle_time.hour not in {0, 6, 12, 18}:
            continue
        key = (cycle_time.strftime("%Y%m%d"), cycle_time.hour)
        if key in seen:
            continue
        seen.add(key)
        date_yyyymmdd, cycle = key
        try:
            hours = available_forecast_hours(
                settings,
                date_yyyymmdd,
                cycle,
                product="sfc",
                source=selected_source,
            )
        except Exception as exc:  # pragma: no cover - live NOAA availability
            errors.append(f"{date_yyyymmdd} {cycle:02d}Z: {exc}")
            continue
        if 48 in hours:
            return {
                "model": settings.default_model,
                "cycle": {
                    "date_yyyymmdd": date_yyyymmdd,
                    "hour_utc": cycle,
                },
                "source": selected_source.title(),
                "policy": "latest_synoptic_run_with_f048_available",
                "available_forecast_hour_max": max(hours),
            }

    # Last-ditch fallback: keep the old age-based behavior rather than failing
    # the service if NOMADS listings are temporarily unavailable.
    cutoff = now - timedelta(hours=settings.full_run_min_age_hours)
    for day_offset in range(0, 4):
        day = cutoff.date() - timedelta(days=day_offset)
        date_yyyymmdd = day.strftime("%Y%m%d")
        for cycle in [18, 12, 6, 0]:
            run_time = datetime(day.year, day.month, day.day, cycle, tzinfo=UTC)
            if run_time <= cutoff:
                return {
                    "model": settings.default_model,
                    "cycle": {
                        "date_yyyymmdd": date_yyyymmdd,
                        "hour_utc": cycle,
                    },
                    "source": selected_source.title(),
                    "policy": "fallback_synoptic_run_by_age",
                    "availability_errors": errors,
                }
    raise RuntimeError("unable to resolve latest full HRRR run")


def render_maps(
    *,
    settings: Settings,
    date_yyyymmdd: str,
    cycle_utc: int,
    forecast_hour: int,
    out_dir: Path,
    products: list[str],
    domain: str | None = None,
    source: str | None = None,
    width: int | None = None,
    height: int | None = None,
    place_label_density: str = "major",
) -> dict[str, Any]:
    request = {
        "model": settings.default_model,
        "date_yyyymmdd": date_yyyymmdd,
        "cycle_utc": cycle_utc,
        "forecast_hour": forecast_hour,
        "source": source or settings.default_source,
        "domain": domain or settings.default_domain,
        "products": products,
        "out_dir": str(out_dir),
        "cache_dir": str(settings.rustwx_cache_dir),
        "use_cache": True,
        "width": width or settings.default_width,
        "height": height or settings.default_height,
        "place_label_density": place_label_density,
    }
    return _json_call(rustwx.render_maps_json(json.dumps(request)))


def render_glm_lightning(
    *,
    settings: Settings,
    out_dir: Path,
    domain: str | None = None,
    label: str | None = None,
    width: int | None = None,
    height: int | None = None,
    max_age_min: float | None = None,
    high_speed_png: bool = True,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "domain": domain or settings.lightning_domain,
        "label": label or settings.lightning_label,
        "data_dir": str(settings.glm_dir),
        "out_dir": str(out_dir),
        "width": width or settings.lightning_width,
        "height": height or settings.lightning_height,
        "max_age_min": max_age_min or settings.lightning_max_age_min,
        "high_speed_png": high_speed_png,
    }
    return _json_call(rustwx.render_glm_lightning_json(json.dumps(request)))


def render_goes_satellite(
    *,
    settings: Settings,
    out_dir: Path,
    domain: str | None = None,
    label: str | None = None,
    products: list[str] | None = None,
    width: int | None = None,
    height: int | None = None,
    skip_scan_id: str | None = None,
    high_speed_png: bool = True,
) -> dict[str, Any]:
    if not hasattr(rustwx, "render_goes_satellite_json"):
        raise RuntimeError("installed rustwx does not expose render_goes_satellite_json")
    request: dict[str, Any] = {
        "satellite": settings.satellite_satellite,
        "abi_product": settings.satellite_abi_product,
        "domain": domain or settings.satellite_domain,
        "label": label or settings.satellite_label,
        "out_dir": str(out_dir),
        "cache_dir": str(settings.rustwx_cache_dir),
        "products": products or settings.satellite_products,
        "width": width or settings.satellite_width,
        "height": height or settings.satellite_height,
        "scan_lookback_hours": settings.satellite_scan_lookback_hours,
        "discovery_retries": settings.satellite_discovery_retries,
        "retry_sleep_ms": settings.satellite_retry_sleep_ms,
        "download_glm": settings.satellite_download_glm,
        "glm_fetch_count": settings.satellite_glm_fetch_count,
        "glm_lookback_hours": settings.satellite_glm_lookback_hours,
        "glm_max_age_min": settings.satellite_glm_max_age_min,
        "high_speed_png": high_speed_png,
    }
    if skip_scan_id:
        request["skip_scan_id"] = skip_scan_id
    return _json_call(rustwx.render_goes_satellite_json(json.dumps(request)))


def sample_point_timeseries(
    *,
    settings: Settings,
    lat: float,
    lon: float,
    date_yyyymmdd: str | None = None,
    cycle_utc: int | None = None,
    source: str | None = None,
    forecast_hour_start: int = 0,
    forecast_hour_end: int = 48,
    forecast_hours: list[int] | None = None,
    variables: list[str] | None = None,
    method: str = "nearest",
) -> dict[str, Any]:
    if date_yyyymmdd is None or cycle_utc is None:
        latest = latest_run(settings, settings.default_model, source)
        date_yyyymmdd = latest["cycle"]["date_yyyymmdd"]
        cycle_utc = latest["cycle"]["hour_utc"]
    request: dict[str, Any] = {
        "model": settings.default_model,
        "date_yyyymmdd": date_yyyymmdd,
        "cycle_utc": cycle_utc,
        "source": source or settings.default_source,
        "lat": lat,
        "lon": lon,
        "cache_dir": str(settings.rustwx_cache_dir),
        "use_cache": True,
        "method": method,
    }
    if forecast_hours:
        request["forecast_hours"] = forecast_hours
    else:
        request["forecast_hour_start"] = forecast_hour_start
        request["forecast_hour_end"] = forecast_hour_end
    if variables:
        request["variables"] = variables
    return _json_call(rustwx.sample_point_timeseries_json(json.dumps(request)))


def sample_point_timeseries_chunked(
    *,
    settings: Settings,
    lat: float,
    lon: float,
    date_yyyymmdd: str,
    cycle_utc: int,
    source: str | None = None,
    forecast_hours: list[int],
    variables: list[str] | None = None,
    method: str = "nearest",
) -> dict[str, Any]:
    hours = sorted(set(forecast_hours))
    chunk_size = settings.meteogram_sample_chunk_size
    workers = settings.meteogram_sample_workers
    if workers <= 1 or len(hours) <= chunk_size:
        return sample_point_timeseries(
            settings=settings,
            lat=lat,
            lon=lon,
            date_yyyymmdd=date_yyyymmdd,
            cycle_utc=cycle_utc,
            source=source,
            forecast_hours=hours,
            variables=variables,
            method=method,
        )

    chunks = [hours[index : index + chunk_size] for index in range(0, len(hours), chunk_size)]

    def run_chunk(chunk: list[int]) -> dict[str, Any]:
        return sample_point_timeseries(
            settings=settings,
            lat=lat,
            lon=lon,
            date_yyyymmdd=date_yyyymmdd,
            cycle_utc=cycle_utc,
            source=source,
            forecast_hours=chunk,
            variables=variables,
            method=method,
        )

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=min(workers, len(chunks))) as executor:
        reports = list(executor.map(run_chunk, chunks))
    if not reports:
        return {}

    merged = dict(reports[0])
    merged["hours"] = sorted(
        [hour for report in reports for hour in report.get("hours", [])],
        key=lambda hour: hour.get("forecast_hour", 0),
    )
    merged["fetches"] = [fetch for report in reports for fetch in report.get("fetches", [])]
    merged["blockers"] = [blocker for report in reports for blocker in report.get("blockers", [])]
    merged["total_ms"] = int((time.perf_counter() - start) * 1000)
    merged["parallel"] = {
        "workers": min(workers, len(chunks)),
        "chunk_size": chunk_size,
        "chunk_total_ms": [report.get("total_ms") for report in reports],
    }
    return merged


def warm_point_timeseries_store(
    *,
    settings: Settings,
    date_yyyymmdd: str,
    cycle_utc: int,
    source: str | None = None,
    forecast_hours: list[int] | None = None,
    variables: list[str] | None = None,
    bounds: tuple[float, float, float, float] | None = None,
) -> dict[str, Any]:
    if not hasattr(rustwx, "warm_point_timeseries_store_json"):
        raise RuntimeError("installed rustwx does not expose warm_point_timeseries_store_json")
    west, east, south, north = bounds or (-125.2, -113.5, 31.0, 43.0)
    request: dict[str, Any] = {
        "model": settings.default_model,
        "date_yyyymmdd": date_yyyymmdd,
        "cycle_utc": cycle_utc,
        "source": source or settings.default_source,
        "cache_dir": str(settings.rustwx_cache_dir),
        "use_cache": True,
        "bounds": [west, east, south, north],
    }
    if forecast_hours:
        request["forecast_hours"] = forecast_hours
    else:
        request["forecast_hour_start"] = 0
        request["forecast_hour_end"] = 48
    if variables:
        request["variables"] = variables
    return _json_call(rustwx.warm_point_timeseries_store_json(json.dumps(request)))


def sample_point_timeseries_store(
    *,
    store_id: str,
    lat: float,
    lon: float,
    forecast_hours: list[int] | None = None,
    method: str = "nearest",
) -> dict[str, Any]:
    if not hasattr(rustwx, "sample_point_timeseries_store_json"):
        raise RuntimeError("installed rustwx does not expose sample_point_timeseries_store_json")
    request: dict[str, Any] = {
        "store_id": store_id,
        "lat": lat,
        "lon": lon,
        "method": method,
    }
    if forecast_hours:
        request["forecast_hours"] = forecast_hours
    return _json_call(rustwx.sample_point_timeseries_store_json(json.dumps(request)))


def rustwx_version() -> str:
    import importlib.metadata

    return importlib.metadata.version("rustwx")
