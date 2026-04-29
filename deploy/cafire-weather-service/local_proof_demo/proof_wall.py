from __future__ import annotations

import argparse
import hashlib
import json
import math
import mimetypes
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen


mimetypes.add_type("image/webp", ".webp")

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROOF_PORT = int(os.environ.get("PROOF_WALL_PORT", "6080"))
ARTIFACT_ROOT = Path(
    os.environ.get(
        "PROOF_WALL_ARTIFACT_ROOT",
        str(REPO_ROOT / "proof" / "cafire_local_artifacts" / "proof_wall_8800"),
    )
)
PRESSURE_STORE = Path(
    os.environ.get(
        "PROOF_WALL_PRESSURE_STORE",
        str(REPO_ROOT / "proof" / "hrrr_pressure_volume_store_latest_f000" / "store"),
    )
)
CONUS_PRESSURE_STORE = Path(
    os.environ.get(
        "PROOF_WALL_CONUS_PRESSURE_STORE",
        str(REPO_ROOT / "proof" / "hrrr_pressure_volume_store_conus_f000" / "store"),
    )
)
VOLUME_RENDERER = Path(
    os.environ.get(
        "PROOF_WALL_VOLUME_RENDERER",
        str(REPO_ROOT / "target" / "release" / "volume_store_cross_section_render.exe"),
    )
)
PRIMARY_MANIFEST = ARTIFACT_ROOT / "hrrr" / "runs" / "20260428" / "00Z" / "manifest.json"
F001_WINDOWED_MANIFEST = (
    ARTIFACT_ROOT / "hrrr" / "runs" / "20260428" / "00Z" / "proof-wall-f001-windowed.json"
)
VOLUME_CROSS_SECTION_REPORT = ARTIFACT_ROOT / "volume_cross_sections" / "volume_cross_section_render_report.json"
CUSTOM_CROSS_SECTION_ROOT = ARTIFACT_ROOT / "custom_cross_sections"

PRESSURE_BASE_URL = os.environ.get("PRESSURE_BASE_URL", "http://127.0.0.1:6077")
CONUS_PRESSURE_BASE_URL = os.environ.get("CONUS_PRESSURE_BASE_URL", "http://127.0.0.1:6078")
CAFIRE_BASE_URL = os.environ.get("CAFIRE_BASE_URL", "http://127.0.0.1:6081")
CA_BOUNDS = {"west": -125.2, "east": -113.5, "south": 31.0, "north": 43.0}
CONUS_BOUNDS = {"west": -125.0, "east": -66.0, "south": 24.0, "north": 50.0}
PRESSURE_DOMAINS = {
    "ca": {
        "label": "California",
        "store": PRESSURE_STORE,
        "bounds": CA_BOUNDS,
        "max_spacing_km": 80.0,
    },
    "conus": {
        "label": "CONUS",
        "store": CONUS_PRESSURE_STORE,
        "bounds": CONUS_BOUNDS,
        "max_spacing_km": 500.0,
    },
}
DEFAULT_CROSS_SECTION_PRODUCTS = "all"

PROFILE_POINTS = [
    {"name": "Redding / Carr Fire Country", "lat": 40.5865, "lon": -122.3917},
    {"name": "Bay Area Ridge", "lat": 37.8909, "lon": -120.4980},
    {"name": "Southern Sierra", "lat": 36.3843, "lon": -118.2349},
]

ROUTES = [
    {
        "name": "Bay to Sierra",
        "lat1": 37.7749,
        "lon1": -122.4194,
        "lat2": 38.5788,
        "lon2": -119.7513,
    },
    {
        "name": "Redding to Tahoe",
        "lat1": 40.5865,
        "lon1": -122.3917,
        "lat2": 39.0968,
        "lon2": -120.0324,
    },
    {
        "name": "LA Basin to Southern Sierra",
        "lat1": 34.0522,
        "lon1": -118.2437,
        "lat2": 36.5786,
        "lon2": -118.2923,
    },
    {
        "name": "North Coast to Central Valley",
        "lat1": 40.8021,
        "lon1": -124.1637,
        "lat2": 39.7285,
        "lon2": -121.8375,
    },
    {
        "name": "San Diego to Inland Empire",
        "lat1": 32.7157,
        "lon1": -117.1611,
        "lat2": 34.1083,
        "lon2": -117.2898,
    },
]

METEOGRAM_REQUESTS = [
    {
        "name": "Central Valley cached f000-f048",
        "lat": 37.5954,
        "lon": -120.3882,
        "date_yyyymmdd": "20260428",
        "cycle_utc": 7,
    },
    {
        "name": "Sierra cached f000-f048",
        "lat": 37.8909,
        "lon": -120.4980,
        "date_yyyymmdd": "20260428",
        "cycle_utc": 7,
    },
    {
        "name": "Southern Sierra cached f000-f048",
        "lat": 36.3843,
        "lon": -118.2349,
        "date_yyyymmdd": "20260428",
        "cycle_utc": 7,
    },
]


def http_json(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None, timeout: float = 30.0) -> dict[str, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, method=method, headers=headers)
    started = time.perf_counter()
    with urlopen(request, timeout=timeout) as response:
        data = json.loads(response.read().decode("utf-8"))
    data["_http_ms"] = int((time.perf_counter() - started) * 1000)
    return data


def safe_json_call(label: str, fn: Any) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        value = fn()
        return {
            "ok": True,
            "label": label,
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "value": value,
        }
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "label": label,
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }


def read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def artifact_url_for_key(key: str) -> str:
    return "/artifacts/" + key.replace("\\", "/")


def artifact_key_for_path(path: Path, fallback_dir: str) -> str:
    resolved = path if path.is_absolute() else (REPO_ROOT / path)
    resolved = resolved.resolve()
    try:
        return resolved.relative_to(ARTIFACT_ROOT.resolve()).as_posix()
    except (OSError, ValueError):
        return f"{fallback_dir}/{resolved.name}"


def render_report_with_urls(report: dict[str, Any] | None, fallback_dir: str) -> dict[str, Any] | None:
    if not report:
        return None
    records = []
    for output in report.get("outputs", []):
        png_path = Path(output.get("png_path", ""))
        png_key = artifact_key_for_path(png_path, fallback_dir)
        png_resolved = (REPO_ROOT / png_path).resolve() if not png_path.is_absolute() else png_path.resolve()
        png_size_bytes = png_resolved.stat().st_size if png_resolved.exists() and png_resolved.is_file() else None
        webp_path = Path(output.get("webp_path", ""))
        webp_key = artifact_key_for_path(webp_path, fallback_dir) if str(webp_path) else None
        webp_resolved = (REPO_ROOT / webp_path).resolve() if str(webp_path) and not webp_path.is_absolute() else webp_path.resolve() if str(webp_path) else None
        webp_size_bytes = (
            webp_resolved.stat().st_size
            if webp_resolved is not None and webp_resolved.exists() and webp_resolved.is_file()
            else None
        )
        preferred_key = webp_key if webp_size_bytes else png_key
        records.append(
            {
                **output,
                "artifact_key": preferred_key,
                "url": artifact_url_for_key(preferred_key),
                "format": "webp" if webp_size_bytes else "png",
                "size_bytes": webp_size_bytes if webp_size_bytes else png_size_bytes,
                "png_artifact_key": png_key,
                "png_url": artifact_url_for_key(png_key),
                "png_size_bytes": png_size_bytes,
                "webp_artifact_key": webp_key,
                "webp_url": artifact_url_for_key(webp_key) if webp_key else None,
                "webp_size_bytes": webp_size_bytes,
            }
        )
    return {**report, "records": records}


def collect_artifact_records(manifest: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not manifest:
        return []
    records: list[dict[str, Any]] = []
    for hour in manifest.get("hours", []):
        for item in hour.get("uploaded", []):
            key = item.get("key") or ""
            fmt = item.get("format")
            if fmt not in {"png", "webp"}:
                continue
            records.append(
                {
                    "forecast_hour": int(hour.get("forecast_hour", 0)),
                    "key": key,
                    "url": artifact_url_for_key(key),
                    "format": fmt,
                    "size_bytes": item.get("size_bytes"),
                    "product": product_from_key(key),
                }
            )
    return sorted(records, key=lambda item: (item["forecast_hour"], item["product"], item["format"]))


def product_from_key(key: str) -> str:
    stem = Path(key).stem
    marker = "_california_"
    if marker in stem:
        return stem.split(marker, 1)[1]
    return stem


def summarize_static_blockers(manifest: dict[str, Any] | None) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    if not manifest:
        return blockers
    for hour in manifest.get("hours", []):
        report = hour.get("rustwx_report", {})
        for domain in report.get("domains", []):
            for blocker in domain.get("windowed", {}).get("blockers", []):
                blockers.append(
                    {
                        "hour": f"f{int(hour.get('forecast_hour', 0)):03d}",
                        "product": str(blocker.get("product", "")),
                        "reason": str(blocker.get("reason", "")),
                    }
                )
    return blockers


def volume_cross_section_images() -> dict[str, Any] | None:
    report = read_manifest(VOLUME_CROSS_SECTION_REPORT)
    return render_report_with_urls(report, "volume_cross_sections")


def pressure_profiles() -> list[dict[str, Any]]:
    reports = []
    for point in PROFILE_POINTS:
        url = f"{PRESSURE_BASE_URL}/api/point?lat={point['lat']}&lon={point['lon']}"
        result = safe_json_call(point["name"], lambda url=url: http_json(url, timeout=15.0))
        if result["ok"]:
            profile = result["value"].get("profile", {})
            samples = profile.get("samples", [])
            result["summary"] = {
                "sample_count": len(samples),
                "sidecar_elapsed_ms": result["value"].get("elapsed_ms"),
                "http_ms": result["value"].get("_http_ms"),
                "variables": sorted({sample.get("variable") for sample in samples}),
                "levels": len({sample.get("level_hpa") for sample in samples}),
            }
        reports.append({"point": point, **result})
    return reports


def cross_sections(metadata: dict[str, Any] | None) -> list[dict[str, Any]]:
    variables = list((metadata or {}).get("variables") or ["TMP", "SPFH", "UGRD", "VGRD", "HGT"])
    hours = list((metadata or {}).get("forecast_hours") or [0])
    hour = int(hours[0])
    reports: list[dict[str, Any]] = []
    for route in ROUTES:
        for variable in variables:
            payload = {
                "lat1": route["lat1"],
                "lon1": route["lon1"],
                "lat2": route["lat2"],
                "lon2": route["lon2"],
                "hour": hour,
                "variable": variable,
                "spacing_km": 20,
            }
            result = safe_json_call(
                f"{route['name']} {variable}",
                lambda payload=payload: http_json(
                    f"{CAFIRE_BASE_URL}/api/v1/public/cross-section",
                    method="POST",
                    payload=payload,
                    timeout=20.0,
                ),
            )
            if result["ok"]:
                section = result["value"].get("section", {})
                values = section.get("values", [])
                route_samples = section.get("route_samples", [])
                finite = [item.get("value") for item in values if isinstance(item.get("value"), (int, float))]
                result["summary"] = {
                    "route_samples": len(route_samples),
                    "value_count": len(values),
                    "sidecar_elapsed_ms": result["value"].get("elapsed_ms"),
                    "proxy_total_ms": result["value"].get("proxy_total_ms"),
                    "http_ms": result["value"].get("_http_ms"),
                    "min_value": min(finite) if finite else None,
                    "max_value": max(finite) if finite else None,
                }
            reports.append({"route": route, "variable": variable, "hour": hour, **result})
    return reports


def meteograms() -> list[dict[str, Any]]:
    reports = []
    for item in METEOGRAM_REQUESTS:
        payload = {
            "lat": item["lat"],
            "lon": item["lon"],
            "date_yyyymmdd": item["date_yyyymmdd"],
            "cycle_utc": item["cycle_utc"],
            "forecast_hour_start": 0,
            "forecast_hour_end": 48,
            "label": "Selected point",
        }
        result = safe_json_call(
            item["name"],
            lambda payload=payload: http_json(
                f"{CAFIRE_BASE_URL}/api/v1/public/meteogram.png",
                method="POST",
                payload=payload,
                timeout=45.0,
            ),
        )
        if result["ok"]:
            value = result["value"]
            result["summary"] = {
                "cache_hit": value.get("cache_hit"),
                "sample_total_ms": value.get("sample_total_ms"),
                "render_total_ms": value.get("render_total_ms"),
                "http_ms": value.get("_http_ms"),
                "hours": len(value.get("forecast_hours") or []),
                "image_url": f"{CAFIRE_BASE_URL}{value.get('url')}" if value.get("url", "").startswith("/") else value.get("url"),
            }
        reports.append({"request": item, **result})
    return reports


def required_float(payload: dict[str, Any], key: str) -> float:
    if key not in payload:
        raise ValueError(f"missing required field '{key}'")
    try:
        value = float(payload[key])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"field '{key}' must be a number") from exc
    if not math.isfinite(value):
        raise ValueError(f"field '{key}' must be finite")
    return value


def normalize_domain(value: Any) -> str:
    domain = str(value or "ca").strip().lower()
    aliases = {
        "california": "ca",
        "cafire": "ca",
        "continental_us": "conus",
        "continental-us": "conus",
        "us": "conus",
        "usa": "conus",
    }
    domain = aliases.get(domain, domain)
    if domain not in PRESSURE_DOMAINS:
        supported = ", ".join(sorted(PRESSURE_DOMAINS))
        raise ValueError(f"unsupported pressure domain '{domain}', expected one of: {supported}")
    return domain


def pressure_domain_config(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    domain = normalize_domain(payload.get("domain"))
    config = PRESSURE_DOMAINS[domain]
    store = config["store"]
    if not store.exists():
        raise RuntimeError(f"{config['label']} pressure VolumeStore does not exist: {store}")
    return domain, config


def validate_point_in_bounds(lat: float, lon: float, label: str, bounds: dict[str, float], domain_label: str) -> None:
    if not (bounds["south"] <= lat <= bounds["north"]):
        raise ValueError(
            f"{label} latitude {lat} is outside {domain_label} proof-store bounds "
            f"{bounds['south']}..{bounds['north']}"
        )
    if not (bounds["west"] <= lon <= bounds["east"]):
        raise ValueError(
            f"{label} longitude {lon} is outside {domain_label} proof-store bounds "
            f"{bounds['west']}..{bounds['east']}"
        )


def pressure_available_hours(store: Path) -> list[int]:
    manifest = read_manifest(store / "manifest.json") or {}
    hours = manifest.get("forecast_hours") or [0]
    return sorted({int(hour) for hour in hours})


def normalize_top_pressure_hpa(payload: dict[str, Any]) -> float:
    top_pressure_hpa = float(payload.get("top_pressure_hpa", 100.0))
    if not math.isfinite(top_pressure_hpa) or top_pressure_hpa < 50.0 or top_pressure_hpa > 1000.0:
        raise ValueError("top_pressure_hpa must be between 50 and 1000")
    return top_pressure_hpa


def command_for_display(command: list[str]) -> str:
    parts = []
    for item in command:
        if any(ch.isspace() for ch in item):
            parts.append(f'"{item}"')
        else:
            parts.append(item)
    return " ".join(parts)


def render_custom_cross_section(payload: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    if not VOLUME_RENDERER.exists():
        raise RuntimeError(f"renderer executable does not exist: {VOLUME_RENDERER}")
    domain, domain_config = pressure_domain_config(payload)
    store = domain_config["store"]
    bounds = domain_config["bounds"]
    domain_label = domain_config["label"]

    lat1 = required_float(payload, "lat1")
    lon1 = required_float(payload, "lon1")
    lat2 = required_float(payload, "lat2")
    lon2 = required_float(payload, "lon2")
    validate_point_in_bounds(lat1, lon1, "start", bounds, domain_label)
    validate_point_in_bounds(lat2, lon2, "end", bounds, domain_label)
    if abs(lat1 - lat2) < 1.0e-6 and abs(lon1 - lon2) < 1.0e-6:
        raise ValueError("start and end points are identical")

    spacing_km = float(payload.get("spacing_km", 5.0))
    max_spacing_km = float(domain_config["max_spacing_km"])
    if not math.isfinite(spacing_km) or spacing_km < 1.0 or spacing_km > max_spacing_km:
        raise ValueError(f"spacing_km must be between 1 and {max_spacing_km:g} for {domain_label}")

    top_pressure_hpa = normalize_top_pressure_hpa(payload)

    hour = int(payload.get("hour", 0))
    available_hours = pressure_available_hours(store)
    if hour not in available_hours:
        supported = ", ".join(f"f{value:03d}" for value in available_hours)
        raise ValueError(f"requested f{hour:03d}, but this {domain_label} pressure store supports only {supported}")

    products = str(payload.get("products") or DEFAULT_CROSS_SECTION_PRODUCTS)
    signature = json.dumps(
        {
            "domain": domain,
            "lat1": round(lat1, 5),
            "lon1": round(lon1, 5),
            "lat2": round(lat2, 5),
            "lon2": round(lon2, 5),
            "spacing_km": round(spacing_km, 3),
            "top_pressure_hpa": round(top_pressure_hpa, 1),
            "hour": hour,
            "products": products,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    route_id = f"{domain}_map_" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:12]
    route_name = f"{domain_label} map {lat1:.3f},{lon1:.3f} to {lat2:.3f},{lon2:.3f}"
    out_dir = CUSTOM_CROSS_SECTION_ROOT / domain / route_id
    out_dir.mkdir(parents=True, exist_ok=True)

    command = [
        str(VOLUME_RENDERER),
        "--store",
        str(store),
        "--out-dir",
        str(out_dir),
        "--products",
        products,
        "--hour",
        str(hour),
        "--spacing-km",
        f"{spacing_km:g}",
        "--top-pressure-hpa",
        f"{top_pressure_hpa:g}",
        "--width",
        "1400",
        "--height",
        "820",
        "--route-id",
        route_id,
        "--route-name",
        route_name,
        "--start-lat",
        f"{lat1:.8f}",
        "--start-lon",
        f"{lon1:.8f}",
        "--end-lat",
        f"{lat2:.8f}",
        "--end-lon",
        f"{lon2:.8f}",
    ]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    display_command = command_for_display(command)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"renderer failed with exit {completed.returncode}: {detail}; command: {display_command}")

    report_path = out_dir / "volume_cross_section_render_report.json"
    report = render_report_with_urls(read_manifest(report_path), f"custom_cross_sections/{domain}/{route_id}")
    if not report:
        raise RuntimeError(f"renderer did not write report: {report_path}; command: {display_command}")

    return {
        "ok": True,
        "domain": domain,
        "domain_label": domain_label,
        "store": str(store),
        "bounds": bounds,
        "request": {
            "domain": domain,
            "lat1": lat1,
            "lon1": lon1,
            "lat2": lat2,
            "lon2": lon2,
            "spacing_km": spacing_km,
            "top_pressure_hpa": top_pressure_hpa,
            "hour": hour,
            "products": products,
        },
        "route_id": route_id,
        "route_name": route_name,
        "command": display_command,
        "server_elapsed_ms": int((time.perf_counter() - started) * 1000),
        "renderer_total_ms": report.get("total_ms"),
        "rendered_count": report.get("rendered_count"),
        "skipped_count": report.get("skipped_count"),
        "skipped": report.get("skipped", []),
        "records": report.get("records", []),
        "report": report,
    }


def normalize_loop_product(value: Any) -> str:
    product = str(value or "wind_speed").strip().lower().replace("-", "_")
    aliases = {
        "wind": "wind_speed",
        "winds": "wind_speed",
        "relative_humidity": "rh",
        "humidity": "rh",
        "fire": "fire_wx",
        "fire_weather": "fire_wx",
    }
    return aliases.get(product, product)


def render_custom_cross_section_loop(payload: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    if not VOLUME_RENDERER.exists():
        raise RuntimeError(f"renderer executable does not exist: {VOLUME_RENDERER}")
    domain, domain_config = pressure_domain_config(payload)
    store = domain_config["store"]
    bounds = domain_config["bounds"]
    domain_label = domain_config["label"]

    lat1 = required_float(payload, "lat1")
    lon1 = required_float(payload, "lon1")
    lat2 = required_float(payload, "lat2")
    lon2 = required_float(payload, "lon2")
    validate_point_in_bounds(lat1, lon1, "start", bounds, domain_label)
    validate_point_in_bounds(lat2, lon2, "end", bounds, domain_label)
    if abs(lat1 - lat2) < 1.0e-6 and abs(lon1 - lon2) < 1.0e-6:
        raise ValueError("start and end points are identical")

    spacing_km = float(payload.get("spacing_km", 5.0))
    max_spacing_km = float(domain_config["max_spacing_km"])
    if not math.isfinite(spacing_km) or spacing_km < 1.0 or spacing_km > max_spacing_km:
        raise ValueError(f"spacing_km must be between 1 and {max_spacing_km:g} for {domain_label}")

    top_pressure_hpa = normalize_top_pressure_hpa(payload)

    product = normalize_loop_product(payload.get("product") or payload.get("products"))
    hours = str(payload.get("hours") or "all").strip() or "all"
    available_hours = pressure_available_hours(store)
    signature = json.dumps(
        {
            "domain": domain,
            "lat1": round(lat1, 5),
            "lon1": round(lon1, 5),
            "lat2": round(lat2, 5),
            "lon2": round(lon2, 5),
            "spacing_km": round(spacing_km, 3),
            "top_pressure_hpa": round(top_pressure_hpa, 1),
            "hours": hours,
            "product": product,
            "format": "webp-loop-v1",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    route_id = f"{domain}_loop_" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:12]
    route_name = f"{domain_label} loop {lat1:.3f},{lon1:.3f} to {lat2:.3f},{lon2:.3f}"
    out_dir = CUSTOM_CROSS_SECTION_ROOT / domain / route_id / product
    out_dir.mkdir(parents=True, exist_ok=True)

    command = [
        str(VOLUME_RENDERER),
        "--store",
        str(store),
        "--out-dir",
        str(out_dir),
        "--products",
        product,
        "--hours",
        hours,
        "--spacing-km",
        f"{spacing_km:g}",
        "--top-pressure-hpa",
        f"{top_pressure_hpa:g}",
        "--width",
        "1400",
        "--height",
        "820",
        "--route-id",
        route_id,
        "--route-name",
        route_name,
        "--start-lat",
        f"{lat1:.8f}",
        "--start-lon",
        f"{lon1:.8f}",
        "--end-lat",
        f"{lat2:.8f}",
        "--end-lon",
        f"{lon2:.8f}",
    ]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=900,
        check=False,
    )
    display_command = command_for_display(command)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"renderer failed with exit {completed.returncode}: {detail}; command: {display_command}")

    report_path = out_dir / "volume_cross_section_render_report.json"
    report = render_report_with_urls(read_manifest(report_path), f"custom_cross_sections/{domain}/{route_id}/{product}")
    if not report:
        raise RuntimeError(f"renderer did not write report: {report_path}; command: {display_command}")
    frames = sorted(report.get("records", []), key=lambda item: (int(item.get("hour", 0)), str(item.get("product", ""))))

    return {
        "ok": True,
        "domain": domain,
        "domain_label": domain_label,
        "store": str(store),
        "bounds": bounds,
        "request": {
            "domain": domain,
            "lat1": lat1,
            "lon1": lon1,
            "lat2": lat2,
            "lon2": lon2,
            "spacing_km": spacing_km,
            "top_pressure_hpa": top_pressure_hpa,
            "hours": hours,
            "product": product,
        },
        "route_id": route_id,
        "route_name": route_name,
        "available_hours": available_hours,
        "rendered_hours": report.get("forecast_hours", []),
        "frame_count": len(frames),
        "frames": frames,
        "command": display_command,
        "server_elapsed_ms": int((time.perf_counter() - started) * 1000),
        "renderer_total_ms": report.get("total_ms"),
        "skipped_count": report.get("skipped_count"),
        "skipped": report.get("skipped", []),
        "report": report,
    }


def build_proof() -> dict[str, Any]:
    primary = read_manifest(PRIMARY_MANIFEST)
    f001 = read_manifest(F001_WINDOWED_MANIFEST)
    metadata_result = safe_json_call("pressure metadata", lambda: http_json(f"{PRESSURE_BASE_URL}/api/metadata", timeout=10.0))
    metadata = metadata_result.get("value") if metadata_result["ok"] else None
    pressure_hours = sorted(int(hour) for hour in (metadata or {}).get("forecast_hours") or [])
    if pressure_hours:
        pressure_limit = (
            f"Current pressure VolumeStore covers f{pressure_hours[0]:03d}-f{pressure_hours[-1]:03d} "
            f"({len(pressure_hours)} forecast hours)."
        )
    else:
        pressure_limit = "Current pressure VolumeStore coverage could not be read from the sidecar metadata."
    return {
        "generated_at_epoch_ms": int(time.time() * 1000),
        "sources": {
            "proof_wall_port": DEFAULT_PROOF_PORT,
            "cafire_api": CAFIRE_BASE_URL,
            "pressure_sidecar": PRESSURE_BASE_URL,
            "conus_pressure_sidecar": CONUS_PRESSURE_BASE_URL,
            "artifact_root": str(ARTIFACT_ROOT),
        },
        "health": {
            "pressure_metadata": metadata_result,
            "cafire_health": safe_json_call("cafire health", lambda: http_json(f"{CAFIRE_BASE_URL}/health", timeout=10.0)),
            "warm_status": safe_json_call(
                "cafire warm status", lambda: http_json(f"{CAFIRE_BASE_URL}/api/v1/public/warm-status", timeout=10.0)
            ),
        },
        "static_plots": {
            "primary_manifest": primary,
            "f001_windowed_manifest": f001,
            "records": collect_artifact_records(primary) + collect_artifact_records(f001),
            "blockers": summarize_static_blockers(primary),
        },
        "volume_cross_section_images": volume_cross_section_images(),
        "pressure_profiles": pressure_profiles(),
        "cross_sections": cross_sections(metadata),
        "meteograms": meteograms(),
        "known_limits": [
            pressure_limit,
            "Classic surface meteograms use cached artifacts / point-timeseries fast-store path, not the pressure VolumeStore schema.",
            "The interrupted f000-f048 pressure store is not openable because index.bin and build_stats.json were never finalized.",
            "Diurnal static products require f024/f048 windowed runs; this proof wall generated f000 hourly maps and f001 one-hour windowed maps.",
        ],
    }


class ProofWallHandler(BaseHTTPRequestHandler):
    server_version = "rustwx-proof-wall/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/":
            self.write_bytes(HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/api/proof":
            self.write_json(build_proof())
            return
        if path.startswith("/artifacts/"):
            self.serve_artifact(path[len("/artifacts/") :])
            return
        self.write_json({"error": "not found"}, status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/render-cross-section":
            self.handle_render_cross_section()
            return
        if parsed.path == "/api/render-cross-section-loop":
            self.handle_render_cross_section_loop()
            return
        self.write_json({"error": "not found"}, status=404)

    def handle_render_cross_section(self) -> None:
        try:
            payload = self.read_json_body()
            self.write_json(render_custom_cross_section(payload))
        except ValueError as exc:
            self.write_json({"ok": False, "error": str(exc)}, status=400)
        except subprocess.TimeoutExpired as exc:
            self.write_json(
                {
                    "ok": False,
                    "error": f"renderer timed out after {exc.timeout} seconds",
                    "command": command_for_display(exc.cmd) if isinstance(exc.cmd, list) else str(exc.cmd),
                },
                status=504,
            )
        except RuntimeError as exc:
            self.write_json({"ok": False, "error": str(exc)}, status=500)

    def handle_render_cross_section_loop(self) -> None:
        try:
            payload = self.read_json_body()
            self.write_json(render_custom_cross_section_loop(payload))
        except ValueError as exc:
            self.write_json({"ok": False, "error": str(exc)}, status=400)
        except subprocess.TimeoutExpired as exc:
            self.write_json(
                {
                    "ok": False,
                    "error": f"renderer timed out after {exc.timeout} seconds",
                    "command": command_for_display(exc.cmd) if isinstance(exc.cmd, list) else str(exc.cmd),
                },
                status=504,
            )
        except RuntimeError as exc:
            self.write_json({"ok": False, "error": str(exc)}, status=500)

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        if length > 32768:
            raise ValueError("request body is too large")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("request body must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def serve_artifact(self, raw_key: str) -> None:
        key = unquote(raw_key).replace("/", "\\")
        path = (ARTIFACT_ROOT / key).resolve()
        try:
            path.relative_to(ARTIFACT_ROOT.resolve())
        except ValueError:
            self.write_json({"error": "invalid artifact path"}, status=400)
            return
        if not path.exists() or not path.is_file():
            self.write_json({"error": "artifact not found"}, status=404)
            return
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.write_bytes(path.read_bytes(), content_type)

    def write_json(self, value: dict[str, Any], status: int = 200) -> None:
        self.write_bytes(json.dumps(value, separators=(",", ":")).encode("utf-8"), "application/json", status)

    def write_bytes(self, data: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.address_string()} - {fmt % args}", flush=True)


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>rustwx CA Fire Proof Wall</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    :root { color-scheme: light; --ink:#172026; --muted:#5f6b74; --line:#d8dde2; --bg:#f5f7f8; --panel:#ffffff; --ok:#0f7a4f; --warn:#a15c00; --bad:#b42318; --blue:#1f5f8b; }
    * { box-sizing: border-box; }
    body { margin:0; font-family: Segoe UI, Arial, sans-serif; color:var(--ink); background:var(--bg); }
    header { position:sticky; top:0; z-index:5; background:#1d2730; color:white; padding:14px 18px; display:flex; align-items:center; gap:18px; border-bottom:1px solid #0d141a; }
    header h1 { margin:0; font-size:18px; font-weight:700; letter-spacing:0; }
    header span { color:#c7d2dc; font-size:13px; }
    main { max-width:1500px; margin:0 auto; padding:16px; display:grid; gap:14px; }
    section { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
    h2 { margin:0 0 10px 0; font-size:16px; }
    h3 { margin:0 0 8px 0; font-size:14px; }
    .grid { display:grid; gap:10px; }
    .metrics { grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); }
    .metric { border:1px solid var(--line); border-radius:6px; padding:10px; background:#fbfcfd; min-height:72px; }
    .metric strong { display:block; font-size:20px; line-height:1.1; }
    .metric span { display:block; margin-top:5px; color:var(--muted); font-size:12px; }
    .ok { color:var(--ok); }
    .warn { color:var(--warn); }
    .bad { color:var(--bad); }
    .muted { color:var(--muted); }
    .maps { grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); }
    .card { border:1px solid var(--line); border-radius:6px; overflow:hidden; background:white; }
    .card img { width:100%; display:block; background:#e9edf0; }
    .card .meta { padding:8px 10px; font-size:12px; display:grid; gap:3px; }
    .card strong { font-size:13px; }
    .xs-grid { grid-template-columns:repeat(auto-fill,minmax(340px,1fr)); }
    .xs { border:1px solid var(--line); border-radius:6px; padding:9px; background:#fbfcfd; }
    .xs canvas { width:100%; height:120px; display:block; border:1px solid #dbe1e6; background:#f8fafb; }
    .xs img { width:100%; display:block; border:1px solid #dbe1e6; background:#f8fafb; }
    .profiles { grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); }
    .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; margin:2px; font-size:12px; background:#f8fafb; }
    .meteograms { grid-template-columns:repeat(auto-fit,minmax(360px,1fr)); }
    .log { white-space:pre-wrap; font:12px Consolas, monospace; background:#f8fafb; border:1px solid var(--line); border-radius:6px; padding:10px; overflow:auto; max-height:260px; }
    button { height:32px; border:1px solid #9aa7b3; border-radius:5px; background:white; color:var(--ink); padding:0 10px; }
    .toolbar { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .map-layout { display:grid; grid-template-columns:minmax(360px,1fr) minmax(320px,420px); gap:12px; align-items:start; }
    #route-map { height:440px; min-height:360px; border:1px solid var(--line); border-radius:6px; overflow:hidden; background:#dfe7ec; }
    .field { display:grid; gap:4px; font-size:12px; color:var(--muted); }
    .field input, .field select { height:32px; border:1px solid #aeb8c2; border-radius:5px; padding:0 8px; font:14px Segoe UI, Arial, sans-serif; color:var(--ink); background:white; }
    .coord-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
    .command { white-space:pre-wrap; overflow:auto; max-height:92px; }
    .loop-stage { display:grid; gap:8px; }
    .loop-stage img { width:100%; display:block; border:1px solid #dbe1e6; background:#f8fafb; }
    .frame-strip { display:flex; gap:6px; overflow:auto; padding-bottom:2px; }
    .frame-strip img { width:116px; height:68px; object-fit:cover; border:2px solid transparent; background:#f8fafb; }
    .frame-strip img.active { border-color:var(--blue); }
    .slider { width:min(460px, 100%); }
    @media (max-width:700px) { header { display:block; } main { padding:10px; } .meteograms { grid-template-columns:1fr; } }
    @media (max-width:900px) { .map-layout { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <header>
    <h1>rustwx CA Fire Proof Wall</h1>
    <span id="runline">loading local proof data...</span>
  </header>
  <main>
    <section>
      <div class="toolbar">
        <h2 style="margin-right:auto">Status</h2>
        <button id="refresh">Refresh</button>
      </div>
      <div id="metrics" class="grid metrics"></div>
    </section>

    <section>
      <h2>Draw Any Pressure VolumeStore Cross-Section</h2>
      <div class="map-layout">
        <div id="route-map"></div>
        <div class="grid">
          <label class="field">Domain
            <select id="domain" style="height:32px;border:1px solid #aeb8c2;border-radius:5px;padding:0 8px;font:14px Segoe UI, Arial, sans-serif;color:var(--ink)">
              <option value="ca">California pressure store</option>
              <option value="conus">CONUS pressure store</option>
            </select>
          </label>
          <div class="coord-grid">
            <label class="field">Start lat<input id="lat1" inputmode="decimal"></label>
            <label class="field">Start lon<input id="lon1" inputmode="decimal"></label>
            <label class="field">End lat<input id="lat2" inputmode="decimal"></label>
            <label class="field">End lon<input id="lon2" inputmode="decimal"></label>
          </div>
          <div class="toolbar">
            <label class="field" style="width:130px">Spacing km<input id="spacing" inputmode="decimal" value="5"></label>
            <label class="field" style="width:120px">Top hPa
              <select id="top-pressure">
                <option value="50">50</option>
                <option value="75">75</option>
                <option value="100" selected>100</option>
                <option value="150">150</option>
                <option value="200">200</option>
                <option value="300">300</option>
                <option value="500">500</option>
              </select>
            </label>
            <button id="render-route">Render all products</button>
            <label class="field" style="width:160px">Loop product
              <select id="loop-product">
                <option value="wind_speed">Wind speed</option>
                <option value="temperature">Temperature</option>
                <option value="rh">RH</option>
                <option value="theta_e">Theta-e</option>
                <option value="fire_wx">Fire wx</option>
              </select>
            </label>
            <button id="render-loop">Render WebP loop</button>
            <button id="clear-route">Clear</button>
          </div>
          <div id="route-state" class="log">Click two points in the map.</div>
        </div>
      </div>
      <div id="custom-cross-sections" class="grid xs-grid" style="margin-top:12px"></div>
    </section>

    <section>
      <h2>Generated CA Static Plots</h2>
      <div id="static-note" class="muted"></div>
      <div id="maps" class="grid maps" style="margin-top:10px"></div>
      <h3 style="margin-top:12px">Static Product Blockers</h3>
      <div id="blockers" class="log"></div>
    </section>

    <section>
      <h2>Pressure VolumeStore Profiles</h2>
      <div id="profiles" class="grid profiles"></div>
    </section>

    <section>
      <h2>Rendered VolumeStore Cross-Sections</h2>
      <div id="xs-note" class="muted"></div>
      <div id="cross-sections" class="grid xs-grid" style="margin-top:10px"></div>
    </section>

    <section>
      <h2>Meteogram Retrieval Checks</h2>
      <div id="meteogram-note" class="muted"></div>
      <div id="meteograms" class="grid meteograms" style="margin-top:10px"></div>
    </section>

    <section>
      <h2>Limits This Page Refuses To Hide</h2>
      <div id="limits" class="log"></div>
    </section>
  </main>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, ch => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[ch]));
}
function bytes(n) {
  if (!n) return "";
  if (n < 1024) return `${n} B`;
  if (n < 1024*1024) return `${Math.round(n/1024)} KB`;
  return `${(n/1024/1024).toFixed(1)} MB`;
}
function metric(label, value, cls="") {
  return `<div class="metric"><strong class="${cls}">${esc(value)}</strong><span>${esc(label)}</span></div>`;
}
function productLabel(value) {
  return String(value).replace(/_/g, " ").replace(/\bqpf\b/i, "QPF").replace(/\bvpd\b/i, "VPD").replace(/\bpm25\b/i, "PM2.5");
}
const PRESSURE_DOMAINS = {
  ca: {
    label: "California",
    bounds: { west: -125.2, east: -113.5, south: 31.0, north: 43.0 },
    defaultSpacingKm: 5
  },
  conus: {
    label: "CONUS",
    bounds: { west: -125.0, east: -66.0, south: 24.0, north: 50.0 },
    defaultSpacingKm: 100
  }
};
let routeMap = null;
let routeLayer = null;
let boundsLayer = null;
let routePoints = [];
let renderSerial = 0;
let loopTimer = null;
function activeDomainKey() {
  return document.getElementById("domain").value || "ca";
}
function activeDomain() {
  return PRESSURE_DOMAINS[activeDomainKey()] || PRESSURE_DOMAINS.ca;
}
function withinActiveBounds(lat, lon) {
  const bounds = activeDomain().bounds;
  return lat >= bounds.south && lat <= bounds.north && lon >= bounds.west && lon <= bounds.east;
}
function setRouteState(text) {
  document.getElementById("route-state").textContent = text;
}
function setCoordinateInputs() {
  const [start, end] = routePoints;
  document.getElementById("lat1").value = start ? start[0].toFixed(5) : "";
  document.getElementById("lon1").value = start ? start[1].toFixed(5) : "";
  document.getElementById("lat2").value = end ? end[0].toFixed(5) : "";
  document.getElementById("lon2").value = end ? end[1].toFixed(5) : "";
}
function stopLoopPlayback() {
  if (loopTimer) {
    clearInterval(loopTimer);
    loopTimer = null;
  }
}
function routePayloadFromInputs() {
  const domain = activeDomainKey();
  const payload = {
    domain,
    lat1: Number(document.getElementById("lat1").value),
    lon1: Number(document.getElementById("lon1").value),
    lat2: Number(document.getElementById("lat2").value),
    lon2: Number(document.getElementById("lon2").value),
    spacing_km: Number(document.getElementById("spacing").value || 5),
    top_pressure_hpa: Number(document.getElementById("top-pressure").value || 100),
    hour: 0
  };
  for (const key of ["lat1", "lon1", "lat2", "lon2", "spacing_km", "top_pressure_hpa"]) {
    if (!Number.isFinite(payload[key])) throw new Error(`${key} must be numeric`);
  }
  if (payload.top_pressure_hpa < 50 || payload.top_pressure_hpa > 1000) {
    throw new Error("top_pressure_hpa must be between 50 and 1000.");
  }
  if (!withinActiveBounds(payload.lat1, payload.lon1) || !withinActiveBounds(payload.lat2, payload.lon2)) {
    throw new Error(`Both endpoints must stay inside the ${activeDomain().label} pressure-store bounds.`);
  }
  return payload;
}
function drawRouteLayer() {
  if (!routeLayer) return;
  routeLayer.clearLayers();
  routePoints.forEach((point, index) => {
    L.circleMarker(point, {
      radius: 6,
      color: index === 0 ? "#1f5f8b" : "#b42318",
      weight: 2,
      fillColor: "#fff",
      fillOpacity: 1
    }).addTo(routeLayer);
  });
  if (routePoints.length === 2) {
    L.polyline(routePoints, { color: "#172026", weight: 3, opacity: 0.9 }).addTo(routeLayer);
  }
}
function resetRoute() {
  stopLoopPlayback();
  routePoints = [];
  setCoordinateInputs();
  document.getElementById("custom-cross-sections").innerHTML = "";
  if (routeLayer) routeLayer.clearLayers();
  setRouteState("Click two points in the map.");
}
function applyDomainToMap() {
  const domain = activeDomain();
  document.getElementById("spacing").value = String(domain.defaultSpacingKm);
  resetRoute();
  if (!routeMap || !window.L) return;
  if (boundsLayer) {
    routeMap.removeLayer(boundsLayer);
    boundsLayer = null;
  }
  const b = domain.bounds;
  const bounds = [[b.south, b.west], [b.north, b.east]];
  routeMap.fitBounds(bounds, { padding: [12, 12] });
  boundsLayer = L.rectangle(bounds, { color: domain === PRESSURE_DOMAINS.conus ? "#0f7a4f" : "#1f5f8b", weight: 1, fillOpacity: 0.04 }).addTo(routeMap);
  setRouteState(`Click two points inside the ${domain.label} pressure-store bounds.`);
}
function initRouteMap() {
  if (!window.L) {
    document.getElementById("route-map").textContent = "Map library did not load. Coordinate inputs still work.";
    return;
  }
  routeMap = L.map("route-map", { preferCanvas: true, zoomControl: true });
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 12,
    attribution: "OpenStreetMap"
  }).addTo(routeMap);
  routeLayer = L.layerGroup().addTo(routeMap);
  applyDomainToMap();
  routeMap.on("click", event => {
    const lat = event.latlng.lat;
    const lon = event.latlng.lng;
    if (!withinActiveBounds(lat, lon)) {
      setRouteState(`Point is outside the local ${activeDomain().label} pressure-store bounds.`);
      return;
    }
    if (routePoints.length >= 2) {
      routePoints = [];
      document.getElementById("custom-cross-sections").innerHTML = "";
    }
    routePoints.push([lat, lon]);
    setCoordinateInputs();
    drawRouteLayer();
    if (routePoints.length === 1) {
      setRouteState(`Start set: ${lat.toFixed(4)}, ${lon.toFixed(4)}`);
    } else {
      setRouteState("Route set. Rendering all cross-section products.");
      renderCustomRoute().catch(err => setRouteState(err.message));
    }
  });
}
async function renderCustomRoute() {
  stopLoopPlayback();
  const serial = ++renderSerial;
  const payload = routePayloadFromInputs();
  routePoints = [[payload.lat1, payload.lon1], [payload.lat2, payload.lon2]];
  drawRouteLayer();
  setRouteState("Rendering the wxsection parity product set from available VolumeStore fields...");
  document.getElementById("custom-cross-sections").innerHTML = "";
  const browserStart = performance.now();
  const response = await fetch("/api/render-cross-section", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await response.json();
  if (serial !== renderSerial) return;
  if (!response.ok || !data.ok) {
    throw new Error(data.error || `render failed with HTTP ${response.status}`);
  }
  renderCustomResults(data, Math.round(performance.now() - browserStart));
}
function renderCustomResults(data, browserMs) {
  const records = data.records || [];
  const skipped = data.skipped || [];
  setRouteState([
    `Rendered ${records.length} ${data.domain_label || ""} products from the pressure VolumeStore to ${data.request?.top_pressure_hpa || 100} hPa; skipped ${skipped.length} products with missing fields.`,
    `server ${data.server_elapsed_ms} ms | renderer ${data.renderer_total_ms} ms | browser ${browserMs} ms`,
    data.command
  ].join("\n"));
  const skippedHtml = skipped.length ? `
    <article class="xs skipped">
      <h3>Unavailable styles for this store</h3>
      <div class="muted">${skipped.map(item => `${esc(item.product_label)}: ${esc((item.missing_requirements || []).join(", "))}`).join("<br>")}</div>
    </article>` : "";
  document.getElementById("custom-cross-sections").innerHTML = records.map(item => `
    <article class="xs">
      <h3>${esc(item.product_label)} | ${esc(data.route_name)}</h3>
      <a href="${esc(item.url)}" target="_blank"><img src="${esc(item.url)}" alt="${esc(item.product_label)} custom cross-section"></a>
      <div class="muted">${esc(item.samples)} samples, ${esc(item.levels)} levels to ${esc(item.top_pressure_hpa || data.request?.top_pressure_hpa || 100)} hPa, terrain mask ${item.terrain_mask ? "on" : "off"}, ${esc(String(item.format || "png").toUpperCase())} ${esc(bytes(item.size_bytes))}, sample ${esc(item.sample_ms)} ms, terrain ${esc(item.terrain_ms || 0)} ms, product ${esc(item.product_ms)} ms, render ${esc(item.render_ms)} ms, total ${esc(item.total_ms)} ms</div>
    </article>`).join("") + skippedHtml;
}
async function renderLoopRoute() {
  stopLoopPlayback();
  const serial = ++renderSerial;
  const payload = routePayloadFromInputs();
  payload.product = document.getElementById("loop-product").value || "wind_speed";
  payload.hours = "all";
  routePoints = [[payload.lat1, payload.lon1], [payload.lat2, payload.lon2]];
  drawRouteLayer();
  setRouteState(`Rendering ${productLabel(payload.product)} WebP loop from all available VolumeStore hours...`);
  document.getElementById("custom-cross-sections").innerHTML = "";
  const browserStart = performance.now();
  const response = await fetch("/api/render-cross-section-loop", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await response.json();
  if (serial !== renderSerial) return;
  if (!response.ok || !data.ok) {
    throw new Error(data.error || `loop render failed with HTTP ${response.status}`);
  }
  renderLoopResults(data, Math.round(performance.now() - browserStart));
}
function renderLoopResults(data, browserMs) {
  const frames = data.frames || [];
  const hours = frames.map(item => Number(item.hour)).filter(Number.isFinite);
  const coverage = hours.length ? `f${String(Math.min(...hours)).padStart(3, "0")}-f${String(Math.max(...hours)).padStart(3, "0")}` : "none";
  setRouteState([
    `Rendered ${frames.length} ${String((frames[0] || {}).format || "webp").toUpperCase()} ${productLabel(data.request?.product || "wind_speed")} loop frames from the pressure VolumeStore (${coverage}) to ${data.request?.top_pressure_hpa || 100} hPa.`,
    `server ${data.server_elapsed_ms} ms | renderer ${data.renderer_total_ms} ms | browser ${browserMs} ms`,
    data.command
  ].join("\n"));
  if (!frames.length) {
    document.getElementById("custom-cross-sections").innerHTML = `<article class="xs"><h3>No loop frames</h3><div class="muted">Renderer completed without writing frames.</div></article>`;
    return;
  }
  document.getElementById("custom-cross-sections").innerHTML = `
    <article class="xs" style="grid-column:1/-1">
      <h3>${esc(productLabel(data.request?.product || "wind_speed"))} WebP loop | ${esc(data.route_name)}</h3>
      <div class="loop-stage">
        <img id="loop-frame" src="${esc(frames[0].url)}" alt="cross-section loop frame">
        <div class="toolbar">
          <button id="loop-play">Pause</button>
          <input id="loop-slider" class="slider" type="range" min="0" max="${frames.length - 1}" value="0">
          <span id="loop-label" class="muted">f${String(frames[0].hour || 0).padStart(3, "0")} | ${esc(bytes(frames[0].size_bytes))}</span>
        </div>
        <div class="muted">${esc(frames.length)} frames to ${esc(data.request?.top_pressure_hpa || 100)} hPa, terrain mask ${frames.some(item => item.terrain_mask) ? "on" : "off"}, rendered ${esc(data.renderer_total_ms)} ms, served ${esc(data.server_elapsed_ms)} ms</div>
        <div class="frame-strip">${frames.map((item, index) => `<img data-frame="${index}" class="${index === 0 ? "active" : ""}" src="${esc(item.url)}" alt="f${String(item.hour || 0).padStart(3, "0")}">`).join("")}</div>
      </div>
    </article>`;
  let index = 0;
  let playing = true;
  const image = document.getElementById("loop-frame");
  const slider = document.getElementById("loop-slider");
  const label = document.getElementById("loop-label");
  const play = document.getElementById("loop-play");
  const thumbs = [...document.querySelectorAll("[data-frame]")];
  function showFrame(next) {
    index = (next + frames.length) % frames.length;
    const frame = frames[index];
    image.src = frame.url;
    slider.value = String(index);
    label.textContent = `f${String(frame.hour || 0).padStart(3, "0")} | ${String(frame.format || "webp").toUpperCase()} ${bytes(frame.size_bytes)}`;
    thumbs.forEach((thumb, thumbIndex) => thumb.classList.toggle("active", thumbIndex === index));
  }
  loopTimer = setInterval(() => {
    if (playing) showFrame(index + 1);
  }, 350);
  play.addEventListener("click", () => {
    playing = !playing;
    play.textContent = playing ? "Pause" : "Play";
  });
  slider.addEventListener("input", () => {
    showFrame(Number(slider.value || 0));
  });
  thumbs.forEach(thumb => thumb.addEventListener("click", () => showFrame(Number(thumb.dataset.frame || 0))));
}
function drawSection(canvas, values) {
  const ctx = canvas.getContext("2d");
  const w = canvas.width = canvas.clientWidth * (window.devicePixelRatio || 1);
  const h = canvas.height = canvas.clientHeight * (window.devicePixelRatio || 1);
  const valid = values.filter(v => Number.isFinite(v.value));
  ctx.fillStyle = "#f8fafb"; ctx.fillRect(0,0,w,h);
  if (!valid.length) return;
  const samples = Math.max(...valid.map(v => v.sample_index)) + 1;
  const levels = [...new Set(valid.map(v => v.level_hpa))].sort((a,b) => b-a);
  const min = Math.min(...valid.map(v => v.value));
  const max = Math.max(...valid.map(v => v.value));
  const lookup = new Map(valid.map(v => [`${v.sample_index}:${v.level_hpa}`, v.value]));
  for (let x=0; x<samples; x++) {
    for (let y=0; y<levels.length; y++) {
      const value = lookup.get(`${x}:${levels[y]}`);
      if (!Number.isFinite(value)) continue;
      const t = (value - min) / Math.max(1e-6, max - min);
      const r = Math.round(33 + 212*t);
      const g = Math.round(88 + 90*(1-Math.abs(t-.5)*2));
      const b = Math.round(150 - 95*t);
      ctx.fillStyle = `rgb(${r},${g},${b})`;
      ctx.fillRect(x*w/samples, y*h/levels.length, Math.ceil(w/samples)+1, Math.ceil(h/levels.length)+1);
    }
  }
}
function render(data) {
  const meta = data.health.pressure_metadata.ok ? data.health.pressure_metadata.value : null;
  const staticRecords = data.static_plots.records.filter(r => r.format === "webp" || r.format === "png");
  const uniqueMaps = new Map();
  for (const r of staticRecords) {
    const key = `${r.forecast_hour}:${r.product}`;
    if (!uniqueMaps.has(key) || r.format === "webp") uniqueMaps.set(key, r);
  }
  const crossOk = data.cross_sections.filter(x => x.ok);
  const renderedXs = data.volume_cross_section_images?.records || [];
  const profileOk = data.pressure_profiles.filter(x => x.ok);
  const metOk = data.meteograms.filter(x => x.ok);
  document.getElementById("runline").textContent = meta ? `${meta.cycle} | pressure f${meta.forecast_hours.join(", f")} | ${meta.variables.join(", ")}` : "pressure metadata unavailable";
  document.getElementById("metrics").innerHTML = [
    metric("Pressure store cycle", meta ? meta.cycle : "unavailable", meta ? "ok" : "bad"),
    metric("Pressure coverage", meta ? `${meta.forecast_hours.length} hour, ${meta.levels_hpa.length} levels` : "none"),
    metric("Static map products rendered", `${uniqueMaps.size}`),
    metric("Rendered cross-section PNGs", `${renderedXs.length}`, renderedXs.length ? "ok" : "warn"),
    metric("Cross-section JSON checks", `${crossOk.length}/${data.cross_sections.length}`, crossOk.length === data.cross_sections.length ? "ok" : "warn"),
    metric("Profiles retrieved", `${profileOk.length}/${data.pressure_profiles.length}`, "ok"),
    metric("Meteograms retrieved", `${metOk.length}/${data.meteograms.length}`, metOk.length === data.meteograms.length ? "ok" : "warn"),
    metric("CA Fire API", data.health.cafire_health.ok ? "ready" : "unavailable", data.health.cafire_health.ok ? "ok" : "bad"),
    metric("Fast meteogram store", data.health.warm_status.ok ? (data.health.warm_status.value.fast_store?.status || "unknown") : "unknown", "warn")
  ].join("");

  document.getElementById("static-note").textContent = "Images below are generated local artifacts, served from proof/cafire_local_artifacts/proof_wall_8800.";
  document.getElementById("maps").innerHTML = [...uniqueMaps.values()].map(r => `
    <article class="card">
      <a href="${esc(r.url)}" target="_blank"><img src="${esc(r.url)}" alt="${esc(r.product)}"></a>
      <div class="meta"><strong>${esc(productLabel(r.product))}</strong><span>f${String(r.forecast_hour).padStart(3,"0")} ${esc(r.format.toUpperCase())} ${esc(bytes(r.size_bytes))}</span></div>
    </article>`).join("");
  document.getElementById("blockers").textContent = data.static_plots.blockers.length
    ? data.static_plots.blockers.map(b => `${b.hour} ${b.product}: ${b.reason}`).join("\n")
    : "No blockers recorded in rendered manifests.";

  document.getElementById("profiles").innerHTML = data.pressure_profiles.map(item => {
    const s = item.summary || {};
    return `<article class="metric"><strong>${esc(item.point.name)}</strong><span>${item.ok ? `${s.sample_count} values | ${s.sidecar_elapsed_ms} ms sidecar | ${s.http_ms} ms HTTP | ${s.levels} levels` : item.error}</span><div>${(s.variables || []).map(v => `<span class="pill">${esc(v)}</span>`).join("")}</div></article>`;
  }).join("");

  document.getElementById("xs-note").textContent = renderedXs.length
    ? "These PNGs are rendered from the pressure VolumeStore through rustwx-cross-section, with normal axes, color tables, legends, and wind overlay."
    : "No rendered cross-section PNG report found; falling back to raw JSON canvas checks.";
  const xsRoot = document.getElementById("cross-sections");
  if (renderedXs.length) {
    xsRoot.innerHTML = renderedXs.map(item => `
      <article class="xs">
        <h3>${esc(item.route_name)} | ${esc(item.product_label)}</h3>
        <a href="${esc(item.url)}" target="_blank"><img src="${esc(item.url)}" alt="${esc(item.route_name)} ${esc(item.product_label)}"></a>
        <div class="muted">${esc(item.samples)} samples, ${esc(item.levels)} levels, range ${Number(item.min_value).toFixed(2)} to ${Number(item.max_value).toFixed(2)}, render ${esc(item.render_ms)} ms</div>
      </article>`).join("");
  } else {
    xsRoot.innerHTML = data.cross_sections.map((item, index) => {
      const s = item.summary || {};
      return `<article class="xs"><h3>${esc(item.route.name)} | ${esc(item.variable)}</h3><canvas data-xs="${index}"></canvas><div class="muted">${item.ok ? `${s.route_samples} samples, ${s.value_count} values, ${s.sidecar_elapsed_ms} ms sidecar, ${s.proxy_total_ms} ms API` : item.error}</div></article>`;
    }).join("");
    data.cross_sections.forEach((item, index) => {
      const canvas = xsRoot.querySelector(`canvas[data-xs="${index}"]`);
      if (canvas && item.ok) drawSection(canvas, item.value.section.values || []);
    });
  }

  document.getElementById("meteogram-note").textContent = "These checks use explicit cached f000-f048 meteogram PNG artifacts so the page proves retrieval without a click or a 100-second resample.";
  document.getElementById("meteograms").innerHTML = data.meteograms.map(item => {
    const s = item.summary || {};
    return `<article class="card">${s.image_url ? `<a href="${esc(s.image_url)}" target="_blank"><img src="${esc(s.image_url)}" alt="${esc(item.request.name)}"></a>` : ""}<div class="meta"><strong>${esc(item.request.name)}</strong><span>${item.ok ? `cache=${s.cache_hit} | ${s.hours} hours | ${s.http_ms} ms HTTP | sample ${s.sample_total_ms} ms | render ${s.render_total_ms} ms` : item.error}</span></div></article>`;
  }).join("");
  document.getElementById("limits").textContent = data.known_limits.join("\n");
}
async function load() {
  document.getElementById("metrics").innerHTML = metric("Loading", "working");
  const response = await fetch("/api/proof", { cache: "no-store" });
  if (!response.ok) throw new Error(await response.text());
  render(await response.json());
}
document.getElementById("refresh").addEventListener("click", () => load().catch(err => alert(err.message)));
document.getElementById("clear-route").addEventListener("click", resetRoute);
document.getElementById("render-route").addEventListener("click", () => renderCustomRoute().catch(err => setRouteState(err.message)));
document.getElementById("render-loop").addEventListener("click", () => renderLoopRoute().catch(err => setRouteState(err.message)));
document.getElementById("domain").addEventListener("change", applyDomainToMap);
initRouteMap();
load().catch(err => {
  document.getElementById("metrics").innerHTML = metric("Load failed", err.message, "bad");
});
</script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the local CA Fire no-click proof wall")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PROOF_PORT)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), ProofWallHandler)
    print(f"proof wall: http://{args.host}:{args.port}/", flush=True)
    print(f"artifact root: {ARTIFACT_ROOT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
