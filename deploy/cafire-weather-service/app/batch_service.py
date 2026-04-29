from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from .batch import atomic_write_json, render_run, should_update_latest_pointer
from .cache_cleanup import run_cache_cleanup
from .config import settings
from .rustwx_client import available_forecast_hours
from .storage import ArtifactStore


EXTENDED_HRRR_CYCLES = {0, 6, 12, 18}
SMOKE_PRODUCTS = {"smoke_pm25_native", "smoke_column"}
DIURNAL_PRODUCTS = {
    "2m_temp_0_24h_range",
    "2m_temp_24_48h_range",
    "2m_temp_0_48h_range",
}
DIURNAL_HOURS = [24, 48]
DIURNAL_RENDER_MANIFEST_NAME = "diurnal-render-manifest.json"
HOUR_ZERO_BLOCKED_PRODUCTS = {"qpf_1h", "10m_wind_1h_max"}
_LAST_CACHE_CLEANUP_MONOTONIC = 0.0


def _run_prefix(date_yyyymmdd: str, cycle_utc: int) -> str:
    return f"hrrr/runs/{date_yyyymmdd}/{cycle_utc:02d}Z"


def _read_json(path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _manifest_for_run(date_yyyymmdd: str, cycle_utc: int) -> dict[str, Any] | None:
    return _read_json(settings.artifact_root / _run_prefix(date_yyyymmdd, cycle_utc) / "manifest.json")


def _diurnal_render_manifest_for_run(date_yyyymmdd: str, cycle_utc: int) -> dict[str, Any] | None:
    return _read_json(settings.artifact_root / _run_prefix(date_yyyymmdd, cycle_utc) / DIURNAL_RENDER_MANIFEST_NAME)


def _rendered_hours(manifest: dict[str, Any] | None) -> set[int]:
    if not manifest:
        return set()
    return {
        int(hour["forecast_hour"])
        for hour in manifest.get("hours", [])
        if isinstance(hour, dict) and "forecast_hour" in hour
    }


def _manifest_matches_current_config(manifest: dict[str, Any] | None) -> bool:
    if not manifest:
        return False
    return (
        manifest.get("domain") == settings.default_domain
        and manifest.get("products") == _hourly_manifest_products()
        and manifest.get("width") == settings.default_width
        and manifest.get("height") == settings.default_height
        and manifest.get("brand_text") == settings.static_map_brand_text
    )


def _recent_runs(now: datetime) -> list[tuple[str, int]]:
    runs: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for offset in range(settings.static_map_worker_cycle_lookback_hours + 1):
        cycle_time = now - timedelta(hours=offset)
        key = (cycle_time.strftime("%Y%m%d"), cycle_time.hour)
        if key not in seen:
            seen.add(key)
            runs.append(key)
    return runs


def _expected_horizon(cycle_utc: int) -> int:
    return 48 if cycle_utc in EXTENDED_HRRR_CYCLES else 18


def _hour_csv(hours: list[int]) -> str:
    return ",".join(str(hour) for hour in hours)


def _core_products() -> list[str]:
    return [
        product
        for product in settings.default_products
        if product not in SMOKE_PRODUCTS and product not in DIURNAL_PRODUCTS
    ]


def _smoke_products() -> list[str]:
    return [product for product in settings.default_products if product in SMOKE_PRODUCTS]


def _diurnal_products() -> list[str]:
    return [product for product in settings.default_products if product in DIURNAL_PRODUCTS]


def _hourly_manifest_products() -> list[str]:
    return [product for product in settings.default_products if product not in DIURNAL_PRODUCTS]


def _expected_core_products_for_hour(hour: int) -> list[str]:
    return [
        product
        for product in _core_products()
        if hour > 0 or product not in HOUR_ZERO_BLOCKED_PRODUCTS
    ]


def _available_hours_for_product(date_yyyymmdd: str, cycle_utc: int, horizon: int, product: str) -> list[int]:
    return [
        hour
        for hour in available_forecast_hours(
            settings,
            date_yyyymmdd,
            cycle_utc,
            product=product,
            source=settings.default_source,
        )
        if 0 <= hour <= horizon
    ]


def _available_hours_for_cycle(date_yyyymmdd: str, cycle_utc: int, horizon: int) -> dict[str, list[int]]:
    by_product: dict[str, list[int]] = {}
    if _core_products():
        by_product["sfc"] = _available_hours_for_product(date_yyyymmdd, cycle_utc, horizon, "sfc")
    if _smoke_products():
        by_product["nat"] = _available_hours_for_product(date_yyyymmdd, cycle_utc, horizon, "nat")
    return by_product


def _smoke_candidate_hours(core_available: list[int], smoke_available: list[int]) -> list[int]:
    interval = settings.static_map_smoke_interval_hours
    available = set(core_available).intersection(smoke_available)
    return sorted(hour for hour in available if hour % interval == 0)


def _hour_dir(date_yyyymmdd: str, cycle_utc: int, forecast_hour: int):
    return (
        settings.artifact_root
        / _run_prefix(date_yyyymmdd, cycle_utc)
        / f"f{forecast_hour:03d}"
        / settings.default_domain
    )


def _product_png_exists(date_yyyymmdd: str, cycle_utc: int, forecast_hour: int, product: str) -> bool:
    hour_dir = _hour_dir(date_yyyymmdd, cycle_utc, forecast_hour)
    return any(hour_dir.glob(f"*_{product}.png"))


def _missing_product_hours(
    date_yyyymmdd: str,
    cycle_utc: int,
    hours: list[int],
    products: list[str],
) -> list[int]:
    if not products:
        return []
    return [
        hour
        for hour in hours
        if not all(_product_png_exists(date_yyyymmdd, cycle_utc, hour, product) for product in products)
    ]


def _missing_core_hours(date_yyyymmdd: str, cycle_utc: int, hours: list[int]) -> list[int]:
    return [
        hour
        for hour in hours
        if not all(
            _product_png_exists(date_yyyymmdd, cycle_utc, hour, product)
            for product in _expected_core_products_for_hour(hour)
        )
    ]


def _expected_diurnal_products_for_hour(hour: int) -> list[str]:
    products = _diurnal_products()
    if hour == 24:
        return [product for product in products if product == "2m_temp_0_24h_range"]
    if hour == 48:
        return products
    return []


def _missing_diurnal_hours(date_yyyymmdd: str, cycle_utc: int, hours: list[int]) -> list[int]:
    return [
        hour
        for hour in hours
        if not all(
            _product_png_exists(date_yyyymmdd, cycle_utc, hour, product)
            for product in _expected_diurnal_products_for_hour(hour)
        )
    ]


def _product_from_upload_key(key: str) -> str:
    leaf = key.rsplit("/", 1)[-1]
    for suffix in [".png", ".webp"]:
        if leaf.endswith(suffix):
            leaf = leaf.removesuffix(suffix)
            break
    marker = f"_{settings.default_domain}_"
    if marker in leaf:
        return leaf.split(marker, 1)[1]
    return leaf.rsplit("_", 1)[-1]


def _write_diurnal_manifest(date_yyyymmdd: str, cycle_utc: int, available_hours: list[int]) -> dict[str, Any] | None:
    manifest = _diurnal_render_manifest_for_run(date_yyyymmdd, cycle_utc) or _manifest_for_run(date_yyyymmdd, cycle_utc)
    products = _diurnal_products()
    if not manifest or not products:
        return None

    wanted_products = set(products)
    filtered_hours = []
    for hour_report in manifest.get("hours", []):
        hour = int(hour_report.get("forecast_hour", -1))
        if hour not in DIURNAL_HOURS:
            continue
        uploads = [
            upload
            for upload in hour_report.get("uploaded", [])
            if str(upload.get("key", "")).endswith((".png", ".webp"))
            and _product_from_upload_key(str(upload.get("key", ""))) in wanted_products
        ]
        if not uploads:
            continue
        filtered = dict(hour_report)
        filtered["uploaded"] = uploads
        filtered_hours.append(filtered)

    if not filtered_hours:
        return None

    run_prefix = _run_prefix(date_yyyymmdd, cycle_utc)
    forecast_hours = [int(item["forecast_hour"]) for item in filtered_hours]
    generated_at = datetime.now(UTC).isoformat()
    diurnal_manifest = {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "model": settings.default_model,
        "source": settings.default_source,
        "date_yyyymmdd": date_yyyymmdd,
        "cycle_utc": cycle_utc,
        "domain": settings.default_domain,
        "products": products,
        "forecast_hours": forecast_hours,
        "expected_forecast_hours": DIURNAL_HOURS,
        "available_forecast_hours": [hour for hour in DIURNAL_HOURS if hour in available_hours],
        "width": settings.default_width,
        "height": settings.default_height,
        "place_label_density": manifest.get("place_label_density", "major"),
        "brand_text": settings.static_map_brand_text,
        "webp_enabled": settings.static_map_webp_enabled,
        "webp_quality": settings.static_map_webp_quality,
        "artifact_prefix": run_prefix,
        "public_base_url": settings.public_artifact_base_url,
        "hours": filtered_hours,
    }

    root = settings.artifact_root / run_prefix
    manifest_path = root / "diurnal-manifest.json"
    latest_path = settings.artifact_root / "hrrr" / "latest-diurnal.json"
    pointer = {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "model": settings.default_model,
        "source": settings.default_source,
        "date_yyyymmdd": date_yyyymmdd,
        "cycle_utc": cycle_utc,
        "domain": settings.default_domain,
        "products": products,
        "forecast_hours": forecast_hours,
        "expected_forecast_hours": DIURNAL_HOURS,
        "available_forecast_hours": diurnal_manifest["available_forecast_hours"],
        "width": settings.default_width,
        "height": settings.default_height,
        "place_label_density": diurnal_manifest["place_label_density"],
        "brand_text": settings.static_map_brand_text,
        "webp_enabled": settings.static_map_webp_enabled,
        "webp_quality": settings.static_map_webp_quality,
        "manifest_key": f"{run_prefix}/diurnal-manifest.json",
        "manifest_url": (
            f"{settings.public_artifact_base_url.rstrip('/')}/{run_prefix}/diurnal-manifest.json"
            if settings.public_artifact_base_url
            else None
        ),
    }

    atomic_write_json(manifest_path, diurnal_manifest)
    store = ArtifactStore(settings)
    if store.enabled():
        store.upload_file(manifest_path, f"{run_prefix}/diurnal-manifest.json", immutable=False)
    if should_update_latest_pointer(latest_path, date_yyyymmdd, cycle_utc):
        atomic_write_json(latest_path, pointer)
        if store.enabled():
            store.upload_file(latest_path, "hrrr/latest-diurnal.json", immutable=False)
    return pointer


def _render_product_group(
    *,
    date_yyyymmdd: str,
    cycle_utc: int,
    horizon: int,
    available_hours: list[int],
    hours: list[int],
    products: list[str],
    force: bool,
    manifest_products: list[str] | None = None,
    update_latest: bool = True,
    manifest_name: str = "manifest.json",
    latest_name: str = "latest.json",
) -> dict[str, Any] | None:
    if not hours or not products:
        return None
    args = argparse.Namespace(
        date_yyyymmdd=date_yyyymmdd,
        cycle_utc=cycle_utc,
        source=settings.default_source,
        domain=settings.default_domain,
        hours=_hour_csv(hours),
        expected_hours=_hour_csv(list(range(horizon + 1))),
        available_hours=_hour_csv(available_hours),
        products=",".join(products),
        manifest_products=",".join(manifest_products or products),
        manifest_name=manifest_name,
        latest_name=latest_name,
        width=settings.default_width,
        height=settings.default_height,
        place_label_density="major",
        parallelism=settings.static_map_worker_parallelism,
        force=force,
        update_latest=update_latest,
    )
    return render_run(args)


def _limit_backfill(hours: list[int], priority: bool) -> list[int]:
    if priority:
        return hours
    return hours[: settings.static_map_backfill_batch_hours]


def _run_once_for_cycle(date_yyyymmdd: str, cycle_utc: int, *, priority: bool) -> dict[str, Any]:
    horizon = _expected_horizon(cycle_utc)
    availability_by_product = _available_hours_for_cycle(date_yyyymmdd, cycle_utc, horizon)
    core_available = availability_by_product.get("sfc", [])
    smoke_available = availability_by_product.get("nat", [])
    core_products = _core_products()
    smoke_products = _smoke_products()
    hourly_manifest_products = _hourly_manifest_products()
    manifest = _manifest_for_run(date_yyyymmdd, cycle_utc)
    config_current = _manifest_matches_current_config(manifest)
    rendered = _rendered_hours(manifest) if config_current else set()
    missing_core = (
        _missing_core_hours(date_yyyymmdd, cycle_utc, core_available)
        if config_current
        else [hour for hour in core_available if hour not in rendered]
    )
    smoke_hours = _smoke_candidate_hours(core_available, smoke_available)
    missing_smoke = _missing_product_hours(date_yyyymmdd, cycle_utc, smoke_hours, smoke_products)
    missing_core = _limit_backfill(missing_core, priority)
    missing_smoke = _limit_backfill(missing_smoke, priority)

    if not missing_core and not missing_smoke:
        return {
            "date_yyyymmdd": date_yyyymmdd,
            "cycle_utc": cycle_utc,
            "priority": priority,
            "horizon": horizon,
            "available_count": len(core_available),
            "availability_by_product": availability_by_product,
            "rendered_count": len(rendered),
            "missing_core": [],
            "missing_smoke": [],
            "skipped": True,
            "reason": "no newly available forecast hours",
        }

    core_payload = _render_product_group(
        date_yyyymmdd=date_yyyymmdd,
        cycle_utc=cycle_utc,
        horizon=horizon,
        available_hours=core_available,
        hours=missing_core,
        products=core_products,
        force=True,
        manifest_products=hourly_manifest_products,
    )
    smoke_payload = _render_product_group(
        date_yyyymmdd=date_yyyymmdd,
        cycle_utc=cycle_utc,
        horizon=horizon,
        available_hours=core_available,
        hours=missing_smoke,
        products=smoke_products,
        force=True,
        manifest_products=hourly_manifest_products,
    )
    rendered_payload = smoke_payload or core_payload or {}
    return {
        "date_yyyymmdd": date_yyyymmdd,
        "cycle_utc": cycle_utc,
        "priority": priority,
        "horizon": horizon,
        "available_count": len(core_available),
        "availability_by_product": availability_by_product,
        "rendered_hours": sorted(set(missing_core + missing_smoke)),
        "rendered_core_hours": missing_core,
        "rendered_smoke_hours": missing_smoke,
        "smoke_interval_hours": settings.static_map_smoke_interval_hours,
        "force": True,
        "manifest_url": rendered_payload.get("manifest_url"),
    }


def _recent_extended_runs(now: datetime) -> list[tuple[str, int]]:
    runs: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for offset in range(max(settings.static_map_worker_cycle_lookback_hours, 30) + 1):
        cycle_time = now - timedelta(hours=offset)
        if cycle_time.hour not in EXTENDED_HRRR_CYCLES:
            continue
        key = (cycle_time.strftime("%Y%m%d"), cycle_time.hour)
        if key not in seen:
            seen.add(key)
            runs.append(key)
    return runs


def _run_once_for_diurnal(date_yyyymmdd: str, cycle_utc: int) -> dict[str, Any]:
    products = _diurnal_products()
    if not products:
        return {
            "date_yyyymmdd": date_yyyymmdd,
            "cycle_utc": cycle_utc,
            "skipped": True,
            "reason": "no diurnal products configured",
        }
    horizon = _expected_horizon(cycle_utc)
    core_available = _available_hours_for_product(date_yyyymmdd, cycle_utc, horizon, "sfc")
    target_hours = [hour for hour in DIURNAL_HOURS if hour in core_available]
    if not target_hours:
        return {
            "date_yyyymmdd": date_yyyymmdd,
            "cycle_utc": cycle_utc,
            "horizon": horizon,
            "available_count": len(core_available),
            "target_hours": [],
            "skipped": True,
            "reason": "f024/f048 are not available yet",
        }

    missing_hours = _missing_diurnal_hours(date_yyyymmdd, cycle_utc, target_hours)
    if missing_hours:
        _render_product_group(
            date_yyyymmdd=date_yyyymmdd,
            cycle_utc=cycle_utc,
            horizon=horizon,
            available_hours=core_available,
            hours=missing_hours,
            products=products,
            force=True,
            manifest_products=products,
            update_latest=False,
            manifest_name=DIURNAL_RENDER_MANIFEST_NAME,
        )
    pointer = _write_diurnal_manifest(date_yyyymmdd, cycle_utc, core_available)
    return {
        "date_yyyymmdd": date_yyyymmdd,
        "cycle_utc": cycle_utc,
        "horizon": horizon,
        "available_count": len(core_available),
        "target_hours": target_hours,
        "rendered_hours": missing_hours,
        "manifest_url": pointer.get("manifest_url") if pointer else None,
        "skipped": not missing_hours,
        "reason": "diurnal products already present" if not missing_hours else None,
    }


def _run_diurnal_lane(now: datetime) -> dict[str, Any]:
    results = []
    for date_yyyymmdd, cycle_utc in _recent_extended_runs(now):
        result = _run_once_for_diurnal(date_yyyymmdd, cycle_utc)
        results.append(result)
        if result.get("manifest_url"):
            return {"ok": True, "results": results, "selected": result}
    return {"ok": False, "results": results, "selected": None}


def run_once() -> dict[str, Any]:
    global _LAST_CACHE_CLEANUP_MONOTONIC
    checked_at = datetime.now(UTC)
    diurnal = _run_diurnal_lane(checked_at)
    results = []
    for index, (date_yyyymmdd, cycle_utc) in enumerate(_recent_runs(checked_at)):
        try:
            result = _run_once_for_cycle(date_yyyymmdd, cycle_utc, priority=index < 2)
            results.append(result)
            if index >= 2 and result.get("rendered_hours"):
                break
        except Exception as exc:  # pragma: no cover - live NOAA/ops path
            results.append(
                {
                    "date_yyyymmdd": date_yyyymmdd,
                    "cycle_utc": cycle_utc,
                    "ok": False,
                    "error": str(exc),
                }
            )
    rendered = [item for item in results if item.get("rendered_hours")]
    cache_cleanup = None
    if settings.cache_cleanup_enabled:
        monotonic_now = time.monotonic()
        if monotonic_now - _LAST_CACHE_CLEANUP_MONOTONIC >= settings.cache_cleanup_interval_sec:
            _LAST_CACHE_CLEANUP_MONOTONIC = monotonic_now
            cache_cleanup = run_cache_cleanup(settings)
    return {
        "ok": True,
        "mode": "all_hrrr_inits_incremental",
        "source": settings.default_source,
        "checked_at_utc": checked_at.isoformat(),
        "lookback_hours": settings.static_map_worker_cycle_lookback_hours,
        "backfill_batch_hours": settings.static_map_backfill_batch_hours,
        "diurnal": diurnal,
        "cache_cleanup": cache_cleanup,
        "rendered_run_count": len(rendered),
        "results": results,
    }


def main() -> None:
    settings.ensure_dirs()
    if not settings.static_map_worker_enabled:
        print("static map worker disabled", flush=True)
        return
    print("starting static map worker", flush=True)
    while True:
        try:
            result = run_once()
            print(json.dumps(result, indent=2), flush=True)
        except Exception as exc:  # pragma: no cover - operational worker
            print(json.dumps({"ok": False, "error": str(exc), "at_utc": datetime.now(UTC).isoformat()}), flush=True)
        time.sleep(settings.static_map_worker_interval_sec)


if __name__ == "__main__":
    main()
