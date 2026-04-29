from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from .batch import atomic_write_json
from .config import Settings


CROSS_SECTION_PRODUCTS: dict[str, str] = {
    "temperature": "Temperature",
    "wind_speed": "Wind Speed",
    "theta_e": "Theta-e",
    "rh": "Relative Humidity",
    "q": "Specific Humidity",
    "omega": "Vertical Motion",
    "vorticity": "Absolute Vorticity",
    "shear": "Deep-Layer Shear",
    "lapse_rate": "Lapse Rate",
    "cloud": "Cloud Water/Ice",
    "cloud_total": "Total Hydrometeors",
    "wetbulb": "Wet Bulb",
    "icing": "Icing",
    "frontogenesis": "Frontogenesis",
    "vpd": "Vapor Pressure Deficit",
    "dewpoint_dep": "Dewpoint Depression",
    "moisture_transport": "Moisture Transport",
    "pv": "Potential Vorticity",
    "fire_wx": "Fire Weather",
}


class CrossSectionArtifactService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._render_slots = threading.BoundedSemaphore(settings.pressure_cross_section_render_max_active)
        self._loop_slots = threading.BoundedSemaphore(settings.pressure_cross_section_loop_max_active)
        self._active_lock = threading.Lock()
        self._active_render_jobs = 0
        self._active_loop_jobs = 0
        self._manifest_cache_lock = threading.Lock()
        self._store_manifest_cache: tuple[Path, int, dict[str, Any]] | None = None

    def product_catalog(self) -> dict[str, Any]:
        return {
            "kind": "hrrr_pressure_cross_section_products",
            "schema_version": 1,
            "products": [
                {"product": product, "label": label}
                for product, label in CROSS_SECTION_PRODUCTS.items()
            ],
            "excluded": [
                {
                    "product": "smoke",
                    "label": "Smoke",
                    "reason": "not supported by the current HRRR pressure VolumeStore variable set",
                }
            ],
        }

    def status(self) -> dict[str, Any]:
        renderer = self._renderer_path()
        stores = self._available_stores()
        store_path = self._store_path()
        renderer_exists = renderer.exists()
        store_exists = bool(stores)
        status = "disabled"
        detail = None
        if self._settings.pressure_volume_enabled:
            if not renderer_exists:
                status = "unavailable"
                detail = f"renderer binary is not present: {renderer}"
            elif not store_exists:
                status = "unavailable"
                detail = "no pressure VolumeStore is present"
            else:
                status = "ready"
        with self._active_lock:
            active_render_jobs = self._active_render_jobs
            active_loop_jobs = self._active_loop_jobs
        return {
            "enabled": self._settings.pressure_volume_enabled,
            "status": status,
            "detail": detail,
            "renderer_path": str(renderer),
            "renderer_present": renderer_exists,
            "store_path": str(store_path),
            "store_present": store_exists,
            "stores": stores,
            "style_version": self._settings.pressure_cross_section_style_version,
            "render_slots": {
                "active": active_render_jobs,
                "max": self._settings.pressure_cross_section_render_max_active,
            },
            "loop_slots": {
                "active": active_loop_jobs,
                "max": self._settings.pressure_cross_section_loop_max_active,
            },
            "default_top_pressure_hpa": self._settings.pressure_cross_section_default_top_hpa,
            "default_dimensions": {
                "width": self._settings.pressure_cross_section_default_width,
                "height": self._settings.pressure_cross_section_default_height,
            },
            "product_count": len(CROSS_SECTION_PRODUCTS),
        }

    def render_still(self, request: dict[str, Any]) -> dict[str, Any]:
        products = self._normalize_products(request.get("products"))
        return self._render(request=request, products=products, loop=False)

    def render_loop(self, request: dict[str, Any]) -> dict[str, Any]:
        product = request.get("product") or "wind_speed"
        products = self._normalize_products(product)
        if len(products) != 1:
            raise HTTPException(status_code=400, detail="cross-section loops support exactly one product")
        return self._render(request=request, products=products, loop=True)

    def _render(self, *, request: dict[str, Any], products: list[str], loop: bool) -> dict[str, Any]:
        self._ensure_renderer_ready()
        normalized = self._normalized_request(request, products=products, loop=loop)
        store_path, store_summary = self._select_store(normalized, loop=loop)
        artifact_prefix, out_dir, route_id = self._artifact_location(normalized, store_summary)
        manifest_path = out_dir / "manifest.json"
        force = bool(request.get("force"))
        if manifest_path.exists() and not force:
            try:
                cached = json.loads(manifest_path.read_text(encoding="utf-8"))
                if cached.get("request_hash") == route_id and cached.get("style_version") == normalized["style_version"]:
                    cached["cache_hit"] = True
                    return cached
            except Exception:
                pass

        slots = self._loop_slots if loop else self._render_slots
        if not slots.acquire(blocking=False):
            kind = "loop" if loop else "render"
            raise HTTPException(status_code=429, detail=f"too many active cross-section {kind} jobs")
        with self._active_lock:
            if loop:
                self._active_loop_jobs += 1
            else:
                self._active_render_jobs += 1
        try:
            return self._run_renderer(
                normalized=normalized,
                store_path=store_path,
                store_summary=store_summary,
                artifact_prefix=artifact_prefix,
                out_dir=out_dir,
                route_id=route_id,
                loop=loop,
            )
        finally:
            with self._active_lock:
                if loop:
                    self._active_loop_jobs -= 1
                else:
                    self._active_render_jobs -= 1
            slots.release()

    def _run_renderer(
        self,
        *,
        normalized: dict[str, Any],
        store_path: Path,
        store_summary: dict[str, Any],
        artifact_prefix: str,
        out_dir: Path,
        route_id: str,
        loop: bool,
    ) -> dict[str, Any]:
        out_dir.mkdir(parents=True, exist_ok=True)
        renderer = self._renderer_path()
        command = [
            str(renderer),
            "--store",
            str(store_path),
            "--out-dir",
            str(out_dir),
            "--products",
            ",".join(normalized["products"]),
            "--spacing-km",
            str(normalized["spacing_km"]),
            "--top-pressure-hpa",
            str(normalized["top_pressure_hpa"]),
            "--width",
            str(normalized["width"]),
            "--height",
            str(normalized["height"]),
            "--route-id",
            route_id,
            "--route-name",
            normalized["route_name"],
            "--start-lat",
            str(normalized["lat1"]),
            "--start-lon",
            str(normalized["lon1"]),
            "--end-lat",
            str(normalized["lat2"]),
            "--end-lon",
            str(normalized["lon2"]),
        ]
        if loop:
            command.extend(["--hours", normalized["hours_spec"]])
            timeout = self._settings.pressure_cross_section_loop_timeout_sec
        else:
            command.extend(["--hour", str(normalized["hour"])])
            timeout = self._settings.pressure_cross_section_render_timeout_sec

        started = time.perf_counter()
        result = subprocess.run(
            command,
            cwd=str(Path.cwd()),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        server_elapsed_ms = int((time.perf_counter() - started) * 1000)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            if len(detail) > 2400:
                detail = detail[-2400:]
            raise HTTPException(
                status_code=502,
                detail=f"cross-section renderer exited {result.returncode}: {detail}",
            )

        report_path = out_dir / "volume_cross_section_render_report.json"
        if not report_path.exists():
            raise HTTPException(status_code=502, detail="cross-section renderer did not write a report")
        report = json.loads(report_path.read_text(encoding="utf-8"))
        generated_at = datetime.now(UTC).isoformat()
        local_file_records = self._index_renderer_files(out_dir, artifact_prefix)
        records = self._records_from_report(report, out_dir, artifact_prefix, local_file_records)
        manifest = self._manifest_payload(
            normalized=normalized,
            store_summary=store_summary,
            artifact_prefix=artifact_prefix,
            route_id=route_id,
            report=report,
            records=records,
            generated_at=generated_at,
            loop=loop,
            server_elapsed_ms=server_elapsed_ms,
            local_file_count=len(local_file_records),
        )
        manifest_path = out_dir / "manifest.json"
        atomic_write_json(manifest_path, manifest)
        manifest_key = f"{artifact_prefix}/manifest.json"
        manifest["manifest_key"] = manifest_key
        manifest["manifest_url"] = self._public_url(manifest_key, None)
        manifest["cache_hit"] = False
        atomic_write_json(manifest_path, manifest)
        return manifest

    def _manifest_payload(
        self,
        *,
        normalized: dict[str, Any],
        store_summary: dict[str, Any],
        artifact_prefix: str,
        route_id: str,
        report: dict[str, Any],
        records: list[dict[str, Any]],
        generated_at: str,
        loop: bool,
        server_elapsed_ms: int,
        local_file_count: int,
    ) -> dict[str, Any]:
        forecast_hours = report.get("forecast_hours") or []
        kind = "hrrr_cross_section_loop" if loop else "hrrr_cross_section_render"
        route = {
            "id": route_id,
            "name": normalized["route_name"],
            "start": {"lat": normalized["lat1"], "lon": normalized["lon1"]},
            "end": {"lat": normalized["lat2"], "lon": normalized["lon2"]},
        }
        return {
            "kind": kind,
            "schema_version": 1,
            "generated_at_utc": generated_at,
            "model": store_summary.get("model") or "hrrr",
            "domain": store_summary.get("domain") or "california",
            "store_cycle": store_summary.get("cycle"),
            "date_yyyymmdd": store_summary.get("date_yyyymmdd"),
            "cycle_utc": store_summary.get("cycle_utc"),
            "store_path": str(store_summary.get("store_path") or self._store_path()),
            "store_kind": store_summary.get("store_kind"),
            "artifact_prefix": artifact_prefix,
            "artifact_serving": "local_api",
            "public_base_url": None,
            "artifact_base_url": "/artifacts",
            "request_hash": route_id,
            "style_version": normalized["style_version"],
            "route": route,
            "products": normalized["products"],
            "product_labels": {
                product: CROSS_SECTION_PRODUCTS.get(product, product)
                for product in normalized["products"]
            },
            "forecast_hours": forecast_hours,
            "spacing_km": normalized["spacing_km"],
            "top_pressure_hpa": normalized["top_pressure_hpa"],
            "width": normalized["width"],
            "height": normalized["height"],
            "loop": loop,
            "loop_kind": "webp_frames" if loop else None,
            "frame_count": len({record["hour"] for record in records}) if loop else None,
            "rendered_count": report.get("rendered_count"),
            "skipped_count": report.get("skipped_count"),
            "skipped": report.get("skipped") or [],
            "renderer_total_ms": report.get("total_ms"),
            "server_elapsed_ms": server_elapsed_ms,
            "upload_elapsed_ms": 0,
            "upload_count": 0,
            "local_file_count": local_file_count,
            "records": records,
            "frames": records if loop else [],
            "renderer_report": f"{artifact_prefix}/volume_cross_section_render_report.json",
            "store_summary": {
                "forecast_hours": store_summary.get("forecast_hours"),
                "variables": store_summary.get("variables"),
                "levels_hpa": store_summary.get("levels_hpa"),
                "grid": store_summary.get("grid"),
            },
        }

    def _records_from_report(
        self,
        report: dict[str, Any],
        out_dir: Path,
        artifact_prefix: str,
        upload_records: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        records = []
        for output in report.get("outputs") or []:
            png_key = self._key_for_output_path(output.get("png_path"), out_dir, artifact_prefix)
            webp_key = self._key_for_output_path(output.get("webp_path"), out_dir, artifact_prefix)
            summary_key = self._key_for_output_path(output.get("summary_path"), out_dir, artifact_prefix)
            png_upload = upload_records.get(png_key, {})
            webp_upload = upload_records.get(webp_key, {})
            summary_upload = upload_records.get(summary_key, {})
            records.append(
                {
                    "route_id": output.get("route_id"),
                    "route_name": output.get("route_name"),
                    "hour": output.get("hour"),
                    "product": output.get("product"),
                    "product_label": output.get("product_label"),
                    "png_key": png_key,
                    "png_url": self._public_url(png_key, png_upload.get("url")),
                    "png_size_bytes": png_upload.get("size_bytes"),
                    "webp_key": webp_key,
                    "webp_url": self._public_url(webp_key, webp_upload.get("url")),
                    "webp_size_bytes": webp_upload.get("size_bytes"),
                    "summary_key": summary_key,
                    "summary_url": self._public_url(summary_key, summary_upload.get("url")),
                    "samples": output.get("samples"),
                    "levels": output.get("levels"),
                    "values": output.get("values"),
                    "min_value": output.get("min_value"),
                    "max_value": output.get("max_value"),
                    "sample_ms": output.get("sample_ms"),
                    "terrain_ms": output.get("terrain_ms"),
                    "product_ms": output.get("product_ms"),
                    "render_ms": output.get("render_ms"),
                    "total_ms": output.get("total_ms"),
                    "top_pressure_hpa": output.get("top_pressure_hpa"),
                }
            )
        return sorted(records, key=lambda item: (int(item.get("hour") or 0), str(item.get("product") or "")))

    def _index_renderer_files(
        self,
        out_dir: Path,
        artifact_prefix: str,
    ) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        for path in sorted(out_dir.iterdir()):
            if not path.is_file() or path.name == "manifest.json":
                continue
            key = f"{artifact_prefix}/{path.name}"
            records[key] = {
                "key": key,
                "url": self._public_url(key, None),
                "format": path.suffix.lower().lstrip("."),
                "size_bytes": path.stat().st_size,
            }
        return records

    def _key_for_output_path(self, value: object, out_dir: Path, artifact_prefix: str) -> str:
        if not isinstance(value, str) or not value:
            return ""
        path = Path(value)
        try:
            rel = path.relative_to(out_dir)
        except ValueError:
            rel = Path(path.name)
        return f"{artifact_prefix}/{rel.as_posix()}"

    def _artifact_location(
        self,
        normalized: dict[str, Any],
        store_summary: dict[str, Any],
    ) -> tuple[str, Path, str]:
        route_id = hashlib.sha256(
            json.dumps(
                {
                    "request": normalized,
                    "store_cycle": store_summary.get("cycle"),
                    "store_model": store_summary.get("model"),
                    "store_domain": store_summary.get("domain"),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:20]
        date_key = str(store_summary.get("date_yyyymmdd") or "unknown")
        cycle_hour = store_summary.get("cycle_utc")
        cycle_key = f"{int(cycle_hour):02d}Z" if isinstance(cycle_hour, int) else "unknownZ"
        artifact_prefix = f"cross-sections/hrrr/{date_key}/{cycle_key}/{route_id}"
        return artifact_prefix, self._settings.artifact_root / artifact_prefix, route_id

    def _normalized_request(
        self,
        request: dict[str, Any],
        *,
        products: list[str],
        loop: bool,
    ) -> dict[str, Any]:
        spacing = float(request.get("spacing_km") or 5.0)
        if not self._settings.pressure_cross_section_min_spacing_km <= spacing <= self._settings.pressure_cross_section_max_spacing_km:
            raise HTTPException(
                status_code=400,
                detail=(
                    "spacing_km must be between "
                    f"{self._settings.pressure_cross_section_min_spacing_km} and "
                    f"{self._settings.pressure_cross_section_max_spacing_km}"
                ),
            )
        top_pressure = float(
            request.get("top_pressure_hpa") or self._settings.pressure_cross_section_default_top_hpa
        )
        if top_pressure < 10.0 or top_pressure > 1000.0:
            raise HTTPException(status_code=400, detail="top_pressure_hpa must be between 10 and 1000")
        width = int(request.get("width") or self._settings.pressure_cross_section_default_width)
        height = int(request.get("height") or self._settings.pressure_cross_section_default_height)
        if width < 600 or width > 2600 or height < 420 or height > 1800:
            raise HTTPException(status_code=400, detail="width/height are outside supported bounds")
        route_name = str(request.get("route_name") or "Custom CA Cross-Section").strip()[:96]
        if not route_name:
            route_name = "Custom CA Cross-Section"
        normalized: dict[str, Any] = {
            "lat1": round(float(request["lat1"]), 5),
            "lon1": round(float(request["lon1"]), 5),
            "lat2": round(float(request["lat2"]), 5),
            "lon2": round(float(request["lon2"]), 5),
            "route_name": route_name,
            "products": products,
            "spacing_km": spacing,
            "top_pressure_hpa": top_pressure,
            "width": width,
            "height": height,
            "style_version": self._settings.pressure_cross_section_style_version,
        }
        if loop:
            normalized["hours_spec"] = self._normalize_hours_spec(request.get("hours"))
        else:
            normalized["hour"] = self._normalize_hour(request.get("hour"))
        if normalized["lat1"] == normalized["lat2"] and normalized["lon1"] == normalized["lon2"]:
            raise HTTPException(status_code=400, detail="cross-section endpoints must be different")
        return normalized

    def _normalize_products(self, raw: object) -> list[str]:
        if raw is None:
            return ["wind_speed"]
        if isinstance(raw, str):
            tokens = [item.strip() for item in raw.split(",") if item.strip()]
        elif isinstance(raw, list):
            tokens = [str(item).strip() for item in raw if str(item).strip()]
        else:
            raise HTTPException(status_code=400, detail="products must be a string or list")
        if not tokens:
            return ["wind_speed"]
        if len(tokens) == 1 and tokens[0].lower() in {"all", "wxsection"}:
            return list(CROSS_SECTION_PRODUCTS.keys())
        normalized = []
        for token in tokens:
            product = token.lower()
            if product == "smoke":
                raise HTTPException(status_code=400, detail="smoke is not available in this pressure VolumeStore")
            if product not in CROSS_SECTION_PRODUCTS:
                raise HTTPException(status_code=400, detail=f"unknown cross-section product: {token}")
            normalized.append(product)
        return sorted(set(normalized), key=normalized.index)

    def _normalize_hour(self, raw: object) -> int:
        hour = int(raw if raw is not None else 0)
        if hour < 0 or hour > 48:
            raise HTTPException(status_code=400, detail="hour must be between 0 and 48")
        return hour

    def _normalize_hours_spec(self, raw: object) -> str:
        if raw is None:
            return "all"
        if isinstance(raw, str):
            value = raw.strip() or "all"
            if value.lower() == "all":
                return "all"
            hours = self._parse_hour_tokens(value)
            return ",".join(str(hour) for hour in hours)
        if isinstance(raw, list):
            hours = sorted({self._normalize_hour(hour) for hour in raw})
            if not hours:
                raise HTTPException(status_code=400, detail="hours list cannot be empty")
            return ",".join(str(hour) for hour in hours)
        raise HTTPException(status_code=400, detail="hours must be all, a range string, or a list")

    def _parse_hour_tokens(self, value: str) -> list[int]:
        hours: set[int] = set()
        for part in value.split(","):
            token = part.strip()
            if not token:
                continue
            if "-" in token:
                start_raw, end_raw = token.split("-", 1)
                start = self._normalize_hour(start_raw.strip().lstrip("fF"))
                end = self._normalize_hour(end_raw.strip().lstrip("fF"))
                if end < start:
                    raise HTTPException(status_code=400, detail=f"invalid hour range: {part}")
                hours.update(range(start, end + 1))
            else:
                hours.add(self._normalize_hour(token.lstrip("fF")))
        if not hours:
            raise HTTPException(status_code=400, detail="no valid hours requested")
        return sorted(hours)

    def _ensure_renderer_ready(self) -> None:
        if not self._settings.pressure_volume_enabled:
            raise HTTPException(status_code=503, detail="pressure VolumeStore rendering is disabled")
        renderer = self._renderer_path()
        if not renderer.exists():
            raise HTTPException(status_code=503, detail=f"renderer binary is not present: {renderer}")

    def _select_store(self, normalized: dict[str, Any], *, loop: bool) -> tuple[Path, dict[str, Any]]:
        requested = self._requested_hours(normalized, loop=loop)
        candidates: list[tuple[Path, dict[str, Any]]] = []
        errors = []
        for store_path in self._candidate_store_paths():
            try:
                summary = self._store_summary(store_path)
            except HTTPException as exc:
                errors.append(str(exc.detail))
                continue
            available = {int(hour) for hour in summary.get("forecast_hours") or []}
            if requested is not None and not requested.issubset(available):
                continue
            if not available:
                continue
            candidates.append((store_path, summary))
        if not candidates:
            if requested:
                hours = ",".join(f"f{hour:03d}" for hour in sorted(requested))
                raise HTTPException(
                    status_code=503,
                    detail=f"no pressure VolumeStore currently contains requested hour(s): {hours}",
                )
            detail = "; ".join(errors) if errors else "no pressure VolumeStore is present"
            raise HTTPException(status_code=503, detail=detail)
        candidates.sort(key=lambda item: self._store_sort_key(item[1]), reverse=True)
        return candidates[0]

    def _requested_hours(self, normalized: dict[str, Any], *, loop: bool) -> set[int] | None:
        if not loop:
            return {int(normalized["hour"])}
        spec = str(normalized.get("hours_spec") or "all")
        if spec.lower() == "all":
            return None
        return set(self._parse_hour_tokens(spec))

    def _store_sort_key(self, summary: dict[str, Any]) -> tuple[str, int, int, int]:
        forecast_hours = [int(hour) for hour in summary.get("forecast_hours") or []]
        return (
            str(summary.get("date_yyyymmdd") or ""),
            int(summary.get("cycle_utc") or -1),
            max(forecast_hours) if forecast_hours else -1,
            len(forecast_hours),
        )

    def _store_summary(self, store_path: Path | None = None) -> dict[str, Any]:
        store_path = store_path or self._store_path()
        manifest_path = store_path / "manifest.json"
        try:
            mtime_ns = manifest_path.stat().st_mtime_ns
        except FileNotFoundError as exc:
            raise HTTPException(status_code=503, detail=f"pressure VolumeStore manifest is not present: {manifest_path}") from exc
        with self._manifest_cache_lock:
            cached = self._store_manifest_cache
            if cached and cached[0] == manifest_path and cached[1] == mtime_ns:
                return dict(cached[2])
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            cycle = data.get("cycle")
            date_yyyymmdd, cycle_utc = self._cycle_fields(cycle)
            summary = {
                "model": data.get("model"),
                "domain": data.get("domain"),
                "cycle": cycle,
                "date_yyyymmdd": date_yyyymmdd,
                "cycle_utc": cycle_utc,
                "store_path": str(store_path),
                "store_kind": self._store_kind(store_path),
                "forecast_hours": data.get("forecast_hours") or [],
                "variables": [item.get("name") for item in data.get("variables") or [] if item.get("name")],
                "levels_hpa": data.get("levels_hpa") or [],
                "grid": {
                    "kind": (data.get("grid") or {}).get("kind"),
                    "nx": (data.get("grid") or {}).get("nx"),
                    "ny": (data.get("grid") or {}).get("ny"),
                },
            }
            self._store_manifest_cache = (manifest_path, mtime_ns, summary)
            return dict(summary)

    def _cycle_fields(self, cycle: object) -> tuple[str | None, int | None]:
        if not isinstance(cycle, str) or not cycle:
            return None, None
        try:
            parsed = datetime.fromisoformat(cycle.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None, None
        return parsed.strftime("%Y%m%d"), parsed.hour

    def _public_url(self, key: str, uploaded_url: object | None) -> str:
        if isinstance(uploaded_url, str) and uploaded_url:
            return uploaded_url
        return f"/artifacts/{key}"

    def _renderer_path(self) -> Path:
        configured = self._settings.pressure_volume_renderer_path
        if configured.exists():
            return configured
        local_name = "volume_store_cross_section_render.exe" if os.name == "nt" else "volume_store_cross_section_render"
        candidates = [
            Path.cwd() / "target" / "release" / local_name,
            Path(__file__).resolve().parents[3] / "target" / "release" / local_name,
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return configured

    def _candidate_store_paths(self) -> list[Path]:
        seen: set[str] = set()
        paths = [
            self._resolve_store_path(self._settings.pressure_volume_partial_store_path),
            self._resolve_store_path(self._settings.pressure_volume_store_path),
        ]
        candidates = []
        for path in paths:
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
        return candidates

    def _available_stores(self) -> list[dict[str, Any]]:
        stores = []
        for path in self._candidate_store_paths():
            try:
                summary = self._store_summary(path)
            except HTTPException:
                continue
            stores.append(
                {
                    "store_kind": summary.get("store_kind"),
                    "store_path": str(path),
                    "cycle": summary.get("cycle"),
                    "date_yyyymmdd": summary.get("date_yyyymmdd"),
                    "cycle_utc": summary.get("cycle_utc"),
                    "forecast_hours": summary.get("forecast_hours"),
                    "max_forecast_hour": max(summary.get("forecast_hours") or [-1]),
                }
            )
        return sorted(stores, key=lambda item: (str(item.get("date_yyyymmdd") or ""), int(item.get("cycle_utc") or -1)), reverse=True)

    def _store_path(self) -> Path:
        return self._resolve_store_path(self._settings.pressure_volume_store_path)

    def _resolve_store_path(self, configured: Path) -> Path:
        if configured.is_absolute() or configured.exists():
            return configured
        repo_candidate = Path(__file__).resolve().parents[3] / configured
        if repo_candidate.exists():
            return repo_candidate
        return configured

    def _store_kind(self, store_path: Path) -> str:
        partial = self._resolve_store_path(self._settings.pressure_volume_partial_store_path)
        if store_path == partial:
            return "partial"
        return "synoptic"
