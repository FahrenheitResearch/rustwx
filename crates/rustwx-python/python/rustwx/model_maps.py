"""Focused local web UI for RustWx model maps.

This is intentionally narrower than RustWx Studio: model-map generation,
cache warming, optional WxStore warming, pressure-store warming, and click
soundings. It remains a local no-AI app and reuses the Studio backend paths
that already wrap RustWx/WxStore binaries.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shutil
import threading
import time
import uuid
import webbrowser
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import rustwx

from .studio import (
    DEFAULT_HOST,
    StudioEnv,
    StudioHandler,
    _inventory_section,
    _inventory_store_dirs,
    _inventory_wxstore_runs,
    _has_wxprofile_store_binary,
    _iso_from_ts,
    _job_context,
    _job_request_summary,
    _job_snapshot,
    _raise_if_job_cancelled,
    _domain_by_slug,
    _load_json,
    _parse_run_string,
    _trim_jobs_locked,
)


APP_TITLE = "RustWx Model Maps"
DEFAULT_PORT = 8777
MODEL_MAP_JOB_KINDS = {
    "render",
    "localize_run",
    "prepare_data",
    "sounding",
    "pressure_store",
    "wxstore",
    "wxstore_plot_existing",
}
MODEL_MAP_HEAVY_JOB_KINDS = {
    "render",
    "localize_run",
    "prepare_data",
    "pressure_store",
    "wxstore",
    "wxstore_plot_existing",
}
GLOBAL_MODEL_IDS = {"gfs", "gdas", "gefs", "aigfs", "aigefs", "hgefs", "ecmwf-open-data", "aifs"}
MODEL_NATIVE_BOUNDS = {
    "hrrr": [-127.0, -66.0, 23.0, 51.5],
    "rap": [-135.0, -60.0, 15.0, 60.0],
    "nam": [-135.0, -55.0, 12.0, 62.0],
    "hiresw": [-127.0, -66.0, 23.0, 51.5],
    "href": [-127.0, -66.0, 23.0, 51.5],
    "sref": [-127.0, -66.0, 23.0, 51.5],
    "rtma": [-127.0, -66.0, 23.0, 51.5],
    "urma": [-127.0, -66.0, 23.0, 51.5],
    "nbm": [-127.0, -66.0, 23.0, 51.5],
    "rrfs-a": [-127.0, -66.0, 23.0, 51.5],
    "rrfs-public": [-127.0, -66.0, 23.0, 51.5],
    "refs": [-127.0, -66.0, 23.0, 51.5],
    "rrfs-firewx": [-127.0, -66.0, 23.0, 51.5],
}
MODEL_NATIVE_DOMAINS = {model: "conus" for model in MODEL_NATIVE_BOUNDS}


def run_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rustwx-model-maps",
        description="Run the focused local RustWx model-map viewer.",
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser tab.")
    parser.add_argument("--out-root", default=None, help="Artifact output root.")
    parser.add_argument("--cache-dir", default=None, help="Shared data cache root.")
    parser.add_argument("--bin-dir", default=None, help="Directory containing optional rustwx binaries.")
    args = parser.parse_args(argv)

    env = StudioEnv.from_args(args)
    server = ModelMapServer((args.host, args.port), env)
    actual_host, actual_port = server.server_address[:2]
    url = f"http://{actual_host}:{actual_port}/"
    print(f"{APP_TITLE} running at {url}", flush=True)
    print(f"rustwx: {env.version}", flush=True)
    print(f"outputs: {env.out_root}", flush=True)
    print(f"cache: {env.cache_dir}", flush=True)
    if not args.no_open:
        threading.Timer(0.2, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()
    return 0


class ModelMapServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], env: StudioEnv):
        super().__init__(server_address, ModelMapHandler)
        self.env = env
        self.started_at = time.time()
        self.allowed_file_roots = {
            env.out_root,
            env.cache_dir,
            (Path.cwd() / "rustwx_outputs").resolve(),
        }
        self.jobs: dict[str, dict] = {}
        self.jobs_lock = threading.Lock()
        self.heavy_job_lock = threading.Lock()
        self.pressure_store_lock = threading.Lock()
        self.wxstore_process = None
        self.wxstore_lock = threading.Lock()
        self.latest_runs_cache: dict[str, tuple[float, dict]] = {}
        self.latest_runs_lock = threading.Lock()
        self.latest_runs_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=8,
            thread_name_prefix="rustwx-latest-runs",
        )


def _update_current_job_progress(label: str, *, current: int, total: int, detail: str | None = None) -> None:
    server, job_id = _job_context()
    if server is None or not job_id:
        return
    total = max(1, int(total))
    current = max(0, min(int(current), total))
    entry = {
        "time": _iso_from_ts(time.time()),
        "label": label,
        "detail": detail,
        "current": current,
        "total": total,
    }
    with server.jobs_lock:
        job = server.jobs.get(job_id)
        if not job:
            return
        job["progress"] = {
            "label": label,
            "detail": detail,
            "current": current,
            "total": total,
            "percent": round((current / total) * 100.0, 1),
        }
        log = list(job.get("log") or [])
        log.append(entry)
        job["log"] = log[-80:]
        job["updated_at_ts"] = time.time()
        job["updated_at"] = _iso_from_ts(float(job["updated_at_ts"]))


def _publish_current_job_result(result: dict) -> None:
    server, job_id = _job_context()
    if server is None or not job_id:
        return
    with server.jobs_lock:
        job = server.jobs.get(job_id)
        if not job:
            return
        job["result"] = result
        job["updated_at_ts"] = time.time()
        job["updated_at"] = _iso_from_ts(float(job["updated_at_ts"]))


RESOURCE_PRESET_LIMITS = {
    "max": {"jobs": 8, "download_workers": 8, "load_parallelism": 8, "memory_mode": "high"},
    "balanced": {"jobs": 4, "download_workers": 4, "load_parallelism": 4, "memory_mode": "balanced"},
    "light": {"jobs": 2, "download_workers": 3, "load_parallelism": 2, "memory_mode": "low"},
}


def _bounded_int(value: object, fallback: int, *, low: int = 1, high: int = 8) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = fallback
    return max(low, min(high, number))


def _apply_resource_limits(payload: dict) -> dict:
    preset_id = str(payload.get("resource_preset") or "balanced").lower()
    preset = RESOURCE_PRESET_LIMITS.get(preset_id, RESOURCE_PRESET_LIMITS["balanced"])
    max_jobs = int(preset["jobs"])
    return {
        **payload,
        "resource_preset": preset_id if preset_id in RESOURCE_PRESET_LIMITS else "balanced",
        "memory_mode": str(payload.get("memory_mode") or preset["memory_mode"]),
        "jobs": _bounded_int(payload.get("jobs"), max_jobs, high=max_jobs),
        "download_workers": _bounded_int(payload.get("download_workers"), int(preset["download_workers"]), high=8),
        "load_parallelism": _bounded_int(payload.get("load_parallelism"), int(preset["load_parallelism"]), high=max_jobs),
    }


def _set_current_job_resource_waiting(waiting: bool) -> None:
    server, job_id = _job_context()
    if server is None or not job_id:
        return
    with server.jobs_lock:
        job = server.jobs.get(job_id)
        if not job:
            return
        job["resource_waiting"] = waiting
        job["updated_at_ts"] = time.time()
        job["updated_at"] = _iso_from_ts(float(job["updated_at_ts"]))


class ModelMapHandler(StudioHandler):
    server: ModelMapServer

    def log_message(self, fmt: str, *args) -> None:
        print(f"[rustwx-model-maps] {self.address_string()} {fmt % args}")

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._send_html(INDEX_HTML)
            elif parsed.path == "/api/bootstrap":
                self._send_json(self._bootstrap())
            elif parsed.path == "/api/products":
                query = parse_qs(parsed.query)
                self._send_json(self._products(_query_one(query, "model", "hrrr")))
            elif parsed.path == "/api/latest-runs":
                query = parse_qs(parsed.query)
                self._send_json(self._latest_runs(
                    _query_one(query, "model", None),
                    _query_one(query, "source", "aws,nomads"),
                    _query_one(query, "timeout", None),
                ))
            elif parsed.path == "/api/domains":
                query = parse_qs(parsed.query)
                self._send_json(self._domains(_query_one(query, "kind", None), _query_one(query, "search", None)))
            elif parsed.path == "/api/data-inventory":
                self._send_json(self._data_inventory())
            elif parsed.path == "/api/jobs":
                self._send_json(self._jobs())
            elif parsed.path.startswith("/api/jobs/"):
                self._send_json(self._job(parsed.path.rsplit("/", 1)[-1]))
            elif parsed.path == "/api/file":
                self._send_file(parse_qs(parsed.query))
            else:
                self._send_json({"ok": False, "error": "not found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            payload = self._read_json()
            if parsed.path == "/api/render":
                self._send_json(self._render_maps(payload))
            elif parsed.path == "/api/prepare-data":
                self._send_json(self._prepare_model_data(payload))
            elif parsed.path == "/api/sounding":
                self._send_json(self._render_sounding(payload))
            elif parsed.path == "/api/pressure-store":
                self._send_json(self._prepare_pressure_store(payload))
            elif parsed.path == "/api/wxstore":
                self._send_json(self._run_wxstore_pipeline(payload))
            elif parsed.path == "/api/wxstore-existing":
                self._send_json(self._plot_existing_wxstore(payload))
            elif parsed.path == "/api/jobs":
                self._send_json(self._start_job(payload))
            elif parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/cancel"):
                job_id = parsed.path.strip("/").split("/")[2]
                self._send_json(self._cancel_job(job_id))
            elif parsed.path == "/api/data-delete":
                self._send_json(self._delete_data(payload))
            else:
                self._send_json({"ok": False, "error": "not found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _bootstrap(self) -> dict:
        env = self.server.env
        return {
            "ok": True,
            "app": APP_TITLE,
            "version": env.version,
            "plot_style": os.environ.get("RUSTWX_PLOT_STYLE", "operational_fast"),
            "uptime_s": round(time.time() - self.server.started_at, 1),
            "doctor": self._doctor(),
            "models": self._models(),
            "domains": self._domains(None, None),
            "sounding_sample_methods": [
                {"slug": "nearest", "label": "Nearest"},
                {"slug": "inverse-distance4", "label": "Inverse Distance"},
                {"slug": "box-mean", "label": "Box Mean"},
            ],
            "jobs": self._jobs(limit=5),
        }

    def _models(self) -> dict:
        models = []
        for model in self.server.env.capabilities.get("models") or []:
            model_id = str(model.get("id") or "")
            sources = model.get("sources") or []
            source_ids = _sources_for_model(model_id, sources)
            models.append({
                "id": model_id,
                "description": model.get("description"),
                "default_product": model.get("default_product"),
                "default_render_product": model.get("default_render_product"),
                "max_forecast_hour": model.get("max_forecast_hour"),
                "cycle_hours_utc": model.get("cycle_hours_utc") or [],
                "runtime_family": model.get("runtime_family"),
                "ensemble_mode": model.get("ensemble_mode"),
                "sources": _source_metadata(model_id, sources, source_ids),
                "default_source": _default_source_for_model(model_id, sources),
                "archive_source": _archive_source_for_model(model_id, sources),
                "direct_recipe_count": len(model.get("direct_recipes") or []),
                "light_derived_recipe_count": len(model.get("light_derived_recipes") or []),
                "heavy_derived_recipe_count": len(model.get("heavy_derived_recipes") or []),
                "windowed_recipe_count": len(model.get("windowed_products") or []),
            })
        return {
            "agent_api": self.server.env.capabilities.get("agent_api"),
            "rustwx_version": self.server.env.version,
            "count": len(models),
            "models": models,
        }

    def _latest_runs(
        self,
        model_id: str | None = None,
        source_filter: str | None = "aws,nomads",
        timeout_raw: str | None = None,
    ) -> dict:
        started = time.time()
        cache_ttl_s = 120
        try:
            timeout_s = float(timeout_raw) if timeout_raw not in {None, ""} else 8.0
        except (TypeError, ValueError):
            timeout_s = 8.0
        timeout_s = max(1.0, min(30.0, timeout_s))
        cache_key = f"{model_id or '*'}|{source_filter or ''}".lower()
        with self.server.latest_runs_lock:
            cached = self.server.latest_runs_cache.get(cache_key)
            if cached and time.time() - cached[0] < cache_ttl_s:
                payload = json.loads(json.dumps(cached[1]))
                payload["cached"] = True
                payload["ui_elapsed_s"] = round(time.time() - started, 2)
                payload["cache_age_s"] = round(time.time() - cached[0], 1)
                return payload

        today = datetime.now(UTC).strftime("%Y%m%d")
        yesterday = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y%m%d")
        requested_sources = _latest_source_filter(source_filter)
        models = [
            model for model in self.server.env.capabilities.get("models") or []
            if (not model_id or str(model.get("id") or "") == model_id)
        ]
        planned: list[tuple[int, int, dict, str, str]] = []
        for model in models:
            model_index = len({item[0] for item in planned})
            mid = str(model.get("id") or "")
            if not mid:
                continue
            sources = model.get("sources") or []
            source_ids = _sources_for_model(mid, sources)
            if requested_sources is None:
                selected_sources = source_ids
            else:
                selected_sources = [source for source in source_ids if source in requested_sources]
            if not selected_sources and not requested_sources:
                selected_sources = [_default_source_for_model(mid, sources)]
            for source_index, source in enumerate(selected_sources):
                planned.append((model_index, source_index, model, mid, source))

        futures = {
            self.server.latest_runs_executor.submit(
                _latest_source_row,
                mid,
                source,
                model.get("cycle_hours_utc") or [],
                today,
                yesterday,
            ): (model_index, source_index, model, mid, source)
            for model_index, source_index, model, mid, source in planned
        }
        source_results: dict[tuple[int, int], dict] = {}
        timed_out = False
        deadline = started + timeout_s
        try:
            for future in concurrent.futures.as_completed(futures, timeout=timeout_s):
                model_index, source_index, _model, _mid, _source = futures[future]
                try:
                    source_results[(model_index, source_index)] = future.result()
                except Exception as exc:  # pragma: no cover - source availability is external
                    source_results[(model_index, source_index)] = {
                        "source": _source,
                        "latest": None,
                        "runs": [],
                        "error": str(exc),
                    }
                if time.time() >= deadline:
                    break
        except concurrent.futures.TimeoutError:
            timed_out = True
        for future, (model_index, source_index, _model, _mid, source) in futures.items():
            if not future.done():
                timed_out = True
                future.cancel()
                source_results[(model_index, source_index)] = {
                    "source": source,
                    "latest": None,
                    "runs": [],
                    "error": f"Timed out after {timeout_s:g}s checking latest cycle.",
                    "timeout": True,
                }

        rows = []
        by_model: dict[int, list[tuple[int, dict, str]]] = {}
        model_by_index: dict[int, dict] = {}
        for model_index, source_index, model, mid, source in planned:
            model_by_index[model_index] = model
            by_model.setdefault(model_index, []).append((
                source_index,
                source_results.get((model_index, source_index), {
                    "source": source,
                    "latest": None,
                    "runs": [],
                    "error": "No result.",
                }),
                mid,
            ))
        for model_index in sorted(by_model):
            model = model_by_index[model_index]
            source_rows = [row for _source_index, row, _mid in sorted(by_model[model_index], key=lambda item: item[0])]
            first_source = source_rows[0] if source_rows else {}
            mid = str(model.get("id") or "")
            sources = model.get("sources") or []
            rows.append({
                "model": mid,
                "label": mid.upper(),
                "description": model.get("description"),
                "source": first_source.get("source") or _default_source_for_model(mid, sources),
                "max_forecast_hour": model.get("max_forecast_hour"),
                "cycle_hours_utc": model.get("cycle_hours_utc") or [],
                "sources": source_rows,
                "latest": first_source.get("latest"),
                "runs": first_source.get("runs") or [],
                "error": first_source.get("error") if not (first_source.get("runs") or []) else None,
            })
        payload = {
            "ok": True,
            "date_checked": today,
            "source_filter": source_filter or "default",
            "cache_ttl_s": cache_ttl_s,
            "cached": False,
            "partial": timed_out,
            "timeout_s": timeout_s,
            "models": rows,
            "ui_elapsed_s": round(time.time() - started, 2),
        }
        if not timed_out:
            with self.server.latest_runs_lock:
                self.server.latest_runs_cache[cache_key] = (time.time(), payload)
        return payload

    def _doctor(self) -> dict:
        env = self.server.env
        binaries = env.binaries
        wanted = [
            "sounding_plot",
            "hrrr_pressure_volume_store",
            "volume_store_sounding_render",
            "model_wxprofile_store",
            "wxprofile_sounding_render",
            "rustwx_grid_export",
            "wxstore",
            "wxstore_wxa_showcase",
        ]
        return {
            "rustwx_version": env.version,
            "agent_api": env.capabilities.get("agent_api"),
            "plot_style": os.environ.get("RUSTWX_PLOT_STYLE", "operational_fast"),
            "cache_dir": str(env.cache_dir),
            "out_root": str(env.out_root),
            "models": [m.get("id") for m in env.capabilities.get("models") or []],
            "domain_count": (env.capabilities.get("domains") or {}).get("count"),
            "optional_binaries": {name: str(binaries[name]) for name in wanted if name in binaries},
            "capabilities": {
                "render_maps": hasattr(rustwx, "render_maps_json"),
                "prepare_model_data": hasattr(rustwx, "prepare_model_data_json"),
                "grib_soundings": "sounding_plot" in binaries,
                "fast_soundings": (
                    (
                        _has_wxprofile_store_binary(binaries)
                        and "wxprofile_sounding_render" in binaries
                    )
                    or (
                        "hrrr_pressure_volume_store" in binaries
                        and "volume_store_sounding_render" in binaries
                    )
                ),
                "wxprofile_soundings": (
                    _has_wxprofile_store_binary(binaries)
                    and "wxprofile_sounding_render" in binaries
                ),
                "wxstore_export": "rustwx_grid_export" in binaries,
                "wxstore_direct_wxa": "rustwx_grid_export" in binaries,
                "wxstore_import": "wxstore" in binaries,
                "wxstore_plot": "wxstore_wxa_showcase" in binaries,
            },
        }

    def _data_inventory(self) -> dict:
        env = self.server.env
        out_root = env.out_root
        cache_root = env.cache_dir
        sections = []
        for section_id, label, path, category in self._data_section_defs():
            sections.append(_inventory_section(section_id, label, path, category))
        sections = [section for section in sections if section.get("exists")]
        stores = [
            *_inventory_wxstore_runs(cache_root / "studio_wxstore_spatial"),
            *_inventory_store_dirs(cache_root / "studio_wxprofile_stores", "wxprofile_store"),
            *_inventory_store_dirs(cache_root / "studio_pressure_stores", "pressure_store"),
        ]
        for store in stores:
            if store.get("path") and not store.get("bytes"):
                store["bytes"] = _dir_bytes(Path(str(store["path"])))
        recent = sorted(
            [item for section in sections for item in section.get("recent", [])],
            key=lambda item: item.get("mtime_ts") or 0,
            reverse=True,
        )[:40]
        total_bytes = sum(int(section.get("bytes") or 0) for section in sections)
        return {
            "ok": True,
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "roots": {
                "out_root": str(env.out_root),
                "cache_dir": str(cache_root),
            },
            "summary": {
                "section_count": len(sections),
                "store_count": len(stores),
                "recent_count": len(recent),
                "bytes": total_bytes,
            },
            "sections": sections,
            "stores": stores,
            "recent": recent,
        }

    def _data_section_defs(self) -> list[tuple[str, str, Path, str]]:
        env = self.server.env
        out_root = env.out_root
        cache_root = env.cache_dir
        sections = [
            ("model_maps", "Model Maps", out_root / "model_maps" / "maps", "outputs"),
            ("studio_maps", "Studio Maps", out_root / "studio" / "maps", "outputs"),
            ("soundings", "Soundings", out_root / "studio" / "soundings", "outputs"),
            ("wxstore_outputs", "WxStore Outputs", out_root / "studio" / "wxstore", "outputs"),
            ("wxstore_existing", "WxStore Plots", out_root / "studio" / "wxstore_existing", "outputs"),
            ("wxprofile_stores", "WxProfile Stores", cache_root / "studio_wxprofile_stores", "cache"),
            ("pressure_stores", "Pressure Stores", cache_root / "studio_pressure_stores", "cache"),
            ("wxstore_spatial", "WxStore Spatial", cache_root / "studio_wxstore_spatial", "cache"),
        ]
        reserved = {
            "studio_pressure_stores",
            "studio_wxprofile_stores",
            "studio_wxstore_spatial",
            "studio_volume_stores",
            "studio_radar_tiles",
            "studio_satellite_tiles",
            "radar",
            "satellite",
            "soundings",
            "cross_sections",
            "plot_lab",
        }
        model_ids = {str(item.get("id") or "").lower() for item in self.server.env.capabilities.get("models") or []}
        if cache_root.is_dir():
            for child in sorted(path for path in cache_root.iterdir() if path.is_dir()):
                name = child.name.lower()
                if name in reserved:
                    continue
                label = f"{child.name.upper()} GRIB Cache" if name in model_ids else f"{child.name} Cache"
                sections.append((f"cache_{name}", label, child, "cache"))
        return sections

    def _delete_data(self, payload: dict) -> dict:
        target = self._resolve_delete_target(payload)
        if not target.get("ok"):
            return target
        path = Path(str(target["path"]))
        before = _dir_bytes(path)
        if not path.exists():
            return {"ok": True, "deleted": False, "path": str(path), "bytes_deleted": 0}
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        inventory = self._data_inventory()
        return {
            "ok": True,
            "deleted": True,
            "target": target.get("target"),
            "label": target.get("label"),
            "path": str(path),
            "bytes_deleted": before,
            "inventory": inventory,
        }

    def _resolve_delete_target(self, payload: dict) -> dict:
        inventory = self._data_inventory()
        target_type = str(payload.get("target_type") or "").strip()
        target_id = str(payload.get("id") or "").strip()
        requested_path = str(payload.get("path") or "").strip()
        allowed: list[dict] = []
        for section in inventory.get("sections") or []:
            allowed.append({
                "target": "section",
                "id": str(section.get("id") or ""),
                "label": str(section.get("label") or section.get("id") or "Data"),
                "path": str(section.get("path") or ""),
            })
        for store in inventory.get("stores") or []:
            allowed.append({
                "target": "store",
                "id": str(store.get("path") or ""),
                "label": _store_label(store),
                "path": str(store.get("path") or ""),
            })
        match = None
        for item in allowed:
            if target_type and item["target"] != target_type:
                continue
            if target_id and item["id"] == target_id:
                match = item
                break
            if requested_path and _same_path(item["path"], requested_path):
                match = item
                break
        if not match:
            return {"ok": False, "error": "Delete target is not in the current RustWx data inventory."}
        path = Path(match["path"]).resolve()
        roots = [self.server.env.out_root.resolve(), self.server.env.cache_dir.resolve()]
        if any(path == root for root in roots):
            return {"ok": False, "error": "Refusing to delete a configured RustWx output/cache root."}
        if not any(_path_is_relative_to(path, root) for root in roots):
            return {"ok": False, "error": "Refusing to delete data outside the configured RustWx output/cache roots."}
        if path.anchor == str(path):
            return {"ok": False, "error": "Refusing to delete a filesystem root."}
        return {**match, "ok": True, "path": str(path)}

    def _start_job(self, payload: dict) -> dict:
        kind = str(payload.get("kind") or "").strip()
        job_payload = payload.get("payload") or {}
        if not isinstance(job_payload, dict):
            return {"ok": False, "error": "Job payload must be an object."}
        if kind not in MODEL_MAP_JOB_KINDS:
            return {"ok": False, "error": f"Unknown model-map job kind {kind!r}."}
        now = time.time()
        job_id = uuid.uuid4().hex[:12]
        job = {
            "id": job_id,
            "kind": kind,
            "status": "queued",
            "created_at_ts": now,
            "updated_at_ts": now,
            "started_at_ts": None,
            "finished_at_ts": None,
            "created_at": _iso_from_ts(now),
            "updated_at": _iso_from_ts(now),
            "request": _job_request_summary(kind, job_payload),
            "result": None,
            "error": None,
            "traceback": None,
            "cancel_requested": False,
            "active_process_pid": None,
            "active_command": None,
            "resource_waiting": False,
        }
        with self.server.jobs_lock:
            if kind in MODEL_MAP_HEAVY_JOB_KINDS:
                for old in self.server.jobs.values():
                    old_kind = str(old.get("kind") or "")
                    if old_kind not in MODEL_MAP_HEAVY_JOB_KINDS:
                        continue
                    if old.get("status") == "queued" or old.get("resource_waiting"):
                        old["cancel_requested"] = True
                        old["status"] = "cancelled" if old.get("status") == "queued" else "cancelling"
                        old["updated_at_ts"] = now
                        old["updated_at"] = _iso_from_ts(now)
                        if old["status"] == "cancelled":
                            old["finished_at_ts"] = now
                            old["finished_at"] = _iso_from_ts(now)
                            old["result"] = {
                                "ok": False,
                                "cancelled": True,
                                "error": "Superseded by a newer map build.",
                            }
                            old["error"] = "cancelled"
            _trim_jobs_locked(self.server.jobs)
            self.server.jobs[job_id] = job
        thread = threading.Thread(target=self._run_job, args=(job_id, kind, job_payload), daemon=True)
        thread.start()
        return {"ok": True, "job": _job_snapshot(job, detail=False)}

    def _execute_job(self, kind: str, payload: dict) -> dict:
        payload = _apply_resource_limits(payload)
        if kind in MODEL_MAP_HEAVY_JOB_KINDS:
            if not self.server.heavy_job_lock.acquire(blocking=False):
                _set_current_job_resource_waiting(True)
                _update_current_job_progress("Waiting for current build", current=0, total=1, detail=kind)
                self.server.heavy_job_lock.acquire()
            try:
                _set_current_job_resource_waiting(False)
                _raise_if_job_cancelled()
                return self._execute_job_unlocked(kind, payload)
            finally:
                self.server.heavy_job_lock.release()
        return self._execute_job_unlocked(kind, payload)

    def _execute_job_unlocked(self, kind: str, payload: dict) -> dict:
        if kind == "render":
            return self._render_maps(payload)
        if kind == "localize_run":
            return self._localize_run(payload)
        if kind == "prepare_data":
            return self._prepare_model_data(payload)
        if kind == "sounding":
            return self._render_sounding(payload)
        if kind == "pressure_store":
            return self._prepare_pressure_store(payload)
        if kind == "wxstore":
            return self._run_wxstore_pipeline(payload)
        if kind == "wxstore_plot_existing":
            return self._plot_existing_wxstore(payload)
        return {"ok": False, "error": f"Unknown model-map job kind {kind!r}."}

    def _render_maps(self, payload: dict) -> dict:
        rejection = _run_request_rejection(payload)
        if rejection:
            return rejection
        payload = {"place_label_density": "none", **payload}
        payload, domain_fallback = _coerce_regional_model_domain(payload)
        if not payload.get("out_dir"):
            model = str(payload.get("model") or "hrrr")
            stamp = time.strftime("%Y%m%d_%H%M%S")
            payload = {
                **payload,
                "out_dir": str(self.server.env.out_root / "model_maps" / "maps" / model / stamp),
            }
        result = super()._render_maps(payload)
        if domain_fallback:
            result["domain_fallback"] = domain_fallback
            result["requested_domain"] = domain_fallback["requested_domain"]
            result["resolved_domain"] = domain_fallback["resolved_domain"]
        return result

    def _localize_run(self, payload: dict) -> dict:
        rejection = _run_request_rejection(payload)
        if rejection:
            return rejection
        started = time.time()
        payload, domain_fallback = _coerce_regional_model_domain(payload)
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        hours = _model_map_hour_list(payload)
        hours_text = _model_map_hour_text(hours)
        active_hour = int(payload.get("active_hour") or payload.get("forecast_hour") or hours[0])
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=max(hours),
        )
        resolved_run = f"{date}/{cycle:02d}z"
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        wants_map_data = bool(payload.get("warm_grib", True))
        wants_wxstore = bool(payload.get("warm_wxstore", False))
        wants_soundings = bool(payload.get("warm_soundings", True))
        wxstore_available = _wxstore_available(self.server.env.binaries)
        if (wants_map_data or wants_wxstore) and not products:
            return {"ok": False, "error": "Select at least one product before localizing map data."}

        base = {
            **payload,
            "model": model,
            "source": source,
            "run_str": resolved_run,
            "forecast_hour": active_hour,
            "forecast_hours": hours,
        }
        stages = []
        previews = []
        wxstore_run_id: str | None = None
        requested_render_products = [
            str(item) for item in payload.get("render_products") or [] if str(item).strip()
        ]
        render_products = requested_render_products or products[: max(1, int(payload.get("render_after_product_count") or 1))]
        wxstore_map_pipeline = bool(wants_wxstore and wxstore_available and payload.get("render_after", True) and render_products)
        hourly_pipeline = bool(payload.get("hourly_pipeline", True)) and bool(payload.get("render_after", True)) and bool(render_products)
        split_map_hours = bool(payload.get("split_map_hours", False)) and len(hours) > 1
        split_pressure_hours = bool(payload.get("split_pressure_hours", True)) and len(hours) > 1
        if hourly_pipeline:
            if wxstore_map_pipeline:
                total_steps = len(hours) * (2 + (1 if wants_soundings else 0))
            else:
                total_steps = len(hours) * (
                    (1 if wants_map_data else 0)
                    + (1 if wants_soundings else 0)
                    + 1
                )
                if wants_wxstore:
                    total_steps += 1
        else:
            total_steps = 0
            if wants_map_data:
                total_steps += len(hours) if split_map_hours else 1
            if wants_soundings:
                total_steps += len(hours) if split_pressure_hours else 1
            if wants_wxstore:
                total_steps += 1
            if payload.get("render_after", True) and render_products:
                total_steps += 1
        total_steps = max(1, total_steps)
        step_index = 0

        def result_payload(*, partial: bool) -> dict:
            ok_now = bool(stages) and all(stage["ok"] for stage in stages)
            return {
                "ok": ok_now,
                "partial": partial or (bool(stages) and not ok_now and any(stage["ok"] for stage in stages)),
                "stage": "localize_run",
                "model": model,
                "source": source,
                "date_yyyymmdd": date,
                "cycle_utc": cycle,
                "run_str": resolved_run,
                "hours": hours_text,
                "forecast_hours": hours,
                "hour_count": len(hours),
                "products": products,
                "product_count": len(products),
                "render_products": render_products,
                "render_product_count": len(render_products),
                "resource_preset": payload.get("resource_preset") or "balanced",
                "memory_mode": payload.get("memory_mode") or "balanced",
                "jobs": int(payload.get("jobs") or 2),
                "download_workers": int(payload.get("download_workers") or payload.get("jobs") or 4),
                "load_parallelism": int(payload.get("load_parallelism") or payload.get("jobs") or 4),
                "cache_mode": "idx_dependency_set",
                "cache_mode_note": "Uses indexed GRIB message subsets where available; full files are used when the source/model route requires them.",
                "hourly_pipeline": hourly_pipeline,
                "stages": stages,
                "previews": previews,
                "domain_fallback": domain_fallback,
                "ui_elapsed_s": round(time.time() - started, 2),
            }

        def add_stage(kind: str, result: dict) -> None:
            stages.append({"kind": kind, "ok": bool(result.get("ok", "error" not in result)), "result": result})
            previews.extend(result.get("previews") or [])
            _publish_current_job_result(result_payload(partial=True))

        def start_step(label: str, detail: str | None = None) -> None:
            nonlocal step_index
            _raise_if_job_cancelled()
            step_index += 1
            _update_current_job_progress(label, current=step_index, total=total_steps, detail=detail)

        if hourly_pipeline:
            bounds = payload.get("bounds")
            if isinstance(bounds, list) and len(bounds) == 4:
                lat = payload.get("lat") or ((float(bounds[2]) + float(bounds[3])) / 2.0)
                lon = payload.get("lon") or ((float(bounds[0]) + float(bounds[1])) / 2.0)
            else:
                lat = payload.get("lat") or 39.0
                lon = payload.get("lon") or -98.0
            for hour in hours:
                hour_label = f"F{hour:03}"
                hour_base = {
                    **base,
                    "forecast_hour": hour,
                    "forecast_hours": [hour],
                }
                if wxstore_map_pipeline:
                    start_step("Building WxStore cache", hour_label)
                    wxstore_result = self._run_wxstore_pipeline({
                        **hour_base,
                        "hours": str(hour),
                        "products": products,
                        "jobs": int(payload.get("jobs") or 2),
                        "import_wxa": True,
                        "render_plots": False,
                        "png_compression": str(payload.get("png_compression") or "fastest"),
                        "export_timeout": int(payload.get("export_timeout") or 1800),
                        "import_timeout": int(payload.get("import_timeout") or 1800),
                        "showcase_timeout": int(payload.get("showcase_timeout") or 900),
                    })
                    wxstore_run_id = str((wxstore_result.get("export_report") or {}).get("run_id") or wxstore_run_id or "latest")
                    add_stage(
                        f"wxstore_f{hour:03}",
                        wxstore_result,
                    )
                    start_step("Rendering WxStore maps", hour_label)
                    add_stage(
                        f"wxstore_plot_f{hour:03}",
                        self._plot_existing_wxstore({
                            **hour_base,
                            "run": wxstore_run_id or "latest",
                            "hours": str(hour),
                            "forecast_hour": hour,
                            "products": render_products,
                            "use_domain_bounds": True,
                            "jobs": int(payload.get("jobs") or 2),
                            "png_compression": str(payload.get("png_compression") or "fastest"),
                            "showcase_timeout": int(payload.get("showcase_timeout") or 900),
                        }),
                    )
                else:
                    if wants_map_data:
                        start_step("Preparing GRIB cache", hour_label)
                        add_stage(f"prepare_data_f{hour:03}", self._prepare_model_data({
                            **hour_base,
                            "products": products,
                            "download_workers": int(payload.get("download_workers") or payload.get("jobs") or 4),
                        }))
                    start_step("Rendering map", hour_label)
                    add_stage(
                        f"render_f{hour:03}",
                        self._render_maps({
                            **hour_base,
                            "products": render_products,
                            "place_label_density": str(payload.get("place_label_density") or "none"),
                        }),
                    )
                if wants_soundings:
                    start_step("Warming sounding store", hour_label)
                    add_stage(
                        f"pressure_store_f{hour:03}",
                        self._prepare_pressure_store({
                            **hour_base,
                            "hours": str(hour),
                            "lat": lat,
                            "lon": lon,
                            "timeout": int(payload.get("store_timeout") or 1800),
                            "load_parallelism": int(payload.get("load_parallelism") or payload.get("jobs") or 4),
                        }),
                    )

            if wxstore_map_pipeline:
                wants_wxstore = False
            if wants_wxstore and not wxstore_available:
                add_stage(
                    "wxstore",
                    {
                        "ok": True,
                        "skipped": True,
                        "stage": "wxstore_pipeline",
                        "reason": "WxStore import/render binaries are not available; using GRIB cache plus pressure-store soundings.",
                    },
                )
                wants_wxstore = False
            if wants_wxstore:
                start_step("Building WxStore cache", hours_text)
                wxstore_result = self._run_wxstore_pipeline({
                    **base,
                    "hours": hours_text,
                    "products": products,
                    "jobs": int(payload.get("jobs") or 2),
                    "import_wxa": True,
                    "render_plots": bool(payload.get("render_wxstore_plots", False)),
                    "png_compression": str(payload.get("png_compression") or "fastest"),
                    "export_timeout": int(payload.get("export_timeout") or 1800),
                    "import_timeout": int(payload.get("import_timeout") or 1800),
                    "showcase_timeout": int(payload.get("showcase_timeout") or 900),
                })
                wxstore_run_id = str((wxstore_result.get("export_report") or {}).get("run_id") or wxstore_run_id or "latest")
                add_stage(
                    "wxstore",
                    wxstore_result,
                )
            ok = bool(stages) and all(stage["ok"] for stage in stages)
            _update_current_job_progress(
                "Complete" if ok else "Finished with issues",
                current=total_steps,
                total=total_steps,
                detail=hours_text,
            )
            final = result_payload(partial=not ok)
            _publish_current_job_result(final)
            return final

        if wants_map_data:
            if split_map_hours:
                for hour in hours:
                    start_step("Preparing GRIB cache", f"F{hour:03}")
                    add_stage(f"prepare_data_f{hour:03}", self._prepare_model_data({
                        **base,
                        "forecast_hour": hour,
                        "forecast_hours": [hour],
                        "products": products,
                        "download_workers": int(payload.get("download_workers") or payload.get("jobs") or 4),
                    }))
            else:
                start_step("Preparing GRIB cache", hours_text)
                add_stage("prepare_data", self._prepare_model_data({
                    **base,
                    "products": products,
                    "download_workers": int(payload.get("download_workers") or payload.get("jobs") or 4),
                }))

        if wants_soundings:
            bounds = payload.get("bounds")
            if isinstance(bounds, list) and len(bounds) == 4:
                lat = payload.get("lat") or ((float(bounds[2]) + float(bounds[3])) / 2.0)
                lon = payload.get("lon") or ((float(bounds[0]) + float(bounds[1])) / 2.0)
            else:
                lat = payload.get("lat") or 39.0
                lon = payload.get("lon") or -98.0
            pressure_batches = [(hour, str(hour), f"F{hour:03}") for hour in hours] if split_pressure_hours else [(max(hours), hours_text, hours_text)]
            for pressure_hour, pressure_hours_text, label in pressure_batches:
                start_step("Warming sounding store", label)
                add_stage(
                    f"pressure_store_f{pressure_hour:03}" if split_pressure_hours else "pressure_store",
                    self._prepare_pressure_store({
                        **base,
                        "hours": pressure_hours_text,
                        "forecast_hour": pressure_hour,
                        "lat": lat,
                        "lon": lon,
                        "timeout": int(payload.get("store_timeout") or 1800),
                        "load_parallelism": int(payload.get("load_parallelism") or payload.get("jobs") or 4),
                    }),
                )

        if wants_wxstore and not _wxstore_available(self.server.env.binaries):
            add_stage(
                "wxstore",
                {
                    "ok": True,
                    "skipped": True,
                    "stage": "wxstore_pipeline",
                    "reason": "WxStore import/render binaries are not available; using GRIB cache plus pressure-store soundings.",
                },
            )
            wants_wxstore = False

        if wants_wxstore:
            start_step("Building WxStore", hours_text)
            wxstore_result = self._run_wxstore_pipeline({
                **base,
                "hours": hours_text,
                "products": products,
                "jobs": int(payload.get("jobs") or 2),
                "import_wxa": True,
                "render_plots": bool(payload.get("render_wxstore_plots", False)),
                "png_compression": str(payload.get("png_compression") or "fastest"),
                "export_timeout": int(payload.get("export_timeout") or 1800),
                "import_timeout": int(payload.get("import_timeout") or 1800),
                "showcase_timeout": int(payload.get("showcase_timeout") or 900),
            })
            wxstore_run_id = str((wxstore_result.get("export_report") or {}).get("run_id") or wxstore_run_id or "latest")
            add_stage(
                "wxstore",
                wxstore_result,
            )

        if payload.get("render_after", True):
            if wants_wxstore and stages and stages[-1]["kind"] == "wxstore" and stages[-1]["ok"]:
                start_step("Rendering from WxStore", f"F{active_hour:03}")
                add_stage(
                    "wxstore_plot_existing",
                    self._plot_existing_wxstore({
                        **base,
                        "run": wxstore_run_id or "latest",
                        "hours": str(active_hour),
                        "forecast_hour": active_hour,
                        "products": render_products,
                        "use_domain_bounds": True,
                        "jobs": int(payload.get("jobs") or 2),
                    }),
                )
            elif render_products:
                start_step("Rendering map", f"F{active_hour:03}")
                add_stage(
                    "render",
                    self._render_maps({
                        **base,
                        "forecast_hour": active_hour,
                        "forecast_hours": [active_hour],
                        "products": render_products,
                        "place_label_density": str(payload.get("place_label_density") or "none"),
                    }),
                )

        ok = bool(stages) and all(stage["ok"] for stage in stages)
        _update_current_job_progress(
            "Complete" if ok else "Finished with issues",
            current=total_steps,
            total=total_steps,
            detail=hours_text,
        )
        final = result_payload(partial=not ok)
        _publish_current_job_result(final)
        return final

    def _prepare_model_data(self, payload: dict) -> dict:
        rejection = _run_request_rejection(payload)
        if rejection:
            return rejection
        return super()._prepare_model_data(payload)

    def _prepare_pressure_store(self, payload: dict) -> dict:
        rejection = _run_request_rejection(payload)
        if rejection:
            return rejection
        backend = str(payload.get("store_backend") or payload.get("sounding_store_backend") or "wxprofile").lower()
        if backend not in {"pressure", "pressure-volume", "volume"}:
            result = super()._prepare_wxprofile_store(payload)
            if result.get("ok") or "hrrr_pressure_volume_store" not in self.server.env.binaries:
                return result
        return super()._prepare_pressure_store(payload)

    def _render_sounding(self, payload: dict) -> dict:
        rejection = _run_request_rejection(payload)
        if rejection:
            return rejection
        return super()._render_sounding(payload)

    def _run_wxstore_pipeline(self, payload: dict) -> dict:
        rejection = _run_request_rejection(payload)
        if rejection:
            return rejection
        return super()._run_wxstore_pipeline(payload)

    def _plot_existing_wxstore(self, payload: dict) -> dict:
        rejection = _run_request_rejection(payload)
        if rejection:
            return rejection
        return super()._plot_existing_wxstore(payload)


def _coerce_regional_model_domain(payload: dict) -> tuple[dict, dict | None]:
    model = str(payload.get("model") or "hrrr").lower()
    if model in GLOBAL_MODEL_IDS:
        return payload, None
    native_bounds = MODEL_NATIVE_BOUNDS.get(model)
    native_slug = MODEL_NATIVE_DOMAINS.get(model)
    if not native_bounds or not native_slug:
        return payload, None
    requested_slug = str(payload.get("domain") or "conus")
    domain = _domain_by_slug(requested_slug)
    bounds = domain.get("bounds") if domain else payload.get("bounds")
    if _bounds_fit(bounds, native_bounds):
        return payload, None
    native_domain = _domain_by_slug(native_slug)
    native_domain_bounds = native_domain.get("bounds") if native_domain else native_bounds
    next_payload = {
        **payload,
        "domain": native_slug,
        "bounds": native_domain_bounds,
    }
    return next_payload, {
        "model": model,
        "requested_domain": requested_slug,
        "resolved_domain": native_slug,
        "reason": f"{model.upper()} does not cover {requested_slug}; rendering the native {native_slug.upper()} domain.",
    }


def _bounds_fit(inner: object, outer: object) -> bool:
    try:
        a = [float(value) for value in list(inner)]
        b = [float(value) for value in list(outer)]
    except (TypeError, ValueError):
        return True
    if len(a) != 4 or len(b) != 4 or a[0] > a[1] or b[0] > b[1]:
        return False
    return (
        a[0] >= b[0] - 0.05
        and a[1] <= b[1] + 0.05
        and a[2] >= b[2] - 0.05
        and a[3] <= b[3] + 0.05
    )


def _path_is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _same_path(left: str, right: str) -> bool:
    try:
        return Path(left).resolve() == Path(right).resolve()
    except OSError:
        return False


def _dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0
    total = 0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += int(entry.stat(follow_symlinks=False).st_size)
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def _store_label(store: dict) -> str:
    if store.get("kind") == "wxstore_spatial":
        return f"WxStore {store.get('model', '')} {store.get('run', '')}".strip()
    return f"{store.get('kind', 'store')} {store.get('name') or Path(str(store.get('path') or '')).name}".strip()


def _default_source_for_model(model_id: str, sources: list[dict]) -> str:
    preferred = {
        "hrrr": "aws",
        "hrrr-ak": "aws",
        "gfs": "nomads",
        "gdas": "aws",
        "gefs": "aws",
        "aigfs": "nomads",
        "aigefs": "nomads",
        "hgefs": "nomads",
        "rap": "aws",
        "nam": "aws",
        "rrfs-a": "aws",
        "rrfs-public": "aws",
        "refs": "aws",
        "rrfs-firewx": "aws",
        "ecmwf-open-data": "ecmwf",
        "aifs": "ecmwf",
        "wrf": "gdex",
    }.get(str(model_id or "").lower())
    available = {str(source.get("id") or "").lower() for source in sources if source.get("id")}
    if preferred and preferred in available:
        return preferred
    if available:
        return sorted(available)[0]
    return preferred or "aws"


def _source_ids(sources: list[dict]) -> list[str]:
    ids = []
    for source in sources:
        source_id = str(source.get("id") or "").lower()
        if source_id and source_id not in ids:
            ids.append(source_id)
    return ids


def _fallback_sources_for_model(model_id: str) -> list[str]:
    model = str(model_id or "").lower()
    if model in {"ecmwf-open-data", "aifs"}:
        return ["ecmwf"]
    if model in {"aigfs", "aigefs", "hgefs"}:
        return ["nomads"]
    if model in {
        "hrrr",
        "hrrr-ak",
        "gfs",
        "gdas",
        "gefs",
        "rap",
        "nam",
        "nbm",
        "href",
        "sref",
        "rtma",
        "urma",
        "rrfs-a",
        "rrfs-public",
        "refs",
        "rrfs-firewx",
    }:
        return ["aws", "nomads"]
    return []


def _sources_for_model(model_id: str, sources: list[dict]) -> list[str]:
    ids = _source_ids(sources)
    return ids or _fallback_sources_for_model(model_id)


def _source_metadata(model_id: str, sources: list[dict], source_ids: list[str]) -> list[dict]:
    by_id = {str(source.get("id") or "").lower(): source for source in sources if source.get("id")}
    out = []
    for source_id in source_ids:
        source = by_id.get(source_id) or {"id": source_id}
        out.append({
            "id": source_id,
            "idx_available": source.get("idx_available"),
            "max_age_hours": source.get("max_age_hours"),
            "priority": source.get("priority"),
        })
    return out


def _latest_source_filter(source_filter: str | None) -> set[str] | None:
    text = str(source_filter or "").strip().lower()
    if not text or text in {"default", "preferred"}:
        return set()
    if text in {"all", "*"}:
        return None
    return {part.strip() for part in text.replace(";", ",").split(",") if part.strip()}


def _latest_source_row(
    model_id: str,
    source: str,
    cycle_hours_utc: list[int] | tuple[int, ...],
    today: str,
    yesterday: str,
) -> dict:
    latest = None
    error = None
    for date in (today, yesterday):
        try:
            latest = _load_json(rustwx.latest_run_json(model_id, date, source, 0))
            if latest.get("cycle"):
                break
        except Exception as exc:  # pragma: no cover - source availability is external
            error = str(exc)
    cycle = latest.get("cycle") if isinstance(latest, dict) else None
    runs = []
    if cycle:
        cycle_date = str(cycle.get("date_yyyymmdd") or today)
        cycle_hour = int(cycle.get("hour_utc") or 0)
        if _run_cycle_is_future(cycle_date, cycle_hour):
            error = f"{source} reported future cycle {cycle_date}/{cycle_hour:02d}z; ignoring it."
            latest = None
        else:
            runs = _previous_cycles(
                cycle_date,
                cycle_hour,
                [int(hour) for hour in cycle_hours_utc or []],
                count=6,
            )
    return {
        "source": source,
        "latest": latest,
        "runs": runs,
        "error": error if not runs else None,
    }


def _archive_source_for_model(model_id: str, sources: list[dict]) -> str:
    preferred = {
        "hrrr": "aws",
        "hrrr-ak": "aws",
        "gfs": "aws",
        "gdas": "aws",
        "gefs": "aws",
        "aigfs": "nomads",
        "aigefs": "nomads",
        "hgefs": "nomads",
        "rap": "aws",
        "nam": "aws",
        "nbm": "aws",
        "rrfs-a": "aws",
        "rrfs-public": "aws",
        "refs": "aws",
        "rrfs-firewx": "aws",
        "ecmwf-open-data": "ecmwf",
        "aifs": "ecmwf",
        "wrf": "gdex",
    }.get(str(model_id or "").lower())
    available = {str(source.get("id") or "").lower() for source in sources if source.get("id")}
    if preferred and preferred in available:
        return preferred
    return _default_source_for_model(model_id, sources)


def _run_datetime(date_yyyymmdd: str, cycle_utc: int) -> datetime:
    return datetime.strptime(f"{date_yyyymmdd}{int(cycle_utc):02d}", "%Y%m%d%H").replace(tzinfo=UTC)


def _run_cycle_is_future(date_yyyymmdd: str, cycle_utc: int, *, grace_hours: int = 6) -> bool:
    try:
        run_dt = _run_datetime(date_yyyymmdd, cycle_utc)
    except ValueError:
        return False
    return run_dt > datetime.now(UTC) + timedelta(hours=grace_hours)


def _run_request_rejection(payload: dict, *, grace_hours: int = 6) -> dict | None:
    raw = str(payload.get("run_str") or payload.get("run") or "latest")
    if not raw or raw.strip().lower() == "latest":
        return None
    try:
        parsed = _parse_run_string(raw)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    if not parsed:
        return None
    date, cycle = parsed
    if not _run_cycle_is_future(date, cycle, grace_hours=grace_hours):
        return None
    limit = datetime.now(UTC) + timedelta(hours=grace_hours)
    return {
        "ok": False,
        "error": (
            f"Run {date}/{cycle:02d}z is in the future for this machine. "
            f"Choose Latest or an archive cycle no later than {limit.strftime('%Y%m%d/%Hz')} UTC."
        ),
        "run_str": f"{date}/{cycle:02d}z",
        "max_future_grace_hours": grace_hours,
    }


def _previous_cycles(date_yyyymmdd: str, hour_utc: int, cycle_hours: list[int], *, count: int = 6) -> list[dict]:
    allowed = {int(hour) for hour in cycle_hours if 0 <= int(hour) <= 23} or {0, 6, 12, 18}
    current = datetime.strptime(f"{date_yyyymmdd}{int(hour_utc):02d}", "%Y%m%d%H").replace(tzinfo=UTC)
    runs = []
    probe = current
    while len(runs) < count and current - probe < timedelta(days=10):
        if probe.hour in allowed:
            ymd = probe.strftime("%Y%m%d")
            cycle = f"{probe.hour:02d}"
            runs.append({
                "date_yyyymmdd": ymd,
                "cycle_utc": probe.hour,
                "run_str": f"{ymd}/{cycle}z",
                "label": f"{ymd}/{cycle}z",
            })
        probe -= timedelta(hours=1)
    return runs


def _wxstore_available(binaries: dict[str, Path]) -> bool:
    return all(name in binaries for name in ("rustwx_grid_export", "wxstore_wxa_showcase"))


def _query_one(query: dict[str, list[str]], name: str, default: str | None) -> str | None:
    values = query.get(name)
    return values[0] if values else default


def _model_map_hour_list(payload: dict) -> list[int]:
    raw = payload.get("hours")
    if raw is None:
        raw = payload.get("forecast_hours")
    if raw is None:
        raw = payload.get("forecast_hour", 0)
    hours: set[int] = set()

    def add(value: object) -> None:
        if value is None:
            return
        if isinstance(value, str):
            for part in value.split(","):
                token = part.strip()
                if not token:
                    continue
                if "-" in token:
                    left, right = token.split("-", 1)
                    start = int(left.strip())
                    end = int(right.strip())
                    step = 1 if start <= end else -1
                    for hour in range(start, end + step, step):
                        if hour >= 0:
                            hours.add(hour)
                else:
                    hour = int(token)
                    if hour >= 0:
                        hours.add(hour)
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                add(item)
            return
        hour = int(value)
        if hour >= 0:
            hours.add(hour)

    add(raw)
    return sorted(hours) or [0]


def _model_map_hour_text(hours: list[int]) -> str:
    if not hours:
        return "0"
    ordered = sorted(set(int(hour) for hour in hours if int(hour) >= 0))
    if not ordered:
        return "0"
    if ordered == list(range(ordered[0], ordered[-1] + 1)):
        return f"{ordered[0]}-{ordered[-1]}" if len(ordered) > 1 else str(ordered[0])
    return ",".join(str(hour) for hour in ordered)


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RustWx Model Maps</title>
  <style>
    :root {
      --ink: #121416;
      --muted: #5b636b;
      --line: #cfd7dd;
      --panel: #f4f6f7;
      --paper: #ffffff;
      --accent: #087f8c;
      --accent-dark: #065e68;
      --warn: #b26b00;
      --bad: #a82020;
      --good: #166534;
      --selected: #d9ecef;
      color-scheme: light;
      font-family: Arial, Helvetica, sans-serif;
    }
    * { box-sizing: border-box; }
    [hidden] { display: none !important; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      background: #e9eef1;
      overflow: hidden;
      cursor: default;
    }
    header {
      height: 42px;
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 0 14px;
      background: #111;
      color: white;
      border-bottom: 3px solid #3b3b3b;
    }
    header strong { font-size: 15px; white-space: nowrap; }
    header select, header input, header button {
      height: 28px;
      border-radius: 4px;
      border: 1px solid #666;
      padding: 0 8px;
      font-weight: 700;
      background: #fff;
      color: #111;
    }
    header button {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent-dark);
      cursor: pointer;
    }
    header button:disabled { opacity: .6; cursor: wait; }
    .top-field { display: flex; align-items: center; gap: 5px; font-size: 12px; font-weight: 700; }
    header .top-field { color: #f7fbfc; }
    #statusText { margin-left: auto; font-size: 12px; color: #d8f3f6; max-width: 34vw; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    main {
      height: calc(100vh - 42px);
      display: grid;
      grid-template-columns: clamp(220px, 23vw, 276px) minmax(320px, 1fr) clamp(260px, 28vw, 336px);
      min-width: 0;
    }
    aside, .right-panel {
      min-height: 0;
      overflow: auto;
      background: var(--panel);
      border-color: var(--line);
    }
    aside { border-right: 1px solid var(--line); }
    .right-panel { border-left: 1px solid var(--line); }
    section {
      padding: 10px;
      border-bottom: 1px solid var(--line);
    }
    h2, h3 {
      margin: 0 0 8px;
      font-size: 13px;
      line-height: 1.2;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    label {
      display: grid;
      gap: 4px;
      font-size: 11px;
      font-weight: 700;
      color: #24292d;
    }
    select, input, button {
      min-width: 0;
      font: inherit;
    }
    aside select, aside input, .right-panel select, .right-panel input {
      width: 100%;
      height: 28px;
      border: 1px solid #aeb8bf;
      border-radius: 4px;
      background: white;
      color: #111;
      padding: 0 7px;
    }
    button {
      border: 1px solid #9aa7ae;
      border-radius: 4px;
      background: #fff;
      color: #111;
      padding: 6px 9px;
      font-weight: 700;
      cursor: pointer;
    }
    main, aside, .right-panel, .plot-stage, .plot-view, .bottom-strip, .sounding-overlay, .sounding-modal {
      cursor: default;
    }
    button.primary {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent-dark);
    }
    button.warning {
      background: #fff7e6;
      border-color: #d49a32;
      color: #5f3700;
    }
    button.danger {
      background: #fff0f0;
      border-color: #b94b4b;
      color: #8f1d1d;
    }
    button:disabled { opacity: .6; cursor: wait; }
    .row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-bottom: 8px;
    }
    .row.three { grid-template-columns: 1fr 1fr 1fr; }
    .wide { grid-column: 1 / -1; }
    .toolbar {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 6px;
      margin: 8px 0;
    }
    .segmented {
      display: grid;
      grid-template-columns: repeat(5, 1fr);
      gap: 4px;
      margin: 8px 0;
    }
    .segmented button {
      padding: 5px 4px;
      border-radius: 4px;
      font-size: 11px;
    }
    .segmented button.active {
      background: var(--ink);
      color: white;
      border-color: var(--ink);
    }
    .mode-tabs {
      display: flex;
      justify-content: flex-end;
      align-items: center;
      gap: 6px;
      padding: 6px 10px 0;
    }
    .mode-tabs button {
      min-height: 26px;
      padding: 4px 8px;
      font-size: 11px;
      background: transparent;
      color: var(--muted);
      border-color: transparent;
    }
    .mode-tabs button.active {
      background: #eef4f6;
      color: var(--ink);
      border-color: var(--line);
    }
    body.mode-easy .advanced-section { display: none; }
    body.mode-advanced #easyPanel { display: none; }
    body.focus-plot main {
      grid-template-columns: minmax(0, 1fr);
    }
    body.focus-plot aside,
    body.focus-plot .right-panel {
      display: none;
    }
    .easy-panel {
      display: grid;
      gap: 8px;
    }
    .easy-action-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px;
    }
    .easy-action-row.single {
      grid-template-columns: 1fr;
    }
    .easy-action-row button {
      min-height: 34px;
    }
    .product-set-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 6px;
      align-items: end;
    }
    .product-set-row button {
      min-height: 28px;
      white-space: nowrap;
    }
    .preset-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px;
    }
    .preset-card {
      display: grid;
      gap: 3px;
      min-height: 48px;
      text-align: left;
      border-color: #aeb8bf;
      background: #fff;
    }
    .preset-card.active {
      background: var(--selected);
      border-color: var(--accent-dark);
      box-shadow: 0 0 0 1px var(--accent-dark);
    }
    .preset-card strong {
      font-size: 12px;
      line-height: 1.1;
    }
    .preset-card span {
      color: var(--muted);
      font-size: 10px;
      line-height: 1.2;
      font-weight: 600;
    }
    .tier-panel {
      display: grid;
      gap: 8px;
      padding: 8px;
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 6px;
    }
    .tier-panel h3 {
      margin: 0 0 5px;
      font-size: 12px;
    }
    .tier-chip-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 5px;
    }
    .tier-chip {
      min-height: 32px;
      padding: 5px 7px;
      text-align: left;
      font-size: 11px;
      line-height: 1.15;
      background: #fff;
      border-color: #b7c8cf;
    }
    .tier-chip.heavy {
      border-color: #9c6a1a;
      background: #fff7e6;
    }
    .hour-custom-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 6px;
      align-items: end;
      margin-top: 6px;
    }
    .hour-custom-row button {
      min-height: 31px;
    }
    .latest-run-list {
      display: grid;
      gap: 6px;
      max-height: 260px;
      overflow: auto;
      padding-right: 3px;
    }
    .latest-model-card {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 7px;
      display: grid;
      gap: 6px;
    }
    .source-cycle-card {
      display: grid;
      gap: 5px;
      padding: 6px;
      background: #f8fafb;
      border: 1px solid #d7e0e4;
      border-radius: 5px;
    }
    .source-cycle-card header {
      height: auto;
      min-height: 0;
      display: block;
      padding: 0;
      background: transparent;
      border: 0;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0;
      color: var(--muted);
    }
    .latest-model-card > header {
      height: auto;
      min-height: 0;
      background: transparent;
      color: var(--ink);
      border: 0;
      padding: 0;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .run-chip-row {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
    }
    .run-chip-row button {
      padding: 4px 6px;
      font-size: 11px;
    }
    .run-chip-row button.active,
    .hour-chip-row button.active {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent-dark);
    }
    .hour-control {
      display: grid;
      gap: 5px;
      margin: -2px 0 8px;
    }
    .hour-chip-row {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      max-height: 112px;
      overflow: auto;
      padding: 4px;
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 5px;
    }
    .hour-chip-row button {
      min-width: 38px;
      padding: 4px 5px;
      font-size: 11px;
      font-variant-numeric: tabular-nums;
    }
    .hour-quick-row {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 4px;
    }
    .hour-quick-row button {
      padding: 4px 3px;
      font-size: 11px;
    }
    .modal-backdrop {
      position: fixed;
      inset: 0;
      z-index: 60;
      background: rgba(7, 10, 13, .46);
      display: grid;
      place-items: center;
      padding: 18px;
    }
    .modal-backdrop.hidden { display: none; }
    .selector-modal {
      width: min(1040px, 96vw);
      max-height: min(840px, 92vh);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      background: var(--panel);
      border: 1px solid #111;
      border-radius: 6px;
      box-shadow: 0 18px 50px rgba(0,0,0,.38);
    }
    .selector-modal.product-picker-modal {
      width: min(980px, 96vw);
    }
    .selector-modal > header {
      height: auto;
      min-height: 42px;
      padding: 8px 10px;
      display: flex;
      align-items: center;
      gap: 10px;
      background: #111;
      color: #fff;
      border-bottom: 1px solid #333;
    }
    .selector-modal-body {
      min-height: 0;
      overflow: auto;
      padding: 10px;
      display: grid;
      grid-template-columns: minmax(240px, .9fr) minmax(320px, 1.1fr);
      gap: 10px;
    }
    .selector-list {
      display: grid;
      gap: 7px;
    }
    .selector-card {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px;
      display: grid;
      gap: 5px;
      text-align: left;
    }
    .selector-card.active {
      border-color: var(--accent-dark);
      background: var(--selected);
    }
    .selector-card strong {
      font-size: 13px;
    }
    .selector-card span {
      color: var(--muted);
      font-size: 11px;
      line-height: 1.25;
      font-weight: 600;
    }
    .archive-run {
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 6px;
      padding: 8px;
      margin: 2px 0 10px;
    }
    #runCycleButtons {
      grid-template-columns: repeat(4, 1fr);
    }
    .archive-run .archive-date-row {
      grid-template-columns: 1fr;
    }
    .product-list {
      display: grid;
      gap: 5px;
      max-height: 360px;
      overflow: auto;
      padding-right: 3px;
    }
    .product-picker-modal .product-list {
      max-height: min(620px, 68vh);
    }
    .product-group {
      border: 1px solid var(--line);
      background: white;
      border-radius: 6px;
      overflow: hidden;
    }
    .product-group h3 {
      margin: 0;
      padding: 7px 8px;
      background: #101214;
      color: white;
      font-size: 12px;
    }
    .product-item {
      display: grid;
      grid-template-columns: 18px 1fr;
      gap: 6px;
      align-items: start;
      padding: 6px 8px;
      border-top: 1px solid #edf1f3;
      font-size: 12px;
      cursor: pointer;
    }
    .product-item input { width: 14px; height: 14px; margin: 0; }
    .product-item.selected { background: var(--selected); }
    .product-item small {
      display: block;
      color: var(--muted);
      font-size: 10px;
      margin-top: 2px;
      overflow-wrap: anywhere;
    }
    .timeline {
      display: grid;
      grid-template-columns: repeat(8, minmax(0, 1fr));
      gap: 4px;
      margin-top: 8px;
    }
    .timeline button {
      padding: 5px 0;
      font-size: 11px;
      font-variant-numeric: tabular-nums;
    }
    .timeline button.active {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent-dark);
    }
    .plot-stage {
      min-width: 0;
      min-height: 0;
      display: grid;
      grid-template-rows: minmax(0, 1fr) auto;
      background: #d8dee2;
    }
    .plot-view {
      min-width: 0;
      min-height: 0;
      overflow: auto;
      padding: 12px;
      display: grid;
      place-items: start center;
    }
    .map-frame {
      position: relative;
      width: min(100%, var(--display-width, 1600px));
      max-width: 100%;
      background: #fff;
      border: 1px solid #9ea7ad;
      box-shadow: 0 1px 6px rgba(0,0,0,.18);
    }
    .map-frame img {
      display: block;
      width: 100%;
      max-width: none;
      height: auto;
      user-select: none;
    }
    .map-frame img.plot-map { cursor: pointer; }
    .map-frame.is-stale img.plot-map {
      opacity: .45;
      cursor: default;
    }
    .map-stale-banner {
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      display: grid;
      gap: 8px;
      justify-items: center;
      min-width: min(420px, calc(100% - 40px));
      padding: 14px;
      background: rgba(255,255,255,.95);
      border: 1px solid #7c878f;
      box-shadow: 0 4px 18px rgba(0,0,0,.22);
      color: var(--ink);
      font-weight: 800;
      text-align: center;
      z-index: 5;
    }
    .map-empty {
      width: min(100%, 1100px);
      min-height: 520px;
      display: grid;
      place-items: center;
      background: #fff;
      border: 1px dashed #aeb8bf;
      color: var(--muted);
      font-weight: 700;
    }
    .click-chip {
      position: absolute;
      pointer-events: none;
      transform: translate(-50%, -100%);
      background: #f6e39a;
      border: 1px solid #6b5a13;
      color: #111;
      padding: 3px 6px;
      font-size: 11px;
      font-weight: 700;
      white-space: nowrap;
    }
    .box-footprint {
      position: absolute;
      pointer-events: none;
      transform: translate(-50%, -50%);
      border: 3px solid #f6e39a;
      background: rgba(246, 227, 154, .24);
      box-shadow: 0 0 0 2px rgba(17,17,17,.7), 0 0 14px rgba(246, 227, 154, .7);
      color: #111;
      z-index: 2;
    }
    .box-hover-footprint {
      position: absolute;
      pointer-events: none;
      transform: translate(-50%, -50%);
      border: 2px dashed #0a7f8c;
      background: rgba(8, 127, 140, .12);
      box-shadow: 0 0 0 1px rgba(255,255,255,.8);
      color: #0a7f8c;
      z-index: 1;
    }
    .box-footprint.drawn-box, .box-hover-footprint.drawn-box {
      transform: none;
      min-width: 12px;
      min-height: 12px;
    }
    .box-footprint::before, .box-hover-footprint::before,
    .box-footprint::after, .box-hover-footprint::after {
      content: "";
      position: absolute;
      background: currentColor;
      opacity: .85;
    }
    .box-footprint::before, .box-hover-footprint::before {
      left: 50%;
      top: 0;
      width: 2px;
      height: 100%;
      transform: translateX(-50%);
    }
    .box-footprint::after, .box-hover-footprint::after {
      left: 0;
      top: 50%;
      width: 100%;
      height: 2px;
      transform: translateY(-50%);
    }
    .box-footprint-label {
      position: absolute;
      left: 50%;
      top: 100%;
      transform: translate(-50%, 4px);
      padding: 2px 5px;
      background: #111;
      color: #fff;
      border: 1px solid rgba(255,255,255,.65);
      font-size: 10px;
      line-height: 1.1;
      white-space: nowrap;
    }
    .bottom-strip {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      padding: 8px 10px;
      border-top: 1px solid var(--line);
      background: #f7f9fa;
    }
    .preview-strip {
      display: flex;
      gap: 6px;
      overflow: auto;
      min-width: 0;
    }
    .preview-chip {
      flex: 0 0 auto;
      max-width: 170px;
      height: 32px;
      padding: 5px 8px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      border: 1px solid var(--line);
      background: #fff;
      color: #111;
      cursor: pointer;
      font-size: 11px;
    }
    .preview-chip.active {
      border-color: var(--accent-dark);
      background: var(--selected);
    }
    .preview-chip.more {
      cursor: default;
      color: var(--muted);
    }
    .meta-line {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }
    .local-run-note {
      padding: 8px;
      border: 1px solid #b7c8cf;
      border-left: 4px solid var(--accent);
      background: #ffffff;
      color: #1f2933;
      font-size: 12px;
      line-height: 1.35;
      margin-bottom: 8px;
    }
    .check-row {
      display: grid;
      gap: 6px;
      margin: 8px 0;
    }
    .check-row label {
      display: flex;
      align-items: center;
      gap: 7px;
      font-size: 12px;
    }
    .check-row input {
      width: 14px;
      height: 14px;
      margin: 0;
    }
    .cache-state {
      display: grid;
      gap: 4px;
      padding: 7px;
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 6px;
    }
    .workflow-panel {
      display: grid;
      gap: 7px;
      padding: 7px;
      margin-bottom: 8px;
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 6px;
    }
    .click-mode-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      margin-bottom: 8px;
    }
    .click-mode-grid button {
      min-height: 36px;
      font-size: 13px;
    }
    .click-mode-grid button.active {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent-dark);
    }
    .box-size-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-bottom: 6px;
    }
    .box-preset-row {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 4px;
      margin: 0 0 8px;
    }
    .box-preset-row button {
      padding: 5px 3px;
      font-size: 11px;
    }
    .job-list, .data-list, .sounding-list {
      display: grid;
      gap: 7px;
    }
    .job-card, .data-row, .sounding-card {
      background: white;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px;
      display: grid;
      gap: 5px;
      font-size: 12px;
    }
    .data-row header {
      display: flex;
      align-items: start;
      justify-content: space-between;
      gap: 8px;
    }
    .data-row header strong {
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .data-path {
      overflow-wrap: anywhere;
      color: var(--muted);
      font-size: 11px;
    }
    .job-status {
      width: max-content;
      padding: 2px 6px;
      border-radius: 999px;
      background: #edf1f3;
      font-weight: 700;
      font-size: 11px;
    }
    .job-status.completed { color: var(--good); }
    .job-status.failed, .job-status.cancelled { color: var(--bad); }
    .job-status.running, .job-status.queued { color: var(--warn); }
    #latestSounding {
      margin-top: 8px;
    }
    #latestSounding img {
      width: 100%;
      height: auto;
      background: #000;
      border: 1px solid #111;
    }
    .sounding-card.active { border-color: var(--accent); box-shadow: 0 0 0 1px var(--accent); }
    .sounding-overlay {
      position: fixed;
      inset: 46px 18px 18px;
      z-index: 40;
      display: grid;
      place-items: center;
      background: rgba(7, 10, 13, .42);
      pointer-events: auto;
    }
    .sounding-overlay.hidden { display: none; pointer-events: none; }
    .sounding-modal {
      width: min(1180px, 96vw);
      height: min(820px, 90vh);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      background: #050505;
      border: 2px solid #111;
      border-radius: 6px;
      box-shadow: 0 18px 50px rgba(0,0,0,.38);
      pointer-events: auto;
    }
    .sounding-modal header {
      height: auto;
      min-height: 38px;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 6px 8px;
      border-bottom: 1px solid #2b2f33;
      background: #111;
      color: #fff;
    }
    .sounding-modal header strong {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .sounding-modal header .toolbar {
      margin: 0 0 0 auto;
      flex-wrap: nowrap;
    }
    .sounding-modal header button {
      height: 28px;
      padding: 4px 8px;
    }
    .sounding-modal-body {
      min-width: 0;
      min-height: 0;
      overflow: auto;
      background: #000;
      display: grid;
      place-items: start center;
    }
    .sounding-modal-body img {
      display: block;
      width: min(100%, 1180px);
      height: auto;
      background: #000;
      cursor: default;
    }
    details {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 7px 8px;
    }
    details summary {
      cursor: pointer;
      font-weight: 700;
      font-size: 12px;
    }
    pre {
      max-height: 260px;
      overflow: auto;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      background: #111;
      color: #e7f7f8;
      border-radius: 6px;
      padding: 8px;
      font-size: 11px;
    }
    a { color: var(--accent-dark); }
    .error-text { color: var(--bad); font-weight: 700; }
    @media (max-width: 1060px) {
      header .top-field, #prevHour, #nextHour { display: none; }
      #statusText { max-width: 54vw; }
    }
    @media (max-width: 760px) {
      body { overflow: auto; }
      main { height: auto; min-height: calc(100vh - 42px); grid-template-columns: 1fr; }
      aside, .right-panel { max-height: none; border: 0; }
      .plot-stage { min-height: 640px; }
      header { flex-wrap: wrap; height: auto; min-height: 42px; padding: 8px; }
      #statusText { margin-left: 0; max-width: 100%; }
    }
  </style>
</head>
<body class="mode-easy">
  <header>
    <strong>RustWx Model Maps</strong>
    <label class="top-field" hidden>Model<select id="modelTop"></select></label>
    <label class="top-field" hidden>Run<input id="runTop" value="latest"></label>
    <label class="top-field" hidden>Hour<input id="hourTop" type="number" min="0" max="43848" value="0"></label>
    <button id="prevHour" title="Previous forecast hour" hidden>&lt;</button>
    <button id="nextHour" title="Next forecast hour" hidden>&gt;</button>
    <button id="renderTop" class="primary">Run</button>
    <button id="resetLatestTop" title="Reset to latest run" hidden>Reset Latest</button>
    <button id="focusPlotTop" title="Toggle plot focus">Focus</button>
    <span id="statusText">loading</span>
  </header>
  <main>
    <aside>
      <div class="mode-tabs">
        <button id="easyModeTab" type="button" class="active">Forecast</button>
        <button id="advancedModeTab" type="button">Debug</button>
      </div>
      <section id="easyPanel" class="easy-panel">
        <h2>Forecast</h2>
        <div class="row three">
          <label>Model<select id="easyModel"></select></label>
          <label>Source<select id="easySource"></select></label>
          <label>Domain<select id="easyDomain"></select></label>
        </div>
        <div class="easy-action-row">
          <button id="easyOpenSetup" type="button">Select Model + Products</button>
          <button id="easyLatest" type="button">Use Latest Run</button>
        </div>
        <div class="row">
          <label>Archive Date<input id="easyDate" type="date"></label>
          <label>Archive Cycle<select id="easyCycle"></select></label>
        </div>
        <label>Computer Use<select id="resourcePreset">
          <option value="balanced">50% - balanced</option>
          <option value="max">100% - fastest</option>
          <option value="light">25% - gentle</option>
        </select></label>
        <div id="resourceMeta" class="meta-line"></div>
        <div id="easyRunSummary" class="meta-line"></div>
        <input id="easyHour" type="hidden" value="0">
        <select id="easyHours" hidden>
          <option value="current">Current</option>
          <option value="0-6">0-6</option>
          <option value="0-18">0-18</option>
          <option value="0-48">0-48</option>
          <option value="full">Full Run</option>
        </select>
        <div class="hour-control">
          <h2>Hours</h2>
          <div class="hour-quick-row">
            <button id="selectHours0_18" type="button">0-18</button>
            <button id="selectHours0_48" type="button">0-48</button>
            <button id="selectHoursFull" type="button">Full</button>
            <button id="clearHourSelection" type="button">One Hour</button>
          </div>
          <div id="easyHourChips" class="hour-chip-row"></div>
          <div class="hour-custom-row">
            <label>Custom Hours<input id="easyCustomHours" placeholder="6-10 or 0,3,6"></label>
            <button id="applyCustomHours" type="button">Set</button>
          </div>
        </div>
        <label>Workflow<select id="easyPreset"></select></label>
        <div class="product-set-row">
          <label>Product Set<select id="easyProductTier">
            <option value="direct">Direct</option>
            <option value="derived">Direct + Derived</option>
            <option value="heavy">Direct + Derived + ECAPE</option>
          </select></label>
          <button id="easyPickProducts" type="button">Pick Products</button>
        </div>
        <div id="easyPresetProducts" class="meta-line"></div>
        <div id="easyPresetGrid" class="preset-grid"></div>
        <div class="check-row" hidden>
          <label><input id="easyWarmStore" type="checkbox" checked>Local store first</label>
          <label><input id="easyAutoLatest" type="checkbox">Auto-check latest</label>
        </div>
        <div class="easy-action-row single">
          <button id="easyGo" class="primary" type="button">Run</button>
          <button id="easyRenderOnly" type="button" hidden>Cached Map</button>
        </div>
        <div class="toolbar" hidden>
          <button id="easyResetLatest" type="button">Reset</button>
          <button id="easyRefreshRuns" type="button">Refresh Runs</button>
        </div>
        <div id="latestRunList" class="latest-run-list"></div>
      </section>
      <section class="advanced-section">
        <h2>Run</h2>
        <div class="row">
          <label>Model<select id="model"></select></label>
          <label>Source<select id="source">
            <option value="aws">aws</option>
            <option value="nomads">nomads</option>
            <option value="google">google</option>
            <option value="azure">azure</option>
            <option value="ncei">ncei</option>
            <option value="gdex">gdex</option>
            <option value="ecmwf">ecmwf</option>
            <option value="earth2-archive">earth2-archive</option>
            <option value="aifs-inference">aifs-inference</option>
          </select></label>
        </div>
        <div class="row">
          <label>Run<input id="run" value="latest"></label>
          <label>Hour<input id="forecastHour" type="number" min="0" max="43848" value="0"></label>
        </div>
        <div class="archive-run">
          <div class="row archive-date-row">
            <label>Date<input id="runDate" type="date"></label>
            <label>Cycle<select id="runCycle"></select></label>
          </div>
          <div class="segmented" id="runCycleButtons">
            <button type="button" data-cycle="00">00z</button>
            <button type="button" data-cycle="06">06z</button>
            <button type="button" data-cycle="12">12z</button>
            <button type="button" data-cycle="18">18z</button>
          </div>
          <div class="toolbar">
            <button id="useArchiveRun" type="button">Use Archive</button>
            <button id="useLatestRun" type="button">Latest</button>
          </div>
          <div id="archiveRunMeta" class="meta-line">latest run</div>
        </div>
        <div class="row">
          <label>Hours<input id="renderHours" value="0"></label>
          <label>Warm<input id="prepareHours" value="0"></label>
        </div>
        <div id="timeline" class="timeline"></div>
      </section>
      <section class="advanced-section">
        <h2>Local Run</h2>
        <div class="local-run-note">
          Map-site mode caches the full dependency set for selected hours. First build uses bandwidth, disk, and decode time; after that maps and soundings should read local data.
        </div>
        <div class="row">
          <label>Hours<input id="localizeHours" value="0-48"></label>
          <label>Jobs<input id="localizeJobs" type="number" min="1" max="8" value="2"></label>
        </div>
        <div class="toolbar">
          <button id="localizeFullRun">Full Run</button>
          <button id="localizeCurrentHour">Current Hour</button>
        </div>
        <div class="check-row">
          <label><input id="localizeMapData" type="checkbox" checked>Map GRIB dependencies</label>
          <label><input id="localizeSoundings" type="checkbox" checked>Fast sounding store</label>
          <label><input id="localizeWxStore" type="checkbox" checked>WxStore product cache</label>
          <label><input id="localizeRenderAfter" type="checkbox" checked>Render preview after warming</label>
        </div>
        <div class="toolbar">
          <button id="localizeRun" class="primary">Make Run Local</button>
        </div>
        <div id="cacheState" class="cache-state meta-line">Cache state loading</div>
      </section>
      <section class="advanced-section">
        <h2>Domain</h2>
        <div class="row">
          <label class="wide">Search<input id="domainSearch" placeholder="conus, plains, okc"></label>
          <label class="wide">Domain<select id="domain"></select></label>
        </div>
        <div id="domainModelMeta" class="meta-line"></div>
      </section>
      <section class="advanced-section">
        <h2>Products</h2>
        <div class="row">
          <label>Group<select id="kind">
            <option value="">All</option>
            <option value="upper_air">Upper Air</option>
            <option value="surface">Surface</option>
            <option value="precip">Precip</option>
            <option value="severe">Severe</option>
            <option value="winter">Winter</option>
            <option value="fire">Fire/Smoke</option>
            <option value="direct">Direct</option>
            <option value="light_derived">Derived</option>
            <option value="heavy_derived">Heavy</option>
            <option value="windowed">Windowed</option>
          </select></label>
          <label>Search<input id="productSearch" placeholder="cape, 500mb, qpf"></label>
        </div>
        <div class="toolbar">
          <button id="preset2m">2m Temp</button>
          <button id="presetSevere">Severe</button>
          <button id="presetUpper">500mb</button>
          <button id="clearProducts">Clear</button>
        </div>
        <div class="workflow-panel">
          <label>Workflow<select id="productWorkflowSelect"></select></label>
          <label>Name<input id="productWorkflowName" placeholder="Severe day"></label>
          <div class="check-row">
            <label><input id="workflowAutoApply" type="checkbox" checked>Auto apply</label>
          </div>
          <div class="toolbar">
            <button id="saveWorkflow">Save</button>
            <button id="applyWorkflow">Apply</button>
            <button id="deleteWorkflow">Delete</button>
          </div>
        </div>
        <div id="productList" class="product-list"></div>
      </section>
      <section class="advanced-section">
        <h2>Output</h2>
        <div class="segmented" id="sizeButtons">
          <button data-size="1600x1100" class="active">Web</button>
          <button data-size="1920x1080">HD</button>
          <button data-size="2560x1440">2K</button>
          <button data-size="3840x2160">4K</button>
          <button data-size="1200x675">Share</button>
        </div>
        <div class="row">
          <label>Width<input id="width" type="number" min="640" max="3840" value="1600"></label>
          <label>Height<input id="height" type="number" min="480" max="2160" value="1100"></label>
        </div>
        <label class="wide">Render Mode<select id="renderMode">
          <option value="grib">GRIB/cache</option>
          <option value="wxstore">WxStore</option>
        </select></label>
        <div class="toolbar">
          <button id="prepareData">Warm GRIB</button>
          <button id="renderBtn" class="primary">Render Map</button>
          <button id="warmWxStore">Warm WxStore</button>
        </div>
      </section>
    </aside>
    <div class="plot-stage">
      <div id="plotView" class="plot-view">
        <div id="mapEmpty" class="map-empty">No map rendered</div>
      </div>
      <div class="bottom-strip">
        <div id="previewStrip" class="preview-strip"></div>
        <div class="toolbar">
          <button id="fitPlot">Fit</button>
          <button id="fullPlot">Full</button>
          <button id="openPlot">Open</button>
        </div>
      </div>
    </div>
    <div class="right-panel">
      <section>
        <h2>Sounding</h2>
        <div class="click-mode-grid">
          <button id="pointClickMode" type="button">Point</button>
          <button id="boxClickMode" type="button">Fixed Box</button>
          <button id="drawBoxMode" type="button">Draw Box</button>
        </div>
        <select id="clickSoundingMode" hidden><option value="point">Point</option><option value="box">Fixed Box</option><option value="draw-box">Draw Box</option></select>
        <div class="meta-line" id="clickModeMeta">Point click active.</div>
        <div class="check-row">
          <label><input id="autoOpenSounding" type="checkbox">Open large automatically</label>
        </div>
        <div class="row">
          <label>Lat<input id="soundingLat" value="35.222"></label>
          <label>Lon<input id="soundingLon" value="-97.439"></label>
        </div>
        <div class="row">
          <label>Hour<input id="soundingHour" type="number" value="0"></label>
          <label hidden>Mode<select id="soundingDataMode"><option value="auto">auto</option><option value="store">store</option><option value="grib">grib</option></select></label>
        </div>
        <div class="row">
          <label>Sample<select id="soundingMethod"><option value="nearest">Nearest</option><option value="inverse-distance4">Inverse Distance</option><option value="box-mean">Box Mean</option></select></label>
          <label>Box Shape<select id="soundingBoxShape"><option value="custom">Custom</option><option value="square">Square</option></select></label>
        </div>
        <div class="box-size-grid">
          <label>Box W km<input id="soundingBoxWidthKm" type="number" min="1" max="800" value="25"></label>
          <label>Box H km<input id="soundingBoxHeightKm" type="number" min="1" max="800" value="25"></label>
        </div>
        <input id="soundingBoxKm" type="hidden" value="12.5">
        <div id="boxPresetRow" class="box-preset-row">
          <button type="button" data-box-size="10x10">10</button>
          <button type="button" data-box-size="25x25">25</button>
          <button type="button" data-box-size="50x50">50</button>
          <button type="button" data-box-size="100x100">100</button>
          <button type="button" data-box-size="200x200">200</button>
        </div>
        <label class="wide">Station<input id="soundingStation" value=""></label>
        <div class="toolbar">
          <button id="prepareStore" hidden>Warm Store</button>
          <button id="soundingRender" class="primary">Sounding</button>
          <button id="cancelSounding" type="button" disabled>Cancel</button>
        </div>
        <div class="meta-line" id="soundingMeta">Click a rendered map to sample.</div>
        <div id="latestSounding"></div>
      </section>
      <section>
        <h2>Jobs</h2>
        <div id="jobList" class="job-list"></div>
      </section>
      <section>
        <h2>Soundings</h2>
        <div id="soundingList" class="sounding-list"></div>
      </section>
      <section>
        <h2>Data</h2>
        <div class="toolbar">
          <button id="refreshData">Refresh</button>
        </div>
        <div id="dataSummary" class="meta-line"></div>
        <div id="dataList" class="data-list"></div>
      </section>
      <section class="advanced-section">
        <details open>
          <summary>Result</summary>
          <pre id="result">{}</pre>
        </details>
      </section>
    </div>
  </main>
  <div id="soundingOverlay" class="sounding-overlay hidden">
    <div class="sounding-modal" role="dialog" aria-label="Sounding viewer">
      <header>
        <strong id="soundingOverlayTitle">Sounding</strong>
        <div class="toolbar">
          <button id="soundingOverlayOpen">Open</button>
          <button id="soundingOverlayClose">Close</button>
        </div>
      </header>
      <div class="sounding-modal-body">
        <img id="soundingOverlayImg" alt="expanded sounding">
      </div>
    </div>
  </div>
  <div id="setupOverlay" class="modal-backdrop hidden">
    <div class="selector-modal" role="dialog" aria-label="Model and product setup">
      <header>
        <strong>Model + Products</strong>
        <span id="setupOverlayMeta" class="meta-line"></span>
        <button id="setupOverlayClose" type="button">Close</button>
      </header>
      <div class="selector-modal-body">
        <section>
          <h2>Models</h2>
          <div id="setupModelList" class="selector-list"></div>
        </section>
        <section>
          <h2>Workflows</h2>
          <div id="setupPresetList" class="selector-list"></div>
        </section>
        <section>
          <h2>Product Set</h2>
          <div id="setupTierList" class="selector-list"></div>
        </section>
        <section>
          <h2>Custom Products</h2>
          <div class="row">
            <label>Group<select id="setupProductGroup">
              <option value="">All</option>
              <option value="upper_air">Upper Air</option>
              <option value="surface">Surface</option>
              <option value="precip">Precip</option>
              <option value="severe">Severe</option>
              <option value="winter">Winter</option>
              <option value="fire">Fire/Smoke</option>
              <option value="direct">Direct</option>
              <option value="light_derived">Derived</option>
              <option value="heavy_derived">Heavy</option>
              <option value="windowed">Windowed</option>
            </select></label>
            <label>Search<input id="setupProductSearch" placeholder="cape, 500mb, qpf"></label>
          </div>
          <div class="toolbar">
            <button id="setupUseCustom" type="button">Use Custom</button>
            <button id="setupClearCustom" type="button">Clear</button>
          </div>
          <div id="setupCustomMeta" class="meta-line"></div>
          <div id="setupProductList" class="product-list custom-product-list"></div>
        </section>
      </div>
    </div>
  </div>
  <div id="productPickerOverlay" class="modal-backdrop hidden">
    <div class="selector-modal product-picker-modal" role="dialog" aria-label="Custom product picker">
      <header>
        <strong>Pick Products</strong>
        <span id="productPickerHeaderMeta" class="meta-line"></span>
        <button id="productPickerClose" type="button">Close</button>
      </header>
      <div class="selector-modal-body">
        <section class="wide">
          <div class="row">
            <label>Group<select id="productPickerGroup">
              <option value="">All</option>
              <option value="upper_air">Upper Air</option>
              <option value="surface">Surface</option>
              <option value="precip">Precip</option>
              <option value="severe">Severe</option>
              <option value="winter">Winter</option>
              <option value="fire">Fire/Smoke</option>
              <option value="direct">Direct</option>
              <option value="light_derived">Derived</option>
              <option value="heavy_derived">Heavy</option>
              <option value="windowed">Windowed</option>
            </select></label>
            <label>Search<input id="productPickerSearch" placeholder="cape, 500mb, qpf"></label>
          </div>
          <div class="toolbar">
            <button id="productPickerUseCustom" class="primary" type="button">Use Custom</button>
            <button id="productPickerSelectVisible" type="button">Select Visible</button>
            <button id="productPickerClear" type="button">Clear</button>
          </div>
          <div id="productPickerMeta" class="meta-line"></div>
          <div id="productPickerList" class="product-list custom-product-list"></div>
        </section>
      </div>
    </div>
  </div>
  <script>
const $ = (id) => document.getElementById(id);
const COMMON_DOMAINS = ["conus", "southern-plains", "southern_plains", "central", "oklahoma", "ok_oklahoma_city", "midwest", "great-lakes", "northeast", "southeast", "california", "gulf-to-kansas"];
const SOURCE_DEFAULTS = new Map([
  ["hrrr", "aws"], ["hrrr-ak", "aws"], ["gfs", "nomads"], ["gdas", "aws"],
  ["gefs", "aws"], ["ecmwf-open-data", "ecmwf"], ["aifs", "ecmwf"],
  ["aigfs", "nomads"], ["aigefs", "nomads"], ["hgefs", "nomads"],
  ["rap", "aws"], ["nam", "aws"], ["rrfs-a", "aws"], ["rrfs-firewx", "aws"]
]);
const ARCHIVE_SOURCE_DEFAULTS = new Map([
  ["hrrr", "aws"], ["hrrr-ak", "aws"], ["gfs", "aws"], ["gdas", "aws"],
  ["gefs", "aws"], ["rap", "aws"], ["nam", "aws"], ["nbm", "aws"],
  ["aigfs", "nomads"], ["aigefs", "nomads"], ["hgefs", "nomads"],
  ["rrfs-a", "aws"], ["rrfs-public", "aws"], ["refs", "aws"], ["rrfs-firewx", "aws"],
  ["ecmwf-open-data", "ecmwf"], ["aifs", "ecmwf"], ["wrf", "gdex"]
]);
const PRESET_WORKFLOWS = [
  {
    id: "forecasting",
    name: "Forecasting",
    summary: "500mb, surface, moisture, precip, severe",
    products: ["500mb_height_winds", "mslp_10m_winds", "2m_temperature_10m_winds", "2m_dewpoint_10m_winds", "precipitable_water", "composite_reflectivity", "sbcape", "mlcape", "bulk_shear_0_6km", "srh_0_3km", "stp_fixed", "qpf_1h", "qpf_total", "total_qpf"]
  },
  {
    id: "severe",
    name: "Severe",
    summary: "Instability, shear, reflectivity, UH",
    products: ["sbcape", "mlcape", "mucape", "sbcin", "mlcin", "lapse_rate_700_500", "bulk_shear_0_6km", "srh_0_1km", "srh_0_3km", "stp_fixed", "scp_mu_0_3km_0_6km_proxy", "composite_reflectivity", "composite_reflectivity_uh", "uh_2to5km", "uh_2to5km_1h_max", "qpf_1h"]
  },
  {
    id: "winter",
    name: "Winter",
    summary: "Thermal profile, precip type, QPF",
    products: ["500mb_height_winds", "850mb_temperature_height_winds", "700mb_temperature_height_winds", "2m_temperature_10m_winds", "2m_dewpoint_10m_winds", "wetbulb_2m", "mslp_10m_winds", "total_qpf", "qpf_total", "categorical_snow", "categorical_freezing_rain", "categorical_ice_pellets", "precipitation_type"]
  },
  {
    id: "fire",
    name: "Fire Weather",
    summary: "Wind, RH, VPD, smoke",
    products: ["10m_wind_gusts", "2m_relative_humidity_10m_winds", "2m_dewpoint_10m_winds", "vpd_2m", "fire_weather_composite", "smoke_pm25_native", "smoke_column", "500mb_height_winds"]
  },
  {
    id: "precip",
    name: "Precip",
    summary: "QPF, PWAT, reflectivity",
    products: ["mslp_10m_winds", "precipitable_water", "total_qpf", "qpf_1h", "qpf_6h", "qpf_12h", "qpf_24h", "qpf_total", "composite_reflectivity"]
  },
  {
    id: "upper",
    name: "Upper Air",
    summary: "Jet, 500mb, vorticity, RH",
    products: ["250mb_height_winds", "300mb_height_winds", "500mb_height_winds", "500mb_absolute_vorticity_height_winds", "700mb_rh_height_winds", "850mb_temperature_height_winds"]
  },
  {
    id: "surface",
    name: "Surface",
    summary: "Temperature, dewpoint, MSLP, winds",
    products: ["2m_temperature_10m_winds", "2m_dewpoint_10m_winds", "mslp_10m_winds", "10m_wind_gusts", "cloud_cover", "visibility", "composite_reflectivity"]
  },
  {
    id: "fast_sounding",
    name: "Sounding Setup",
    summary: "Minimal maps plus fast point/box soundings",
    products: ["500mb_height_winds", "2m_temperature_10m_winds", "2m_dewpoint_10m_winds", "sbcape", "mlcape", "bulk_shear_0_6km", "srh_0_3km"]
  },
  {
    id: "custom",
    name: "Custom",
    summary: "Pick exact products and hours",
    products: []
  }
];
const TIER1_CORE_PRODUCTS = [
  "500mb_height_winds",
  "2m_temperature_10m_winds",
  "2m_dewpoint_10m_winds",
  "mslp_10m_winds",
  "700mb_rh_height_winds",
  "850mb_temperature_height_winds",
  "precipitable_water",
  "composite_reflectivity",
  "qpf_1h",
  "total_qpf"
];
const TIER2_DERIVED_PRODUCTS = [
  "sbcape",
  "mlcape",
  "mucape",
  "sbcin",
  "mlcin",
  "lapse_rate_700_500",
  "bulk_shear_0_6km",
  "srh_0_1km",
  "srh_0_3km",
  "ehi_0_1km",
  "ehi_0_3km",
  "stp_fixed",
  "scp_mu_0_3km_0_6km_proxy",
  "uh_2to5km",
  "uh_2to5km_1h_max"
];
const TIER3_HEAVY_PRODUCTS = [
  "sbecape",
  "mlecape",
  "muecape",
  "sb_ecape_derived_cape_ratio",
  "ml_ecape_derived_cape_ratio",
  "mu_ecape_derived_cape_ratio",
  "ecape_scp",
  "ecape_ehi_0_1km",
  "ecape_ehi_0_3km",
  "ecape_stp"
];
const PRODUCT_TIER_LABELS = {
  direct: "Direct",
  derived: "Direct + Derived",
  heavy: "Direct + Derived + ECAPE"
};
const RESOURCE_PRESETS = {
  max: {label: "100%", cpuFraction: 1.0, memoryMode: "high", minWorkers: 2, maxWorkers: 8},
  balanced: {label: "50%", cpuFraction: 0.5, memoryMode: "balanced", minWorkers: 1, maxWorkers: 4},
  light: {label: "25%", cpuFraction: 0.25, memoryMode: "low", minWorkers: 1, maxWorkers: 2}
};
const GLOBAL_MODELS = new Set(["gfs", "gdas", "gefs", "aigfs", "aigefs", "hgefs", "ecmwf-open-data", "aifs"]);
const MODEL_NATIVE_BOUNDS = new Map([
  ["hrrr", [-127, -66, 23, 51.5]],
  ["rap", [-135, -60, 15, 60]],
  ["nam", [-135, -55, 12, 62]],
  ["hiresw", [-127, -66, 23, 51.5]],
  ["href", [-127, -66, 23, 51.5]],
  ["sref", [-127, -66, 23, 51.5]],
  ["rtma", [-127, -66, 23, 51.5]],
  ["urma", [-127, -66, 23, 51.5]],
  ["nbm", [-127, -66, 23, 51.5]],
  ["rrfs-a", [-127, -66, 23, 51.5]],
  ["rrfs-public", [-127, -66, 23, 51.5]],
  ["refs", [-127, -66, 23, 51.5]],
  ["rrfs-firewx", [-127, -66, 23, 51.5]],
  ["hrrr-ak", [-180, -128, 49, 73]]
]);
const MODEL_NATIVE_DOMAINS = new Map([
  ["hrrr", "conus"], ["rap", "conus"], ["nam", "conus"], ["hiresw", "conus"],
  ["href", "conus"], ["sref", "conus"], ["rtma", "conus"], ["urma", "conus"],
  ["nbm", "conus"], ["rrfs-a", "conus"], ["rrfs-public", "conus"],
  ["refs", "conus"], ["rrfs-firewx", "conus"]
]);
const STORAGE_KEY = "rustwx:model-maps:viewer:v1";
const WORKFLOW_KEY = "rustwx:model-maps:product-workflows:v1";
const state = {
  bootstrap: null,
  models: [],
  products: null,
  domains: [],
  selectedProducts: new Set(["2m_temperature_10m_winds"]),
  jobs: new Map(),
  pollers: new Map(),
  jobClientMeta: new Map(),
  previews: [],
  activePreview: null,
  soundings: [],
  displayMode: "fit",
  soundingContext: null,
  boxDrag: null,
  mapSoundingLaunchInFlight: false,
  mapSoundingLaunchStartedAt: 0,
  mapSoundingRequestToken: 0,
  mapSoundingAbortController: null,
  activeMapSoundingJobId: null,
  activeMapSoundingJobStartedAt: 0,
  activeSoundingJobId: null,
  activeSoundingJobStartedAt: 0,
  queuedMapSoundingPayload: null,
  handledSoundingJobIds: new Set(),
  renderTimer: null,
  restoring: false,
  lastInventory: null,
  productWorkflows: [],
  activeWorkflowId: "",
  easyPresetId: "forecasting",
  easyProductTier: "direct",
  easyCustomProducts: new Set(),
  easySelectedHours: new Set(),
  viewMode: "easy",
  latestRuns: null,
  autoLatestTimer: null,
  backgroundWarmJobId: null,
  backgroundWarmProducts: new Set(),
  pendingWarmRenderSlug: ""
};
function loadSavedState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const saved = raw ? JSON.parse(raw) : null;
    if (Array.isArray(saved?.soundings)) saved.soundings = saved.soundings.map(persistableSounding);
    return saved;
  } catch (_err) {
    return null;
  }
}
function setControlValue(id, value) {
  if (value === undefined || value === null || !$(id)) return;
  const element = $(id);
  if (element.tagName === "SELECT") {
    if ([...element.options].some((option) => option.value === String(value))) element.value = String(value);
    return;
  }
  if (element.type === "checkbox") {
    element.checked = Boolean(value);
    return;
  }
  element.value = String(value);
}
function populateRunCycleOptions() {
  if (!$("runCycle") || $("runCycle").options.length) return;
  for (let hour = 0; hour < 24; hour += 1) {
    const value = String(hour).padStart(2, "0");
    const option = document.createElement("option");
    option.value = value;
    option.textContent = `${value}z`;
    $("runCycle").appendChild(option);
  }
  $("runCycle").value = "00";
}
function populateEasyCycleOptions() {
  if (!$("easyCycle") || $("easyCycle").options.length) return;
  for (let hour = 0; hour < 24; hour += 1) {
    const value = String(hour).padStart(2, "0");
    const option = document.createElement("option");
    option.value = value;
    option.textContent = `${value}z`;
    $("easyCycle").appendChild(option);
  }
  $("easyCycle").value = "00";
}
function populatePresetControls() {
  $("easyPreset").innerHTML = "";
  for (const preset of PRESET_WORKFLOWS) {
    const option = document.createElement("option");
    option.value = preset.id;
    option.textContent = preset.name;
    $("easyPreset").appendChild(option);
  }
  $("easyPreset").value = state.easyPresetId;
  renderEasyPresetGrid();
}
function renderEasyPresetGrid() {
  if (!$("easyPresetGrid")) return;
  $("easyPresetGrid").innerHTML = "";
  for (const preset of PRESET_WORKFLOWS) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "preset-card" + (preset.id === state.easyPresetId ? " active" : "");
    const title = document.createElement("strong");
    title.textContent = preset.name;
    const summary = document.createElement("span");
    summary.textContent = preset.summary;
    button.append(title, summary);
    button.addEventListener("click", () => applyEasyPreset(preset.id));
    $("easyPresetGrid").appendChild(button);
  }
}
function availableProductsFrom(slugs) {
  const available = availableProductSlugs();
  return [...new Set(slugs)].filter((slug) => available.has(slug));
}
function coreAutoProducts() {
  const core = availableProductsFrom(TIER1_CORE_PRODUCTS);
  if (core.length) return core;
  const fallback = modelEntry().default_render_product || productRows()[0]?.slug;
  return fallback ? [fallback] : [];
}
function normalizedProductTier(value = state.easyProductTier) {
  return Object.prototype.hasOwnProperty.call(PRODUCT_TIER_LABELS, value) ? value : "direct";
}
function easyIsCustomPreset(id = state.easyPresetId) {
  return id === "custom";
}
function easyCustomProductArray({fallback = true} = {}) {
  const available = availableProductSlugs();
  const custom = [...state.easyCustomProducts].filter((slug) => available.has(slug));
  if (custom.length || !fallback) return custom;
  const selected = selectedProductArray().filter((slug) => available.has(slug));
  if (selected.length) return selected;
  const fallbackSlug = modelEntry().default_render_product || productRows()[0]?.slug;
  return fallbackSlug ? [fallbackSlug] : [];
}
function workflowDerivedProducts(coreProducts = coreAutoProducts()) {
  const heavy = new Set(availableProductsFrom(TIER3_HEAVY_PRODUCTS));
  const core = new Set(coreProducts);
  return presetProducts(state.easyPresetId).filter((slug) => !core.has(slug) && !heavy.has(slug));
}
function easyProductSetProducts() {
  if (easyIsCustomPreset()) return easyCustomProductArray({fallback: false});
  const core = coreAutoProducts();
  const tier = normalizedProductTier();
  if (tier === "direct") return core;
  const products = [...core, ...workflowDerivedProducts(core)];
  if (tier === "heavy") products.push(...availableProductsFrom(TIER3_HEAVY_PRODUCTS));
  return [...new Set(products)];
}
function setViewMode(mode, {save = true} = {}) {
  state.viewMode = mode === "advanced" ? "advanced" : "easy";
  document.body.classList.toggle("mode-advanced", state.viewMode === "advanced");
  document.body.classList.toggle("mode-easy", state.viewMode !== "advanced");
  $("easyModeTab").classList.toggle("active", state.viewMode === "easy");
  $("advancedModeTab").classList.toggle("active", state.viewMode === "advanced");
  if (save) saveViewerState();
}
function resourceSettings() {
  const presetId = $("resourcePreset")?.value || "balanced";
  const preset = RESOURCE_PRESETS[presetId] || RESOURCE_PRESETS.balanced;
  const logical = Math.max(1, Number(navigator.hardwareConcurrency || 4));
  const workers = Math.max(
    preset.minWorkers,
    Math.min(preset.maxWorkers, Math.max(1, Math.round(logical * preset.cpuFraction)))
  );
  const downloadWorkers = Math.max(1, Math.min(8, presetId === "light" ? workers + 1 : workers));
  return {
    preset: presetId,
    label: preset.label,
    logical,
    jobs: workers,
    download_workers: downloadWorkers,
    load_parallelism: workers,
    memory_mode: preset.memoryMode,
    split_pressure_hours: true,
    hourly_pipeline: true
  };
}
function updateResourceMeta() {
  if (!$("resourceMeta")) return;
  const settings = resourceSettings();
  $("resourceMeta").textContent = `${settings.label} | ${settings.jobs} CPU worker${settings.jobs === 1 ? "" : "s"} | ${settings.memory_mode} memory plan`;
  if ($("localizeJobs")) $("localizeJobs").value = String(settings.jobs);
}
function withResourceSettings(payload) {
  const settings = resourceSettings();
  return {
    ...payload,
    jobs: settings.jobs,
    download_workers: settings.download_workers,
    load_parallelism: settings.load_parallelism,
    memory_mode: settings.memory_mode,
    resource_preset: settings.preset,
    split_pressure_hours: settings.split_pressure_hours,
    hourly_pipeline: settings.hourly_pipeline
  };
}
function syncEasyDomainOptions() {
  if (!$("easyDomain") || !$("domain")) return;
  $("easyDomain").innerHTML = $("domain").innerHTML;
  if ([...$("easyDomain").options].some((option) => option.value === $("domain").value)) {
    $("easyDomain").value = $("domain").value;
  }
}
function syncEasyFromMain() {
  if (!$("easyModel") || !$("model")) return;
  if ([...$("easyModel").options].some((option) => option.value === $("model").value)) $("easyModel").value = $("model").value;
  syncEasySourceOptions();
  if ([...$("easySource").options].some((option) => option.value === $("source").value)) $("easySource").value = $("source").value;
  syncEasyDomainOptions();
  $("easyHour").value = $("forecastHour").value;
  const parsed = parseRunString($("run").value);
  if (parsed) {
    $("easyDate").value = dateInputFromYmd(parsed.ymd);
    $("easyCycle").value = parsed.cycle;
    $("easyRunSummary").textContent = `${$("model").value.toUpperCase()} ${parsed.runStr} F${String($("forecastHour").value || 0).padStart(3, "0")}`;
  } else {
    if (!$("easyDate").value) $("easyDate").value = utcDateInputValue();
    $("easyRunSummary").textContent = `${$("model").value.toUpperCase()} latest F${String($("forecastHour").value || 0).padStart(3, "0")}`;
  }
  renderEasyHourChips();
}
async function syncMainFromEasy({archive = false, load = false} = {}) {
  if ($("easyModel").value && $("easyModel").value !== $("model").value) {
    $("model").value = $("easyModel").value;
    $("modelTop").value = $("easyModel").value;
    setDefaultSourceForModel();
    renderDomainOptions();
    syncEasyDomainOptions();
    if (load) await loadProducts();
  }
  syncEasySourceOptions();
  if ($("easySource")?.value && $("easySource").value !== $("source").value) {
    $("source").value = $("easySource").value;
  }
  if ($("easyDomain").value && $("easyDomain").value !== $("domain").value) {
    $("domain").value = $("easyDomain").value;
  }
  setHour($("easyHour").value || 0);
  if (archive) {
    $("runDate").value = $("easyDate").value || utcDateInputValue();
    $("runCycle").value = $("easyCycle").value || "00";
    useArchiveRun({announce: true, render: false});
  }
  syncRunControls(false);
  updateActiveMapFreshness();
  syncEasyFromMain();
  renderEasyHourChips();
}
function presetById(id) {
  return PRESET_WORKFLOWS.find((preset) => preset.id === id) || PRESET_WORKFLOWS[0];
}
function presetProducts(id) {
  if (easyIsCustomPreset(id)) return easyCustomProductArray({fallback: false});
  const preset = presetById(id);
  const available = availableProductSlugs();
  const chosen = [];
  for (const slug of preset.products || []) {
    if (available.has(slug) && !chosen.includes(slug)) chosen.push(slug);
  }
  if (!chosen.length) {
    const fallback = modelEntry().default_render_product || productRows()[0]?.slug;
    if (fallback) chosen.push(fallback);
  }
  return chosen;
}
function renderEasyPresetSummary() {
  const products = easyProductSetProducts();
  const tier = normalizedProductTier();
  if ($("easyProductTier")) $("easyProductTier").disabled = easyIsCustomPreset();
  $("easyPresetProducts").textContent = easyIsCustomPreset()
    ? `Custom | ${products.length} product${products.length === 1 ? "" : "s"} for selected hours`
    : `${PRODUCT_TIER_LABELS[tier]} | ${products.length} product${products.length === 1 ? "" : "s"} for selected hours`;
  renderEasyPresetGrid();
  renderSetupOverlay();
}
function applyEasyPreset(id, {save = true} = {}) {
  state.easyPresetId = presetById(id).id;
  $("easyPreset").value = state.easyPresetId;
  if (easyIsCustomPreset() && !state.easyCustomProducts.size) {
    state.easyCustomProducts = new Set(selectedProductArray());
  }
  state.selectedProducts = new Set(easyProductSetProducts());
  renderProductList();
  renderEasyPresetSummary();
  if (save) saveViewerState();
}
function activateCustomRun() {
  if (!state.easyCustomProducts.size) state.easyCustomProducts = new Set(selectedProductArray());
  applyEasyPreset("custom");
  setStatus("Custom run selected");
}
function compactHourList(hours) {
  const ordered = [...new Set(hours.map((h) => Math.max(0, Math.round(Number(h)))).filter(Number.isFinite))].sort((a, b) => a - b);
  if (!ordered.length) return "";
  const ranges = [];
  let start = ordered[0];
  let last = ordered[0];
  for (let index = 1; index < ordered.length; index += 1) {
    const hour = ordered[index];
    if (hour === last + 1) {
      last = hour;
      continue;
    }
    ranges.push(start === last ? String(start) : `${start}-${last}`);
    start = hour;
    last = hour;
  }
  ranges.push(start === last ? String(start) : `${start}-${last}`);
  return ranges.join(",");
}
function selectedEasyHoursText() {
  return compactHourList([...state.easySelectedHours]);
}
function selectedCycleHour() {
  const parsed = parseRunString($("run").value);
  if (parsed) return Number(parsed.cycle);
  const easyCycle = Number($("easyCycle")?.value);
  return Number.isFinite(easyCycle) ? easyCycle : null;
}
function effectiveMaxForecastHour() {
  const max = Number(modelEntry().max_forecast_hour || 48);
  if ($("model").value === "hrrr") {
    const cycle = selectedCycleHour();
    if (Number.isFinite(cycle) && ![0, 6, 12, 18].includes(Number(cycle))) return Math.min(max, 18);
  }
  return max;
}
function updateHourPresetAvailability() {
  if (!$("selectHours0_48")) return;
  const max = effectiveMaxForecastHour();
  const limited = $("model").value === "hrrr" && max < 48;
  $("selectHours0_48").disabled = limited;
  $("selectHours0_48").title = limited ? "HRRR 0-48 is for 00z/06z/12z/18z cycles." : "";
  $("selectHoursFull").hidden = max === 18 || max === 48;
  $("selectHoursFull").textContent = `0-${max}`;
}
function easyHoursValue() {
  const max = effectiveMaxForecastHour();
  const selected = selectedEasyHoursText();
  if (selected) return compactHourList([...state.easySelectedHours].filter((hour) => hour <= max));
  const value = $("easyHours").value;
  if (value === "current") return String($("forecastHour").value || 0);
  if (value === "full") return `0-${max}`;
  const hours = parseHourList(value, $("forecastHour").value).filter((hour) => hour <= max);
  return compactHourList(hours) || String(Math.min(max, Number($("forecastHour").value || 0)));
}
function visibleForecastHours() {
  const max = effectiveMaxForecastHour();
  const active = Number($("forecastHour").value || 0);
  const limit = max <= 60 ? max : Math.min(max, 84);
  const hours = [];
  for (let hour = 0; hour <= limit; hour += (max > 84 && hour >= 48 ? 3 : 1)) hours.push(hour);
  if (active > limit && active <= max) hours.push(active);
  return [...new Set(hours)].sort((a, b) => a - b);
}
function renderEasyHourChips() {
  const box = $("easyHourChips");
  if (!box) return;
  updateHourPresetAvailability();
  const activeHour = Number($("forecastHour").value || 0);
  box.innerHTML = "";
  for (const hour of visibleForecastHours()) {
    const button = document.createElement("button");
    button.type = "button";
    const selected = state.easySelectedHours.has(hour);
    button.className = (selected || (!state.easySelectedHours.size && hour === activeHour)) ? "active" : "";
    button.textContent = `F${String(hour).padStart(3, "0")}`;
    button.title = selected ? "Selected for build" : "Click to select this forecast hour";
    button.addEventListener("click", () => {
      setHour(hour);
      if (state.easySelectedHours.has(hour)) state.easySelectedHours.delete(hour);
      else state.easySelectedHours.add(hour);
      if (state.easySelectedHours.size) $("easyHours").value = "current";
      renderEasyHourChips();
      syncEasyFromMain();
      saveViewerState();
    });
    box.appendChild(button);
  }
}
function selectEasyHourRange(start, end) {
  const max = effectiveMaxForecastHour();
  const last = Math.min(max, Math.max(start, end));
  state.easySelectedHours = new Set();
  for (let hour = Math.max(0, Math.min(start, end)); hour <= last; hour += 1) state.easySelectedHours.add(hour);
  $("easyHours").value = `${Math.max(0, Math.min(start, end))}-${last}`;
  if ($("easyCustomHours")) $("easyCustomHours").value = "";
  renderEasyHourChips();
  saveViewerState();
}
function clearEasyHourSelection() {
  state.easySelectedHours = new Set();
  $("easyHours").value = "current";
  if ($("easyCustomHours")) $("easyCustomHours").value = "";
  renderEasyHourChips();
  saveViewerState();
}
function applyEasyCustomHours() {
  const max = effectiveMaxForecastHour();
  const requested = parseHourList($("easyCustomHours").value, $("forecastHour").value);
  const hours = requested.filter((hour) => hour <= max);
  if (!hours.length) {
    setStatus("Enter hours like 6-10 or 0,3,6.", true);
    return;
  }
  if (hours.length !== requested.length) setStatus(`Selected cycle is limited to F${String(max).padStart(3, "0")}.`, true);
  state.easySelectedHours = new Set(hours);
  $("easyHours").value = compactHourList(hours);
  renderEasyHourChips();
  syncEasyFromMain();
  saveViewerState();
}
function wxstoreReady() {
  return Boolean(state.bootstrap?.doctor?.capabilities?.wxstore_export
    && state.bootstrap?.doctor?.capabilities?.wxstore_plot);
}
function wxstoreRunId(model, runStr) {
  const parsed = parseRunString(runStr);
  if (!parsed) return runStr || "latest";
  return `${parsed.ymd}_${String(model || "hrrr").toLowerCase()}_${parsed.cycle}z`;
}
async function tryCachedWxstoreProduct(pinned, slug, hour) {
  if (!wxstoreReady()) return false;
  const run = wxstoreRunId(pinned.model, pinned.run_str);
  const result = await post("/api/wxstore-existing", withResourceSettings({
    ...pinned,
    run,
    hours: String(hour),
    forecast_hour: hour,
    forecast_hours: [hour],
    products: [slug],
    use_domain_bounds: true,
    png_compression: "fastest"
  }));
  if (!result?.ok) return false;
  showResult(result);
  handleJobResult({kind: "wxstore_plot_existing", request: {...pinned, products: [slug], run}}, result, {});
  setStatus(`${productLabel(slug)} rendered from WxStore cache`);
  return true;
}
async function renderEasyProduct(slug) {
  await syncMainFromEasy({archive: parseRunString($("run").value) !== null, load: false});
  const mapContext = activePreviewContext();
  const fallback = payloadBase();
  const hour = Number(mapContext?.forecastHour ?? $("forecastHour").value ?? 0);
  const pinned = mapContext ? {
    ...fallback,
    model: mapContext.model || fallback.model,
    source: mapContext.source || fallback.source,
    run_str: mapContext.runStr || fallback.run_str,
    domain: mapContext.domain || fallback.domain,
    bounds: mapContext.bounds || fallback.bounds,
    forecast_hour: hour,
    forecast_hours: [hour]
  } : {
    ...fallback,
    forecast_hour: hour,
    forecast_hours: [hour]
  };
  state.selectedProducts = new Set([slug]);
  renderProductList();
  $("renderMode").value = wxstoreReady() ? "wxstore" : "grib";
  $("renderHours").value = String(hour);
  $("prepareHours").value = String(hour);
  saveViewerState();
  setStatus(`${productLabel(slug)} queued for ${pinned.run_str || "latest"} F${String(hour).padStart(3, "0")}`);
  if (wxstoreReady()) {
    if (await tryCachedWxstoreProduct(pinned, slug, hour)) return {ok: true, cached: true};
    if (state.backgroundWarmJobId && state.backgroundWarmProducts.has(slug)) {
      state.pendingWarmRenderSlug = slug;
      setStatus(`${productLabel(slug)} is warming; it will open when ready`);
      return {ok: true, warming: true};
    }
    return runJob("localize_run", withResourceSettings({
      ...pinned,
      active_hour: hour,
      hours: String(hour),
      products: [slug],
      warm_grib: true,
      warm_soundings: false,
      warm_wxstore: true,
      render_after: true,
      render_products: [slug],
      render_after_product_count: 1,
      png_compression: "fastest",
      hourly_pipeline: false,
      split_map_hours: false,
      split_pressure_hours: false
    }), $("renderBtn"));
  }
  return runJob("render", withResourceSettings({
    ...pinned,
    products: [slug],
    place_label_density: "none"
  }), $("renderBtn"));
}
function warmWorkflowTier2(basePayload, coreProducts, activeHour) {
  if (!wxstoreReady()) return;
  const warmProducts = workflowDerivedProducts(coreProducts);
  if (!warmProducts.length) return;
  state.backgroundWarmProducts = new Set(warmProducts);
  state.pendingWarmRenderSlug = "";
  const payload = withResourceSettings({
    ...basePayload,
    active_hour: activeHour,
    forecast_hour: activeHour,
    forecast_hours: [activeHour],
    hours: String(activeHour),
    products: warmProducts,
    warm_grib: true,
    warm_soundings: false,
    warm_wxstore: true,
    render_after: false,
    render_products: [],
    render_after_product_count: 0,
    png_compression: "fastest",
    hourly_pipeline: false,
    split_map_hours: false,
    split_pressure_hours: false
  });
  runJob("localize_run", payload, null, {
    lockButton: false,
    ignoreResult: true,
    onLaunch: (job) => {
      state.backgroundWarmJobId = job.id;
      setStatus(`warming ${warmProducts.length} workflow products`);
    },
    onComplete: (_job, result) => {
      state.backgroundWarmJobId = null;
      state.backgroundWarmProducts = new Set();
      const pending = state.pendingWarmRenderSlug;
      state.pendingWarmRenderSlug = "";
      if (result?.ok && pending && warmProducts.includes(pending)) {
        renderEasyProduct(pending).catch((err) => setStatus(String(err), true));
      }
    }
  }).catch((err) => {
    state.backgroundWarmJobId = null;
    state.backgroundWarmProducts = new Set();
    setStatus(String(err), true);
  });
}
async function easyBuildAndPlot() {
  await syncMainFromEasy({archive: parseRunString($("run").value) !== null, load: true});
  state.easyPresetId = presetById($("easyPreset").value).id;
  state.easyProductTier = normalizedProductTier($("easyProductTier")?.value || state.easyProductTier);
  const products = easyProductSetProducts();
  const hoursText = easyHoursValue();
  const launchRun = await resolvedLaunchRun(hoursText);
  state.selectedProducts = new Set(products);
  renderProductList();
  renderEasyPresetSummary();
  $("easyWarmStore").checked = true;
  const useStore = wxstoreReady();
  $("renderMode").value = useStore ? "wxstore" : "grib";
  $("localizeHours").value = hoursText;
  $("renderHours").value = String($("forecastHour").value || 0);
  $("localizeMapData").checked = true;
  $("localizeSoundings").checked = false;
  $("localizeWxStore").checked = useStore;
  $("localizeRenderAfter").checked = true;
  const basePayload = payloadBase();
  if (launchRun?.run_str) {
    basePayload.run_str = launchRun.run_str;
    setStatus(`Using ${launchRun.run_str} for this run`);
  }
  const payload = withResourceSettings({
    ...basePayload,
    active_hour: Number($("forecastHour").value || 0),
    forecast_hour: Number($("forecastHour").value || 0),
    hours: hoursText,
    products,
    warm_grib: true,
    warm_soundings: true,
    warm_wxstore: useStore,
    render_after: true,
    render_products: products,
    render_after_product_count: Math.max(1, products.length),
    png_compression: "fastest"
  });
  saveViewerState();
  return runJob("localize_run", payload, $("easyGo"));
}
async function easyRenderOnly() {
  await syncMainFromEasy({archive: false, load: true});
  applyEasyPreset($("easyPreset").value, {save: false});
  state.selectedProducts = new Set(easyProductSetProducts());
  $("renderMode").value = "grib";
  $("renderHours").value = String($("forecastHour").value || 0);
  saveViewerState();
  return renderMaps();
}
async function resetToLatest() {
  if (state.renderTimer) clearTimeout(state.renderTimer);
  state.renderTimer = null;
  state.previews = [];
  state.activePreview = null;
  state.soundings = [];
  state.soundingContext = null;
  $("plotView").innerHTML = '<div id="mapEmpty" class="map-empty">No map rendered</div>';
  renderPreviewStrip();
  renderSoundings();
  renderLatestSounding(null);
  const defaultModel = state.models.some((m) => m.id === "hrrr") ? "hrrr" : state.models[0]?.id || "";
  $("model").value = defaultModel;
  $("modelTop").value = defaultModel;
  $("easyModel").value = defaultModel;
  setDefaultSourceForModel();
  syncEasySourceOptions();
  $("easySource").value = $("source").value;
  setRunValue("latest", {save: false, status: "Reset to latest"});
  $("runDate").value = utcDateInputValue();
  $("easyDate").value = $("runDate").value;
  $("runCycle").value = "00";
  $("easyCycle").value = "00";
  $("domainSearch").value = "";
  renderDomainOptions();
  if ([...$("domain").options].some((option) => option.value === "conus" && !option.disabled)) $("domain").value = "conus";
  syncEasyDomainOptions();
  setHour(0);
  state.easySelectedHours = new Set();
  $("easyHours").value = "current";
  $("easyCustomHours").value = "";
  $("resourcePreset").value = "balanced";
  updateResourceMeta();
  $("localizeHours").value = "0";
  $("renderHours").value = "0";
  $("easyWarmStore").checked = true;
  $("autoOpenSounding").checked = false;
  $("soundingDataMode").value = "auto";
  setClickSoundingMode("point");
  await loadProducts();
  applyEasyPreset("forecasting", {save: false});
  setViewMode("easy", {save: false});
  saveViewerState();
  refreshData();
}
async function refreshLatestRuns() {
  $("latestRunList").textContent = "Loading runs...";
  try {
    const model = $("easyModel").value || $("model").value || "hrrr";
    const source = $("easySource").value || $("source").value || "aws";
    const data = await api(`/api/latest-runs?model=${encodeURIComponent(model)}&source=${encodeURIComponent(source)}&timeout=8`);
    state.latestRuns = data;
    renderLatestRuns();
  } catch (err) {
    $("latestRunList").textContent = String(err);
  }
}
function renderLatestRuns() {
  const list = $("latestRunList");
  if (!list || !state.latestRuns) return;
  list.innerHTML = "";
  const metaRow = document.createElement("div");
  metaRow.className = "meta-line";
  metaRow.textContent = `Checked ${state.latestRuns.date_checked || "UTC"} | ${state.latestRuns.cached ? "cached" : "fresh"} | manual refresh`;
  list.appendChild(metaRow);
  for (const row of state.latestRuns.models || []) {
    const card = document.createElement("div");
    card.className = "latest-model-card";
    const header = document.createElement("header");
    const title = document.createElement("strong");
    title.textContent = row.label || String(row.model || "").toUpperCase();
    const meta = document.createElement("span");
    meta.className = "meta-line";
    meta.textContent = `F${row.max_forecast_hour ?? "?"}`;
    header.append(title, meta);
    card.appendChild(header);
    const sourceRows = Array.isArray(row.sources) && row.sources.length
      ? row.sources
      : [{source: row.source, runs: row.runs || [], error: row.error}];
    for (const sourceRow of sourceRows) {
      const sourceCard = document.createElement("div");
      sourceCard.className = "source-cycle-card";
      const sourceHeader = document.createElement("header");
      sourceHeader.textContent = sourceLabel(sourceRow.source || "");
      const chips = document.createElement("div");
      chips.className = "run-chip-row";
      for (const run of sourceRow.runs || []) {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = run.run_str.replace("/", " ");
        const isActive = $("model").value === row.model
          && $("source").value === sourceRow.source
          && normalizeRunForCompare($("run").value) === normalizeRunForCompare(run.run_str);
        button.className = isActive ? "active" : "";
        button.addEventListener("click", () => selectLatestRun(row.model, sourceRow.source, run.run_str, false));
        chips.appendChild(button);
      }
      if (!chips.children.length) {
        const empty = document.createElement("div");
        empty.className = "meta-line";
        empty.textContent = sourceRow.error || "No cycle found";
        chips.appendChild(empty);
      }
      sourceCard.append(sourceHeader, chips);
      card.appendChild(sourceCard);
    }
    if (!sourceRows.length) {
      const empty = document.createElement("div");
      empty.className = "meta-line";
      empty.textContent = row.error || "No cycle found";
      card.appendChild(empty);
    }
    list.appendChild(card);
  }
}
function latestRunCandidate(model, source, hoursText = "") {
  const rows = state.latestRuns?.models || [];
  const row = rows.find((item) => String(item.model || "").toLowerCase() === String(model || "").toLowerCase());
  if (!row) return null;
  const sourceRows = Array.isArray(row.sources) && row.sources.length
    ? row.sources
    : [{source: row.source, latest: row.latest, runs: row.runs || []}];
  const sourceRow = sourceRows.find((item) => String(item.source || "").toLowerCase() === String(source || "").toLowerCase()) || sourceRows[0];
  const runs = sourceRow?.runs || [];
  if (!runs.length && sourceRow?.latest?.run_str) return sourceRow.latest;
  const hours = parseHourList(hoursText, $("forecastHour").value);
  const maxHour = hours.length ? Math.max(...hours) : Number($("forecastHour").value || 0);
  const needsSynopticHrrr = String(model || "").toLowerCase() === "hrrr" && maxHour > 18;
  if (needsSynopticHrrr) {
    const synoptic = runs.find((run) => {
      const parsed = parseRunString(run.run_str);
      return parsed && [0, 6, 12, 18].includes(Number(parsed.cycle));
    });
    if (synoptic) return synoptic;
  }
  return runs[0] || sourceRow?.latest || null;
}
async function resolvedLaunchRun(hoursText = "") {
  if (normalizeRunForCompare($("run").value) !== "latest") return null;
  let candidate = latestRunCandidate($("model").value, $("source").value, hoursText);
  if (!candidate?.run_str) {
    await refreshLatestRuns();
    candidate = latestRunCandidate($("model").value, $("source").value, hoursText);
  }
  return candidate?.run_str ? candidate : null;
}
async function selectLatestRun(model, source, runStr, render = false) {
  if ([...$("model").options].some((option) => option.value === model)) {
    $("model").value = model;
    $("modelTop").value = model;
    $("easyModel").value = model;
  }
  if ([...$("source").options].some((option) => option.value === source)) $("source").value = source;
  syncEasySourceOptions();
  setControlValue("easySource", source);
  setRunValue(runStr, {save: false, status: `${String(model).toUpperCase()} ${runStr}`});
  await loadProducts();
  renderDomainOptions();
  syncEasyFromMain();
  applyEasyPreset($("easyPreset").value, {save: false});
  saveViewerState();
  renderLatestRuns();
  if (render) easyBuildAndPlot();
}
function updateAutoLatestTimer() {
  if (state.autoLatestTimer) clearInterval(state.autoLatestTimer);
  state.autoLatestTimer = null;
  if (!$("easyAutoLatest").checked) return;
  state.autoLatestTimer = setInterval(() => {
    if (String($("run").value || "").trim().toLowerCase() === "latest") {
      easyBuildAndPlot();
    } else {
      refreshLatestRuns();
    }
  }, 10 * 60 * 1000);
}
function openSetupOverlay() {
  $("setupOverlay").classList.remove("hidden");
  renderSetupOverlay();
}
function closeSetupOverlay() {
  $("setupOverlay").classList.add("hidden");
}
function renderSetupOverlay() {
  if (!$("setupModelList")) return;
  const tierLabel = easyIsCustomPreset() ? "Custom" : PRODUCT_TIER_LABELS[normalizedProductTier()];
  $("setupOverlayMeta").textContent = `${$("model").value.toUpperCase()} | ${presetById(state.easyPresetId).name} | ${tierLabel}`;
  $("setupModelList").innerHTML = "";
  for (const model of state.models) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "selector-card" + (model.id === $("model").value ? " active" : "");
    const title = document.createElement("strong");
    title.textContent = model.id.toUpperCase();
    const summary = document.createElement("span");
    summary.textContent = `${model.default_source || ""} | F${model.max_forecast_hour ?? 0} | ${(model.cycle_hours_utc || []).length} cycles`;
    button.append(title, summary);
    button.addEventListener("click", async () => {
      $("easyModel").value = model.id;
      await syncMainFromEasy({load: true});
      renderSetupOverlay();
      saveViewerState();
    });
    $("setupModelList").appendChild(button);
  }
  $("setupPresetList").innerHTML = "";
  for (const preset of PRESET_WORKFLOWS) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "selector-card" + (preset.id === state.easyPresetId ? " active" : "");
    const title = document.createElement("strong");
    title.textContent = preset.name;
    const summary = document.createElement("span");
    summary.textContent = preset.id === "custom"
      ? `${preset.summary} | ${easyCustomProductArray({fallback: false}).length} selected`
      : `${preset.summary} | ${presetProducts(preset.id).length} available`;
    button.append(title, summary);
    button.addEventListener("click", () => {
      applyEasyPreset(preset.id);
      renderSetupOverlay();
    });
    $("setupPresetList").appendChild(button);
  }
  if ($("setupTierList")) {
    $("setupTierList").innerHTML = "";
    for (const [tier, label] of Object.entries(PRODUCT_TIER_LABELS)) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "selector-card" + (tier === normalizedProductTier() ? " active" : "");
      const title = document.createElement("strong");
      title.textContent = label;
      const summary = document.createElement("span");
      summary.textContent = tier === "direct"
        ? "fastest useful maps"
        : (tier === "derived" ? "adds workflow diagnostics" : "adds ECAPE-heavy products");
      button.append(title, summary);
      button.addEventListener("click", () => {
        $("easyProductTier").value = tier;
        state.easyProductTier = tier;
        state.selectedProducts = new Set(easyProductSetProducts());
        renderProductList();
        renderEasyPresetSummary();
        saveViewerState();
      });
      $("setupTierList").appendChild(button);
    }
  }
  renderSetupCustomProducts();
}
function utcDateInputValue(date = new Date()) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}
function ymdFromDateInput(value) {
  const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return match ? `${match[1]}${match[2]}${match[3]}` : "";
}
function dateInputFromYmd(ymd) {
  const value = String(ymd || "");
  return /^\d{8}$/.test(value) ? `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}` : "";
}
function normalizeCycle(value) {
  const hour = Math.max(0, Math.min(23, Math.round(Number(value) || 0)));
  return String(hour).padStart(2, "0");
}
function parseRunString(value) {
  const text = String(value || "").trim().toLowerCase().replaceAll("_", " ").replaceAll("z", " ");
  if (!text || text === "latest") return null;
  const patterns = [
    /(?<ymd>\d{8})\D*(?<hour>\d{1,2})/,
    /(?<ymdh>\d{10})/,
    /(?<year>\d{4})[-/](?<month>\d{1,2})[-/](?<day>\d{1,2})\D+(?<hour>\d{1,2})/,
    /(?<month>\d{1,2})[-/](?<day>\d{1,2})[-/](?<year>\d{2,4})\D+(?<hour>\d{1,2})/
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match?.groups) continue;
    let ymd = "";
    let hour = 0;
    if (match.groups.ymdh) {
      ymd = match.groups.ymdh.slice(0, 8);
      hour = Number(match.groups.ymdh.slice(8, 10));
    } else if (match.groups.ymd) {
      ymd = match.groups.ymd;
      hour = Number(match.groups.hour);
    } else {
      let year = Number(match.groups.year);
      if (year < 100) year += year < 70 ? 2000 : 1900;
      ymd = `${String(year).padStart(4, "0")}${String(Number(match.groups.month)).padStart(2, "0")}${String(Number(match.groups.day)).padStart(2, "0")}`;
      hour = Number(match.groups.hour);
    }
    if (/^\d{8}$/.test(ymd) && Number.isFinite(hour) && hour >= 0 && hour <= 23) {
      return {ymd, cycle: normalizeCycle(hour), runStr: `${ymd}/${normalizeCycle(hour)}z`};
    }
  }
  return null;
}
function runDateFromParsed(parsed) {
  if (!parsed?.ymd) return null;
  const year = Number(parsed.ymd.slice(0, 4));
  const month = Number(parsed.ymd.slice(4, 6)) - 1;
  const day = Number(parsed.ymd.slice(6, 8));
  const hour = Number(parsed.cycle || 0);
  const time = Date.UTC(year, month, day, hour, 0, 0);
  return Number.isFinite(time) ? new Date(time) : null;
}
function runIsFuture(value, graceHours = 6) {
  const parsed = parseRunString(value);
  const dt = runDateFromParsed(parsed);
  if (!dt) return false;
  return dt.getTime() > Date.now() + graceHours * 3600 * 1000;
}
function validRestoredRun(value) {
  return !runIsFuture(value);
}
function restoredPreviewAllowed(preview) {
  const runStr = runStringFromPreview(preview);
  return !runStr || validRestoredRun(runStr);
}
function syncArchiveControlsFromRun() {
  const parsed = parseRunString($("run").value);
  if (parsed) {
    $("runDate").value = dateInputFromYmd(parsed.ymd);
    $("runCycle").value = parsed.cycle;
    $("archiveRunMeta").textContent = `${parsed.ymd}/${parsed.cycle}z pinned`;
  } else {
    if (!$("runDate").value) $("runDate").value = utcDateInputValue();
    $("archiveRunMeta").textContent = "latest run";
  }
  for (const button of $("runCycleButtons").querySelectorAll("button")) {
    button.classList.toggle("active", button.dataset.cycle === $("runCycle").value);
  }
}
function setRunValue(value, {save = true, status = ""} = {}) {
  if (runIsFuture(value)) {
    setStatus(`Run ${parseRunString(value)?.runStr || value} is in the future; using latest.`, true);
    value = "latest";
    status = "";
  }
  $("run").value = value;
  syncRunControls(false);
  syncArchiveControlsFromRun();
  updateActiveMapFreshness();
  if (status) setStatus(status);
  if (save) saveViewerState();
}
function useArchiveRun({save = true, announce = true, render = false} = {}) {
  if (!$("runDate").value) $("runDate").value = utcDateInputValue();
  const ymd = ymdFromDateInput($("runDate").value);
  const cycle = normalizeCycle($("runCycle").value);
  $("runCycle").value = cycle;
  if (!ymd) {
    setStatus("Choose a valid archive date.", true);
    return;
  }
  const archiveSource = setArchiveSourceForModel();
  setRunValue(`${ymd}/${cycle}z`, {
    save,
    status: announce ? `Archive run ${ymd}/${cycle}z selected${archiveSource ? ` | ${archiveSource}` : ""}` : ""
  });
  if (render) scheduleRenderCurrentMap("Rendering archive map");
}
function useLatestRun() {
  setDefaultSourceForModel();
  setRunValue("latest", {status: "Latest run selected"});
  scheduleRenderCurrentMap("Rendering latest map");
}
function snapshotControls() {
  return {
    model: $("model").value,
    source: $("source").value,
    run: $("run").value,
    runDate: $("runDate").value,
    runCycle: $("runCycle").value,
    forecastHour: $("forecastHour").value,
    renderHours: $("renderHours").value,
    prepareHours: $("prepareHours").value,
    localizeHours: $("localizeHours").value,
    localizeJobs: $("localizeJobs").value,
    localizeMapData: $("localizeMapData").checked,
    localizeSoundings: $("localizeSoundings").checked,
    localizeWxStore: $("localizeWxStore").checked,
    localizeRenderAfter: $("localizeRenderAfter").checked,
    resourcePreset: $("resourcePreset")?.value || "balanced",
    domainSearch: $("domainSearch").value,
    domain: $("domain").value,
    productGroup: $("kind").value,
    productSearch: $("productSearch").value,
    products: selectedProductArray(),
    width: $("width").value,
    height: $("height").value,
    renderMode: $("renderMode").value,
    soundingLat: $("soundingLat").value,
    soundingLon: $("soundingLon").value,
    soundingHour: $("soundingHour").value,
    soundingDataMode: $("soundingDataMode").value,
    soundingMethod: $("soundingMethod").value,
    soundingBoxKm: $("soundingBoxKm").value,
    soundingBoxShape: $("soundingBoxShape").value,
    soundingBoxWidthKm: $("soundingBoxWidthKm").value,
    soundingBoxHeightKm: $("soundingBoxHeightKm").value,
    clickSoundingMode: $("clickSoundingMode").value,
    autoOpenSounding: $("autoOpenSounding").checked,
    soundingStation: $("soundingStation").value,
    displayMode: state.displayMode,
    viewMode: state.viewMode,
    easyPresetId: state.easyPresetId,
    easyProductTier: state.easyProductTier,
    easyCustomProducts: [...state.easyCustomProducts],
    easySource: $("easySource")?.value || $("source").value,
    easyHours: $("easyHours").value,
    easyCustomHours: $("easyCustomHours")?.value || "",
    easySelectedHours: [...state.easySelectedHours],
    easyWarmStore: $("easyWarmStore").checked,
    easyAutoLatest: $("easyAutoLatest").checked,
    activeWorkflowId: state.activeWorkflowId,
    workflowAutoApply: $("workflowAutoApply").checked
  };
}
function saveViewerState() {
  if (state.restoring) return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      savedAt: new Date().toISOString(),
      controls: snapshotControls(),
      activePreview: state.activePreview,
      previews: state.previews.slice(0, 40),
      soundings: state.soundings.slice(0, 20).map(persistableSounding),
      soundingContext: state.soundingContext
    }));
  } catch (_err) {}
}
function persistableSounding(item) {
  return {
    id: item.id,
    preview: item.preview,
    lat: item.lat,
    lon: item.lon,
    hour: item.hour,
    backend: item.backend,
    boxLabel: item.boxLabel || ""
  };
}
function restoreMedia(saved) {
  if (!saved) return;
  state.previews = Array.isArray(saved.previews)
    ? saved.previews.filter((item) => item?.url && restoredPreviewAllowed(item)).slice(0, 40)
    : [];
  state.soundingContext = saved.soundingContext && !runIsFuture(saved.soundingContext.runStr) ? saved.soundingContext : null;
  const savedActive = saved.activePreview?.url && restoredPreviewAllowed(saved.activePreview) ? saved.activePreview : null;
  const active = savedActive || state.previews[0];
  if (active) setActivePreview(active);
  else renderPreviewStrip();
  state.soundings = Array.isArray(saved.soundings)
    ? saved.soundings.filter((item) => item?.preview?.url && restoredPreviewAllowed(item.preview)).slice(0, 20)
    : [];
  syncSoundingPanelForActivePreview(state.activePreview);
  renderSoundings();
  renderLatestSounding(state.soundings[0]);
}
function loadWorkflowStore() {
  try {
    const raw = localStorage.getItem(WORKFLOW_KEY);
    const data = raw ? JSON.parse(raw) : {};
    state.productWorkflows = Array.isArray(data.workflows) ? data.workflows.filter((item) => item?.id && item?.name) : [];
    state.activeWorkflowId = data.activeWorkflowId || state.productWorkflows[0]?.id || "";
    $("workflowAutoApply").checked = data.autoApply !== false;
  } catch (_err) {
    state.productWorkflows = [];
    state.activeWorkflowId = "";
  }
}
function saveWorkflowStore() {
  try {
    localStorage.setItem(WORKFLOW_KEY, JSON.stringify({
      version: 1,
      activeWorkflowId: state.activeWorkflowId,
      autoApply: $("workflowAutoApply").checked,
      workflows: state.productWorkflows
    }));
  } catch (_err) {}
}
function renderWorkflowControls() {
  const select = $("productWorkflowSelect");
  select.innerHTML = "";
  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = "None";
  select.appendChild(empty);
  for (const workflow of state.productWorkflows) {
    const option = document.createElement("option");
    option.value = workflow.id;
    option.textContent = `${workflow.name} (${workflow.products?.length || 0})`;
    select.appendChild(option);
  }
  if (state.productWorkflows.some((workflow) => workflow.id === state.activeWorkflowId)) {
    select.value = state.activeWorkflowId;
    $("productWorkflowName").value = state.productWorkflows.find((workflow) => workflow.id === state.activeWorkflowId)?.name || "";
  } else {
    state.activeWorkflowId = "";
    select.value = "";
    $("productWorkflowName").value = "";
  }
}
function availableProductSlugs() {
  return new Set(productRows().map((row) => row.slug));
}
function applyProductWorkflow(id = state.activeWorkflowId, options = {}) {
  const workflow = state.productWorkflows.find((item) => item.id === id);
  if (!workflow) return false;
  const available = availableProductSlugs();
  const next = (workflow.products || []).filter((slug) => available.has(slug));
  if (!next.length) {
    setStatus(`Workflow ${workflow.name} has no products for ${$("model").value.toUpperCase()}`, true);
    return false;
  }
  state.activeWorkflowId = workflow.id;
  state.selectedProducts = new Set(next);
  renderWorkflowControls();
  renderProductList();
  if (options.save !== false) {
    saveWorkflowStore();
    saveViewerState();
  }
  setStatus(`Workflow ${workflow.name} applied`);
  return true;
}
function saveProductWorkflow() {
  const products = selectedProductArray();
  if (!products.length) {
    setStatus("Select at least one product before saving a workflow.", true);
    return;
  }
  const current = state.productWorkflows.find((item) => item.id === $("productWorkflowSelect").value);
  const name = $("productWorkflowName").value.trim() || current?.name || `${$("model").value.toUpperCase()} workflow`;
  const now = new Date().toISOString();
  if (current) {
    current.name = name;
    current.products = products;
    current.updatedAt = now;
    state.activeWorkflowId = current.id;
  } else {
    const workflow = {
      id: `wf_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`,
      name,
      products,
      createdAt: now,
      updatedAt: now
    };
    state.productWorkflows.unshift(workflow);
    state.activeWorkflowId = workflow.id;
  }
  saveWorkflowStore();
  renderWorkflowControls();
  saveViewerState();
  setStatus(`Workflow ${name} saved`);
}
function deleteProductWorkflow() {
  const id = $("productWorkflowSelect").value;
  if (!id) return;
  state.productWorkflows = state.productWorkflows.filter((workflow) => workflow.id !== id);
  state.activeWorkflowId = state.productWorkflows[0]?.id || "";
  saveWorkflowStore();
  renderWorkflowControls();
  saveViewerState();
}
function api(path) {
  return fetch(path).then(async (response) => {
    const data = await response.json();
    if (!response.ok || data.ok === false) throw new Error(data.error || response.statusText);
    return data;
  });
}
function post(path, payload, options = {}) {
  return fetch(path, {
    method: "POST",
    headers: {"content-type": "application/json"},
    signal: options.signal,
    body: JSON.stringify(payload)
  }).then(async (response) => {
    const data = await response.json();
    if (!response.ok || data.ok === false) return data;
    return data;
  });
}
function setStatus(text, isError = false) {
  $("statusText").textContent = text;
  $("statusText").className = isError ? "error-text" : "";
}
const RESULT_SKIP_KEYS = new Set(["report", "geojson", "grid", "levels", "points", "column", "profile", "box_profile"]);
function compactResult(value, depth = 0) {
  if (value === null || typeof value !== "object") return value;
  if (depth > 4) return "[truncated]";
  if (Array.isArray(value)) {
    const items = value.slice(0, 18).map((item) => compactResult(item, depth + 1));
    if (value.length > 18) items.push(`+${value.length - 18} more`);
    return items;
  }
  const out = {};
  for (const [key, item] of Object.entries(value)) {
    if (RESULT_SKIP_KEYS.has(key)) {
      out[key] = "[omitted from UI preview]";
      continue;
    }
    if (key === "sounding" && item && typeof item === "object") {
      out[key] = {
        ok: item.ok,
        station_id: item.station_id,
        sample_method: item.sample_method,
        box_radius_lat_deg: item.box_radius_lat_deg,
        box_radius_lon_deg: item.box_radius_lon_deg
      };
      continue;
    }
    out[key] = compactResult(item, depth + 1);
  }
  return out;
}
function showResult(value) {
  const text = JSON.stringify(compactResult(value), null, 2);
  $("result").textContent = text.length > 12000 ? `${text.slice(0, 12000)}\n... result preview truncated ...` : text;
}
function syncRunControls(fromTop = false) {
  const pairs = [
    [$("model"), $("modelTop")],
    [$("run"), $("runTop")],
    [$("forecastHour"), $("hourTop")]
  ];
  for (const [side, top] of pairs) {
    if (fromTop) side.value = top.value;
    else top.value = side.value;
  }
}
function modelEntry() {
  return state.models.find((model) => model.id === $("model").value) || state.models[0] || {};
}
function modelById(modelId) {
  return state.models.find((model) => model.id === modelId) || {};
}
function sourceLabel(source) {
  return String(source || "").toUpperCase();
}
function availableSourcesForModel(modelId = $("model").value) {
  const model = modelById(modelId);
  const sources = Array.isArray(model.sources) ? model.sources.map((source) => String(source.id || "").toLowerCase()).filter(Boolean) : [];
  return sources.length ? [...new Set(sources)] : ["aws"];
}
function sourceSupportedByModel(source, modelId = $("model").value) {
  return availableSourcesForModel(modelId).includes(String(source || "").toLowerCase());
}
function syncEasySourceOptions() {
  if (!$("easySource")) return;
  const previous = $("easySource").value || $("source").value;
  const modelId = $("easyModel").value || $("model").value;
  $("easySource").innerHTML = "";
  for (const source of availableSourcesForModel(modelId)) {
    const option = document.createElement("option");
    option.value = source;
    option.textContent = sourceLabel(source);
    $("easySource").appendChild(option);
  }
  const desired = sourceSupportedByModel(previous, modelId)
    ? previous
    : (modelById(modelId).default_source || availableSourcesForModel(modelId)[0] || "aws");
  if ([...$("easySource").options].some((option) => option.value === desired)) $("easySource").value = desired;
}
function setDefaultSourceForModel() {
  const model = modelEntry();
  const desired = model.default_source || SOURCE_DEFAULTS.get(model.id) || "aws";
  if ([...$("source").options].some((option) => option.value === desired)) $("source").value = desired;
  if ($("easySource")) syncEasySourceOptions();
}
function setArchiveSourceForModel() {
  const model = modelEntry();
  const desired = model.archive_source || ARCHIVE_SOURCE_DEFAULTS.get($("model").value);
  if (!desired || $("source").value !== "nomads") return "";
  if ([...$("source").options].some((option) => option.value === desired)) {
    $("source").value = desired;
    if ($("easySource")) syncEasySourceOptions();
    return desired;
  }
  return "";
}
function parseHourList(value, fallback) {
  const text = String(value || fallback || "0").trim();
  const out = [];
  for (const part of text.split(",")) {
    const item = part.trim();
    if (!item) continue;
    if (item.includes("-")) {
      const [a, b] = item.split("-").map((x) => Number(x.trim()));
      if (Number.isFinite(a) && Number.isFinite(b)) {
        const step = a <= b ? 1 : -1;
        for (let h = a; step > 0 ? h <= b : h >= b; h += step) out.push(h);
      }
    } else {
      const hour = Number(item);
      if (Number.isFinite(hour)) out.push(hour);
    }
  }
  return [...new Set(out.map((h) => Math.max(0, Math.round(h))))].slice(0, 80);
}
function setHour(hour) {
  const max = Number(modelEntry().max_forecast_hour || 43848);
  const next = Math.max(0, Math.min(max, Math.round(Number(hour) || 0)));
  $("forecastHour").value = next;
  $("hourTop").value = next;
  $("renderHours").value = String(next);
  $("prepareHours").value = String(next);
  $("soundingHour").value = String(next);
  renderTimeline();
  syncEasyFromMain();
  updateActiveMapFreshness();
  saveViewerState();
}
function renderTimeline() {
  const base = Number($("forecastHour").value || 0);
  const max = Number(modelEntry().max_forecast_hour || 48);
  const start = Math.max(0, Math.floor(base / 8) * 8);
  $("timeline").innerHTML = "";
  for (let hour = start; hour <= Math.min(max, start + 31); hour += 1) {
    const button = document.createElement("button");
    button.textContent = String(hour).padStart(3, "0");
    button.className = hour === base ? "active" : "";
    button.addEventListener("click", () => setHour(hour));
    $("timeline").appendChild(button);
  }
}
function productLabel(slug) {
  return String(slug || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (m) => m.toUpperCase())
    .replace(/\bMb\b/g, "mb")
    .replace(/\bKm\b/g, "km")
    .replace(/\bQpf\b/g, "QPF")
    .replace(/\bCape\b/g, "CAPE")
    .replace(/\bSbcape\b/g, "SBCAPE")
    .replace(/\bMlcape\b/g, "MLCAPE")
    .replace(/\bMucape\b/g, "MUCAPE")
    .replace(/\bSbcin\b/g, "SBCIN")
    .replace(/\bMlcin\b/g, "MLCIN")
    .replace(/\bCin\b/g, "CIN")
    .replace(/\bSrh\b/g, "SRH")
    .replace(/\bStp\b/g, "STP")
    .replace(/\bScp\b/g, "SCP")
    .replace(/\bUh\b/g, "UH")
    .replace(/\bRh\b/g, "RH")
    .replace(/\bMslp\b/g, "MSLP")
    .replace(/\bEhi\b/g, "EHI")
    .replace(/\bEcape\b/g, "ECAPE")
    .replace(/\bDcape\b/g, "DCAPE")
    .replace(/\bVpd\b/g, "VPD")
    .replace(/\bPwat\b/g, "PWAT");
}
function semanticGroup(slug, backendKind) {
  const s = String(slug);
  if (backendKind === "windowed" || /qpf|precip|rain|snow|ice|freezing|sleet/.test(s)) return "precip";
  if (/cape|cin|lcl|li|lapse|shear|srh|ehi|stp|scp|uh|helicity|ecape|dcape/.test(s)) return "severe";
  if (/smoke|fire|vpd|heat_index|wind_chill|apparent/.test(s)) return "fire";
  if (/snow|ice|freezing|winter/.test(s)) return "winter";
  if (/(200|250|300|500|700|850|925)mb|height_winds|vorticity/.test(s)) return "upper_air";
  if (/2m|10m|mslp|gust|dewpoint|visibility|cloud|reflectivity|satellite|pw|precipitable/.test(s)) return "surface";
  return backendKind;
}
function groupLabel(group) {
  return {
    upper_air: "Upper Air",
    surface: "Surface",
    precip: "Precipitation",
    severe: "Severe Weather",
    winter: "Winter",
    fire: "Fire and Smoke",
    direct: "Direct",
    light_derived: "Derived",
    heavy_derived: "Heavy Derived",
    windowed: "Windowed"
  }[group] || productLabel(group);
}
function productRows() {
  const groups = state.products?.groups || {};
  const rows = [];
  for (const [backendKind, slugs] of Object.entries(groups)) {
    for (const slug of slugs || []) {
      rows.push({slug, backendKind, semantic: semanticGroup(slug, backendKind)});
    }
  }
  return rows;
}
function renderProductList() {
  const selectedKind = $("kind").value;
  const search = $("productSearch").value.trim().toLowerCase();
  const rows = productRows().filter((row) => {
    const text = `${row.slug} ${productLabel(row.slug)} ${row.backendKind} ${row.semantic}`.toLowerCase();
    if (selectedKind && selectedKind !== row.backendKind && selectedKind !== row.semantic) return false;
    return !search || text.includes(search);
  });
  const grouped = new Map();
  for (const row of rows) {
    const key = row.semantic || row.backendKind;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  }
  const order = ["upper_air", "surface", "precip", "severe", "winter", "fire", "direct", "light_derived", "heavy_derived", "windowed"];
  $("productList").innerHTML = "";
  for (const key of [...grouped.keys()].sort((a, b) => order.indexOf(a) - order.indexOf(b))) {
    const box = document.createElement("div");
    box.className = "product-group";
    const h = document.createElement("h3");
    h.textContent = `${groupLabel(key)} (${grouped.get(key).length})`;
    box.appendChild(h);
    for (const row of grouped.get(key).sort((a, b) => productLabel(a.slug).localeCompare(productLabel(b.slug)))) {
      const label = document.createElement("label");
      label.className = "product-item" + (state.selectedProducts.has(row.slug) ? " selected" : "");
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = state.selectedProducts.has(row.slug);
      input.addEventListener("change", () => {
        if (input.checked) state.selectedProducts.add(row.slug);
        else state.selectedProducts.delete(row.slug);
        if (easyIsCustomPreset()) state.easyCustomProducts = new Set(state.selectedProducts);
        renderProductList();
        renderSetupCustomProducts();
        renderProductPicker();
        renderEasyPresetSummary();
        saveViewerState();
      });
      const text = document.createElement("span");
      text.textContent = productLabel(row.slug);
      const small = document.createElement("small");
      small.textContent = `${row.slug} | ${row.backendKind}`;
      text.appendChild(small);
      label.append(input, text);
      box.appendChild(label);
    }
    $("productList").appendChild(box);
  }
}
function filteredCustomProductRows(groupId, searchId) {
  const selectedKind = $(groupId)?.value || "";
  const search = ($(searchId)?.value || "").trim().toLowerCase();
  return productRows().filter((row) => {
    const text = `${row.slug} ${productLabel(row.slug)} ${row.backendKind} ${row.semantic}`.toLowerCase();
    if (selectedKind && selectedKind !== row.backendKind && selectedKind !== row.semantic) return false;
    return !search || text.includes(search);
  });
}
function renderCustomProductPicker({groupId, searchId, metaId, listId, headerMetaId = ""}) {
  if (!$(listId)) return;
  const rows = filteredCustomProductRows(groupId, searchId);
  const selectedCount = easyCustomProductArray({fallback: false}).length;
  const meta = `${selectedCount} custom product${selectedCount === 1 ? "" : "s"} selected | ${rows.length} shown`;
  if ($(metaId)) $(metaId).textContent = meta;
  if (headerMetaId && $(headerMetaId)) $(headerMetaId).textContent = meta;
  const grouped = new Map();
  for (const row of rows) {
    const key = row.semantic || row.backendKind;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  }
  const order = ["upper_air", "surface", "precip", "severe", "winter", "fire", "direct", "light_derived", "heavy_derived", "windowed"];
  $(listId).innerHTML = "";
  for (const key of [...grouped.keys()].sort((a, b) => order.indexOf(a) - order.indexOf(b))) {
    const box = document.createElement("div");
    box.className = "product-group";
    const h = document.createElement("h3");
    h.textContent = `${groupLabel(key)} (${grouped.get(key).length})`;
    box.appendChild(h);
    for (const row of grouped.get(key).sort((a, b) => productLabel(a.slug).localeCompare(productLabel(b.slug)))) {
      const label = document.createElement("label");
      label.className = "product-item" + (state.easyCustomProducts.has(row.slug) ? " selected" : "");
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = state.easyCustomProducts.has(row.slug);
      input.addEventListener("change", () => {
        if (input.checked) state.easyCustomProducts.add(row.slug);
        else state.easyCustomProducts.delete(row.slug);
        if (easyIsCustomPreset()) {
          state.selectedProducts = new Set(easyCustomProductArray());
          renderProductList();
          renderEasyPresetSummary();
        } else {
          renderSetupCustomProducts();
        }
        renderProductPicker();
        saveViewerState();
      });
      const text = document.createElement("span");
      text.textContent = productLabel(row.slug);
      const small = document.createElement("small");
      small.textContent = `${row.slug} | ${row.backendKind}`;
      text.appendChild(small);
      label.append(input, text);
      box.appendChild(label);
    }
    $(listId).appendChild(box);
  }
}
function renderSetupCustomProducts() {
  renderCustomProductPicker({
    groupId: "setupProductGroup",
    searchId: "setupProductSearch",
    metaId: "setupCustomMeta",
    listId: "setupProductList"
  });
}
function renderProductPicker() {
  renderCustomProductPicker({
    groupId: "productPickerGroup",
    searchId: "productPickerSearch",
    metaId: "productPickerMeta",
    headerMetaId: "productPickerHeaderMeta",
    listId: "productPickerList"
  });
}
function selectVisibleProductPickerRows() {
  for (const row of filteredCustomProductRows("productPickerGroup", "productPickerSearch")) {
    state.easyCustomProducts.add(row.slug);
  }
  if (easyIsCustomPreset()) {
    state.selectedProducts = new Set(easyCustomProductArray());
    renderProductList();
    renderEasyPresetSummary();
  }
  renderSetupCustomProducts();
  renderProductPicker();
  saveViewerState();
}
function clearCustomProducts() {
  state.easyCustomProducts = new Set();
  if (easyIsCustomPreset()) {
    state.selectedProducts = new Set();
    state.selectedProducts = new Set(easyProductSetProducts());
    renderProductList();
    renderEasyPresetSummary();
  } else {
    renderSetupCustomProducts();
  }
  renderProductPicker();
  saveViewerState();
}
function openProductPicker() {
  if (!state.easyCustomProducts.size) state.easyCustomProducts = new Set(selectedProductArray());
  applyEasyPreset("custom", {save: false});
  $("productPickerOverlay").classList.remove("hidden");
  renderProductPicker();
  saveViewerState();
}
function closeProductPicker() {
  $("productPickerOverlay").classList.add("hidden");
}
function domainLabel(domain) {
  return `${domain.label || domain.slug} (${domain.slug})`;
}
function domainOption(domain) {
  const option = document.createElement("option");
  option.value = domain.slug;
  const supported = domainSupportedByModel($("model").value, domain);
  option.textContent = supported ? domainLabel(domain) : `${domainLabel(domain)} - needs global model`;
  option.disabled = !supported;
  if (!supported) option.title = `${$("model").value.toUpperCase()} does not cover this domain. Use GFS or another global model.`;
  return option;
}
function renderDomainOptions() {
  const previous = $("domain").value || "conus";
  const search = $("domainSearch").value.trim().toLowerCase();
  $("domain").innerHTML = "";
  const added = new Set();
  const nativeDomain = nativeDomainForModel();
  if (search && nativeDomain) {
    const native = document.createElement("optgroup");
    native.label = "Native";
    native.appendChild(domainOption(nativeDomain));
    $("domain").appendChild(native);
    added.add(nativeDomain.slug);
  }
  if (!search) {
    const common = document.createElement("optgroup");
    common.label = "Common";
    for (const slug of COMMON_DOMAINS) {
      const domain = state.domains.find((item) => item.slug === slug);
      if (!domain || added.has(slug)) continue;
      common.appendChild(domainOption(domain));
      added.add(slug);
    }
    if (common.children.length) $("domain").appendChild(common);
  }
  const matches = state.domains.filter((domain) => {
    if (added.has(domain.slug)) return false;
    if (!search) return true;
    return String(domain.slug || "").toLowerCase().includes(search)
      || String(domain.label || "").toLowerCase().includes(search)
      || (domain.tags || []).some((tag) => String(tag).toLowerCase().includes(search));
  }).slice(0, search ? 600 : 1200);
  const byKind = new Map();
  for (const domain of matches) {
    const kind = String(domain.kind || "other").replaceAll("_", " ");
    if (!byKind.has(kind)) byKind.set(kind, []);
    byKind.get(kind).push(domain);
  }
  for (const [kind, domains] of [...byKind.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const group = document.createElement("optgroup");
    group.label = kind;
    for (const domain of domains.sort((a, b) => domainLabel(a).localeCompare(domainLabel(b)))) group.appendChild(domainOption(domain));
    $("domain").appendChild(group);
  }
  const options = [...$("domain").options];
  const values = options.map((option) => option.value);
  const enabledValues = options.filter((option) => !option.disabled).map((option) => option.value);
  const previousEnabled = options.some((option) => option.value === previous && !option.disabled);
  const nativeSlug = nativeDomain?.slug || "";
  const nextValue = previousEnabled
    ? previous
    : (enabledValues.includes(nativeSlug) ? nativeSlug : (enabledValues.includes("conus") ? "conus" : (enabledValues[0] || values[0])));
  if (nextValue && nextValue !== previous) state.soundingContext = null;
  if (nextValue) $("domain").value = nextValue;
  updateDomainModelMeta();
}
function activeBounds() {
  const domain = currentDomainEntry();
  return domain?.bounds || [-125, -66, 24, 50];
}
function currentDomainEntry() {
  return state.domains.find((item) => item.slug === $("domain").value);
}
function normalBounds(bounds) {
  if (!Array.isArray(bounds) || bounds.length !== 4) return null;
  const values = bounds.map(Number);
  if (values.some((value) => !Number.isFinite(value))) return null;
  return values;
}
function boundsFitWithin(inner, outer) {
  const a = normalBounds(inner);
  const b = normalBounds(outer);
  if (!a || !b) return true;
  if (a[0] > a[1] || b[0] > b[1]) return false;
  return a[0] >= b[0] - 0.05
    && a[1] <= b[1] + 0.05
    && a[2] >= b[2] - 0.05
    && a[3] <= b[3] + 0.05;
}
function domainSupportedByModel(modelId, domain = currentDomainEntry()) {
  if (!domain) return true;
  if (GLOBAL_MODELS.has(modelId)) return true;
  const coverage = MODEL_NATIVE_BOUNDS.get(modelId);
  if (!coverage) return true;
  return boundsFitWithin(domain.bounds, coverage);
}
function preferredModelForDomain(domain = currentDomainEntry()) {
  if (!domain) return "gfs";
  const bounds = normalBounds(domain.bounds);
  if (!bounds) return "gfs";
  if (boundsFitWithin(bounds, MODEL_NATIVE_BOUNDS.get("hrrr"))) return "hrrr";
  if (boundsFitWithin(bounds, MODEL_NATIVE_BOUNDS.get("hrrr-ak"))) return "hrrr-ak";
  return "gfs";
}
function nativeDomainForModel(modelId = $("model").value) {
  const slug = MODEL_NATIVE_DOMAINS.get(modelId);
  return slug ? state.domains.find((item) => item.slug === slug) : null;
}
function coerceDomainForRegionalModel({announce = true, save = true} = {}) {
  const modelId = $("model").value;
  const domain = currentDomainEntry();
  if (!domain || domainSupportedByModel(modelId, domain)) return false;
  const nativeDomain = nativeDomainForModel(modelId);
  if (!nativeDomain) return false;
  $("domain").value = nativeDomain.slug;
  state.soundingContext = null;
  updateDomainModelMeta();
  updateActiveMapFreshness();
  if (announce) {
    setStatus(`${modelId.toUpperCase()} does not cover ${domain.label || domain.slug}; using ${nativeDomain.label || nativeDomain.slug}.`);
  }
  if (save) saveViewerState();
  return true;
}
function domainCompatibilityMessage(modelId = $("model").value, domain = currentDomainEntry()) {
  if (!domain) return "";
  if (domainSupportedByModel(modelId, domain)) {
    return GLOBAL_MODELS.has(modelId)
      ? `${modelId.toUpperCase()} can render ${domain.label || domain.slug}.`
      : `${modelId.toUpperCase()} native grid covers this domain.`;
  }
  const nativeDomain = nativeDomainForModel(modelId);
  const nativeText = nativeDomain ? ` Render uses ${nativeDomain.label || nativeDomain.slug} for that model.` : "";
  return `${modelId.toUpperCase()} is regional and does not cover ${domain.label || domain.slug}.${nativeText} Choose GFS for real global data.`;
}
function updateDomainModelMeta() {
  const el = $("domainModelMeta");
  if (!el) return;
  const modelId = $("model").value;
  el.textContent = domainCompatibilityMessage(modelId);
  el.className = domainSupportedByModel(modelId) ? "meta-line" : "meta-line error-text";
}
async function ensureDomainModelCompatibility({autoSwitch = false} = {}) {
  const domain = currentDomainEntry();
  if (!domain || domainSupportedByModel($("model").value, domain)) {
    updateDomainModelMeta();
    return false;
  }
  const nextModel = preferredModelForDomain(domain);
  if (autoSwitch && nextModel && [...$("model").options].some((option) => option.value === nextModel)) {
    for (const select of [$("model"), $("modelTop")]) select.value = nextModel;
    setDefaultSourceForModel();
    await loadProducts();
    syncRunControls(false);
    updateDomainModelMeta();
    setStatus(`Switched to ${nextModel.toUpperCase()} for ${domain.label || domain.slug}`);
    saveViewerState();
    return true;
  }
  updateDomainModelMeta();
  setStatus(domainCompatibilityMessage($("model").value, domain), true);
  return false;
}
function payloadBase() {
  const topRunControlsHidden = getComputedStyle($("modelTop").closest(".top-field")).display === "none";
  syncRunControls(!topRunControlsHidden);
  return {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    domain: $("domain").value,
    bounds: activeBounds(),
    width: Number($("width").value || 1600),
    height: Number($("height").value || 1100)
  };
}
function normalizeRunForCompare(value) {
  const text = String(value || "").trim();
  if (!text || text.toLowerCase() === "latest") return "latest";
  return parseRunString(text)?.runStr || text;
}
function activeMapStaleReason() {
  const img = $("plotView").querySelector(".plot-map");
  if (!img) return "";
  const expectedRun = normalizeRunForCompare($("run").value);
  const actualRun = normalizeRunForCompare(img.dataset.runStr || "");
  const expectedHour = String(Math.round(Number($("forecastHour").value || 0)));
  const actualHour = String(Math.round(Number(img.dataset.forecastHour || 0)));
  const checks = [
    [img.dataset.model, $("model").value, "model"],
    [img.dataset.source, $("source").value, "source"],
    [img.dataset.domain, $("domain").value, "domain"],
    [actualHour, expectedHour, "hour"]
  ];
  if (expectedRun && expectedRun !== "latest") checks.push([actualRun, expectedRun, "run"]);
  for (const [actual, expected, label] of checks) {
    if (actual && expected && String(actual).toLowerCase() !== String(expected).toLowerCase()) return label;
  }
  return "";
}
function updateActiveMapFreshness() {
  const frame = $("plotView").querySelector(".map-frame");
  if (!frame) return "";
  frame.querySelectorAll(".map-stale-banner").forEach((node) => node.remove());
  const reason = activeMapStaleReason();
  frame.classList.toggle("is-stale", Boolean(reason));
  if (reason) {
    const banner = document.createElement("div");
    banner.className = "map-stale-banner";
    const text = document.createElement("div");
    text.textContent = "Displayed map does not match the selected run.";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "primary";
    button.textContent = "Render Selected Run";
    button.addEventListener("click", () => easyBuildAndPlot().catch((err) => setStatus(String(err), true)));
    banner.append(text, button);
    frame.appendChild(banner);
  }
  return reason;
}
function scheduleRenderCurrentMap(status = "") {
  if (state.restoring) return;
  if (state.renderTimer) clearTimeout(state.renderTimer);
  if (status) setStatus(status);
  state.renderTimer = setTimeout(() => {
    state.renderTimer = null;
    renderMaps().catch((err) => setStatus(String(err), true));
  }, 350);
}
function selectedProductArray() {
  return [...state.selectedProducts];
}
async function loadProducts() {
  setDefaultSourceForModel();
  if (parseRunString($("run").value)) setArchiveSourceForModel();
  const data = await api(`/api/products?model=${encodeURIComponent($("model").value)}`);
  state.products = data;
  const rows = productRows().map((row) => row.slug);
  if (state.viewMode === "easy" && state.easyPresetId) {
    state.selectedProducts = new Set(easyProductSetProducts());
    renderProductList();
  } else if ($("workflowAutoApply").checked && state.activeWorkflowId && state.productWorkflows.length) {
    applyProductWorkflow(state.activeWorkflowId, {save: false});
  } else if (![...state.selectedProducts].some((slug) => rows.includes(slug))) {
    state.selectedProducts = new Set([modelEntry().default_render_product || rows[0]].filter(Boolean));
    renderProductList();
  } else {
    renderProductList();
  }
  renderWorkflowControls();
  if ($("easyPreset")) renderEasyPresetSummary();
  renderTimeline();
}
async function loadBootstrap() {
  const saved = loadSavedState();
  state.restoring = true;
  populateRunCycleOptions();
  populateEasyCycleOptions();
  loadWorkflowStore();
  const data = await api("/api/bootstrap");
  state.bootstrap = data;
  state.models = data.models.models || [];
  state.domains = data.domains.domains || [];
  let savedControls = saved?.controls || {};
  if (savedControls.run && runIsFuture(savedControls.run)) {
    savedControls = {...savedControls, run: "latest", runTop: "latest"};
    setStatus(`Ignored saved future run ${saved.controls.run}; using latest.`, true);
  }
  state.viewMode = savedControls.viewMode || state.viewMode;
  state.easyPresetId = savedControls.easyPresetId || state.easyPresetId;
  state.easyProductTier = normalizedProductTier(savedControls.easyProductTier || state.easyProductTier);
  const defaultModel = state.models.some((m) => m.id === "hrrr") ? "hrrr" : state.models[0]?.id || "";
  const savedModel = state.models.some((m) => m.id === savedControls.model) ? savedControls.model : defaultModel;
  for (const select of [$("model"), $("modelTop"), $("easyModel")]) {
    select.innerHTML = "";
    for (const model of state.models) {
      const option = document.createElement("option");
      option.value = model.id;
      option.textContent = model.id.toUpperCase();
      select.appendChild(option);
    }
    select.value = savedModel;
  }
  populatePresetControls();
  setControlValue("easyProductTier", state.easyProductTier);
  setControlValue("source", savedControls.source);
  syncEasySourceOptions();
  setControlValue("easySource", savedControls.easySource || savedControls.source);
  setControlValue("run", savedControls.run);
  setControlValue("runDate", savedControls.runDate);
  setControlValue("runCycle", savedControls.runCycle);
  setControlValue("forecastHour", savedControls.forecastHour);
  setControlValue("renderHours", savedControls.renderHours);
  setControlValue("prepareHours", savedControls.prepareHours);
  setControlValue("localizeHours", savedControls.localizeHours);
  setControlValue("localizeJobs", savedControls.localizeJobs);
  setControlValue("localizeMapData", savedControls.localizeMapData);
  setControlValue("localizeSoundings", savedControls.localizeSoundings);
  setControlValue("localizeWxStore", savedControls.localizeWxStore);
  setControlValue("localizeRenderAfter", savedControls.localizeRenderAfter);
  setControlValue("resourcePreset", savedControls.resourcePreset);
  setControlValue("width", savedControls.width);
  setControlValue("height", savedControls.height);
  setControlValue("renderMode", savedControls.renderMode);
  setControlValue("kind", savedControls.productGroup);
  setControlValue("productSearch", savedControls.productSearch);
  setControlValue("soundingLat", savedControls.soundingLat);
  setControlValue("soundingLon", savedControls.soundingLon);
  setControlValue("soundingHour", savedControls.soundingHour);
  setControlValue("soundingDataMode", savedControls.soundingDataMode);
  setControlValue("soundingMethod", savedControls.soundingMethod);
  setControlValue("soundingBoxKm", savedControls.soundingBoxKm);
  setControlValue("soundingBoxShape", savedControls.soundingBoxShape);
  setControlValue("soundingBoxWidthKm", savedControls.soundingBoxWidthKm);
  setControlValue("soundingBoxHeightKm", savedControls.soundingBoxHeightKm);
  setControlValue("clickSoundingMode", savedControls.clickSoundingMode);
  $("autoOpenSounding").checked = false;
  setControlValue("soundingStation", savedControls.soundingStation);
  setControlValue("easyHours", savedControls.easyHours);
  setControlValue("easyCustomHours", savedControls.easyCustomHours);
  if (Array.isArray(savedControls.easySelectedHours)) {
    state.easySelectedHours = new Set(savedControls.easySelectedHours.map(Number).filter(Number.isFinite));
  }
  if (Array.isArray(savedControls.easyCustomProducts)) {
    state.easyCustomProducts = new Set(savedControls.easyCustomProducts.map(String).filter(Boolean));
  }
  setControlValue("easyWarmStore", savedControls.easyWarmStore);
  setControlValue("easyAutoLatest", savedControls.easyAutoLatest);
  setControlValue("workflowAutoApply", savedControls.workflowAutoApply);
  if (savedControls.activeWorkflowId) state.activeWorkflowId = savedControls.activeWorkflowId;
  renderWorkflowControls();
  if (Array.isArray(savedControls.products) && savedControls.products.length) {
    state.selectedProducts = new Set(savedControls.products);
  }
  state.displayMode = savedControls.displayMode || state.displayMode;
  syncRunControls(false);
  syncArchiveControlsFromRun();
  setDefaultSourceForModel();
  if (parseRunString($("run").value)) setArchiveSourceForModel();
  if (savedControls.source) setControlValue("source", savedControls.source);
  updateResourceMeta();
  syncEasySourceOptions();
  setControlValue("easySource", savedControls.easySource || $("source").value);
  setControlValue("domainSearch", savedControls.domainSearch);
  renderDomainOptions();
  setControlValue("domain", savedControls.domain);
  syncEasyDomainOptions();
  await loadProducts();
  renderTimeline();
  renderJobsFromBootstrap(data.jobs);
  await refreshData();
  restoreMedia(saved);
  await hydratePreviewsFromBootstrapJobs(data.jobs);
  state.restoring = false;
  setViewMode(state.viewMode, {save: false});
  syncEasyFromMain();
  syncClickModeButtons();
  updateAutoLatestTimer();
  const staleReason = updateActiveMapFreshness();
  setStatus(`${data.app} ready | ${data.plot_style}`);
  if (staleReason && parseRunString($("run").value)) scheduleRenderCurrentMap("Rendering pinned archive map");
  saveViewerState();
  refreshLatestRuns();
}
function renderJobsFromBootstrap(jobs) {
  for (const job of jobs?.jobs || []) {
    if (jobAllowed(job)) state.jobs.set(job.id, job);
  }
  renderJobs();
}
async function hydratePreviewsFromBootstrapJobs(jobs) {
  const previewJobs = (jobs?.jobs || [])
    .filter((job) => jobAllowed(job) && Number(job.preview_count || 0) > 0)
    .slice(0, 5);
  if (!previewJobs.length) return;
  const shouldActivate = !state.activePreview;
  for (const job of previewJobs) {
    try {
      const detail = await api(`/api/jobs/${encodeURIComponent(job.id)}`);
      if (detail.job) upsertJob(detail.job);
      const result = detail.job?.result;
      const previews = (result?.previews || [])
        .map((preview) => enrichPreview(preview, result, detail.job?.request || {}))
        .filter(isMapPreview);
      if (previews.length) addPreviews(previews, {activate: shouldActivate && !state.activePreview});
    } catch (_err) {}
  }
}
function upsertJob(job) {
  if (!job || !job.id) return;
  if (!jobAllowed(job)) {
    state.jobs.delete(job.id);
    renderJobs();
    return;
  }
  state.jobs.set(job.id, job);
  renderJobs();
}
function jobAllowed(job) {
  const reqRun = job?.request?.run_str || job?.request?.run || "";
  const resultRun = runStringFromResult(job?.result || {});
  return !runIsFuture(reqRun) && !runIsFuture(resultRun);
}
function jobKindLabel(kind) {
  return {
    localize_run: "run",
    pressure_store: "sounding store",
    wxstore_plot_existing: "plot from cache",
    wxstore: "WxStore",
    prepare_data: "prepare data",
    render: "render",
    sounding: "sounding"
  }[kind] || String(kind || "job").replaceAll("_", " ");
}
function renderJobs() {
  const jobs = [...state.jobs.values()].sort((a, b) => String(b.created_at || "").localeCompare(String(a.created_at || ""))).slice(0, 8);
  $("jobList").innerHTML = "";
  for (const job of jobs) {
    const card = document.createElement("div");
    card.className = "job-card";
    const title = document.createElement("strong");
    title.textContent = `${jobKindLabel(job.kind)} ${job.id}`;
    const status = document.createElement("span");
    status.className = `job-status ${job.status || ""}`;
    status.textContent = job.status || "unknown";
    const meta = document.createElement("div");
    meta.className = "meta-line";
    const req = job.request || {};
    meta.textContent = [req.model, req.domain, req.hours || req.forecast_hour, req.product_count ? `${req.product_count} products` : null, job.elapsed_s ? `${job.elapsed_s}s` : null].filter(Boolean).join(" | ");
    const progress = document.createElement("div");
    progress.className = "meta-line";
    if (job.progress) {
      const bits = [job.progress.label, job.progress.detail, `${job.progress.current || 0}/${job.progress.total || 1}`, `${job.progress.percent || 0}%`].filter(Boolean);
      progress.textContent = bits.join(" | ");
    }
    const bar = document.createElement("div");
    bar.className = "toolbar";
    const view = document.createElement("button");
    view.textContent = "View";
    view.addEventListener("click", () => viewJob(job.id));
    bar.appendChild(view);
    if (["queued", "running", "cancelling"].includes(job.status || "")) {
      const cancel = document.createElement("button");
      cancel.textContent = job.status === "cancelling" ? "Stopping" : "Cancel";
      cancel.disabled = job.status === "cancelling";
      cancel.addEventListener("click", () => cancelJob(job.id));
      bar.appendChild(cancel);
    }
    card.append(title, status, meta);
    if (job.progress) card.appendChild(progress);
    card.appendChild(bar);
    $("jobList").appendChild(card);
  }
}
function updateSoundingBusy() {
  $("cancelSounding").disabled = !state.mapSoundingLaunchInFlight
    && !state.activeMapSoundingJobId
    && !state.activeSoundingJobId
    && !state.queuedMapSoundingPayload;
}
function clearDeadInteractionLocks() {
  if (state.mapSoundingLaunchInFlight && (Date.now() - Number(state.mapSoundingLaunchStartedAt || 0)) > 8000) {
    state.mapSoundingLaunchInFlight = false;
    state.mapSoundingLaunchStartedAt = 0;
    if (state.mapSoundingAbortController) state.mapSoundingAbortController.abort();
    state.mapSoundingAbortController = null;
  }
  if (state.activeMapSoundingJobId) {
    const job = state.jobs.get(state.activeMapSoundingJobId);
    const terminal = !job || ["completed", "failed", "cancelled"].includes(job.status || "");
    const pollerMissing = !state.pollers.has(state.activeMapSoundingJobId);
    const stalePoll = pollerMissing && (Date.now() - Number(state.activeMapSoundingJobStartedAt || 0)) > 5000;
    if (terminal || stalePoll) {
      state.activeMapSoundingJobId = null;
      state.activeMapSoundingJobStartedAt = 0;
    }
  }
  if (state.activeSoundingJobId) {
    const job = state.jobs.get(state.activeSoundingJobId);
    const terminal = !job || ["completed", "failed", "cancelled"].includes(job.status || "");
    const pollerMissing = !state.pollers.has(state.activeSoundingJobId);
    const stalePoll = pollerMissing && (Date.now() - Number(state.activeSoundingJobStartedAt || 0)) > 5000;
    if (terminal || stalePoll) {
      state.activeSoundingJobId = null;
      state.activeSoundingJobStartedAt = 0;
    }
  }
  if (!$("soundingOverlay").classList.contains("hidden")) return;
  $("soundingOverlay").style.pointerEvents = "none";
  updateSoundingBusy();
}
function emergencyUnlockInteraction() {
  cancelBoxDrag();
  state.mapSoundingRequestToken += 1;
  if (state.mapSoundingAbortController) state.mapSoundingAbortController.abort();
  state.mapSoundingAbortController = null;
  state.mapSoundingLaunchInFlight = false;
  state.mapSoundingLaunchStartedAt = 0;
  state.activeMapSoundingJobId = null;
  state.activeMapSoundingJobStartedAt = 0;
  state.activeSoundingJobId = null;
  state.activeSoundingJobStartedAt = 0;
  state.queuedMapSoundingPayload = null;
  closeSoundingOverlay();
  closeSetupOverlay();
  closeProductPicker();
  document.querySelectorAll(".box-hover-footprint").forEach((node) => node.remove());
  updateSoundingBusy();
  setStatus("interaction unlocked");
}
async function runJob(kind, payload, button, options = {}) {
  const lockButton = button && options.lockButton !== false;
  if (lockButton) button.disabled = true;
  setStatus(`${jobKindLabel(kind)} queued`);
  let launched;
  try {
    launched = await post("/api/jobs", {kind, payload}, {signal: options.signal});
    showResult(launched);
  } catch (err) {
    if (lockButton) button.disabled = false;
    if (err?.name === "AbortError") {
      const cancelled = {ok: false, cancelled: true, error: "request cancelled"};
      if (!options.silentAbort) setStatus("request cancelled");
      return cancelled;
    }
    const failed = {ok: false, error: String(err)};
    showResult(failed);
    setStatus(String(err), true);
    return failed;
  }
  if (!launched.ok) {
    if (lockButton) button.disabled = false;
    setStatus(launched.error || "job failed", true);
    return launched;
  }
  if (options && launched.job?.id) {
    if (typeof options.isCurrent === "function" && !options.isCurrent()) {
      cancelJob(launched.job.id);
      return {ok: false, cancelled: true, stale: true, job: launched.job};
    }
    state.jobClientMeta.set(launched.job.id, options);
    if (typeof options.onLaunch === "function") options.onLaunch(launched.job);
  }
  upsertJob(launched.job);
  pollJob(launched.job.id, lockButton ? button : null);
  return launched;
}
function pollJob(jobId, button) {
  if (state.pollers.has(jobId)) clearTimeout(state.pollers.get(jobId));
  const tick = async () => {
    try {
      const data = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
      const job = data.job;
      upsertJob(job);
      setStatus(`${jobKindLabel(job.kind)} ${job.status}`);
      if (job?.kind !== "sounding" && job?.result?.previews?.length) {
        handleJobResult(job, job.result, state.jobClientMeta.get(jobId) || {});
      }
      if (!job || ["completed", "failed", "cancelled"].includes(job.status)) {
        state.pollers.delete(jobId);
        if (button) button.disabled = false;
        const result = job?.result || data;
        const clientMeta = state.jobClientMeta.get(jobId) || {};
        state.jobClientMeta.delete(jobId);
        showResult(result);
        handleJobResult(job, result, clientMeta);
        if (typeof clientMeta.onComplete === "function") clientMeta.onComplete(job, result);
        const detail = job.error || result?.error || (Array.isArray(result?.stderr_tail) ? result.stderr_tail.slice(-1)[0] : "");
        setStatus(job.status === "completed" ? `${jobKindLabel(job.kind)} complete` : (detail || `${jobKindLabel(job.kind)} ${job.status}`), job.status !== "completed");
        if (job?.kind !== "sounding") refreshData();
        return;
      }
      state.pollers.set(jobId, setTimeout(tick, 1100));
    } catch (err) {
      state.pollers.delete(jobId);
      if (button) button.disabled = false;
      const clientMeta = state.jobClientMeta.get(jobId) || {};
      state.jobClientMeta.delete(jobId);
      if (typeof clientMeta.onComplete === "function") {
        clientMeta.onComplete(state.jobs.get(jobId) || {id: jobId, status: "failed"}, {ok: false, error: String(err)});
      }
      setStatus(String(err), true);
    }
  };
  state.pollers.set(jobId, setTimeout(tick, 300));
}
async function viewJob(jobId) {
  const data = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
  upsertJob(data.job);
  const result = data.job?.result || data;
  showResult(result);
  handleJobResult(data.job, result);
}
async function cancelJob(jobId) {
  const data = await post(`/api/jobs/${encodeURIComponent(jobId)}/cancel`, {});
  if (data.job) upsertJob(data.job);
  showResult(data);
}
function handleJobResult(job, result, clientMeta = {}) {
  const previews = (result?.previews || []).map((preview) => enrichPreview(preview, result, job?.request || {}));
  if (job?.kind === "render" || job?.kind === "localize_run" || job?.kind === "wxstore" || job?.kind === "wxstore_plot_existing") {
    const mapPreviews = previews.filter(isMapPreview);
    if (mapPreviews.length) addPreviews(mapPreviews);
  }
  if (job?.kind === "sounding" && clientMeta.ignoreResult !== true) {
    if (job?.id && state.handledSoundingJobIds.has(job.id)) return;
    if (job?.id) state.handledSoundingJobIds.add(job.id);
    addSounding(result);
  }
}
function runStringFromResult(result) {
  if (result?.run_str) return result.run_str;
  const date = result?.date_yyyymmdd || result?.date;
  const cycle = result?.cycle_utc ?? result?.cycle;
  if (date && cycle !== undefined && cycle !== null) return `${date}/${String(Number(cycle)).padStart(2, "0")}z`;
  return "";
}
function previewIdentityFromPath(preview) {
  const text = `${preview?.name || ""} ${preview?.path || ""}`;
  const file = text.split(/[\\/]/).pop() || text;
  const match = file.match(/^rustwx_([^_]+)_(\d{8})_(\d{1,2})z_f(\d{1,5})_(.+?)(?:\.(?:png|jpg|jpeg|webp))?$/i);
  if (!match) return {};
  const tail = match[5] || "";
  const knownDomains = state.domains
    .map((domain) => domain.slug)
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);
  const domain = knownDomains.find((slug) => tail === slug || tail.startsWith(`${slug}_`)) || "";
  return {
    model: match[1].toLowerCase(),
    run_str: `${match[2]}/${String(Number(match[3])).padStart(2, "0")}z`,
    forecast_hour: Number(match[4]),
    domain
  };
}
function normalizePreviewSource(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.replace(/([a-z0-9])([A-Z])/g, "$1-$2").toLowerCase().replaceAll("_", "-");
}
function runStringFromPreview(preview) {
  const identity = previewIdentityFromPath(preview);
  if (identity.run_str) return identity.run_str;
  const existing = preview?.run_str || preview?.run;
  if (existing) return existing;
  const text = `${preview?.name || ""} ${preview?.path || ""}`;
  const match = text.match(/_(\d{8})_(\d{1,2})z_/i);
  if (match) return `${match[1]}/${String(Number(match[2])).padStart(2, "0")}z`;
  return "";
}
function enrichPreview(preview, result = null, request = null) {
  if (!preview) return preview;
  const identity = previewIdentityFromPath(preview);
  const runStr = identity.run_str || runStringFromResult(result) || preview.run_str || runStringFromPreview(preview);
  const model = identity.model || request?.model || preview.model || "";
  const source = normalizePreviewSource(request?.source || preview.source || result?.source);
  const forecastHour = Number.isFinite(identity.forecast_hour) ? identity.forecast_hour : (preview.forecast_hour ?? request?.forecast_hour ?? result?.forecast_hour);
  const domain = identity.domain || preview.domain || request?.domain || result?.ui_domain || "";
  return {
    ...preview,
    ...(runStr ? {run_str: runStr} : {}),
    ...(model ? {model} : {}),
    ...(source ? {source} : {}),
    ...(forecastHour !== undefined && forecastHour !== null ? {forecast_hour: forecastHour} : {}),
    ...(domain ? {domain} : {})
  };
}
function activePreviewContext(preview = state.activePreview) {
  if (!preview) return null;
  const enriched = enrichPreview(preview);
  const forecastHour = Number(enriched.forecast_hour ?? $("forecastHour").value ?? 0);
  return {
    model: String(enriched.model || $("model").value || "").toLowerCase(),
    source: normalizePreviewSource(enriched.source || $("source").value || ""),
    domain: enriched.domain || $("domain").value,
    runStr: enriched.run_str || runStringFromPreview(enriched) || $("run").value,
    forecastHour: Number.isFinite(forecastHour) ? forecastHour : 0,
    bounds: enriched.bounds || activeBounds()
  };
}
function sameText(a, b) {
  return String(a || "").toLowerCase() === String(b || "").toLowerCase();
}
function soundingContextMatchesPreview(context, previewContext) {
  if (!context || !previewContext) return false;
  const source = normalizePreviewSource(context.source || "");
  const hour = Number(context.forecastHour ?? $("soundingHour").value ?? NaN);
  return sameText(context.model, previewContext.model)
    && sameText(source, previewContext.source)
    && sameText(context.domain, previewContext.domain)
    && sameText(context.runStr, previewContext.runStr)
    && Number.isFinite(hour)
    && hour === previewContext.forecastHour;
}
function syncSoundingPanelForActivePreview(preview = state.activePreview) {
  const previewContext = activePreviewContext(preview);
  if (!previewContext) return;
  if (state.soundingContext && !soundingContextMatchesPreview(state.soundingContext, previewContext)) {
    state.soundingContext = null;
    $("soundingMeta").textContent = "Click a rendered map to sample.";
  }
  if (!state.soundingContext) {
    $("soundingStation").value = "";
    $("soundingHour").value = String(previewContext.forecastHour);
  }
}
function isMapPreview(item) {
  const path = String(item.path || "").replaceAll("\\", "/");
  return path.includes("/model_maps/maps/") || path.includes("/studio/maps/") || path.includes("/studio/wxstore");
}
function addPreviews(previews, {activate = true} = {}) {
  const allowed = previews.map((preview) => enrichPreview(preview)).filter(restoredPreviewAllowed);
  if (!allowed.length) return;
  const seen = new Set();
  const merged = [];
  for (const preview of allowed.concat(state.previews.filter(restoredPreviewAllowed))) {
    const key = preview.path || preview.url || preview.name || JSON.stringify(preview);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(preview);
  }
  state.previews = merged.slice(0, 80);
  if (activate) {
    setActivePreview(allowed.find((preview) => (preview.path || preview.url) === (state.previews[0]?.path || state.previews[0]?.url)) || state.previews[0]);
  } else {
    renderPreviewStrip();
    saveViewerState();
  }
}
function setActivePreview(preview) {
  if (!preview) return;
  preview = enrichPreview(preview);
  if (!restoredPreviewAllowed(preview)) {
    setStatus(`Ignored future run preview ${preview.run_str || ""}`.trim(), true);
    return;
  }
  state.activePreview = preview;
  syncSoundingPanelForActivePreview(preview);
  $("plotView").innerHTML = "";
  const frame = document.createElement("div");
  frame.className = "map-frame";
  const img = document.createElement("img");
  img.className = "plot-map";
  img.src = preview.url;
  img.alt = preview.name || "model map";
  img.dataset.bounds = JSON.stringify(preview.bounds || activeBounds());
  img.dataset.domain = preview.domain || $("domain").value;
  img.dataset.model = preview.model || $("model").value;
  img.dataset.source = preview.source || $("source").value;
  img.dataset.forecastHour = preview.forecast_hour ?? $("forecastHour").value;
  img.dataset.runStr = preview.run_str || runStringFromPreview(preview) || $("run").value;
  img.addEventListener("pointerdown", (event) => handleMapPointerDown(event, img, frame));
  img.addEventListener("pointerup", (event) => handleMapPointerUp(event, img, frame));
  img.addEventListener("pointermove", (event) => handleMapPointerMove(event, img, frame));
  img.addEventListener("pointerleave", () => handleMapPointerLeave(frame));
  frame.appendChild(img);
  $("plotView").appendChild(frame);
  applyDisplayMode();
  renderPreviewStrip();
  updateActiveMapFreshness();
  saveViewerState();
}
function renderPreviewStrip() {
  $("previewStrip").innerHTML = "";
  const visible = state.previews.slice(0, 24);
  for (let index = 0; index < visible.length; index += 1) {
    const preview = visible[index];
    const button = document.createElement("button");
    button.type = "button";
    button.className = "preview-chip" + (state.activePreview?.url === preview.url ? " active" : "");
    button.textContent = previewLabel(preview, index);
    button.title = preview.name || preview.path || "";
    button.addEventListener("click", () => setActivePreview(preview));
    $("previewStrip").appendChild(button);
  }
  if (state.previews.length > visible.length) {
    const more = document.createElement("button");
    more.type = "button";
    more.className = "preview-chip more";
    more.disabled = true;
    more.textContent = `+${state.previews.length - visible.length} more`;
    $("previewStrip").appendChild(more);
  }
}
function previewLabel(preview, index) {
  const hour = Number(preview.forecast_hour ?? preview.hour);
  const hourText = Number.isFinite(hour) ? `F${String(hour).padStart(3, "0")}` : `#${index + 1}`;
  let name = String(preview.name || preview.path || "").split(/[\\/]/).pop() || "map";
  name = name.replace(/\.(png|jpg|jpeg|webp)$/i, "");
  const match = name.match(/f\d{3}_(.+)$/i);
  if (match) name = match[1];
  name = name.replace(/^rustwx_[^_]+_\d{8}_\d{1,2}z_/i, "");
  name = name.replace(/^conus_|^central_|^southern[-_]plains_/i, "");
  return `${hourText} ${productLabel(name).slice(0, 42)}`;
}
function applyDisplayMode() {
  const img = $("plotView").querySelector(".map-frame img");
  const frame = $("plotView").querySelector(".map-frame");
  if (!img || !frame) return;
  if (state.displayMode === "fit") {
    const width = Math.max(320, $("plotView").clientWidth - 24);
    frame.style.setProperty("--display-width", `${width}px`);
  } else {
    frame.style.setProperty("--display-width", `${img.naturalWidth || Number($("width").value) || 1600}px`);
  }
}
function boundsForImage(img) {
  try {
    const bounds = JSON.parse(img.dataset.bounds || "[]");
    if (Array.isArray(bounds) && bounds.length === 4) return bounds.map(Number);
  } catch (_err) {}
  return activeBounds();
}
function pointFromImage(event, img) {
  const bounds = boundsForImage(img);
  const rect = img.getBoundingClientRect();
  const x = Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width));
  const y = Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height));
  return {
    x,
    y,
    lon: bounds[0] + x * (bounds[1] - bounds[0]),
    lat: bounds[3] - y * (bounds[3] - bounds[2]),
    bounds,
    domain: img.dataset.domain || $("domain").value,
    model: img.dataset.model || $("model").value,
    source: img.dataset.source || $("source").value,
    forecastHour: Number(img.dataset.forecastHour || $("forecastHour").value || 0),
    runStr: img.dataset.runStr || $("run").value
  };
}
function positiveNumber(id, fallback) {
  const value = Number($(id).value);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}
function cleanKm(value) {
  const number = Number(value);
  return Number.isInteger(number) ? String(number) : number.toFixed(1);
}
function boxDimensions() {
  let widthKm = positiveNumber("soundingBoxWidthKm", 25);
  let heightKm = positiveNumber("soundingBoxHeightKm", widthKm);
  if ($("soundingBoxShape").value === "square") heightKm = widthKm;
  widthKm = Math.max(1, Math.min(800, widthKm));
  heightKm = Math.max(1, Math.min(800, heightKm));
  if ($("soundingBoxWidthKm").value !== cleanKm(widthKm)) $("soundingBoxWidthKm").value = cleanKm(widthKm);
  if ($("soundingBoxHeightKm").value !== cleanKm(heightKm)) $("soundingBoxHeightKm").value = cleanKm(heightKm);
  const radiusKm = Math.max(widthKm, heightKm) / 2;
  $("soundingBoxKm").value = String(radiusKm);
  return {widthKm, heightKm, radiusKm};
}
function boxRadiusDegrees(lat, dims = boxDimensions()) {
  const safeLat = Number.isFinite(Number(lat)) ? Number(lat) : 0;
  const cosLat = Math.max(0.2, Math.abs(Math.cos(safeLat * Math.PI / 180)));
  return {
    latDeg: (dims.heightKm / 2) / 111.0,
    lonDeg: (dims.widthKm / 2) / (111.0 * cosLat)
  };
}
function boxLabel(dims = boxDimensions()) {
  return `${cleanKm(dims.widthKm)}x${cleanKm(dims.heightKm)} km`;
}
function syncBoxControls(changedId = "") {
  if (changedId === "soundingBoxShape" && $("soundingBoxShape").value === "square") {
    $("soundingBoxHeightKm").value = $("soundingBoxWidthKm").value;
  }
  if (changedId === "soundingBoxWidthKm" && $("soundingBoxShape").value === "square") {
    $("soundingBoxHeightKm").value = $("soundingBoxWidthKm").value;
  }
  if (changedId === "soundingBoxHeightKm" && $("soundingBoxShape").value === "square") {
    $("soundingBoxWidthKm").value = $("soundingBoxHeightKm").value;
  }
  boxDimensions();
  syncClickModeButtons();
  saveViewerState();
}
function drawBoxFromPoints(start, end) {
  const west = Math.min(start.lon, end.lon);
  const east = Math.max(start.lon, end.lon);
  const south = Math.min(start.lat, end.lat);
  const north = Math.max(start.lat, end.lat);
  const centerLat = (south + north) / 2;
  const centerLon = (west + east) / 2;
  const latDeg = Math.abs(north - south) / 2;
  const lonDeg = Math.abs(east - west) / 2;
  const cosLat = Math.max(0.2, Math.abs(Math.cos(centerLat * Math.PI / 180)));
  const widthKm = lonDeg * 2 * 111.0 * cosLat;
  const heightKm = latDeg * 2 * 111.0;
  return {
    start,
    end,
    x1: Math.min(start.x, end.x),
    x2: Math.max(start.x, end.x),
    y1: Math.min(start.y, end.y),
    y2: Math.max(start.y, end.y),
    west,
    east,
    south,
    north,
    centerLat,
    centerLon,
    latDeg,
    lonDeg,
    widthKm,
    heightKm,
    radiusKm: Math.max(widthKm, heightKm) / 2,
    label: boxLabel({widthKm, heightKm})
  };
}
function drawBoxRect(frame, boxInfo, className) {
  frame.querySelectorAll(`.${className}`).forEach((node) => node.remove());
  const box = document.createElement("div");
  box.className = `${className} drawn-box`;
  box.style.left = `${boxInfo.x1 * 100}%`;
  box.style.top = `${boxInfo.y1 * 100}%`;
  box.style.width = `${Math.max(0.4, (boxInfo.x2 - boxInfo.x1) * 100)}%`;
  box.style.height = `${Math.max(0.4, (boxInfo.y2 - boxInfo.y1) * 100)}%`;
  const label = document.createElement("span");
  label.className = "box-footprint-label";
  label.textContent = boxInfo.label;
  box.appendChild(label);
  frame.appendChild(box);
}
function cancelBoxDrag(message = "") {
  if (!state.boxDrag) return;
  const drag = state.boxDrag;
  state.boxDrag = null;
  try { drag.img?.releasePointerCapture(drag.pointerId); } catch (_err) {}
  drag.frame?.querySelectorAll(".box-hover-footprint").forEach((node) => node.remove());
  if (message) setStatus(message);
}
function handleMapPointerDown(event, img, frame) {
  if (activeMapStaleReason()) {
    event.preventDefault();
    updateActiveMapFreshness();
    setStatus("Render the selected run before sampling soundings.", true);
    return;
  }
  if ($("clickSoundingMode").value !== "draw-box" || event.button !== 0) return;
  event.preventDefault();
  const start = pointFromImage(event, img);
  state.boxDrag = {pointerId: event.pointerId, img, frame, start, latest: start, moved: false};
  frame.querySelectorAll(".click-chip, .box-footprint").forEach((node) => node.remove());
  try { img.setPointerCapture(event.pointerId); } catch (_err) {}
  setStatus("draw box: drag over the map");
}
function handleMapPointerMove(event, img, frame) {
  if (state.boxDrag && state.boxDrag.pointerId === event.pointerId) {
    event.preventDefault();
    const latest = pointFromImage(event, img);
    state.boxDrag.latest = latest;
    state.boxDrag.moved = state.boxDrag.moved
      || Math.abs(latest.x - state.boxDrag.start.x) > 0.004
      || Math.abs(latest.y - state.boxDrag.start.y) > 0.004;
    drawBoxRect(frame, drawBoxFromPoints(state.boxDrag.start, latest), "box-hover-footprint");
    return;
  }
  handleMapHover(event, img, frame);
}
function handleMapPointerUp(event, img, frame) {
  if (activeMapStaleReason()) {
    event.preventDefault();
    updateActiveMapFreshness();
    setStatus("Render the selected run before sampling soundings.", true);
    return;
  }
  if ($("clickSoundingMode").value === "draw-box" && state.boxDrag && state.boxDrag.pointerId === event.pointerId) {
    event.preventDefault();
    const drag = state.boxDrag;
    const end = pointFromImage(event, img);
    state.boxDrag = null;
    try { img.releasePointerCapture(event.pointerId); } catch (_err) {}
    frame.querySelectorAll(".box-hover-footprint").forEach((node) => node.remove());
    if (!drag.moved) {
      setStatus("draw box: drag a rectangle on the map");
      return;
    }
    finalizeDrawnBox(frame, drag.start, end);
    return;
  }
  if ($("clickSoundingMode").value === "draw-box") {
    setStatus("draw box: drag a rectangle on the map");
    return;
  }
  handleMapClick(event, img, frame);
}
function handleMapPointerLeave(frame) {
  if (!state.boxDrag) frame.querySelectorAll(".box-hover-footprint").forEach((node) => node.remove());
}
function finalizeDrawnBox(frame, start, end) {
  const boxInfo = drawBoxFromPoints(start, end);
  if (boxInfo.widthKm < 1 || boxInfo.heightKm < 1) {
    setStatus("draw box: box is too small", true);
    return;
  }
  frame.querySelectorAll(".click-chip, .box-footprint").forEach((node) => node.remove());
  drawBoxRect(frame, boxInfo, "box-footprint");
  const chip = document.createElement("div");
  chip.className = "click-chip";
  chip.style.left = `${((boxInfo.x1 + boxInfo.x2) / 2) * 100}%`;
  chip.style.top = `${boxInfo.y1 * 100}%`;
  chip.textContent = `${boxInfo.centerLat.toFixed(2)}, ${boxInfo.centerLon.toFixed(2)} | ${boxInfo.label} box`;
  frame.appendChild(chip);
  $("soundingMethod").value = "box-mean";
  $("soundingLat").value = boxInfo.centerLat.toFixed(3);
  $("soundingLon").value = boxInfo.centerLon.toFixed(3);
  $("soundingHour").value = start.forecastHour;
  $("soundingBoxWidthKm").value = cleanKm(boxInfo.widthKm);
  $("soundingBoxHeightKm").value = cleanKm(boxInfo.heightKm);
  $("soundingBoxShape").value = "custom";
  const modelLabel = String(start.model || $("model").value || "").toUpperCase();
  $("soundingStation").value = `${modelLabel} ${start.runStr || $("run").value} DRAW BOX ${boxInfo.centerLat.toFixed(2)},${boxInfo.centerLon.toFixed(2)}`;
  state.soundingContext = {
    model: start.model,
    source: start.source,
    domain: start.domain,
    bounds: start.bounds,
    runStr: start.runStr,
    forecastHour: start.forecastHour,
    fromClick: true,
    clickMode: "draw-box",
    drawnBox: boxInfo
  };
  saveViewerState();
  renderSounding(true).catch((err) => setStatus(String(err), true));
}
async function handleMapClick(event, img, frame) {
  const point = pointFromImage(event, img);
  const clickMode = $("clickSoundingMode").value;
  const dims = boxDimensions();
  frame.querySelectorAll(".click-chip, .box-footprint").forEach((node) => node.remove());
  const chip = document.createElement("div");
  chip.className = "click-chip";
  chip.style.left = `${point.x * 100}%`;
  chip.style.top = `${point.y * 100}%`;
  chip.textContent = clickMode === "box"
    ? `${point.lat.toFixed(2)}, ${point.lon.toFixed(2)} | ${boxLabel(dims)} box`
    : `${point.lat.toFixed(2)}, ${point.lon.toFixed(2)}`;
  frame.appendChild(chip);
  if (clickMode === "box") {
    drawBoxFootprint(frame, point, "box-footprint");
    $("soundingMethod").value = "box-mean";
  } else if ($("soundingMethod").value === "box-mean") {
    $("soundingMethod").value = "nearest";
  }
  state.soundingContext = {
    model: point.model,
    source: point.source,
    domain: point.domain,
    bounds: point.bounds,
    runStr: point.runStr,
    forecastHour: point.forecastHour,
    fromClick: true,
    clickMode
  };
  $("soundingLat").value = point.lat.toFixed(3);
  $("soundingLon").value = point.lon.toFixed(3);
  $("soundingHour").value = point.forecastHour;
  const modelLabel = String(point.model || $("model").value || "").toUpperCase();
  $("soundingStation").value = `${modelLabel} ${point.runStr || $("run").value} ${clickMode === "box" ? "BOX" : "PT"} ${point.lat.toFixed(2)},${point.lon.toFixed(2)}`;
  saveViewerState();
  renderSounding(true).catch((err) => setStatus(String(err), true));
}
function boxFootprintPercent(point) {
  const dims = boxDimensions();
  const radii = boxRadiusDegrees(point.lat, dims);
  const latRadiusDeg = radii.latDeg;
  const lonRadiusDeg = radii.lonDeg;
  const widthPct = Math.min(100, Math.abs((lonRadiusDeg * 2) / (point.bounds[1] - point.bounds[0]) * 100));
  const heightPct = Math.min(100, Math.abs((latRadiusDeg * 2) / (point.bounds[3] - point.bounds[2]) * 100));
  return {
    widthPct: Math.max(7.5, widthPct),
    heightPct: Math.max(7.5, heightPct),
    label: boxLabel(dims)
  };
}
function drawBoxFootprint(frame, point, className) {
  frame.querySelectorAll(`.${className}`).forEach((node) => node.remove());
  const size = boxFootprintPercent(point);
  const box = document.createElement("div");
  box.className = className;
  box.style.left = `${point.x * 100}%`;
  box.style.top = `${point.y * 100}%`;
  box.style.width = `${size.widthPct}%`;
  box.style.height = `${size.heightPct}%`;
  const label = document.createElement("span");
  label.className = "box-footprint-label";
  label.textContent = size.label;
  box.appendChild(label);
  frame.appendChild(box);
}
function handleMapHover(event, img, frame) {
  if ($("clickSoundingMode").value !== "box") {
    frame.querySelectorAll(".box-hover-footprint").forEach((node) => node.remove());
    return;
  }
  drawBoxFootprint(frame, pointFromImage(event, img), "box-hover-footprint");
}
function addSounding(result) {
  const preview = (result?.previews || []).find((item) => String(item.path || "").toLowerCase().endsWith(".png"));
  if (!preview) return;
  const resultBoxLabel = result.box_width_km && result.box_height_km
    ? boxLabel({widthKm: Number(result.box_width_km), heightKm: Number(result.box_height_km)})
    : "";
  const item = {
    id: `${Date.now()}-${Math.random()}`,
    preview,
    result: compactResult(result),
    lat: result.lat,
    lon: result.lon,
    hour: result.forecast_hour,
    backend: result.backend,
    boxLabel: result.sample_method_used === "box-mean" || result.sample_method_requested === "box-mean" ? (resultBoxLabel || boxLabel()) : ""
  };
  state.soundings.unshift(item);
  state.soundings = state.soundings.slice(0, 20);
  renderLatestSounding(item);
  renderSoundings();
  if ($("autoOpenSounding").checked) showSoundingOverlay(item);
  saveViewerState();
}
function showSoundingOverlay(item) {
  if (!item?.preview?.url) return;
  $("soundingOverlayTitle").textContent = `${Number(item.lat).toFixed(2)}, ${Number(item.lon).toFixed(2)} F${String(item.hour ?? 0).padStart(3, "0")} | ${item.backend || "sounding"}`;
  $("soundingOverlayImg").src = item.preview.url;
  $("soundingOverlayImg").alt = item.preview.name || "expanded sounding";
  $("soundingOverlay").style.pointerEvents = "auto";
  $("soundingOverlay").classList.remove("hidden");
}
function closeSoundingOverlay() {
  $("soundingOverlay").classList.add("hidden");
  $("soundingOverlay").style.pointerEvents = "none";
}
function renderLatestSounding(item) {
  $("latestSounding").innerHTML = "";
  if (!item) return;
  const title = document.createElement("div");
  title.className = "meta-line";
  title.textContent = `Latest: ${Number(item.lat).toFixed(2)}, ${Number(item.lon).toFixed(2)} F${String(item.hour ?? 0).padStart(3, "0")} | ${[item.backend, item.boxLabel].filter(Boolean).join(" | ")}`;
  const img = document.createElement("img");
  img.src = item.preview.url;
  img.alt = item.preview.name || "latest sounding";
  img.decoding = "async";
  img.addEventListener("click", () => showSoundingOverlay(item));
  const bar = document.createElement("div");
  bar.className = "toolbar";
  const open = document.createElement("button");
  open.textContent = "Open Large";
  open.addEventListener("click", () => showSoundingOverlay(item));
  bar.appendChild(open);
  $("latestSounding").append(title, img, bar);
}
function renderSoundings() {
  $("soundingList").innerHTML = "";
  for (const item of state.soundings) {
    const card = document.createElement("div");
    card.className = "sounding-card";
    const title = document.createElement("strong");
    title.textContent = `${Number(item.lat).toFixed(2)}, ${Number(item.lon).toFixed(2)} F${String(item.hour ?? 0).padStart(3, "0")}`;
    const meta = document.createElement("div");
    meta.className = "meta-line";
    meta.textContent = [item.backend, item.boxLabel].filter(Boolean).join(" | ");
    const bar = document.createElement("div");
    bar.className = "toolbar";
    const open = document.createElement("button");
    open.textContent = "Open";
    open.addEventListener("click", () => showSoundingOverlay(item));
    const pop = document.createElement("button");
    pop.textContent = "Pop Out";
    pop.addEventListener("click", () => window.open(item.preview.url, "_blank", "noreferrer"));
    bar.appendChild(open);
    bar.appendChild(pop);
    card.append(title, meta, bar);
    $("soundingList").appendChild(card);
  }
}
async function renderMaps() {
  coerceDomainForRegionalModel();
  const payload = withResourceSettings({
    ...payloadBase(),
    forecast_hour: Number($("forecastHour").value || 0),
    forecast_hours: parseHourList($("renderHours").value, $("forecastHour").value),
    products: selectedProductArray(),
    place_label_density: "none"
  });
  if ($("renderMode").value === "wxstore") {
    return runJob("wxstore_plot_existing", {
      ...payload,
      run: "latest",
      hours: $("renderHours").value || $("forecastHour").value || "0",
      use_domain_bounds: true,
      png_compression: "fastest"
    }, $("renderBtn"));
  }
  return runJob("render", payload, $("renderBtn"));
}
async function localizeRun() {
  coerceDomainForRegionalModel();
  const payload = withResourceSettings({
    ...payloadBase(),
    active_hour: Number($("forecastHour").value || 0),
    forecast_hour: Number($("forecastHour").value || 0),
    hours: $("localizeHours").value || $("forecastHour").value || "0",
    products: selectedProductArray(),
    warm_grib: $("localizeMapData").checked,
    warm_soundings: $("localizeSoundings").checked,
    warm_wxstore: $("localizeWxStore").checked,
    render_after: $("localizeRenderAfter").checked,
    render_after_product_count: 1,
    png_compression: "fastest"
  });
  return runJob("localize_run", payload, $("localizeRun"));
}
async function prepareData() {
  coerceDomainForRegionalModel();
  const payload = withResourceSettings({
    ...payloadBase(),
    forecast_hour: Number($("forecastHour").value || 0),
    forecast_hours: parseHourList($("prepareHours").value, $("forecastHour").value),
    products: selectedProductArray()
  });
  return runJob("prepare_data", payload, $("prepareData"));
}
async function warmWxStore() {
  coerceDomainForRegionalModel();
  const payload = withResourceSettings({
    ...payloadBase(),
    hours: $("renderHours").value || $("forecastHour").value || "0",
    products: selectedProductArray(),
    import_wxa: true,
    render_plots: true,
    png_compression: "fastest"
  });
  return runJob("wxstore", payload, $("warmWxStore"));
}
async function preparePressureStore() {
  if (!state.soundingContext?.domain) coerceDomainForRegionalModel();
  const base = payloadBase();
  const mapContext = activePreviewContext();
  const forecastHour = Number($("soundingHour").value || state.soundingContext?.forecastHour || mapContext?.forecastHour || $("forecastHour").value || 0);
  const payload = withResourceSettings({
    ...base,
    model: state.soundingContext?.model || mapContext?.model || base.model,
    source: state.soundingContext?.source || mapContext?.source || base.source,
    run_str: state.soundingContext?.runStr || mapContext?.runStr || base.run_str,
    forecast_hour: forecastHour,
    hours: String(forecastHour),
    lat: $("soundingLat").value,
    lon: $("soundingLon").value,
    domain: state.soundingContext?.domain || mapContext?.domain || $("domain").value,
    bounds: state.soundingContext?.bounds || mapContext?.bounds || activeBounds()
  });
  return runJob("pressure_store", payload, $("prepareStore"));
}
function launchSoundingPayload(payload, fromMapClick = false) {
  clearDeadInteractionLocks();
  if (fromMapClick) return launchMapSoundingDirect(payload);
  return runJob("sounding", payload, $("soundingRender"), {
    lockButton: true,
    onLaunch: (job) => {
      state.activeSoundingJobId = job.id;
      state.activeSoundingJobStartedAt = Date.now();
      updateSoundingBusy();
    },
    onComplete: (job) => {
      if (state.activeSoundingJobId === job?.id) {
        state.activeSoundingJobId = null;
        state.activeSoundingJobStartedAt = 0;
        updateSoundingBusy();
      }
    }
  });
}
async function launchMapSoundingDirect(payload) {
  state.mapSoundingRequestToken += 1;
  const token = state.mapSoundingRequestToken;
  if (state.mapSoundingAbortController) state.mapSoundingAbortController.abort();
  const controller = new AbortController();
  state.mapSoundingAbortController = controller;
  state.mapSoundingLaunchInFlight = true;
  state.mapSoundingLaunchStartedAt = Date.now();
  state.activeMapSoundingJobId = null;
  state.activeMapSoundingJobStartedAt = 0;
  state.queuedMapSoundingPayload = null;
  updateSoundingBusy();
  setStatus("sounding");
  try {
    const result = await post("/api/sounding", payload, {signal: controller.signal});
    if (token !== state.mapSoundingRequestToken) return {ok: false, cancelled: true, stale: true};
    showResult(result);
    if (result?.ok) {
      addSounding(result);
      setStatus("sounding complete");
    } else {
      setStatus(result?.error || "sounding failed", true);
    }
    return result;
  } catch (err) {
    if (err?.name === "AbortError") return {ok: false, cancelled: true};
    if (token === state.mapSoundingRequestToken) setStatus(String(err), true);
    return {ok: false, error: String(err)};
  } finally {
    if (token === state.mapSoundingRequestToken) {
      state.mapSoundingLaunchInFlight = false;
      state.mapSoundingLaunchStartedAt = 0;
      state.mapSoundingAbortController = null;
      updateSoundingBusy();
    }
  }
}
async function renderSounding(fromMapClick = false) {
  const base = payloadBase();
  const mapContext = activePreviewContext();
  const clickMode = $("clickSoundingMode").value;
  const sampleMethod = fromMapClick === true && (clickMode === "box" || clickMode === "draw-box")
    ? "box-mean"
    : $("soundingMethod").value;
  const lat = Number($("soundingLat").value);
  const drawnBox = sampleMethod === "box-mean" ? state.soundingContext?.drawnBox : null;
  const dims = drawnBox
    ? {widthKm: Number(drawnBox.widthKm), heightKm: Number(drawnBox.heightKm), radiusKm: Number(drawnBox.radiusKm)}
    : boxDimensions();
  const radii = drawnBox
    ? {latDeg: Number(drawnBox.latDeg), lonDeg: Number(drawnBox.lonDeg)}
    : boxRadiusDegrees(lat, dims);
  const payload = withResourceSettings({
    ...base,
    model: state.soundingContext?.model || mapContext?.model || base.model,
    source: state.soundingContext?.source || mapContext?.source || base.source,
    run_str: state.soundingContext?.runStr || mapContext?.runStr || base.run_str,
    forecast_hour: Number($("soundingHour").value || $("forecastHour").value || 0),
    lat: $("soundingLat").value,
    lon: $("soundingLon").value,
    station_id: $("soundingStation").value,
    sample_method: sampleMethod,
    data_mode: $("soundingDataMode").value,
    box_radius_km: dims.radiusKm,
    box_width_km: dims.widthKm,
    box_height_km: dims.heightKm,
    box_radius_lat_deg: radii.latDeg,
    box_radius_lon_deg: radii.lonDeg,
    box_bounds: drawnBox ? [drawnBox.west, drawnBox.south, drawnBox.east, drawnBox.north] : null,
    crop_radius_deg: Math.max(1, radii.latDeg, radii.lonDeg) + 0.1,
    domain: state.soundingContext?.domain || mapContext?.domain || $("domain").value,
    bounds: state.soundingContext?.bounds || mapContext?.bounds || activeBounds()
  });
  $("soundingMeta").textContent = sampleMethod === "box-mean"
    ? `${Number(payload.lat).toFixed(3)}, ${Number(payload.lon).toFixed(3)} F${String(payload.forecast_hour).padStart(3, "0")} | ${boxLabel(dims)} box`
    : `${Number(payload.lat).toFixed(3)}, ${Number(payload.lon).toFixed(3)} F${String(payload.forecast_hour).padStart(3, "0")}`;
  return launchSoundingPayload(payload, fromMapClick);
}
async function refreshData() {
  try {
    const data = await api("/api/data-inventory");
    state.lastInventory = data;
    const outputsBytes = (data.sections || []).filter((section) => section.category === "outputs").reduce((sum, section) => sum + Number(section.bytes || 0), 0);
    const cacheBytes = (data.sections || []).filter((section) => section.category === "cache").reduce((sum, section) => sum + Number(section.bytes || 0), 0);
    $("dataSummary").textContent = `${formatBytes(data.summary.bytes)} total | ${formatBytes(cacheBytes)} cache | ${formatBytes(outputsBytes)} outputs | ${data.summary.store_count} stores`;
    $("cacheState").innerHTML = "";
    const current = document.createElement("div");
    current.textContent = `${$("model").value.toUpperCase()} ${$("run").value} | ${$("source").value} | ${$("domain").value}`;
    const totals = document.createElement("div");
    totals.textContent = `${formatBytes(data.summary.bytes)} cached | ${data.summary.store_count} local stores`;
    const mode = document.createElement("div");
    mode.textContent = "Full dependency caching uses idx subsets where available; full GRIB files are used on routes without idx subsetting.";
    $("cacheState").append(current, totals, mode);
    $("dataList").innerHTML = "";
    for (const section of (data.sections || []).sort((a, b) => Number(b.bytes || 0) - Number(a.bytes || 0))) {
      $("dataList").appendChild(dataRow({
        title: `${section.label || section.id}`,
        subtitle: `${formatBytes(section.bytes)} | ${section.files || 0} files | ${section.dirs || 0} dirs | ${section.category}`,
        path: section.path,
        payload: {target_type: "section", id: section.id},
        disabled: !Number(section.bytes || 0)
      }));
    }
    const stores = (data.stores || []).filter((store) => store.path);
    if (stores.length) {
      const heading = document.createElement("div");
      heading.className = "meta-line";
      heading.textContent = "Local stores";
      $("dataList").appendChild(heading);
      for (const store of stores.slice(0, 40)) {
        const bytes = Number(store.bytes || store.manifest?.bytes || 0);
        const details = [
          store.kind,
          store.model,
          store.run,
          store.forecast_hours?.length ? `${store.forecast_hours.length} hours` : null,
          store.product_count ? `${store.product_count} products` : null,
          bytes ? formatBytes(bytes) : null
        ].filter(Boolean).join(" | ");
        $("dataList").appendChild(dataRow({
          title: storeLabel(store),
          subtitle: details,
          path: store.path,
          payload: {target_type: "store", path: store.path},
          disabled: false
        }));
      }
    }
    if (!$("dataList").children.length) {
      const empty = document.createElement("div");
      empty.className = "meta-line";
      empty.textContent = "No local model-map data yet.";
      $("dataList").appendChild(empty);
    }
  } catch (err) {
    $("dataSummary").textContent = String(err);
    $("cacheState").textContent = String(err);
  }
}
function storeLabel(store) {
  if (store.kind === "wxstore_spatial") return `WxStore ${[store.model, store.run].filter(Boolean).join(" ")}`;
  return `${store.kind || "Store"} ${store.name || String(store.path || "").split(/[\\/]/).pop()}`;
}
function dataRow({title, subtitle, path, payload, disabled = false}) {
  const row = document.createElement("div");
  row.className = "data-row";
  const header = document.createElement("header");
  const titleEl = document.createElement("strong");
  titleEl.textContent = title;
  const del = document.createElement("button");
  del.type = "button";
  del.className = "danger";
  del.textContent = "Delete";
  del.disabled = disabled;
  del.addEventListener("click", () => deleteData(payload, title, path, del));
  header.append(titleEl, del);
  const meta = document.createElement("div");
  meta.className = "meta-line";
  meta.textContent = subtitle || "";
  const pathEl = document.createElement("div");
  pathEl.className = "data-path";
  pathEl.textContent = path || "";
  row.append(header, meta, pathEl);
  return row;
}
async function deleteData(payload, label, path, button) {
  const ok = confirm(`Delete ${label}?\n\n${path || ""}\n\nThis removes local files and cannot be undone.`);
  if (!ok) return;
  if (button) button.disabled = true;
  setStatus(`Deleting ${label}`);
  const result = await post("/api/data-delete", payload);
  showResult(result);
  if (!result.ok) {
    setStatus(result.error || "Delete failed", true);
    if (button) button.disabled = false;
    return;
  }
  setStatus(`Deleted ${label} | ${formatBytes(result.bytes_deleted || 0)}`);
  await refreshData();
}
function formatBytes(bytes) {
  const value = Number(bytes || 0);
  if (value > 1024 ** 3) return `${(value / 1024 ** 3).toFixed(2)} GiB`;
  if (value > 1024 ** 2) return `${(value / 1024 ** 2).toFixed(1)} MiB`;
  if (value > 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${value} B`;
}
function applySize(size) {
  const [w, h] = size.split("x").map(Number);
  if (Number.isFinite(w) && Number.isFinite(h)) {
    $("width").value = w;
    $("height").value = h;
  }
  for (const btn of $("sizeButtons").querySelectorAll("button")) btn.classList.toggle("active", btn.dataset.size === size);
  saveViewerState();
}
function setPreset(products) {
  state.selectedProducts = new Set(products);
  if (easyIsCustomPreset()) state.easyCustomProducts = new Set(products);
  renderProductList();
  renderSetupCustomProducts();
  renderProductPicker();
  renderEasyPresetSummary();
  saveViewerState();
}
function syncClickModeButtons() {
  const mode = $("clickSoundingMode").value || "point";
  $("pointClickMode").classList.toggle("active", mode === "point");
  $("boxClickMode").classList.toggle("active", mode === "box");
  $("drawBoxMode").classList.toggle("active", mode === "draw-box");
  const dims = boxDimensions();
  $("clickModeMeta").textContent = mode === "draw-box"
    ? "Draw box active | drag a rectangle on the map"
    : (mode === "box" ? `Fixed box active | ${boxLabel(dims)}` : "Point click active");
}
function setClickSoundingMode(mode) {
  $("clickSoundingMode").value = mode === "draw-box" ? "draw-box" : (mode === "box" ? "box" : "point");
  if ($("clickSoundingMode").value === "box" || $("clickSoundingMode").value === "draw-box") $("soundingMethod").value = "box-mean";
  else if ($("soundingMethod").value === "box-mean") $("soundingMethod").value = "nearest";
  syncClickModeButtons();
  saveViewerState();
}
$("model").addEventListener("change", async () => { syncRunControls(false); renderDomainOptions(); await loadProducts(); syncEasyFromMain(); updateActiveMapFreshness(); saveViewerState(); });
$("modelTop").addEventListener("change", async () => { syncRunControls(true); renderDomainOptions(); await loadProducts(); syncEasyFromMain(); updateActiveMapFreshness(); saveViewerState(); });
$("source").addEventListener("change", () => {
  syncEasySourceOptions();
  setControlValue("easySource", $("source").value);
  updateActiveMapFreshness();
  saveViewerState();
});
$("run").addEventListener("input", () => { syncRunControls(false); syncArchiveControlsFromRun(); syncEasyFromMain(); updateActiveMapFreshness(); saveViewerState(); });
$("runTop").addEventListener("input", () => { syncRunControls(true); syncArchiveControlsFromRun(); syncEasyFromMain(); updateActiveMapFreshness(); saveViewerState(); });
$("run").addEventListener("blur", () => {
  const parsed = parseRunString($("run").value);
  if (parsed) setRunValue(parsed.runStr);
});
$("runTop").addEventListener("blur", () => {
  const parsed = parseRunString($("runTop").value);
  if (parsed) setRunValue(parsed.runStr);
});
$("runDate").addEventListener("change", () => useArchiveRun({announce: false, render: true}));
$("runCycle").addEventListener("change", () => useArchiveRun({announce: false, render: true}));
$("useArchiveRun").addEventListener("click", () => useArchiveRun({render: true}));
$("useLatestRun").addEventListener("click", useLatestRun);
$("easyLatest").addEventListener("click", () => {
  useLatestRun();
  syncEasyFromMain();
});
for (const button of $("runCycleButtons").querySelectorAll("button")) {
  button.addEventListener("click", () => {
    $("runCycle").value = button.dataset.cycle || "00";
    useArchiveRun({render: true});
  });
}
$("forecastHour").addEventListener("input", () => { syncRunControls(false); renderTimeline(); syncEasyFromMain(); updateActiveMapFreshness(); saveViewerState(); });
$("hourTop").addEventListener("input", () => { syncRunControls(true); renderTimeline(); syncEasyFromMain(); updateActiveMapFreshness(); saveViewerState(); });
$("prevHour").addEventListener("click", () => setHour(Number($("forecastHour").value || 0) - 1));
$("nextHour").addEventListener("click", () => setHour(Number($("forecastHour").value || 0) + 1));
$("renderTop").addEventListener("click", easyBuildAndPlot);
$("resetLatestTop").addEventListener("click", resetToLatest);
$("focusPlotTop").addEventListener("click", () => {
  document.body.classList.toggle("focus-plot");
  $("focusPlotTop").textContent = document.body.classList.contains("focus-plot") ? "Panels" : "Focus";
  applyDisplayMode();
});
$("easyModeTab").addEventListener("click", () => setViewMode("easy"));
$("advancedModeTab").addEventListener("click", () => setViewMode("advanced"));
$("easyModel").addEventListener("change", async () => { await syncMainFromEasy({load: true}); saveViewerState(); });
$("easySource").addEventListener("change", async () => { await syncMainFromEasy(); updateActiveMapFreshness(); saveViewerState(); });
$("easyDomain").addEventListener("change", async () => { await syncMainFromEasy(); saveViewerState(); });
$("easyDate").addEventListener("change", async () => {
  $("runDate").value = $("easyDate").value;
  $("runCycle").value = $("easyCycle").value || "00";
  useArchiveRun({announce: true, render: false});
  syncEasyFromMain();
});
$("easyCycle").addEventListener("change", async () => {
  $("runDate").value = $("easyDate").value || utcDateInputValue();
  $("runCycle").value = $("easyCycle").value || "00";
  useArchiveRun({announce: true, render: false});
  syncEasyFromMain();
});
$("easyHour").addEventListener("input", async () => { await syncMainFromEasy(); saveViewerState(); });
$("easyHours").addEventListener("change", () => { state.easySelectedHours = new Set(); renderEasyHourChips(); saveViewerState(); });
$("selectHours0_18").addEventListener("click", () => selectEasyHourRange(0, 18));
$("selectHours0_48").addEventListener("click", () => selectEasyHourRange(0, 48));
$("selectHoursFull").addEventListener("click", () => {
  state.easySelectedHours = new Set();
  $("easyHours").value = "full";
  $("easyCustomHours").value = "";
  renderEasyHourChips();
  saveViewerState();
});
$("clearHourSelection").addEventListener("click", clearEasyHourSelection);
$("applyCustomHours").addEventListener("click", applyEasyCustomHours);
$("easyCustomHours").addEventListener("keydown", (event) => {
  if (event.key === "Enter") applyEasyCustomHours();
});
$("easyPreset").addEventListener("change", () => applyEasyPreset($("easyPreset").value));
$("easyProductTier").addEventListener("change", () => {
  state.easyProductTier = normalizedProductTier($("easyProductTier").value);
  state.selectedProducts = new Set(easyProductSetProducts());
  renderProductList();
  renderEasyPresetSummary();
  saveViewerState();
});
$("easyWarmStore").addEventListener("change", saveViewerState);
$("easyAutoLatest").addEventListener("change", () => { updateAutoLatestTimer(); saveViewerState(); });
$("resourcePreset").addEventListener("change", () => { updateResourceMeta(); saveViewerState(); });
$("easyGo").addEventListener("click", easyBuildAndPlot);
$("easyRenderOnly").addEventListener("click", easyRenderOnly);
$("easyResetLatest").addEventListener("click", resetToLatest);
$("easyRefreshRuns").addEventListener("click", refreshLatestRuns);
$("easyOpenSetup").addEventListener("click", openSetupOverlay);
$("easyPickProducts").addEventListener("click", openProductPicker);
$("renderBtn").addEventListener("click", renderMaps);
$("localizeRun").addEventListener("click", localizeRun);
$("localizeFullRun").addEventListener("click", () => {
  const max = effectiveMaxForecastHour();
  $("localizeHours").value = `0-${max}`;
  saveViewerState();
});
$("localizeCurrentHour").addEventListener("click", () => {
  $("localizeHours").value = String($("forecastHour").value || 0);
  saveViewerState();
});
$("prepareData").addEventListener("click", prepareData);
$("warmWxStore").addEventListener("click", warmWxStore);
$("prepareStore").addEventListener("click", preparePressureStore);
$("soundingRender").addEventListener("click", () => renderSounding(false));
$("cancelSounding").addEventListener("click", () => {
  const jobIds = [...new Set([state.activeMapSoundingJobId, state.activeSoundingJobId].filter(Boolean))];
  state.mapSoundingRequestToken += 1;
  if (state.mapSoundingAbortController) state.mapSoundingAbortController.abort();
  state.mapSoundingAbortController = null;
  state.mapSoundingLaunchInFlight = false;
  state.mapSoundingLaunchStartedAt = 0;
  state.queuedMapSoundingPayload = null;
  state.activeMapSoundingJobId = null;
  state.activeMapSoundingJobStartedAt = 0;
  state.activeSoundingJobId = null;
  state.activeSoundingJobStartedAt = 0;
  updateSoundingBusy();
  for (const jobId of jobIds) cancelJob(jobId);
});
$("pointClickMode").addEventListener("click", () => setClickSoundingMode("point"));
$("boxClickMode").addEventListener("click", () => setClickSoundingMode("box"));
$("drawBoxMode").addEventListener("click", () => setClickSoundingMode("draw-box"));
$("domainSearch").addEventListener("input", () => { renderDomainOptions(); saveViewerState(); });
$("domain").addEventListener("change", () => { syncEasyDomainOptions(); updateActiveMapFreshness(); saveViewerState(); });
$("kind").addEventListener("change", () => { renderProductList(); saveViewerState(); });
$("productSearch").addEventListener("input", () => { renderProductList(); saveViewerState(); });
$("preset2m").addEventListener("click", () => setPreset(["2m_temperature_10m_winds"]));
$("presetSevere").addEventListener("click", () => setPreset(["sbcape", "mlcape", "srh_0_3km", "stp_fixed"]));
$("presetUpper").addEventListener("click", () => setPreset(["500mb_temperature_height_winds"]));
$("clearProducts").addEventListener("click", () => setPreset([]));
$("setupOverlayClose").addEventListener("click", closeSetupOverlay);
$("setupOverlay").addEventListener("click", (event) => {
  if (event.target === $("setupOverlay")) closeSetupOverlay();
});
$("setupProductGroup").addEventListener("change", renderSetupCustomProducts);
$("setupProductSearch").addEventListener("input", renderSetupCustomProducts);
$("setupUseCustom").addEventListener("click", activateCustomRun);
$("setupClearCustom").addEventListener("click", clearCustomProducts);
$("productPickerClose").addEventListener("click", closeProductPicker);
$("productPickerOverlay").addEventListener("click", (event) => {
  if (event.target === $("productPickerOverlay")) closeProductPicker();
});
$("productPickerGroup").addEventListener("change", renderProductPicker);
$("productPickerSearch").addEventListener("input", renderProductPicker);
$("productPickerUseCustom").addEventListener("click", () => {
  activateCustomRun();
  closeProductPicker();
});
$("productPickerSelectVisible").addEventListener("click", selectVisibleProductPickerRows);
$("productPickerClear").addEventListener("click", clearCustomProducts);
$("productWorkflowSelect").addEventListener("change", () => {
  state.activeWorkflowId = $("productWorkflowSelect").value;
  const workflow = state.productWorkflows.find((item) => item.id === state.activeWorkflowId);
  $("productWorkflowName").value = workflow?.name || "";
  saveWorkflowStore();
  saveViewerState();
});
$("saveWorkflow").addEventListener("click", saveProductWorkflow);
$("applyWorkflow").addEventListener("click", () => applyProductWorkflow($("productWorkflowSelect").value));
$("deleteWorkflow").addEventListener("click", deleteProductWorkflow);
$("workflowAutoApply").addEventListener("change", () => {
  saveWorkflowStore();
  saveViewerState();
  if ($("workflowAutoApply").checked && state.activeWorkflowId) applyProductWorkflow(state.activeWorkflowId);
});
$("fitPlot").addEventListener("click", () => { state.displayMode = "fit"; applyDisplayMode(); saveViewerState(); });
$("fullPlot").addEventListener("click", () => { state.displayMode = "full"; applyDisplayMode(); saveViewerState(); });
$("openPlot").addEventListener("click", () => { if (state.activePreview) window.open(state.activePreview.url, "_blank", "noreferrer"); });
$("refreshData").addEventListener("click", refreshData);
$("soundingOverlayClose").addEventListener("click", closeSoundingOverlay);
$("soundingOverlayOpen").addEventListener("click", () => {
  const src = $("soundingOverlayImg").src;
  if (src) window.open(src, "_blank", "noreferrer");
});
$("soundingOverlay").addEventListener("click", (event) => {
  if (event.target === $("soundingOverlay")) closeSoundingOverlay();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    emergencyUnlockInteraction();
  }
});
document.addEventListener("pointerdown", clearDeadInteractionLocks, true);
setInterval(clearDeadInteractionLocks, 1500);
window.addEventListener("resize", applyDisplayMode);
window.addEventListener("pointerup", () => setTimeout(() => cancelBoxDrag("draw box canceled"), 0));
window.addEventListener("pointercancel", () => cancelBoxDrag("draw box canceled"));
window.addEventListener("blur", () => cancelBoxDrag());
for (const btn of $("sizeButtons").querySelectorAll("button")) btn.addEventListener("click", () => applySize(btn.dataset.size));
for (const id of ["renderHours", "prepareHours", "localizeHours", "localizeJobs", "localizeMapData", "localizeSoundings", "localizeWxStore", "localizeRenderAfter", "width", "height", "renderMode", "soundingLat", "soundingLon", "soundingHour", "soundingDataMode", "soundingMethod", "soundingBoxKm", "soundingBoxShape", "soundingBoxWidthKm", "soundingBoxHeightKm", "clickSoundingMode", "autoOpenSounding", "soundingStation", "productWorkflowName", "resourcePreset"]) {
  $(id).addEventListener("input", saveViewerState);
  $(id).addEventListener("change", saveViewerState);
}
for (const id of ["soundingBoxShape", "soundingBoxWidthKm", "soundingBoxHeightKm"]) {
  $(id).addEventListener("input", () => syncBoxControls(id));
  $(id).addEventListener("change", () => syncBoxControls(id));
}
for (const button of $("boxPresetRow").querySelectorAll("button")) {
  button.addEventListener("click", () => {
    const [width, height] = String(button.dataset.boxSize || "25x25").split("x");
    $("soundingBoxWidthKm").value = width;
    $("soundingBoxHeightKm").value = height || width;
    $("soundingBoxShape").value = "custom";
    syncBoxControls();
  });
}
$("clickSoundingMode").addEventListener("change", syncClickModeButtons);
loadBootstrap().catch((err) => {
  setStatus(String(err), true);
  showResult({ok: false, error: String(err)});
});
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(run_cli())
