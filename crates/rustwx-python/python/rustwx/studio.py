"""Standalone local web UI for rustwx.

This server intentionally depends only on the public ``rustwx`` Python wheel
surface and optional rustwx workspace binaries. It is separate from Hermes and
is meant to be the no-AI browser path for model maps, satellite, radar, and
point sampling.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.metadata
import json
import math
import mimetypes
import os
import re
import shutil
import subprocess
import threading
import time
import traceback
import uuid
import webbrowser
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

import rustwx
from rustwx.radar_sites import conus_radar_sites, radar_sites_geojson


_JOB_CONTEXT = threading.local()


class JobCancelled(RuntimeError):
    pass


APP_TITLE = "RustWx Studio"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8776
DEFAULT_WXSTORE_URL = "http://127.0.0.1:8897"
NEXRAD_LEVEL2_BASE_URL = "https://unidata-nexrad-level2.s3.amazonaws.com"
DEFAULT_OUTPUT_WIDTH = 1600
DEFAULT_OUTPUT_HEIGHT = 1100
MAX_BODY_BYTES = 2_000_000
WXPROFILE_STORE_BINARY_NAMES = ("model_wxprofile_store", "hrrr_wxprofile_store")
RADAR_BASEMAP_WIDTH = 1000
RADAR_BASEMAP_HEIGHT = 590
RADAR_BASEMAP_BOUNDS = (-126.0, -66.0, 24.0, 50.0)
RADAR_BASEMAP_PROJECTION = {
    "map_proj": 1,
    "truelat1": 33.0,
    "truelat2": 45.0,
    "stand_lon": -98.0,
    "cen_lat": 38.5,
    "cen_lon": -98.0,
}


def _wxprofile_store_binary(binaries: dict[str, Path]) -> Path | None:
    for name in WXPROFILE_STORE_BINARY_NAMES:
        binary = binaries.get(name)
        if binary is not None:
            return binary
    return None


def _has_wxprofile_store_binary(binaries: dict[str, Path]) -> bool:
    return _wxprofile_store_binary(binaries) is not None

SATELLITE_PRODUCTS = [
    "goes_geocolor",
    "goes_glm_fed_geocolor",
    "goes_airmass_rgb",
    "goes_sandwich_rgb",
    "goes_day_night_cloud_micro_combo_rgb",
    "goes_fire_temperature_rgb",
    "goes_dust_rgb",
    *[f"goes_abi_band_{i:02d}" for i in range(1, 17)],
]

RADAR_PRODUCTS = [
    ("ref", "Reflectivity"),
    ("vel", "Velocity"),
    ("sw", "Spectrum Width"),
    ("zdr", "ZDR"),
    ("cc", "Correlation Coef"),
    ("kdp", "KDP"),
    ("vil", "VIL"),
    ("et", "Echo Tops"),
    ("all", "All Products"),
]

CROSS_SECTION_PRODUCTS = [
    ("temperature", "Temperature"),
    ("wind_speed", "Wind Speed"),
    ("theta_e", "Theta-E"),
    ("rh", "Relative Humidity"),
    ("q", "Specific Humidity"),
    ("omega", "Vertical Velocity"),
    ("vorticity", "Vorticity"),
    ("shear", "Wind Shear"),
    ("lapse_rate", "Lapse Rate"),
    ("cloud", "Cloud Water"),
    ("cloud_total", "Total Condensate"),
    ("wetbulb", "Wet-Bulb"),
    ("icing", "Icing"),
    ("frontogenesis", "Frontogenesis"),
    ("smoke", "Smoke"),
    ("vpd", "Vapor Pressure Deficit"),
    ("dewpoint_dep", "Dewpoint Depression"),
    ("moisture_transport", "Moisture Transport"),
    ("pv", "Potential Vorticity"),
    ("fire_wx", "Fire Weather"),
]

CROSS_SECTION_ROUTES = [
    {
        "id": "socal-coast-desert",
        "name": "SoCal Coast to Desert",
        "start": [34.0195, -118.4912],
        "end": [33.8303, -116.5453],
    },
    {
        "id": "dryline-southern-plains",
        "name": "Southern Plains Dryline",
        "start": [34.739, -103.205],
        "end": [36.153, -95.992],
    },
    {
        "id": "front-range-high-plains",
        "name": "Front Range to High Plains",
        "start": [39.739, -105.000],
        "end": [39.114, -100.873],
    },
    {
        "id": "gulf-to-mid-south",
        "name": "Gulf to Mid-South",
        "start": [29.951, -90.072],
        "end": [35.149, -90.049],
    },
    {
        "id": "custom",
        "name": "Custom",
        "start": [35.222, -99.000],
        "end": [35.222, -95.000],
    },
]

COMMON_DOMAINS = [
    "conus",
    "southern-plains",
    "southern_plains",
    "oklahoma",
    "ok_oklahoma_city",
    "midwest",
    "great-lakes",
    "great_lakes",
    "northeast",
    "southeast",
    "california",
    "gulf-to-kansas",
    "gulf_to_kansas",
]


def run_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rustwx-studio",
        description="Run the standalone local RustWx web UI.",
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser tab.")
    parser.add_argument("--out-root", default=None, help="Artifact output root.")
    parser.add_argument("--cache-dir", default=None, help="Shared data cache root.")
    parser.add_argument("--bin-dir", default=None, help="Directory containing optional rustwx binaries.")
    args = parser.parse_args(argv)

    env = StudioEnv.from_args(args)
    server = StudioServer((args.host, args.port), env)
    url = f"http://{args.host}:{args.port}/"
    print(f"{APP_TITLE} running at {url}")
    print(f"rustwx: {env.version}")
    print(f"outputs: {env.out_root}")
    if not args.no_open:
        threading.Timer(0.2, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()
    return 0


class StudioEnv:
    def __init__(self, *, out_root: Path, cache_dir: Path, bin_dir: Path | None):
        self.out_root = out_root.resolve()
        self.cache_dir = cache_dir.resolve()
        self.bin_dir = bin_dir.resolve() if bin_dir else None
        self.version = _package_version()
        self.capabilities = _load_json(rustwx.agent_capabilities_json())
        self.binaries = _discover_binaries(self.bin_dir)

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "StudioEnv":
        out_root = Path(args.out_root or os.environ.get("RUSTWX_OUTPUT_DIR") or "rustwx_outputs")
        cache_dir = Path(args.cache_dir or os.environ.get("RUSTWX_CACHE_DIR") or out_root / "cache")
        bin_dir = args.bin_dir or os.environ.get("RUSTWX_BIN_DIR")
        return cls(out_root=out_root, cache_dir=cache_dir, bin_dir=Path(bin_dir) if bin_dir else None)

    def subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.setdefault("RUSTWX_PLOT_STYLE", "operational_fast")
        env.setdefault("RUSTWX_CACHE_DIR", str(self.cache_dir))
        if self.bin_dir:
            env.setdefault("RUSTWX_BIN_DIR", str(self.bin_dir))
        return env


class StudioServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], env: StudioEnv):
        super().__init__(server_address, StudioHandler)
        self.env = env
        self.started_at = time.time()
        self.allowed_file_roots = {
            env.out_root,
            env.cache_dir,
            (Path.cwd() / "rustwx_outputs").resolve(),
        }
        self.jobs: dict[str, dict] = {}
        self.jobs_lock = threading.Lock()
        self.pressure_store_lock = threading.Lock()
        self.wxstore_process: subprocess.Popen | None = None
        self.wxstore_lock = threading.Lock()


class StudioHandler(BaseHTTPRequestHandler):
    server: StudioServer

    def log_message(self, fmt: str, *args) -> None:
        print(f"[rustwx-studio] {self.address_string()} {fmt % args}")

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
            elif parsed.path == "/api/domains":
                query = parse_qs(parsed.query)
                self._send_json(self._domains(_query_one(query, "kind", None), _query_one(query, "search", None)))
            elif parsed.path == "/api/radar-sites":
                self._send_json(_radar_sites_response())
            elif parsed.path == "/api/radar-basemap":
                self._send_json(_radar_basemap_response(self.server.env.out_root))
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
            elif parsed.path == "/api/satellite":
                self._send_json(self._render_satellite(payload))
            elif parsed.path == "/api/satellite-sequence":
                self._send_json(self._render_satellite_sequence(payload))
            elif parsed.path == "/api/satellite-tile-loop":
                self._send_json(self._render_satellite_tile_loop(payload))
            elif parsed.path == "/api/generation-plan":
                self._send_json(self._run_generation_plan(payload))
            elif parsed.path == "/api/case-dataset":
                self._send_json(self._run_case_dataset(payload))
            elif parsed.path == "/api/radar":
                self._send_json(self._render_radar(payload))
            elif parsed.path == "/api/radar-tiles":
                self._send_json(self._render_radar_tiles(payload))
            elif parsed.path == "/api/radar-tile-loop":
                self._send_json(self._render_radar_tile_loop(payload))
            elif parsed.path == "/api/meteogram":
                self._send_json(self._sample_meteogram(payload))
            elif parsed.path == "/api/meteogram-store":
                self._send_json(self._warm_meteogram_store(payload))
            elif parsed.path == "/api/sounding":
                self._send_json(self._render_sounding(payload))
            elif parsed.path == "/api/pressure-store":
                self._send_json(self._prepare_pressure_store(payload))
            elif parsed.path == "/api/cross-section":
                self._send_json(self._render_cross_section(payload))
            elif parsed.path == "/api/wxstore-inspect":
                self._send_json(self._inspect_wxstore(payload))
            elif parsed.path == "/api/wxstore-service":
                self._send_json(self._wxstore_service(payload))
            elif parsed.path == "/api/wxstore":
                self._send_json(self._run_wxstore_pipeline(payload))
            elif parsed.path == "/api/ecape-profile":
                self._send_json(self._run_ecape_profile(payload))
            elif parsed.path == "/api/ecape-grid":
                self._send_json(self._run_ecape_grid(payload))
            elif parsed.path == "/api/ecape-ratio":
                self._send_json(self._run_ecape_ratio(payload))
            elif parsed.path == "/api/native-dataset-plan":
                self._send_json(self._run_native_dataset_plan(payload))
            elif parsed.path == "/api/native-dataset-run":
                self._send_json(self._run_native_dataset_runner(payload))
            elif parsed.path == "/api/native-obs-preview":
                self._send_json(self._run_native_obs_preview(payload))
            elif parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/cancel"):
                job_id = parsed.path.strip("/").split("/")[2]
                self._send_json(self._cancel_job(job_id))
            elif parsed.path == "/api/jobs":
                self._send_json(self._start_job(payload))
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
            "satellite_default": "goes19",
            "uptime_s": round(time.time() - self.server.started_at, 1),
            "doctor": self._doctor(),
            "models": self._models(),
            "domains": self._domains(None, None),
            "satellite_products": SATELLITE_PRODUCTS,
            "radar_products": [{"slug": slug, "label": label} for slug, label in RADAR_PRODUCTS],
            "radar_sites": _radar_sites_response(),
            "radar_basemap": _radar_basemap_response(self.server.env.out_root),
            "sounding_sample_methods": [
                {"slug": "nearest", "label": "Nearest"},
                {"slug": "inverse-distance4", "label": "Inverse Distance"},
                {"slug": "box-mean", "label": "Box Mean"},
            ],
            "cross_section_products": [
                {"slug": slug, "label": label, "kind": "section"}
                for slug, label in CROSS_SECTION_PRODUCTS
            ],
            "cross_section_routes": CROSS_SECTION_ROUTES,
            "jobs": self._jobs(limit=5),
        }

    def _jobs(self, *, limit: int | None = None) -> dict:
        with self.server.jobs_lock:
            jobs = sorted(
                self.server.jobs.values(),
                key=lambda job: float(job.get("created_at_ts", 0.0)),
                reverse=True,
            )
            if limit is not None:
                jobs = jobs[:limit]
            return {
                "ok": True,
                "count": len(self.server.jobs),
                "jobs": [_job_snapshot(job, detail=False) for job in jobs],
            }

    def _data_inventory(self) -> dict:
        env = self.server.env
        out_root = env.out_root / "studio"
        cache_root = env.cache_dir
        sections = []
        for section_id, label, path, category in [
            ("maps", "Maps", out_root / "maps", "outputs"),
            ("satellite", "Satellite", out_root / "satellite", "outputs"),
            ("satellite_native", "Satellite Native", out_root / "satellite_native", "outputs"),
            ("satellite_tile_lanes", "Satellite Tile Lanes", cache_root / "studio_satellite_tiles", "cache"),
            ("radar", "Radar PNGs", out_root / "radar", "outputs"),
            ("radar_tiles", "Radar Tiles", out_root / "radar_tiles", "outputs"),
            ("soundings", "Soundings", out_root / "soundings", "outputs"),
            ("cross_sections", "Cross Sections", out_root / "cross_sections", "outputs"),
            ("wxstore_outputs", "WxStore Outputs", out_root / "wxstore", "outputs"),
            ("wxstore_existing", "WxStore Plots", out_root / "wxstore_existing", "outputs"),
            ("case_dataset", "Case Datasets", out_root / "case_dataset", "outputs"),
            ("native_dataset", "Native Datasets", out_root / "native_dataset", "outputs"),
            ("native_obs_preview", "Native Obs", out_root / "native_obs_preview", "outputs"),
            ("satellite_cache", "Satellite Cache", cache_root / "satellite", "cache"),
            ("radar_cache", "Radar Level-II Cache", cache_root / "radar", "cache"),
            ("wxprofile_stores", "WxProfile Stores", cache_root / "studio_wxprofile_stores", "cache"),
            ("pressure_stores", "Pressure Stores", cache_root / "studio_pressure_stores", "cache"),
            ("volume_stores", "Volume Stores", cache_root / "studio_volume_stores", "cache"),
            ("wxstore_spatial", "WxStore Spatial", cache_root / "studio_wxstore_spatial", "cache"),
            ("radar_tile_lanes", "Radar Tile Lanes", cache_root / "studio_radar_tiles", "cache"),
        ]:
            sections.append(_inventory_section(section_id, label, path, category))
        model_ids = {
            str(model.get("id"))
            for model in (getattr(env, "capabilities", {}) or {}).get("models", [])
            if model.get("id")
        }
        model_dirs = sorted(path for path in cache_root.iterdir() if path.is_dir()) if cache_root.is_dir() else []
        for model_dir in model_dirs:
            if model_dir.name in model_ids:
                sections.append(_inventory_section(f"model_cache_{model_dir.name}", f"{model_dir.name} Cache", model_dir, "cache"))
        sections = [section for section in sections if section.get("exists")]
        stores = [
            *_inventory_wxstore_runs(cache_root / "studio_wxstore_spatial"),
            *_inventory_satellite_layers(cache_root / "studio_satellite_tiles"),
            *_inventory_radar_layers(cache_root / "studio_radar_tiles"),
            *_inventory_store_dirs(cache_root / "studio_wxprofile_stores", "wxprofile_store"),
            *_inventory_store_dirs(cache_root / "studio_pressure_stores", "pressure_store"),
            *_inventory_store_dirs(cache_root / "studio_volume_stores", "volume_store"),
        ]
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
                "studio_root": str(out_root),
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

    def _job(self, job_id: str) -> dict:
        with self.server.jobs_lock:
            job = self.server.jobs.get(job_id)
            if not job:
                return {"ok": False, "error": f"unknown job {job_id!r}"}
            return {"ok": True, "job": _job_snapshot(job, detail=True)}

    def _start_job(self, payload: dict) -> dict:
        kind = str(payload.get("kind") or "").strip()
        job_payload = payload.get("payload") or {}
        if not isinstance(job_payload, dict):
            return {"ok": False, "error": "Job payload must be an object."}
        if kind not in _job_kinds():
            return {"ok": False, "error": f"Unknown job kind {kind!r}."}
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
        }
        with self.server.jobs_lock:
            self.server.jobs[job_id] = job
            _trim_jobs_locked(self.server.jobs)
        thread = threading.Thread(
            target=self._run_job,
            args=(job_id, kind, job_payload),
            name=f"rustwx-studio-{kind}-{job_id}",
            daemon=True,
        )
        thread.start()
        return {"ok": True, "job": _job_snapshot(job, detail=True)}

    def _cancel_job(self, job_id: str) -> dict:
        process = None
        with self.server.jobs_lock:
            job = self.server.jobs.get(job_id)
            if not job:
                return {"ok": False, "error": f"unknown job {job_id!r}"}
            if job.get("status") in {"completed", "failed", "cancelled"}:
                return {"ok": True, "job": _job_snapshot(job, detail=True), "message": "Job already finished."}
            job["cancel_requested"] = True
            job["status"] = "cancelling" if job.get("status") == "running" else "cancelled"
            job["updated_at_ts"] = time.time()
            job["updated_at"] = _iso_from_ts(float(job["updated_at_ts"]))
            if job["status"] == "cancelled":
                job["finished_at_ts"] = job["updated_at_ts"]
                job["finished_at"] = job["updated_at"]
                job["result"] = {"ok": False, "cancelled": True, "error": "Job cancelled before it started."}
                job["error"] = "cancelled"
            process = job.get("_active_process")
            snapshot = _job_snapshot(job, detail=True)
        if process is not None and process.poll() is None:
            try:
                process.terminate()
            except Exception:
                pass
        return {"ok": True, "job": snapshot}

    def _run_job(self, job_id: str, kind: str, payload: dict) -> None:
        with self.server.jobs_lock:
            job = self.server.jobs.get(job_id)
            if job and job.get("cancel_requested"):
                self._update_job(
                    job_id,
                    status="cancelled",
                    result={"ok": False, "cancelled": True, "error": "Job cancelled before it started."},
                    error="cancelled",
                    finished_at_ts=time.time(),
                )
                return
        _JOB_CONTEXT.server = self.server
        _JOB_CONTEXT.job_id = job_id
        self._update_job(job_id, status="running", started_at_ts=time.time())
        try:
            _raise_if_job_cancelled()
            result = self._execute_job(kind, payload)
            ok = bool(result.get("ok", "error" not in result)) if isinstance(result, dict) else True
            cancelled = _job_cancel_requested() or (isinstance(result, dict) and bool(result.get("cancelled")))
            if cancelled and isinstance(result, dict):
                result["ok"] = False
                result["cancelled"] = True
                result["error"] = "Job cancelled."
            self._update_job(
                job_id,
                status="cancelled" if cancelled else ("completed" if ok else "failed"),
                result=result,
                error="cancelled" if cancelled else (None if ok else (result.get("error") if isinstance(result, dict) else "job failed")),
                finished_at_ts=time.time(),
            )
        except JobCancelled as exc:
            self._update_job(
                job_id,
                status="cancelled",
                result={"ok": False, "cancelled": True, "error": str(exc) or "Job cancelled."},
                error="cancelled",
                finished_at_ts=time.time(),
            )
        except Exception as exc:
            self._update_job(
                job_id,
                status="failed",
                result={"ok": False, "error": str(exc)},
                error=str(exc),
                traceback=traceback.format_exc().splitlines()[-40:],
                finished_at_ts=time.time(),
            )
        finally:
            _clear_job_process()
            _JOB_CONTEXT.server = None
            _JOB_CONTEXT.job_id = None

    def _update_job(self, job_id: str, **updates: object) -> None:
        now = time.time()
        with self.server.jobs_lock:
            job = self.server.jobs.get(job_id)
            if not job:
                return
            job.update(updates)
            job["updated_at_ts"] = now
            job["updated_at"] = _iso_from_ts(now)
            if "started_at_ts" in updates and updates["started_at_ts"]:
                job["started_at"] = _iso_from_ts(float(updates["started_at_ts"]))
            if "finished_at_ts" in updates and updates["finished_at_ts"]:
                job["finished_at"] = _iso_from_ts(float(updates["finished_at_ts"]))

    def _execute_job(self, kind: str, payload: dict) -> dict:
        if kind == "render":
            return self._render_maps(payload)
        if kind == "prepare_data":
            return self._prepare_model_data(payload)
        if kind == "satellite":
            return self._render_satellite(payload)
        if kind == "satellite_sequence":
            return self._render_satellite_sequence(payload)
        if kind == "satellite_tile_loop":
            return self._render_satellite_tile_loop(payload)
        if kind == "generation_plan":
            return self._run_generation_plan(payload)
        if kind == "case_dataset":
            return self._run_case_dataset(payload)
        if kind == "radar":
            return self._render_radar(payload)
        if kind == "radar_tiles":
            return self._render_radar_tiles(payload)
        if kind == "radar_tile_loop":
            return self._render_radar_tile_loop(payload)
        if kind == "meteogram":
            return self._sample_meteogram(payload)
        if kind == "meteogram_store":
            return self._warm_meteogram_store(payload)
        if kind == "sounding":
            return self._render_sounding(payload)
        if kind == "pressure_store":
            return self._prepare_pressure_store(payload)
        if kind == "cross_section":
            return self._render_cross_section(payload)
        if kind == "wxstore":
            return self._run_wxstore_pipeline(payload)
        if kind == "wxstore_plot_existing":
            return self._plot_existing_wxstore(payload)
        if kind == "ecape_profile":
            return self._run_ecape_profile(payload)
        if kind == "ecape_grid":
            return self._run_ecape_grid(payload)
        if kind == "ecape_ratio":
            return self._run_ecape_ratio(payload)
        if kind == "native_dataset_plan":
            return self._run_native_dataset_plan(payload)
        if kind == "native_dataset_run":
            return self._run_native_dataset_runner(payload)
        if kind == "native_obs_preview":
            return self._run_native_obs_preview(payload)
        return {"ok": False, "error": f"Unknown job kind {kind!r}."}

    def _doctor(self) -> dict:
        env = self.server.env
        models = env.capabilities.get("models") or []
        domains = env.capabilities.get("domains") or {}
        return {
            "rustwx_version": env.version,
            "agent_api": env.capabilities.get("agent_api"),
            "plot_style": os.environ.get("RUSTWX_PLOT_STYLE", "operational_fast"),
            "cache_dir": str(env.cache_dir),
            "out_root": str(env.out_root),
            "models": [m.get("id") for m in models],
            "domain_count": domains.get("count"),
            "optional_binaries": {name: str(path) for name, path in env.binaries.items()},
            "specialty_tools": {
                "radar": "radar_export" in env.binaries,
                "radar_tiles": "radar_web_tiles" in env.binaries,
                "satellite_tiles": "goes_web_tiles" in env.binaries,
                "sounding": "sounding_plot" in env.binaries,
                "fast_soundings": (
                    (
                        _has_wxprofile_store_binary(env.binaries)
                        and "wxprofile_sounding_render" in env.binaries
                    )
                    or (
                        "hrrr_pressure_volume_store" in env.binaries
                        and "volume_store_sounding_render" in env.binaries
                    )
                ),
                "wxprofile_soundings": (
                    _has_wxprofile_store_binary(env.binaries)
                    and "wxprofile_sounding_render" in env.binaries
                ),
                "cross_section": (
                    "hrrr_pressure_volume_store" in env.binaries
                    and "volume_store_cross_section_render" in env.binaries
                ),
                "wxstore_export": "rustwx_grid_export" in env.binaries,
                "wxstore_import": "wxstore" in env.binaries,
                "wxstore_showcase": "wxstore_wxa_showcase" in env.binaries,
                "ecape_profile": "hrrr_ecape_profile_probe" in env.binaries,
                "ecape_grid": "hrrr_ecape_grid_research" in env.binaries,
                "native_dataset_plan": "native_dataset_plan" in env.binaries,
                "native_dataset_run": "native_dataset_runner" in env.binaries,
                "native_obs_preview": "native_obs_preview" in env.binaries,
                "point_store": (
                    hasattr(rustwx, "warm_point_timeseries_store_json")
                    and hasattr(rustwx, "sample_point_timeseries_store_json")
                ),
            },
        }

    def _models(self) -> dict:
        models = []
        for model in self.server.env.capabilities.get("models") or []:
            models.append({
                "id": model.get("id"),
                "default_product": model.get("default_product"),
                "default_render_product": model.get("default_render_product"),
                "max_forecast_hour": model.get("max_forecast_hour"),
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

    def _products(self, model_id: str) -> dict:
        model = _model_entry(self.server.env.capabilities, model_id)
        if not model:
            return {"ok": False, "error": f"unknown model {model_id!r}"}
        groups = {
            "direct": model.get("direct_recipes") or [],
            "light_derived": model.get("light_derived_recipes") or [],
            "heavy_derived": model.get("heavy_derived_recipes") or [],
            "windowed": model.get("windowed_products") or [],
        }
        return {
            "ok": True,
            "model": model_id,
            "groups": groups,
            "count": sum(len(items) for items in groups.values()),
        }

    def _run_generation_plan(self, payload: dict) -> dict:
        action = str(payload.get("action") or "plan").strip().lower().replace("-", "_")
        if action not in {"plan", "prepare_data", "render"}:
            return {"ok": False, "error": "Batch action must be plan, prepare_data, or render."}
        models = _batch_model_ids(self.server.env.capabilities, payload, str(payload.get("current_model") or "hrrr"))
        if not models:
            return {"ok": False, "error": "No batch models matched."}
        source = str(payload.get("source") or "aws")
        run_str = str(payload.get("run_str") or "latest")
        domain = str(payload.get("domain") or "conus")
        forecast_hours = _forecast_hours_from_payload(payload)
        product_mode = str(payload.get("product_mode") or "default").strip().lower().replace("-", "_")
        product_kind = str(payload.get("product_kind") or "").strip()
        explicit_products = [
            str(item).strip()
            for item in payload.get("products") or []
            if str(item).strip()
        ]
        max_products_raw = payload.get("max_products_per_model")
        max_products_per_model = int(max_products_raw) if str(max_products_raw or "").strip() else None
        width = int(payload.get("width") or DEFAULT_OUTPUT_WIDTH)
        height = int(payload.get("height") or DEFAULT_OUTPUT_HEIGHT)
        started = time.time()
        plan_rows = []
        for model_id in models:
            products = _batch_products_for_model(
                self.server.env.capabilities,
                model_id,
                product_mode,
                product_kind,
                explicit_products,
            )
            if max_products_per_model is not None:
                products = products[: max(0, max_products_per_model)]
            model_entry = _model_entry(self.server.env.capabilities, model_id) or {}
            plan_rows.append({
                "model": model_id,
                "products": products,
                "product_count": len(products),
                "forecast_hours": forecast_hours,
                "hour_count": len(forecast_hours),
                "plot_count": len(products) * len(forecast_hours),
                "max_forecast_hour": model_entry.get("max_forecast_hour"),
            })
        total_products = sum(row["product_count"] for row in plan_rows)
        total_plots = sum(row["plot_count"] for row in plan_rows)
        if action == "plan":
            return {
                "ok": True,
                "stage": "generation_plan",
                "action": action,
                "dry_run": True,
                "model_count": len(plan_rows),
                "total_products": total_products,
                "total_plots": total_plots,
                "domain": domain,
                "source": source,
                "run_str": run_str,
                "width": width,
                "height": height,
                "models": plan_rows,
                "ui_elapsed_s": round(time.time() - started, 2),
            }

        results = []
        for row in plan_rows:
            if not row["products"]:
                results.append({"ok": False, "model": row["model"], "error": "No products selected."})
                continue
            request = {
                "model": row["model"],
                "source": source,
                "run_str": run_str,
                "domain": domain,
                "forecast_hour": forecast_hours[0],
                "forecast_hours": forecast_hours,
                "products": row["products"],
                "width": width,
                "height": height,
                "chunk_size": int(payload.get("chunk_size") or 8),
            }
            if action == "prepare_data":
                results.append(self._prepare_model_data(request))
            else:
                results.append(self._render_maps(request))
        result = {
            "ok": all(bool(item.get("ok", "error" not in item)) for item in results),
            "stage": "generation_plan",
            "action": action,
            "dry_run": False,
            "model_count": len(plan_rows),
            "total_products": total_products,
            "total_plots": total_plots,
            "domain": domain,
            "source": source,
            "run_str": run_str,
            "width": width,
            "height": height,
            "models": plan_rows,
            "results": results,
        }
        return self._attach_previews(result, started, [f"{len(plan_rows)} models"])

    def _run_case_dataset(self, payload: dict) -> dict:
        action = str(payload.get("action") or "plan").strip().lower().replace("-", "_")
        mode = str(payload.get("mode") or "render").strip().lower().replace("-", "_")
        if action not in {"plan", "render", "probe"}:
            return {"ok": False, "error": "Case action must be plan, render, or probe."}
        if mode not in {"render", "probe"}:
            return {"ok": False, "error": "Case mode must be render or probe."}
        if action == "probe":
            mode = "probe"
        started = time.time()
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        start_date = str(payload.get("start_date") or datetime.now(UTC).strftime("%Y-%m-%d"))
        end_date = str(payload.get("end_date") or start_date)
        cycles = _cycles_from_payload(payload, model)
        forecast_hours = _forecast_hours_from_payload(payload)
        specs = _case_specs(
            model=model,
            start_date=start_date,
            end_date=end_date,
            cycles=cycles,
            forecast_hours=forecast_hours,
        )
        limit_raw = payload.get("limit")
        limit = int(limit_raw) if str(limit_raw or "").strip() else None
        if limit is not None:
            specs = specs[: max(0, limit)]
        products = [
            str(item).strip()
            for item in payload.get("products") or []
            if str(item).strip()
        ]
        domain_slug = str(payload.get("domain") or "conus")
        points = _case_points_from_payload(payload)
        unit_count = len(specs) * (len(points) if mode == "probe" else 1)
        plan = {
            "ok": True,
            "stage": "case_dataset",
            "dry_run": action == "plan",
            "mode": mode,
            "model": model,
            "source": source,
            "start_date": start_date,
            "end_date": end_date,
            "cycles": cycles,
            "forecast_hours": forecast_hours,
            "domain": domain_slug,
            "spec_count": len(specs),
            "unit_count": unit_count,
            "products": products,
            "product_count": len(products),
            "profile_points": points,
            "profile_point_count": len(points),
            "limit": limit,
            "specs": specs[:200],
            "ui_elapsed_s": round(time.time() - started, 2),
        }
        if action == "plan":
            return plan
        if mode == "render" and not products:
            return {"ok": False, "error": "Render mode needs at least one selected product."}
        if mode == "probe" and not points:
            return {"ok": False, "error": "Probe mode needs at least one lat,lon point."}

        out_root = self.server.env.out_root / "studio" / "case_dataset" / mode / time.strftime("%Y%m%d_%H%M%S")
        out_root.mkdir(parents=True, exist_ok=True)
        results = []
        if mode == "render":
            for spec in specs:
                spec_out = out_root / f"{spec['date_yyyymmdd']}_{spec['cycle_utc']:02d}z_f{spec['forecast_hour']:03d}"
                request = {
                    "model": model,
                    "source": source,
                    "run_str": f"{spec['date_yyyymmdd']}/{spec['cycle_utc']:02d}z",
                    "domain": domain_slug,
                    "forecast_hour": int(spec["forecast_hour"]),
                    "forecast_hours": [int(spec["forecast_hour"])],
                    "products": products,
                    "width": int(payload.get("width") or DEFAULT_OUTPUT_WIDTH),
                    "height": int(payload.get("height") or DEFAULT_OUTPUT_HEIGHT),
                    "chunk_size": int(payload.get("chunk_size") or 8),
                    "out_dir": str(spec_out),
                }
                rendered = self._render_maps(request)
                rendered["case_spec"] = spec
                results.append(rendered)
        else:
            for spec in specs:
                for point in points:
                    request = {
                        "model": model,
                        "source": source,
                        "run_str": f"{spec['date_yyyymmdd']}/{spec['cycle_utc']:02d}z",
                        "forecast_hour": int(spec["forecast_hour"]),
                        "lat": point["lat"],
                        "lon": point["lon"],
                        "crop_radius_deg": float(payload.get("crop_radius_deg") or 0.5),
                        "include_input_column": bool(payload.get("include_input_column")),
                    "timeout": int(payload.get("timeout_per_run") or 180),
                }
                probed = self._run_ecape_profile(request)
                probed["case_spec"] = spec
                probed["case_point"] = point
                results.append(probed)
        index = out_root / "index.json"
        result = {
            **plan,
            "dry_run": False,
            "out_dir": str(out_root),
            "result_count": len(results),
            "ok_count": sum(1 for item in results if item.get("ok")),
            "failed_count": sum(1 for item in results if not item.get("ok")),
            "results": results,
        }
        index.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        result["index"] = str(index)
        result["ok"] = result["failed_count"] == 0
        return self._attach_previews(result, started, products or ["ecape_profile"])

    def _domains(self, kind: str | None, search: str | None) -> dict:
        result = _load_json(rustwx.list_domains_json(kind=kind, limit=None))
        domains = result.get("domains") or []
        if search:
            needle = search.lower()
            domains = [
                item for item in domains
                if needle in str(item.get("slug", "")).lower()
                or needle in str(item.get("label", "")).lower()
                or any(needle in str(tag).lower() for tag in item.get("tags") or [])
            ]
        return {"ok": True, "count": len(domains), "domains": domains}

    def _render_maps(self, payload: dict) -> dict:
        selected = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        if not selected:
            return {"ok": False, "error": "Select at least one map product."}
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        forecast_hours = _forecast_hours_from_payload(payload)
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=max(forecast_hours),
        )
        domain_slug = str(payload.get("domain") or "conus")
        domain = _domain_by_slug(domain_slug)
        out_dir = Path(payload.get("out_dir")) if payload.get("out_dir") else (
            self.server.env.out_root / "studio" / "maps" / model / time.strftime("%Y%m%d_%H%M%S")
        )
        started = time.time()
        batches = []
        for forecast_hour in forecast_hours:
            hour_dir = out_dir / f"f{forecast_hour:03d}" if len(forecast_hours) > 1 else out_dir
            request = {
                "date_yyyymmdd": date,
                "cycle_utc": cycle,
                "forecast_hour": forecast_hour,
                "model": model,
                "source": source,
                "domain": domain_slug,
                "products": selected,
                "cache_dir": str(self.server.env.cache_dir),
                "out_dir": str(hour_dir),
                "output_width": int(payload.get("width") or DEFAULT_OUTPUT_WIDTH),
                "output_height": int(payload.get("height") or DEFAULT_OUTPUT_HEIGHT),
                "place_label_density": str(payload.get("place_label_density") or "major"),
            }
            for index, group in enumerate(_chunks(selected, int(payload.get("chunk_size") or 8))):
                batch_request = dict(request)
                batch_request["products"] = group
                if len(selected) > len(group):
                    batch_request["out_dir"] = str(hour_dir / f"batch_{index + 1:02d}")
                batch = _load_json(rustwx.render_maps_json(json.dumps(batch_request)))
                batch["ui_forecast_hour"] = forecast_hour
                batches.append(batch)
        result = batches[0] if len(batches) == 1 else {
            "ok": all(batch.get("error") is None for batch in batches),
            "batches": batches,
            "forecast_hours": forecast_hours,
            "batch_count": len(batches),
        }
        result["ui_domain"] = domain_slug
        if domain and domain.get("bounds"):
            result["ui_domain_bounds"] = domain["bounds"]
        return self._attach_previews(result, started, selected)

    def _prepare_model_data(self, payload: dict) -> dict:
        selected = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        if not selected:
            return {"ok": False, "error": "Select at least one map product."}
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        forecast_hours = _forecast_hours_from_payload(payload)
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=max(forecast_hours),
        )
        request = {
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "forecast_hour": int(forecast_hours[0]),
            "forecast_hours": forecast_hours,
            "model": model,
            "source": source,
            "products": selected,
            "cache_dir": str(self.server.env.cache_dir),
            "download_workers": int(payload.get("download_workers") or payload.get("jobs") or 4),
        }
        started = time.time()
        result = _load_json(rustwx.prepare_model_data_json(json.dumps(request)))
        result["stage"] = "prepare_model_data"
        return self._attach_previews(result, started, selected)

    def _render_satellite(self, payload: dict) -> dict:
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        if not products:
            return {"ok": False, "error": "Select at least one satellite product."}
        out_dir = self.server.env.out_root / "studio" / "satellite" / time.strftime("%Y%m%d_%H%M%S")
        request = {
            "satellite": str(payload.get("satellite") or "goes19"),
            "sector": str(payload.get("sector") or "conus"),
            "domain": str(payload.get("domain") or "pacific_southwest"),
            "products": products,
            "cache_dir": str(self.server.env.cache_dir),
            "out_dir": str(out_dir),
            "width": int(payload.get("width") or DEFAULT_OUTPUT_WIDTH),
            "height": int(payload.get("height") or DEFAULT_OUTPUT_HEIGHT),
            "scan_lookback_hours": int(payload.get("scan_lookback_hours") or 3),
            "use_cache": bool(payload.get("use_cache", True)),
            "auto_bounds": bool(payload.get("auto_bounds", False)),
            "allow_high_resolution_full_disk": bool(payload.get("allow_high_resolution_full_disk", False)),
            "high_speed_png": True,
            "sequence_count": int(payload.get("sequence_count") or 1),
            "sequence_gif": bool(payload.get("sequence_gif")),
            "sequence_gif_delay_ms": int(payload.get("sequence_gif_delay_ms") or 180),
        }
        started = time.time()
        result = _load_json(rustwx.render_goes_satellite_json(json.dumps(request)))
        return self._attach_previews(result, started, products)

    def _render_satellite_sequence(self, payload: dict) -> dict:
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        if not products:
            products = ["goes_geocolor"]
        satellite = str(payload.get("satellite") or "goes19")
        sector = str(payload.get("sector") or "conus")
        domain_slug = str(payload.get("domain") or "conus")
        domain = _domain_by_slug(domain_slug)
        if not domain or not domain.get("bounds"):
            return {"ok": False, "error": f"unknown satellite domain {domain_slug!r}"}
        bounds = [float(value) for value in domain["bounds"]]
        out_root = (
            self.server.env.out_root
            / "studio"
            / "satellite_native"
            / satellite
            / time.strftime("%Y%m%d_%H%M%S")
        )
        started = time.time()
        reports = []
        errors = []
        latest_count = max(1, int(payload.get("latest_count") or payload.get("sequence_count") or 4))
        for product in products:
            product_out = out_root / _safe_component(product)
            request = {
                "satellite": satellite,
                "sector": sector,
                "product": product,
                "domain": domain_slug,
                "label": domain.get("label") or domain_slug,
                "bounds": bounds,
                "out_dir": str(product_out),
                "cache_dir": str(self.server.env.cache_dir),
                "latest_count": latest_count,
                "scan_lookback_hours": int(payload.get("scan_lookback_hours") or 3),
                "downsample": float(payload.get("downsample") or 1.0),
                "max_width": int(payload.get("max_width") or payload.get("width") or DEFAULT_OUTPUT_WIDTH),
                "max_height": int(payload.get("max_height") or payload.get("height") or DEFAULT_OUTPUT_HEIGHT),
                "download_workers": int(payload.get("download_workers") or 8),
                "render_workers": int(payload.get("render_workers") or 0),
                "high_speed_png": True,
            }
            if payload.get("min_step_minutes"):
                request["min_step_minutes"] = int(payload["min_step_minutes"])
            try:
                report = _load_json(rustwx.render_goes_native_sequence_json(json.dumps(request)))
                reports.append(report)
            except Exception as exc:
                errors.append({"product": product, "error": str(exc), "request": request})
        result = {
            "ok": bool(reports) and not errors and all(bool(report.get("ok", True)) for report in reports),
            "partial": bool(reports) and bool(errors),
            "stage": "goes_native_sequence",
            "satellite": satellite,
            "sector": sector,
            "domain": domain_slug,
            "bounds": bounds,
            "products": products,
            "latest_count": latest_count,
            "out_root": str(out_root),
            "reports": reports,
            "errors": errors,
            "ui_domain": domain_slug,
            "ui_domain_bounds": bounds,
        }
        return self._attach_previews(result, started, products)

    def _render_satellite_tile_loop(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("goes_web_tiles")
        if binary is None:
            return {
                "ok": False,
                "error": "goes_web_tiles binary not found. Build it with cargo build -p rustwx-cli --release --bin goes_web_tiles or pass --bin-dir.",
            }
        satellite = str(payload.get("satellite") or "goes19")
        sector = str(payload.get("sector") or "conus")
        domain_slug = str(payload.get("domain") or "conus")
        domain = _domain_by_slug(domain_slug)
        if not domain or not domain.get("bounds"):
            return {"ok": False, "stage": "satellite_tile_loop", "error": f"unknown satellite domain {domain_slug!r}"}
        bounds = _coerce_bounds(payload.get("bounds"), domain.get("bounds"))
        west, east, south, north = bounds
        frame_count = max(1, min(24, int(payload.get("latest_count") or payload.get("frame_count") or 3)))
        layer_mode = str(payload.get("layer") or "geocolor").strip().lower()
        if layer_mode not in {"geocolor", "clouds"}:
            return {"ok": False, "stage": "satellite_tile_loop", "error": "satellite tile layer must be geocolor or clouds."}

        started = time.time()
        stamp = time.strftime("%Y%m%d_%H%M%S")
        product = "goes_geocolor"
        source_out = (
            self.server.env.out_root
            / "studio"
            / "satellite_tile_sources"
            / satellite
            / stamp
            / _safe_slug(domain_slug)
            / layer_mode
        )
        source_request = {
            "satellite": satellite,
            "sector": sector,
            "product": product,
            "domain": domain_slug,
            "label": domain.get("label") or domain_slug,
            "bounds": [west, east, south, north],
            "out_dir": str(source_out),
            "cache_dir": str(self.server.env.cache_dir),
            "latest_count": frame_count,
            "scan_lookback_hours": int(payload.get("scan_lookback_hours") or 3),
            "downsample": float(payload.get("downsample") or 4.0),
            "max_width": int(payload.get("max_width") or 512),
            "max_height": int(payload.get("max_height") or 512),
            "download_workers": int(payload.get("download_workers") or 8),
            "render_workers": int(payload.get("render_workers") or 0),
            "high_speed_png": True,
        }
        if payload.get("min_step_minutes"):
            source_request["min_step_minutes"] = int(payload["min_step_minutes"])
        try:
            source_report = _load_json(rustwx.render_goes_native_sequence_json(json.dumps(source_request)))
        except Exception as exc:
            return {
                "ok": False,
                "stage": "satellite_tile_loop",
                "error": f"{type(exc).__name__}: {exc}",
                "source_request": source_request,
                "ui_elapsed_s": round(time.time() - started, 2),
            }

        source_frames = source_report.get("frames") if isinstance(source_report.get("frames"), list) else []
        if not source_frames:
            return {
                "ok": False,
                "stage": "satellite_tile_loop",
                "error": "GOES native sequence did not return any frames to tile.",
                "source_request": source_request,
                "source_report": source_report,
                "ui_elapsed_s": round(time.time() - started, 2),
            }

        satellite_root = self.server.env.cache_dir / "studio_satellite_tiles"
        layer_id = _safe_slug(f"{satellite}_{domain_slug}_{layer_mode}")
        layer_root = satellite_root / layer_id
        layer_root.joinpath("frames").mkdir(parents=True, exist_ok=True)
        tile_size = int(payload.get("tile_size") or 256)
        min_zoom = int(payload.get("min_zoom") or 4)
        max_zoom = int(payload.get("max_zoom") or 6)
        opacity = float(payload.get("opacity") or (0.78 if layer_mode == "clouds" else 0.92))
        compression = str(payload.get("png_compression") or "fast")
        commands = []
        frames = []
        missing_channels = []
        for index, frame in enumerate(source_frames[-frame_count:]):
            channel1 = _satellite_channel_local_path(frame, 1)
            channel2 = _satellite_channel_local_path(frame, 2)
            channel3 = _satellite_channel_local_path(frame, 3)
            if not (channel1 and channel2 and channel3):
                missing_channels.append({
                    "scan_time_utc": frame.get("scan_time_utc"),
                    "available_channels": sorted((frame.get("channel_files") or {}).keys()),
                })
                continue
            channel13 = _satellite_channel_local_path(frame, 13)
            frame_id = _satellite_frame_id(frame, fallback_index=index)
            frame_dir = layer_root / "frames" / frame_id
            frame_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = frame_dir / "tiles_manifest.json"
            run = {
                "ok": True,
                "cache_hit": True,
                "elapsed_s": 0.0,
                "command": [],
            } if manifest_path.exists() and not payload.get("force") else None
            if run is None:
                cmd = [
                    str(binary),
                    "--channel1",
                    str(channel1),
                    "--channel2",
                    str(channel2),
                    "--channel3",
                    str(channel3),
                    "--out-dir",
                    str(frame_dir),
                    "--name",
                    layer_id,
                    f"--west={west}",
                    f"--east={east}",
                    f"--south={south}",
                    f"--north={north}",
                    "--min-zoom",
                    str(min_zoom),
                    "--max-zoom",
                    str(max_zoom),
                    "--tile-size",
                    str(tile_size),
                    "--opacity",
                    str(opacity),
                    "--layer",
                    layer_mode,
                    "--png-compression",
                    compression,
                ]
                if channel13:
                    cmd.extend(["--channel13", str(channel13)])
                if payload.get("opaque_clouds") or layer_mode == "clouds":
                    cmd.append("--opaque-clouds")
                run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 900))
            commands.append(run)
            manifest = _json_file_payload(manifest_path)
            if not manifest and run.get("json"):
                manifest = run["json"]
            if not run.get("ok") or not manifest:
                return {
                    "ok": False,
                    "stage": "satellite_tile_loop",
                    "error": _command_error(run) or f"goes_web_tiles did not produce a manifest for {frame.get('scan_time_utc') or frame_id}.",
                    "source_report": source_report,
                    "commands": commands,
                    "missing_channels": missing_channels,
                    "frames": frames,
                    "ui_elapsed_s": round(time.time() - started, 2),
                }
            frames.append(_satellite_loop_frame_record(layer_id, frame_id, manifest, source_frame=frame, layer_mode=layer_mode))

        frames.sort(key=lambda item: str(item.get("scan_time_utc") or item.get("id") or ""))
        if not frames:
            return {
                "ok": False,
                "stage": "satellite_tile_loop",
                "error": "No tileable GOES frames had channels 1, 2, and 3 available.",
                "source_report": source_report,
                "missing_channels": missing_channels,
                "ui_elapsed_s": round(time.time() - started, 2),
            }
        frames_json = {
            "ok": True,
            "schema": "wxstore.satellite.frames.v1",
            "layer": layer_id,
            "satellite": satellite,
            "sector": sector,
            "product": product,
            "tile_layer": layer_mode,
            "domain": domain_slug,
            "bounds": [west, south, east, north],
            "delay_ms": int(payload.get("loop_delay_ms") or 700),
            "frames": frames,
        }
        frames_path = layer_root / "frames.json"
        frames_path.write_text(json.dumps(frames_json, indent=2, default=str), encoding="utf-8")
        viewer_path = layer_root / "loop_viewer.html"
        _write_satellite_tile_loop_viewer(viewer_path, frames_json, satellite_root)
        artifact = {
            "path": str(viewer_path),
            "name": viewer_path.name,
            "url": f"/api/file?path={quote(str(viewer_path.resolve()))}",
        }
        png_count = len([path for path in layer_root.rglob("*.png") if path.is_file()])
        return {
            "ok": True,
            "stage": "satellite_tile_loop",
            "satellite": satellite,
            "sector": sector,
            "product": product,
            "tile_layer": layer_mode,
            "domain": domain_slug,
            "layer_id": layer_id,
            "satellite_tiles_root": str(satellite_root),
            "layer_root": str(layer_root),
            "frames_path": str(frames_path),
            "viewer_html": str(viewer_path),
            "viewer_url": artifact["url"],
            "frame_count": len(frames),
            "png_count": png_count,
            "frames": frames,
            "missing_channels": missing_channels,
            "source_report": source_report,
            "source_request": source_request,
            "commands": commands,
            "html_artifacts": [artifact],
            "previews": [],
            "requested_products": [f"goes_{layer_mode}_tiles"],
            "ui_elapsed_s": round(time.time() - started, 2),
        }

    def _render_radar(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("radar_export")
        if binary is None:
            return {
                "ok": False,
                "error": "radar_export binary not found. Build it with cargo build -p rustwx-cli --release --bin radar_export or pass --bin-dir.",
            }
        site = str(payload.get("site") or "").strip().upper()
        lat = str(payload.get("lat") or "").strip()
        lon = str(payload.get("lon") or "").strip()
        if not site and not (lat and lon):
            return {"ok": False, "error": "Provide a radar site or both lat/lon."}
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        requested = ",".join(products) if products else "ref"
        out_dir = self.server.env.out_root / "studio" / "radar" / (site or "point") / time.strftime("%Y%m%d_%H%M%S")
        out_dir.mkdir(parents=True, exist_ok=True)
        multi = requested.lower() == "all" or "," in requested
        png_target = out_dir if multi else out_dir / f"radar_{requested.lower()}.png"
        json_target = out_dir / "radar.json"
        cmd = [str(binary)]
        if site:
            cmd.extend(["--site", site])
        else:
            cmd.extend(["--lat", lat, "--lon", lon])
        cmd.extend(["--products" if multi else "--product", requested])
        cmd.extend(["--png", str(png_target), "--json", str(json_target)])
        cmd.extend(["--size", str(int(payload.get("size") or 1024))])
        if payload.get("dealias"):
            cmd.append("--dealias")
        if payload.get("render_mode"):
            cmd.extend(["--render-mode", str(payload["render_mode"])])
        started = time.time()
        proc = subprocess.run(
            cmd,
            env=self.server.env.subprocess_env(),
            capture_output=True,
            text=True,
            timeout=int(payload.get("timeout") or 300),
        )
        result = {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "elapsed_s": round(time.time() - started, 2),
            "stdout_tail": proc.stdout.splitlines()[-40:],
            "stderr_tail": proc.stderr.splitlines()[-40:],
            "command": cmd,
        }
        if json_target.exists():
            try:
                result["radar"] = json.loads(json_target.read_text(encoding="utf-8"))
            except Exception:
                result["radar_json_path"] = str(json_target)
        return self._attach_previews(result, started, products or [requested])

    def _render_radar_tiles(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("radar_web_tiles")
        if binary is None:
            return {
                "ok": False,
                "error": "radar_web_tiles binary not found. Build it with cargo build -p rustwx-cli --release --bin radar_web_tiles or pass --bin-dir.",
            }
        site = str(payload.get("site") or "").upper().strip()
        lat = str(payload.get("lat") or "").strip()
        lon = str(payload.get("lon") or "").strip()
        if not site and not (lat and lon):
            return {"ok": False, "error": "Provide a radar site or both lat/lon."}
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        product = str(payload.get("product") or (products[0] if products else "ref")).strip().lower()
        if product == "all":
            product = "ref"
        domain_slug = str(payload.get("domain") or "oklahoma")
        domain = _domain_by_slug(domain_slug)
        bounds = _coerce_bounds(payload.get("bounds"), domain.get("bounds") if domain else [-100.5, -95.0, 33.5, 37.5])
        stamp = time.strftime("%Y%m%d_%H%M%S")
        label = _safe_slug(site or f"{lat}_{lon}")
        out_dir = self.server.env.out_root / "studio" / "radar_tiles" / label / stamp / _safe_slug(product)
        out_dir.mkdir(parents=True, exist_ok=True)
        west, east, south, north = bounds
        cmd = [
            str(binary),
            "--out-dir",
            str(out_dir),
            "--cache-dir",
            str(self.server.env.cache_dir / "radar"),
            "--product",
            product,
            f"--west={west}",
            f"--east={east}",
            f"--south={south}",
            f"--north={north}",
            "--min-zoom",
            str(int(payload.get("min_zoom") or 5)),
            "--max-zoom",
            str(int(payload.get("max_zoom") or 7)),
            "--tile-size",
            str(int(payload.get("tile_size") or 256)),
            "--opacity",
            str(float(payload.get("opacity") or 1.0)),
            "--color-table",
            str(payload.get("color_table") or "default"),
            "--supersample",
            str(int(payload.get("supersample") or 1)),
            "--png-compression",
            str(payload.get("png_compression") or "fast"),
        ]
        if site:
            cmd.extend(["--site", site])
        else:
            cmd.extend(["--lat", lat, "--lon", lon])
        if payload.get("input"):
            cmd.extend(["--input", str(payload["input"])])
        if payload.get("all_tilts"):
            cmd.append("--all-tilts")
        if payload.get("clip_to_bounds"):
            cmd.append("--clip-to-bounds")
        if payload.get("dealias"):
            cmd.append("--dealias")
            cmd.extend(["--dealias-method", str(payload.get("dealias_method") or "sweep")])
        if payload.get("velocity_quality_filter"):
            cmd.append("--velocity-quality-filter")
        if payload.get("reflectivity_despeckle"):
            cmd.append("--reflectivity-despeckle")
            if payload.get("reflectivity_despeckle_min_neighbors") not in (None, ""):
                cmd.extend([
                    "--reflectivity-despeckle-min-neighbors",
                    str(int(payload.get("reflectivity_despeckle_min_neighbors") or 2)),
                ])
        if payload.get("keep_empty_tiles"):
            cmd.append("--keep-empty-tiles")
        started = time.time()
        run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 300))
        manifest_path = out_dir / ("all_tilts_manifest.json" if payload.get("all_tilts") else "tiles_manifest.json")
        manifest = _json_file_payload(manifest_path)
        if not manifest and run.get("json"):
            manifest = run["json"]
        viewer_path = out_dir / "viewer.html"
        html_artifacts = []
        if manifest:
            _write_radar_tile_viewer(viewer_path, manifest, out_dir)
            html_artifacts.append({
                "path": str(viewer_path),
                "name": viewer_path.name,
                "url": f"/api/file?path={quote(str(viewer_path.resolve()))}",
            })
        png_count = len([path for path in out_dir.rglob("*.png") if path.is_file()])
        result = {
            "ok": bool(run["ok"]) and bool(manifest),
            "stage": "radar_tiles",
            "site": site or None,
            "lat": lat or None,
            "lon": lon or None,
            "product": product,
            "domain": domain_slug,
            "bounds": [west, east, south, north],
            "out_dir": str(out_dir),
            "manifest_path": str(manifest_path) if manifest_path.exists() else None,
            "viewer_html": str(viewer_path) if viewer_path.exists() else None,
            "viewer_url": html_artifacts[0]["url"] if html_artifacts else None,
            "png_count": png_count,
            "manifest": _radar_tile_manifest_summary(manifest),
            "command": run,
            "html_artifacts": html_artifacts,
            "previews": [],
            "requested_products": [product],
            "ui_elapsed_s": round(time.time() - started, 2),
        }
        if not result["ok"]:
            result["error"] = _command_error(run) or "radar_web_tiles did not produce a tile manifest."
        return result

    def _render_radar_tile_loop(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("radar_web_tiles")
        if binary is None:
            return {
                "ok": False,
                "error": "radar_web_tiles binary not found. Build it with cargo build -p rustwx-cli --release --bin radar_web_tiles or pass --bin-dir.",
            }
        site = str(payload.get("site") or "").upper().strip()
        if not site:
            return {"ok": False, "error": "Radar loops need an explicit NEXRAD site."}
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        product = str(payload.get("product") or (products[0] if products else "ref")).strip().lower()
        if product == "all":
            product = "ref"
        frame_count = max(1, min(24, int(payload.get("latest_count") or payload.get("frame_count") or 4)))
        domain_slug = str(payload.get("domain") or "oklahoma")
        domain = _domain_by_slug(domain_slug)
        bounds = _coerce_bounds(payload.get("bounds"), domain.get("bounds") if domain else [-100.5, -95.0, 33.5, 37.5])
        west, east, south, north = bounds
        radar_root = self.server.env.cache_dir / "studio_radar_tiles"
        layer_id = f"nexrad_level2_{site.lower()}_{_safe_slug(product)}"
        layer_root = radar_root / layer_id
        layer_root.joinpath("frames").mkdir(parents=True, exist_ok=True)
        started = time.time()
        try:
            objects = _radar_latest_objects(site, frame_count)
        except Exception as exc:
            return {"ok": False, "stage": "radar_tile_loop", "error": f"Could not list latest {site} Level-II files: {exc}"}
        if not objects:
            return {"ok": False, "stage": "radar_tile_loop", "error": f"No recent public Level-II files found for {site}."}
        frames = []
        commands = []
        downloads = []
        for item in objects[-frame_count:]:
            frame_id, scan_iso = _radar_frame_id(item)
            frame_dir = layer_root / "frames" / frame_id
            frame_dir.mkdir(parents=True, exist_ok=True)
            volume_path, cache_hit = _download_radar_object_to_cache(
                self.server.env.cache_dir / "radar",
                item,
                timeout=float(payload.get("download_timeout") or 90.0),
            )
            downloads.append({
                "key": item.get("key"),
                "display_name": item.get("display_name"),
                "cache_hit": cache_hit,
                "path": str(volume_path),
                "bytes": item.get("size"),
            })
            cmd = [
                str(binary),
                "--site",
                site,
                "--input",
                str(volume_path),
                "--out-dir",
                str(frame_dir),
                "--cache-dir",
                str(self.server.env.cache_dir / "radar"),
                "--product",
                product,
                f"--west={west}",
                f"--east={east}",
                f"--south={south}",
                f"--north={north}",
                "--min-zoom",
                str(int(payload.get("min_zoom") or 5)),
                "--max-zoom",
                str(int(payload.get("max_zoom") or 7)),
                "--tile-size",
                str(int(payload.get("tile_size") or 256)),
                "--opacity",
                str(float(payload.get("opacity") or 1.0)),
                "--color-table",
                str(payload.get("color_table") or "default"),
                "--supersample",
                str(int(payload.get("supersample") or 1)),
                "--png-compression",
                str(payload.get("png_compression") or "fast"),
            ]
            if payload.get("all_tilts"):
                cmd.append("--all-tilts")
            if payload.get("clip_to_bounds"):
                cmd.append("--clip-to-bounds")
            if payload.get("dealias"):
                cmd.append("--dealias")
                cmd.extend(["--dealias-method", str(payload.get("dealias_method") or "sweep")])
            if payload.get("velocity_quality_filter"):
                cmd.append("--velocity-quality-filter")
            if payload.get("reflectivity_despeckle"):
                cmd.append("--reflectivity-despeckle")
            if payload.get("keep_empty_tiles"):
                cmd.append("--keep-empty-tiles")
            run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 300))
            commands.append(run)
            manifest_path = frame_dir / ("all_tilts_manifest.json" if payload.get("all_tilts") else "tiles_manifest.json")
            manifest = _json_file_payload(manifest_path)
            if not manifest and run.get("json"):
                manifest = run["json"]
            if not run.get("ok") or not manifest:
                return {
                    "ok": False,
                    "stage": "radar_tile_loop",
                    "error": _command_error(run) or f"radar_web_tiles did not produce a manifest for {item.get('display_name')}.",
                    "command": run,
                    "downloads": downloads,
                    "frames": frames,
                }
            frames.append(_radar_loop_frame_record(layer_id, frame_id, scan_iso, manifest))
        frames.sort(key=lambda frame: str(frame.get("scan_time_utc") or frame.get("id") or ""))
        frames_json = {
            "ok": True,
            "schema": "wxstore.radar.frames.v1",
            "layer": layer_id,
            "site": site,
            "product": product,
            "domain": domain_slug,
            "bounds": [west, south, east, north],
            "delay_ms": int(payload.get("loop_delay_ms") or 650),
            "frames": frames,
        }
        frames_path = layer_root / "frames.json"
        frames_path.write_text(json.dumps(frames_json, indent=2), encoding="utf-8")
        viewer_path = layer_root / "loop_viewer.html"
        _write_radar_tile_loop_viewer(viewer_path, frames_json, radar_root)
        artifact = {
            "path": str(viewer_path),
            "name": viewer_path.name,
            "url": f"/api/file?path={quote(str(viewer_path.resolve()))}",
        }
        png_count = len([path for path in layer_root.rglob("*.png") if path.is_file()])
        return {
            "ok": True,
            "stage": "radar_tile_loop",
            "site": site,
            "product": product,
            "domain": domain_slug,
            "layer_id": layer_id,
            "radar_tiles_root": str(radar_root),
            "layer_root": str(layer_root),
            "frames_path": str(frames_path),
            "viewer_html": str(viewer_path),
            "viewer_url": artifact["url"],
            "frame_count": len(frames),
            "png_count": png_count,
            "frames": frames,
            "downloads": downloads,
            "commands": commands,
            "html_artifacts": [artifact],
            "previews": [],
            "requested_products": [product],
            "ui_elapsed_s": round(time.time() - started, 2),
        }

    def _sample_meteogram(self, payload: dict) -> dict:
        try:
            lat = float(payload.get("lat"))
            lon = float(payload.get("lon"))
        except (TypeError, ValueError):
            return {"ok": False, "error": "Lat and lon must be numeric."}
        store_id = str(payload.get("store_id") or "").strip()
        forecast_hours = _forecast_hours_from_payload({
            "forecast_hours": payload.get("forecast_hours"),
            "forecast_hour": payload.get("forecast_hour_start") or 0,
        })
        if payload.get("forecast_hour_end") not in (None, "") and not payload.get("forecast_hours"):
            start = int(payload.get("forecast_hour_start") or 0)
            end = int(payload.get("forecast_hour_end") or start)
            if end < start:
                return {"ok": False, "error": "forecast_hour_end must be >= forecast_hour_start."}
            forecast_hours = list(range(start, end + 1))
        method = str(payload.get("method") or "nearest")
        if store_id:
            request = {
                "store_id": store_id,
                "lat": lat,
                "lon": lon,
                "method": method,
                "forecast_hours": forecast_hours,
            }
            started = time.time()
            try:
                result = _load_json(rustwx.sample_point_timeseries_store_json(json.dumps(request)))
            except Exception as exc:
                return {
                    "ok": False,
                    "stage": "sample_meteogram_store",
                    "error": f"{type(exc).__name__}: {exc}",
                    "request": request,
                    "ui_elapsed_s": round(time.time() - started, 2),
                }
            result["ok"] = "error" not in result
            result["stage"] = "sample_meteogram_store"
            result["store_id"] = store_id
            result["request"] = request
            result["ui_elapsed_s"] = round(time.time() - started, 2)
            return result
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "nomads")
        forecast_hour_end = max(forecast_hours)
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=forecast_hour_end,
        )
        request = {
            "model": model,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "source": source,
            "lat": lat,
            "lon": lon,
            "forecast_hours": forecast_hours,
            "method": method,
            "cache_dir": str(self.server.env.cache_dir),
        }
        variables = _csv_items(payload.get("variables"))
        if variables:
            request["variables"] = variables
        started = time.time()
        try:
            result = _load_json(rustwx.sample_point_timeseries_json(json.dumps(request)))
        except Exception as exc:
            return {
                "ok": False,
                "stage": "sample_meteogram",
                "error": f"{type(exc).__name__}: {exc}",
                "request": request,
                "ui_elapsed_s": round(time.time() - started, 2),
            }
        result["ok"] = "error" not in result
        result["stage"] = "sample_meteogram"
        result["request"] = request
        result["ui_elapsed_s"] = round(time.time() - started, 2)
        return result

    def _warm_meteogram_store(self, payload: dict) -> dict:
        if not hasattr(rustwx, "warm_point_timeseries_store_json"):
            return {
                "ok": False,
                "error": "installed rustwx does not expose warm_point_timeseries_store_json; rebuild or reinstall the rustwx Python wheel.",
            }
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "nomads")
        forecast_hours = _forecast_hours_from_payload(payload)
        if payload.get("forecast_hour_end") not in (None, "") and not payload.get("forecast_hours"):
            start = int(payload.get("forecast_hour_start") or 0)
            end = int(payload.get("forecast_hour_end") or start)
            if end < start:
                return {"ok": False, "error": "forecast_hour_end must be >= forecast_hour_start."}
            forecast_hours = list(range(start, end + 1))
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=max(forecast_hours),
        )
        domain_slug = str(payload.get("domain") or "conus")
        domain = _domain_by_slug(domain_slug)
        bounds = _coerce_bounds(payload.get("bounds"), domain.get("bounds") if domain else RADAR_BASEMAP_BOUNDS)
        request = {
            "model": model,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "source": source,
            "forecast_hours": forecast_hours,
            "bounds": list(bounds),
            "cache_dir": str(self.server.env.cache_dir),
            "use_cache": True,
        }
        variables = _csv_items(payload.get("variables"))
        if variables:
            request["variables"] = variables
        started = time.time()
        try:
            report = _load_json(rustwx.warm_point_timeseries_store_json(json.dumps(request)))
        except Exception as exc:
            return {
                "ok": False,
                "stage": "warm_meteogram_store",
                "error": f"{type(exc).__name__}: {exc}",
                "request": request,
                "ui_elapsed_s": round(time.time() - started, 2),
            }
        return {
            "ok": "error" not in report,
            "stage": "warm_meteogram_store",
            "store_id": report.get("store_id"),
            "model": model,
            "source": source,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "domain": domain_slug,
            "bounds": list(bounds),
            "forecast_hours": forecast_hours,
            "variables": variables,
            "request": request,
            "report": report,
            "ui_elapsed_s": round(time.time() - started, 2),
        }

    def _prepare_pressure_store(self, payload: dict) -> dict:
        started = time.time()
        context = self._pressure_store_context(payload)
        result = self._ensure_pressure_store(
            context,
            payload,
            force=bool(payload.get("force")),
            timeout=int(payload.get("timeout") or payload.get("store_timeout") or 1200),
        )
        return self._attach_previews(result, started, ["pressure_volume_store"])

    def _prepare_wxprofile_store(self, payload: dict) -> dict:
        started = time.time()
        context = self._wxprofile_store_context(payload)
        result = self._ensure_wxprofile_store(
            context,
            payload,
            force=bool(payload.get("force") or payload.get("force_store")),
            timeout=int(payload.get("timeout") or payload.get("store_timeout") or 1200),
        )
        result["ui_elapsed_s"] = round(time.time() - started, 2)
        return result

    def _pressure_store_context(self, payload: dict, *, route: dict | None = None) -> dict:
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        forecast_hour = int(payload.get("forecast_hour") or payload.get("hour") or 0)
        store_hours = _forecast_hours_from_payload({"forecast_hours": payload.get("hours"), "forecast_hour": forecast_hour})
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=max(store_hours),
        )

        if route:
            domain_slug = str(route["id"])
            bounds = _route_bounds(route)
        else:
            domain_slug = str(payload.get("domain") or "conus")
            domain = _domain_by_slug(domain_slug)
            fallback_bounds = domain.get("bounds") if domain else [-125.0, -66.0, 24.0, 50.0]
            bounds = _coerce_bounds(payload.get("bounds"), fallback_bounds)

        center_lat = (float(bounds[2]) + float(bounds[3])) / 2.0
        center_lon = (float(bounds[0]) + float(bounds[1])) / 2.0
        try:
            sample_lat = float(payload.get("lat"))
            sample_lon = float(payload.get("lon"))
        except (TypeError, ValueError):
            sample_lat = center_lat
            sample_lon = center_lon
        if not _point_in_bounds(sample_lat, sample_lon, bounds):
            sample_lat = center_lat
            sample_lon = center_lon

        hours = str(payload.get("hours") or forecast_hour)
        key = {
            "model": model,
            "source": source,
            "date": date,
            "cycle": cycle,
            "hours": hours,
            "domain": domain_slug,
            "bounds": [round(float(value), 4) for value in bounds],
        }
        store_root = (
            self.server.env.cache_dir
            / "studio_pressure_stores"
            / _safe_slug(model)
            / f"{date}_{cycle:02d}z"
            / f"{_safe_slug(domain_slug)}_{_safe_slug(hours)}_{_short_hash(key)}"
        )
        return {
            "model": model,
            "source": source,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "forecast_hour": forecast_hour,
            "hours": hours,
            "domain": domain_slug,
            "bounds": [float(value) for value in bounds],
            "sample_lat": sample_lat,
            "sample_lon": sample_lon,
            "route": route,
            "store_root": store_root,
            "store_path": store_root / "store",
        }

    def _wxprofile_store_context(self, payload: dict, *, route: dict | None = None) -> dict:
        context = self._pressure_store_context(payload, route=route)
        key = {
            "model": context["model"],
            "source": context["source"],
            "date": context["date_yyyymmdd"],
            "cycle": context["cycle_utc"],
            "hours": context["hours"],
            "domain": context["domain"],
            "bounds": [round(float(value), 4) for value in context["bounds"]],
            "format": "wxprofile-v0",
        }
        store_root = (
            self.server.env.cache_dir
            / "studio_wxprofile_stores"
            / _safe_slug(context["model"])
            / f"{context['date_yyyymmdd']}_{int(context['cycle_utc']):02d}z"
            / f"{_safe_slug(context['domain'])}_{_safe_slug(context['hours'])}_{_short_hash(key)}"
        )
        return {
            **context,
            "store_root": store_root,
            "store_path": store_root / "store",
        }

    def _ensure_pressure_store(
        self,
        context: dict,
        payload: dict,
        *,
        force: bool = False,
        timeout: int = 1200,
    ) -> dict:
        binary = self.server.env.binaries.get("hrrr_pressure_volume_store")
        store_path = Path(context["store_path"])
        if binary is None:
            return {
                "ok": False,
                "stage": "build_pressure_store",
                "error": "hrrr_pressure_volume_store binary not found. Build it with cargo build -p rustwx-cli --release --bin hrrr_pressure_volume_store or pass --bin-dir.",
                "store_root": str(context["store_root"]),
                "store_path": str(store_path),
            }

        with self.server.pressure_store_lock:
            if _pressure_store_complete(store_path) and not force:
                return {
                    "ok": True,
                    "stage": "pressure_store_ready",
                    "cache_hit": True,
                    "model": context["model"],
                    "source": context["source"],
                    "date_yyyymmdd": context["date_yyyymmdd"],
                    "cycle_utc": context["cycle_utc"],
                    "hours": context["hours"],
                    "domain": context["domain"],
                    "bounds": context["bounds"],
                    "store_root": str(context["store_root"]),
                    "store_path": str(store_path),
                }

            route = context.get("route") or _store_route_for_bounds(
                context["bounds"],
                float(context["sample_lat"]),
                float(context["sample_lon"]),
            )
            context["store_root"].mkdir(parents=True, exist_ok=True)
            cmd = [
                str(binary),
                "--model",
                context["model"],
                "--domain",
                context["domain"],
                "--date",
                context["date_yyyymmdd"],
                "--cycle",
                str(context["cycle_utc"]),
                "--hours",
                str(context["hours"]),
                "--source",
                context["source"],
                f"--west={context['bounds'][0]}",
                f"--east={context['bounds'][1]}",
                f"--south={context['bounds'][2]}",
                f"--north={context['bounds'][3]}",
                "--cache-dir",
                str(self.server.env.cache_dir),
                "--out-dir",
                str(context["store_root"]),
                f"--sample-lat={float(context['sample_lat'])}",
                f"--sample-lon={float(context['sample_lon'])}",
                f"--route-start-lat={route['start'][0]}",
                f"--route-start-lon={route['start'][1]}",
                f"--route-end-lat={route['end'][0]}",
                f"--route-end-lon={route['end'][1]}",
                "--route-spacing-km",
                str(float(payload.get("spacing_km") or payload.get("route_spacing_km") or 10.0)),
                "--chunk-t",
                str(int(payload.get("chunk_t") or 1)),
                "--chunk-z",
                str(int(payload.get("chunk_z") or 4)),
                "--chunk-y",
                str(int(payload.get("chunk_y") or 64)),
                "--chunk-x",
                str(int(payload.get("chunk_x") or 64)),
            ]
            if payload.get("load_parallelism"):
                cmd.extend(["--load-parallelism", str(int(payload["load_parallelism"]))])
            if payload.get("no_cache"):
                cmd.append("--no-cache")

            started = time.time()
            command_result = _run_command(
                cmd,
                self.server.env.subprocess_env(),
                timeout=timeout,
            )
            report_path = Path(context["store_root"]) / "report.json"
            report = None
            if report_path.exists():
                try:
                    report = json.loads(report_path.read_text(encoding="utf-8"))
                except Exception:
                    report = None
            elif command_result.get("json"):
                report = command_result["json"]
            return {
                "ok": bool(command_result["ok"]) and _pressure_store_complete(store_path),
                "stage": "build_pressure_store",
                "cache_hit": False,
                "model": context["model"],
                "source": context["source"],
                "date_yyyymmdd": context["date_yyyymmdd"],
                "cycle_utc": context["cycle_utc"],
                "hours": context["hours"],
                "domain": context["domain"],
                "bounds": context["bounds"],
                "store_root": str(context["store_root"]),
                "store_path": str(store_path),
                "report_path": str(report_path),
                "report": report,
                "build": command_result,
                "elapsed_s": round(time.time() - started, 2),
            }

    def _ensure_wxprofile_store(
        self,
        context: dict,
        payload: dict,
        *,
        force: bool = False,
        timeout: int = 1200,
    ) -> dict:
        binary = _wxprofile_store_binary(self.server.env.binaries)
        store_path = Path(context["store_path"])
        if binary is None:
            return {
                "ok": False,
                "stage": "build_wxprofile_store",
                "error": "model_wxprofile_store binary not found. Build it with cargo build -p rustwx-cli --release --bin model_wxprofile_store or pass --bin-dir.",
                "store_root": str(context["store_root"]),
                "store_path": str(store_path),
            }

        with self.server.pressure_store_lock:
            if _wxprofile_store_complete(store_path) and not force:
                return {
                    "ok": True,
                    "stage": "wxprofile_store_ready",
                    "cache_hit": True,
                    "model": context["model"],
                    "source": context["source"],
                    "date_yyyymmdd": context["date_yyyymmdd"],
                    "cycle_utc": context["cycle_utc"],
                    "hours": context["hours"],
                    "domain": context["domain"],
                    "bounds": context["bounds"],
                    "store_root": str(context["store_root"]),
                    "store_path": str(store_path),
                }

            context["store_root"].mkdir(parents=True, exist_ok=True)
            cmd = [
                str(binary),
                "--model",
                context["model"],
                "--domain",
                context["domain"],
                "--date",
                context["date_yyyymmdd"],
                "--cycle",
                str(context["cycle_utc"]),
                "--hours",
                str(context["hours"]),
                "--source",
                context["source"],
                f"--west={context['bounds'][0]}",
                f"--east={context['bounds'][1]}",
                f"--south={context['bounds'][2]}",
                f"--north={context['bounds'][3]}",
                "--cache-dir",
                str(self.server.env.cache_dir),
                "--out-dir",
                str(context["store_root"]),
                "--chunk-x",
                str(int(payload.get("wxprofile_chunk_x") or payload.get("chunk_x") or 4096)),
                "--chunk-y",
                str(int(payload.get("wxprofile_chunk_y") or payload.get("chunk_y") or 4)),
            ]
            if payload.get("no_cache"):
                cmd.append("--no-cache")

            started = time.time()
            command_result = _run_command(
                cmd,
                self.server.env.subprocess_env(),
                timeout=timeout,
            )
            report_path = Path(context["store_root"]) / "report.json"
            report = None
            if report_path.exists():
                try:
                    report = json.loads(report_path.read_text(encoding="utf-8"))
                except Exception:
                    report = None
            elif command_result.get("json"):
                report = command_result["json"]
            return {
                "ok": bool(command_result["ok"]) and _wxprofile_store_complete(store_path),
                "stage": "build_wxprofile_store",
                "cache_hit": False,
                "model": context["model"],
                "source": context["source"],
                "date_yyyymmdd": context["date_yyyymmdd"],
                "cycle_utc": context["cycle_utc"],
                "hours": context["hours"],
                "domain": context["domain"],
                "bounds": context["bounds"],
                "store_root": str(context["store_root"]),
                "store_path": str(store_path),
                "report_path": str(report_path),
                "report": report,
                "build": command_result,
                "elapsed_s": round(time.time() - started, 2),
            }

    def _render_sounding(self, payload: dict) -> dict:
        try:
            lat = float(payload.get("lat"))
            lon = float(payload.get("lon"))
        except (TypeError, ValueError):
            return {"ok": False, "error": "Lat and lon must be numeric."}
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        forecast_hour = int(payload.get("forecast_hour") or 0)
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=forecast_hour,
        )
        started = time.time()
        sample_method = str(payload.get("sample_method") or "nearest")
        data_mode = str(payload.get("data_mode") or "auto").lower().replace("_", "-")
        store_result = None
        wxprofile_store_result = None
        wxprofile_binary = self.server.env.binaries.get("wxprofile_sounding_render")
        can_try_wxprofile = (
            data_mode != "grib"
            and wxprofile_binary is not None
            and _has_wxprofile_store_binary(self.server.env.binaries)
            and sample_method in {"nearest", "box-mean"}
            and 0 <= forecast_hour <= 255
        )
        if can_try_wxprofile:
            context = self._wxprofile_store_context(
                {
                    **payload,
                    "model": model,
                    "source": source,
                    "run_str": f"{date}/{cycle:02d}",
                    "forecast_hour": forecast_hour,
                    "lat": lat,
                    "lon": lon,
                }
            )
            wxprofile_store_result = self._ensure_wxprofile_store(
                context,
                payload,
                force=bool(payload.get("force_store")),
                timeout=int(payload.get("store_timeout") or 1200),
            )
            if wxprofile_store_result.get("ok"):
                stamp = time.strftime("%Y%m%d_%H%M%S")
                out_dir = self.server.env.out_root / "studio" / "soundings" / model / stamp
                out_dir.mkdir(parents=True, exist_ok=True)
                output = out_dir / f"rustwx_{model}_{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}_sounding.png"
                manifest = out_dir / "sounding_manifest.json"
                wxprofile_method = "box-mean" if sample_method == "box-mean" else "nearest"
                cmd = [
                    str(wxprofile_binary),
                    "--store",
                    str(context["store_path"]),
                    "--out-dir",
                    str(out_dir),
                    "--hour",
                    str(forecast_hour),
                    "--lat",
                    str(lat),
                    "--lon",
                    str(lon),
                    "--sample-method",
                    wxprofile_method,
                    "--output",
                    str(output),
                    "--manifest",
                    str(manifest),
                ]
                station_id = str(payload.get("station_id") or "").strip()
                if station_id:
                    cmd.extend(["--station-id", station_id])
                if wxprofile_method == "box-mean":
                    lat_deg, lon_deg = _box_radius_degrees_from_payload(payload, lat)
                    cmd.extend(["--box-radius-lat-deg", str(lat_deg), "--box-radius-lon-deg", str(lon_deg)])
                else:
                    lat_deg = lon_deg = None
                if payload.get("include_column"):
                    cmd.append("--include-column")
                proc_result = _run_command(
                    cmd,
                    self.server.env.subprocess_env(),
                    timeout=int(payload.get("timeout") or 420),
                )
                result = {
                    **proc_result,
                    "backend": "wxprofile",
                    "model": model,
                    "source": "wxprofile",
                    "date_yyyymmdd": date,
                    "cycle_utc": cycle,
                    "forecast_hour": forecast_hour,
                    "lat": lat,
                    "lon": lon,
                    "sample_method_requested": sample_method,
                    "sample_method_used": wxprofile_method,
                    "box_width_km": payload.get("box_width_km"),
                    "box_height_km": payload.get("box_height_km"),
                    "box_bounds": payload.get("box_bounds"),
                    "box_radius_lat_deg": lat_deg,
                    "box_radius_lon_deg": lon_deg,
                    "out_dir": str(out_dir),
                    "wxprofile_store": wxprofile_store_result,
                }
                if manifest.exists():
                    try:
                        result["sounding"] = json.loads(manifest.read_text(encoding="utf-8"))
                    except Exception:
                        result["sounding_manifest"] = str(manifest)
                elif proc_result.get("json"):
                    result["sounding"] = proc_result["json"]
                return self._attach_previews(result, started, ["sounding"])

        volume_binary = self.server.env.binaries.get("volume_store_sounding_render")
        can_try_store = (
            data_mode != "grib"
            and volume_binary is not None
            and self.server.env.binaries.get("hrrr_pressure_volume_store") is not None
            and sample_method in {"nearest", "box-mean"}
            and 0 <= forecast_hour <= 255
        )
        if can_try_store:
            context = self._pressure_store_context(
                {
                    **payload,
                    "model": model,
                    "source": source,
                    "run_str": f"{date}/{cycle:02d}",
                    "forecast_hour": forecast_hour,
                    "lat": lat,
                    "lon": lon,
                }
            )
            store_result = self._ensure_pressure_store(
                context,
                payload,
                force=bool(payload.get("force_store")),
                timeout=int(payload.get("store_timeout") or 1200),
            )
            if store_result.get("ok"):
                stamp = time.strftime("%Y%m%d_%H%M%S")
                out_dir = self.server.env.out_root / "studio" / "soundings" / model / stamp
                out_dir.mkdir(parents=True, exist_ok=True)
                output = out_dir / f"rustwx_{model}_{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}_sounding.png"
                manifest = out_dir / "sounding_manifest.json"
                volume_method = "box-mean" if sample_method == "box-mean" else "nearest"
                cmd = [
                    str(volume_binary),
                    "--store",
                    str(context["store_path"]),
                    "--out-dir",
                    str(out_dir),
                    "--hour",
                    str(forecast_hour),
                    "--lat",
                    str(lat),
                    "--lon",
                    str(lon),
                    "--sample-method",
                    volume_method,
                    "--output",
                    str(output),
                    "--manifest",
                    str(manifest),
                ]
                station_id = str(payload.get("station_id") or "").strip()
                if station_id:
                    cmd.extend(["--station-id", station_id])
                if volume_method == "box-mean":
                    lat_deg, lon_deg = _box_radius_degrees_from_payload(payload, lat)
                    cmd.extend(["--box-radius-lat-deg", str(lat_deg), "--box-radius-lon-deg", str(lon_deg)])
                else:
                    lat_deg = lon_deg = None
                if payload.get("include_column"):
                    cmd.append("--include-column")
                proc_result = _run_command(
                    cmd,
                    self.server.env.subprocess_env(),
                    timeout=int(payload.get("timeout") or 420),
                )
                result = {
                    **proc_result,
                    "backend": "pressure_volume",
                    "model": model,
                    "source": "pressure_volume",
                    "date_yyyymmdd": date,
                    "cycle_utc": cycle,
                    "forecast_hour": forecast_hour,
                    "lat": lat,
                    "lon": lon,
                    "sample_method_requested": sample_method,
                    "sample_method_used": volume_method,
                    "box_width_km": payload.get("box_width_km"),
                    "box_height_km": payload.get("box_height_km"),
                    "box_bounds": payload.get("box_bounds"),
                    "box_radius_lat_deg": lat_deg,
                    "box_radius_lon_deg": lon_deg,
                    "out_dir": str(out_dir),
                    "wxprofile_store": wxprofile_store_result,
                    "pressure_store": store_result,
                }
                if manifest.exists():
                    try:
                        result["sounding"] = json.loads(manifest.read_text(encoding="utf-8"))
                    except Exception:
                        result["sounding_manifest"] = str(manifest)
                elif proc_result.get("json"):
                    result["sounding"] = proc_result["json"]
                return self._attach_previews(result, started, ["sounding"])
            if data_mode == "store":
                return self._attach_previews(
                    {
                        "ok": False,
                        "backend": "pressure_volume",
                        "stage": "build_pressure_store",
                        "model": model,
                        "source": source,
                        "date_yyyymmdd": date,
                        "cycle_utc": cycle,
                        "forecast_hour": forecast_hour,
                        "lat": lat,
                        "lon": lon,
                        "wxprofile_store": wxprofile_store_result,
                        "pressure_store": store_result,
                    },
                    started,
                    ["sounding"],
                )

        binary = self.server.env.binaries.get("sounding_plot")
        if binary is None:
            return self._attach_previews(
                {
                    "ok": False,
                    "backend": "grib",
                    "error": "sounding_plot binary not found. Build it with cargo build -p rustwx-cli --release --bin sounding_plot or pass --bin-dir.",
                    "wxprofile_store": wxprofile_store_result,
                    "pressure_store": store_result,
                },
                started,
                ["sounding"],
            )
        stamp = time.strftime("%Y%m%d_%H%M%S")
        out_dir = self.server.env.out_root / "studio" / "soundings" / model / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        output = out_dir / f"rustwx_{model}_{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}_sounding.png"
        manifest = out_dir / "sounding_manifest.json"
        cmd = [
            str(binary),
            "--model",
            model,
            "--date",
            date,
            "--cycle",
            str(cycle),
            "--forecast-hour",
            str(forecast_hour),
            "--source",
            source,
            "--lat",
            str(lat),
            "--lon",
            str(lon),
            "--cache-dir",
            str(self.server.env.cache_dir),
            "--out-dir",
            str(out_dir),
            "--output",
            str(output),
            "--manifest",
            str(manifest),
            "--sample-method",
            sample_method,
            "--crop-radius-deg",
            str(float(payload.get("crop_radius_deg") or 1.0)),
        ]
        station_id = str(payload.get("station_id") or "").strip()
        if station_id:
            cmd.extend(["--station-id", station_id])
        if sample_method == "box-mean":
            lat_deg, lon_deg = _box_radius_degrees_from_payload(payload, lat)
            cmd.extend(["--box-radius-lat-deg", str(lat_deg), "--box-radius-lon-deg", str(lon_deg)])
        else:
            lat_deg = lon_deg = None
        if payload.get("include_column"):
            cmd.append("--include-column")
        proc_result = _run_command(
            cmd,
            self.server.env.subprocess_env(),
            timeout=int(payload.get("timeout") or 420),
        )
        result = {
            **proc_result,
            "model": model,
            "source": source,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "forecast_hour": forecast_hour,
            "lat": lat,
            "lon": lon,
            "sample_method_requested": sample_method,
            "sample_method_used": sample_method,
            "box_width_km": payload.get("box_width_km"),
            "box_height_km": payload.get("box_height_km"),
            "box_bounds": payload.get("box_bounds"),
            "box_radius_lat_deg": lat_deg,
            "box_radius_lon_deg": lon_deg,
            "out_dir": str(out_dir),
            "backend": "grib",
            "wxprofile_store": wxprofile_store_result,
            "pressure_store": store_result,
        }
        if manifest.exists():
            try:
                result["sounding"] = json.loads(manifest.read_text(encoding="utf-8"))
            except Exception:
                result["sounding_manifest"] = str(manifest)
        elif proc_result.get("json"):
            result["sounding"] = proc_result["json"]
        return self._attach_previews(result, started, ["sounding"])

    def _render_cross_section(self, payload: dict) -> dict:
        store_binary = self.server.env.binaries.get("hrrr_pressure_volume_store")
        render_binary = self.server.env.binaries.get("volume_store_cross_section_render")
        if store_binary is None or render_binary is None:
            return {
                "ok": False,
                "error": "Cross-section binaries not found. Build hrrr_pressure_volume_store and volume_store_cross_section_render or pass --bin-dir.",
            }
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        if not products:
            products = ["temperature"]
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        forecast_hour = int(payload.get("forecast_hour") or 0)
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=forecast_hour,
        )
        route = _section_route(payload)
        west, east, south, north = _route_bounds(route)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        store_context = self._pressure_store_context(
            {
                **payload,
                "model": model,
                "source": source,
                "run_str": f"{date}/{cycle:02d}",
                "forecast_hour": forecast_hour,
                "domain": route["id"],
                "lat": (route["start"][0] + route["end"][0]) / 2.0,
                "lon": (route["start"][1] + route["end"][1]) / 2.0,
            },
            route=route,
        )
        render_out = self.server.env.out_root / "studio" / "cross_sections" / model / stamp
        render_out.mkdir(parents=True, exist_ok=True)

        render_cmd = [
            str(render_binary),
            "--store",
            str(store_context["store_path"]),
            "--out-dir",
            str(render_out),
            "--products",
            ",".join(products),
            "--hour",
            str(forecast_hour),
            "--spacing-km",
            str(float(payload.get("spacing_km") or 5.0)),
            "--top-pressure-hpa",
            str(float(payload.get("top_pressure_hpa") or 100.0)),
            "--width",
            str(int(payload.get("width") or 1400)),
            "--height",
            str(int(payload.get("height") or 820)),
            "--route-id",
            route["id"],
            "--route-name",
            route["name"],
            "--start-lat",
            str(route["start"][0]),
            "--start-lon",
            str(route["start"][1]),
            "--end-lat",
            str(route["end"][0]),
            "--end-lon",
            str(route["end"][1]),
        ]
        started = time.time()
        store_result = self._ensure_pressure_store(
            store_context,
            payload,
            force=bool(payload.get("force_store")),
            timeout=int(payload.get("store_timeout") or 900),
        )
        if not store_result["ok"]:
            return self._attach_previews(
                {
                    "ok": False,
                    "stage": "build_volume_store",
                    "store": store_result,
                    "store_root": str(store_context["store_root"]),
                    "render_out": str(render_out),
                },
                started,
                products,
            )
        render_result = _run_command(
            render_cmd,
            self.server.env.subprocess_env(),
            timeout=int(payload.get("render_timeout") or 420),
        )
        result = {
            "ok": render_result["ok"],
            "stage": "render_cross_section",
            "model": model,
            "source": source,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "forecast_hour": forecast_hour,
            "route": route,
            "bounds": [west, east, south, north],
            "products": products,
            "store_root": str(store_context["store_root"]),
            "render_out": str(render_out),
            "store": store_result,
            "render": render_result,
        }
        report = render_out / "volume_cross_section_render_report.json"
        if report.exists():
            try:
                result["cross_section"] = json.loads(report.read_text(encoding="utf-8"))
            except Exception:
                result["cross_section_report"] = str(report)
        elif render_result.get("json"):
            result["cross_section"] = render_result["json"]
        return self._attach_previews(result, started, products)

    def _run_wxstore_pipeline(self, payload: dict) -> dict:
        export_binary = self.server.env.binaries.get("rustwx_grid_export")
        import_binary = self.server.env.binaries.get("wxstore")
        showcase_binary = self.server.env.binaries.get("wxstore_wxa_showcase")
        if export_binary is None:
            return {
                "ok": False,
                "error": "rustwx_grid_export binary not found. Build it with cargo build -p rustwx-cli --release --bin rustwx_grid_export or pass --bin-dir.",
            }
        direct_wxa = bool(payload.get("direct_wxa", True))
        import_wxa = bool(payload.get("import_wxa", True)) and not direct_wxa
        render_plots = bool(payload.get("render_plots", True))
        if import_wxa and import_binary is None:
            return {
                "ok": False,
                "error": "wxstore binary not found. Build WxStore or place wxstore.exe on PATH.",
            }
        if render_plots and showcase_binary is None:
            return {
                "ok": False,
                "error": "wxstore_wxa_showcase binary not found. Build it with cargo build -p rustwx-cli --release --bin wxstore_wxa_showcase or pass --bin-dir.",
            }
        products = [str(item) for item in payload.get("products") or [] if str(item).strip()]
        if not products:
            return {"ok": False, "error": "Select at least one WxStore product."}
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        hours = str(payload.get("hours") or payload.get("forecast_hour") or "0").strip() or "0"
        wxstore_hours = _forecast_hours_from_payload({"forecast_hours": hours, "forecast_hour": 0})
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=max(wxstore_hours),
        )
        domain_slug = str(payload.get("domain") or "conus")
        domain = _domain_by_slug(domain_slug)
        if not domain:
            return {"ok": False, "error": f"unknown domain {domain_slug!r}"}
        bounds = [float(value) for value in domain["bounds"]]
        bounds_arg = ",".join(str(value) for value in bounds)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        root = self.server.env.out_root / "studio" / "wxstore" / model / stamp
        export_out = root / "grid_export"
        plot_out = root / "plots"
        spatial_root = self.server.env.cache_dir / "studio_wxstore_spatial"
        export_out.mkdir(parents=True, exist_ok=True)
        jobs = max(1, int(payload.get("jobs") or 1))
        started = time.time()

        export_cmd = [
            str(export_binary),
            "--model",
            model,
            "--date",
            date,
            "--cycle",
            str(cycle),
            "--forecast-hour",
            hours,
            "--source",
            source,
            f"--bounds={bounds_arg}",
            "--domain-slug",
            domain_slug,
            "--product",
            ",".join(products),
            "--out-dir",
            str(export_out),
            "--cache-dir",
            str(self.server.env.cache_dir),
            "--jobs",
            str(jobs),
        ]
        if direct_wxa:
            export_cmd.extend([
                "--direct-wxa-root",
                str(spatial_root),
                "--publish-wxa-latest",
            ])
        if payload.get("hour_chunk_size"):
            export_cmd.extend(["--hour-chunk-size", str(int(payload["hour_chunk_size"]))])
        export_result = _run_command(
            export_cmd,
            self.server.env.subprocess_env(),
            timeout=int(payload.get("export_timeout") or 900),
        )
        export_manifest = _latest_named_file(export_out, "manifest.json")
        export_report = None
        if export_manifest:
            export_report = json.loads(export_manifest.read_text(encoding="utf-8"))
        if not export_result["ok"]:
            return self._attach_previews(
                {
                    "ok": False,
                    "stage": "export_grids",
                    "export": export_result,
                    "export_manifest": str(export_manifest) if export_manifest else None,
                    "export_report": export_report,
                    "export_out": str(export_out),
                    "spatial_root": str(spatial_root),
                },
                started,
                products,
            )
        if export_manifest is None or export_report is None:
            return self._attach_previews(
                {
                    "ok": False,
                    "stage": "export_grids",
                    "error": "rustwx_grid_export completed but no manifest.json was found.",
                    "export": export_result,
                    "export_out": str(export_out),
                    "spatial_root": str(spatial_root),
                },
                started,
                products,
            )

        import_result = {"ok": True, "skipped": True, "mode": "direct_wxa"} if direct_wxa else None
        if import_wxa:
            import_cmd = [
                str(import_binary),
                "import-rustwx-grids",
                "--manifest",
                str(export_manifest),
                "--spatial-root",
                str(spatial_root),
                "--publish-latest",
            ]
            import_result = _run_command(
                import_cmd,
                self.server.env.subprocess_env(),
                timeout=int(payload.get("import_timeout") or 900),
            )
            if not import_result["ok"]:
                return self._attach_previews(
                    {
                        "ok": False,
                        "stage": "import_wxa",
                        "export": export_result,
                        "import": import_result,
                        "export_manifest": str(export_manifest),
                        "export_report": export_report,
                        "spatial_root": str(spatial_root),
                    },
                    started,
                    products,
                )

        showcase_result = None
        showcase_report = None
        if render_plots:
            if not (import_wxa or direct_wxa):
                return {
                    "ok": False,
                    "error": "Rendering WxStore plots requires WXA data first.",
                }
            run_id = str(export_report.get("run_id") or f"{date}_{model}_{cycle:02d}z")
            showcase_cmd = [
                str(showcase_binary),
                "--spatial-root",
                str(spatial_root),
                "--model",
                model,
                "--run",
                run_id,
                "--member",
                str(export_report.get("member") or "control"),
                "--product",
                ",".join(products),
                "--forecast-hour",
                hours,
                "--out-dir",
                str(plot_out),
                "--width",
                str(int(payload.get("width") or DEFAULT_OUTPUT_WIDTH)),
                "--height",
                str(int(payload.get("height") or 900)),
                "--jobs",
                str(jobs),
                f"--bounds={bounds_arg}",
                "--png-compression",
                str(payload.get("png_compression") or "fastest"),
                "--plot-style",
                "operational-fast",
            ]
            if payload.get("max_products"):
                showcase_cmd.extend(["--max-products", str(int(payload["max_products"]))])
            showcase_result = _run_command(
                showcase_cmd,
                self.server.env.subprocess_env(),
                timeout=int(payload.get("showcase_timeout") or 600),
            )
            report_path = plot_out / "wxstore_wxa_showcase_report.json"
            if report_path.exists():
                showcase_report = json.loads(report_path.read_text(encoding="utf-8"))

        result = {
            "ok": bool(export_result["ok"])
            and (import_result is None or bool(import_result["ok"]))
            and (showcase_result is None or bool(showcase_result["ok"])),
            "stage": "wxstore_pipeline",
            "model": model,
            "source": source,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "hours": hours,
            "domain": domain,
            "products": products,
            "export_out": str(export_out),
            "spatial_root": str(spatial_root),
            "plot_out": str(plot_out),
            "export_manifest": str(export_manifest),
            "export_report": export_report,
            "showcase_report": showcase_report,
            "export": export_result,
            "import": import_result,
            "showcase": showcase_result,
        }
        return self._attach_previews(result, started, products)

    def _wxstore_spatial_root(self, payload: dict) -> Path:
        raw = str(payload.get("spatial_root") or "").strip()
        return Path(raw).expanduser().resolve() if raw else self.server.env.cache_dir / "studio_wxstore_spatial"

    def _resolve_wxstore_run_id(self, spatial_root: Path, model: str, run: str) -> tuple[str | None, dict]:
        run_text = str(run or "").strip()
        if run_text and run_text != "latest":
            return run_text, {}
        latest_path = spatial_root / model / "latest.json"
        latest = _json_file_payload(latest_path)
        return str(latest.get("run") or "") or None, latest

    def _read_wxstore_run_manifest(self, spatial_root: Path, model: str, run: str | None) -> tuple[Path, dict]:
        manifest_path = spatial_root / model / str(run or "") / "run-manifest.json"
        return manifest_path, _json_file_payload(manifest_path)

    def _inspect_wxstore(self, payload: dict) -> dict:
        model = str(payload.get("model") or "hrrr")
        member = str(payload.get("member") or "control")
        spatial_root = self._wxstore_spatial_root(payload)
        run_id, latest = self._resolve_wxstore_run_id(spatial_root, model, str(payload.get("run") or "latest"))
        manifest_path, manifest = self._read_wxstore_run_manifest(spatial_root, model, run_id)
        binary = self.server.env.binaries.get("wxstore")
        inspect_result = None
        if binary is not None and spatial_root.exists():
            cmd = [
                str(binary),
                "inspect-spatial",
                "--spatial-root",
                str(spatial_root),
                "--model",
                model,
            ]
            inspect_result = _run_command(cmd, self.server.env.subprocess_env(), timeout=60)
        products = _wxstore_product_rows(manifest, member=member)
        plot_products = [row for row in products if "__" not in row["slug"]]
        return {
            "ok": bool(spatial_root.exists() and run_id and manifest),
            "stage": "wxstore_inspect",
            "spatial_root": str(spatial_root),
            "model": model,
            "run": run_id,
            "member": member,
            "latest": latest,
            "manifest_path": str(manifest_path),
            "manifest": manifest,
            "products": products,
            "plot_products": plot_products or products,
            "product_count": len(products),
            "inspect": inspect_result,
            "error": None if (spatial_root.exists() and run_id and manifest) else "No imported WxStore run was found for this model.",
        }

    def _wxstore_service(self, payload: dict) -> dict:
        action = str(payload.get("action") or "status").strip().lower().replace("-", "_")
        base_url = _wxstore_base_url(payload.get("base_url"))
        if action == "start":
            return self._start_wxstore_service(payload, base_url)
        if action == "status":
            return self._wxstore_status(base_url, timeout=float(payload.get("timeout") or 5.0))
        path, params = _wxstore_service_request(action, payload)
        if not path:
            return {"ok": False, "error": f"unknown WxStore service action {action!r}"}
        return _wxstore_http_json(
            base_url,
            path,
            params=params,
            timeout=float(payload.get("timeout") or 20.0),
        )

    def _wxstore_status(self, base_url: str, *, timeout: float = 5.0) -> dict:
        livez = _wxstore_http_json(base_url, "/livez", timeout=timeout)
        readyz = _wxstore_http_json(base_url, "/readyz", timeout=timeout)
        status = _wxstore_http_json(base_url, "/v1/status", timeout=timeout)
        proc = self.server.wxstore_process
        return {
            "ok": bool(livez.get("ok") and status.get("ok")),
            "stage": "wxstore_service_status",
            "base_url": base_url,
            "livez": livez,
            "readyz": readyz,
            "status": status,
            "local_process": {
                "started_by_studio": proc is not None,
                "pid": proc.pid if proc else None,
                "running": proc is not None and proc.poll() is None,
            },
            "wxstore_binary": str(self.server.env.binaries.get("wxstore")) if self.server.env.binaries.get("wxstore") else None,
            "spatial_root": str(self.server.env.cache_dir / "studio_wxstore_spatial"),
            "satellite_tiles_root": str(self.server.env.cache_dir / "studio_satellite_tiles"),
            "radar_tiles_root": str(self.server.env.cache_dir / "studio_radar_tiles"),
        }

    def _start_wxstore_service(self, payload: dict, base_url: str) -> dict:
        binary = self.server.env.binaries.get("wxstore")
        if not binary:
            return {"ok": False, "error": "wxstore binary not found. Build WxStore or place wxstore.exe on PATH."}
        parsed = urlparse(base_url)
        host = parsed.hostname or "127.0.0.1"
        port = int(parsed.port or 8897)
        spatial_root = self._wxstore_spatial_root(payload)
        satellite_tiles_root = self.server.env.cache_dir / "studio_satellite_tiles"
        radar_tiles_root = self.server.env.cache_dir / "studio_radar_tiles"
        spatial_root.mkdir(parents=True, exist_ok=True)
        satellite_tiles_root.mkdir(parents=True, exist_ok=True)
        radar_tiles_root.mkdir(parents=True, exist_ok=True)
        with self.server.wxstore_lock:
            current = self.server.wxstore_process
            if current is not None and current.poll() is None:
                status = self._wxstore_status(base_url, timeout=2.0)
                status["started"] = False
                status["message"] = "WxStore service is already managed by Studio."
                return status
            if self._wxstore_status(base_url, timeout=1.0).get("ok"):
                status = self._wxstore_status(base_url, timeout=2.0)
                status["started"] = False
                status["message"] = "WxStore service is already reachable."
                return status
            cmd = [
                str(binary),
                "serve",
                "--spatial-root",
                str(spatial_root),
                "--satellite-tiles-root",
                str(satellite_tiles_root),
                "--radar-tiles-root",
                str(radar_tiles_root),
                "--host",
                host,
                "--port",
                str(port),
            ]
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
            proc = subprocess.Popen(
                cmd,
                cwd=str(Path(binary).parent),
                env=self.server.env.subprocess_env(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
            )
            self.server.wxstore_process = proc
        deadline = time.time() + float(payload.get("startup_timeout") or 8.0)
        status = self._wxstore_status(base_url, timeout=1.5)
        while not status.get("ok") and time.time() < deadline:
            time.sleep(0.3)
            status = self._wxstore_status(base_url, timeout=1.5)
        status["started"] = True
        status["command"] = cmd
        status["pid"] = proc.pid
        return status

    def _plot_existing_wxstore(self, payload: dict) -> dict:
        showcase_binary = self.server.env.binaries.get("wxstore_wxa_showcase")
        if showcase_binary is None:
            return {
                "ok": False,
                "error": "wxstore_wxa_showcase binary not found. Build it with cargo build -p rustwx-cli --release --bin wxstore_wxa_showcase or pass --bin-dir.",
            }
        started = time.time()
        model = str(payload.get("model") or "hrrr")
        member = str(payload.get("member") or "control")
        spatial_root = self._wxstore_spatial_root(payload)
        run_id, latest = self._resolve_wxstore_run_id(spatial_root, model, str(payload.get("run") or "latest"))
        manifest_path, manifest = self._read_wxstore_run_manifest(spatial_root, model, run_id)
        if not run_id or not manifest:
            return {
                "ok": False,
                "stage": "wxstore_plot_existing",
                "error": "No imported WxStore run was found. Build/import a store first, then inspect it.",
                "spatial_root": str(spatial_root),
                "model": model,
                "run": run_id,
                "latest": latest,
                "manifest_path": str(manifest_path),
            }
        available = _wxstore_product_rows(manifest, member=member)
        available_slugs = {row["slug"] for row in available}
        requested_products = [
            str(item).strip()
            for item in payload.get("products") or []
            if str(item).strip()
        ]
        products = [item for item in requested_products if item in available_slugs]
        missing_products = [item for item in requested_products if item not in available_slugs]
        if requested_products and not products:
            return {
                "ok": False,
                "stage": "wxstore_plot_existing",
                "error": "Requested WXA products are not available in this store.",
                "model": model,
                "run": run_id,
                "member": member,
                "requested_products": requested_products,
                "missing_products": missing_products,
                "available_products": sorted(available_slugs)[:100],
                "spatial_root": str(spatial_root),
                "manifest_path": str(manifest_path),
            }
        if not products:
            products = [row["slug"] for row in available if "__" not in row["slug"]][:1]
        if not products:
            products = [row["slug"] for row in available[:1]]
        if not products:
            return {"ok": False, "error": "No plottable WXA products were found in this store."}

        hours = str(payload.get("hours") or payload.get("forecast_hour") or "0").strip() or "0"
        stamp = time.strftime("%Y%m%d_%H%M%S")
        out_dir = self.server.env.out_root / "studio" / "wxstore_existing" / model / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs = max(1, int(payload.get("jobs") or 1))
        cmd = [
            str(showcase_binary),
            "--spatial-root",
            str(spatial_root),
            "--model",
            model,
            "--run",
            run_id,
            "--member",
            member,
            "--product",
            ",".join(products),
            "--forecast-hour",
            hours,
            "--out-dir",
            str(out_dir),
            "--width",
            str(int(payload.get("width") or DEFAULT_OUTPUT_WIDTH)),
            "--height",
            str(int(payload.get("height") or 900)),
            "--jobs",
            str(jobs),
            "--png-compression",
            str(payload.get("png_compression") or "fastest"),
            "--plot-style",
            "operational-fast",
        ]
        ui_domain = "store"
        ui_bounds = _wxstore_first_product_bounds(manifest, products)
        if payload.get("use_domain_bounds"):
            domain_slug = str(payload.get("domain") or "conus")
            domain = _domain_by_slug(domain_slug)
            if domain:
                bounds = [float(value) for value in domain["bounds"]]
                cmd.append(f"--bounds={','.join(str(value) for value in bounds)}")
                ui_domain = domain_slug
                ui_bounds = bounds
        if payload.get("max_products"):
            cmd.extend(["--max-products", str(int(payload["max_products"]))])
        showcase_result = _run_command(
            cmd,
            self.server.env.subprocess_env(),
            timeout=int(payload.get("showcase_timeout") or 600),
        )
        report_path = out_dir / "wxstore_wxa_showcase_report.json"
        showcase_report = _json_file_payload(report_path)
        result = {
            "ok": bool(showcase_result["ok"]) and bool((showcase_report or {}).get("rendered_count", 0) or _collect_paths(showcase_report, ".png")),
            "stage": "wxstore_plot_existing",
            "model": model,
            "run": run_id,
            "member": member,
            "hours": hours,
            "products": products,
            "spatial_root": str(spatial_root),
            "manifest_path": str(manifest_path),
            "out_dir": str(out_dir),
            "showcase_report": showcase_report,
            "showcase": showcase_result,
            "ui_domain": ui_domain,
            "ui_domain_bounds": ui_bounds,
        }
        if not result["ok"]:
            result["error"] = _command_error(showcase_result) or "WxStore plot did not render a PNG."
        return self._attach_previews(result, started, products)

    def _run_ecape_profile(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("hrrr_ecape_profile_probe")
        if not binary:
            return {
                "ok": False,
                "error": "hrrr_ecape_profile_probe binary not found. Build it with cargo build -p rustwx-cli --release --bin hrrr_ecape_profile_probe or pass --bin-dir.",
            }
        started = time.time()
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        forecast_hour = _int_payload(payload, "forecast_hour", 1)
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=forecast_hour,
        )
        lat = float(payload.get("lat") or 35.222)
        lon = float(payload.get("lon") or -97.439)
        stamp = f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}"
        out_dir = self.server.env.out_root / "studio" / "ecape_profile" / _safe_slug(stamp)
        out_dir.mkdir(parents=True, exist_ok=True)
        output_json = out_dir / "profile.json"
        cmd = [
            str(binary),
            "--model",
            model,
            "--date",
            date,
            "--cycle",
            str(cycle),
            "--forecast-hour",
            str(forecast_hour),
            "--source",
            source,
            f"--lat={lat:.6f}",
            f"--lon={lon:.6f}",
            "--crop-radius-deg",
            str(float(payload.get("crop_radius_deg") or 1.0)),
            "--cache-dir",
            str(self.server.env.cache_dir),
            "--output",
            str(output_json),
        ]
        if payload.get("include_input_column"):
            cmd.append("--include-input-column")
        run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 180))
        diagnostics = _json_file_payload(output_json)
        result = {
            "ok": bool(run["ok"]) and output_json.exists(),
            "stage": "ecape_profile",
            "model": model,
            "source": source,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "forecast_hour": forecast_hour,
            "lat": lat,
            "lon": lon,
            "out_dir": str(out_dir),
            "output_json": str(output_json),
            "diagnostics": diagnostics,
            "command": run,
        }
        if not result["ok"]:
            result["error"] = _command_error(run) or "ECAPE profile did not produce profile.json."
        return self._attach_previews(result, started, ["ecape_profile"])

    def _run_ecape_grid(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("hrrr_ecape_grid_research")
        if not binary:
            return {
                "ok": False,
                "error": "hrrr_ecape_grid_research binary not found. Build it with cargo build -p rustwx-cli --release --bin hrrr_ecape_grid_research or pass --bin-dir.",
            }
        started = time.time()
        model = str(payload.get("model") or "hrrr")
        source = str(payload.get("source") or "aws")
        forecast_hour = _int_payload(payload, "forecast_hour", 1)
        date, cycle = self._resolve_run(
            str(payload.get("run_str") or "latest"),
            model,
            source,
            forecast_hour=forecast_hour,
        )
        domain_slug = str(payload.get("domain") or "oklahoma")
        domain = _domain_by_slug(domain_slug)
        bounds = _coerce_bounds(payload.get("bounds"), domain.get("bounds") if domain else RADAR_BASEMAP_BOUNDS)
        stamp = f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{domain_slug}"
        out_dir = self.server.env.out_root / "studio" / "ecape_grid" / _safe_slug(stamp)
        out_dir.mkdir(parents=True, exist_ok=True)
        output_json = out_dir / f"{_safe_slug(domain_slug)}.json"
        west, east, south, north = bounds
        cmd = [
            str(binary),
            "--model",
            model,
            "--date",
            date,
            "--cycle",
            str(cycle),
            "--forecast-hour",
            str(forecast_hour),
            "--source",
            source,
            f"--west={west:.4f}",
            f"--east={east:.4f}",
            f"--south={south:.4f}",
            f"--north={north:.4f}",
            "--domain-slug",
            domain_slug,
            "--cache-dir",
            str(self.server.env.cache_dir),
            "--output",
            str(output_json),
        ]
        if payload.get("write_csv"):
            cmd.extend([
                "--components-csv",
                str(out_dir / "components.csv"),
                "--reports-csv",
                str(out_dir / "reports.csv"),
                "--report-overlap-csv",
                str(out_dir / "report_overlap.csv"),
                "--field-grid-csv",
                str(out_dir / "field_grid.csv"),
            ])
        run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 1800))
        statistics = _json_file_payload(output_json)
        result = {
            "ok": bool(run["ok"]) and output_json.exists(),
            "stage": "ecape_grid",
            "model": model,
            "source": source,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "forecast_hour": forecast_hour,
            "domain": domain_slug,
            "bounds": [west, east, south, north],
            "out_dir": str(out_dir),
            "output_json": str(output_json),
            "statistics": statistics,
            "command": run,
        }
        if not result["ok"]:
            result["error"] = _command_error(run) or "ECAPE grid did not produce output JSON."
        return self._attach_previews(result, started, ["ecape_grid"])

    def _run_ecape_ratio(self, payload: dict) -> dict:
        parcel = str(payload.get("parcel") or "ml").lower().strip()
        if parcel not in {"sb", "ml", "mu"}:
            return {"ok": False, "error": f"parcel must be sb, ml, or mu; got {parcel!r}."}
        products = [
            f"{parcel}ecape",
            f"{parcel}cape",
            f"{parcel}_ecape_derived_cape_ratio",
        ]
        if payload.get("include_native_ratio"):
            products.append(f"{parcel}_ecape_native_cape_ratio")
        mapped = dict(payload)
        mapped["products"] = products
        mapped.setdefault("domain", payload.get("domain") or "oklahoma")
        mapped.setdefault("forecast_hour", _int_payload(payload, "forecast_hour", 1))
        mapped.setdefault("forecast_hours", [int(mapped["forecast_hour"])])
        return self._render_maps(mapped)

    def _run_native_dataset_plan(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("native_dataset_plan")
        if not binary:
            return {
                "ok": False,
                "error": "native_dataset_plan binary not found. Build it with cargo build -p rustwx-cli --release --bin native_dataset_plan or pass --bin-dir.",
            }
        started = time.time()
        stamp = time.strftime("%Y%m%d_%H%M%S")
        out_dir = self.server.env.out_root / "studio" / "native_dataset" / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = Path(payload.get("out") or out_dir / "dataset_plan.json")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        domain_slug = str(payload.get("domain") or "oklahoma")
        domain = _domain_by_slug(domain_slug)
        tile_grid = str(payload.get("tile_grid") or "").strip()
        if not tile_grid and domain and domain.get("bounds"):
            west, east, south, north = _coerce_bounds(domain.get("bounds"), None)
            tile_grid = f"{west:.4f},{east:.4f},{south:.4f},{north:.4f},1,1"
        cmd = [
            str(binary),
            "--dataset-name",
            str(payload.get("dataset_name") or "rustwx_hrrr_multisource_v1"),
            "--shard-index",
            str(int(payload.get("shard_index") or 0)),
            "--shard-count",
            str(int(payload.get("shard_count") or 1)),
            "--grid-size",
            str(int(payload.get("grid_size") or 512)),
            "--history-steps",
            str(int(payload.get("history_steps") or 3)),
            "--forecast-step-frames",
            str(int(payload.get("forecast_step_frames") or 1)),
            "--out",
            str(out_path),
        ]
        cases = _native_list(payload.get("cases"))
        case = str(payload.get("case") or "").strip()
        for item in cases or ([case] if case else ["20240506_ok_ks,2024-05-06T12:00:00Z,1"]):
            cmd.extend(["--case", item])
        if tile_grid:
            cmd.extend(["--tile-grid", tile_grid])
        for item in _native_list(payload.get("tiles")):
            cmd.extend(["--tile", item])
        for flag, key in (
            ("--hrrr-fields", "hrrr_fields"),
            ("--mrms-fields", "mrms_fields"),
            ("--goes-channels", "goes_channels"),
            ("--goes-derived", "goes_derived"),
            ("--level2-products", "level2_products"),
        ):
            value = _native_csv(payload.get(key))
            if value:
                cmd.extend([flag, value])
        if payload.get("goes_product_family"):
            cmd.extend(["--goes-product-family", str(payload["goes_product_family"])])
        if payload.get("goes_sector"):
            cmd.extend(["--goes-sector", str(payload["goes_sector"])])
        if payload.get("print_plan"):
            cmd.append("--print")
        run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 120))
        plan = _json_file_payload(out_path)
        config = plan.get("config", {}) if isinstance(plan, dict) else {}
        result = {
            "ok": bool(run["ok"]) and out_path.exists(),
            "stage": "native_dataset_plan",
            "plan_path": str(out_path),
            "domain": domain_slug,
            "tile_grid": tile_grid,
            "dataset_name": plan.get("dataset_name") if isinstance(plan, dict) else None,
            "case_count": len(config.get("cases") or []) if isinstance(config, dict) else None,
            "tile_count": len(config.get("tiles") or []) if isinstance(config, dict) else None,
            "source_count": len(config.get("sources") or []) if isinstance(config, dict) else None,
            "sources": config.get("sources") if isinstance(config, dict) else None,
            "plan": plan,
            "command": run,
        }
        if not result["ok"]:
            result["error"] = _command_error(run) or "native_dataset_plan did not produce dataset_plan.json."
        return self._attach_previews(result, started, ["native_dataset_plan"])

    def _run_native_dataset_runner(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("native_dataset_runner")
        if not binary:
            return {
                "ok": False,
                "error": "native_dataset_runner binary not found. Build it with cargo build -p rustwx-cli --release --bin native_dataset_runner or pass --bin-dir.",
            }
        started = time.time()
        plan_path = str(payload.get("plan_path") or "").strip()
        plan_result = None
        if not plan_path:
            plan_result = self._run_native_dataset_plan(payload)
            if not plan_result.get("ok"):
                return plan_result
            plan_path = str(plan_result.get("plan_path") or "")
        if not plan_path:
            return {"ok": False, "error": "No native dataset plan path was supplied or generated."}
        plan = Path(plan_path)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        out_dir = self.server.env.out_root / "studio" / "native_dataset_run" / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        progress_out = Path(payload.get("progress_out") or out_dir / "progress.json")
        report_out = Path(payload.get("report_out") or out_dir / "report.json")
        fetch_requested = bool(payload.get("fetch_hrrr") or payload.get("fetch_obs") or payload.get("fetch_radar"))
        materialize = bool(payload.get("materialize") or payload.get("shard_out") or fetch_requested)
        shard_out = Path(payload.get("shard_out") or out_dir / "shards") if materialize else None
        cmd = [
            str(binary),
            "--plan",
            str(plan),
            "--progress-out",
            str(progress_out),
            "--report-out",
            str(report_out),
            "--cache-root",
            str(Path(payload.get("cache_root") or self.server.env.cache_dir / "native_dataset")),
            "--max-attempts",
            str(int(payload.get("max_attempts") or 3)),
        ]
        if shard_out is not None:
            cmd.extend(["--shard-out", str(shard_out)])
        if payload.get("source_root"):
            cmd.extend(["--source-root", str(payload["source_root"])])
        if payload.get("allow_missing_sources", True):
            cmd.append("--allow-missing-sources")
        if payload.get("fetch_hrrr"):
            cmd.append("--fetch-hrrr")
        if payload.get("fetch_obs"):
            cmd.append("--fetch-obs")
        if payload.get("fetch_radar"):
            cmd.append("--fetch-radar")
        if payload.get("continue_on_error"):
            cmd.append("--continue-on-error")
        if payload.get("rayon_threads"):
            cmd.extend(["--rayon-threads", str(int(payload["rayon_threads"]))])
        run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 3600))
        result = {
            "ok": bool(run["ok"]),
            "stage": "native_dataset_run",
            "plan_path": str(plan),
            "generated_plan": plan_result,
            "out_dir": str(out_dir),
            "materialize": materialize,
            "progress_path": str(progress_out) if progress_out.exists() else None,
            "report_path": str(report_out) if report_out.exists() else None,
            "shard_out": str(shard_out) if shard_out else None,
            "report": _json_file_payload(report_out),
            "progress": _json_lines_or_json_file_payload(progress_out),
            "shards": [str(path) for path in sorted(shard_out.rglob("*")) if path.is_file()] if shard_out and shard_out.exists() else [],
            "command": run,
        }
        if not result["ok"]:
            result["error"] = _command_error(run) or "native_dataset_runner failed."
        return self._attach_previews(result, started, ["native_dataset_run"])

    def _run_native_obs_preview(self, payload: dict) -> dict:
        binary = self.server.env.binaries.get("native_obs_preview")
        if not binary:
            return {
                "ok": False,
                "error": "native_obs_preview binary not found. Build it with cargo build -p rustwx-cli --release --bin native_obs_preview or pass --bin-dir.",
            }
        started = time.time()
        input_path = str(payload.get("input") or "").strip()
        if not input_path:
            return {"ok": False, "error": "Native obs preview needs an input file path."}
        kind = str(payload.get("kind") or "goes")
        stamp = time.strftime("%Y%m%d_%H%M%S")
        out_dir = self.server.env.out_root / "studio" / "native_obs_preview" / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = Path(payload.get("out") or out_dir / f"{_safe_slug(kind)}_{Path(input_path).stem}.png")
        cmd = [
            str(binary),
            "--kind",
            kind,
            "--input",
            input_path,
            "--out",
            str(out_path),
            "--size",
            str(int(payload.get("size") or 768)),
        ]
        bounds = payload.get("bounds")
        if bounds:
            west, east, south, north = _coerce_bounds(bounds, None)
            cmd.extend(["--bounds", f"{west:.4f},{east:.4f},{south:.4f},{north:.4f}"])
        for flag, key in (
            ("--channel", "channel"),
            ("--product", "product"),
            ("--radar-site", "radar_site"),
            ("--center-lat", "center_lat"),
            ("--center-lon", "center_lon"),
            ("--span-km", "span_km"),
            ("--min", "min_value"),
            ("--max", "max_value"),
            ("--grid-export-dir", "grid_export_dir"),
        ):
            value = str(payload.get(key) or "").strip()
            if value:
                cmd.extend([flag, value])
        dealias = str(payload.get("dealias") or "auto").strip()
        if dealias:
            cmd.extend(["--dealias", dealias])
        run = _run_command(cmd, self.server.env.subprocess_env(), timeout=int(payload.get("timeout") or 300))
        report_path = out_path.with_suffix(".json")
        result = {
            "ok": bool(run["ok"]) and out_path.exists(),
            "stage": "native_obs_preview",
            "kind": kind,
            "input": input_path,
            "png": str(out_path),
            "report_path": str(report_path) if report_path.exists() else None,
            "report": _json_file_payload(report_path),
            "command": run,
        }
        if not result["ok"]:
            result["error"] = _command_error(run) or "native_obs_preview did not produce a PNG."
        return self._attach_previews(result, started, ["native_obs_preview"])

    def _resolve_run(self, run_str: str, model: str, source: str, *, forecast_hour: int = 0) -> tuple[str, int]:
        if run_str and run_str != "latest":
            parsed = _parse_run_string(run_str)
            if parsed:
                return parsed
            raise ValueError("Run must be latest or a date/cycle such as YYYYMMDD/HH, YYYY-MM-DD HHz, or M/D/YY HHz.")
        today = datetime.now(UTC).strftime("%Y%m%d")
        latest = _load_json(rustwx.latest_run_json(model, today, source, int(forecast_hour)))
        cycle = latest.get("cycle") or {}
        return str(cycle["date_yyyymmdd"]), int(cycle["hour_utc"])

    def _attach_previews(self, result: dict, started: float, requested: list[str]) -> dict:
        pngs = sorted(set(_collect_paths(result, ".png")))
        gifs = sorted(set(_collect_paths(result, ".gif")))
        webps = sorted(set(_collect_paths(result, ".webp")))
        result.setdefault("ok", "error" not in result)
        result["ui_elapsed_s"] = round(time.time() - started, 2)
        result["requested_products"] = requested
        previews = []
        for path in [*pngs, *gifs, *webps]:
            preview = {"path": path, "name": Path(path).name, "url": f"/api/file?path={quote(path)}"}
            forecast_hour = _forecast_hour_from_path(path)
            if forecast_hour is None and result.get("ui_forecast_hour") is not None:
                forecast_hour = int(result["ui_forecast_hour"])
            if forecast_hour is not None:
                preview["forecast_hour"] = forecast_hour
            if result.get("ui_domain"):
                preview["domain"] = result["ui_domain"]
            if result.get("ui_domain_bounds"):
                preview["bounds"] = result["ui_domain_bounds"]
            previews.append(preview)
        result["previews"] = previews
        return result

    def _read_json(self) -> dict:
        length = int(self.headers.get("content-length") or 0)
        if length > MAX_BODY_BYTES:
            return {}
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def _send_json(self, payload: dict, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, query: dict[str, list[str]]) -> None:
        raw = unquote(_query_one(query, "path", ""))
        try:
            path = Path(raw).resolve()
        except Exception:
            self._send_json({"ok": False, "error": "bad path"}, status=HTTPStatus.BAD_REQUEST)
            return
        if not _path_allowed(path, self.server.allowed_file_roots):
            self._send_json({"ok": False, "error": "file outside allowed roots"}, status=HTTPStatus.FORBIDDEN)
            return
        if not path.is_file():
            self._send_json({"ok": False, "error": "file not found"}, status=HTTPStatus.NOT_FOUND)
            return
        ctype = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(path.stat().st_size))
        self.end_headers()
        with path.open("rb") as handle:
            shutil.copyfileobj(handle, self.wfile)


def _package_version() -> str:
    try:
        return importlib.metadata.version("rustwx")
    except importlib.metadata.PackageNotFoundError:
        return "editable"


def _load_json(payload: str) -> dict:
    return json.loads(payload)


def _parse_run_string(run_str: str) -> tuple[str, int] | None:
    value = str(run_str or "").strip()
    if not value or value.lower() == "latest":
        return None
    normalized = value.lower().replace("_", " ").replace("z", " ")
    patterns = [
        r"(?P<ymd>\d{8})\D*(?P<hour>\d{1,2})",
        r"(?P<ymdh>\d{10})",
        r"(?P<year>\d{4})[-/](?P<month>\d{1,2})[-/](?P<day>\d{1,2})\D+(?P<hour>\d{1,2})",
        r"(?P<month>\d{1,2})[-/](?P<day>\d{1,2})[-/](?P<year>\d{2,4})\D+(?P<hour>\d{1,2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if not match:
            continue
        groups = match.groupdict()
        if groups.get("ymdh"):
            ymd = str(groups["ymdh"][:8])
            hour = int(groups["ymdh"][8:10])
        elif groups.get("ymd"):
            ymd = str(groups["ymd"])
            hour = int(groups["hour"])
        else:
            year = int(groups["year"])
            if year < 100:
                year += 2000 if year < 70 else 1900
            month = int(groups["month"])
            day = int(groups["day"])
            hour = int(groups["hour"])
            ymd = f"{year:04d}{month:02d}{day:02d}"
        if not 0 <= hour <= 23:
            raise ValueError(f"Run cycle hour must be 00-23, got {hour:02d}.")
        try:
            datetime.strptime(ymd, "%Y%m%d")
        except ValueError as exc:
            raise ValueError(f"Run date must be a real UTC date, got {ymd}.") from exc
        return ymd, hour
    return None


def _json_file_payload(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"parse_error": f"{type(exc).__name__}: {exc}", "path": str(path)}
    return {}


def _inventory_section(section_id: str, label: str, path: Path, category: str) -> dict:
    exists = path.exists()
    stats = _tree_stats(path) if exists else {}
    return {
        "id": section_id,
        "label": label,
        "category": category,
        "path": str(path),
        "exists": exists,
        **stats,
        "recent": _recent_inventory_files(path) if exists else [],
    }


def _tree_stats(root: Path, *, max_files: int = 50_000) -> dict:
    files = 0
    dirs = 0
    bytes_total = 0
    latest_ts = 0.0
    truncated = False
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            dirs += 1
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            stat = entry.stat(follow_symlinks=False)
                            files += 1
                            bytes_total += int(stat.st_size)
                            latest_ts = max(latest_ts, float(stat.st_mtime))
                            if files >= max_files:
                                truncated = True
                                stack.clear()
                                break
                    except OSError:
                        continue
        except OSError:
            continue
    return {
        "dirs": dirs,
        "files": files,
        "bytes": bytes_total,
        "latest_mtime": _iso_from_local_ts(latest_ts) if latest_ts else None,
        "latest_mtime_ts": latest_ts or None,
        "truncated": truncated,
    }


def _recent_inventory_files(
    root: Path,
    *,
    limit: int = 8,
    max_scan: int = 40_000,
    suffixes: tuple[str, ...] = (".png", ".gif", ".webp", ".html", ".json", ".wxa", ".csv"),
) -> list[dict]:
    rows = []
    scanned = 0
    stack = [root]
    while stack and scanned < max_scan:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            scanned += 1
                            if Path(entry.name).suffix.lower() not in suffixes:
                                continue
                            stat = entry.stat(follow_symlinks=False)
                            rows.append(_inventory_file(Path(entry.path), stat=stat))
                    except OSError:
                        continue
                    if scanned >= max_scan:
                        break
        except OSError:
            continue
    rows.sort(key=lambda item: item.get("mtime_ts") or 0, reverse=True)
    return rows[:limit]


def _inventory_file(path: Path, *, stat: os.stat_result | None = None) -> dict:
    stat = stat or path.stat()
    return {
        "name": path.name,
        "path": str(path),
        "url": f"/api/file?path={quote(str(path.resolve()))}",
        "bytes": int(stat.st_size),
        "mtime": _iso_from_local_ts(float(stat.st_mtime)),
        "mtime_ts": float(stat.st_mtime),
        "suffix": path.suffix.lower(),
    }


def _inventory_wxstore_runs(root: Path) -> list[dict]:
    rows = []
    if not root.is_dir():
        return rows
    for model_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        latest = _json_file_payload(model_dir / "latest.json")
        for run_dir in sorted(path for path in model_dir.iterdir() if path.is_dir()):
            manifest_path = run_dir / "run-manifest.json"
            manifest = _json_file_payload(manifest_path)
            if not manifest:
                continue
            products = manifest.get("products") if isinstance(manifest.get("products"), list) else []
            forecast_hours = sorted({
                int(hour)
                for product in products
                if isinstance(product, dict)
                for hour in product.get("forecast_hours", [])
            })
            stat = manifest_path.stat() if manifest_path.exists() else None
            rows.append({
                "kind": "wxstore_spatial",
                "model": model_dir.name,
                "run": run_dir.name,
                "latest": latest.get("run") == run_dir.name,
                "path": str(run_dir),
                "manifest": _inventory_file(manifest_path, stat=stat) if stat else None,
                "product_count": len(products),
                "forecast_hours": forecast_hours,
            })
    rows.sort(key=lambda item: (not item.get("latest"), item.get("model") or "", item.get("run") or ""))
    return rows


def _inventory_satellite_layers(root: Path) -> list[dict]:
    rows = []
    if not root.is_dir():
        return rows
    for layer_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        frames_path = layer_dir / "frames.json"
        frames_doc = _json_file_payload(frames_path)
        frames = frames_doc.get("frames") if isinstance(frames_doc.get("frames"), list) else []
        latest = frames[-1] if frames else {}
        stat = frames_path.stat() if frames_path.exists() else None
        rows.append({
            "kind": "satellite_tile_lane",
            "layer": layer_dir.name,
            "path": str(layer_dir),
            "manifest": _inventory_file(frames_path, stat=stat) if stat else None,
            "frame_count": len(frames),
            "latest_frame": latest.get("id"),
            "latest_time": latest.get("scan_time_utc"),
            "product": latest.get("product") or frames_doc.get("product"),
            "satellite": frames_doc.get("satellite"),
            "tile_layer": frames_doc.get("tile_layer"),
            "tile_count": sum(int(frame.get("tile_count") or 0) for frame in frames if isinstance(frame, dict)),
        })
    return rows


def _inventory_radar_layers(root: Path) -> list[dict]:
    rows = []
    if not root.is_dir():
        return rows
    for layer_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        frames_path = layer_dir / "frames.json"
        frames_doc = _json_file_payload(frames_path)
        frames = frames_doc.get("frames") if isinstance(frames_doc.get("frames"), list) else []
        latest = frames[-1] if frames else {}
        stat = frames_path.stat() if frames_path.exists() else None
        rows.append({
            "kind": "radar_tile_lane",
            "layer": layer_dir.name,
            "path": str(layer_dir),
            "manifest": _inventory_file(frames_path, stat=stat) if stat else None,
            "frame_count": len(frames),
            "latest_frame": latest.get("id"),
            "latest_time": latest.get("scan_time_utc"),
            "product": latest.get("product"),
            "site": latest.get("site"),
            "tile_count": sum(int(frame.get("tile_count") or 0) for frame in frames if isinstance(frame, dict)),
        })
    return rows


def _inventory_store_dirs(root: Path, kind: str) -> list[dict]:
    rows = []
    if not root.is_dir():
        return rows
    for store_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        manifest = next(
            (candidate for candidate in [store_dir / "manifest.json", store_dir / "volume_store_manifest.json"] if candidate.exists()),
            None,
        )
        rows.append({
            "kind": kind,
            "name": store_dir.name,
            "path": str(store_dir),
            "manifest": _inventory_file(manifest) if manifest else None,
            **_tree_stats(store_dir, max_files=10_000),
        })
    return rows


def _iso_from_local_ts(value: float) -> str:
    return datetime.fromtimestamp(value, UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json_lines_or_json_file_payload(path: Path) -> dict | list:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        rows = []
        for line in text.splitlines()[-200:]:
            try:
                rows.append(json.loads(line))
            except Exception:
                rows.append({"line": line})
        return rows


def _command_error(result: dict) -> str:
    stderr = "\n".join(str(line) for line in result.get("stderr_tail") or []).strip()
    stdout = "\n".join(str(line) for line in result.get("stdout_tail") or []).strip()
    return (stderr or stdout)[-1200:]


def _native_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [line.strip() for line in text.replace(";", "\n").splitlines() if line.strip()]


def _native_csv(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        text = ",".join(str(item).strip() for item in value if str(item).strip())
    else:
        text = str(value).strip()
    return text or None


def _csv_items(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [
        item.strip()
        for item in text.replace(";", ",").replace("\n", ",").split(",")
        if item.strip()
    ]


def _int_payload(payload: dict, key: str, default: int) -> int:
    value = payload.get(key)
    if value in (None, ""):
        return default
    return int(value)


def _date_range_yyyymmdd(start_date: str, end_date: str) -> list[str]:
    start = _parse_date_yyyymmdd(start_date)
    end = _parse_date_yyyymmdd(end_date)
    if end < start:
        start, end = end, start
    days = []
    current = start
    while current <= end:
        days.append(current.strftime("%Y%m%d"))
        current = current + timedelta(days=1)
    return days


def _parse_date_yyyymmdd(value: str):
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def _cycles_from_payload(payload: dict, model: str) -> list[int]:
    raw = payload.get("cycles")
    cycles: set[int] = set()
    if isinstance(raw, (list, tuple)):
        values = raw
    else:
        text = str(raw or "").strip()
        values = [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]
    for value in values:
        cycles.add(int(value) % 24)
    if cycles:
        return sorted(cycles)
    if model == "hrrr":
        return list(range(24))
    if model == "gfs":
        return [0, 6, 12, 18]
    return [0, 12]


def _case_specs(
    *,
    model: str,
    start_date: str,
    end_date: str,
    cycles: list[int],
    forecast_hours: list[int],
) -> list[dict]:
    specs = []
    for date in _date_range_yyyymmdd(start_date, end_date):
        for cycle in cycles:
            for forecast_hour in forecast_hours:
                specs.append({
                    "model": model,
                    "date_yyyymmdd": date,
                    "cycle_utc": int(cycle),
                    "forecast_hour": int(forecast_hour),
                })
    return specs


def _case_points_from_payload(payload: dict) -> list[dict]:
    raw = payload.get("profile_points") or payload.get("points")
    if raw is None:
        lat = payload.get("lat")
        lon = payload.get("lon")
        if lat not in (None, "") and lon not in (None, ""):
            return [{"lat": float(lat), "lon": float(lon), "label": "point"}]
        return []
    if isinstance(raw, str):
        items = [part.strip() for part in raw.replace("\n", ";").split(";") if part.strip()]
    else:
        items = list(raw)
    points = []
    for index, item in enumerate(items, start=1):
        if isinstance(item, dict):
            lat = item.get("lat")
            lon = item.get("lon")
            label = str(item.get("label") or item.get("name") or f"point_{index}")
        else:
            pieces = [part.strip() for part in str(item).replace(" ", ",").split(",") if part.strip()]
            if len(pieces) < 2:
                continue
            lat, lon = pieces[0], pieces[1]
            label = pieces[2] if len(pieces) > 2 else f"point_{index}"
        try:
            points.append({"lat": float(lat), "lon": float(lon), "label": label})
        except (TypeError, ValueError):
            continue
    return points


def _model_entry(caps: dict, model_id: str) -> dict | None:
    for model in caps.get("models") or []:
        if model.get("id") == model_id:
            return model
    return None


def _batch_model_ids(caps: dict, payload: dict, current_model: str) -> list[str]:
    available = [str(model.get("id")) for model in caps.get("models") or [] if model.get("id")]
    available_set = set(available)
    mode = str(payload.get("models_mode") or "current").strip().lower().replace("-", "_")
    raw = str(payload.get("models") or "").strip()
    if mode == "all":
        return available
    if mode in {"list", "custom"} and raw:
        wanted = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
        return [model for model in wanted if model in available_set]
    return [current_model] if current_model in available_set else available[:1]


def _batch_products_for_model(
    caps: dict,
    model_id: str,
    product_mode: str,
    product_kind: str,
    explicit_products: list[str],
) -> list[str]:
    model = _model_entry(caps, model_id) or {}
    groups = {
        "direct": list(model.get("direct_recipes") or []),
        "light_derived": list(model.get("light_derived_recipes") or []),
        "heavy_derived": list(model.get("heavy_derived_recipes") or []),
        "windowed": list(model.get("windowed_products") or []),
    }
    if product_mode == "selected" and explicit_products:
        return _dedupe(explicit_products)
    if product_mode == "kind":
        if product_kind and product_kind in groups:
            return _dedupe(groups[product_kind])
        return _dedupe([product for products in groups.values() for product in products])
    if product_mode == "all":
        return _dedupe([product for products in groups.values() for product in products])
    default = str(model.get("default_render_product") or model.get("default_product") or "").strip()
    if default:
        return [default]
    return _dedupe([product for products in groups.values() for product in products])[:1]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result = []
    for value in values:
        if value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _wxstore_product_rows(manifest: dict, *, member: str = "control") -> list[dict]:
    rows = []
    seen: set[str] = set()
    for entry in manifest.get("products") or []:
        if not isinstance(entry, dict):
            continue
        entry_member = str(entry.get("member") or "")
        if member and entry_member and entry_member != member:
            continue
        slug = str(entry.get("product") or "").strip()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        hours = [int(hour) for hour in entry.get("forecast_hours") or []]
        grid = entry.get("grid") if isinstance(entry.get("grid"), dict) else {}
        units = str(entry.get("units") or "")
        rows.append({
            "slug": slug,
            "label": slug.replace("_", " "),
            "kind": units or str(entry.get("format") or "wxa"),
            "member": entry_member or member,
            "units": units,
            "format": entry.get("format"),
            "hours": hours,
            "bytes": entry.get("bytes"),
            "nx": entry.get("nx"),
            "ny": entry.get("ny"),
            "bounds": grid.get("bounds"),
            "path": entry.get("path"),
        })
    return rows


def _wxstore_first_product_bounds(manifest: dict, products: list[str]) -> list[float] | None:
    wanted = set(products)
    for entry in manifest.get("products") or []:
        if not isinstance(entry, dict) or str(entry.get("product") or "") not in wanted:
            continue
        grid = entry.get("grid") if isinstance(entry.get("grid"), dict) else {}
        raw = grid.get("bounds")
        if not isinstance(raw, (list, tuple)) or len(raw) != 4:
            continue
        west, second, third, north = [float(value) for value in raw]
        if -90.0 <= second <= 90.0 and -180.0 <= third <= 180.0:
            return [west, third, second, north]
        return [west, second, third, north]
    return None


def _wxstore_base_url(value: object) -> str:
    text = str(value or "").strip() or os.environ.get("RUSTWX_STUDIO_WXSTORE_URL") or DEFAULT_WXSTORE_URL
    return text.rstrip("/")


def _wxstore_http_json(
    base_url: str,
    path: str,
    *,
    params: dict[str, object] | None = None,
    timeout: float = 20.0,
) -> dict:
    query = _clean_query_params(params or {})
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if query:
        url = f"{url}?{urlencode(query)}"
    started = time.time()
    try:
        request = Request(url, headers={"accept": "application/json"})
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
            text = body.decode("utf-8", errors="replace")
            ctype = response.headers.get("content-type", "")
            payload: object
            if "json" in ctype.lower() or text.lstrip().startswith(("{", "[")):
                try:
                    payload = json.loads(text)
                except Exception:
                    payload = text
            else:
                payload = text
            return {
                "ok": 200 <= int(response.status) < 300,
                "status_code": int(response.status),
                "elapsed_s": round(time.time() - started, 3),
                "url": url,
                "payload": payload,
            }
    except Exception as exc:
        return {
            "ok": False,
            "elapsed_s": round(time.time() - started, 3),
            "url": url,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _clean_query_params(params: dict[str, object]) -> dict[str, str]:
    cleaned = {}
    for key, value in params.items():
        if value in (None, ""):
            continue
        if isinstance(value, bool):
            cleaned[key] = "true" if value else "false"
        elif isinstance(value, (list, tuple, set)):
            joined = ",".join(str(item).strip() for item in value if str(item).strip())
            if joined:
                cleaned[key] = joined
        else:
            cleaned[key] = str(value)
    return cleaned


def _wxstore_service_request(action: str, payload: dict) -> tuple[str | None, dict[str, object]]:
    model = str(payload.get("model") or "hrrr")
    run = str(payload.get("run") or "latest")
    member = str(payload.get("member") or "").strip() or None
    variable = str(payload.get("variable") or "").strip()
    lat = payload.get("lat")
    lon = payload.get("lon")
    hours = str(payload.get("hours") or payload.get("forecast_hours") or "").strip()
    endpoint = str(payload.get("endpoint") or "").strip().lower().replace("-", "_")
    if action == "catalog":
        if endpoint in {"", "models"}:
            return "/v1/models", {}
        if endpoint == "status":
            return "/v1/status", {}
        if endpoint == "products":
            return "/v1/products", {}
        if endpoint == "variables":
            return "/v1/variables", {"model": model, "run": run}
        if endpoint == "layers":
            return "/v1/layers", {"model": model, "run": run, "variable": variable or None}
        if endpoint == "static_plots":
            return "/v1/static-plots", {"model": model, "run": run}
        if endpoint == "observation_sources":
            return "/v1/observations/sources", {}
        if endpoint == "satellite_layers":
            return "/v1/satellite/layers", {}
        if endpoint == "radar_layers":
            return "/v1/radar/layers", {}
        if endpoint == "mapbox_layers":
            return f"/v1/mapbox/layers/{model}/{run}/{variable or 'temperature_2m'}", {}
        return None, {}
    if action == "forecast":
        variables = str(payload.get("variables") or "").strip() or "temperature_2m,dew_point_2m,wind_gusts_10m"
        return "/v1/forecast", {
            "lat": lat,
            "lon": lon,
            "model": model,
            "run": run,
            "member": member,
            "hourly": variables,
            "forecast_hours": hours or None,
        }
    if action == "sample":
        return "/v1/sample", {
            "lat": lat,
            "lon": lon,
            "model": model,
            "run": run,
            "member": member,
            "variable": variable or "temperature_2m",
            "forecast_hour": payload.get("forecast_hour") or 0,
        }
    if action == "temporal_sounding":
        return "/v1/temporal-sounding", {
            "lat": lat,
            "lon": lon,
            "hours": hours or "0-48",
            "diagnostics": payload.get("diagnostics") or "basic",
            "variables": payload.get("variables") or None,
        }
    if action == "objects":
        return "/v1/objects", {
            "kind": payload.get("kind") or None,
            "q": payload.get("q") or None,
            "bbox": payload.get("bbox") or None,
            "lat": lat,
            "lon": lon,
            "radius_km": payload.get("radius_km") or None,
            "network": payload.get("network") or None,
            "parameter": payload.get("parameter") or None,
            "quality_tier": payload.get("quality_tier") or None,
            "limit": payload.get("limit") or 50,
        }
    if action == "mesoanalysis":
        if endpoint in {"", "status"}:
            return "/v1/mesoanalysis/innovation/status", {}
        if endpoint == "query":
            return "/v1/mesoanalysis/innovation/query", {
                "kind": payload.get("kind") or None,
                "station": payload.get("station") or None,
                "source": payload.get("source_id") or None,
                "variable": variable or None,
                "q": payload.get("q") or None,
            }
        if endpoint == "watchlist":
            return "/v1/mesoanalysis/innovation/watchlist", {
                "kind": payload.get("kind") or None,
                "top": payload.get("limit") or 20,
                "variable": variable or None,
            }
        return None, {}
    return None, {}


def _job_kinds() -> set[str]:
    return {
        "render",
        "prepare_data",
        "satellite",
        "satellite_sequence",
        "satellite_tile_loop",
        "generation_plan",
        "case_dataset",
        "radar",
        "radar_tiles",
        "radar_tile_loop",
        "meteogram",
        "meteogram_store",
        "sounding",
        "pressure_store",
        "cross_section",
        "wxstore",
        "wxstore_plot_existing",
        "ecape_profile",
        "ecape_grid",
        "ecape_ratio",
        "native_dataset_plan",
        "native_dataset_run",
        "native_obs_preview",
    }


def _iso_from_ts(value: float) -> str:
    return datetime.fromtimestamp(value, UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _job_snapshot(job: dict, *, detail: bool) -> dict:
    started = job.get("started_at_ts")
    finished = job.get("finished_at_ts")
    now = finished or time.time()
    elapsed = None
    if started:
        elapsed = round(float(now) - float(started), 2)
    result = job.get("result") if isinstance(job.get("result"), dict) else None
    snapshot = {
        "id": job.get("id"),
        "kind": job.get("kind"),
        "status": job.get("status"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "elapsed_s": elapsed,
        "request": job.get("request"),
        "error": job.get("error"),
        "cancel_requested": bool(job.get("cancel_requested")),
        "active_process_pid": job.get("active_process_pid"),
        "active_command": job.get("active_command"),
        "progress": job.get("progress"),
    }
    if job.get("log"):
        snapshot["log"] = list(job.get("log") or [])[-12:]
    if result:
        snapshot["ok"] = result.get("ok")
        snapshot["preview_count"] = len(result.get("previews") or [])
        snapshot["requested_products"] = result.get("requested_products")
        snapshot["result_elapsed_s"] = result.get("ui_elapsed_s")
    if detail:
        snapshot["result"] = result
        snapshot["traceback"] = job.get("traceback")
    return snapshot


def _trim_jobs_locked(jobs: dict[str, dict], keep: int = 80) -> None:
    if len(jobs) <= keep:
        return
    removable = sorted(
        jobs.values(),
        key=lambda job: float(job.get("created_at_ts", 0.0)),
    )
    for job in removable[: max(0, len(jobs) - keep)]:
        if job.get("status") in {"completed", "failed", "cancelled"}:
            jobs.pop(str(job.get("id")), None)


def _job_context() -> tuple[StudioServer | None, str | None]:
    return getattr(_JOB_CONTEXT, "server", None), getattr(_JOB_CONTEXT, "job_id", None)


def _job_cancel_requested() -> bool:
    server, job_id = _job_context()
    if server is None or not job_id:
        return False
    with server.jobs_lock:
        job = server.jobs.get(job_id)
        return bool(job and job.get("cancel_requested"))


def _raise_if_job_cancelled() -> None:
    if _job_cancel_requested():
        raise JobCancelled("Job cancelled.")


def _set_job_process(proc: subprocess.Popen | None, cmd: list[str] | None = None) -> None:
    server, job_id = _job_context()
    if server is None or not job_id:
        return
    with server.jobs_lock:
        job = server.jobs.get(job_id)
        if not job:
            return
        job["_active_process"] = proc
        job["active_process_pid"] = proc.pid if proc else None
        job["active_command"] = cmd if proc and cmd else None
        job["updated_at_ts"] = time.time()
        job["updated_at"] = _iso_from_ts(float(job["updated_at_ts"]))


def _clear_job_process() -> None:
    _set_job_process(None, None)


def _job_request_summary(kind: str, payload: dict) -> dict:
    keys = [
        "model",
        "source",
        "run",
        "run_str",
        "member",
        "domain",
        "forecast_hour",
        "forecast_hours",
        "hours",
        "sequence_count",
        "latest_count",
        "models_mode",
        "product_mode",
        "product_kind",
        "site",
        "lat",
        "lon",
        "store_id",
        "data_mode",
        "satellite",
        "sector",
        "route_id",
        "action",
        "mode",
        "start_date",
        "end_date",
        "cycles",
        "limit",
        "parcel",
        "plan_path",
        "dataset_name",
        "case",
        "tile_grid",
        "input",
        "use_domain_bounds",
        "layer",
        "min_zoom",
        "max_zoom",
    ]
    summary = {key: payload.get(key) for key in keys if key in payload}
    products = payload.get("products")
    if isinstance(products, list):
        summary["product_count"] = len(products)
        summary["products"] = products[:8]
        if len(products) > 8:
            summary["products"].append(f"+{len(products) - 8} more")
    return {"kind": kind, **summary}


def _radar_sites_response() -> dict:
    sites = conus_radar_sites()
    return {
        "ok": True,
        "count": len(sites),
        "sites": sites,
        "geojson": radar_sites_geojson(conus=True),
    }


def _radar_basemap_response(out_root: Path) -> dict:
    try:
        asset_dir = out_root / "studio" / "assets"
        svg_path = asset_dir / "radar_conus_basemap.svg"
        meta_path = asset_dir / "radar_conus_basemap.json"
        if not svg_path.exists() or not meta_path.exists():
            _write_radar_basemap_assets(svg_path, meta_path)
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return {
            "ok": True,
            "url": f"/api/file?path={quote(str(svg_path.resolve()))}",
            **meta,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "width": RADAR_BASEMAP_WIDTH,
            "height": RADAR_BASEMAP_HEIGHT,
            "bounds": list(RADAR_BASEMAP_BOUNDS),
            "sites": _fallback_radar_site_pixels(),
        }


def _radar_latest_objects(site: str, count: int) -> list[dict]:
    today = datetime.now(UTC).date()
    objects = []
    for date in [today - timedelta(days=1), today]:
        objects.extend(_radar_list_day(site, date))
    objects.sort(key=lambda item: str(item.get("key") or ""))
    return objects[-count:]


def _radar_list_day(site: str, date: object) -> list[dict]:
    prefix = f"{date.year:04d}/{date.month:02d}/{date.day:02d}/{site.upper()}/"
    url = f"{NEXRAD_LEVEL2_BASE_URL}?list-type=2&prefix={quote(prefix)}"
    request = Request(url, headers={"accept": "application/xml"})
    with urlopen(request, timeout=30.0) as response:
        text = response.read().decode("utf-8", errors="replace")
    root = ElementTree.fromstring(text)
    objects = []
    for contents in root.iter():
        if _xml_local_name(contents.tag) != "Contents":
            continue
        key = _xml_child_text(contents, "Key")
        display_name = key.rsplit("/", 1)[-1] if key else ""
        if not key or display_name.endswith("_MDM") or display_name.endswith(".md"):
            continue
        objects.append({
            "key": key,
            "display_name": display_name,
            "size": int(_xml_child_text(contents, "Size") or 0),
            "last_modified": _xml_child_text(contents, "LastModified"),
        })
    return objects


def _download_radar_object_to_cache(cache_dir: Path, item: dict, *, timeout: float) -> tuple[Path, bool]:
    key = str(item.get("key") or "")
    if not key:
        raise ValueError("radar object is missing a key")
    path = _radar_level2_cache_path(cache_dir, key)
    if path.is_file():
        return path, True
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"{NEXRAD_LEVEL2_BASE_URL}/{quote(key)}"
    request = Request(url, headers={"accept": "application/octet-stream"})
    with urlopen(request, timeout=timeout) as response:
        data = response.read()
    if len(data) >= 2 and data[0] == 0x1f and data[1] == 0x8b:
        data = gzip.decompress(data)
    path.write_bytes(data)
    return path, False


def _radar_level2_cache_path(cache_dir: Path, key: str) -> Path:
    safe_key = "".join("_" if char in {"/", "\\", ":"} else char for char in key)
    return cache_dir / "radar_level2" / safe_key


def _radar_frame_id(item: dict) -> tuple[str, str]:
    name = str(item.get("display_name") or item.get("key") or "")
    for index in range(max(0, len(name) - 14)):
        date = name[index:index + 8]
        sep = name[index + 8:index + 9]
        time_part = name[index + 9:index + 15]
        if sep != "_" or not date.isdigit() or not time_part.isdigit():
            continue
        try:
            dt = datetime(
                int(date[0:4]),
                int(date[4:6]),
                int(date[6:8]),
                int(time_part[0:2]),
                int(time_part[2:4]),
                int(time_part[4:6]),
                tzinfo=UTC,
            )
        except ValueError:
            continue
        return dt.strftime("%Y%m%dT%H%M%SZ"), dt.isoformat(timespec="seconds").replace("+00:00", "Z")
    text = str(item.get("last_modified") or "").replace(".000Z", "Z")
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
        return dt.strftime("%Y%m%dT%H%M%SZ"), dt.isoformat(timespec="seconds").replace("+00:00", "Z")
    except ValueError:
        fallback = _safe_slug(name or time.time())
        return fallback, ""


def _radar_loop_frame_record(layer_id: str, frame_id: str, scan_iso: str, manifest: dict) -> dict:
    if isinstance(manifest.get("manifests"), list):
        first = manifest["manifests"][0] if manifest["manifests"] else {}
        frame = _radar_loop_frame_base(layer_id, frame_id, scan_iso, first)
        frame["tile_count"] = manifest.get("total_tile_count")
        frame["candidate_tile_count"] = manifest.get("total_candidate_tile_count")
        frame["tilts"] = [
            _radar_loop_tilt_record(layer_id, frame_id, tilt_manifest)
            for tilt_manifest in manifest.get("manifests") or []
        ]
        return frame
    return _radar_loop_frame_base(layer_id, frame_id, scan_iso, manifest)


def _satellite_channel_local_path(frame: dict, channel: int) -> Path | None:
    channels = frame.get("channel_files")
    if not isinstance(channels, dict):
        return None
    keys = [
        str(channel),
        f"{channel:02d}",
        f"c{channel:02d}",
        f"C{channel:02d}",
        f"channel{channel}",
    ]
    for key in keys:
        info = channels.get(key)
        if isinstance(info, dict):
            raw = info.get("local_path") or info.get("path")
        else:
            raw = info
        if not raw:
            continue
        path = Path(str(raw))
        if path.is_file():
            return path
    return None


def _satellite_frame_id(frame: dict, *, fallback_index: int = 0) -> str:
    raw = str(frame.get("scan_time_utc") or frame.get("scan_id") or "").strip()
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        except Exception:
            pass
        safe = _safe_slug(raw).upper()
        if safe:
            return safe
    return f"FRAME_{fallback_index:03d}"


def _satellite_loop_frame_record(
    layer_id: str,
    frame_id: str,
    manifest: dict,
    *,
    source_frame: dict,
    layer_mode: str,
) -> dict:
    scan_iso = manifest.get("scan_time_utc") or source_frame.get("scan_time_utc") or frame_id
    record = {
        "id": frame_id,
        "layer": layer_id,
        "label": scan_iso,
        "satellite": source_frame.get("satellite"),
        "product": source_frame.get("product") or "goes_geocolor",
        "tile_layer": layer_mode,
        "scan_id": source_frame.get("scan_id"),
        "scan_time_utc": scan_iso,
        "scan_end_time_utc": source_frame.get("scan_end_time_utc"),
        "url_template": f"{layer_id}/frames/{frame_id}/{{z}}/{{x}}/{{y}}.png",
        "bounds": manifest.get("bounds") or source_frame.get("bounds"),
        "domain": source_frame.get("domain"),
        "minzoom": manifest.get("minzoom"),
        "maxzoom": manifest.get("maxzoom"),
        "tile_size": manifest.get("tile_size"),
        "opacity": manifest.get("opacity"),
        "tile_count": manifest.get("tile_count"),
        "skipped_empty_tiles": manifest.get("skipped_empty_tiles"),
        "total_ms": manifest.get("total_ms"),
        "source_files": manifest.get("source_files"),
        "source_keys": source_frame.get("source_keys"),
        "source_png": source_frame.get("png_path"),
    }
    return {key: value for key, value in record.items() if value not in (None, [])}


def _write_satellite_tile_loop_viewer(path: Path, frames_json: dict, satellite_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    layer_id = str(frames_json.get("layer") or "satellite")
    frames = frames_json.get("frames") if isinstance(frames_json.get("frames"), list) else []
    viewer_frames = _satellite_loop_viewer_frames(frames, satellite_root, layer_id)
    first = viewer_frames[0] if viewer_frames else {}
    bounds = first.get("bounds") or frames_json.get("bounds") or [-126.0, 24.0, -66.0, 50.0]
    west, south, east, north = _coerce_tile_bounds(bounds)
    title = f"{frames_json.get('satellite', 'GOES')} {frames_json.get('tile_layer', 'satellite')}"
    delay_ms = int(frames_json.get("delay_ms") or 700)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RustWx Satellite Tiles</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body, #map {{ height: 100%; margin: 0; }}
    .panel {{ position: absolute; z-index: 900; left: 10px; right: 10px; bottom: 10px; display: flex; gap: 8px; align-items: center; background: rgba(255,255,255,.93); padding: 8px 10px; border: 1px solid #b8c6cc; border-radius: 6px; font: 13px system-ui, sans-serif; }}
    .panel strong {{ min-width: 150px; }}
    .panel input[type=range] {{ flex: 1; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="panel"><strong>{title}</strong><button id="play">Play</button><input id="frame" type="range" min="0" max="{max(0, len(viewer_frames) - 1)}" value="{max(0, len(viewer_frames) - 1)}"><span id="label"></span></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const frames = {json.dumps(viewer_frames, default=str)};
    const map = L.map('map').fitBounds([[{south}, {west}], [{north}, {east}]]);
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom: 12, attribution: '&copy; OpenStreetMap' }}).addTo(map);
    let layer = null;
    let timer = null;
    const slider = document.getElementById('frame');
    const label = document.getElementById('label');
    function show(index) {{
      const frame = frames[index];
      if (!frame) return;
      if (layer) map.removeLayer(layer);
      layer = L.tileLayer(frame.template, {{
        minZoom: Number(frame.minzoom || 2),
        maxZoom: Number(frame.maxzoom || 10),
        opacity: Number(frame.opacity || 0.92),
        tileSize: Number(frame.tile_size || 256),
        errorTileUrl: ''
      }}).addTo(map);
      slider.value = String(index);
      label.textContent = frame.label || frame.id || '';
    }}
    slider.addEventListener('input', () => show(Number(slider.value || 0)));
    document.getElementById('play').addEventListener('click', (event) => {{
      if (timer) {{
        clearInterval(timer);
        timer = null;
        event.currentTarget.textContent = 'Play';
        return;
      }}
      event.currentTarget.textContent = 'Pause';
      timer = setInterval(() => show((Number(slider.value || 0) + 1) % Math.max(1, frames.length)), {delay_ms});
    }});
    show(frames.length ? frames.length - 1 : 0);
  </script>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def _satellite_loop_viewer_frames(frames: list[dict], satellite_root: Path, layer_id: str) -> list[dict]:
    rows = []
    for frame in frames:
        frame_id = str(frame.get("id") or "")
        if not frame_id:
            continue
        tile_root = satellite_root / layer_id / "frames" / frame_id
        root_url = f"/api/file?path={quote(str(tile_root.resolve()).replace(os.sep, '/'))}"
        rows.append({
            "id": frame_id,
            "label": frame.get("scan_time_utc") or frame_id,
            "template": f"{root_url}/{{z}}/{{x}}/{{y}}.png",
            "bounds": frame.get("bounds"),
            "minzoom": frame.get("minzoom"),
            "maxzoom": frame.get("maxzoom"),
            "tile_size": frame.get("tile_size") or 256,
            "opacity": frame.get("opacity") or 0.92,
        })
    return rows


def _radar_loop_frame_base(layer_id: str, frame_id: str, scan_iso: str, manifest: dict) -> dict:
    record = {
        "id": frame_id,
        "layer": layer_id,
        "label": manifest.get("scan_time_utc") or scan_iso or frame_id,
        "site": _radar_manifest_site_id(manifest),
        "product": manifest.get("product"),
        "product_name": manifest.get("product_name"),
        "product_provenance": manifest.get("product_provenance"),
        "source_key_or_url": manifest.get("source_key_or_url"),
        "scan_time_utc": manifest.get("scan_time_utc") or scan_iso,
        "url_template": f"{layer_id}/frames/{frame_id}/{{z}}/{{x}}/{{y}}.png",
        "bounds": manifest.get("bounds"),
        "clip_to_bounds": manifest.get("clip_to_bounds"),
        "sampling_bounds": manifest.get("sampling_bounds"),
        "minzoom": manifest.get("minzoom"),
        "maxzoom": manifest.get("maxzoom"),
        "tile_size": manifest.get("tile_size"),
        "tile_count": manifest.get("tile_count"),
        "candidate_tile_count": manifest.get("candidate_tile_count"),
        "native_gate_size_m": manifest.get("native_gate_size_m"),
        "native_azimuth_spacing_deg": manifest.get("native_azimuth_spacing_deg"),
        "maxzoom_site_meters_per_pixel": manifest.get("maxzoom_site_meters_per_pixel"),
        "velocity_quality_filter": manifest.get("velocity_quality_filter"),
        "velocity_quality_qc": manifest.get("velocity_quality_qc"),
        "reflectivity_despeckle": manifest.get("reflectivity_despeckle"),
        "reflectivity_qc": manifest.get("reflectivity_qc"),
        "numeric_sidecar": manifest.get("numeric_sidecar"),
    }
    return {key: value for key, value in record.items() if value not in (None, [])}


def _radar_loop_tilt_record(layer_id: str, frame_id: str, manifest: dict) -> dict:
    out_dir = Path(str(manifest.get("out_dir") or ""))
    tilt_id = out_dir.name if out_dir.name else f"sweep{int(manifest.get('sweep_index') or 0):02d}"
    record = _radar_loop_frame_base(layer_id, frame_id, str(manifest.get("scan_time_utc") or ""), manifest)
    record["id"] = tilt_id
    record["name"] = tilt_id
    record["sweep_index"] = manifest.get("sweep_index")
    record["elevation_deg"] = manifest.get("elevation_deg")
    record["url_template"] = f"{layer_id}/frames/{frame_id}/{tilt_id}/{{z}}/{{x}}/{{y}}.png"
    return record


def _radar_manifest_site_id(manifest: dict) -> str | None:
    site = manifest.get("site")
    if isinstance(site, dict):
        return site.get("id")
    return site


def _write_radar_tile_loop_viewer(path: Path, frames_json: dict, radar_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    layer_id = str(frames_json.get("layer") or "radar")
    frames = frames_json.get("frames") if isinstance(frames_json.get("frames"), list) else []
    viewer_frames = _radar_loop_viewer_frames(frames, radar_root, layer_id)
    first = viewer_frames[0] if viewer_frames else {}
    bounds = first.get("bounds") or frames_json.get("bounds") or [-100.5, 33.5, -95.0, 37.5]
    west, south, east, north = _coerce_tile_bounds(bounds)
    title = f"{frames_json.get('site', 'Radar')} {frames_json.get('product', 'loop')}"
    delay_ms = int(frames_json.get("delay_ms") or 650)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RustWx Radar Loop</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body, #map {{ height: 100%; margin: 0; }}
    .panel {{ position: absolute; z-index: 900; left: 10px; right: 10px; bottom: 10px; display: flex; gap: 8px; align-items: center; background: rgba(255,255,255,.93); padding: 8px 10px; border: 1px solid #b8c6cc; border-radius: 6px; font: 13px system-ui, sans-serif; }}
    .panel strong {{ min-width: 120px; }}
    .panel input[type=range] {{ flex: 1; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="panel"><strong>{title}</strong><button id="play">Play</button><input id="frame" type="range" min="0" max="{max(0, len(viewer_frames) - 1)}" value="{max(0, len(viewer_frames) - 1)}"><span id="label"></span></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const frames = {json.dumps(viewer_frames, default=str)};
    const map = L.map('map').fitBounds([[{south}, {west}], [{north}, {east}]]);
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom: 12, attribution: '&copy; OpenStreetMap' }}).addTo(map);
    let layer = null;
    let timer = null;
    const slider = document.getElementById('frame');
    const label = document.getElementById('label');
    function show(index) {{
      const frame = frames[index];
      if (!frame) return;
      if (layer) map.removeLayer(layer);
      layer = L.tileLayer(frame.template, {{
        minZoom: Number(frame.minzoom || 2),
        maxZoom: Number(frame.maxzoom || 10),
        opacity: 0.95,
        tileSize: Number(frame.tile_size || 256),
        errorTileUrl: ''
      }}).addTo(map);
      slider.value = String(index);
      label.textContent = frame.label || frame.id || '';
    }}
    slider.addEventListener('input', () => show(Number(slider.value || 0)));
    document.getElementById('play').addEventListener('click', (event) => {{
      if (timer) {{
        clearInterval(timer);
        timer = null;
        event.currentTarget.textContent = 'Play';
        return;
      }}
      event.currentTarget.textContent = 'Pause';
      timer = setInterval(() => show((Number(slider.value || 0) + 1) % Math.max(1, frames.length)), {delay_ms});
    }});
    show(frames.length ? frames.length - 1 : 0);
  </script>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def _radar_loop_viewer_frames(frames: list[dict], radar_root: Path, layer_id: str) -> list[dict]:
    rows = []
    for frame in frames:
        frame_id = str(frame.get("id") or "")
        if not frame_id:
            continue
        tile_root = radar_root / layer_id / "frames" / frame_id
        display = frame
        if frame.get("tilts"):
            tilt = frame["tilts"][0]
            tile_root = tile_root / str(tilt.get("id") or "")
            display = tilt
        root_url = f"/api/file?path={quote(str(tile_root.resolve()).replace(os.sep, '/'))}"
        rows.append({
            "id": frame_id,
            "label": frame.get("scan_time_utc") or frame_id,
            "template": f"{root_url}/{{z}}/{{x}}/{{y}}.png",
            "bounds": display.get("bounds") or frame.get("bounds"),
            "minzoom": display.get("minzoom") or frame.get("minzoom"),
            "maxzoom": display.get("maxzoom") or frame.get("maxzoom"),
            "tile_size": display.get("tile_size") or frame.get("tile_size") or 256,
        })
    return rows


def _coerce_tile_bounds(bounds: object) -> tuple[float, float, float, float]:
    values = list(bounds) if isinstance(bounds, (list, tuple)) and len(bounds) == 4 else [-100.5, 33.5, -95.0, 37.5]
    west, second, third, north = [float(value) for value in values]
    if -90.0 <= second <= 90.0 and -180.0 <= third <= 180.0:
        return west, second, third, north
    return west, third, second, north


def _xml_local_name(tag: str) -> str:
    return str(tag).rsplit("}", 1)[-1]


def _xml_child_text(parent: ElementTree.Element, name: str) -> str:
    for child in parent:
        if _xml_local_name(child.tag) == name:
            return child.text or ""
    return ""


def _radar_tile_manifest_summary(manifest: dict) -> dict:
    if not isinstance(manifest, dict):
        return {}
    if manifest.get("manifests"):
        return {
            "ok": manifest.get("ok"),
            "site": manifest.get("site"),
            "product": manifest.get("product"),
            "scan_time_utc": manifest.get("scan_time_utc"),
            "tilt_count": manifest.get("tilt_count"),
            "tile_count": manifest.get("total_tile_count"),
            "candidate_tile_count": manifest.get("total_candidate_tile_count"),
            "total_ms": manifest.get("total_ms"),
        }
    return {
        "ok": manifest.get("ok"),
        "name": manifest.get("name"),
        "site": (manifest.get("site") or {}).get("id") if isinstance(manifest.get("site"), dict) else manifest.get("site"),
        "product": manifest.get("product"),
        "product_name": manifest.get("product_name"),
        "scan_time_utc": manifest.get("scan_time_utc"),
        "sweep_index": manifest.get("sweep_index"),
        "elevation_deg": manifest.get("elevation_deg"),
        "minzoom": manifest.get("minzoom"),
        "maxzoom": manifest.get("maxzoom"),
        "tile_count": manifest.get("tile_count"),
        "candidate_tile_count": manifest.get("candidate_tile_count"),
        "tiles_per_second": manifest.get("tiles_per_second"),
        "total_ms": manifest.get("total_ms"),
        "numeric_sidecar": bool(manifest.get("numeric_sidecar")),
    }


def _write_radar_tile_viewer(path: Path, manifest: dict, tile_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    layer_manifests = manifest.get("manifests") if isinstance(manifest.get("manifests"), list) else [manifest]
    first = layer_manifests[0] if layer_manifests else manifest
    bounds = first.get("bounds") or first.get("sampling_bounds") or [-100.5, 33.5, -95.0, 37.5]
    if len(bounds) == 4:
        west, south, east, north = [float(value) for value in bounds]
    else:
        west, south, east, north = -100.5, 33.5, -95.0, 37.5
    root_url = f"/api/file?path={quote(str(tile_root.resolve()).replace(os.sep, '/'))}"
    title = f"{first.get('site', {}).get('id', manifest.get('site', 'Radar')) if isinstance(first.get('site'), dict) else manifest.get('site', 'Radar')} {first.get('product', manifest.get('product', 'radar'))}"
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RustWx Radar Tiles</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body, #map {{ height: 100%; margin: 0; }}
    .panel {{ position: absolute; z-index: 900; top: 10px; left: 10px; background: rgba(255,255,255,.92); padding: 8px 10px; border: 1px solid #b8c6cc; border-radius: 6px; font: 13px system-ui, sans-serif; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="panel"><strong>{title}</strong><br>{first.get('scan_time_utc', '')}<br>{len(layer_manifests)} layer(s)</div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const map = L.map('map').fitBounds([[{south}, {west}], [{north}, {east}]]);
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom: 12, attribution: '&copy; OpenStreetMap' }}).addTo(map);
    const layers = {json.dumps(_radar_viewer_layers(layer_manifests, root_url), default=str)};
    for (const layer of layers) {{
      L.tileLayer(layer.template, {{
        minZoom: Number(layer.minzoom || 2),
        maxZoom: Number(layer.maxzoom || 10),
        opacity: Number(layer.opacity || 0.95),
        tileSize: Number(layer.tile_size || 256),
        errorTileUrl: ''
      }}).addTo(map);
    }}
  </script>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def _radar_viewer_layers(manifests: list[dict], root_url: str) -> list[dict]:
    layers = []
    for item in manifests:
        out_dir = str(item.get("out_dir") or "").replace("\\", "/")
        suffix = ""
        if out_dir:
            root_path = root_url.split("path=", 1)[-1]
            decoded_root = unquote(root_path).rstrip("/")
            if out_dir.startswith(decoded_root):
                suffix = out_dir[len(decoded_root):].strip("/")
        template_root = f"{root_url}/{suffix}" if suffix else root_url
        layers.append({
            "template": f"{template_root}/{{z}}/{{x}}/{{y}}.png",
            "minzoom": item.get("minzoom"),
            "maxzoom": item.get("maxzoom"),
            "opacity": item.get("opacity", 0.95),
            "tile_size": item.get("tile_size", 256),
        })
    return layers


def _write_radar_basemap_assets(svg_path: Path, meta_path: Path) -> None:
    import numpy as np

    west, east, south, north = RADAR_BASEMAP_BOUNDS
    ny, nx = 24, 36
    lat = np.linspace(south, north, ny)[:, None] * np.ones((ny, nx))
    lon = np.ones((ny, nx)) * np.linspace(west, east, nx)[None, :]
    spec = {
        "projection": RADAR_BASEMAP_PROJECTION,
        "width": RADAR_BASEMAP_WIDTH,
        "height": RADAR_BASEMAP_HEIGHT,
        "colorbar": False,
        "basemap_style": "filled",
        "domain_frame": True,
        "visual_mode": "filled_meteorology",
    }
    overlays = _load_json(
        rustwx.build_projected_basemap_overlays_json(
            json.dumps(spec),
            lat,
            lon,
            include_geometry=True,
        )
    )
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    svg_path.write_text(_radar_basemap_svg(overlays), encoding="utf-8")
    meta = {
        "width": RADAR_BASEMAP_WIDTH,
        "height": RADAR_BASEMAP_HEIGHT,
        "bounds": list(RADAR_BASEMAP_BOUNDS),
        "projection": RADAR_BASEMAP_PROJECTION,
        "sites": _projected_radar_site_pixels(overlays),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _radar_basemap_svg(overlays: dict) -> str:
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {RADAR_BASEMAP_WIDTH} {RADAR_BASEMAP_HEIGHT}">',
        '<rect width="100%" height="100%" fill="#e9f0f3"/>',
    ]
    for polygon in overlays.get("polygon_fills") or []:
        path = _polygon_path(polygon.get("rings") or [], overlays, max_points=450)
        if not path:
            continue
        color = _svg_color(polygon.get("color") or {})
        parts.append(f'<path d="{path}" fill="{color[0]}" fill-opacity="{color[1]:.3f}" fill-rule="evenodd" stroke="none"/>')
    for line in overlays.get("line_overlays") or []:
        role = str(line.get("role") or "")
        if role == "county":
            continue
        points = _polyline_points(line.get("points") or [], overlays, max_points=520)
        if len(points) < 2:
            continue
        color = _svg_color(line.get("color") or {})
        width = max(1, int(line.get("width") or 1))
        opacity = 0.85 if role in {"state", "international", "coast"} else 0.55
        parts.append(
            f'<polyline points="{" ".join(points)}" fill="none" stroke="{color[0]}" '
            f'stroke-opacity="{min(color[1], opacity):.3f}" stroke-width="{width}" '
            'stroke-linejoin="round" stroke-linecap="round"/>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def _projected_radar_site_pixels(overlays: dict) -> list[dict[str, object]]:
    sites = []
    for site in conus_radar_sites():
        x, y = _lambert_project(
            float(site["lat"]),
            float(site["lon"]),
            RADAR_BASEMAP_PROJECTION,
        )
        px, py = _projected_to_pixel(x, y, overlays)
        site_payload = dict(site)
        site_payload["x"] = round(px, 2)
        site_payload["y"] = round(py, 2)
        sites.append(site_payload)
    return sites


def _fallback_radar_site_pixels() -> list[dict[str, object]]:
    west, east, south, north = RADAR_BASEMAP_BOUNDS
    sites = []
    for site in conus_radar_sites():
        site_payload = dict(site)
        site_payload["x"] = round(((float(site["lon"]) - west) / (east - west)) * RADAR_BASEMAP_WIDTH, 2)
        site_payload["y"] = round(((north - float(site["lat"])) / (north - south)) * RADAR_BASEMAP_HEIGHT, 2)
        sites.append(site_payload)
    return sites


def _polygon_path(rings: list, overlays: dict, *, max_points: int) -> str:
    commands = []
    for ring in rings:
        points = _polyline_points(ring, overlays, max_points=max_points)
        if len(points) >= 3:
            commands.append("M" + " L".join(points) + " Z")
    return " ".join(commands)


def _polyline_points(points: list, overlays: dict, *, max_points: int) -> list[str]:
    sampled = _sample_points(points, max_points=max_points)
    projected = []
    for point in sampled:
        if len(point) < 2:
            continue
        if point[0] is None or point[1] is None:
            continue
        x, y = _projected_to_pixel(float(point[0]), float(point[1]), overlays)
        if math.isfinite(x) and math.isfinite(y):
            projected.append(f"{x:.1f},{y:.1f}")
    return projected


def _sample_points(points: list, *, max_points: int) -> list:
    if len(points) <= max_points:
        return points
    stride = max(1, math.ceil(len(points) / max_points))
    sampled = points[::stride]
    if sampled[-1] != points[-1]:
        sampled.append(points[-1])
    return sampled


def _projected_to_pixel(x: float, y: float, overlays: dict) -> tuple[float, float]:
    extent = overlays["extents"]["padded"]
    bounds = overlays["layout"]["pixel_bounds"]
    x_min = float(extent["x_min"])
    x_max = float(extent["x_max"])
    y_min = float(extent["y_min"])
    y_max = float(extent["y_max"])
    map_x = float(bounds["x_start"])
    map_y = float(bounds["y_start"])
    map_w = max(1.0, float(bounds["x_end"] - bounds["x_start"]))
    map_h = max(1.0, float(bounds["y_end"] - bounds["y_start"]))
    rx = (x - x_min) / max(1.0, x_max - x_min)
    ry = 1.0 - (y - y_min) / max(1.0, y_max - y_min)
    return map_x + rx * (map_w - 1.0), map_y + ry * (map_h - 1.0)


def _lambert_project(lat: float, lon: float, spec: dict) -> tuple[float, float]:
    earth_radius_m = 6_370_000.0
    phi1 = math.radians(_stabilize_reference_latitude(float(spec["truelat1"])))
    phi2 = math.radians(_stabilize_reference_latitude(float(spec["truelat2"])))
    phi0 = math.radians(_stabilize_reference_latitude(float(spec["cen_lat"])))
    if abs(float(spec["truelat1"]) - float(spec["truelat2"])) < 1.0e-10:
        n = math.sin(phi1)
    else:
        num = math.log(math.cos(phi1)) - math.log(math.cos(phi2))
        den = math.log(math.tan(math.pi / 4.0 + phi2 / 2.0)) - math.log(math.tan(math.pi / 4.0 + phi1 / 2.0))
        n = num / den
    if abs(n) < 1.0e-8:
        fallback = phi0 if abs(phi0) >= 1.0e-8 else (phi1 if abs(phi1) >= 1.0e-8 else math.radians(10.0))
        n = math.sin(fallback)
    f = math.cos(phi1) * math.tan(math.pi / 4.0 + phi1 / 2.0) ** n / n
    rho0 = earth_radius_m * f / math.tan(math.pi / 4.0 + phi0 / 2.0) ** n
    phi = math.radians(max(-89.999, min(89.999, lat)))
    delta_lon = math.radians(_normalize_longitude_deg(lon - float(spec["stand_lon"])))
    rho = earth_radius_m * f / math.tan(math.pi / 4.0 + phi / 2.0) ** n
    theta = n * delta_lon
    return rho * math.sin(theta), rho0 - rho * math.cos(theta)


def _stabilize_reference_latitude(lat: float) -> float:
    clamped = max(-85.0, min(85.0, lat))
    if abs(clamped) < 1.0:
        return -10.0 if clamped < 0.0 else 10.0
    return clamped


def _normalize_longitude_deg(lon: float) -> float:
    value = math.fmod(lon, 360.0)
    if value > 180.0:
        value -= 360.0
    elif value <= -180.0:
        value += 360.0
    return value


def _svg_color(color: dict) -> tuple[str, float]:
    r = int(color.get("r", 0))
    g = int(color.get("g", 0))
    b = int(color.get("b", 0))
    a = max(0.0, min(1.0, float(color.get("a", 255)) / 255.0))
    return f"rgb({r},{g},{b})", a


def _domain_by_slug(slug: str) -> dict | None:
    result = _load_json(rustwx.list_domains_json(kind=None, limit=None))
    for domain in result.get("domains") or []:
        if domain.get("slug") == slug:
            return domain
    return None


def _coerce_bounds(value: object, fallback: object) -> tuple[float, float, float, float]:
    raw = value if value not in (None, "", []) else fallback
    if isinstance(raw, str):
        parts = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
    elif isinstance(raw, (list, tuple)):
        parts = list(raw)
    else:
        parts = list(fallback) if isinstance(fallback, (list, tuple)) else [-125.0, -66.0, 24.0, 50.0]
    if len(parts) != 4:
        parts = list(fallback) if isinstance(fallback, (list, tuple)) else [-125.0, -66.0, 24.0, 50.0]
    west, east, south, north = [float(part) for part in parts]
    west, east = sorted((max(-180.0, west), min(180.0, east)))
    south, north = sorted((max(-90.0, south), min(90.0, north)))
    if east - west < 0.05:
        mid = (east + west) / 2.0
        west, east = mid - 0.025, mid + 0.025
    if north - south < 0.05:
        mid = (north + south) / 2.0
        south, north = mid - 0.025, mid + 0.025
    return west, east, south, north


def _point_in_bounds(lat: float, lon: float, bounds: tuple[float, float, float, float] | list[float]) -> bool:
    west, east, south, north = [float(value) for value in bounds]
    return south <= lat <= north and west <= lon <= east


def _pressure_store_complete(store_path: Path) -> bool:
    return (
        store_path.joinpath("manifest.json").is_file()
        and store_path.joinpath("index.bin").is_file()
        and store_path.joinpath("chunks.bin").is_file()
    )


def _wxprofile_store_complete(store_path: Path) -> bool:
    required = [
        "manifest.json",
        "profile_wx/TMP.wxp",
        "profile_wx/SPFH.wxp",
        "profile_wx/UGRD.wxp",
        "profile_wx/VGRD.wxp",
        "profile_wx/HGT.wxp",
        "surface_wx/LAT.f32",
        "surface_wx/LON.f32",
        "surface_wx/PSFC.f32",
        "surface_wx/OROG.f32",
        "surface_wx/T2.f32",
        "surface_wx/Q2.f32",
        "surface_wx/U10.f32",
        "surface_wx/V10.f32",
    ]
    return all(store_path.joinpath(part).is_file() for part in required)


def _safe_slug(value: object) -> str:
    text = str(value).strip().lower()
    cleaned = []
    for char in text:
        if char.isalnum():
            cleaned.append(char)
        elif char in {"-", "_", "."}:
            cleaned.append("_" if char == "." else char)
        else:
            cleaned.append("_")
    slug = "".join(cleaned).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "data"


def _short_hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:10]


def _store_route_for_bounds(
    bounds: tuple[float, float, float, float] | list[float],
    sample_lat: float,
    sample_lon: float,
) -> dict:
    west, east, south, north = [float(value) for value in bounds]
    lat = max(south + 0.01, min(north - 0.01, sample_lat))
    left = west + (east - west) * 0.2
    right = west + (east - west) * 0.8
    if abs(right - left) < 0.05:
        left = sample_lon - 0.05
        right = sample_lon + 0.05
    return {
        "id": "store-domain",
        "name": "Store Domain",
        "start": [lat, max(west + 0.01, min(east - 0.01, left))],
        "end": [lat, max(west + 0.01, min(east - 0.01, right))],
    }


def _box_radius_degrees(radius_km: float, lat: float) -> tuple[float, float]:
    radius = max(0.0, float(radius_km))
    lat_deg = radius / 111.0
    cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
    lon_deg = radius / (111.0 * cos_lat)
    return lat_deg, lon_deg


def _box_radius_degrees_from_payload(payload: dict, lat: float) -> tuple[float, float]:
    if payload.get("box_radius_lat_deg") is not None or payload.get("box_radius_lon_deg") is not None:
        lat_deg = float(payload.get("box_radius_lat_deg") or 0.0)
        lon_deg = float(payload.get("box_radius_lon_deg") or 0.0)
        return max(0.0, lat_deg), max(0.0, lon_deg)
    if payload.get("box_width_km") is not None or payload.get("box_height_km") is not None:
        width_km = max(0.0, float(payload.get("box_width_km") or payload.get("box_height_km") or 25.0))
        height_km = max(0.0, float(payload.get("box_height_km") or payload.get("box_width_km") or 25.0))
        cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
        return (height_km / 2.0) / 111.0, (width_km / 2.0) / (111.0 * cos_lat)
    return _box_radius_degrees(float(payload.get("box_radius_km") or 25.0), lat)


def _run_command(cmd: list[str], env: dict[str, str], *, timeout: int) -> dict:
    started = time.time()
    if _job_cancel_requested():
        return {
            "ok": False,
            "cancelled": True,
            "returncode": None,
            "elapsed_s": 0.0,
            "stdout_tail": [],
            "stderr_tail": [],
            "command": cmd,
            "error": "Job cancelled before command started.",
        }
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    proc: subprocess.Popen | None = None
    try:
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=creationflags,
        )
        _set_job_process(proc, cmd)
        deadline = started + float(timeout)
        stdout = ""
        stderr = ""
        while True:
            if _job_cancel_requested():
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    stdout, stderr = proc.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    stdout, stderr = proc.communicate()
                return {
                    "ok": False,
                    "cancelled": True,
                    "returncode": proc.returncode,
                    "elapsed_s": round(time.time() - started, 2),
                    "stdout_tail": (stdout or "").splitlines()[-80:],
                    "stderr_tail": (stderr or "").splitlines()[-80:],
                    "command": cmd,
                    "error": "Job cancelled.",
                }
            remaining = deadline - time.time()
            if remaining <= 0:
                try:
                    proc.kill()
                except Exception:
                    pass
                stdout, stderr = proc.communicate()
                return {
                    "ok": False,
                    "returncode": proc.returncode,
                    "elapsed_s": round(time.time() - started, 2),
                    "stdout_tail": (stdout or "").splitlines()[-80:],
                    "stderr_tail": (stderr or "").splitlines()[-80:],
                    "command": cmd,
                    "error": f"command timed out after {timeout}s",
                }
            try:
                stdout, stderr = proc.communicate(timeout=min(0.3, max(0.05, remaining)))
                break
            except subprocess.TimeoutExpired:
                continue
    finally:
        _clear_job_process()
    if proc is None:
        return {
            "ok": False,
            "returncode": None,
            "elapsed_s": round(time.time() - started, 2),
            "stdout_tail": [],
            "stderr_tail": [],
            "command": cmd,
            "error": "command did not start",
        }
    result = {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "elapsed_s": round(time.time() - started, 2),
        "stdout_tail": (stdout or "").splitlines()[-80:],
        "stderr_tail": (stderr or "").splitlines()[-80:],
        "command": cmd,
    }
    text = (stdout or "").strip()
    if text:
        try:
            result["json"] = json.loads(text)
        except Exception:
            pass
    return result


def _latest_named_file(root: Path, name: str) -> Path | None:
    if not root.exists():
        return None
    matches = [path for path in root.rglob(name) if path.is_file()]
    if not matches:
        return None
    matches.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return matches[0]


def _section_route(payload: dict) -> dict:
    route_id = str(payload.get("route_id") or "socal-coast-desert")
    if route_id != "custom":
        for route in CROSS_SECTION_ROUTES:
            if route["id"] == route_id:
                return {
                    "id": str(route["id"]),
                    "name": str(route["name"]),
                    "start": [float(route["start"][0]), float(route["start"][1])],
                    "end": [float(route["end"][0]), float(route["end"][1])],
                }
    return {
        "id": "custom",
        "name": str(payload.get("route_name") or "Custom"),
        "start": [float(payload.get("start_lat")), float(payload.get("start_lon"))],
        "end": [float(payload.get("end_lat")), float(payload.get("end_lon"))],
    }


def _route_bounds(route: dict, padding_deg: float = 1.5) -> tuple[float, float, float, float]:
    lats = [float(route["start"][0]), float(route["end"][0])]
    lons = [float(route["start"][1]), float(route["end"][1])]
    west = max(-180.0, min(lons) - padding_deg)
    east = min(180.0, max(lons) + padding_deg)
    south = max(-90.0, min(lats) - padding_deg)
    north = min(90.0, max(lats) + padding_deg)
    return west, east, south, north


def _discover_binaries(bin_dir: Path | None) -> dict[str, Path]:
    names = [
        "radar_export",
        "radar_web_tiles",
        "goes_web_tiles",
        "sounding_plot",
        "hrrr_pressure_volume_store",
        "model_wxprofile_store",
        "hrrr_wxprofile_store",
        "volume_store_cross_section_render",
        "volume_store_sounding_render",
        "wxprofile_sounding_render",
        "rustwx_grid_export",
        "wxstore_wxa_showcase",
        "wxstore",
        "hrrr_ecape_profile_probe",
        "hrrr_ecape_grid_research",
        "native_dataset_plan",
        "native_dataset_runner",
        "native_obs_preview",
    ]
    packaged = os.environ.get("RUSTWX_PACKAGED") == "1"
    roots = []
    if bin_dir:
        roots.append(bin_dir)
    if not packaged:
        roots.extend([
            Path.cwd() / "target" / "release",
            Path.cwd() / "target" / "debug",
            Path.home() / "rustwx" / "target" / "release",
            Path.home() / "rustwx" / "target" / "debug",
            Path.home() / "wxstore" / "target" / "release",
            Path.home() / "wxstore" / "target" / "debug",
            Path.home() / ".cargo" / "bin",
        ])
    found: dict[str, Path] = {}
    for name in names:
        candidates = [f"{name}.exe", name] if os.name == "nt" else [name, f"{name}.exe"]
        for root in roots:
            for candidate in candidates:
                path = root / candidate
                if path.exists():
                    found[name] = path.resolve()
                    break
            if name in found:
                break
        if name not in found and not packaged:
            for candidate in candidates:
                hit = shutil.which(candidate)
                if hit:
                    found[name] = Path(hit).resolve()
                    break
    if "model_wxprofile_store" in found:
        found.pop("hrrr_wxprofile_store", None)
    return found


def _collect_paths(value: object, suffix: str) -> list[str]:
    paths: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"previews", "url"}:
                continue
            paths.extend(_collect_paths(item, suffix))
    elif isinstance(value, list):
        for item in value:
            paths.extend(_collect_paths(item, suffix))
    elif isinstance(value, str) and value.lower().endswith(suffix):
        if value.startswith("/api/file") or value.startswith("http://") or value.startswith("https://"):
            return paths
        paths.append(str(Path(value).resolve()))
    return paths


def _chunks(values: list[str], size: int) -> list[list[str]]:
    size = max(1, int(size))
    return [values[index:index + size] for index in range(0, len(values), size)]


def _safe_component(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value.strip())
    return cleaned.strip("_") or "item"


def _forecast_hours_from_payload(payload: dict) -> list[int]:
    raw = payload.get("forecast_hours")
    hours: set[int] = set()

    def add_value(value: object) -> None:
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
                    lo, hi = sorted((start, end))
                    hours.update(range(lo, hi + 1))
                else:
                    hours.add(int(token))
            return
        hours.add(int(value))

    if isinstance(raw, (list, tuple)):
        for item in raw:
            add_value(item)
    else:
        add_value(raw)
    if not hours:
        add_value(payload.get("forecast_hour") or 0)
    valid = sorted(hour for hour in hours if hour >= 0)
    return valid or [0]


def _forecast_hour_from_path(path: str) -> int | None:
    path_obj = Path(path)
    for part in path_obj.parts:
        if len(part) > 1 and part[0].lower() == "f" and part[1:].isdigit():
            return int(part[1:])
    for part in path_obj.stem.replace("-", "_").split("_"):
        if len(part) > 1 and part[0].lower() == "f" and part[1:].isdigit():
            return int(part[1:])
    return None


def _path_allowed(path: Path, roots: set[Path]) -> bool:
    try:
        resolved = path.resolve()
        return any(resolved == root or root in resolved.parents for root in roots)
    except Exception:
        return False


def _query_one(query: dict[str, list[str]], key: str, default: str | None) -> str | None:
    values = query.get(key)
    return values[0] if values else default


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RustWx Studio</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #172026;
      --muted: #60717c;
      --line: #c9d3d8;
      --panel: #f6f8f9;
      --accent: #0d7285;
      --accent-dark: #074f5e;
      --ok: #1f9d64;
      --bad: #b73535;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: #ffffff;
    }
    header {
      display: flex;
      align-items: center;
      gap: 12px;
      min-height: 48px;
      padding: 0 16px;
      background: #20313a;
      color: #fff;
    }
    header strong { font-size: 16px; }
    #statusDot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: var(--bad);
      display: inline-block;
    }
    #statusDot.ok { background: var(--ok); }
    #statusText { color: #d8e7ec; }
    main {
      display: grid;
      grid-template-columns: minmax(320px, 390px) minmax(0, 1fr);
      min-height: calc(100vh - 48px);
    }
    aside {
      border-right: 1px solid var(--line);
      background: var(--panel);
      overflow: auto;
    }
    section {
      padding: 16px;
      border-bottom: 1px solid var(--line);
    }
    h2 {
      margin: 0 0 12px;
      font-size: 13px;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    label {
      display: grid;
      gap: 4px;
      color: #33434b;
      font-weight: 600;
      font-size: 12px;
    }
    input, select, button {
      font: inherit;
      border: 1px solid #aebcc3;
      border-radius: 6px;
      background: #fff;
      color: var(--ink);
      min-height: 36px;
      padding: 7px 9px;
    }
    button {
      cursor: pointer;
      font-weight: 700;
      border-color: var(--accent);
      color: var(--accent-dark);
    }
    button.primary {
      background: var(--accent);
      color: #fff;
    }
    button:disabled {
      opacity: .55;
      cursor: wait;
    }
    .tabs {
      display: flex;
      overflow-x: auto;
      border-bottom: 1px solid var(--line);
    }
    .tabs button {
      flex: 0 0 auto;
      border: 0;
      border-right: 1px solid var(--line);
      border-radius: 0;
      min-height: 42px;
      min-width: 64px;
      background: #e9eef1;
      color: #33434b;
    }
    .tabs button.active {
      background: #fff;
      color: var(--accent-dark);
      box-shadow: inset 0 -3px 0 var(--accent);
    }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    .row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-bottom: 9px;
    }
    .wide { grid-column: 1 / -1; }
    .toolbar {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }
    .product-list {
      height: 310px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      padding: 6px;
    }
    .product-item {
      display: grid;
      grid-template-columns: 22px 1fr auto;
      gap: 6px;
      align-items: center;
      min-height: 32px;
      padding: 4px;
      border-bottom: 1px solid #edf1f3;
    }
    .badge {
      color: var(--muted);
      border: 1px solid var(--line);
      border-radius: 5px;
      padding: 1px 5px;
      font-size: 11px;
    }
    .store-summary {
      min-height: 32px;
      margin: 8px 0;
      color: var(--muted);
      font-size: 12px;
    }
    .mini-map {
      position: relative;
      width: 100%;
      aspect-ratio: 1.7;
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: hidden;
      background:
        linear-gradient(135deg, rgba(13, 114, 133, .08), rgba(226, 236, 239, .72)),
        #eef4f5;
      margin-bottom: 10px;
    }
    .mini-map svg {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
    }
    .radar-basemap {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      object-fit: contain;
    }
    .radar-stage {
      display: none;
      margin-bottom: 14px;
    }
    .radar-stage.active {
      display: block;
    }
    .radar-stage-map {
      min-height: min(68vh, 760px);
      aspect-ratio: 1.7;
    }
    .radar-stage-map .radar-site {
      r: 4.6;
      stroke-width: 1.3;
    }
    .radar-stage-map .radar-site.active {
      r: 7;
    }
    .radar-site {
      cursor: pointer;
      fill: #0d7285;
      stroke: #fff;
      stroke-width: 1.5;
    }
    .radar-site.active {
      fill: #d24b32;
      r: 5;
    }
    .map-clickable {
      cursor: crosshair;
    }
    .map-image-wrap {
      position: relative;
      line-height: 0;
      background: #eef4f5;
      touch-action: none;
    }
    .map-drag-box {
      position: absolute;
      display: none;
      border: 2px solid var(--accent);
      background: rgba(13, 114, 133, .14);
      pointer-events: none;
    }
    #workspace {
      padding: 16px;
      overflow: auto;
    }
    .job-strip {
      display: grid;
      gap: 8px;
      margin-bottom: 12px;
    }
    .job-card {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 4px 10px;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      padding: 8px 10px;
      font-size: 12px;
    }
    .job-card strong { font-size: 13px; }
    .job-card small { color: var(--muted); }
    .job-card button {
      min-height: 28px;
      padding: 3px 8px;
      font-size: 12px;
    }
    .job-status {
      border-radius: 5px;
      padding: 2px 7px;
      background: #e8eef1;
      color: #33434b;
      font-weight: 700;
    }
    .job-status.completed { background: #dff2e9; color: #17633f; }
    .job-status.failed { background: #f5dddd; color: #8f2727; }
    .job-status.running { background: #e4f3f6; color: var(--accent-dark); }
    .job-status.cancelling { background: #fff0cf; color: #7a4c00; }
    .job-status.cancelled { background: #ece2f1; color: #5c3b73; }
    .status-grid {
      display: grid;
      gap: 8px;
    }
    .status-card {
      display: grid;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      padding: 9px;
      overflow-wrap: anywhere;
    }
    .status-card strong {
      font-size: 13px;
    }
    .status-card small {
      color: var(--muted);
      line-height: 1.35;
    }
    .data-list {
      display: grid;
      gap: 8px;
    }
    .data-row {
      display: grid;
      gap: 5px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      padding: 9px;
      overflow-wrap: anywhere;
    }
    .data-row a {
      color: var(--accent-dark);
      font-weight: 700;
      text-decoration: none;
    }
    .data-row small {
      color: var(--muted);
      line-height: 1.35;
    }
    .status-pills {
      display: flex;
      gap: 5px;
      flex-wrap: wrap;
    }
    .status-ok {
      border-color: #a8d7c1;
      background: #e9f6ef;
      color: #17633f;
    }
    .status-missing {
      border-color: #e2c2c2;
      background: #f8eded;
      color: #8f2727;
    }
    #result {
      white-space: pre-wrap;
      background: #101820;
      color: #d9edf2;
      border-radius: 6px;
      padding: 12px;
      max-height: 300px;
      overflow: auto;
    }
    #gallery {
      display: flex;
      flex-direction: column;
      gap: 12px;
      margin-bottom: 14px;
    }
    #outputGallery {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    figure {
      margin: 0;
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: hidden;
      background: #fff;
    }
    figure img {
      display: block;
      width: 100%;
      height: auto;
    }
    #gallery figure img {
      max-height: calc(100vh - 170px);
      object-fit: contain;
      background: #eef4f5;
    }
    figcaption {
      padding: 8px;
      color: var(--muted);
      font-size: 12px;
      border-top: 1px solid var(--line);
      overflow-wrap: anywhere;
    }
    .artifact-link {
      display: flex;
      min-height: 120px;
      align-items: center;
      justify-content: center;
      padding: 18px;
      background: #f2f7f8;
      color: var(--ink);
      font-weight: 700;
      text-decoration: none;
      text-align: center;
    }
    .artifact-link:hover {
      background: #e4eef0;
    }
    @media (max-width: 900px) {
      main { grid-template-columns: 1fr; }
      aside { border-right: 0; }
    }
  </style>
</head>
<body>
  <header>
    <strong>RustWx Studio</strong>
    <span id="statusDot"></span>
    <span id="statusText">loading</span>
  </header>
  <main>
    <aside>
      <div class="tabs">
        <button class="active" data-tab="maps">Maps</button>
        <button data-tab="satellite">Sat</button>
        <button data-tab="batchTab">Batch</button>
        <button data-tab="caseTab">Cases</button>
        <button data-tab="radar">Radar</button>
        <button data-tab="sounding">Sound</button>
        <button data-tab="sectionTab">X-Sec</button>
        <button data-tab="store">Store</button>
        <button data-tab="nativeTab">Native</button>
        <button data-tab="point">Point</button>
        <button data-tab="dataTab">Data</button>
        <button data-tab="statusTab">Status</button>
      </div>
      <div id="maps" class="tab-panel active">
        <section>
          <h2>Model Map</h2>
          <div class="row">
            <label>Model<select id="model"></select></label>
            <label>Source<select id="source"><option>aws</option><option>nomads</option><option>earth2-archive</option></select></label>
          </div>
          <div class="row">
            <label>Run<input id="run" value="latest"></label>
            <label>Hour<input id="forecastHour" type="number" min="0" max="43848" value="0"></label>
          </div>
          <div class="row">
            <label>Find Domain<input id="domainSearch" placeholder="conus, plains, okc"></label>
            <label class="wide">Domain<select id="domain"></select></label>
          </div>
          <div class="row">
            <label class="wide">Plot Size<select id="sizePreset">
              <option value="1600x1100" selected>High - 1600 x 1100</option>
              <option value="1200x825">Standard - 1200 x 825</option>
              <option value="1920x1320">Sharp - 1920 x 1320</option>
              <option value="2400x1650">Poster - 2400 x 1650</option>
              <option value="1280x720">Wide - 1280 x 720</option>
              <option value="1920x1080">Wide HD - 1920 x 1080</option>
              <option value="custom">Custom</option>
            </select></label>
            <label>Width<input id="width" type="number" min="320" max="3600" value="1600"></label>
            <label>Height<input id="height" type="number" min="240" max="2600" value="1100"></label>
          </div>
        </section>
        <section>
          <h2>Products</h2>
          <div class="row">
            <label>Kind<select id="kind"><option value="">All</option><option value="direct">Direct</option><option value="light_derived">Derived</option><option value="heavy_derived">Heavy</option><option value="windowed">Windowed</option></select></label>
            <label>Search<input id="productSearch" placeholder="temperature, cape, qpf"></label>
          </div>
          <div class="toolbar">
            <button id="preset2m">2m Temp</button>
            <button id="presetSevere">Severe</button>
            <button id="selectVisibleProducts">All Visible</button>
            <button id="clearProducts">Clear</button>
          </div>
          <div id="productList" class="product-list"></div>
          <div class="row">
            <label>Render Hours<input id="renderHours" value="0" placeholder="0-2,6,12"></label>
            <label>Cache Hours<input id="prepareHours" value="0" placeholder="0-2,6,12"></label>
          </div>
          <div class="toolbar"><button id="prepareData">Prepare Data</button><button id="renderBtn" class="primary">Render Maps</button></div>
        </section>
      </div>
      <div id="satellite" class="tab-panel">
        <section>
          <h2>GOES</h2>
          <div class="row">
            <label>Satellite<select id="satelliteId"><option value="goes19">GOES-19</option><option value="goes18">GOES-18</option><option value="goes16">GOES-16</option></select></label>
            <label>Sector<select id="satSector"><option value="conus">CONUS</option><option value="full_disk">Full Disk</option><option value="meso1">Meso 1</option><option value="meso2">Meso 2</option></select></label>
          </div>
          <div class="row">
            <label>Domain<select id="satDomain"></select></label>
            <label>Scan Window Hr<input id="satLookback" type="number" value="2" min="1" max="24"></label>
          </div>
          <div class="row">
            <label>Frames<input id="satSequenceCount" type="number" value="1" min="1" max="48"></label>
            <label>GIF<select id="satGif"><option value="">No</option><option value="1">Yes</option></select></label>
          </div>
          <div class="row">
            <label>Native Scale<input id="satNativeDownsample" type="number" value="1" min="1" max="8" step=".5"></label>
            <label>Min Step Min<input id="satNativeMinStep" type="number" min="1" max="120" placeholder="optional"></label>
          </div>
          <div class="row">
            <label>Tile Layer<select id="satTileLayer"><option value="geocolor">GeoColor</option><option value="clouds">Clouds</option></select></label>
            <label>Tile Frames<input id="satTileFrames" type="number" value="3" min="1" max="24"></label>
          </div>
          <div class="row">
            <label>Min Zoom<input id="satTileMinZoom" type="number" value="4" min="1" max="10"></label>
            <label>Max Zoom<input id="satTileMaxZoom" type="number" value="6" min="1" max="10"></label>
          </div>
          <div class="row">
            <label>Tile Opacity<input id="satTileOpacity" type="number" value=".92" min=".05" max="1" step=".05"></label>
            <label>Compression<select id="satTileCompression"><option value="fast">Fast</option><option value="fastest">Fastest</option><option value="default">Default</option></select></label>
          </div>
          <div id="satProductList" class="product-list"></div>
          <div class="toolbar">
            <button id="satGeo">GeoColor</button>
            <button id="satBand13">Band 13</button>
            <button id="satRender" class="primary">Render Satellite</button>
            <button id="satNativeRender">Native Seq</button>
            <button id="satTileLoopRender">Render Tiles</button>
          </div>
        </section>
      </div>
      <div id="batchTab" class="tab-panel">
        <section>
          <h2>Batch Maps</h2>
          <div class="row">
            <label>Models<select id="batchModelsMode"><option value="current">Current</option><option value="all">All</option><option value="list">List</option></select></label>
            <label>Model IDs<input id="batchModels" placeholder="hrrr,gfs,nam"></label>
          </div>
          <div class="row">
            <label>Products<select id="batchProductMode"><option value="default">Default</option><option value="selected">Selected</option><option value="kind">Kind</option><option value="all">All</option></select></label>
            <label>Kind<select id="batchProductKind"><option value="">All Kinds</option><option value="direct">Direct</option><option value="light_derived">Derived</option><option value="heavy_derived">Heavy</option><option value="windowed">Windowed</option></select></label>
          </div>
          <div class="row">
            <label>Hours<input id="batchHours" value="0" placeholder="0-2,6,12"></label>
            <label>Max Products<input id="batchMaxProducts" type="number" min="1" placeholder="optional"></label>
          </div>
          <div class="row">
            <label>Domain<select id="batchDomain"></select></label>
            <label>Action<select id="batchAction"><option value="plan">Plan</option><option value="prepare_data">Prepare Data</option><option value="render">Render</option></select></label>
          </div>
          <div class="toolbar"><button id="batchPlan">Plan Batch</button><button id="batchRun" class="primary">Run Batch</button></div>
        </section>
      </div>
      <div id="caseTab" class="tab-panel">
        <section>
          <h2>Case Sweep</h2>
          <div class="row">
            <label>Mode<select id="caseMode"><option value="render">Render</option><option value="probe">ECAPE Probe</option></select></label>
            <label>Action<select id="caseAction"><option value="plan">Plan</option><option value="render">Run</option></select></label>
          </div>
          <div class="row">
            <label>Start<input id="caseStart" placeholder="2026-05-22"></label>
            <label>End<input id="caseEnd" placeholder="2026-05-22"></label>
          </div>
          <div class="row">
            <label>Cycles<input id="caseCycles" value="19" placeholder="0,6,12,18"></label>
            <label>Hours<input id="caseHours" value="0" placeholder="0-2,6"></label>
          </div>
          <div class="row">
            <label>Domain<select id="caseDomain"></select></label>
            <label>Limit<input id="caseLimit" type="number" min="1" value="1"></label>
          </div>
          <div class="row">
            <label class="wide">Points<input id="casePoints" value="35.222,-97.439,Norman"></label>
          </div>
          <div class="toolbar"><button id="casePlan">Plan Case</button><button id="caseRun" class="primary">Run Case</button></div>
        </section>
      </div>
      <div id="radar" class="tab-panel">
        <section>
          <h2>NEXRAD</h2>
          <div id="radarMap" class="mini-map"></div>
          <div class="row">
            <label>Site<input id="radarSite" value="KTLX"></label>
            <label>Size<input id="radarSize" type="number" value="1024" min="512" max="2400"></label>
          </div>
          <div class="row">
            <label>Lat<input id="radarLat" placeholder="optional"></label>
            <label>Lon<input id="radarLon" placeholder="optional"></label>
          </div>
          <div class="row">
            <label>Mode<select id="radarMode"><option value="classic">Classic</option><option value="smooth">Smooth</option></select></label>
            <label>Dealias<select id="radarDealias"><option value="">No</option><option value="1">Yes</option></select></label>
          </div>
          <div id="radarProductList" class="product-list"></div>
          <div class="toolbar"><button id="radarRender" class="primary">Render Radar</button></div>
        </section>
        <section>
          <h2>Tiles</h2>
          <div class="row">
            <label>Domain<select id="radarTileDomain"></select></label>
            <label>Color<select id="radarTileColor"><option value="default">Default</option><option value="gr2analyst">GR2Analyst</option><option value="nssl">NSSL</option><option value="classic">Classic</option><option value="dark">Dark</option><option value="colorblind">Colorblind</option></select></label>
          </div>
          <div class="row">
            <label>Min Zoom<input id="radarTileMinZoom" type="number" min="2" max="12" value="5"></label>
            <label>Max Zoom<input id="radarTileMaxZoom" type="number" min="2" max="12" value="7"></label>
          </div>
          <div class="row">
            <label>Loop Frames<input id="radarTileLoopFrames" type="number" min="1" max="24" value="4"></label>
            <label>Loop Delay ms<input id="radarTileLoopDelay" type="number" min="100" max="3000" value="650"></label>
          </div>
          <div class="row">
            <label>Supersample<input id="radarTileSupersample" type="number" min="1" max="4" value="1"></label>
            <label>Compression<select id="radarTileCompression"><option value="fastest">Fastest</option><option value="fast" selected>Fast</option><option value="default">Default</option></select></label>
          </div>
          <div class="row">
            <label>QC<select id="radarTileQc"><option value="despeckle" selected>Despeckle</option><option value="velocity">Velocity Filter</option><option value="">None</option></select></label>
            <label>All Tilts<select id="radarTileAllTilts"><option value="">No</option><option value="1">Yes</option></select></label>
          </div>
          <div class="row">
            <label>Clip<select id="radarTileClip"><option value="">No</option><option value="1">Yes</option></select></label>
            <label>Keep Empty<select id="radarTileKeepEmpty"><option value="">No</option><option value="1">Yes</option></select></label>
          </div>
          <div class="toolbar"><button id="radarTilesRender">Render Tiles</button><button id="radarTileLoopRender" class="primary">Render Loop</button></div>
        </section>
      </div>
      <div id="sounding" class="tab-panel">
        <section>
          <h2>Sounding</h2>
          <div class="row">
            <label>Lat<input id="soundingLat" value="35.222"></label>
            <label>Lon<input id="soundingLon" value="-97.439"></label>
          </div>
          <div class="row">
            <label>Station<input id="soundingStation" value="KOUN"></label>
            <label>Hour<input id="soundingHour" type="number" min="0" max="43848" value="0"></label>
          </div>
          <div class="row">
            <label>Sample<select id="soundingMethod"></select></label>
            <label>Box Radius km<input id="soundingBoxKm" type="number" min="1" max="250" value="25"></label>
          </div>
          <div class="row">
            <label>Data<select id="soundingDataMode"><option value="auto">Fast Store</option><option value="grib">Raw GRIB</option><option value="store">Store Only</option></select></label>
            <label>Crop Radius deg<input id="soundingCropDeg" type="number" min=".1" max="5" step=".1" value="1.0"></label>
          </div>
          <div class="row">
            <label>Column<select id="soundingColumn"><option value="">No</option><option value="1">Yes</option></select></label>
          </div>
          <div class="toolbar"><button id="prepareStore">Prepare Store</button><button id="soundingRender" class="primary">Render Sounding</button></div>
        </section>
      </div>
      <div id="sectionTab" class="tab-panel">
        <section>
          <h2>Cross Section</h2>
          <div class="row">
            <label class="wide">Route<select id="sectionRoute"></select></label>
            <label>Hour<input id="sectionHour" type="number" min="0" max="43848" value="0"></label>
          </div>
          <div class="row">
            <label>Start Lat<input id="sectionStartLat" value="34.0195"></label>
            <label>Start Lon<input id="sectionStartLon" value="-118.4912"></label>
          </div>
          <div class="row">
            <label>End Lat<input id="sectionEndLat" value="33.8303"></label>
            <label>End Lon<input id="sectionEndLon" value="-116.5453"></label>
          </div>
          <div class="row">
            <label>Spacing km<input id="sectionSpacing" type="number" min="1" max="50" value="5"></label>
            <label>Top hPa<input id="sectionTop" type="number" min="10" max="700" value="100"></label>
          </div>
          <div id="sectionProductList" class="product-list"></div>
          <div class="toolbar"><button id="sectionRender" class="primary">Render Section</button></div>
        </section>
      </div>
      <div id="store" class="tab-panel">
        <section>
          <h2>WxStore</h2>
          <div class="row">
            <label>Hours<input id="storeHours" value="0"></label>
            <label>Jobs<input id="storeJobs" type="number" min="1" max="16" value="2"></label>
          </div>
          <div class="row">
            <label>Import<select id="storeImport"><option value="1">Yes</option><option value="">No</option></select></label>
            <label>Plot<select id="storePlot"><option value="1">Yes</option><option value="">No</option></select></label>
          </div>
          <div class="row">
            <label>Max Plots<input id="storeMaxProducts" type="number" min="1" max="500" placeholder="optional"></label>
            <label>Compression<select id="storeCompression"><option value="fastest">Fastest</option><option value="fast">Fast</option><option value="default">Default</option></select></label>
          </div>
        </section>
        <section>
          <h2>WxStore Service</h2>
          <div class="row">
            <label class="wide">URL<input id="wxstoreServiceUrl" value="http://127.0.0.1:8897"></label>
          </div>
          <div class="row">
            <label>Action<select id="wxstoreServiceAction"><option value="status">Status</option><option value="catalog">Catalog</option><option value="forecast">Forecast</option><option value="sample">Sample</option><option value="temporal_sounding">Temporal Sounding</option><option value="objects">Objects</option><option value="mesoanalysis">Mesoanalysis</option></select></label>
            <label>Endpoint<select id="wxstoreServiceEndpoint"><option value="models">Models</option><option value="products">Products</option><option value="variables">Variables</option><option value="layers">Layers</option><option value="static_plots">Static Plots</option><option value="observation_sources">Obs Sources</option><option value="satellite_layers">Sat Layers</option><option value="radar_layers">Radar Layers</option><option value="status">Status</option><option value="query">Query</option><option value="watchlist">Watchlist</option></select></label>
          </div>
          <div class="row">
            <label>Run<input id="wxstoreServiceRun" value="latest"></label>
            <label>Member<input id="wxstoreServiceMember" value="control"></label>
          </div>
          <div class="row">
            <label>Variable<input id="wxstoreServiceVariable" value="temperature_2m"></label>
            <label>Hours<input id="wxstoreServiceHours" value="0-2"></label>
          </div>
          <div class="row">
            <label>Lat<input id="wxstoreServiceLat" value="35.222"></label>
            <label>Lon<input id="wxstoreServiceLon" value="-97.439"></label>
          </div>
          <div class="row">
            <label>Kind<input id="wxstoreServiceKind" placeholder="surface_observation"></label>
            <label>Query<input id="wxstoreServiceQuery" placeholder="KOUN"></label>
          </div>
          <div class="row">
            <label>Limit<input id="wxstoreServiceLimit" type="number" min="1" max="500" value="25"></label>
            <label>Radius km<input id="wxstoreServiceRadius" type="number" min="1" max="500" placeholder="optional"></label>
          </div>
          <div class="toolbar">
            <button id="wxstoreServiceStart">Start Service</button>
            <button id="wxstoreServiceQueryRun" class="primary">Query Service</button>
          </div>
        </section>
        <section>
          <h2>Store Products</h2>
          <div class="row">
            <label>Kind<select id="storeKind"><option value="">All</option><option value="direct">Direct</option><option value="light_derived">Derived</option><option value="heavy_derived">Heavy</option><option value="windowed">Windowed</option></select></label>
            <label>Search<input id="storeProductSearch" placeholder="temperature, wind, qpf"></label>
          </div>
          <div class="toolbar">
            <button id="storePreset2m">2m Temp</button>
            <button id="storeSelectVisible">All Visible</button>
            <button id="storeClearProducts">Clear</button>
          </div>
          <div id="storeProductList" class="product-list"></div>
          <div class="toolbar"><button id="storeRun" class="primary">Build Store</button></div>
        </section>
        <section>
          <h2>Store Browser</h2>
          <div class="row">
            <label>Run<input id="storeExistingRun" value="latest"></label>
            <label>Member<input id="storeExistingMember" value="control"></label>
          </div>
          <div class="row">
            <label>Hours<input id="storeExistingHours" value="0"></label>
            <label>View<select id="storeExistingView"><option value="store">Store</option><option value="domain">Domain</option></select></label>
          </div>
          <div class="row">
            <label>Max Plots<input id="storeExistingMaxProducts" type="number" min="1" max="500" placeholder="optional"></label>
            <label>Compression<select id="storeExistingCompression"><option value="fastest">Fastest</option><option value="fast">Fast</option><option value="default">Default</option></select></label>
          </div>
          <div class="toolbar">
            <button id="storeInspect">Inspect</button>
            <button id="storePlotExisting" class="primary">Plot Store</button>
          </div>
          <div id="storeExistingSummary" class="store-summary">No store loaded.</div>
          <div id="storeExistingProductList" class="product-list"></div>
        </section>
      </div>
      <div id="nativeTab" class="tab-panel">
        <section>
          <h2>ECAPE</h2>
          <div class="row">
            <label>Lat<input id="ecapeLat" value="35.222"></label>
            <label>Lon<input id="ecapeLon" value="-97.439"></label>
          </div>
          <div class="row">
            <label>Hour<input id="ecapeHour" type="number" min="0" max="48" value="1"></label>
            <label>Domain<select id="ecapeDomain"></select></label>
          </div>
          <div class="row">
            <label>Parcel<select id="ecapeParcel"><option value="ml">ML</option><option value="sb">SB</option><option value="mu">MU</option></select></label>
            <label>Crop deg<input id="ecapeCropDeg" type="number" min=".1" max="5" step=".1" value="1.0"></label>
          </div>
          <div class="row">
            <label>Column<select id="ecapeColumn"><option value="">No</option><option value="1">Yes</option></select></label>
            <label>CSV<select id="ecapeCsv"><option value="">No</option><option value="1">Yes</option></select></label>
          </div>
          <div class="toolbar">
            <button id="ecapeProfile">Profile</button>
            <button id="ecapeGrid">Grid</button>
            <button id="ecapeRatio" class="primary">Ratio Maps</button>
          </div>
        </section>
        <section>
          <h2>Dataset</h2>
          <div class="row">
            <label>Dataset<input id="nativeDatasetName" value="rustwx_hrrr_multisource_v1"></label>
            <label>Grid<input id="nativeGridSize" type="number" min="64" max="2048" value="512"></label>
          </div>
          <div class="row">
            <label class="wide">Case<input id="nativeCase" value="20240506_ok_ks,2024-05-06T12:00:00Z,1"></label>
          </div>
          <div class="row">
            <label class="wide">Tile Grid<input id="nativeTileGrid" placeholder="-103.75,-93.5,32.75,38.25,1,1"></label>
          </div>
          <div class="row">
            <label>Domain<select id="nativeDomain"></select></label>
            <label>History<input id="nativeHistory" type="number" min="1" max="24" value="3"></label>
          </div>
          <div class="row">
            <label>HRRR<input id="nativeHrrrFields" value="t2m,d2m,u10,v10,cape,cin,refc,mslp,terrain,pwat"></label>
            <label>GOES<input id="nativeGoesChannels" value="C01,C02,C03,C07,C08,C09,C10,C13"></label>
          </div>
          <div class="row">
            <label>MRMS<input id="nativeMrmsFields" value="refc,llz,prate"></label>
            <label>Level-II<input id="nativeLevel2Products" value="reflectivity,velocity"></label>
          </div>
          <div class="row">
            <label>Run<select id="nativeRunMode"><option value="dry">Dry</option><option value="materialize">Materialize</option></select></label>
            <label>Fetch<select id="nativeFetch"><option value="">No</option><option value="1">Yes</option></select></label>
          </div>
          <div class="row">
            <label>Plan Path<input id="nativePlanPath" placeholder="optional"></label>
          </div>
          <div class="toolbar"><button id="nativePlan">Plan</button><button id="nativeRun" class="primary">Run</button></div>
        </section>
        <section>
          <h2>Obs Preview</h2>
          <div class="row">
            <label>Kind<select id="nativeObsKind"><option value="goes">GOES</option><option value="mrms">MRMS</option><option value="level2">Level-II</option></select></label>
            <label>Size<input id="nativeObsSize" type="number" min="256" max="2400" value="768"></label>
          </div>
          <div class="row">
            <label class="wide">Input<input id="nativeObsInput" placeholder="C:\\path\\to\\file"></label>
          </div>
          <div class="row">
            <label>Channel<input id="nativeObsChannel" value="C13"></label>
            <label>Product<input id="nativeObsProduct" value="reflectivity"></label>
          </div>
          <div class="row">
            <label>Radar<input id="nativeObsRadar" value="KTLX"></label>
            <label>Dealias<select id="nativeObsDealias"><option value="auto">Auto</option><option value="off">Off</option><option value="radial">Radial</option><option value="sweep">Sweep</option></select></label>
          </div>
          <div class="toolbar"><button id="nativeObsPreview" class="primary">Preview</button></div>
        </section>
      </div>
      <div id="point" class="tab-panel">
        <section>
          <h2>Point Sample</h2>
          <div class="row">
            <label>Lat<input id="pointLat" value="35.222"></label>
            <label>Lon<input id="pointLon" value="-97.439"></label>
          </div>
          <div class="row">
            <label>Start Hour<input id="pointStart" type="number" value="0"></label>
            <label>End Hour<input id="pointEnd" type="number" value="6"></label>
          </div>
          <div class="row">
            <label>Method<select id="pointMethod"><option value="nearest">Nearest</option><option value="inverse-distance4">Inverse Distance</option></select></label>
            <label>Store<input id="pointStoreId" placeholder="optional"></label>
          </div>
          <div class="row">
            <label class="wide">Variables<input id="pointVariables" placeholder="optional"></label>
          </div>
          <div class="toolbar"><button id="pointSample" class="primary">Sample Point</button></div>
        </section>
        <section>
          <h2>Point Store</h2>
          <div class="row">
            <label class="wide">Domain<select id="pointDomain"></select></label>
          </div>
          <div class="toolbar"><button id="pointWarmStore" class="primary">Warm Store</button></div>
        </section>
      </div>
      <div id="dataTab" class="tab-panel">
        <section>
          <h2>Inventory</h2>
          <div class="row">
            <label>Search<input id="dataSearch" placeholder="hrrr, radar, wxstore"></label>
          </div>
          <div class="toolbar"><button id="dataRefresh" class="primary">Refresh</button></div>
          <div id="dataSummary" class="status-grid"></div>
        </section>
        <section>
          <h2>Stores</h2>
          <div id="dataStores" class="data-list"></div>
        </section>
        <section>
          <h2>Recent</h2>
          <div id="dataRecent" class="data-list"></div>
        </section>
      </div>
      <div id="statusTab" class="tab-panel">
        <section>
          <h2>Runtime</h2>
          <div id="statusSummary" class="status-grid"></div>
        </section>
        <section>
          <h2>Capabilities</h2>
          <div id="capabilitySummary" class="status-grid"></div>
        </section>
      </div>
    </aside>
    <div id="workspace">
      <div id="jobStrip" class="job-strip"></div>
      <div id="radarStage" class="radar-stage">
        <div id="radarStageMap" class="mini-map radar-stage-map"></div>
      </div>
      <div id="gallery"></div>
      <div id="outputGallery"></div>
      <pre id="result">Ready.</pre>
    </div>
  </main>
  <script>
const state = {
  bootstrap: null,
  products: null,
  domains: [],
  selectedProducts: new Set(["2m_temperature_10m_winds"]),
  selectedSatellite: new Set(["goes_geocolor"]),
  selectedRadar: new Set(["ref"]),
  selectedSection: new Set(["temperature"]),
  selectedStore: new Set(["2m_temperature_10m_winds"]),
  selectedExistingStore: new Set(),
  wxstoreInspect: null,
  dataInventory: null,
  radarSites: [],
  radarBasemap: null,
  nextSectionPoint: "start",
  mapPick: null,
  soundingContext: null,
  visibleProducts: [],
  visibleStoreProducts: [],
  visibleExistingStoreProducts: [],
  jobs: new Map(),
  jobPollers: new Map(),
};
const COMMON_DOMAINS = [
  "conus",
  "southern-plains",
  "southern_plains",
  "oklahoma",
  "ok_oklahoma_city",
  "midwest",
  "great-lakes",
  "great_lakes",
  "northeast",
  "southeast",
  "california",
  "gulf-to-kansas",
  "gulf_to_kansas",
];
const $ = (id) => document.getElementById(id);

async function api(path, options = {}) {
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}
function post(path, payload) {
  return api(path, { method: "POST", headers: {"content-type": "application/json"}, body: JSON.stringify(payload) });
}
function productLabel(slug) { return slug.replaceAll("_", " "); }
function formatInt(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number.toLocaleString() : String(value || 0);
}
function parseHourList(raw, fallbackHour = 0) {
  const text = String(raw || "").trim();
  if (!text) return [Number(fallbackHour || 0)];
  const hours = new Set();
  for (const part of text.split(",")) {
    const item = part.trim();
    if (!item) continue;
    if (item.includes("-")) {
      const pieces = item.split("-", 2).map((value) => Number(value.trim()));
      if (pieces.every(Number.isFinite)) {
        const [start, end] = pieces[0] <= pieces[1] ? pieces : [pieces[1], pieces[0]];
        for (let hour = start; hour <= end; hour += 1) hours.add(hour);
      }
    } else {
      const hour = Number(item);
      if (Number.isFinite(hour)) hours.add(hour);
    }
  }
  return [...hours].filter((hour) => hour >= 0).sort((a, b) => a - b);
}
function appendStatusCard(parent, title, value, detail = "", pills = []) {
  if (!parent) return;
  const card = document.createElement("div");
  card.className = "status-card";
  const heading = document.createElement("strong");
  heading.textContent = title;
  const body = document.createElement("div");
  body.textContent = value;
  card.append(heading, body);
  if (detail) {
    const small = document.createElement("small");
    small.textContent = detail;
    card.appendChild(small);
  }
  if (pills.length) {
    const row = document.createElement("div");
    row.className = "status-pills";
    for (const pill of pills) {
      const badge = document.createElement("span");
      badge.className = `badge ${pill.ok ? "status-ok" : "status-missing"}`;
      badge.textContent = pill.label;
      row.appendChild(badge);
    }
    card.appendChild(row);
  }
  parent.appendChild(card);
}
function renderStatusPanel() {
  const statusSummary = $("statusSummary");
  const capabilitySummary = $("capabilitySummary");
  if (!statusSummary || !capabilitySummary || !state.bootstrap) return;
  const data = state.bootstrap;
  const doctor = data.doctor || {};
  const models = data.models?.models || [];
  const domains = data.domains?.domains || [];
  const productTotals = models.reduce((acc, model) => {
    acc.direct += Number(model.direct_recipe_count || 0);
    acc.light += Number(model.light_derived_recipe_count || 0);
    acc.heavy += Number(model.heavy_derived_recipe_count || 0);
    acc.windowed += Number(model.windowed_recipe_count || 0);
    return acc;
  }, { direct: 0, light: 0, heavy: 0, windowed: 0 });
  const currentCount = state.products?.count ? `Current ${$("model").value}: ${formatInt(state.products.count)} products` : "";
  statusSummary.replaceChildren();
  appendStatusCard(
    statusSummary,
    "RustWx",
    `${doctor.rustwx_version || data.version || "unknown"} | ${doctor.plot_style || data.plot_style || "default"}`,
    `Cache: ${doctor.cache_dir || ""}`
  );
  appendStatusCard(
    statusSummary,
    "Models",
    `${formatInt(models.length)} models | ${formatInt(domains.length || doctor.domain_count)} domains`,
    currentCount || `Output: ${doctor.out_root || ""}`
  );
  appendStatusCard(
    statusSummary,
    "Products",
    `${formatInt(productTotals.direct + productTotals.light + productTotals.heavy + productTotals.windowed)} recipes`,
    `Direct ${formatInt(productTotals.direct)} | Derived ${formatInt(productTotals.light)} | Heavy ${formatInt(productTotals.heavy)} | Windowed ${formatInt(productTotals.windowed)}`
  );
  capabilitySummary.replaceChildren();
  const tools = doctor.specialty_tools || {};
  appendStatusCard(capabilitySummary, "Maps", "Model plots and shared GRIB cache", "Operational Fast is the default plot style.", [
    { label: "prepare-data", ok: true },
    { label: "batch planner", ok: true },
    { label: "case sweeps", ok: true },
    { label: "click soundings", ok: !!tools.sounding },
  ]);
  appendStatusCard(capabilitySummary, "Soundings", "Point, box, and store-backed fast render", "Warm pressure stores should keep repeat clicks quick.", [
    { label: "sounding", ok: !!tools.sounding },
    { label: "fast store", ok: !!tools.fast_soundings },
    { label: "cross sections", ok: !!tools.cross_section },
    { label: "point store", ok: !!tools.point_store },
  ]);
  appendStatusCard(capabilitySummary, "WxStore", "Export, import, and WXA plotting", "Use this when users want reusable 2D/3D stores.", [
    { label: "export", ok: !!tools.wxstore_export },
    { label: "import", ok: !!tools.wxstore_import },
    { label: "showcase", ok: !!tools.wxstore_showcase },
  ]);
  appendStatusCard(capabilitySummary, "Native / ECAPE", "Profiles, swaths, dataset plans, and raw obs preview", "These use optional Rust proof and native data binaries.", [
    { label: "ECAPE profile", ok: !!tools.ecape_profile },
    { label: "ECAPE grid", ok: !!tools.ecape_grid },
    { label: "dataset", ok: !!tools.native_dataset_plan && !!tools.native_dataset_run },
    { label: "obs preview", ok: !!tools.native_obs_preview },
  ]);
  appendStatusCard(capabilitySummary, "Radar / Satellite", `${formatInt(data.radar_sites?.count)} NEXRAD sites | ${formatInt((data.satellite_products || []).length)} GOES products`, "GOES-19 is selected by default.", [
    { label: "radar", ok: !!tools.radar },
    { label: "radar tiles", ok: !!tools.radar_tiles },
    { label: "sat tiles", ok: !!tools.satellite_tiles },
    { label: "GOES", ok: (data.satellite_products || []).length > 0 },
    { label: "native seq", ok: true },
  ]);
}
function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes)) return "";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let amount = bytes;
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  return `${amount >= 10 || unit === 0 ? amount.toFixed(0) : amount.toFixed(1)} ${units[unit]}`;
}
async function loadDataInventory(show = false) {
  const data = await api("/api/data-inventory");
  state.dataInventory = data;
  renderDataInventory();
  if (show) showResult(data);
  return data;
}
function renderDataInventory() {
  const data = state.dataInventory;
  const summary = $("dataSummary");
  const stores = $("dataStores");
  const recent = $("dataRecent");
  if (!summary || !stores || !recent) return;
  summary.replaceChildren();
  stores.replaceChildren();
  recent.replaceChildren();
  if (!data || !data.ok) {
    appendDataRow(stores, "No inventory", "", "");
    return;
  }
  appendStatusCard(summary, "Roots", `${formatBytes(data.summary?.bytes)} indexed`, data.roots?.cache_dir || "");
  appendStatusCard(summary, "Sections", `${formatInt(data.summary?.section_count)} sections`, `${formatInt(data.summary?.store_count)} reusable stores`);
  appendStatusCard(summary, "Recent", `${formatInt(data.summary?.recent_count)} files`, data.generated_at || "");
  const search = ($("dataSearch")?.value || "").trim().toLowerCase();
  const sectionRows = (data.sections || []).filter((item) => dataRowMatches(item, search));
  for (const item of sectionRows) {
    appendDataRow(
      stores,
      item.label,
      `${item.category} | ${formatBytes(item.bytes)} | ${formatInt(item.files)} files${item.truncated ? " | truncated" : ""}`,
      item.path
    );
  }
  const storeRows = (data.stores || []).filter((item) => dataRowMatches(item, search));
  for (const item of storeRows) {
    const title = item.kind === "wxstore_spatial"
      ? `${item.model} ${item.run}${item.latest ? " latest" : ""}`
      : (item.layer || item.name || item.kind);
    const detail = item.kind === "radar_tile_lane" || item.kind === "satellite_tile_lane"
      ? `${item.kind} | ${formatInt(item.frame_count)} frames | ${item.latest_time || ""}`
      : `${item.kind} | ${formatInt(item.product_count || item.files || 0)} items`;
    appendDataRow(stores, title, detail, item.path, item.manifest);
  }
  const recentRows = (data.recent || []).filter((item) => dataRowMatches(item, search));
  for (const item of recentRows) {
    appendDataRow(recent, item.name, `${formatBytes(item.bytes)} | ${item.mtime || ""}`, item.path, item);
  }
}
function dataRowMatches(item, search) {
  if (!search) return true;
  return JSON.stringify(item).toLowerCase().includes(search);
}
function appendDataRow(parent, title, detail, path, file = null) {
  const row = document.createElement("div");
  row.className = "data-row";
  const heading = document.createElement(file?.url ? "a" : "strong");
  heading.textContent = title || "Item";
  if (file?.url) {
    heading.href = file.url;
    heading.target = "_blank";
    heading.rel = "noreferrer";
  }
  row.appendChild(heading);
  if (detail) {
    const small = document.createElement("small");
    small.textContent = detail;
    row.appendChild(small);
  }
  if (path) {
    const small = document.createElement("small");
    small.textContent = path;
    row.appendChild(small);
  }
  parent.appendChild(row);
}
function setBusy(button, busy) { button.disabled = busy; }
function showResult(value) { $("result").textContent = JSON.stringify(value, null, 2); }
function renderPreviews(previews, targetId, {append = false} = {}) {
  const target = $(targetId);
  if (!append) target.innerHTML = "";
  const rows = append ? [...(previews || [])].reverse() : [...(previews || [])];
  for (const item of rows) {
    const fig = document.createElement("figure");
    const wrap = document.createElement("div");
    wrap.className = "map-image-wrap";
    const img = document.createElement("img");
    img.src = item.url;
    img.alt = item.name;
    if (isMapPreview(item)) {
      img.className = "map-clickable";
      img.dataset.bounds = JSON.stringify(item.bounds || []);
      img.dataset.domain = item.domain || "";
      if (item.forecast_hour !== undefined && item.forecast_hour !== null) {
        img.dataset.forecastHour = item.forecast_hour;
      }
      attachMapPicker(wrap, img);
    }
    const dragBox = document.createElement("div");
    dragBox.className = "map-drag-box";
    wrap.append(img, dragBox);
    const cap = document.createElement("figcaption");
    cap.textContent = item.name;
    fig.append(wrap, cap);
    target.prepend(fig);
  }
}
function renderHtmlArtifacts(artifacts, targetId, {append = false} = {}) {
  const target = $(targetId);
  if (!append) target.innerHTML = "";
  const rows = append ? [...(artifacts || [])].reverse() : [...(artifacts || [])];
  for (const item of rows) {
    const fig = document.createElement("figure");
    const link = document.createElement("a");
    link.className = "artifact-link";
    link.href = item.url;
    link.target = "_blank";
    link.rel = "noreferrer";
    link.textContent = item.name || "Open viewer";
    const cap = document.createElement("figcaption");
    cap.textContent = item.path || item.url || "";
    fig.append(link, cap);
    target.prepend(fig);
  }
}
function displayJobResult(job, result) {
  const kind = job?.kind || "";
  const hasPreviews = Array.isArray(result?.previews) && result.previews.length > 0;
  const hasArtifacts = Array.isArray(result?.html_artifacts) && result.html_artifacts.length > 0;
  if (!hasPreviews && !hasArtifacts) return;
  if (["render", "satellite", "satellite_sequence", "generation_plan", "case_dataset", "wxstore", "wxstore_plot_existing", "ecape_ratio"].includes(kind)) {
    renderPreviews(result.previews || [], "gallery");
    if (hasArtifacts) renderHtmlArtifacts(result.html_artifacts, "gallery", {append: true});
    $("outputGallery").innerHTML = "";
    return;
  }
  if (hasPreviews) renderPreviews(result.previews, "outputGallery", {append: true});
  if (hasArtifacts) renderHtmlArtifacts(result.html_artifacts, "outputGallery", {append: true});
}
function upsertJob(job) {
  if (!job || !job.id) return;
  state.jobs.set(job.id, job);
  renderJobStrip();
}
function renderJobStrip() {
  const jobs = [...state.jobs.values()]
    .sort((a, b) => String(b.created_at || "").localeCompare(String(a.created_at || "")))
    .slice(0, 5);
  $("jobStrip").innerHTML = "";
  for (const job of jobs) {
    const card = document.createElement("div");
    card.className = "job-card";
    const title = document.createElement("strong");
    title.textContent = `${job.kind || "job"} ${job.id || ""}`;
    const status = document.createElement("span");
    status.className = `job-status ${job.status || ""}`;
    status.textContent = job.status || "unknown";
    const meta = document.createElement("small");
    const req = job.request || {};
    const parts = [req.model, req.domain, req.hours || req.forecast_hour, req.product_count ? `${req.product_count} products` : null]
      .filter(Boolean);
    meta.textContent = [parts.join(" | "), job.elapsed_s ? `${job.elapsed_s}s` : null].filter(Boolean).join(" | ");
    const actions = document.createElement("div");
    actions.className = "toolbar";
    const button = document.createElement("button");
    button.textContent = "View";
    button.addEventListener("click", () => viewJob(job.id));
    actions.appendChild(button);
    if (["queued", "running", "cancelling"].includes(job.status || "")) {
      const cancel = document.createElement("button");
      cancel.textContent = job.status === "cancelling" ? "Stopping" : "Cancel";
      cancel.disabled = job.status === "cancelling";
      cancel.addEventListener("click", () => cancelJob(job.id));
      actions.appendChild(cancel);
    }
    card.append(title, status, meta, actions);
    $("jobStrip").appendChild(card);
  }
}
async function viewJob(jobId) {
  const data = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
  const job = data.job || {};
  upsertJob(job);
  const result = job.result || job;
  displayJobResult(job, result);
  showResult(result);
}
async function cancelJob(jobId) {
  const data = await post(`/api/jobs/${encodeURIComponent(jobId)}/cancel`, {});
  if (data.job) upsertJob(data.job);
  showResult(data);
  return data;
}
async function runJob(kind, payload, button, options = {}) {
  setBusy(button, true);
  try {
    const launched = await post("/api/jobs", { kind, payload });
    if (!launched.ok) {
      showResult(launched);
      setBusy(button, false);
      return launched;
    }
    upsertJob(launched.job);
    showResult(launched);
    pollJob(launched.job.id, button, options);
    return launched;
  } catch (err) {
    setBusy(button, false);
    showResult({ ok: false, error: String(err) });
    throw err;
  }
}
async function pollJob(jobId, button, options = {}) {
  if (state.jobPollers.has(jobId)) clearTimeout(state.jobPollers.get(jobId));
  const tick = async () => {
    try {
      const data = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
      const job = data.job;
      upsertJob(job);
      if (!job || ["completed", "failed", "cancelled"].includes(job.status)) {
        state.jobPollers.delete(jobId);
        setBusy(button, false);
        const result = job?.result || data;
        displayJobResult(job, result);
        showResult(result);
        if (options.onDone) options.onDone(job);
        return;
      }
      state.jobPollers.set(jobId, setTimeout(tick, 1250));
    } catch (err) {
      state.jobPollers.delete(jobId);
      setBusy(button, false);
      showResult({ ok: false, error: String(err), job_id: jobId });
    }
  };
  state.jobPollers.set(jobId, setTimeout(tick, 350));
}
function isMapPreview(item) {
  const path = String(item.path || "").replaceAll("\\", "/");
  return path.includes("/studio/maps/");
}
function attachMapPicker(wrap, image) {
  const dragBox = wrap.querySelector(".map-drag-box");
  image.addEventListener("pointerdown", (event) => {
    event.preventDefault();
    image.setPointerCapture?.(event.pointerId);
    const point = pointFromImageEvent(event, image);
    state.mapPick = {
      image,
      dragBox,
      startPoint: point,
      startClientX: event.clientX,
      startClientY: event.clientY,
      moved: false,
    };
    updateDragBox(dragBox, image, event.clientX, event.clientY, event.clientX, event.clientY, false);
  });
  image.addEventListener("pointermove", (event) => {
    const pick = state.mapPick;
    if (!pick || pick.image !== image) return;
    const distance = Math.hypot(event.clientX - pick.startClientX, event.clientY - pick.startClientY);
    if (distance > 6) pick.moved = true;
    updateDragBox(dragBox, image, pick.startClientX, pick.startClientY, event.clientX, event.clientY, pick.moved);
  });
  image.addEventListener("pointerup", async (event) => {
    const pick = state.mapPick;
    if (!pick || pick.image !== image) return;
    state.mapPick = null;
    updateDragBox(dragBox, image, 0, 0, 0, 0, false);
    const endPoint = pointFromImageEvent(event, image);
    if (pick.moved) await handleMapImageDrag(pick.startPoint, endPoint);
    else await handleMapImageClick(endPoint);
  });
  image.addEventListener("pointercancel", () => {
    state.mapPick = null;
    updateDragBox(dragBox, image, 0, 0, 0, 0, false);
  });
}
function updateDragBox(box, image, x1, y1, x2, y2, visible) {
  if (!box) return;
  if (!visible) {
    box.style.display = "none";
    return;
  }
  const rect = image.getBoundingClientRect();
  const left = Math.max(0, Math.min(rect.width, Math.min(x1, x2) - rect.left));
  const top = Math.max(0, Math.min(rect.height, Math.min(y1, y2) - rect.top));
  const right = Math.max(0, Math.min(rect.width, Math.max(x1, x2) - rect.left));
  const bottom = Math.max(0, Math.min(rect.height, Math.max(y1, y2) - rect.top));
  box.style.display = "block";
  box.style.left = `${left}px`;
  box.style.top = `${top}px`;
  box.style.width = `${Math.max(1, right - left)}px`;
  box.style.height = `${Math.max(1, bottom - top)}px`;
}
function activeDomainBounds() {
  const slug = $("domain").value || "conus";
  const domain = state.domains.find((item) => item.slug === slug);
  return (domain && domain.bounds) || [-125, -66, 24, 50];
}
function boundsForImage(image) {
  try {
    const bounds = JSON.parse(image.dataset.bounds || "[]");
    if (Array.isArray(bounds) && bounds.length === 4) return bounds.map(Number);
  } catch (_err) {
    return activeDomainBounds();
  }
  return activeDomainBounds();
}
function pointFromImageEvent(event, image) {
  const bounds = boundsForImage(image);
  const rect = image.getBoundingClientRect();
  const x = Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width));
  const y = Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height));
  const lon = bounds[0] + x * (bounds[1] - bounds[0]);
  const lat = bounds[3] - y * (bounds[3] - bounds[2]);
  const forecastHour = Number(image.dataset.forecastHour);
  return {
    lat,
    lon,
    bounds,
    domain: image.dataset.domain || $("domain").value || "conus",
    forecastHour: Number.isFinite(forecastHour) ? forecastHour : null,
  };
}
async function handleMapImageClick(point) {
  if (document.querySelector(".tabs button.active")?.dataset.tab === "sectionTab") {
    if (updateSectionPick(point)) await renderCrossSection();
    return;
  }
  state.soundingContext = { domain: point.domain, bounds: point.bounds };
  $("soundingLat").value = point.lat.toFixed(3);
  $("soundingLon").value = point.lon.toFixed(3);
  $("soundingHour").value = point.forecastHour ?? $("forecastHour").value ?? 0;
  $("soundingStation").value = `${$("model").value.toUpperCase()} ${point.lat.toFixed(2)},${point.lon.toFixed(2)}`;
  await renderSounding();
}
async function handleMapImageDrag(startPoint, endPoint) {
  if (document.querySelector(".tabs button.active")?.dataset.tab === "sectionTab") {
    $("sectionRoute").value = "custom";
    $("sectionStartLat").value = startPoint.lat.toFixed(3);
    $("sectionStartLon").value = startPoint.lon.toFixed(3);
    $("sectionEndLat").value = endPoint.lat.toFixed(3);
    $("sectionEndLon").value = endPoint.lon.toFixed(3);
    $("sectionHour").value = startPoint.forecastHour ?? endPoint.forecastHour ?? $("forecastHour").value ?? 0;
    state.nextSectionPoint = "start";
    await renderCrossSection();
    return;
  }
  const center = {
    lat: (startPoint.lat + endPoint.lat) / 2,
    lon: (startPoint.lon + endPoint.lon) / 2,
  };
  state.soundingContext = { domain: startPoint.domain || endPoint.domain, bounds: startPoint.bounds || endPoint.bounds };
  const radiusKm = Math.max(1, Math.min(250, haversineKm(center, startPoint)));
  $("soundingLat").value = center.lat.toFixed(3);
  $("soundingLon").value = center.lon.toFixed(3);
  $("soundingHour").value = startPoint.forecastHour ?? endPoint.forecastHour ?? $("forecastHour").value ?? 0;
  $("soundingStation").value = `${$("model").value.toUpperCase()} box ${center.lat.toFixed(2)},${center.lon.toFixed(2)}`;
  $("soundingMethod").value = "box-mean";
  $("soundingBoxKm").value = Math.round(radiusKm);
  await renderSounding();
}
function updateSectionPick(point) {
  const startIsNext = state.nextSectionPoint !== "end";
  $("sectionRoute").value = "custom";
  if (point.forecastHour !== null && point.forecastHour !== undefined) {
    $("sectionHour").value = point.forecastHour;
  }
  if (startIsNext) {
    $("sectionStartLat").value = point.lat.toFixed(3);
    $("sectionStartLon").value = point.lon.toFixed(3);
    state.nextSectionPoint = "end";
  } else {
    $("sectionEndLat").value = point.lat.toFixed(3);
    $("sectionEndLon").value = point.lon.toFixed(3);
    state.nextSectionPoint = "start";
    $("sectionHour").value = point.forecastHour ?? $("sectionHour").value ?? $("forecastHour").value ?? 0;
    return true;
  }
  return false;
}
function haversineKm(a, b) {
  const r = 6371;
  const toRad = (value) => value * Math.PI / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * r * Math.asin(Math.min(1, Math.sqrt(h)));
}
function domainLabel(domain) { return `${domain.label || domain.slug} (${domain.slug})`; }
function renderDomainOptions() {
  const search = $("domainSearch").value.trim().toLowerCase();
  for (const selectId of ["domain", "satDomain", "batchDomain", "caseDomain", "radarTileDomain", "ecapeDomain", "nativeDomain", "pointDomain"]) {
    const select = $(selectId);
    if (!select) continue;
    const previous = select.value || (selectId === "satDomain" ? "pacific_southwest" : (["radarTileDomain", "ecapeDomain", "nativeDomain"].includes(selectId) ? "oklahoma" : "conus"));
    select.innerHTML = "";
    const matches = state.domains.filter((domain) => {
      if (!search || selectId === "satDomain") return true;
      return String(domain.slug || "").toLowerCase().includes(search)
        || String(domain.label || "").toLowerCase().includes(search)
        || (domain.tags || []).some((tag) => String(tag).toLowerCase().includes(search));
    });
    const added = new Set();
    if (!search && selectId === "domain") {
      const common = document.createElement("optgroup");
      common.label = "Common";
      for (const slug of COMMON_DOMAINS) {
        const domain = state.domains.find((item) => item.slug === slug);
        if (!domain || added.has(domain.slug)) continue;
        common.appendChild(domainOption(domain));
        added.add(domain.slug);
      }
      if (common.children.length) select.appendChild(common);
    }
    const byKind = new Map();
    for (const domain of matches) {
      if (added.has(domain.slug)) continue;
      const kind = String(domain.kind || "other").replaceAll("_", " ");
      if (!byKind.has(kind)) byKind.set(kind, []);
      byKind.get(kind).push(domain);
    }
    for (const [kind, domains] of [...byKind.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      const group = document.createElement("optgroup");
      group.label = kind;
      for (const domain of domains.sort((a, b) => domainLabel(a).localeCompare(domainLabel(b)))) {
        group.appendChild(domainOption(domain));
      }
      select.appendChild(group);
    }
    const values = [...select.options].map((option) => option.value);
    select.value = values.includes(previous) ? previous : (values.includes("conus") ? "conus" : values[0]);
  }
}
function domainOption(domain) {
  const opt = document.createElement("option");
  opt.value = domain.slug;
  opt.textContent = domainLabel(domain);
  return opt;
}
function renderRadarMap() {
  renderRadarMapInto($("radarMap"));
  renderRadarMapInto($("radarStageMap"));
}
function renderRadarMapInto(box) {
  if (!box) return;
  const basemap = state.radarBasemap || {};
  const sites = (basemap.sites && basemap.sites.length ? basemap.sites : state.radarSites) || [];
  const width = Number(basemap.width || 1000);
  const height = Number(basemap.height || 590);
  const bounds = basemap.bounds || [-126, -66, 24, 50];
  const project = (site) => {
    if (Number.isFinite(Number(site.x)) && Number.isFinite(Number(site.y))) {
      return [Number(site.x), Number(site.y)];
    }
    const x = ((Number(site.lon) - bounds[0]) / (bounds[1] - bounds[0])) * width;
    const y = ((bounds[3] - Number(site.lat)) / (bounds[3] - bounds[2])) * height;
    return [x, y];
  };
  let html = "";
  if (basemap.url) {
    html += `<img class="radar-basemap" src="${basemap.url}" alt="">`;
  }
  html += `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="CONUS radar sites">`;
  if (!basemap.url) {
    html += `<rect width="${width}" height="${height}" fill="#eef5f6"/>`;
  }
  for (const site of sites) {
    const [x, y] = project(site);
    const active = String(site.id).toUpperCase() === $("radarSite").value.trim().toUpperCase() ? " active" : "";
    html += `<circle class="radar-site${active}" data-site="${site.id}" tabindex="0" role="button" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="3.8"><title>${site.label || site.id}</title></circle>`;
  }
  html += "</svg>";
  box.innerHTML = html;
  for (const marker of box.querySelectorAll(".radar-site")) {
    const selectSite = () => {
      $("radarSite").value = marker.dataset.site;
      $("radarLat").value = "";
      $("radarLon").value = "";
      renderRadarMap();
    };
    marker.addEventListener("click", selectSite);
    marker.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectSite();
      }
    });
  }
}
function updateWorkspaceMode(tabId) {
  $("radarStage").classList.toggle("active", tabId === "radar");
}
function populateSampleMethods(rows) {
  $("soundingMethod").innerHTML = "";
  for (const row of rows || []) {
    const opt = document.createElement("option");
    opt.value = row.slug;
    opt.textContent = row.label;
    $("soundingMethod").appendChild(opt);
  }
}
function populateSectionRoutes(rows) {
  $("sectionRoute").innerHTML = "";
  for (const row of rows || []) {
    const opt = document.createElement("option");
    opt.value = row.id;
    opt.textContent = row.name;
    opt.dataset.startLat = row.start?.[0];
    opt.dataset.startLon = row.start?.[1];
    opt.dataset.endLat = row.end?.[0];
    opt.dataset.endLon = row.end?.[1];
    $("sectionRoute").appendChild(opt);
  }
  applySectionRoute();
}
function applySectionRoute() {
  const opt = $("sectionRoute").selectedOptions[0];
  if (!opt || $("sectionRoute").value === "custom") return;
  $("sectionStartLat").value = opt.dataset.startLat;
  $("sectionStartLon").value = opt.dataset.startLon;
  $("sectionEndLat").value = opt.dataset.endLat;
  $("sectionEndLon").value = opt.dataset.endLon;
}
function applySizePreset() {
  const preset = $("sizePreset").value;
  if (preset === "custom") return;
  const [width, height] = preset.split("x");
  $("width").value = width;
  $("height").value = height;
}
async function loadBootstrap() {
  const data = await api("/api/bootstrap");
  state.bootstrap = data;
  state.domains = data.domains.domains || [];
  $("satelliteId").value = data.satellite_default || "goes19";
  for (const job of data.jobs?.jobs || []) upsertJob(job);
  $("statusDot").classList.toggle("ok", true);
  $("statusText").textContent = `rustwx ${data.doctor.rustwx_version} | ${data.doctor.plot_style} | ${data.models.count} models`;
  state.radarSites = data.radar_sites?.sites || [];
  state.radarBasemap = data.radar_basemap || null;
  const today = new Date().toISOString().slice(0, 10);
  if (!$("caseStart").value) $("caseStart").value = today;
  if (!$("caseEnd").value) $("caseEnd").value = today;
  $("model").innerHTML = "";
  for (const model of data.models.models || []) {
    const opt = document.createElement("option");
    opt.value = model.id;
    opt.textContent = model.id;
    if (model.id === "hrrr") opt.selected = true;
    $("model").appendChild(opt);
  }
  renderDomainOptions();
  renderStaticProductList("satProductList", data.satellite_products.map((slug) => ({ slug, label: productLabel(slug), kind: "sat" })), state.selectedSatellite);
  renderStaticProductList("radarProductList", data.radar_products, state.selectedRadar);
  renderStaticProductList("sectionProductList", data.cross_section_products, state.selectedSection);
  populateSampleMethods(data.sounding_sample_methods);
  populateSectionRoutes(data.cross_section_routes);
  renderRadarMap();
  renderStatusPanel();
  loadDataInventory(false).catch((err) => console.warn(err));
  updateWorkspaceMode(document.querySelector(".tabs button.active")?.dataset.tab || "maps");
  await loadProducts();
}
async function loadProducts() {
  const model = $("model").value || "hrrr";
  state.products = await api(`/api/products?model=${encodeURIComponent(model)}`);
  state.selectedProducts.clear();
  state.selectedStore.clear();
  state.selectedExistingStore.clear();
  state.wxstoreInspect = null;
  const def = (state.bootstrap.models.models || []).find((item) => item.id === model)?.default_render_product || "2m_temperature_10m_winds";
  state.selectedProducts.add(def);
  state.selectedStore.add(def);
  renderProductList();
  renderStoreProductList();
  renderExistingStoreProductList();
  renderStatusPanel();
}
function renderProductList() {
  const list = $("productList");
  list.innerHTML = "";
  if (!state.products || !state.products.ok) return;
  const wantedKind = $("kind").value;
  const search = $("productSearch").value.trim().toLowerCase();
  const rows = [];
  for (const [kind, products] of Object.entries(state.products.groups || {})) {
    if (wantedKind && kind !== wantedKind) continue;
    for (const slug of products) {
      if (search && !slug.toLowerCase().includes(search)) continue;
      rows.push({ slug, label: productLabel(slug), kind });
    }
  }
  state.visibleProducts = rows.map((row) => row.slug);
  renderStaticProductList("productList", rows, state.selectedProducts);
}
function renderStoreProductList() {
  const list = $("storeProductList");
  list.innerHTML = "";
  if (!state.products || !state.products.ok) return;
  const wantedKind = $("storeKind").value;
  const search = $("storeProductSearch").value.trim().toLowerCase();
  const rows = [];
  for (const [kind, products] of Object.entries(state.products.groups || {})) {
    if (wantedKind && kind !== wantedKind) continue;
    for (const slug of products) {
      if (search && !slug.toLowerCase().includes(search)) continue;
      rows.push({ slug, label: productLabel(slug), kind });
    }
  }
  state.visibleStoreProducts = rows.map((row) => row.slug);
  renderStaticProductList("storeProductList", rows, state.selectedStore);
}
function renderExistingStoreProductList() {
  const list = $("storeExistingProductList");
  const summary = $("storeExistingSummary");
  list.innerHTML = "";
  if (!state.wxstoreInspect || !state.wxstoreInspect.ok) {
    state.visibleExistingStoreProducts = [];
    summary.textContent = state.wxstoreInspect?.error || "No store loaded.";
    return;
  }
  const rows = (state.wxstoreInspect.plot_products || state.wxstoreInspect.products || []).map((row) => ({
    slug: row.slug,
    label: row.label || productLabel(row.slug),
    kind: row.kind || "wxa",
  }));
  state.visibleExistingStoreProducts = rows.map((row) => row.slug);
  if (!state.selectedExistingStore.size && rows.length) state.selectedExistingStore.add(rows[0].slug);
  summary.textContent = `${state.wxstoreInspect.model} | ${state.wxstoreInspect.run} | ${rows.length} plottable products`;
  renderStaticProductList("storeExistingProductList", rows, state.selectedExistingStore);
}
function renderStaticProductList(id, rows, selectedSet) {
  const list = $(id);
  list.innerHTML = "";
  for (const row of rows) {
    const item = document.createElement("label");
    item.className = "product-item";
    const check = document.createElement("input");
    check.type = "checkbox";
    check.checked = selectedSet.has(row.slug);
    check.addEventListener("change", () => {
      if (check.checked) selectedSet.add(row.slug);
      else selectedSet.delete(row.slug);
    });
    const name = document.createElement("span");
    name.textContent = row.label || productLabel(row.slug);
    const badge = document.createElement("span");
    badge.className = "badge";
    badge.textContent = row.kind || row.slug;
    item.append(check, name, badge);
    list.appendChild(item);
  }
}
async function renderMaps() {
  const btn = $("renderBtn");
  const hours = parseHourList($("renderHours").value, $("forecastHour").value);
  return runJob("render", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    forecast_hour: Number(hours[0] ?? $("forecastHour").value ?? 0),
    forecast_hours: hours,
    domain: $("domain").value,
    products: [...state.selectedProducts],
    width: Number($("width").value || 1600),
    height: Number($("height").value || 1100),
  }, btn);
}
async function prepareData() {
  const btn = $("prepareData");
  const hours = parseHourList($("prepareHours").value, $("forecastHour").value);
  return runJob("prepare_data", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    forecast_hour: Number(hours[0] ?? $("forecastHour").value ?? 0),
    forecast_hours: hours,
    domain: $("domain").value,
    products: [...state.selectedProducts],
  }, btn);
}
async function renderSatellite() {
  const btn = $("satRender");
  return runJob("satellite", {
    satellite: $("satelliteId").value,
    sector: $("satSector").value,
    domain: $("satDomain").value,
    products: [...state.selectedSatellite],
    width: Number($("width").value || 1600),
    height: Number($("height").value || 1100),
    scan_lookback_hours: Number($("satLookback").value || 3),
    sequence_count: Number($("satSequenceCount").value || 1),
    sequence_gif: !!$("satGif").value,
    auto_bounds: ["full_disk", "meso1", "meso2"].includes($("satSector").value),
  }, btn);
}
async function renderNativeSatelliteSequence() {
  const btn = $("satNativeRender");
  return runJob("satellite_sequence", {
    satellite: $("satelliteId").value,
    sector: $("satSector").value,
    domain: $("satDomain").value,
    products: [...state.selectedSatellite],
    width: Number($("width").value || 1600),
    height: Number($("height").value || 1100),
    scan_lookback_hours: Number($("satLookback").value || 3),
    latest_count: Number($("satSequenceCount").value || 4),
    downsample: Number($("satNativeDownsample").value || 1),
    min_step_minutes: $("satNativeMinStep").value ? Number($("satNativeMinStep").value) : null,
  }, btn);
}
async function renderSatelliteTileLoop() {
  const btn = $("satTileLoopRender");
  return runJob("satellite_tile_loop", {
    satellite: $("satelliteId").value,
    sector: $("satSector").value,
    domain: $("satDomain").value,
    layer: $("satTileLayer").value,
    scan_lookback_hours: Number($("satLookback").value || 3),
    latest_count: Number($("satTileFrames").value || 3),
    min_zoom: Number($("satTileMinZoom").value || 4),
    max_zoom: Number($("satTileMaxZoom").value || 6),
    opacity: Number($("satTileOpacity").value || .92),
    png_compression: $("satTileCompression").value,
    min_step_minutes: $("satNativeMinStep").value ? Number($("satNativeMinStep").value) : null,
  }, btn);
}
async function runGenerationPlan(actionOverride = null) {
  const btn = actionOverride === "plan" ? $("batchPlan") : $("batchRun");
  const hours = parseHourList($("batchHours").value, $("forecastHour").value);
  return runJob("generation_plan", {
    action: actionOverride || $("batchAction").value,
    models_mode: $("batchModelsMode").value,
    models: $("batchModels").value,
    current_model: $("model").value,
    product_mode: $("batchProductMode").value,
    product_kind: $("batchProductKind").value,
    products: [...state.selectedProducts],
    source: $("source").value,
    run_str: $("run").value,
    domain: $("batchDomain").value || $("domain").value,
    forecast_hour: Number(hours[0] ?? $("forecastHour").value ?? 0),
    forecast_hours: hours,
    max_products_per_model: $("batchMaxProducts").value ? Number($("batchMaxProducts").value) : null,
    width: Number($("width").value || 1600),
    height: Number($("height").value || 1100),
  }, btn);
}
async function runCaseDataset(actionOverride = null) {
  const action = actionOverride || $("caseAction").value;
  const mode = $("caseMode").value;
  const btn = actionOverride === "plan" ? $("casePlan") : $("caseRun");
  const hours = parseHourList($("caseHours").value, $("forecastHour").value);
  return runJob("case_dataset", {
    action,
    mode,
    model: $("model").value,
    source: $("source").value,
    start_date: $("caseStart").value,
    end_date: $("caseEnd").value || $("caseStart").value,
    cycles: $("caseCycles").value,
    forecast_hour: Number(hours[0] ?? $("forecastHour").value ?? 0),
    forecast_hours: hours,
    domain: $("caseDomain").value || $("domain").value,
    products: [...state.selectedProducts],
    profile_points: $("casePoints").value,
    limit: $("caseLimit").value ? Number($("caseLimit").value) : null,
    width: Number($("width").value || 1600),
    height: Number($("height").value || 1100),
  }, btn);
}
async function renderRadar() {
  const btn = $("radarRender");
  return runJob("radar", {
    site: $("radarSite").value,
    lat: $("radarLat").value,
    lon: $("radarLon").value,
    products: [...state.selectedRadar],
    size: Number($("radarSize").value || 1024),
    render_mode: $("radarMode").value,
    dealias: !!$("radarDealias").value,
  }, btn);
}
function radarTilePayload() {
  const qc = $("radarTileQc").value;
  const selected = [...state.selectedRadar].filter((item) => item && item !== "all");
  return {
    site: $("radarSite").value,
    lat: $("radarLat").value,
    lon: $("radarLon").value,
    product: selected[0] || "ref",
    products: selected.length ? selected : ["ref"],
    domain: $("radarTileDomain").value || "oklahoma",
    min_zoom: Number($("radarTileMinZoom").value || 5),
    max_zoom: Number($("radarTileMaxZoom").value || 7),
    color_table: $("radarTileColor").value,
    supersample: Number($("radarTileSupersample").value || 1),
    png_compression: $("radarTileCompression").value,
    all_tilts: !!$("radarTileAllTilts").value,
    clip_to_bounds: !!$("radarTileClip").value,
    keep_empty_tiles: !!$("radarTileKeepEmpty").value,
    reflectivity_despeckle: qc === "despeckle",
    velocity_quality_filter: qc === "velocity",
    dealias: !!$("radarDealias").value,
  };
}
async function renderRadarTiles() {
  const btn = $("radarTilesRender");
  return runJob("radar_tiles", radarTilePayload(), btn);
}
async function renderRadarTileLoop() {
  const btn = $("radarTileLoopRender");
  return runJob("radar_tile_loop", {
    ...radarTilePayload(),
    latest_count: Number($("radarTileLoopFrames").value || 4),
    loop_delay_ms: Number($("radarTileLoopDelay").value || 650),
  }, btn);
}
async function samplePoint() {
  const btn = $("pointSample");
  return runJob("meteogram", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    lat: $("pointLat").value,
    lon: $("pointLon").value,
    forecast_hour_start: Number($("pointStart").value || 0),
    forecast_hour_end: Number($("pointEnd").value || 6),
    method: $("pointMethod").value,
    store_id: $("pointStoreId").value,
    variables: $("pointVariables").value,
  }, btn);
}
async function warmPointStore() {
  const btn = $("pointWarmStore");
  return runJob("meteogram_store", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    domain: $("pointDomain").value || $("domain").value,
    forecast_hour_start: Number($("pointStart").value || 0),
    forecast_hour_end: Number($("pointEnd").value || 6),
    variables: $("pointVariables").value,
  }, btn, {
    onDone: (job) => {
      const storeId = job?.result?.store_id || job?.result?.report?.store_id;
      if (storeId) $("pointStoreId").value = storeId;
    },
  });
}
async function renderSounding() {
  const btn = $("soundingRender");
  const context = state.soundingContext || { domain: $("domain").value, bounds: activeDomainBounds() };
  return runJob("sounding", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    domain: context.domain || $("domain").value,
    bounds: context.bounds || activeDomainBounds(),
    forecast_hour: Number($("soundingHour").value || $("forecastHour").value || 0),
    lat: $("soundingLat").value,
    lon: $("soundingLon").value,
    station_id: $("soundingStation").value,
    sample_method: $("soundingMethod").value,
    data_mode: $("soundingDataMode").value,
    box_radius_km: Number($("soundingBoxKm").value || 25),
    crop_radius_deg: Number($("soundingCropDeg").value || 1),
    include_column: !!$("soundingColumn").value,
  }, btn);
}
async function preparePressureStore(force = false) {
  const btn = $("prepareStore");
  const context = state.soundingContext || { domain: $("domain").value, bounds: activeDomainBounds() };
  return runJob("pressure_store", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    domain: context.domain || $("domain").value,
    bounds: context.bounds || activeDomainBounds(),
    forecast_hour: Number($("soundingHour").value || $("forecastHour").value || 0),
    lat: $("soundingLat").value,
    lon: $("soundingLon").value,
    force,
  }, btn);
}
async function renderCrossSection() {
  const btn = $("sectionRender");
  return runJob("cross_section", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    forecast_hour: Number($("sectionHour").value || $("forecastHour").value || 0),
    route_id: $("sectionRoute").value,
    start_lat: $("sectionStartLat").value,
    start_lon: $("sectionStartLon").value,
    end_lat: $("sectionEndLat").value,
    end_lon: $("sectionEndLon").value,
    products: [...state.selectedSection],
    spacing_km: Number($("sectionSpacing").value || 5),
    top_pressure_hpa: Number($("sectionTop").value || 100),
    width: Number($("width").value || 1400),
    height: Math.max(520, Math.round(Number($("height").value || 900) * 0.75)),
  }, btn);
}
async function runWxStore() {
  const btn = $("storeRun");
  return runJob("wxstore", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    domain: $("domain").value,
    hours: $("storeHours").value,
    products: [...state.selectedStore],
    jobs: Number($("storeJobs").value || 1),
    import_wxa: !!$("storeImport").value,
    render_plots: !!$("storePlot").value,
    max_products: $("storeMaxProducts").value ? Number($("storeMaxProducts").value) : null,
    png_compression: $("storeCompression").value,
    width: Number($("width").value || 1600),
    height: Number($("height").value || 900),
  }, btn);
}
function wxstoreServicePayload(actionOverride = null) {
  return {
    action: actionOverride || $("wxstoreServiceAction").value,
    base_url: $("wxstoreServiceUrl").value,
    endpoint: $("wxstoreServiceEndpoint").value,
    model: $("model").value,
    run: $("wxstoreServiceRun").value,
    member: $("wxstoreServiceMember").value,
    variable: $("wxstoreServiceVariable").value,
    variables: $("wxstoreServiceVariable").value,
    forecast_hour: Number((String($("wxstoreServiceHours").value || "0").split(/[,-]/)[0]) || 0),
    hours: $("wxstoreServiceHours").value,
    forecast_hours: $("wxstoreServiceHours").value,
    lat: $("wxstoreServiceLat").value,
    lon: $("wxstoreServiceLon").value,
    kind: $("wxstoreServiceKind").value,
    q: $("wxstoreServiceQuery").value,
    limit: Number($("wxstoreServiceLimit").value || 25),
    radius_km: $("wxstoreServiceRadius").value ? Number($("wxstoreServiceRadius").value) : null,
  };
}
async function runWxStoreService(actionOverride = null, buttonId = "wxstoreServiceQueryRun") {
  const btn = $(buttonId);
  setBusy(btn, true);
  try {
    const data = await post("/api/wxstore-service", wxstoreServicePayload(actionOverride));
    showResult(data);
    return data;
  } catch (err) {
    showResult({ ok: false, error: String(err) });
    throw err;
  } finally {
    setBusy(btn, false);
  }
}
async function inspectWxStore() {
  const btn = $("storeInspect");
  setBusy(btn, true);
  try {
    const data = await post("/api/wxstore-inspect", {
      model: $("model").value,
      run: $("storeExistingRun").value,
      member: $("storeExistingMember").value,
    });
    state.wxstoreInspect = data;
    state.selectedExistingStore.clear();
    if (data.run) $("storeExistingRun").value = data.run;
    renderExistingStoreProductList();
    showResult(data);
    return data;
  } finally {
    setBusy(btn, false);
  }
}
async function plotExistingWxStore() {
  const btn = $("storePlotExisting");
  if (!state.wxstoreInspect || state.wxstoreInspect.run !== $("storeExistingRun").value) {
    await inspectWxStore();
  }
  return runJob("wxstore_plot_existing", {
    model: $("model").value,
    run: $("storeExistingRun").value,
    member: $("storeExistingMember").value,
    domain: $("domain").value,
    use_domain_bounds: $("storeExistingView").value === "domain",
    hours: $("storeExistingHours").value,
    products: [...state.selectedExistingStore],
    jobs: Number($("storeJobs").value || 1),
    max_products: $("storeExistingMaxProducts").value ? Number($("storeExistingMaxProducts").value) : null,
    png_compression: $("storeExistingCompression").value,
    width: Number($("width").value || 1600),
    height: Number($("height").value || 900),
  }, btn);
}
function nativeDatasetPayload() {
  return {
    dataset_name: $("nativeDatasetName").value,
    case: $("nativeCase").value,
    tile_grid: $("nativeTileGrid").value,
    domain: $("nativeDomain").value,
    grid_size: Number($("nativeGridSize").value || 512),
    history_steps: Number($("nativeHistory").value || 3),
    forecast_step_frames: 1,
    hrrr_fields: $("nativeHrrrFields").value,
    mrms_fields: $("nativeMrmsFields").value,
    goes_channels: $("nativeGoesChannels").value,
    level2_products: $("nativeLevel2Products").value,
    plan_path: $("nativePlanPath").value,
    out: $("nativePlanPath").value,
    allow_missing_sources: true,
    materialize: $("nativeRunMode").value === "materialize",
    fetch_hrrr: $("nativeRunMode").value === "materialize" && !!$("nativeFetch").value,
    fetch_obs: $("nativeRunMode").value === "materialize" && !!$("nativeFetch").value,
    fetch_radar: $("nativeRunMode").value === "materialize" && !!$("nativeFetch").value,
  };
}
async function runEcapeProfile() {
  const btn = $("ecapeProfile");
  return runJob("ecape_profile", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    forecast_hour: Number($("ecapeHour").value || 1),
    lat: $("ecapeLat").value,
    lon: $("ecapeLon").value,
    crop_radius_deg: Number($("ecapeCropDeg").value || 1),
    include_input_column: !!$("ecapeColumn").value,
  }, btn);
}
async function runEcapeGrid() {
  const btn = $("ecapeGrid");
  return runJob("ecape_grid", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    forecast_hour: Number($("ecapeHour").value || 1),
    domain: $("ecapeDomain").value,
    write_csv: !!$("ecapeCsv").value,
  }, btn);
}
async function runEcapeRatio() {
  const btn = $("ecapeRatio");
  return runJob("ecape_ratio", {
    model: $("model").value,
    source: $("source").value,
    run_str: $("run").value,
    forecast_hour: Number($("ecapeHour").value || 1),
    forecast_hours: [Number($("ecapeHour").value || 1)],
    domain: $("ecapeDomain").value,
    parcel: $("ecapeParcel").value,
    include_native_ratio: false,
    width: Number($("width").value || 1600),
    height: Number($("height").value || 1100),
  }, btn);
}
async function runNativePlan() {
  const btn = $("nativePlan");
  return runJob("native_dataset_plan", nativeDatasetPayload(), btn, {
    onDone: (job) => {
      const planPath = job?.result?.plan_path;
      if (planPath) $("nativePlanPath").value = planPath;
    },
  });
}
async function runNativeDataset() {
  const btn = $("nativeRun");
  return runJob("native_dataset_run", nativeDatasetPayload(), btn, {
    onDone: (job) => {
      const planPath = job?.result?.plan_path;
      if (planPath) $("nativePlanPath").value = planPath;
    },
  });
}
async function runNativeObsPreview() {
  const btn = $("nativeObsPreview");
  return runJob("native_obs_preview", {
    kind: $("nativeObsKind").value,
    input: $("nativeObsInput").value,
    size: Number($("nativeObsSize").value || 768),
    channel: $("nativeObsChannel").value,
    product: $("nativeObsProduct").value,
    radar_site: $("nativeObsRadar").value,
    dealias: $("nativeObsDealias").value,
  }, btn);
}
for (const button of document.querySelectorAll(".tabs button")) {
  button.addEventListener("click", () => {
    document.querySelectorAll(".tabs button").forEach((item) => item.classList.remove("active"));
    document.querySelectorAll(".tab-panel").forEach((item) => item.classList.remove("active"));
    button.classList.add("active");
    $(button.dataset.tab).classList.add("active");
    updateWorkspaceMode(button.dataset.tab);
  });
}
$("model").addEventListener("change", loadProducts);
$("kind").addEventListener("change", renderProductList);
$("productSearch").addEventListener("input", renderProductList);
$("storeKind").addEventListener("change", renderStoreProductList);
$("storeProductSearch").addEventListener("input", renderStoreProductList);
$("domainSearch").addEventListener("input", renderDomainOptions);
$("dataSearch").addEventListener("input", renderDataInventory);
$("dataRefresh").addEventListener("click", () => loadDataInventory(true));
$("domain").addEventListener("change", () => { state.soundingContext = null; });
$("sizePreset").addEventListener("change", applySizePreset);
$("width").addEventListener("input", () => { $("sizePreset").value = "custom"; });
$("height").addEventListener("input", () => { $("sizePreset").value = "custom"; });
$("preset2m").addEventListener("click", () => { state.selectedProducts = new Set(["2m_temperature_10m_winds"]); renderProductList(); });
$("presetSevere").addEventListener("click", () => { state.selectedProducts = new Set(["sbcape", "mlcape", "srh_0_3km", "stp_fixed"]); renderProductList(); });
$("selectVisibleProducts").addEventListener("click", () => { for (const slug of state.visibleProducts) state.selectedProducts.add(slug); renderProductList(); });
$("clearProducts").addEventListener("click", () => { state.selectedProducts.clear(); renderProductList(); });
$("storePreset2m").addEventListener("click", () => { state.selectedStore = new Set(["2m_temperature_10m_winds"]); renderStoreProductList(); });
$("storeSelectVisible").addEventListener("click", () => { for (const slug of state.visibleStoreProducts) state.selectedStore.add(slug); renderStoreProductList(); });
$("storeClearProducts").addEventListener("click", () => { state.selectedStore.clear(); renderStoreProductList(); });
$("satGeo").addEventListener("click", () => { state.selectedSatellite = new Set(["goes_geocolor"]); renderStaticProductList("satProductList", state.bootstrap.satellite_products.map((slug) => ({ slug, label: productLabel(slug), kind: "sat" })), state.selectedSatellite); });
$("satBand13").addEventListener("click", () => { state.selectedSatellite = new Set(["goes_abi_band_13"]); renderStaticProductList("satProductList", state.bootstrap.satellite_products.map((slug) => ({ slug, label: productLabel(slug), kind: "sat" })), state.selectedSatellite); });
$("prepareData").addEventListener("click", prepareData);
$("renderBtn").addEventListener("click", renderMaps);
$("satRender").addEventListener("click", renderSatellite);
$("satNativeRender").addEventListener("click", renderNativeSatelliteSequence);
$("satTileLoopRender").addEventListener("click", renderSatelliteTileLoop);
$("batchPlan").addEventListener("click", () => runGenerationPlan("plan"));
$("batchRun").addEventListener("click", () => runGenerationPlan(null));
$("casePlan").addEventListener("click", () => runCaseDataset("plan"));
$("caseRun").addEventListener("click", () => runCaseDataset(null));
$("radarRender").addEventListener("click", renderRadar);
$("radarTilesRender").addEventListener("click", renderRadarTiles);
$("radarTileLoopRender").addEventListener("click", renderRadarTileLoop);
$("radarSite").addEventListener("input", renderRadarMap);
$("prepareStore").addEventListener("click", () => preparePressureStore(false));
$("soundingRender").addEventListener("click", renderSounding);
$("soundingLat").addEventListener("input", () => { state.soundingContext = null; });
$("soundingLon").addEventListener("input", () => { state.soundingContext = null; });
$("sectionRoute").addEventListener("change", applySectionRoute);
$("sectionRender").addEventListener("click", renderCrossSection);
$("storeRun").addEventListener("click", runWxStore);
$("wxstoreServiceStart").addEventListener("click", () => runWxStoreService("start", "wxstoreServiceStart"));
$("wxstoreServiceQueryRun").addEventListener("click", () => runWxStoreService(null, "wxstoreServiceQueryRun"));
$("storeInspect").addEventListener("click", inspectWxStore);
$("storePlotExisting").addEventListener("click", plotExistingWxStore);
$("ecapeProfile").addEventListener("click", runEcapeProfile);
$("ecapeGrid").addEventListener("click", runEcapeGrid);
$("ecapeRatio").addEventListener("click", runEcapeRatio);
$("nativePlan").addEventListener("click", runNativePlan);
$("nativeRun").addEventListener("click", runNativeDataset);
$("nativeObsPreview").addEventListener("click", runNativeObsPreview);
$("pointSample").addEventListener("click", samplePoint);
$("pointWarmStore").addEventListener("click", warmPointStore);
loadBootstrap().catch((err) => {
  $("statusText").textContent = String(err);
  showResult({ ok: false, error: String(err) });
});
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(run_cli())
