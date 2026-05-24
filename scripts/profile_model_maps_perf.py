#!/usr/bin/env python3
"""Profile RustWx Model Maps pipelines through the local web API.

This script launches an isolated Model Maps server, runs focused one-hour
scenarios, samples process memory, and writes JSON + Markdown summaries.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover - optional profiler dependency
    psutil = None


FORECASTING_PRODUCTS = [
    "500mb_height_winds",
    "mslp_10m_winds",
    "2m_temperature_10m_winds",
    "2m_dewpoint_10m_winds",
    "precipitable_water",
    "composite_reflectivity",
    "sbcape",
    "mlcape",
    "bulk_shear_0_6km",
    "srh_0_3km",
    "stp_fixed",
    "qpf_1h",
    "qpf_total",
    "total_qpf",
]


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                pass
    return total


def fmt_bytes(value: int | float | None) -> str:
    if value is None:
        return "-"
    value = float(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit = 0
    while value >= 1024 and unit < len(units) - 1:
        value /= 1024
        unit += 1
    return f"{value:.1f} {units[unit]}" if unit else f"{int(value)} B"


def proc_rss_tree(pid: int) -> int:
    if psutil is None:
        return 0
    try:
        root = psutil.Process(pid)
    except psutil.Error:
        return 0
    procs = [root]
    try:
        procs.extend(root.children(recursive=True))
    except psutil.Error:
        pass
    total = 0
    for proc in procs:
        try:
            total += int(proc.memory_info().rss)
        except psutil.Error:
            pass
    return total


class ApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def get(self, path: str, *, timeout: float = 20.0) -> dict[str, Any]:
        with urllib.request.urlopen(self.base_url + path, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def post(self, path: str, payload: dict[str, Any], *, timeout: float = 20.0) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))


def wait_ready(client: ApiClient, timeout_s: float = 60.0) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            return client.get("/api/bootstrap", timeout=3)
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"server did not become ready: {last_error}")


def write_status(path: Path, payload: dict[str, Any]) -> None:
    payload = {**payload, "updated_at": now_iso()}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_job(
    *,
    client: ApiClient,
    server_pid: int,
    status_path: Path,
    name: str,
    kind: str,
    payload: dict[str, Any],
    poll_s: float = 1.0,
) -> dict[str, Any]:
    started = time.time()
    launch = client.post("/api/jobs", {"kind": kind, "payload": payload}, timeout=30)
    job_id = launch["job"]["id"]
    peak_rss = proc_rss_tree(server_pid)
    first_preview_s: float | None = None
    last: dict[str, Any] = launch
    while True:
        time.sleep(poll_s)
        peak_rss = max(peak_rss, proc_rss_tree(server_pid))
        last = client.get(f"/api/jobs/{job_id}", timeout=30)
        job = last.get("job") or {}
        preview_count = int(job.get("preview_count") or 0)
        elapsed = round(time.time() - started, 2)
        if first_preview_s is None and preview_count > 0:
            first_preview_s = elapsed
        write_status(
            status_path,
            {
                "phase": "running",
                "scenario": name,
                "job_id": job_id,
                "job_status": job.get("status"),
                "elapsed_s": elapsed,
                "first_preview_s": first_preview_s,
                "preview_count": preview_count,
                "progress": job.get("progress"),
                "peak_rss_bytes": peak_rss,
            },
        )
        if job.get("status") in {"completed", "failed", "cancelled"}:
            break
    job = last.get("job") or {}
    return {
        "name": name,
        "kind": kind,
        "job_id": job_id,
        "status": job.get("status"),
        "ok": job.get("ok"),
        "elapsed_s": round(time.time() - started, 2),
        "first_preview_s": first_preview_s,
        "preview_count": int(job.get("preview_count") or 0),
        "peak_rss_bytes": peak_rss,
        "request": job.get("request"),
        "progress": job.get("progress"),
        "result": job.get("result"),
        "error": job.get("error"),
    }


def result_elapsed(result: dict[str, Any] | None) -> float | None:
    if not result:
        return None
    for key in ("ui_elapsed_s", "elapsed_s", "result_elapsed_s"):
        value = result.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def summarize_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    result = scenario.get("result") or {}
    row: dict[str, Any] = {
        "name": scenario["name"],
        "kind": scenario["kind"],
        "status": scenario.get("status"),
        "ok": scenario.get("ok"),
        "wall_s": scenario.get("elapsed_s"),
        "result_s": result_elapsed(result),
        "first_preview_s": scenario.get("first_preview_s"),
        "preview_count": scenario.get("preview_count"),
        "peak_rss_bytes": scenario.get("peak_rss_bytes"),
    }
    if scenario["kind"] == "prepare_data":
        row.update(
            {
                "planned_fetch_count": result.get("planned_fetch_count"),
                "cache_hits": result.get("cache_hits"),
                "skipped_fetch_count": result.get("skipped_fetch_count"),
                "total_bytes": result.get("total_bytes"),
            }
        )
    if scenario["kind"] == "render":
        row["batch_count"] = result.get("batch_count")
        row["rendered_count"] = len(result.get("previews") or [])
        row["render_batches"] = [
            {
                "total_ms": batch.get("total_ms"),
                "shared": batch.get("shared_timing"),
                "fanout": batch.get("fanout_timing"),
                "requested": batch.get("requested"),
            }
            for batch in (result.get("batches") or [])
            if isinstance(batch, dict)
        ]
    if scenario["kind"] == "pressure_store":
        report = result.get("report") or {}
        row["cache_hit"] = result.get("cache_hit")
        row["store_bytes"] = ((report.get("files") or {}).get("total_bytes") if isinstance(report, dict) else None)
        row["loaded_hours"] = report.get("loaded_hours") if isinstance(report, dict) else None
    if scenario["kind"] == "sounding":
        sounding = result.get("sounding") or {}
        timing = sounding.get("timing") if isinstance(sounding, dict) else None
        row["backend"] = result.get("backend")
        row["sounding_timing"] = timing
        row["pressure_store_cache_hit"] = ((result.get("pressure_store") or {}).get("cache_hit") if isinstance(result.get("pressure_store"), dict) else None)
    if scenario["kind"] in {"wxstore", "wxstore_plot_existing"}:
        row["stage"] = result.get("stage")
        row["products"] = len(result.get("products") or [])
        row["export_s"] = (result.get("export") or {}).get("elapsed_s") if isinstance(result.get("export"), dict) else None
        row["import_s"] = (result.get("import") or {}).get("elapsed_s") if isinstance(result.get("import"), dict) else None
        row["showcase_s"] = (result.get("showcase") or {}).get("elapsed_s") if isinstance(result.get("showcase"), dict) else None
        report = result.get("showcase_report") or {}
        if isinstance(report, dict):
            row["rendered_count"] = report.get("rendered_count") or len(result.get("previews") or [])
    return row


def markdown_report(summary: dict[str, Any]) -> str:
    lines = [
        "# RustWx Model Maps Perf Profile",
        "",
        f"- Generated: {summary['generated_at']}",
        f"- Server port: {summary['port']}",
        f"- Bench root: `{summary['bench_root']}`",
        f"- Model/run/hour: `{summary['model']} {summary['run_str']} F{summary['forecast_hour']:03d}`",
        f"- Products: {len(summary['products'])}",
        f"- Threads/jobs: {summary['jobs']}",
        "",
        "| Scenario | OK | Wall | Result | First Preview | Peak RSS | Key Notes |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in summary["rows"]:
        notes = []
        if row.get("planned_fetch_count") is not None:
            notes.append(f"fetches {row.get('planned_fetch_count')}, hits {row.get('cache_hits')}, skipped {row.get('skipped_fetch_count')}, bytes {fmt_bytes(row.get('total_bytes'))}")
        if row.get("rendered_count") is not None:
            notes.append(f"rendered {row.get('rendered_count')}")
        if row.get("cache_hit") is not None:
            notes.append(f"store cache_hit={row.get('cache_hit')}, store {fmt_bytes(row.get('store_bytes'))}")
        if row.get("backend"):
            timing = row.get("sounding_timing") or {}
            notes.append(f"{row.get('backend')}, total_ms={timing.get('total_ms') if isinstance(timing, dict) else '-'}")
        if row.get("stage"):
            notes.append(f"{row.get('stage')}, export {row.get('export_s')}, import {row.get('import_s')}, plot {row.get('showcase_s')}")
        lines.append(
            "| {name} | {ok} | {wall:.2f}s | {result} | {first} | {rss} | {notes} |".format(
                name=row["name"],
                ok=str(row.get("ok")),
                wall=float(row.get("wall_s") or 0),
                result="-" if row.get("result_s") is None else f"{float(row['result_s']):.2f}s",
                first="-" if row.get("first_preview_s") is None else f"{float(row['first_preview_s']):.2f}s",
                rss=fmt_bytes(row.get("peak_rss_bytes")),
                notes="; ".join(notes),
            )
        )
    lines.extend(["", "## Raw Result", "", f"- JSON: `{summary['result_path']}`"])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--port", type=int, default=8789)
    parser.add_argument("--model", default="hrrr")
    parser.add_argument("--source", default="aws")
    parser.add_argument("--run-str", default="latest")
    parser.add_argument("--domain", default="conus")
    parser.add_argument("--forecast-hour", type=int, default=0)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--width", type=int, default=1600)
    parser.add_argument("--height", type=int, default=1100)
    parser.add_argument("--bench-root")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    bench_root = Path(args.bench_root).resolve() if args.bench_root else repo / "scratch" / "model-maps-server" / f"profile4core-{utc_stamp()}"
    cache_dir = bench_root / "cache"
    out_root = bench_root / "out"
    cache_dir.mkdir(parents=True, exist_ok=True)
    out_root.mkdir(parents=True, exist_ok=True)
    status_path = bench_root / "status.json"
    result_path = bench_root / "result.json"
    report_path = bench_root / "report.md"
    stdout_path = bench_root / "server.out.log"
    stderr_path = bench_root / "server.err.log"

    env = os.environ.copy()
    env["RAYON_NUM_THREADS"] = str(args.jobs)
    env["RUSTWX_PLOT_STYLE"] = "operational_fast"
    server_cmd = [
        sys.executable,
        "scripts/rustwx_model_maps.py",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
        "--no-open",
        "--out-root",
        str(out_root),
        "--cache-dir",
        str(cache_dir),
        "--bin-dir",
        str(repo / "target" / "release"),
    ]
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    write_status(status_path, {"phase": "starting", "bench_root": str(bench_root), "port": args.port})
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
        server = subprocess.Popen(
            server_cmd,
            cwd=str(repo),
            env=env,
            stdout=stdout,
            stderr=stderr,
            creationflags=creationflags,
        )
        client = ApiClient(f"http://127.0.0.1:{args.port}")
        scenarios: list[dict[str, Any]] = []
        try:
            bootstrap = wait_ready(client)
            base = {
                "model": args.model,
                "source": args.source,
                "run_str": args.run_str,
                "domain": args.domain,
                "forecast_hour": args.forecast_hour,
                "width": args.width,
                "height": args.height,
                "jobs": args.jobs,
                "png_compression": "fastest",
                "place_label_density": "none",
            }
            wxstore_ok = bool((bootstrap.get("doctor") or {}).get("capabilities", {}).get("wxstore_plot"))
            scenario_defs: list[tuple[str, str, dict[str, Any]]] = [
                (
                    "prepare_data_cold_forecasting",
                    "prepare_data",
                    {
                        **base,
                        "forecast_hours": [args.forecast_hour],
                        "products": FORECASTING_PRODUCTS,
                        "download_workers": args.jobs,
                    },
                ),
                ("direct_render_500mb_warm", "render", {**base, "products": ["500mb_height_winds"]}),
                ("direct_render_forecasting_warm", "render", {**base, "products": FORECASTING_PRODUCTS}),
                ("direct_render_forecasting_hot", "render", {**base, "products": FORECASTING_PRODUCTS}),
                (
                    "pressure_store_build",
                    "pressure_store",
                    {
                        **base,
                        "hours": str(args.forecast_hour),
                        "lat": 39.0,
                        "lon": -98.0,
                        "load_parallelism": args.jobs,
                        "timeout": 1800,
                    },
                ),
                (
                    "sounding_warm_store",
                    "sounding",
                    {
                        **base,
                        "lat": 39.0,
                        "lon": -98.0,
                        "data_mode": "store",
                        "sample_method": "nearest",
                        "timeout": 420,
                        "store_timeout": 1800,
                    },
                ),
            ]
            if wxstore_ok:
                scenario_defs.extend(
                    [
                        (
                            "wxstore_build_import_forecasting",
                            "wxstore",
                            {
                                **base,
                                "hours": str(args.forecast_hour),
                                "products": FORECASTING_PRODUCTS,
                                "import_wxa": True,
                                "render_plots": False,
                                "export_timeout": 1800,
                                "import_timeout": 1800,
                            },
                        ),
                        (
                            "wxstore_plot_500mb",
                            "wxstore_plot_existing",
                            {
                                **base,
                                "run": "latest",
                                "hours": str(args.forecast_hour),
                                "products": ["500mb_height_winds"],
                                "use_domain_bounds": True,
                                "showcase_timeout": 900,
                            },
                        ),
                        (
                            "wxstore_plot_forecasting",
                            "wxstore_plot_existing",
                            {
                                **base,
                                "run": "latest",
                                "hours": str(args.forecast_hour),
                                "products": FORECASTING_PRODUCTS,
                                "use_domain_bounds": True,
                                "showcase_timeout": 900,
                            },
                        ),
                    ]
                )

            for name, kind, payload in scenario_defs:
                scenarios.append(
                    run_job(
                        client=client,
                        server_pid=server.pid,
                        status_path=status_path,
                        name=name,
                        kind=kind,
                        payload=payload,
                    )
                )

            rows = [summarize_scenario(item) for item in scenarios]
            summary = {
                "ok": True,
                "generated_at": now_iso(),
                "bench_root": str(bench_root),
                "port": args.port,
                "model": args.model,
                "source": args.source,
                "run_str": args.run_str,
                "domain": args.domain,
                "forecast_hour": args.forecast_hour,
                "jobs": args.jobs,
                "products": FORECASTING_PRODUCTS,
                "cache_bytes": dir_bytes(cache_dir),
                "output_bytes": dir_bytes(out_root),
                "rows": rows,
                "scenarios": scenarios,
                "result_path": str(result_path),
                "report_path": str(report_path),
            }
            result_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            report_path.write_text(markdown_report(summary), encoding="utf-8")
            write_status(status_path, {"phase": "finished", **summary})
            print(json.dumps({"ok": True, "bench_root": str(bench_root), "report_path": str(report_path), "result_path": str(result_path)}, indent=2))
            return 0
        except Exception as exc:
            payload = {
                "ok": False,
                "phase": "error",
                "bench_root": str(bench_root),
                "port": args.port,
                "error": str(exc),
                "result_path": str(result_path),
                "report_path": str(report_path),
            }
            result_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            write_status(status_path, payload)
            print(json.dumps(payload, indent=2), file=sys.stderr)
            return 1
        finally:
            try:
                server.terminate()
                server.wait(timeout=5)
            except Exception:
                try:
                    server.kill()
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
